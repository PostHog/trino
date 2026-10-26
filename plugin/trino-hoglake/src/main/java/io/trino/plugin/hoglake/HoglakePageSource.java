/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hoglake;

import com.google.common.collect.ImmutableMap;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.variant.ShreddedVariantAssembler;
import io.trino.parquet.variant.ShreddedVariantColumns;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Adapts ParquetReader pages to the requested column order, filling
 * catalog columns absent from the file (schema evolution: column added
 * after the file was written) with null blocks, and drops the rows a
 * deletion vector marks deleted. The pattern is trino-hive's
 * ParquetPageSource, trimmed to the adaptations this connector needs.
 *
 * <p>When a vector must be applied the reader is asked for each row's file
 * row position as an extra trailing channel, so a row is matched against
 * the vector by its original, file-relative position. That position is
 * unaffected by projected columns, page and batch boundaries, and pruned
 * earlier row groups — unlike a page-local index.
 *
 * <p>Besides the Parquet reader's own metrics, each split reports whether it
 * found its file's footer already decoded in the footer cache, and how many
 * row groups it reads after pruning. The engine sums these over the query's
 * splits.
 */
public class HoglakePageSource
        implements ConnectorPageSource
{
    /**
     * 1 when the split found its footer decoded in the footer cache, else 0.
     */
    public static final String FOOTER_CACHE_HITS = "footerCacheHits";
    /**
     * 1 when the split decoded its footer itself, else 0.
     */
    public static final String FOOTER_CACHE_MISSES = "footerCacheMisses";
    /**
     * The row groups the split reads: those starting in its byte range that
     * the predicate does not prune.
     */
    public static final String ROW_GROUPS_READ = "rowGroupsRead";
    /**
     * 1 when the split reads no row group, because none starts in its byte
     * range or the predicate prunes all of them, else 0.
     */
    public static final String RANGE_WITH_NO_ROW_GROUPS = "rangeWithNoRowGroups";
    /**
     * The rows of pushed-down VARIANT subscripts that were built with new
     * metadata, because a subscript reads an object or an array of the row
     * whole, or, for subscripts with an empty key, every row. Other rows are
     * built without decoding their objects.
     */
    public static final String VARIANT_SLOW_PATH_ROWS = "variantSlowPathRows";
    /**
     * The reader pages of a shredded VARIANT column with pushed-down
     * subscripts that read its top-level {@code metadata} or {@code value}
     * column, because a row has no {@code typed_value} object, a subscript
     * reads a key that is not shredded, or reads an object or an array whole.
     */
    public static final String VARIANT_RESIDUAL_BATCHES_LOADED = "variantResidualBatchesLoaded";
    /**
     * The reader pages of such a column that read neither column.
     */
    public static final String VARIANT_RESIDUAL_BATCHES_SKIPPED = "variantResidualBatchesSkipped";

    /**
     * Either "channel i of the reader page" or "all nulls of this type".
     */
    sealed interface ColumnAdaptation
            permits NullColumn,
                    RowIdColumn,
                    ShreddedVariantColumn,
                    SourceColumn,
                    UnsignedColumn,
                    VariantNullAsSqlNullColumn {}

    record SourceColumn(int sourceChannel)
            implements ColumnAdaptation {}

    record UnsignedColumn(int sourceChannel, HoglakeColumnHandle column)
            implements ColumnAdaptation {}

    record RowIdColumn(long fileId)
            implements ColumnAdaptation {}

    /**
     * A shredded VARIANT column. The reader returns it as the row of its
     * {@code metadata}, {@code value} and {@code typed_value} columns in
     * {@code sourceChannel}, or, when the top-level {@code metadata} and
     * {@code value} columns are read lazily, as the row of its
     * {@code typed_value} column in {@code sourceChannel} and the rows of the
     * other two columns in their own channels, which the assembler loads only
     * for the batches that need them.
     *
     * @param variantNullIsSqlNull whether the file's writer stores a SQL NULL
     *         as a variant null, which this column then converts. An assembler
     *         can convert them itself instead.
     */
    record ShreddedVariantColumn(int sourceChannel, OptionalInt metadataChannel, OptionalInt valueChannel, ShreddedVariantAssembler assembler, boolean variantNullIsSqlNull)
            implements ColumnAdaptation
    {
        public ShreddedVariantColumn
        {
            requireNonNull(metadataChannel, "metadataChannel is null");
            requireNonNull(valueChannel, "valueChannel is null");
            checkArgument(metadataChannel.isPresent() == valueChannel.isPresent(), "metadata and value channels must both be present or absent");
            requireNonNull(assembler, "assembler is null");
        }

        public ShreddedVariantColumn(int sourceChannel, ShreddedVariantAssembler assembler, boolean variantNullIsSqlNull)
        {
            this(sourceChannel, OptionalInt.empty(), OptionalInt.empty(), assembler, variantNullIsSqlNull);
        }

        /**
         * The VARIANT values of up to {@code maxPositions} rows from
         * {@code start}, stopping once they reach {@code maxSizeInBytes}.
         */
        public Block read(ShreddedVariantColumns shredded, int start, int maxPositions, long maxSizeInBytes)
                throws ParquetCorruptionException
        {
            Block variants = assembler.assemble(shredded, start, maxPositions, maxSizeInBytes);
            if (variantNullIsSqlNull) {
                return variantNullsToSqlNulls(variants);
            }
            return variants;
        }

        /**
         * The columns of this VARIANT in a reader page.
         */
        public PageColumns columns(SourcePage page)
        {
            if (metadataChannel.isEmpty()) {
                return new PageColumns(page, sourceChannel, -1, -1);
            }
            return new PageColumns(page, sourceChannel, metadataChannel.orElseThrow(), valueChannel.orElseThrow());
        }
    }

    /**
     * The columns of a shredded VARIANT in a reader page, whose top-level
     * {@code metadata} and {@code value} columns, when they have their own
     * channels, are loaded the first time that the assembler reads them.
     */
    static final class PageColumns
            implements ShreddedVariantColumns
    {
        private final SourcePage page;
        private final int typedValueChannel;
        private final int metadataChannel;
        private final int valueChannel;
        private ShreddedVariantColumns group;
        private boolean residualLoaded;

        private PageColumns(SourcePage page, int typedValueChannel, int metadataChannel, int valueChannel)
        {
            this.page = requireNonNull(page, "page is null");
            this.typedValueChannel = typedValueChannel;
            this.metadataChannel = metadataChannel;
            this.valueChannel = valueChannel;
        }

        /**
         * Whether the top-level metadata or value column was read.
         */
        public boolean residualLoaded()
        {
            return residualLoaded;
        }

        public boolean lazy()
        {
            return metadataChannel >= 0;
        }

        @Override
        public int positionCount()
        {
            return page.getPositionCount();
        }

        @Override
        public boolean isNull(int position)
        {
            return typedValueGroup().isNull(position);
        }

        @Override
        public Block metadata()
        {
            if (!lazy()) {
                return group().metadata();
            }
            residualLoaded = true;
            return getRowFieldsFromBlock(page.getBlock(metadataChannel)).getFirst();
        }

        @Override
        public Block value()
        {
            if (!lazy()) {
                return group().value();
            }
            residualLoaded = true;
            return getRowFieldsFromBlock(page.getBlock(valueChannel)).getFirst();
        }

        @Override
        public Block typedValue()
        {
            if (!lazy()) {
                return group().typedValue();
            }
            return getRowFieldsFromBlock(typedValueGroup()).getFirst();
        }

        private Block typedValueGroup()
        {
            return page.getBlock(typedValueChannel);
        }

        private ShreddedVariantColumns group()
        {
            if (group == null) {
                group = ShreddedVariantColumns.of(page.getBlock(typedValueChannel));
            }
            return group;
        }
    }

    /**
     * An unshredded VARIANT column from a writer that stores a SQL NULL as a
     * variant null.
     */
    record VariantNullAsSqlNullColumn(int sourceChannel)
            implements ColumnAdaptation {}

    private static Block variantNullsToSqlNulls(Block variants)
    {
        int positionCount = variants.getPositionCount();
        boolean hasVariantNull = false;
        for (int position = 0; position < positionCount && !hasVariantNull; position++) {
            hasVariantNull = !variants.isNull(position) && VARIANT.getObject(variants, position).isNull();
        }
        if (!hasVariantNull) {
            return variants;
        }
        BlockBuilder builder = VARIANT.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (variants.isNull(position) || VARIANT.getObject(variants, position).isNull()) {
                builder.appendNull();
            }
            else {
                VARIANT.writeObject(builder, VARIANT.getObject(variants, position));
            }
        }
        return builder.build();
    }

    record NullColumn(Type type)
            implements ColumnAdaptation {}

    private final ParquetReader parquetReader;
    private final List<ColumnAdaptation> columns;
    private final boolean hasShreddedColumns;
    /**
     * Assembling a shredded VARIANT copies every value, so the reader cannot
     * size a page by its columns. A page of assembled values ends once it
     * reaches this size, and the rest of the reader page follows in the next
     * pages.
     */
    private final long maxShreddedPageSizeInBytes;
    private final List<Block> nullBlocks;
    /**
     * Null when the split has no deletion vector, in which case the reader
     * page carries no row-position channel and pages pass through
     * unchanged.
     */
    private final HoglakeDeletionVector deletionVector;
    /**
     * The split's accounting and ownership, adopted from its construction
     * scope. One owner, one total: the reader's buffers and any deletion
     * vector bitmap are charged in the same aggregation.
     */
    private final HoglakeSplitResources resources;
    private final Metrics splitMetrics;

    private boolean closed;
    private long completedPositions;
    /**
     * A reader page with the deletion vector applied, whose shredded VARIANT
     * values are assembled in later calls, the position of its first row that
     * is not returned yet, and its channels that are read so far.
     */
    private SourcePage pendingPage;
    private int pendingPosition;
    private Block[] pendingBlocks;
    private PageColumns[] pendingVariantColumns;
    private long variantResidualBatchesLoaded;
    private long variantResidualBatchesSkipped;

    public HoglakePageSource(
            ParquetReader parquetReader,
            List<ColumnAdaptation> columns,
            HoglakeDeletionVector deletionVector,
            HoglakeSplitResources resources,
            Metrics splitMetrics,
            long maxShreddedPageSizeInBytes)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.columns = List.copyOf(columns);
        this.hasShreddedColumns = columns.stream().anyMatch(ShreddedVariantColumn.class::isInstance);
        checkArgument(maxShreddedPageSizeInBytes > 0, "maxShreddedPageSizeInBytes must be positive");
        this.maxShreddedPageSizeInBytes = maxShreddedPageSizeInBytes;
        this.deletionVector = deletionVector;
        this.resources = requireNonNull(resources, "resources is null");
        this.splitMetrics = requireNonNull(splitMetrics, "splitMetrics is null");
        this.nullBlocks = new ArrayList<>(columns.size());
        for (ColumnAdaptation column : columns) {
            nullBlocks.add(column instanceof NullColumn(Type type)
                    ? type.createBlockBuilder(null, 1, 0).appendNull().build()
                    : null);
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (pendingPage == null) {
            SourcePage page;
            try {
                page = parquetReader.nextPage();
            }
            catch (IOException | RuntimeException e) {
                close();
                throw handleException(parquetReader.getDataSource().getId(), e);
            }
            if (page == null) {
                close();
                return null;
            }
            if (!hasShreddedColumns) {
                Page adapted;
                try {
                    adapted = adapt(page);
                }
                catch (RuntimeException e) {
                    close();
                    throw handleException(parquetReader.getDataSource().getId(), e);
                }
                completedPositions += adapted.getPositionCount();
                return SourcePage.create(adapted);
            }
            try {
                // The shredded VARIANT channels stay unread until the assembler reads them
                pendingPage = selectRows(page);
            }
            catch (RuntimeException e) {
                close();
                throw handleException(parquetReader.getDataSource().getId(), e);
            }
            pendingPosition = 0;
            pendingBlocks = new Block[columns.size()];
            pendingVariantColumns = new PageColumns[columns.size()];
        }
        Page next;
        try {
            next = nextPendingRows();
        }
        catch (IOException | RuntimeException e) {
            close();
            throw handleException(parquetReader.getDataSource().getId(), e);
        }
        completedPositions += next.getPositionCount();
        return SourcePage.create(next);
    }

    /**
     * The next rows of the pending page, with assembled VARIANT values for
     * its shredded channels.
     */
    private Page nextPendingRows()
            throws ParquetCorruptionException
    {
        int positions = pendingPage.getPositionCount() - pendingPosition;
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            if (columns.get(channel) instanceof ShreddedVariantColumn column) {
                if (pendingVariantColumns[channel] == null) {
                    pendingVariantColumns[channel] = column.columns(pendingPage);
                }
                blocks[channel] = column.read(pendingVariantColumns[channel], pendingPosition, positions, maxShreddedPageSizeInBytes);
                positions = min(positions, blocks[channel].getPositionCount());
            }
        }
        for (int channel = 0; channel < columns.size(); channel++) {
            if (blocks[channel] == null) {
                blocks[channel] = pendingBlock(channel).getRegion(pendingPosition, positions);
            }
            else if (blocks[channel].getPositionCount() > positions) {
                // Another shredded channel reached the size limit in fewer rows
                blocks[channel] = blocks[channel].getRegion(0, positions);
            }
        }
        pendingPosition += positions;
        if (pendingPosition == pendingPage.getPositionCount()) {
            for (PageColumns variantColumns : pendingVariantColumns) {
                if (variantColumns != null && variantColumns.lazy()) {
                    if (variantColumns.residualLoaded()) {
                        variantResidualBatchesLoaded++;
                    }
                    else {
                        variantResidualBatchesSkipped++;
                    }
                }
            }
            pendingPage = null;
            pendingBlocks = null;
            pendingVariantColumns = null;
        }
        return new Page(positions, blocks);
    }

    /**
     * The block of a channel that is not a shredded VARIANT, for the rows of
     * the pending page.
     */
    private Block pendingBlock(int channel)
    {
        if (pendingBlocks[channel] == null) {
            pendingBlocks[channel] = switch (columns.get(channel)) {
                case SourceColumn(int sourceChannel) -> pendingPage.getBlock(sourceChannel);
                case UnsignedColumn(int sourceChannel, HoglakeColumnHandle column) -> HoglakeUnsigned.convert(column, pendingPage.getBlock(sourceChannel), false);
                case NullColumn _ -> RunLengthEncodedBlock.create(nullBlocks.get(channel), pendingPage.getPositionCount());
                case RowIdColumn(long fileId) -> rowIds(fileId, pendingPage);
                case VariantNullAsSqlNullColumn(int sourceChannel) -> variantNullsToSqlNulls(pendingPage.getBlock(sourceChannel));
                case ShreddedVariantColumn _ -> throw new IllegalStateException("A shredded VARIANT channel is assembled");
            };
        }
        return pendingBlocks[channel];
    }

    /**
     * The page without the rows that the deletion vector deletes. The reader
     * reads the channels that are not read yet only for the remaining rows.
     */
    private SourcePage selectRows(SourcePage page)
    {
        if (deletionVector == null) {
            return page;
        }
        int[] retained = retainedPositions(page);
        if (retained.length < page.getPositionCount()) {
            page.selectPositions(retained, 0, retained.length);
        }
        return page;
    }

    private int[] retainedPositions(SourcePage page)
    {
        // The reader's last channel holds each row's file row position.
        Block rowPositions = page.getBlock(page.getChannelCount() - 1);
        int positionCount = page.getPositionCount();
        int[] retained = new int[positionCount];
        int survivors = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!deletionVector.isRowDeleted(BIGINT.getLong(rowPositions, position))) {
                retained[survivors] = position;
                survivors++;
            }
        }
        return Arrays.copyOf(retained, survivors);
    }

    private Page adapt(SourcePage page)
    {
        if (deletionVector == null) {
            return passThrough(page);
        }
        int[] retained = retainedPositions(page);
        int survivors = retained.length;
        if (survivors == page.getPositionCount()) {
            return passThrough(page);
        }
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            blocks[channel] = switch (columns.get(channel)) {
                case SourceColumn(int sourceChannel) -> page.getBlock(sourceChannel).getPositions(retained, 0, survivors);
                case UnsignedColumn(int sourceChannel, HoglakeColumnHandle column) -> HoglakeUnsigned.convert(column, page.getBlock(sourceChannel).getPositions(retained, 0, survivors), false);
                case NullColumn _ -> RunLengthEncodedBlock.create(nullBlocks.get(channel), survivors);
                case RowIdColumn(long fileId) -> rowIds(fileId, page).getPositions(retained, 0, survivors);
                case ShreddedVariantColumn _ -> throw new IllegalStateException("A page with a shredded VARIANT is not adapted");
                case VariantNullAsSqlNullColumn(int sourceChannel) -> variantNullsToSqlNulls(page.getBlock(sourceChannel).getPositions(retained, 0, survivors));
            };
        }
        return new Page(survivors, blocks);
    }

    private Page passThrough(SourcePage page)
    {
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            blocks[channel] = switch (columns.get(channel)) {
                case SourceColumn(int sourceChannel) -> page.getBlock(sourceChannel);
                case UnsignedColumn(int sourceChannel, HoglakeColumnHandle column) -> HoglakeUnsigned.convert(column, page.getBlock(sourceChannel), false);
                case NullColumn _ -> RunLengthEncodedBlock.create(nullBlocks.get(channel), page.getPositionCount());
                case RowIdColumn(long fileId) -> rowIds(fileId, page);
                case ShreddedVariantColumn _ -> throw new IllegalStateException("A page with a shredded VARIANT is not adapted");
                case VariantNullAsSqlNullColumn(int sourceChannel) -> variantNullsToSqlNulls(page.getBlock(sourceChannel));
            };
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static Block rowIds(long fileId, SourcePage page)
    {
        var id = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(id, fileId);
        return RowBlock.fromNotNullSuppressedFieldBlocks(page.getPositionCount(), Optional.empty(), new Block[] {
                RunLengthEncodedBlock.create(id.build(), page.getPositionCount()), page.getBlock(page.getChannelCount() - 1),
        });
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getMemoryUsage()
    {
        // The split's one aggregation: the reader's buffers and the deletion
        // vector's bitmap. Reporting that total keeps this accessor and the
        // engine's context describing the same accounting.
        return resources.allocation().getBytes();
    }

    @Override
    public Metrics getMetrics()
    {
        Metrics metrics = splitMetrics.mergeWith(parquetReader.getMetrics());
        if (!hasShreddedColumns) {
            return metrics;
        }
        long slowPathRows = 0;
        boolean lazyResidual = false;
        for (ColumnAdaptation column : columns) {
            if (column instanceof ShreddedVariantColumn shredded) {
                slowPathRows += shredded.assembler().rowsWithNewMetadata();
                lazyResidual |= shredded.metadataChannel().isPresent();
            }
        }
        ImmutableMap.Builder<String, Metric<?>> variantMetrics = ImmutableMap.builder();
        variantMetrics.put(VARIANT_SLOW_PATH_ROWS, new LongCount(slowPathRows));
        if (lazyResidual) {
            variantMetrics.put(VARIANT_RESIDUAL_BATCHES_LOADED, new LongCount(variantResidualBatchesLoaded));
            variantMetrics.put(VARIANT_RESIDUAL_BATCHES_SKIPPED, new LongCount(variantResidualBatchesSkipped));
        }
        return metrics.mergeWith(new Metrics(variantMetrics.buildOrThrow()));
    }

    static Metrics splitMetrics(boolean footerCacheHit, int rowGroupsRead)
    {
        return new Metrics(ImmutableMap.<String, Metric<?>>of(
                FOOTER_CACHE_HITS, new LongCount(footerCacheHit ? 1 : 0),
                FOOTER_CACHE_MISSES, new LongCount(footerCacheHit ? 0 : 1),
                ROW_GROUPS_READ, new LongCount(rowGroupsRead),
                RANGE_WITH_NO_ROW_GROUPS, new LongCount(rowGroupsRead == 0 ? 1 : 0)));
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            // The reader charges into the split's aggregation, so it is closed
            // before the split owner releases that aggregation.
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            resources.close();
        }
    }

    static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(GENERIC_INTERNAL_ERROR, "Corrupt parquet data: " + dataSourceId, exception);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read parquet file: " + dataSourceId, exception);
    }
}
