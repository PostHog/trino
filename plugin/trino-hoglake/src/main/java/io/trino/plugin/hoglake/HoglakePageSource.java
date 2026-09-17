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

import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
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
 */
public class HoglakePageSource
        implements ConnectorPageSource
{
    /**
     * Either "channel i of the reader page" or "all nulls of this type".
     */
    sealed interface ColumnAdaptation
            permits SourceColumn, NullColumn {}

    record SourceColumn(int sourceChannel)
            implements ColumnAdaptation {}

    record NullColumn(Type type)
            implements ColumnAdaptation {}

    private final ParquetReader parquetReader;
    private final List<ColumnAdaptation> columns;
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

    private boolean closed;
    private long completedPositions;

    public HoglakePageSource(
            ParquetReader parquetReader,
            List<ColumnAdaptation> columns,
            HoglakeDeletionVector deletionVector,
            HoglakeSplitResources resources)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.columns = List.copyOf(columns);
        this.deletionVector = deletionVector;
        this.resources = requireNonNull(resources, "resources is null");
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

    private Page adapt(SourcePage page)
    {
        if (deletionVector == null) {
            return passThrough(page);
        }
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
        if (survivors == positionCount) {
            return passThrough(page);
        }
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            blocks[channel] = switch (columns.get(channel)) {
                case SourceColumn(int sourceChannel) -> page.getBlock(sourceChannel).getPositions(retained, 0, survivors);
                case NullColumn _ -> RunLengthEncodedBlock.create(nullBlocks.get(channel), survivors);
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
                case NullColumn _ -> RunLengthEncodedBlock.create(nullBlocks.get(channel), page.getPositionCount());
            };
        }
        return new Page(page.getPositionCount(), blocks);
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
        return parquetReader.getMetrics();
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
