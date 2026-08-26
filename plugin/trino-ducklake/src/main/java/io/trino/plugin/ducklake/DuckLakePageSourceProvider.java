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
package io.trino.plugin.ducklake;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMappingEntry;
import io.trino.plugin.ducklake.util.DuckLakeFieldIds;
import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.trino.memory.context.AggregatedMemoryContext.newAggregatedMemoryContext;
import static io.trino.plugin.ducklake.DuckLakeColumnHandle.ROW_COUNT_COLUMN;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DuckLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final int DOMAIN_COMPACTION_THRESHOLD = 100;
    /**
     * Prefix of the synthetic Parquet column name used for a column that a name mapping does not
     * map to any column of the file. No Parquet column can match it, so the column reads as NULL.
     */
    private static final String MISSING_COLUMN_NAME_PREFIX = "$ducklake_missing_column$";
    private static final int LONG_OPEN_HASH_SET_INSTANCE_SIZE = instanceSize(LongOpenHashSet.class);
    /**
     * Positions of a page produced for a scan that reads row counts only. A page of no columns
     * holds no data, so this only bounds the position count to an {@code int}.
     */
    private static final long MAX_ROW_COUNT_PAGE_POSITIONS = 1024 * 1024;
    /**
     * Bytes a Parquet file ends with: the length of the footer preceding them, followed by the
     * file magic. The catalog records the length of the footer without them.
     */
    private static final int PARQUET_POST_SCRIPT_SIZE = Integer.BYTES + 4;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public DuckLakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        if (split instanceof DuckLakeRowCountSplit rowCountSplit) {
            return rowCountPageSource(rowCountSplit, columns);
        }
        DuckLakeSplit duckLakeSplit = (DuckLakeSplit) split;
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) table;
        List<DuckLakeColumnHandle> requestedColumns = columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .collect(toImmutableList());
        List<DuckLakeColumnHandle> dataColumns = requestedColumns.stream()
                .filter(column -> !DuckLakeMergeRowId.isRowIdColumn(column))
                .collect(toImmutableList());
        boolean rowIdRequested = dataColumns.size() != requestedColumns.size();

        for (DuckLakeColumnHandle column : dataColumns) {
            if (column.initialDefault().isPresent()) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Column '%s' has a non-NULL initial default value, which is not yet supported".formatted(column.name()));
            }
        }

        TupleDomain<DuckLakeColumnHandle> effectivePredicate = tableHandle.unenforcedConstraint()
                .intersect(dynamicFilter.getCurrentPredicate().transformKeys(DuckLakeColumnHandle.class::cast));
        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }
        if (isRowCountOnly(requestedColumns, effectivePredicate, duckLakeSplit)) {
            return new DuckLakeCountPageSource(rowCountPages(duckLakeSplit.recordCount()));
        }
        Optional<DuckLakeNameMapping> nameMapping = duckLakeSplit.nameMapping();
        TupleDomain<HiveColumnHandle> parquetPredicate = toParquetPredicate(effectivePredicate.simplify(DOMAIN_COMPACTION_THRESHOLD), nameMapping);

        Optional<DuckLakeDeleteFileHandle> deleteFile = duckLakeSplit.deleteFile();

        ImmutableList.Builder<HiveColumnHandle> hiveColumnsBuilder = ImmutableList.builderWithExpectedSize(dataColumns.size() + 1);
        dataColumns.stream()
                .map(column -> toHiveColumnHandle(column, nameMapping))
                .forEach(hiveColumnsBuilder::add);
        // the row index is read to apply positional deletes and to identify rows a merge changes
        if (deleteFile.isPresent() || rowIdRequested) {
            hiveColumnsBuilder.add(PARQUET_ROW_INDEX_COLUMN);
        }
        List<HiveColumnHandle> hiveColumns = hiveColumnsBuilder.build();
        int rowIndexChannel = dataColumns.size();
        boolean hasRowIndexChannel = hiveColumns.size() > dataColumns.size();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(duckLakeSplit.path()), duckLakeSplit.fileSizeBytes());

        ParquetReaderOptions options = ParquetReaderOptions.builder(parquetReaderOptions)
                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                // row skipping via the column index would break the row-index alignment needed to apply positional
                // deletes; row-group pruning remains safe because the row-index column is absolute within the file
                .withUseColumnIndex(deleteFile.isEmpty() && !rowIdRequested && isParquetUseColumnIndex(session))
                .withIgnoreStatistics(isParquetIgnoreStatistics(session))
                .build();

        if (deleteFile.isPresent()) {
            AggregatedMemoryContext splitMemoryContext = newAggregatedMemoryContext(memoryContext);
            LocalMemoryContext dataFileMemoryContext = splitMemoryContext.newLocalMemoryContext(DuckLakePageSourceProvider.class.getSimpleName());
            ConnectorPageSource pageSource = createParquetPageSource(inputFile, duckLakeSplit, dataColumns, hiveColumns, parquetPredicate, options, dataFileMemoryContext::setBytes);
            LocalMemoryContext deletedPositionsMemoryContext = splitMemoryContext.newLocalMemoryContext("deletedPositions");
            // Every split of the data file loads the whole delete file, so a file split into
            // several byte ranges re-reads its delete file once per split. The positions are
            // file-absolute, so each split simply ignores the ones outside the rows it reads.
            Supplier<LongOpenHashSet> deletedPositions = Suppliers.memoize(() -> readDeletedPositions(fileSystem, deleteFile.get(), splitMemoryContext, deletedPositionsMemoryContext));
            pageSource = TransformConnectorPageSource.create(
                    pageSource,
                    page -> filterDeletedRows(page, deletedPositions.get(), rowIndexChannel));
            return new MemoryContextClosingPageSource(
                    projectRequestedColumns(requestedColumns, rowIndexChannel, hasRowIndexChannel, duckLakeSplit.dataFileId(), pageSource),
                    splitMemoryContext);
        }

        ConnectorPageSource pageSource = createParquetPageSource(inputFile, duckLakeSplit, dataColumns, hiveColumns, parquetPredicate, options, memoryContext);
        return projectRequestedColumns(requestedColumns, rowIndexChannel, hasRowIndexChannel, duckLakeSplit.dataFileId(), pageSource);
    }

    /**
     * Produces the columns the engine asked for from the columns read out of the data file: the
     * wider engine types of DuckLake's unsigned and 128 bit integers, and the synthetic row
     * identifier a merge deletes rows by.
     */
    private static ConnectorPageSource projectRequestedColumns(
            List<DuckLakeColumnHandle> requestedColumns,
            int rowIndexChannel,
            boolean hasRowIndexChannel,
            long dataFileId,
            ConnectorPageSource pageSource)
    {
        boolean needsTransform = requestedColumns.stream()
                .anyMatch(column -> column.isUnsignedInteger() || column.isUnsignedBigint() || column.isInt128());
        // the row index is read to apply deletes and to build row identifiers, and is projected
        // away again unless the engine asked for the identifier itself
        if (!hasRowIndexChannel && !needsTransform) {
            return pageSource;
        }
        TransformConnectorPageSource.Builder transform = TransformConnectorPageSource.builder();
        if (hasRowIndexChannel) {
            // the projection is needed even when it selects the leading channels unchanged,
            // because it is what drops the row index the reader added
            transform.forceTransforms();
        }
        int dataChannel = 0;
        for (DuckLakeColumnHandle column : requestedColumns) {
            if (DuckLakeMergeRowId.isRowIdColumn(column)) {
                transform.transform(page -> rowIdBlock(dataFileId, page.getBlock(rowIndexChannel)));
                continue;
            }
            int channel = dataChannel;
            dataChannel++;
            if (column.isUnsignedInteger()) {
                transform.transform(channel, new UnsignedIntegerToBigint());
            }
            else if (column.isUnsignedBigint()) {
                transform.transform(channel, new UnsignedBigintToDecimal());
            }
            else if (column.isInt128()) {
                transform.transform(channel, new DoubleToInt128Decimal());
            }
            else {
                transform.column(channel);
            }
        }
        return transform.build(pageSource);
    }

    private static Block rowIdBlock(long dataFileId, Block rowIndexBlock)
    {
        int positionCount = rowIndexBlock.getPositionCount();
        BlockBuilder dataFileIds = BIGINT.createFixedSizeBlockBuilder(positionCount);
        for (int position = 0; position < positionCount; position++) {
            BIGINT.writeLong(dataFileIds, dataFileId);
        }
        return RowBlock.fromFieldBlocks(positionCount, new Block[] {dataFileIds.build(), rowIndexBlock});
    }

    /**
     * The single row holding the row count of a table whose {@code count(*)} was pushed into the
     * catalog. Only {@link DuckLakeColumnHandle#ROW_COUNT_COLUMN} can be read from such a scan.
     */
    private static ConnectorPageSource rowCountPageSource(DuckLakeRowCountSplit split, List<ColumnHandle> columns)
    {
        checkArgument(columns.equals(ImmutableList.of(ROW_COUNT_COLUMN)), "Unexpected columns for a row count split: %s", columns);
        BlockBuilder rowCount = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(rowCount, split.rowCount());
        return new DuckLakeCountPageSource(ImmutableList.of(new Page(1, rowCount.build())));
    }

    /**
     * Whether the split can be answered from its record count alone, without opening the data file.
     * That needs three things to hold:
     * <ul>
     * <li>the scan projects no column, so the only thing the engine reads is the number of rows,
     * as it does for {@code count(*)};
     * <li>no predicate is left for the reader, so every row of the split is counted. Predicates the
     * connector enforces are already applied by pruning files in the split manager, and a predicate
     * it does not enforce is applied by the engine, which would then have to project the column it
     * reads;
     * <li>the split covers the whole data file, because only then is its record count the exact
     * number of visible rows rather than a share of the file apportioned to a byte range.
     * </ul>
     */
    private static boolean isRowCountOnly(List<DuckLakeColumnHandle> columns, TupleDomain<DuckLakeColumnHandle> effectivePredicate, DuckLakeSplit split)
    {
        return columns.isEmpty()
                && effectivePredicate.isAll()
                && split.start() == 0
                && split.length() == split.fileSizeBytes();
    }

    /**
     * Splits a row count into pages of no columns, which carry only their position count. Such a
     * page holds no data, so the only reason to produce more than one is that a page counts its
     * positions in an {@code int}.
     */
    private static List<Page> rowCountPages(long rowCount)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (long remaining = rowCount; remaining > 0; remaining -= MAX_ROW_COUNT_PAGE_POSITIONS) {
            pages.add(new Page(toIntExact(min(remaining, MAX_ROW_COUNT_PAGE_POSITIONS))));
        }
        return pages.build();
    }

    /**
     * Applies the footer size the catalog records for a file, so that the reader fetches the
     * footer in one request instead of reading a fixed length guess and reading again when the
     * footer turns out to be longer. Files written with many columns or many row groups carry a
     * footer well past the default guess, and every split of such a file pays for the second
     * request.
     * <p>
     * The size is only a hint. A missing or stale one costs no more than the request it was meant
     * to save, because the reader reads the footer again whenever the bytes it holds do not cover
     * it, and it locates the footer from the file itself either way.
     */
    private static ParquetReaderOptions withCatalogFooterSize(ParquetReaderOptions options, OptionalLong footerSize, long fileSizeBytes)
    {
        if (footerSize.isEmpty()) {
            return options;
        }
        long footerLength = footerSize.orElseThrow();
        if (footerLength < 0 || footerLength + PARQUET_POST_SCRIPT_SIZE > fileSizeBytes) {
            // the catalog disagrees with the file, so let the reader find the footer on its own
            return options;
        }
        // Reading beyond the configured maximum is wasted, because a footer that long is rejected.
        long footerReadSize = min(footerLength + PARQUET_POST_SCRIPT_SIZE, options.getMaxFooterReadSize().toBytes());
        return ParquetReaderOptions.builder(options)
                .withFooterReadSize(DataSize.ofBytes(footerReadSize))
                .build();
    }

    private ConnectorPageSource createParquetPageSource(
            TrinoInputFile inputFile,
            DuckLakeSplit split,
            List<DuckLakeColumnHandle> dataColumns,
            List<HiveColumnHandle> hiveColumns,
            TupleDomain<HiveColumnHandle> parquetPredicate,
            ParquetReaderOptions options,
            MemoryContext memoryContext)
    {
        // A file resolved through a name mapping carries no identifiers of its own, and one is
        // only reached here when the catalog has no mapping for it, so its identifiers are what
        // name its columns.
        ParquetPageSourceFactory.ColumnsForFile columnsForFile = split.nameMapping().isPresent()
                ? (_, columns) -> columns
                : (fileSchema, columns) -> columnsByFieldId(fileSchema, dataColumns, columns);
        // the reader only returns the row groups starting inside the byte range of the split, so
        // the splits of a file read every row group of it exactly once
        return ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.start(),
                split.length(),
                hiveColumns,
                ImmutableList.of(parquetPredicate),
                true, // resolve columns by name, after the identifiers have chosen which name
                DateTimeZone.UTC,
                fileFormatDataSourceStats,
                withCatalogFooterSize(options, split.footerSize(), split.fileSizeBytes()),
                Optional.empty(),
                Optional.empty(),
                DOMAIN_COMPACTION_THRESHOLD,
                OptionalLong.of(split.fileSizeBytes()),
                memoryContext,
                columnsForFile);
    }

    /**
     * Renames the columns to read to the names this file gives them, matching the column
     * identifier of each against the Parquet field ids the file records.
     * <p>
     * A column the file does not record is read as NULL, under a name no column of the file
     * carries. Letting it keep the name the catalog gives it would not do: renaming a column frees
     * its old name for a new one, and the file still stores the renamed column under that name, so
     * the new column would be handed the old column's values. The row index column the reader
     * appends carries no identifier and is left alone.
     */
    private static List<HiveColumnHandle> columnsByFieldId(MessageType fileSchema, List<DuckLakeColumnHandle> dataColumns, List<HiveColumnHandle> columns)
    {
        Map<Integer, String> namesByFieldId = DuckLakeFieldIds.columnNamesByFieldId(fileSchema);
        if (namesByFieldId.isEmpty()) {
            return columns;
        }
        Set<String> fileColumnNames = DuckLakeFieldIds.columnNames(fileSchema);
        ImmutableList.Builder<HiveColumnHandle> resolved = ImmutableList.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            if (i >= dataColumns.size()) {
                resolved.add(columns.get(i));
                continue;
            }
            DuckLakeColumnHandle column = dataColumns.get(i);
            String name = DuckLakeFieldIds.columnName(namesByFieldId, column.columnId())
                    .orElseGet(() -> DuckLakeFieldIds.absentColumnName(fileColumnNames, column.columnId()));
            resolved.add(column.toHiveColumnHandle(name));
        }
        return resolved.build();
    }

    /**
     * Converts the predicate to the Hive column handles used for reading Parquet files,
     * keeping only top-level primitive columns whose raw Parquet values are directly
     * comparable with the engine values. Columns of a file that uses a name mapping are
     * pushed down under their mapped Parquet name; columns the mapping does not resolve to
     * a Parquet column of the file are not pushed down, which is safe because the predicate
     * is not enforced by the reader.
     */
    private static TupleDomain<HiveColumnHandle> toParquetPredicate(TupleDomain<DuckLakeColumnHandle> effectivePredicate, Optional<DuckLakeNameMapping> nameMapping)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }
        ImmutableMap.Builder<HiveColumnHandle, Domain> domains = ImmutableMap.builder();
        effectivePredicate.getDomains().orElseThrow().forEach((column, domain) -> {
            if (!isParquetPushableColumn(column)) {
                return;
            }
            if (nameMapping.isEmpty()) {
                domains.put(column.toHiveColumnHandle(), domain);
                return;
            }
            nameMapping.get().entry(column.columnId())
                    .filter(entry -> !entry.hivePartition() && !entry.hasNestedFields())
                    .ifPresent(entry -> domains.put(column.toHiveColumnHandle(entry.sourceName()), domain));
        });
        return TupleDomain.withColumnDomains(domains.buildOrThrow());
    }

    /**
     * Returns the Hive column handle used to read the column from the data file. Columns of a
     * file that uses a name mapping are read under the Parquet name the mapping assigns to the
     * DuckLake field; columns the mapping does not cover are not stored in the file and are read
     * as NULL, like columns added after the file was written.
     */
    private static HiveColumnHandle toHiveColumnHandle(DuckLakeColumnHandle column, Optional<DuckLakeNameMapping> nameMapping)
    {
        if (nameMapping.isEmpty()) {
            return column.toHiveColumnHandle();
        }
        Optional<DuckLakeNameMappingEntry> entry = nameMapping.get().entry(column.columnId());
        if (entry.isEmpty()) {
            return column.toHiveColumnHandle(MISSING_COLUMN_NAME_PREFIX + column.columnId());
        }
        if (entry.get().hivePartition()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Column '%s' is stored as a Hive partition value in the file path, which is not supported".formatted(column.name()));
        }
        if (entry.get().hasNestedFields()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Column '%s' has a name mapping for its nested fields, which is not supported".formatted(column.name()));
        }
        return column.toHiveColumnHandle(entry.get().sourceName());
    }

    private static boolean isParquetPushableColumn(DuckLakeColumnHandle column)
    {
        if (column.isUnsignedInteger() || column.isUnsignedBigint() || column.isInt128()) {
            // read as a narrower physical type and reinterpreted afterwards, so domains
            // over the engine type do not apply to the raw Parquet values
            return false;
        }
        Type type = column.type();
        // Parquet only stores statistics for primitive columns; UUID statistics use a
        // different ordering than the engine, so they are skipped conservatively
        return !(type instanceof ArrayType) && !(type instanceof MapType) && !(type instanceof RowType) && !type.equals(UuidType.UUID);
    }

    private static SourcePage filterDeletedRows(SourcePage page, LongOpenHashSet deletedPositions, int rowIndexChannel)
    {
        int positionCount = page.getPositionCount();
        Block rowIndexBlock = page.getBlock(rowIndexChannel);
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!deletedPositions.contains(BIGINT.getLong(rowIndexBlock, position))) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount < positionCount) {
            page.selectPositions(retained, 0, retainedCount);
        }
        return page;
    }

    /**
     * Reads the row positions from a DuckLake positional delete file: a Parquet file with a
     * {@code file_path} column and a {@code pos} column holding the 0-based row indexes deleted
     * from the single data file the delete file belongs to. The retained size of the loaded
     * positions is accounted in {@code deletedPositionsMemoryContext}, which stays open until
     * the enclosing page source is closed.
     */
    private LongOpenHashSet readDeletedPositions(
            TrinoFileSystem fileSystem,
            DuckLakeDeleteFileHandle deleteFile,
            AggregatedMemoryContext memoryContext,
            LocalMemoryContext deletedPositionsMemoryContext)
    {
        HiveColumnHandle positionColumn = new HiveColumnHandle(
                "pos",
                0,
                DuckLakeTypes.toHiveType(BIGINT),
                BIGINT,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(deleteFile.path()), deleteFile.fileSizeBytes());
        int expectedPositions = toIntExact(deleteFile.deleteCount());
        LongOpenHashSet deletedPositions = new LongOpenHashSet(expectedPositions);
        deletedPositionsMemoryContext.setBytes(estimatedRetainedSizeOfLongSet(expectedPositions));
        long rowCount = 0;
        LocalMemoryContext readerMemoryContext = memoryContext.newLocalMemoryContext("deleteFileReader");
        try (ConnectorPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                0,
                deleteFile.fileSizeBytes(),
                ImmutableList.of(positionColumn),
                ImmutableList.of(TupleDomain.all()),
                true, // resolve columns by name
                DateTimeZone.UTC,
                fileFormatDataSourceStats,
                withCatalogFooterSize(parquetReaderOptions, deleteFile.footerSize(), deleteFile.fileSizeBytes()),
                Optional.empty(),
                Optional.empty(),
                DOMAIN_COMPACTION_THRESHOLD,
                OptionalLong.of(deleteFile.fileSizeBytes()),
                readerMemoryContext::setBytes)) {
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Delete file %s contains a null position".formatted(deleteFile.path()));
                    }
                    deletedPositions.add(BIGINT.getLong(block, position));
                }
                rowCount += page.getPositionCount();
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Failed to read delete file %s".formatted(deleteFile.path()), e);
        }
        finally {
            readerMemoryContext.close();
        }
        if (rowCount != deleteFile.deleteCount()) {
            // guards against delete files whose rows are only partially visible at the snapshot
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Delete file %s contains %s positions but the catalog records %s deleted rows".formatted(deleteFile.path(), rowCount, deleteFile.deleteCount()));
        }
        deletedPositionsMemoryContext.setBytes(estimatedRetainedSizeOfLongSet(deletedPositions.size()));
        return deletedPositions;
    }

    private static long estimatedRetainedSizeOfLongSet(int size)
    {
        // LongOpenHashSet stores the values in a power-of-two sized open-addressed array plus one slot for the zero key
        return LONG_OPEN_HASH_SET_INSTANCE_SIZE + sizeOfLongArray(HashCommon.arraySize(size, Hash.DEFAULT_LOAD_FACTOR) + 1);
    }

    /**
     * Forwards to the wrapped page source and closes the given memory context when the page
     * source is closed, freeing memory tracked for the lifetime of the page source (such as
     * the loaded delete positions).
     */
    private static final class MemoryContextClosingPageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSource delegate;
        private final AggregatedMemoryContext memoryContext;

        private MemoryContextClosingPageSource(ConnectorPageSource delegate, AggregatedMemoryContext memoryContext)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return delegate.getCompletedPositions();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            return delegate.getNextSourcePage();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public Metrics getMetrics()
        {
            return delegate.getMetrics();
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                delegate.close();
            }
            finally {
                memoryContext.close();
            }
        }
    }

    /**
     * Reinterprets raw 64-bit values read as {@code BIGINT} into unsigned values of type
     * {@code DECIMAL(20, 0)}. The unsigned interpretation of the raw two's complement bits
     * is the 128-bit value with a zero high half and the raw bits as the low half.
     */
    /**
     * Zero-extends raw 32-bit values read as {@code INTEGER} into unsigned values of type
     * {@code BIGINT}.
     */
    private static class UnsignedIntegerToBigint
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            BlockBuilder builder = BIGINT.createBlockBuilder(null, block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, Integer.toUnsignedLong(INTEGER.getInt(block, position)));
                }
            }
            return builder.build();
        }
    }

    private static class UnsignedBigintToDecimal
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            BlockBuilder builder = DuckLakeTypes.UNSIGNED_BIGINT_DECIMAL_TYPE.createBlockBuilder(null, block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    builder.appendNull();
                }
                else {
                    DuckLakeTypes.UNSIGNED_BIGINT_DECIMAL_TYPE.writeObject(builder, Int128.valueOf(0, BIGINT.getLong(block, position)));
                }
            }
            return builder.build();
        }
    }

    /**
     * Converts {@code DOUBLE} values to {@code DECIMAL(38, 0)}. DuckDB stores 128-bit integer
     * columns as {@code DOUBLE} in Parquet, so the conversion follows the same (lossy) rounding
     * DuckDB applies when reading these columns back.
     */
    private static class DoubleToInt128Decimal
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            BlockBuilder builder = DuckLakeTypes.INT128_DECIMAL_TYPE.createBlockBuilder(null, block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    builder.appendNull();
                }
                else {
                    double value = DOUBLE.getDouble(block, position);
                    if (!Double.isFinite(value)) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Cannot convert non-finite value to DECIMAL(38, 0): " + value);
                    }
                    Int128 decimal;
                    try {
                        decimal = Int128.valueOf(new BigDecimal(value).setScale(0, RoundingMode.HALF_UP).toBigIntegerExact());
                    }
                    catch (ArithmeticException e) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Value out of range for DECIMAL(38, 0): " + value, e);
                    }
                    if (Decimals.overflows(decimal)) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Value out of range for DECIMAL(38, 0): " + value);
                    }
                    DuckLakeTypes.INT128_DECIMAL_TYPE.writeObject(builder, decimal);
                }
            }
            return builder.build();
        }
    }
}
