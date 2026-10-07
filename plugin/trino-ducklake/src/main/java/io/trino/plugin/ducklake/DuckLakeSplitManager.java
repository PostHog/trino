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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeleteFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeletions;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedData;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionInfo;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.PartitionTransforms;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_CONCURRENT_MODIFICATION;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getMaxSplitSize;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isFileStatisticsPruningEnabled;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Math.clamp;
import static java.util.Objects.requireNonNull;

public class DuckLakeSplitManager
        implements ConnectorSplitManager
{
    /**
     * Weight floor for the splits of a file much smaller than the target split size, so that a
     * query over many tiny files still schedules a bounded number of splits per node.
     */
    private static final double MINIMUM_ASSIGNED_SPLIT_WEIGHT = 0.05;
    /**
     * Listings taken before a scan gives up on a table whose inlined rows keep being flushed while
     * its splits are listed. A flush takes far longer than a listing, so a second attempt is
     * normally enough.
     */
    private static final int MAX_LISTING_ATTEMPTS = 3;

    private final JdbcDuckLakeMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final SplitAffinityProvider affinityProvider;
    private final ExecutorService executor;

    @Inject
    public DuckLakeSplitManager(
            JdbcDuckLakeMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            SplitAffinityProvider affinityProvider,
            @ForDuckLakeSplitManager ExecutorService executor)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.affinityProvider = requireNonNull(affinityProvider, "affinityProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        if (handle.rowCount().isPresent()) {
            // The count was read from the catalog when the aggregation was pushed down, so the
            // scan has no file to read and produces the single row holding it.
            return new FixedSplitSource(new DuckLakeRowCountSplit(handle.rowCount().orElseThrow()));
        }

        TupleDomain<DuckLakeColumnHandle> effectivePredicate = handle.enforcedConstraint()
                .intersect(handle.unenforcedConstraint())
                .intersect(constraint.getSummary().transformKeys(DuckLakeColumnHandle.class::cast));
        if (effectivePredicate.isNone()) {
            return new FixedSplitSource(ImmutableList.of());
        }
        Map<DuckLakeColumnHandle, Domain> domains = effectivePredicate.getDomains().orElseThrow();

        // DuckDB moves inlined rows into data files by deleting them from the catalog database and
        // registering the files under the snapshots the rows were written in, so a flush landing
        // while the splits are listed can make the listing see the rows in neither place, or the
        // deletions of a flushed row in neither. Such a listing is discarded and taken again.
        for (int attempt = 1; ; attempt++) {
            Listing listing = listSplits(session, handle, domains);
            if (listing.inlinedWatermark().isEmpty() || !metastore.inlinedDataFlushedAfter(handle.tableId(), listing.inlinedWatermark().orElseThrow())) {
                return listing.splitSource();
            }
            listing.splitSource().close();
            if (attempt == MAX_LISTING_ATTEMPTS) {
                throw new TrinoException(DUCKLAKE_CONCURRENT_MODIFICATION, "Inlined data of table %s was flushed to Parquet files while the query was planned, %s times in a row; run the query again".formatted(handle.schemaTableName(), attempt));
            }
        }
    }

    /**
     * The splits of a table, and, when the table may hold inlined rows, the newest snapshot of the
     * catalog when they were looked for.
     */
    private record Listing(ConnectorSplitSource splitSource, OptionalLong inlinedWatermark) {}

    private Listing listSplits(ConnectorSession session, DuckLakeTableHandle handle, Map<DuckLakeColumnHandle, Domain> domains)
    {
        Set<DuckLakeColumnHandle> enforcedColumns = handle.enforcedConstraint().getDomains()
                .map(Map::keySet)
                .orElse(Set.of());

        // the inlined rows are listed first, so that a flush that moves them into data files after
        // this point is one the check in getSplits sees
        DuckLakeInlinedData inlinedData = metastore.inlinedData(handle.snapshotId(), handle.tableId());
        List<ConnectorSplit> inlinedSplits = inlinedData.tables().stream()
                .map(inlinedTable -> (ConnectorSplit) new DuckLakeInlinedSplit(
                        handle.schemaName(),
                        handle.tableName(),
                        handle.tableId(),
                        handle.snapshotId(),
                        inlinedData.watermarkSnapshotId(),
                        inlinedTable.tableName(),
                        inlinedTable.columns()))
                .collect(toImmutableList());
        if (!inlinedSplits.isEmpty() && !enforcedColumns.isEmpty()) {
            // inlined rows carry no partition values, so nothing can have been enforced by pruning
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Table %s holds inlined rows, but a partition predicate was enforced".formatted(handle.schemaTableName()));
        }
        OptionalLong inlinedWatermark = metastore.inlinedDataSupported() ? OptionalLong.of(inlinedData.watermarkSnapshotId()) : OptionalLong.empty();

        DuckLakeDeletions deletions = metastore.deletions(handle.snapshotId(), handle.tableId());
        Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId = deleteFilesByDataFileId(handle, deletions.deleteFiles());

        Optional<DuckLakePartitionInfo> partitionInfo = Optional.empty();
        ListMultimap<Long, DuckLakePartitionColumn> transformsByColumnId = ArrayListMultimap.create();
        if (!domains.isEmpty()) {
            partitionInfo = metastore.partitionInfo(handle.snapshotId(), handle.tableId());
            partitionInfo.ifPresent(info -> info.columns().forEach(column -> transformsByColumnId.put(column.columnId(), column)));
        }

        Map<Long, Map<Long, DuckLakeFileColumnStats>> statsByDataFileId = Map.of();
        if (!domains.isEmpty() && isFileStatisticsPruningEnabled(session)) {
            Set<Long> constrainedColumnIds = domains.keySet().stream()
                    .map(DuckLakeColumnHandle::columnId)
                    .collect(toImmutableSet());
            statsByDataFileId = metastore.fileColumnStats(handle.tableId(), constrainedColumnIds).stream()
                    .collect(Collectors.groupingBy(
                            DuckLakeFileColumnStats::dataFileId,
                            HashMap::new,
                            Collectors.toMap(DuckLakeFileColumnStats::columnId, stats -> stats)));
        }

        long maxSplitSize = getMaxSplitSize(session).toBytes();
        List<DuckLakeDataFileEntry> dataFiles = metastore.dataFiles(handle.snapshotId(), handle.tableId());
        Map<Long, DuckLakeNameMapping> nameMappings = nameMappings(dataFiles);
        boolean metadataOnly = handle.projectedColumns().map(Set::isEmpty).orElse(false)
                && handle.unenforcedConstraint().isAll();
        ImmutableList.Builder<DuckLakeSplit> files = ImmutableList.builder();
        for (DuckLakeDataFileEntry dataFile : dataFiles) {
            validateDataFile(handle, dataFile);
            if (prunedByPartitionValues(handle, dataFile, partitionInfo, transformsByColumnId, domains, enforcedColumns)) {
                continue;
            }
            if (prunedByFileStatistics(dataFile, statsByDataFileId.getOrDefault(dataFile.dataFileId(), Map.of()), domains)) {
                continue;
            }
            Optional<DuckLakeDeleteFileHandle> deleteFile = Optional.ofNullable(deleteFilesByDataFileId.get(dataFile.dataFileId()))
                    .map(entry -> new DuckLakeDeleteFileHandle(
                            PathResolver.resolve(handle.tableLocation(), entry.path(), entry.pathIsRelative()),
                            entry.fileSizeBytes(),
                            entry.footerSize(),
                            entry.deleteCount(),
                            !mayHoldNewerDeletions(handle, dataFile, entry)));
            long recordCount = dataFile.recordCount() - deleteFile.map(DuckLakeDeleteFileHandle::deleteCount).orElse(0L);
            // the rows of newer snapshots are counted too, so the count of such a file is not exact
            OptionalLong rowSnapshotFilter = rowSnapshotFilter(handle, dataFile);
            Optional<DuckLakeInlinedDeletions> inlinedDeletions = Optional.ofNullable(deletions.inlinedDeletions().get(dataFile.dataFileId()))
                    .map(DuckLakeInlinedDeletions::new);
            String path = PathResolver.resolve(handle.tableLocation(), dataFile.path(), dataFile.pathIsRelative());
            Optional<DuckLakeNameMapping> nameMapping = Optional.empty();
            if (dataFile.mappingId().isPresent()) {
                long mappingId = dataFile.mappingId().orElseThrow();
                DuckLakeNameMapping mapping = nameMappings.get(mappingId);
                if (mapping == null) {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Name mapping %s of data file %s of table %s is not present in the catalog".formatted(mappingId, dataFile.path(), handle.schemaTableName()));
                }
                nameMapping = Optional.of(mapping);
            }
            if (metadataOnly) {
                // File pruning has enforced the remaining predicate. Keep exact catalog counts
                // and avoid fetching any footer for a scan that needs no stored column. A file with
                // rows deleted inline, or with rows of snapshots newer than the one read, has no
                // exact count, and its split reads which rows are left.
                files.add(new DuckLakeSplit(
                        dataFile.dataFileId(),
                        path,
                        0,
                        dataFile.fileSizeBytes(),
                        dataFile.fileSizeBytes(),
                        dataFile.footerSize(),
                        recordCount,
                        dataFile.rowIdStart(),
                        deleteFile,
                        dataFile.partitionValues(),
                        nameMapping,
                        SplitWeight.standard(),
                        Optional.empty(),
                        Optional.empty(),
                        inlinedDeletions,
                        rowSnapshotFilter));
                continue;
            }
            files.add(new DuckLakeSplit(
                    dataFile.dataFileId(),
                    path,
                    0,
                    dataFile.fileSizeBytes(),
                    dataFile.fileSizeBytes(),
                    dataFile.footerSize(),
                    recordCount,
                    dataFile.rowIdStart(),
                    deleteFile,
                    dataFile.partitionValues(),
                    nameMapping,
                    SplitWeight.standard(),
                    Optional.empty(),
                    Optional.empty(),
                    inlinedDeletions,
                    rowSnapshotFilter));
        }
        List<DuckLakeSplit> retainedFiles = files.build();
        if (metadataOnly || retainedFiles.isEmpty()) {
            return new Listing(
                    new FixedSplitSource(ImmutableList.<ConnectorSplit>builder().addAll(inlinedSplits).addAll(retainedFiles).build()),
                    inlinedWatermark);
        }
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        return new Listing(
                new DuckLakeSplitSource(retainedFiles, inlinedSplits, fileSystem, parquetReaderOptions, fileFormatDataSourceStats, maxSplitSize, affinityProvider, executor),
                inlinedWatermark);
    }

    static SplitWeight splitWeight(long length, long maxSplitSize)
    {
        return SplitWeight.fromProportion(clamp((double) length / maxSplitSize, MINIMUM_ASSIGNED_SPLIT_WEIGHT, 1.0));
    }

    /**
     * Loads the name mappings referenced by the data files of the table with a single query.
     */
    private Map<Long, DuckLakeNameMapping> nameMappings(List<DuckLakeDataFileEntry> dataFiles)
    {
        Set<Long> mappingIds = dataFiles.stream()
                .map(DuckLakeDataFileEntry::mappingId)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .collect(toImmutableSet());
        return metastore.nameMappings(mappingIds);
    }

    private static Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId(DuckLakeTableHandle handle, List<DuckLakeDeleteFileEntry> deleteFiles)
    {
        Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId = new HashMap<>();
        for (DuckLakeDeleteFileEntry deleteFile : deleteFiles) {
            validateDeleteFile(handle, deleteFile);
            if (deleteFilesByDataFileId.putIfAbsent(deleteFile.dataFileId(), deleteFile) != null) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Multiple delete files are visible for data file %s of table %s".formatted(deleteFile.dataFileId(), handle.schemaTableName()));
            }
        }
        return deleteFilesByDataFileId;
    }

    /**
     * Prunes a data file when its partition values cannot match the predicate. Partition values
     * are only interpreted when the file was written with the partitioning scheme visible at the
     * snapshot; a file that cannot be checked is kept, unless the predicate on the column was
     * enforced in {@link DuckLakeMetadata#applyFilter}, which guarantees such files do not exist.
     */
    private static boolean prunedByPartitionValues(
            DuckLakeTableHandle handle,
            DuckLakeDataFileEntry dataFile,
            Optional<DuckLakePartitionInfo> partitionInfo,
            ListMultimap<Long, DuckLakePartitionColumn> transformsByColumnId,
            Map<DuckLakeColumnHandle, Domain> domains,
            Set<DuckLakeColumnHandle> enforcedColumns)
    {
        if (partitionInfo.isEmpty() || dataFile.partitionId().isEmpty() || dataFile.partitionId().orElseThrow() != partitionInfo.get().partitionId()) {
            if (!enforcedColumns.isEmpty()) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Data file %s of table %s was not written with the current partitioning scheme, but the partition predicate was enforced".formatted(dataFile.path(), handle.schemaTableName()));
            }
            return false;
        }
        for (Map.Entry<DuckLakeColumnHandle, Domain> entry : domains.entrySet()) {
            DuckLakeColumnHandle column = entry.getKey();
            List<DuckLakePartitionColumn> transforms = transformsByColumnId.get(column.columnId());
            if (transforms.isEmpty()) {
                continue;
            }
            Optional<Domain> partitionDomain = PartitionTransforms.partitionDomain(column, transforms, dataFile.partitionValues());
            if (partitionDomain.isEmpty()) {
                if (enforcedColumns.contains(column)) {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Cannot interpret partition value of column '%s' for data file %s of table %s".formatted(column.name(), dataFile.path(), handle.schemaTableName()));
                }
                continue;
            }
            if (!entry.getValue().overlaps(partitionDomain.get())) {
                return true;
            }
        }
        return false;
    }

    private static boolean prunedByFileStatistics(
            DuckLakeDataFileEntry dataFile,
            Map<Long, DuckLakeFileColumnStats> statsByColumnId,
            Map<DuckLakeColumnHandle, Domain> domains)
    {
        for (Map.Entry<DuckLakeColumnHandle, Domain> entry : domains.entrySet()) {
            DuckLakeColumnHandle column = entry.getKey();
            if (column.isInt128()) {
                // int128 values are stored lossily as doubles in Parquet, so the exact catalog
                // statistics may disagree with the values the engine compares against
                continue;
            }
            DuckLakeFileColumnStats stats = statsByColumnId.get(column.columnId());
            if (stats == null) {
                continue;
            }
            Optional<Domain> statisticsDomain = statisticsDomain(column.type(), stats, dataFile.recordCount());
            if (statisticsDomain.isPresent() && !entry.getValue().overlaps(statisticsDomain.get())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the domain implied by the file column statistics, or {@link Optional#empty()} when
     * the statistics do not constrain the column reliably (so the file must be kept).
     */
    private static Optional<Domain> statisticsDomain(Type type, DuckLakeFileColumnStats stats, long recordCount)
    {
        try {
            return buildStatisticsDomain(type, stats, recordCount);
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Optional<Domain> buildStatisticsDomain(Type type, DuckLakeFileColumnStats stats, long recordCount)
    {
        if ((type.equals(REAL) || type.equals(DOUBLE)) && !stats.containsNan().equals(Optional.of(false))) {
            // min_value and max_value do not cover NaN values
            return Optional.empty();
        }
        if (stats.nullCount().isPresent() && stats.nullCount().orElseThrow() == recordCount) {
            return Optional.of(Domain.onlyNull(type));
        }
        boolean nullAllowed = stats.nullCount().isEmpty() || stats.nullCount().orElseThrow() > 0;
        Optional<Object> min = stats.minValue().flatMap(value -> StatsValueParser.parse(type, value));
        Optional<Object> max = stats.maxValue().flatMap(value -> StatsValueParser.parse(type, value));
        if (min.isPresent() && max.isPresent()) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, min.get(), true, max.get(), true)), nullAllowed));
        }
        if (min.isPresent()) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, min.get())), nullAllowed));
        }
        if (max.isPresent()) {
            return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, max.get())), nullAllowed));
        }
        if (!nullAllowed) {
            return Optional.of(Domain.notNull(type));
        }
        return Optional.empty();
    }

    private static void validateDataFile(DuckLakeTableHandle handle, DuckLakeDataFileEntry dataFile)
    {
        if (!dataFile.fileFormat().toLowerCase(Locale.ENGLISH).equals("parquet")) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Data file %s of table %s has unsupported format '%s'. Only Parquet is supported".formatted(dataFile.path(), handle.schemaTableName(), dataFile.fileFormat()));
        }
        if (dataFile.encryptionKey().isPresent()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Data file %s of table %s is encrypted, which is not supported".formatted(dataFile.path(), handle.schemaTableName()));
        }
    }

    /**
     * The newest snapshot whose rows of the data file are visible, present when the file also
     * holds rows of newer snapshots, which the page source then leaves out.
     * <p>
     * DuckDB writes a file holding the rows of several snapshots when it merges adjacent files or
     * flushes inlined rows. Each row carries the snapshot it was inserted in, in the
     * {@code _ducklake_internal_snapshot_id} column of the file, and the file is registered from
     * the oldest of those snapshots on, with the newest recorded as its partial_max, replacing the
     * files or inlined rows it was written from. A reader of a snapshot between the two sees the
     * file, and DuckDB reads only its rows of that snapshot and older ones
     * ({@code SetSnapshotFilter}, called by {@code DuckLakeMetadataManager::GetFilesForTable},
     * and the filter {@code DuckLakeMultiFileReader::InitializeReader} adds on the column). When
     * partial_max is at or below the snapshot read, every row of the file is visible and nothing
     * is filtered. DuckDB also bounds such a file from below, but only when it lists the rows
     * inserted between two snapshots, which this connector does not do.
     * <p>
     * A row-level change reads such a file the same way, and identifies the rows it changes by
     * their position in the file, which leaving out rows does not move. It cannot commit what it
     * read: the rows it left out were inserted into the table after the snapshot it read, which
     * makes its commit conflict, so it never writes deletions computed without them.
     */
    private static OptionalLong rowSnapshotFilter(DuckLakeTableHandle handle, DuckLakeDataFileEntry dataFile)
    {
        if (dataFile.partialMax().isPresent() && dataFile.partialMax().orElseThrow() > handle.snapshotId()) {
            return OptionalLong.of(handle.snapshotId());
        }
        return OptionalLong.empty();
    }

    private static void validateDeleteFile(DuckLakeTableHandle handle, DuckLakeDeleteFileEntry deleteFile)
    {
        if (!deleteFile.format().toLowerCase(Locale.ENGLISH).equals("parquet")) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Delete file %s of table %s has unsupported format '%s'. Only Parquet is supported".formatted(deleteFile.path(), handle.schemaTableName(), deleteFile.format()));
        }
        if (deleteFile.encryptionKey().isPresent()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Delete file %s of table %s is encrypted, which is not supported".formatted(deleteFile.path(), handle.schemaTableName()));
        }
    }

    /**
     * Whether a delete file may hold deletions of snapshots newer than the one read. DuckDB tags
     * each deletion with the snapshot that made it when it writes a delete file over an existing
     * one or flushes inlined deletions or inlined rows, and registers that file from the oldest of
     * those snapshots on, replacing the file it merged. The page source applies only the deletions
     * of the snapshot read and older ones, like DuckDB, so the catalog's delete count of such a
     * file overstates the rows it removes. DuckDB records the newest snapshot of the file as its
     * partial_max, except for the delete file it writes when it flushes inlined rows, which belongs
     * to a data file of that flush, the only kind of data file with a partial_max.
     */
    private static boolean mayHoldNewerDeletions(DuckLakeTableHandle handle, DuckLakeDataFileEntry dataFile, DuckLakeDeleteFileEntry deleteFile)
    {
        if (deleteFile.partialMax().isPresent()) {
            return deleteFile.partialMax().orElseThrow() > handle.snapshotId();
        }
        return dataFile.partialMax().isPresent();
    }
}
