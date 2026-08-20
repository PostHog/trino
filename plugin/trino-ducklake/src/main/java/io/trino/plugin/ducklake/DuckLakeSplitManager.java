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
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeleteFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionInfo;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.PartitionTransforms;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
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
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isFileStatisticsPruningEnabled;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;

public class DuckLakeSplitManager
        implements ConnectorSplitManager
{
    private final JdbcDuckLakeMetastore metastore;

    @Inject
    public DuckLakeSplitManager(JdbcDuckLakeMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
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

        TupleDomain<DuckLakeColumnHandle> effectivePredicate = handle.enforcedConstraint()
                .intersect(handle.unenforcedConstraint())
                .intersect(constraint.getSummary().transformKeys(DuckLakeColumnHandle.class::cast));
        if (effectivePredicate.isNone()) {
            return new FixedSplitSource(ImmutableList.of());
        }
        Map<DuckLakeColumnHandle, Domain> domains = effectivePredicate.getDomains().orElseThrow();
        Set<DuckLakeColumnHandle> enforcedColumns = handle.enforcedConstraint().getDomains()
                .map(Map::keySet)
                .orElse(Set.of());

        Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId = deleteFilesByDataFileId(handle);

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

        List<DuckLakeDataFileEntry> dataFiles = metastore.dataFiles(handle.snapshotId(), handle.tableId());
        Map<Long, DuckLakeNameMapping> nameMappings = nameMappings(dataFiles);
        ImmutableList.Builder<DuckLakeSplit> splits = ImmutableList.builderWithExpectedSize(dataFiles.size());
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
                            entry.deleteCount()));
            long recordCount = dataFile.recordCount() - deleteFile.map(DuckLakeDeleteFileHandle::deleteCount).orElse(0L);
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
            splits.add(new DuckLakeSplit(
                    path,
                    dataFile.fileSizeBytes(),
                    dataFile.footerSize(),
                    recordCount,
                    dataFile.rowIdStart(),
                    deleteFile,
                    dataFile.partitionValues(),
                    nameMapping,
                    SplitWeight.standard()));
        }
        return new FixedSplitSource(splits.build());
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

    private Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId(DuckLakeTableHandle handle)
    {
        Map<Long, DuckLakeDeleteFileEntry> deleteFilesByDataFileId = new HashMap<>();
        for (DuckLakeDeleteFileEntry deleteFile : metastore.deleteFiles(handle.snapshotId(), handle.tableId())) {
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
        if (dataFile.partialMax().isPresent()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Data file %s of table %s is a partial file, which is not supported".formatted(dataFile.path(), handle.schemaTableName()));
        }
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
}
