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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeleteFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionInfo;
import io.trino.plugin.ducklake.metastore.DuckLakeSchemaEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeTableColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeTableColumnsEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeTableEntry;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Multimaps.index;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FORMAT_VERSION;
import static io.trino.plugin.ducklake.util.PartitionTransforms.IDENTITY_TRANSFORM;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DuckLakeMetadata
        implements ConnectorMetadata
{
    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;

    @GuardedBy("this")
    private Long snapshotId;

    public DuckLakeMetadata(JdbcDuckLakeMetastore metastore, String dataPath)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
    }

    /**
     * The snapshot all metadata operations of this transaction are pinned to. On first access
     * this verifies the DuckLake format version and reads the latest snapshot from the catalog.
     */
    public synchronized long snapshotId()
    {
        if (snapshotId == null) {
            String version = metastore.formatVersion()
                    .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "DuckLake catalog has no 'version' entry in ducklake_metadata"));
            verifyFormatVersion(version);
            snapshotId = metastore.currentSnapshotId();
        }
        return snapshotId;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Set<String> schemaNames = new LinkedHashSet<>();
        for (DuckLakeSchemaEntry schema : metastore.listSchemas(snapshotId())) {
            String schemaName = schema.schemaName().toLowerCase(ENGLISH);
            if (!schemaNames.add(schemaName)) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Ambiguous schema name '%s': multiple schemas differ only in case".formatted(schemaName));
            }
        }
        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    @Nullable
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        long snapshot = snapshotId();
        Optional<DuckLakeTableEntry> table = metastore.findTable(snapshot, tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }
        DuckLakeTableEntry tableEntry = table.get();
        if (metastore.hasInlinedData(snapshot, tableEntry.tableId())) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Table %s has inlined data, which is not supported. Flush inlined data to Parquet with DuckDB first".formatted(tableName));
        }
        return new DuckLakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableEntry.tableId(),
                snapshot,
                PathResolver.tableLocation(dataPath, tableEntry),
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        List<ColumnMetadata> columns = columnHandles(handle).stream()
                .map(DuckLakeColumnHandle::columnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Set<SchemaTableName> tableNames = new LinkedHashSet<>();
        for (DuckLakeTableEntry table : metastore.listTables(snapshotId(), schemaName)) {
            // SchemaTableName lowercases the names, so the listed names are queryable
            SchemaTableName tableName = new SchemaTableName(table.schemaName(), table.tableName());
            if (!tableNames.add(tableName)) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Ambiguous table name '%s': multiple tables differ only in case".formatted(tableName));
            }
        }
        return ImmutableList.copyOf(tableNames);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new LinkedHashMap<>();
        for (DuckLakeTableColumnsEntry table : metastore.columnsForAllTables(snapshotId(), schemaName)) {
            SchemaTableName tableName = new SchemaTableName(table.schemaName(), table.tableName());
            List<ColumnMetadata> columns = toColumnHandles(table.columns()).stream()
                    .map(DuckLakeColumnHandle::columnMetadata)
                    .collect(toImmutableList());
            if (relationColumns.putIfAbsent(tableName, RelationColumnsMetadata.forTable(tableName, columns)) != null) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Ambiguous table name '%s': multiple tables differ only in case".formatted(tableName));
            }
        }
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DuckLakeColumnHandle column : columnHandles(handle)) {
            columnHandles.put(column.name(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DuckLakeColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        TupleDomain<DuckLakeColumnHandle> predicate = constraint.getSummary().transformKeys(DuckLakeColumnHandle.class::cast);
        if (predicate.isAll()) {
            return Optional.empty();
        }

        TupleDomain<DuckLakeColumnHandle> newEnforcedConstraint;
        TupleDomain<DuckLakeColumnHandle> newUnenforcedConstraint;
        if (predicate.isNone()) {
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
        }
        else {
            Set<Long> enforceableColumnIds = enforceablePartitionColumnIds(handle);
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> enforceableDomains = ImmutableMap.builder();
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> unenforceableDomains = ImmutableMap.builder();
            for (Map.Entry<DuckLakeColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
                DuckLakeColumnHandle column = entry.getKey();
                if (enforceableColumnIds.contains(column.columnId()) && isEnforceableType(column.type())) {
                    enforceableDomains.put(column, entry.getValue());
                }
                else {
                    unenforceableDomains.put(column, entry.getValue());
                }
            }
            newEnforcedConstraint = TupleDomain.withColumnDomains(enforceableDomains.buildOrThrow());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(unenforceableDomains.buildOrThrow());
        }

        DuckLakeTableHandle newHandle = new DuckLakeTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.tableId(),
                handle.snapshotId(),
                handle.tableLocation(),
                // Do not simplify the enforced constraint, the connector guarantees it is applied as is.
                // The unenforced constraint is still checked by the engine.
                handle.enforcedConstraint().intersect(newEnforcedConstraint),
                handle.unenforcedConstraint().intersect(newUnenforcedConstraint));

        if (handle.equals(newHandle)) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                newUnenforcedConstraint.transformKeys(ColumnHandle.class::cast),
                constraint.getExpression(),
                false));
    }

    /**
     * Returns the ids of partition columns whose predicates the connector can fully enforce by
     * pruning files: identity-transformed columns where every visible data file was written with
     * the current partitioning scheme, so every row of a kept file carries the partition value.
     */
    private Set<Long> enforceablePartitionColumnIds(DuckLakeTableHandle handle)
    {
        Optional<DuckLakePartitionInfo> partitionInfo = metastore.partitionInfo(handle.snapshotId(), handle.tableId());
        if (partitionInfo.isEmpty()) {
            return ImmutableSet.of();
        }
        if (!metastore.allDataFilesUsePartition(handle.snapshotId(), handle.tableId(), partitionInfo.get().partitionId())) {
            return ImmutableSet.of();
        }
        return partitionInfo.get().columns().stream()
                .filter(column -> column.transform().equalsIgnoreCase(IDENTITY_TRANSFORM))
                .map(DuckLakePartitionColumn::columnId)
                .collect(toImmutableSet());
    }

    /**
     * Types whose identity partition values round-trip exactly through the string
     * representation in {@code ducklake_file_partition_value}, so pruning on them can be
     * used to enforce a predicate. {@code DATE} is excluded because DuckDB writes values
     * such as {@code infinity}, {@code -infinity}, BC dates and years with five or more
     * digits that cannot be parsed back reliably; predicates on it stay unenforced and
     * pruning remains fail-open.
     */
    private static boolean isEnforceableType(Type type)
    {
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(BOOLEAN)
                || type instanceof VarcharType varcharType && varcharType.isUnbounded();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        long rowCount = metastore.dataFiles(handle.snapshotId(), handle.tableId()).stream()
                .mapToLong(DuckLakeDataFileEntry::recordCount)
                .sum();
        rowCount -= metastore.deleteFiles(handle.snapshotId(), handle.tableId()).stream()
                .mapToLong(DuckLakeDeleteFileEntry::deleteCount)
                .sum();
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount));

        Map<Long, DuckLakeTableColumnStats> columnStats = metastore.tableColumnStatistics(handle.tableId()).stream()
                .collect(toImmutableMap(DuckLakeTableColumnStats::columnId, stats -> stats));
        for (DuckLakeColumnHandle column : columnHandles(handle)) {
            DuckLakeTableColumnStats stats = columnStats.get(column.columnId());
            if (stats == null) {
                continue;
            }
            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            if (stats.containsNull().equals(Optional.of(false))) {
                columnStatistics.setNullsFraction(Estimate.zero());
            }
            columnRange(column.type(), stats).ifPresent(columnStatistics::setRange);
            tableStatistics.setColumnStatistics(column, columnStatistics.build());
        }
        return tableStatistics.build();
    }

    private static Optional<DoubleRange> columnRange(Type type, DuckLakeTableColumnStats stats)
    {
        if ((type.equals(REAL) || type.equals(DOUBLE)) && !stats.containsNan().equals(Optional.of(false))) {
            // min_value and max_value do not cover NaN values
            return Optional.empty();
        }
        Optional<Object> min = stats.minValue().flatMap(value -> StatsValueParser.parse(type, value));
        Optional<Object> max = stats.maxValue().flatMap(value -> StatsValueParser.parse(type, value));
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }
        try {
            return DoubleRange.from(type, min.get(), max.get());
        }
        catch (RuntimeException _) {
            // invalid catalog statistics (e.g. min greater than max) must not fail planning
            return Optional.empty();
        }
    }

    private List<DuckLakeColumnHandle> columnHandles(DuckLakeTableHandle handle)
    {
        return toColumnHandles(metastore.columns(handle.snapshotId(), handle.tableId()));
    }

    private static List<DuckLakeColumnHandle> toColumnHandles(List<DuckLakeColumnEntry> columns)
    {
        var childrenByParent = index(
                columns.stream()
                        .filter(column -> column.parentColumn().isPresent())
                        .collect(toImmutableList()),
                column -> column.parentColumn().orElseThrow());
        ImmutableList.Builder<DuckLakeColumnHandle> handles = ImmutableList.builder();
        for (DuckLakeColumnEntry column : columns) {
            if (column.parentColumn().isPresent()) {
                continue;
            }
            handles.add(new DuckLakeColumnHandle(
                    column.columnId(),
                    column.columnName(),
                    column.columnType(),
                    DuckLakeTypes.toTrinoType(column, childrenByParent),
                    column.nullsAllowed(),
                    column.initialDefault()));
        }
        return handles.build();
    }

    private static void verifyFormatVersion(String version)
    {
        String major = version;
        int dot = version.indexOf('.');
        if (dot >= 0) {
            major = version.substring(0, dot);
        }
        if (!major.equals("0") && !major.equals("1")) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FORMAT_VERSION, "Unsupported DuckLake format version: " + version);
        }
    }
}
