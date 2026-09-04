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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnRow;
import io.trino.plugin.ducklake.metastore.DuckLakeCommit;
import io.trino.plugin.ducklake.metastore.DuckLakeCommitAction;
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeleteFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStatsRow;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionInfo;
import io.trino.plugin.ducklake.metastore.DuckLakeRowCount;
import io.trino.plugin.ducklake.metastore.DuckLakeSchemaEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeTableColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeTableColumnsEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeTableEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeViewEntry;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeColumns;
import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.ducklake.util.PartitionTransforms;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.index;
import static io.trino.plugin.ducklake.DuckLakeColumnHandle.ROW_COUNT_COLUMN;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FORMAT_VERSION;
import static io.trino.plugin.ducklake.util.PartitionTransforms.IDENTITY_TRANSFORM;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DuckLakeMetadata
        implements ConnectorMetadata
{
    private static final String COMMENT_TAG_KEY = "comment";
    private static final String VIEW_DEFINITION_TAG_KEY = "trino_view_definition";
    private static final String TRINO_VIEW_DIALECT = "trino";

    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;
    private final Map<Long, DuckLakeRowCount> rowCounts = new ConcurrentHashMap<>();
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec;
    private final JsonCodec<ConnectorViewDefinition> viewDefinitionCodec;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DuckLakeWriterFactory writerFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;

    @GuardedBy("this")
    private Long snapshotId;

    public DuckLakeMetadata(
            JdbcDuckLakeMetastore metastore,
            String dataPath,
            JsonCodec<DuckLakeDataFile> dataFileCodec,
            JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec,
            JsonCodec<ConnectorViewDefinition> viewDefinitionCodec,
            TrinoFileSystemFactory fileSystemFactory,
            DuckLakeWriterFactory writerFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.mergeFragmentCodec = requireNonNull(mergeFragmentCodec, "mergeFragmentCodec is null");
        this.viewDefinitionCodec = requireNonNull(viewDefinitionCodec, "viewDefinitionCodec is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
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

    /**
     * Runs a statement's catalog changes as one DuckLake snapshot and moves this transaction onto
     * it, so that everything read afterwards sees the change.
     */
    private <T> T commit(DuckLakeCommitAction<T> action)
    {
        // pin the transaction to a snapshot first, which also verifies the catalog format version
        snapshotId();

        class Result
        {
            T value;
            long snapshot;
        }

        Result result = metastore.commit(commit -> {
            Result committed = new Result();
            committed.value = action.run(commit);
            committed.snapshot = commit.effectiveSnapshotId();
            return committed;
        });
        synchronized (this) {
            snapshotId = result.snapshot;
        }
        return result.value;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        commit(commit -> {
            if (commit.findSchema(schemaName).isPresent()) {
                throw new TrinoException(SCHEMA_ALREADY_EXISTS, "Schema already exists: " + schemaName);
            }
            commit.insertSchema(schemaName, directoryName(schemaName));
            commit.recordCreatedSchema(schemaName);
            return null;
        });
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        commit(commit -> {
            DuckLakeCommit.SchemaIdentity schema = commit.findSchema(schemaName)
                    .orElseThrow(() -> new SchemaNotFoundException(schemaName));
            if (commit.schemaHasRelations(schema.schemaId())) {
                if (!cascade) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(schemaName));
                }
                throw new TrinoException(NOT_SUPPORTED, "Dropping a schema with tables is not supported; drop the tables first");
            }
            commit.endSchema(schema.schemaId());
            commit.recordDroppedSchema(schema.schemaId());
            return null;
        });
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        commit(commit -> {
            DuckLakeCommit.SchemaIdentity schema = commit.findSchema(source)
                    .orElseThrow(() -> new SchemaNotFoundException(source));
            if (commit.findSchema(target).isPresent()) {
                throw new TrinoException(SCHEMA_ALREADY_EXISTS, "Schema already exists: " + target);
            }
            commit.endSchema(schema.schemaId());
            // the location keeps the original name, matching how DuckDB renames a schema
            commit.insertSchemaRow(schema.schemaId(), target, schema.path());
            commit.recordCreatedSchema(target);
            return null;
        });
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
                TupleDomain.all(),
                OptionalLong.empty());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        Map<Long, String> columnComments = metastore.columnComments(handle.snapshotId(), handle.tableId());
        List<ColumnMetadata> columns = columnHandles(handle).stream()
                .map(column -> ColumnMetadata.builderFrom(column.columnMetadata())
                        .setComment(Optional.ofNullable(columnComments.get(column.columnId())))
                        .build())
                .collect(toImmutableList());
        return new ConnectorTableMetadata(
                handle.schemaTableName(),
                columns,
                tableProperties(handle),
                metastore.tableComment(handle.snapshotId(), handle.tableId()));
    }

    /**
     * The properties that describe how the table is laid out, so that a table created here can be
     * recreated from the definition the engine prints.
     */
    private Map<String, Object> tableProperties(DuckLakeTableHandle handle)
    {
        Optional<DuckLakePartitioning> partitioning = partitioningOf(handle);
        if (partitioning.isEmpty()) {
            return ImmutableMap.of();
        }
        return ImmutableMap.of(
                DuckLakeTableProperties.PARTITIONING_PROPERTY, partitioning.get().fields().stream()
                        .map(field -> DuckLakeTableProperties.formatPartitionKey(field.columnName(), field.transform()))
                        .collect(toImmutableList()));
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
        // views share the namespace with tables and are listed alongside them
        for (DuckLakeViewEntry view : metastore.listViews(snapshotId(), schemaName)) {
            tableNames.add(new SchemaTableName(view.schemaName(), view.viewName()));
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();
        commit(commit -> {
            NewTable table = createTable(commit, tableName, tableMetadata.getColumns(), DuckLakeTableProperties.getPartitioning(tableMetadata.getProperties()), saveMode);
            if (table != null) {
                tableMetadata.getComment().ifPresent(comment -> commit.setTableTag(table.tableId(), COMMENT_TAG_KEY, Optional.of(comment)));
            }
            return null;
        });
    }

    /**
     * Creates the catalog rows of a new table and returns its identifier and location. Shared by
     * {@code CREATE TABLE} and {@code CREATE TABLE AS}, which then writes data into it.
     */
    private NewTable createTable(DuckLakeCommit commit, SchemaTableName tableName, List<ColumnMetadata> columns, List<String> partitionKeys, SaveMode saveMode)
    {
        DuckLakeCommit.SchemaIdentity schema = commit.findSchema(tableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(tableName.getSchemaName()));
        if (commit.findView(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            throw new TrinoException(ALREADY_EXISTS, "View already exists: " + tableName);
        }
        Optional<DuckLakeCommit.TableIdentity> existing = commit.findTable(tableName.getSchemaName(), tableName.getTableName());
        if (existing.isPresent()) {
            switch (saveMode) {
                case FAIL -> throw new TrinoException(TABLE_ALREADY_EXISTS, "Table already exists: " + tableName);
                case IGNORE -> {
                    return null;
                }
                case REPLACE -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
            }
        }

        long tableId = commit.allocateCatalogId();
        String tablePath = directoryName(tableName.getTableName());
        commit.insertTableRow(tableId, schema.schemaId(), tableName.getTableName(), tablePath, Optional.empty());

        List<DuckLakeWriteColumn> writeColumns = DuckLakeColumns.assignColumnIds(columns);
        for (DuckLakeColumnRow row : DuckLakeColumns.toColumnRows(writeColumns)) {
            commit.insertColumn(tableId, row);
        }
        for (int i = 0; i < columns.size(); i++) {
            Optional<String> comment = columns.get(i).getComment();
            if (comment.isPresent()) {
                commit.setColumnTag(tableId, writeColumns.get(i).columnId(), COMMENT_TAG_KEY, comment);
            }
        }
        Optional<DuckLakePartitioning> partitioning = createPartitioning(commit, tableId, writeColumns, partitionKeys);
        commit.recordCreatedTable(tableName.getSchemaName(), tableName.getTableName(), tableId);

        String schemaLocation = PathResolver.resolve(dataPath, schema.path(), true);
        return new NewTable(tableId, writeColumns, PathResolver.resolve(schemaLocation, tablePath, true), partitioning);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            // the data files stay in place, still visible to readers of the snapshots they belonged to
            commit.endTable(handle.tableId());
            commit.endTableContents(handle.tableId());
            commit.deleteTableStats(handle.tableId());
            commit.recordDroppedTable(handle.tableId());
            return null;
        });
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            DuckLakeCommit.TableIdentity table = commit.findTable(handle.schemaName(), handle.tableName())
                    .orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
            DuckLakeCommit.SchemaIdentity targetSchema = commit.findSchema(newTableName.getSchemaName())
                    .orElseThrow(() -> new SchemaNotFoundException(newTableName.getSchemaName()));
            if (commit.findTable(newTableName.getSchemaName(), newTableName.getTableName()).isPresent()) {
                throw new TrinoException(TABLE_ALREADY_EXISTS, "Table already exists: " + newTableName);
            }
            commit.endTable(table.tableId());
            // the location keeps the original name, so already written data files stay reachable
            commit.insertTableRow(table.tableId(), targetSchema.schemaId(), newTableName.getTableName(), table.path(), table.tableUuid());
            commit.recordCreatedTable(newTableName.getSchemaName(), newTableName.getTableName(), table.tableId());
            return null;
        });
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            commit.setTableTag(handle.tableId(), COMMENT_TAG_KEY, comment);
            commit.recordTableMetadataChange(handle.tableId());
            return null;
        });
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeColumnHandle columnHandle = (DuckLakeColumnHandle) column;
        commit(commit -> {
            commit.setColumnTag(handle.tableId(), columnHandle.columnId(), COMMENT_TAG_KEY, comment);
            commit.recordTableMetadataChange(handle.tableId());
            return null;
        });
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        if (!(position instanceof ColumnPosition.Last)) {
            // a column's position is its identifier, which orders it after every column the table
            // has ever had; placing it elsewhere would renumber the ones that follow
            throw new TrinoException(NOT_SUPPORTED, "This connector only supports adding columns at the end of a table");
        }
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            List<DuckLakeColumnRow> existing = commit.columns(handle.tableId());
            existing.stream()
                    .filter(row -> row.parentColumn().isEmpty() && row.columnName().equalsIgnoreCase(column.getName()))
                    .findAny()
                    .ifPresent(_ -> {
                        throw new TrinoException(COLUMN_ALREADY_EXISTS, "Column already exists: " + column.getName());
                    });
            // identifiers are never reused, because data files written while a dropped column
            // existed still refer to it
            DuckLakeWriteColumn added = DuckLakeColumns.assignColumnIds(column, commit.maxColumnId(handle.tableId()));
            for (DuckLakeColumnRow row : DuckLakeColumns.toColumnRows(added)) {
                commit.insertColumn(handle.tableId(), row);
            }
            column.getComment().ifPresent(comment -> commit.setColumnTag(handle.tableId(), added.columnId(), COMMENT_TAG_KEY, Optional.of(comment)));
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
        commit(commit -> {
            List<DuckLakeColumnRow> columns = commit.columns(handle.tableId());
            if (columns.stream().filter(row -> row.parentColumn().isEmpty()).count() <= 1) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot drop the only column of a table");
            }
            endColumnTree(commit, handle.tableId(), columns, column.columnId());
            commit.setColumnTag(handle.tableId(), column.columnId(), COMMENT_TAG_KEY, Optional.empty());
            commit.deleteTableColumnStats(handle.tableId(), column.columnId());
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String target)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
        commit(commit -> {
            // Data files keep the name the column had when they were written, and are not
            // rewritten. They are read by the column identifier they also carry, which a rename
            // does not change, so the rows written so far keep reading correctly under the new
            // name.
            DuckLakeColumnRow current = currentColumn(commit, handle, column.columnId());
            commit.endColumn(handle.tableId(), column.columnId());
            commit.insertColumn(handle.tableId(), current.withName(target));
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, Type type)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
        if (!isWideningTypeChange(column.type(), type)) {
            // data files already written keep the old physical type, so the new type has to be one
            // every value of the old one reads back as
            throw new TrinoException(NOT_SUPPORTED, "Cannot change the type of column '%s' from %s to %s".formatted(column.name(), column.type(), type));
        }
        replaceColumn(handle, column.columnId(), row -> row.withType(DuckLakeTypes.toDuckLakeType(type)));
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
        replaceColumn(handle, column.columnId(), row -> row.withNullsAllowed(true));
    }

    /**
     * Replaces a column's definition, keeping its identifier. The row describing it until now is
     * ended and a new one takes over at this snapshot, so a reader at an earlier snapshot still
     * sees the column as it was.
     */
    private void replaceColumn(DuckLakeTableHandle handle, long columnId, UnaryOperator<DuckLakeColumnRow> update)
    {
        commit(commit -> {
            DuckLakeColumnRow current = currentColumn(commit, handle, columnId);
            commit.endColumn(handle.tableId(), columnId);
            commit.insertColumn(handle.tableId(), update.apply(current));
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    private static DuckLakeColumnRow currentColumn(DuckLakeCommit commit, DuckLakeTableHandle handle, long columnId)
    {
        return commit.columns(handle.tableId()).stream()
                .filter(row -> row.columnId() == columnId)
                .findFirst()
                .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "Column %s of table %s is not visible".formatted(columnId, handle.schemaTableName())));
    }

    /**
     * Ends a column and every field nested inside it.
     */
    private static void endColumnTree(DuckLakeCommit commit, long tableId, List<DuckLakeColumnRow> columns, long columnId)
    {
        commit.endColumn(tableId, columnId);
        for (DuckLakeColumnRow row : columns) {
            if (row.parentColumn().isPresent() && row.parentColumn().orElseThrow() == columnId) {
                endColumnTree(commit, tableId, columns, row.columnId());
            }
        }
    }

    /**
     * Whether every value stored under the old type reads back correctly as the new one. DuckLake
     * does not rewrite data files when a column's type changes, so only widenings are allowed.
     */
    private static boolean isWideningTypeChange(Type oldType, Type newType)
    {
        if (oldType.equals(newType)) {
            return true;
        }
        if (oldType.equals(INTEGER) && newType.equals(BIGINT)) {
            return true;
        }
        if (oldType.equals(SMALLINT) && (newType.equals(INTEGER) || newType.equals(BIGINT))) {
            return true;
        }
        if (oldType.equals(TINYINT) && (newType.equals(SMALLINT) || newType.equals(INTEGER) || newType.equals(BIGINT))) {
            return true;
        }
        if (oldType.equals(REAL) && newType.equals(DOUBLE)) {
            return true;
        }
        return false;
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        return writePartitioningOf(handle).flatMap(DuckLakeMetadata::preferredWriteLayout);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<String> identityColumns = DuckLakeTableProperties.getPartitioning(tableMetadata.getProperties()).stream()
                .map(DuckLakeTableProperties::parsePartitionKey)
                .map(DuckLakeTableProperties.PartitionKey::asColumnName)
                .flatMap(Optional::stream)
                .collect(toImmutableList());
        return toWriteLayout(identityColumns);
    }

    /**
     * Asks the engine to send the rows of a partition to the same writers.
     * <p>
     * Without it every writer sees rows of every partition and has to keep a file open for each,
     * which both fragments the table into small files and bounds how many partitions a single
     * statement can write. Only the keys that file rows by the column value itself are used: a
     * transformed key would have the engine group by the underlying column, which spreads the rows
     * of one partition rather than gathering them.
     */
    private static Optional<ConnectorTableLayout> preferredWriteLayout(DuckLakePartitioning partitioning)
    {
        return toWriteLayout(partitioning.fields().stream()
                .filter(field -> field.transform().equalsIgnoreCase(DuckLakeWritePartitioner.IDENTITY_TRANSFORM))
                .map(DuckLakePartitioning.Field::columnName)
                .collect(toImmutableList()));
    }

    private static Optional<ConnectorTableLayout> toWriteLayout(List<String> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorTableLayout(partitionColumns));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName tableName = tableMetadata.getTable();
        // the table is created in its own snapshot, so that the rows written afterwards land in a
        // table that already exists, exactly as an insert into an existing table would
        return commit(commit -> {
            NewTable table = createTable(commit, tableName, tableMetadata.getColumns(), DuckLakeTableProperties.getPartitioning(tableMetadata.getProperties()), SaveMode.FAIL);
            tableMetadata.getComment().ifPresent(comment -> commit.setTableTag(table.tableId(), COMMENT_TAG_KEY, Optional.of(comment)));
            return new DuckLakeWriteTarget(tableName, table.tableId(), table.location(), table.columns(), table.partitioning());
        });
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return finishWrite((DuckLakeWriteTarget) tableHandle, fragments);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        List<DuckLakeWriteColumn> writeColumns = DuckLakeColumns.fromCatalog(metastore.columns(handle.snapshotId(), handle.tableId()));
        Set<Long> insertedColumnIds = columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .map(DuckLakeColumnHandle::columnId)
                .collect(toImmutableSet());
        List<DuckLakeWriteColumn> missingColumns = writeColumns.stream()
                .filter(column -> !insertedColumnIds.contains(column.columnId()))
                .collect(toImmutableList());
        if (!missingColumns.isEmpty()) {
            // every column of the table is written to every data file, so a partial insert would
            // leave the omitted columns undefined rather than null
            throw new TrinoException(NOT_SUPPORTED, "Inserting into a subset of the columns is not supported; column '%s' is missing".formatted(missingColumns.getFirst().name()));
        }
        return new DuckLakeWriteTarget(
                handle.schemaTableName(),
                handle.tableId(),
                handle.tableLocation(),
                writeColumns,
                writePartitioningOf(handle));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return finishWrite((DuckLakeWriteTarget) insertHandle, fragments);
    }

    /**
     * Registers the data files the workers wrote, assigns them row identifiers, and folds their
     * statistics into the table's.
     */
    private Optional<ConnectorOutputMetadata> finishWrite(DuckLakeWriteTarget target, Collection<Slice> fragments)
    {
        List<DuckLakeDataFile> dataFiles = fragments.stream()
                .map(fragment -> dataFileCodec.fromJson(fragment.getBytes()))
                .collect(toImmutableList());
        if (dataFiles.isEmpty()) {
            return Optional.empty();
        }
        commit(commit -> {
            addDataFiles(commit, target, dataFiles);
            commit.recordInsert(target.tableId());
            return null;
        });
        return Optional.empty();
    }

    /**
     * Adds data files to a table, numbering their rows from the table's next row identifier and
     * updating the statistics the catalog keeps for the whole table.
     */
    private void addDataFiles(DuckLakeCommit commit, DuckLakeWriteTarget target, List<DuckLakeDataFile> dataFiles)
    {
        DuckLakeCommit.TableStatsRow stats = commit.tableStats(target.tableId())
                .orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
        long nextRowId = stats.nextRowId();
        long recordCount = stats.recordCount();
        long fileSizeBytes = stats.fileSizeBytes();

        Map<Long, DuckLakeTableColumnStats> columnStats = commit.tableColumnStats(target.tableId()).stream()
                .collect(toImmutableMap(DuckLakeTableColumnStats::columnId, stats1 -> stats1, (first, _) -> first));
        Map<Long, DuckLakeWriteColumn> columnsById = DuckLakeWriteColumn.flatten(target.columns()).stream()
                .collect(toImmutableMap(DuckLakeWriteColumn::columnId, column -> column, (first, _) -> first));
        Map<Long, DuckLakeTableColumnStats> mergedStats = new LinkedHashMap<>(columnStats);

        for (DuckLakeDataFile dataFile : dataFiles) {
            long dataFileId = commit.allocateFileId();
            commit.insertDataFile(target.tableId(), dataFileId, new DuckLakeCommit.DataFileRow(
                    dataFile.path(),
                    dataFile.recordCount(),
                    dataFile.fileSizeBytes(),
                    dataFile.footerSize(),
                    nextRowId,
                    target.partitioning().map(DuckLakePartitioning::partitionId).map(OptionalLong::of).orElse(OptionalLong.empty())));
            nextRowId += dataFile.recordCount();
            recordCount += dataFile.recordCount();
            fileSizeBytes += dataFile.fileSizeBytes();

            for (int index = 0; index < dataFile.partitionValues().size(); index++) {
                commit.insertFilePartitionValue(target.tableId(), dataFileId, index, dataFile.partitionValues().get(index));
            }
            for (DuckLakeFileColumnStatsRow fileStats : dataFile.columnStatistics()) {
                commit.insertFileColumnStats(target.tableId(), dataFileId, fileStats);
                DuckLakeWriteColumn column = columnsById.get(fileStats.columnId());
                if (column != null) {
                    mergedStats.merge(
                            fileStats.columnId(),
                            toTableColumnStats(column, fileStats),
                            (existing, added) -> mergeColumnStats(column.type(), existing, added));
                }
            }
        }

        commit.writeTableStats(target.tableId(), new DuckLakeCommit.TableStatsRow(recordCount, nextRowId, fileSizeBytes));
        for (DuckLakeTableColumnStats stats2 : mergedStats.values()) {
            commit.writeTableColumnStats(target.tableId(), stats2);
        }
    }

    private static DuckLakeTableColumnStats toTableColumnStats(DuckLakeWriteColumn column, DuckLakeFileColumnStatsRow fileStats)
    {
        Optional<Boolean> containsNull = Optional.empty();
        if (fileStats.nullCount().isPresent()) {
            containsNull = Optional.of(fileStats.nullCount().orElseThrow() > 0);
        }
        return new DuckLakeTableColumnStats(
                column.columnId(),
                containsNull,
                fileStats.containsNan(),
                fileStats.minValue(),
                fileStats.maxValue());
    }

    /**
     * Widens a column's table-wide statistics to also cover a newly written file. Anything the new
     * file leaves unknown makes the combined statistic unknown, so a reader never sees bounds that
     * exclude rows the table holds.
     */
    private static DuckLakeTableColumnStats mergeColumnStats(Type type, DuckLakeTableColumnStats existing, DuckLakeTableColumnStats added)
    {
        return new DuckLakeTableColumnStats(
                existing.columnId(),
                mergeFlag(existing.containsNull(), added.containsNull()),
                mergeFlag(existing.containsNan(), added.containsNan()),
                mergeBound(type, existing.minValue(), added.minValue(), true),
                mergeBound(type, existing.maxValue(), added.maxValue(), false));
    }

    private static Optional<Boolean> mergeFlag(Optional<Boolean> existing, Optional<Boolean> added)
    {
        if (existing.isEmpty() || added.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(existing.get() || added.get());
    }

    private static Optional<String> mergeBound(Type type, Optional<String> existing, Optional<String> added, boolean minimum)
    {
        if (existing.isEmpty() || added.isEmpty()) {
            return Optional.empty();
        }
        Optional<Object> existingValue = StatsValueParser.parse(type, existing.get());
        Optional<Object> addedValue = StatsValueParser.parse(type, added.get());
        if (existingValue.isEmpty() || addedValue.isEmpty()) {
            return Optional.empty();
        }
        try {
            @SuppressWarnings("unchecked")
            Comparable<Object> left = (Comparable<Object>) existingValue.get();
            int comparison = left.compareTo(addedValue.get());
            if (minimum) {
                return comparison <= 0 ? existing : added;
            }
            return comparison >= 0 ? existing : added;
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // DuckLake has no in-place update: a changed row is removed from the file it lives in and
        // written again elsewhere
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DuckLakeMergeRowId.columnHandle();
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        List<DuckLakeWriteColumn> writeColumns = DuckLakeColumns.fromCatalog(metastore.columns(handle.snapshotId(), handle.tableId()));
        return new DuckLakeMergeTableHandle(
                handle,
                new DuckLakeWriteTarget(
                        handle.schemaTableName(),
                        handle.tableId(),
                        handle.tableLocation(),
                        writeColumns,
                        writePartitioningOf(handle)));
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DuckLakeMergeTableHandle handle = (DuckLakeMergeTableHandle) mergeTableHandle;
        DuckLakeWriteTarget target = handle.writeTarget();

        ImmutableList.Builder<DuckLakeDataFile> insertedFiles = ImmutableList.builder();
        Map<Long, LongOpenHashSet> removedPositionsByDataFile = new LinkedHashMap<>();
        for (Slice fragment : fragments) {
            DuckLakeMergeFragment parsed = mergeFragmentCodec.fromJson(fragment.getBytes());
            parsed.insertedFile().ifPresent(insertedFiles::add);
            parsed.deletedRows().ifPresent(deleted -> {
                LongOpenHashSet positions = removedPositionsByDataFile.computeIfAbsent(deleted.dataFileId(), _ -> new LongOpenHashSet());
                for (long position : deleted.unpack()) {
                    positions.add(position);
                }
            });
        }
        List<DuckLakeDataFile> dataFiles = insertedFiles.build();
        if (dataFiles.isEmpty() && removedPositionsByDataFile.isEmpty()) {
            return;
        }

        List<RewrittenDeletes> rewrittenDeletes = rewriteDeleteFiles(session, target, removedPositionsByDataFile);
        commit(commit -> {
            commit.verifyDataFilesUnchanged(target.tableId(), removedPositionsByDataFile.keySet());
            commit.verifyDeleteFilesUnchanged(
                    target.tableId(),
                    removedPositionsByDataFile.keySet(),
                    rewrittenDeletes.stream()
                            .filter(rewritten -> rewritten.replacedDeleteFileId().isPresent())
                            .map(rewritten -> rewritten.replacedDeleteFileId().orElseThrow())
                            .collect(toImmutableSet()));
            applyDeletes(commit, target, rewrittenDeletes);
            if (!dataFiles.isEmpty()) {
                addDataFiles(commit, target, dataFiles);
                commit.recordInsert(target.tableId());
            }
            if (!rewrittenDeletes.isEmpty()) {
                commit.recordDelete(target.tableId());
            }
            return null;
        });
    }

    /**
     * Writes the delete files a row-level change needs, before the commit that registers them.
     * Because a data file has at most one delete file, each one covers both the positions already
     * recorded and the ones the statement removed; a file with nothing left is not given a delete
     * file at all and is dropped instead.
     */
    private List<RewrittenDeletes> rewriteDeleteFiles(ConnectorSession session, DuckLakeWriteTarget target, Map<Long, LongOpenHashSet> removedPositionsByDataFile)
    {
        if (removedPositionsByDataFile.isEmpty()) {
            return ImmutableList.of();
        }
        Map<Long, DuckLakeDataFileEntry> dataFiles = metastore.dataFiles(snapshotId(), target.tableId()).stream()
                .collect(toImmutableMap(DuckLakeDataFileEntry::dataFileId, file -> file, (first, _) -> first));
        Map<Long, DuckLakeDeleteFileEntry> deleteFiles = metastore.deleteFiles(snapshotId(), target.tableId()).stream()
                .collect(toImmutableMap(DuckLakeDeleteFileEntry::dataFileId, file -> file, (first, _) -> first));
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        ImmutableList.Builder<RewrittenDeletes> rewritten = ImmutableList.builder();
        for (Map.Entry<Long, LongOpenHashSet> entry : removedPositionsByDataFile.entrySet()) {
            long dataFileId = entry.getKey();
            DuckLakeDataFileEntry dataFile = dataFiles.get(dataFileId);
            if (dataFile == null) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Data file %s of table %s is no longer visible".formatted(dataFileId, target.tableName()));
            }
            LongOpenHashSet positions = entry.getValue();
            DuckLakeDeleteFileEntry existing = deleteFiles.get(dataFileId);
            if (existing != null) {
                positions.addAll(DuckLakeDeleteFiles.readPositions(
                        fileSystem,
                        fileFormatDataSourceStats,
                        parquetReaderOptions,
                        PathResolver.resolve(target.tableLocation(), existing.path(), existing.pathIsRelative()),
                        existing.fileSizeBytes(),
                        existing.deleteCount()));
            }
            Optional<Long> replacedDeleteFileId = Optional.ofNullable(existing).map(DuckLakeDeleteFileEntry::deleteFileId);
            long removedNow = positions.size() - (existing == null ? 0 : existing.deleteCount());

            if (positions.size() >= dataFile.recordCount()) {
                // nothing of the file is left, so it is dropped rather than fully covered by deletes
                rewritten.add(new RewrittenDeletes(dataFileId, replacedDeleteFileId, Optional.empty(), removedNow, dataFile.fileSizeBytes()));
                continue;
            }
            long[] sorted = positions.toLongArray();
            Arrays.sort(sorted);
            String relativePath = DuckLakeWriterFactory.newDeleteFileName();
            DuckLakeDeleteFiles.WrittenFile written = DuckLakeDeleteFiles.write(
                    session,
                    fileSystem,
                    writerFactory,
                    PathResolver.resolve(target.tableLocation(), relativePath, true),
                    PathResolver.resolve(target.tableLocation(), dataFile.path(), dataFile.pathIsRelative()),
                    sorted);
            rewritten.add(new RewrittenDeletes(
                    dataFileId,
                    replacedDeleteFileId,
                    Optional.of(new NewDeleteFile(relativePath, sorted.length, written.fileSizeBytes(), written.footerSize())),
                    removedNow,
                    0));
        }
        return rewritten.build();
    }

    private static void applyDeletes(DuckLakeCommit commit, DuckLakeWriteTarget target, List<RewrittenDeletes> rewrittenDeletes)
    {
        if (rewrittenDeletes.isEmpty()) {
            return;
        }
        long removedRecords = 0;
        long removedFileBytes = 0;
        for (RewrittenDeletes rewritten : rewrittenDeletes) {
            commit.endDeleteFilesFor(target.tableId(), ImmutableList.of(rewritten.dataFileId()));
            removedRecords += rewritten.removedRecordCount();
            if (rewritten.newDeleteFile().isEmpty()) {
                commit.endDataFiles(target.tableId(), ImmutableList.of(rewritten.dataFileId()));
                removedFileBytes += rewritten.removedFileSizeBytes();
                continue;
            }
            NewDeleteFile deleteFile = rewritten.newDeleteFile().orElseThrow();
            commit.insertDeleteFile(
                    target.tableId(),
                    commit.allocateFileId(),
                    rewritten.dataFileId(),
                    deleteFile.path(),
                    deleteFile.deleteCount(),
                    deleteFile.fileSizeBytes(),
                    deleteFile.footerSize());
        }
        DuckLakeCommit.TableStatsRow stats = commit.tableStats(target.tableId())
                .orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
        commit.writeTableStats(target.tableId(), new DuckLakeCommit.TableStatsRow(
                Math.max(0, stats.recordCount() - removedRecords),
                stats.nextRowId(),
                Math.max(0, stats.fileSizeBytes() - removedFileBytes)));
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) handle;
        // anything the connector cannot decide from the catalog alone needs the positions of the
        // rows that go, which the engine produces through a merge
        if (!tableHandle.unenforcedConstraint().isAll()) {
            return Optional.empty();
        }
        if (tableHandle.enforcedConstraint().isAll() || fullyMatchingDataFiles(tableHandle).isPresent()) {
            return Optional.of(tableHandle);
        }
        return Optional.empty();
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) handle;
        if (tableHandle.enforcedConstraint().isAll()) {
            return OptionalLong.of(commit(commit -> {
                long removed = removeAllRows(commit, tableHandle.tableId());
                commit.recordDelete(tableHandle.tableId());
                return removed;
            }));
        }
        List<Long> dataFileIds = fullyMatchingDataFiles(tableHandle)
                .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "The rows to delete are no longer decidable from the catalog"));
        return OptionalLong.of(commit(commit -> {
            commit.verifyDataFilesUnchanged(tableHandle.tableId(), ImmutableSet.copyOf(dataFileIds));
            long removed = removeDataFiles(commit, tableHandle.tableId(), dataFileIds);
            commit.recordDelete(tableHandle.tableId());
            return removed;
        }));
    }

    /**
     * The data files a delete can drop whole, or {@link Optional#empty()} when the predicate
     * cannot be decided from the catalog.
     * <p>
     * A predicate reaches this point only if {@link #applyFilter} enforced all of it, which it
     * does only for partition keys that file rows by the column value itself and only while every
     * visible file carries such a value. Each file therefore holds one value of the column, and
     * either every row of it matches or none does.
     */
    private Optional<List<Long>> fullyMatchingDataFiles(DuckLakeTableHandle handle)
    {
        Map<DuckLakeColumnHandle, Domain> domains = handle.enforcedConstraint().getDomains().orElse(null);
        if (domains == null || domains.isEmpty()) {
            return Optional.empty();
        }
        Optional<DuckLakePartitionInfo> partitionInfo = metastore.partitionInfo(handle.snapshotId(), handle.tableId());
        if (partitionInfo.isEmpty()) {
            return Optional.empty();
        }
        ListMultimap<Long, DuckLakePartitionColumn> transformsByColumnId = ArrayListMultimap.create();
        partitionInfo.get().columns().forEach(column -> transformsByColumnId.put(column.columnId(), column));

        ImmutableList.Builder<Long> matching = ImmutableList.builder();
        for (DuckLakeDataFileEntry dataFile : metastore.dataFiles(handle.snapshotId(), handle.tableId())) {
            Boolean matches = fileMatches(domains, transformsByColumnId, dataFile);
            if (matches == null) {
                return Optional.empty();
            }
            if (matches) {
                matching.add(dataFile.dataFileId());
            }
        }
        return Optional.of(matching.build());
    }

    /**
     * Whether every row of the file matches the predicate, or {@code null} when the file's
     * partition values do not decide it.
     */
    @Nullable
    private static Boolean fileMatches(
            Map<DuckLakeColumnHandle, Domain> domains,
            ListMultimap<Long, DuckLakePartitionColumn> transformsByColumnId,
            DuckLakeDataFileEntry dataFile)
    {
        for (Map.Entry<DuckLakeColumnHandle, Domain> entry : domains.entrySet()) {
            Optional<Domain> fileDomain = PartitionTransforms.partitionDomain(
                    entry.getKey(),
                    transformsByColumnId.get(entry.getKey().columnId()),
                    dataFile.partitionValues());
            if (fileDomain.isEmpty() || (!fileDomain.get().isSingleValue() && !fileDomain.get().isOnlyNull())) {
                return null;
            }
            if (fileDomain.get().intersect(entry.getValue()).isNone()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Ends the given data files, removing every row of them.
     */
    private static long removeDataFiles(DuckLakeCommit commit, long tableId, List<Long> dataFileIds)
    {
        if (dataFileIds.isEmpty()) {
            return 0;
        }
        Map<Long, DuckLakeCommit.VisibleDataFile> visible = commit.visibleDataFiles(tableId).stream()
                .collect(toImmutableMap(DuckLakeCommit.VisibleDataFile::dataFileId, file -> file, (first, _) -> first));
        long removedRecords = 0;
        long removedBytes = 0;
        for (long dataFileId : dataFileIds) {
            DuckLakeCommit.VisibleDataFile file = visible.get(dataFileId);
            if (file == null) {
                throw new DuckLakeCommit.ConcurrentModificationFailure("data file %s of table %s was removed by another transaction".formatted(dataFileId, tableId));
            }
            removedRecords += file.visibleRecordCount();
            removedBytes += file.fileSizeBytes();
        }
        commit.endDataFiles(tableId, dataFileIds);
        commit.endDeleteFilesFor(tableId, dataFileIds);
        DuckLakeCommit.TableStatsRow stats = commit.tableStats(tableId).orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
        commit.writeTableStats(tableId, new DuckLakeCommit.TableStatsRow(
                Math.max(0, stats.recordCount() - removedRecords),
                stats.nextRowId(),
                Math.max(0, stats.fileSizeBytes() - removedBytes)));
        return removedRecords;
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            removeAllRows(commit, handle.tableId());
            commit.recordDelete(handle.tableId());
            return null;
        });
    }

    /**
     * Ends every data file of the table, which removes all of its rows while leaving them visible
     * to readers of earlier snapshots. Row identifiers keep counting from where they were, so a
     * row written later never reuses one.
     */
    private static long removeAllRows(DuckLakeCommit commit, long tableId)
    {
        List<DuckLakeCommit.VisibleDataFile> dataFiles = commit.visibleDataFiles(tableId);
        List<Long> dataFileIds = dataFiles.stream()
                .map(DuckLakeCommit.VisibleDataFile::dataFileId)
                .collect(toImmutableList());
        commit.endDataFiles(tableId, dataFileIds);
        commit.endDeleteFilesFor(tableId, dataFileIds);
        long removed = dataFiles.stream().mapToLong(DuckLakeCommit.VisibleDataFile::visibleRecordCount).sum();
        DuckLakeCommit.TableStatsRow stats = commit.tableStats(tableId).orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
        commit.writeTableStats(tableId, new DuckLakeCommit.TableStatsRow(0, stats.nextRowId(), 0));
        return removed;
    }

    private record RewrittenDeletes(
            long dataFileId,
            Optional<Long> replacedDeleteFileId,
            Optional<NewDeleteFile> newDeleteFile,
            long removedRecordCount,
            long removedFileSizeBytes) {}

    private record NewDeleteFile(String path, long deleteCount, long fileSizeBytes, long footerSize) {}

    /**
     * The partitioning recorded in the catalog, as the table is described.
     * <p>
     * This does not check that the connector could write those transforms. A table that another
     * engine partitioned in a way this connector cannot write must still be readable, and
     * {@code getTableMetadata} runs on every read.
     */
    private Optional<DuckLakePartitioning> partitioningOf(DuckLakeTableHandle handle)
    {
        Optional<DuckLakePartitionInfo> partitionInfo = metastore.partitionInfo(handle.snapshotId(), handle.tableId());
        if (partitionInfo.isEmpty()) {
            return Optional.empty();
        }
        List<DuckLakeWriteColumn> columns = DuckLakeColumns.fromCatalog(metastore.columns(handle.snapshotId(), handle.tableId()));
        ImmutableList.Builder<DuckLakePartitioning.Field> fields = ImmutableList.builder();
        for (DuckLakePartitionColumn partitionColumn : partitionInfo.get().columns()) {
            int channel = -1;
            for (int index = 0; index < columns.size(); index++) {
                if (columns.get(index).columnId() == partitionColumn.columnId()) {
                    channel = index;
                    break;
                }
            }
            if (channel < 0) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Partition key of table %s refers to column %s, which the table does not have"
                        .formatted(handle.schemaTableName(), partitionColumn.columnId()));
            }
            DuckLakeWriteColumn column = columns.get(channel);
            fields.add(new DuckLakePartitioning.Field(channel, column.columnId(), column.name(), partitionColumn.transform()));
        }
        return Optional.of(new DuckLakePartitioning(partitionInfo.get().partitionId(), fields.build()));
    }

    /**
     * The partitioning to write a table with, taken from the scheme recorded in the catalog.
     * <p>
     * A transform this connector cannot apply is rejected here, where the write begins, rather
     * than on the read path. Writing it would file rows under a partition value that disagrees
     * with the one another engine computes for the same row.
     */
    private Optional<DuckLakePartitioning> writePartitioningOf(DuckLakeTableHandle handle)
    {
        Optional<DuckLakePartitioning> partitioning = partitioningOf(handle);
        if (partitioning.isPresent()) {
            List<DuckLakeWriteColumn> columns = DuckLakeColumns.fromCatalog(metastore.columns(handle.snapshotId(), handle.tableId()));
            for (DuckLakePartitioning.Field field : partitioning.get().fields()) {
                DuckLakeWriteColumn column = columns.get(field.sourceChannel());
                DuckLakeWritePartitioner.validateTransform(field.transform(), column.name(), column.type());
            }
        }
        return partitioning;
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        if (!viewProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support view properties");
        }
        requireViewSupport();
        commit(commit -> {
            DuckLakeCommit.SchemaIdentity schema = commit.findSchema(viewName.getSchemaName())
                    .orElseThrow(() -> new SchemaNotFoundException(viewName.getSchemaName()));
            if (commit.findTable(viewName.getSchemaName(), viewName.getTableName()).isPresent()) {
                throw new TrinoException(TABLE_ALREADY_EXISTS, "Table already exists: " + viewName);
            }
            Optional<DuckLakeCommit.ViewIdentity> existing = commit.findView(viewName.getSchemaName(), viewName.getTableName());
            if (existing.isPresent()) {
                if (!replace) {
                    throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
                }
                commit.endView(existing.get().viewId());
                commit.setTableTag(existing.get().viewId(), VIEW_DEFINITION_TAG_KEY, Optional.empty());
            }

            long viewId = commit.allocateCatalogId();
            // The row is a DuckLake view like any other, so another engine lists it and can read
            // it when the query happens to parse in its own dialect. What Trino needs beyond the
            // query text — the column types, the owner and how the view runs — has no column of
            // its own, so it is kept as a tag beside the view.
            commit.insertViewRow(
                    viewId,
                    schema.schemaId(),
                    viewName.getTableName(),
                    TRINO_VIEW_DIALECT,
                    definition.getOriginalSql(),
                    formatColumnAliases(definition));
            commit.setTableTag(viewId, VIEW_DEFINITION_TAG_KEY, Optional.of(viewDefinitionCodec.toJson(definition)));
            commit.recordCreatedView(viewName.getSchemaName(), viewName.getTableName());
            return null;
        });
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        requireViewSupport();
        commit(commit -> {
            DuckLakeCommit.ViewIdentity view = commit.findView(viewName.getSchemaName(), viewName.getTableName())
                    .orElseThrow(() -> new ViewNotFoundException(viewName));
            commit.endView(view.viewId());
            commit.setTableTag(view.viewId(), VIEW_DEFINITION_TAG_KEY, Optional.empty());
            commit.recordDroppedView(view.viewId());
            return null;
        });
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        requireViewSupport();
        commit(commit -> {
            DuckLakeViewEntry view = metastore.findView(commit.baseSnapshotId(), source.getSchemaName(), source.getTableName())
                    .orElseThrow(() -> new ViewNotFoundException(source));
            DuckLakeCommit.SchemaIdentity targetSchema = commit.findSchema(target.getSchemaName())
                    .orElseThrow(() -> new SchemaNotFoundException(target.getSchemaName()));
            if (commit.findView(target.getSchemaName(), target.getTableName()).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + target);
            }
            commit.endView(view.viewId());
            commit.insertViewRow(view.viewId(), targetSchema.schemaId(), target.getTableName(), view.dialect(), view.sql(), view.columnAliases());
            commit.recordCreatedView(target.getSchemaName(), target.getTableName());
            return null;
        });
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return metastore.listViews(snapshotId(), schemaName).stream()
                .map(view -> new SchemaTableName(view.schemaName(), view.viewName()))
                .distinct()
                .collect(toImmutableList());
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return metastore.findView(snapshotId(), viewName.getSchemaName(), viewName.getTableName())
                .flatMap(this::toViewDefinition);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (DuckLakeViewEntry view : metastore.listViews(snapshotId(), schemaName)) {
            toViewDefinition(view).ifPresent(definition -> views.put(new SchemaTableName(view.schemaName(), view.viewName()), definition));
        }
        return views.buildKeepingLast();
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        requireViewSupport();
        DuckLakeViewEntry view = metastore.findView(snapshotId(), viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition definition = toViewDefinition(view)
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition updated = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                comment,
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());
        commit(commit -> {
            commit.setTableTag(view.viewId(), VIEW_DEFINITION_TAG_KEY, Optional.of(viewDefinitionCodec.toJson(updated)));
            commit.recordTableMetadataChange(view.viewId());
            return null;
        });
    }

    /**
     * Reads back the definition Trino stored beside the view. A view another engine defined has no
     * such definition, and is left out rather than guessed at: its query is written in a dialect
     * this connector cannot resolve the columns of.
     */
    private Optional<ConnectorViewDefinition> toViewDefinition(DuckLakeViewEntry view)
    {
        if (!view.dialect().equalsIgnoreCase(TRINO_VIEW_DIALECT)) {
            return Optional.empty();
        }
        return metastore.tag(snapshotId(), view.viewId(), VIEW_DEFINITION_TAG_KEY)
                .map(viewDefinitionCodec::fromJson);
    }

    private void requireViewSupport()
    {
        if (!metastore.viewsSupported()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "This DuckLake catalog was created before views were added to the format");
        }
    }

    /**
     * The view's column names in the form DuckLake stores them, each quoted and separated by
     * commas, which is what another engine reads to name the view's columns.
     */
    private static String formatColumnAliases(ConnectorViewDefinition definition)
    {
        return definition.getColumns().stream()
                .map(column -> "\"" + column.getName().replace("\"", "\"\"") + "\"")
                .collect(joining(","));
    }

    /**
     * The directory a schema or table stores its files in. DuckDB names it after the object, which
     * keeps the layout readable; names that cannot be a single path segment fall back to a
     * generated one.
     */
    private static String directoryName(String name)
    {
        if (name.isEmpty() || name.equals(".") || name.equals("..") || name.contains("/") || name.contains("\\")) {
            return UUID.randomUUID() + "/";
        }
        return name + "/";
    }

    private record NewTable(long tableId, List<DuckLakeWriteColumn> columns, String location, Optional<DuckLakePartitioning> partitioning) {}

    /**
     * Records the partitioning of a table being created or altered, resolving each key to the
     * column it reads and checking that the transform can be applied to it.
     */
    private static Optional<DuckLakePartitioning> createPartitioning(DuckLakeCommit commit, long tableId, List<DuckLakeWriteColumn> columns, List<String> partitionKeys)
    {
        if (partitionKeys.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<DuckLakePartitionColumn> catalogColumns = ImmutableList.builder();
        ImmutableList.Builder<DuckLakePartitioning.Field> fields = ImmutableList.builder();
        for (int index = 0; index < partitionKeys.size(); index++) {
            DuckLakeTableProperties.PartitionKey key = DuckLakeTableProperties.parsePartitionKey(partitionKeys.get(index));
            int channel = -1;
            for (int column = 0; column < columns.size(); column++) {
                if (columns.get(column).name().equalsIgnoreCase(key.columnName())) {
                    channel = column;
                    break;
                }
            }
            if (channel < 0) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Partition key refers to a column the table does not have: " + key.columnName());
            }
            DuckLakeWriteColumn column = columns.get(channel);
            DuckLakeWritePartitioner.validateTransform(key.transform(), column.name(), column.type());
            catalogColumns.add(new DuckLakePartitionColumn(index, column.columnId(), key.transform()));
            fields.add(new DuckLakePartitioning.Field(channel, column.columnId(), column.name(), key.transform()));
        }
        long partitionId = commit.insertPartitioning(tableId, catalogColumns.build());
        return Optional.of(new DuckLakePartitioning(partitionId, fields.build()));
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        Set<String> unsupported = properties.keySet().stream()
                .filter(name -> !name.equals(DuckLakeTableProperties.PARTITIONING_PROPERTY))
                .collect(toImmutableSet());
        if (!unsupported.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting the table property " + unsupported.iterator().next());
        }
        @SuppressWarnings("unchecked")
        List<String> partitionKeys = (List<String>) properties.get(DuckLakeTableProperties.PARTITIONING_PROPERTY)
                .orElse(ImmutableList.of());
        commit(commit -> {
            List<DuckLakeWriteColumn> columns = DuckLakeColumns.fromCatalog(metastore.columns(handle.snapshotId(), handle.tableId()));
            // data files already written keep the partitioning they were written with, which the
            // read path notices and stops using partition values to prune with
            commit.endPartitioning(handle.tableId());
            createPartitioning(commit, handle.tableId(), columns, partitionKeys);
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        TupleDomain<DuckLakeColumnHandle> predicate = constraint.getSummary().transformKeys(DuckLakeColumnHandle.class::cast);
        if (predicate.isAll() || handle.rowCount().isPresent()) {
            // A scan reduced to the row count of the table has no column left to filter on
            return Optional.empty();
        }

        TupleDomain<DuckLakeColumnHandle> newEnforcedConstraint;
        TupleDomain<DuckLakeColumnHandle> newUnenforcedConstraint;
        if (predicate.isNone()) {
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
        }
        else {
            Map<Long, DuckLakePartitionColumn> partitionColumns = enforceablePartitionColumns(handle);
            // the files are only listed for a column whose values have to be checked one by one,
            // and then only once however many such columns the predicate constrains
            Supplier<List<DuckLakeDataFileEntry>> dataFiles = Suppliers.memoize(() -> metastore.dataFiles(handle.snapshotId(), handle.tableId()));
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> enforceableDomains = ImmutableMap.builder();
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> unenforceableDomains = ImmutableMap.builder();
            for (Map.Entry<DuckLakeColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
                DuckLakeColumnHandle column = entry.getKey();
                DuckLakePartitionColumn partitionColumn = partitionColumns.get(column.columnId());
                if (partitionColumn != null && isEnforceableType(column.type())
                        && (!isTemporalType(column.type()) || everyPartitionValueDecidesTheColumn(column, partitionColumn, dataFiles.get()))) {
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
                handle.unenforcedConstraint().intersect(newUnenforcedConstraint),
                handle.rowCount());

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
     * Returns the partition columns whose predicates the connector can fully enforce by pruning
     * files, by column id: identity-transformed columns where every visible data file was written
     * with the current partitioning scheme, so every row of a kept file carries the partition
     * value.
     */
    private Map<Long, DuckLakePartitionColumn> enforceablePartitionColumns(DuckLakeTableHandle handle)
    {
        Optional<DuckLakePartitionInfo> partitionInfo = metastore.partitionInfo(handle.snapshotId(), handle.tableId());
        if (partitionInfo.isEmpty()) {
            return ImmutableMap.of();
        }
        if (!metastore.allDataFilesUsePartition(handle.snapshotId(), handle.tableId(), partitionInfo.get().partitionId())) {
            return ImmutableMap.of();
        }
        return partitionInfo.get().columns().stream()
                .filter(column -> column.transform().equalsIgnoreCase(IDENTITY_TRANSFORM))
                // a column filed under two identity keys is read from the first of them, which is
                // the one PartitionTransforms picks as well
                .collect(toImmutableMap(DuckLakePartitionColumn::columnId, column -> column, (first, _) -> first));
    }

    /**
     * Types whose identity partition values round-trip exactly through the string representation
     * in {@code ducklake_file_partition_value}, so pruning on them can be used to enforce a
     * predicate. A temporal type is admitted here only as a candidate: DuckDB writes values such
     * as {@code infinity}, {@code -infinity}, BC dates and years with five or more digits, which
     * do not parse back, so {@link #isTemporalType} sends its columns through
     * {@link #everyPartitionValueDecidesTheColumn} first.
     */
    private static boolean isEnforceableType(Type type)
    {
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(BOOLEAN)
                || type instanceof VarcharType varcharType && varcharType.isUnbounded()
                || isTemporalType(type);
    }

    /**
     * Types a partition value of which DuckDB may write in a form that does not parse back, so
     * that the values of the table decide whether a predicate on the column can be enforced.
     */
    private static boolean isTemporalType(Type type)
    {
        return type.equals(DATE)
                || type instanceof TimestampType
                || type instanceof TimestampWithTimeZoneType;
    }

    /**
     * Whether the partition value of every visible data file says which value of the column the
     * rows of that file hold. A value the connector cannot read back leaves the file unprunable,
     * and a predicate the connector cannot apply to one file it is not allowed to prune is one it
     * cannot enforce for the table, so the engine keeps filtering the rows instead.
     * <p>
     * The check runs over the same files and through the same code as
     * {@link DuckLakeSplitManager#getSplits}, which reads the same snapshot, so a predicate
     * enforced here is a predicate that splits can be pruned by there.
     */
    private static boolean everyPartitionValueDecidesTheColumn(
            DuckLakeColumnHandle column,
            DuckLakePartitionColumn partitionColumn,
            List<DuckLakeDataFileEntry> dataFiles)
    {
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(partitionColumn);
        return dataFiles.stream()
                .allMatch(dataFile -> PartitionTransforms.partitionDomain(column, transforms, dataFile.partitionValues()).isPresent());
    }

    /**
     * Answers a global {@code count(*)} from the catalog, which counts the rows of the table
     * without listing its files, let alone reading them.
     * <p>
     * Only a {@code count(*)} over the whole table is pushed down. Grouping needs the grouped
     * values to be read, and any other aggregate function needs the aggregated column. A predicate
     * the connector enforces would have to be applied to the counted files, which the catalog
     * query does not do; such a scan keeps listing its files, and reads none of them because a
     * split that projects no column is answered from its record count.
     */
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        if (handle.rowCount().isPresent()) {
            return Optional.empty();
        }
        if (!handle.enforcedConstraint().isAll() || !handle.unenforcedConstraint().isAll()) {
            return Optional.empty();
        }
        if (groupingSets.size() != 1 || !getOnlyElement(groupingSets).isEmpty()) {
            return Optional.empty();
        }
        if (aggregates.size() != 1 || !isCountStar(getOnlyElement(aggregates))) {
            return Optional.empty();
        }
        DuckLakeRowCount rowCount = rowCount(handle);
        if (!rowCount.exact()) {
            return Optional.empty();
        }
        return Optional.of(new AggregationApplicationResult<>(
                handle.withRowCount(rowCount.rowCount()),
                ImmutableList.of(new Variable(ROW_COUNT_COLUMN.name(), BIGINT)),
                ImmutableList.of(new Assignment(ROW_COUNT_COLUMN.name(), ROW_COUNT_COLUMN, BIGINT)),
                ImmutableMap.of(),
                false));
    }

    private static boolean isCountStar(AggregateFunction aggregate)
    {
        return aggregate.getFunctionName().equals("count")
                && aggregate.getArguments().isEmpty()
                && aggregate.getOutputType().equals(BIGINT)
                && !aggregate.isDistinct()
                && aggregate.getFilter().isEmpty()
                && aggregate.getSortItems().isEmpty();
    }

    /**
     * The row count of a table in this transaction's snapshot. The count is read once per table
     * because the snapshot is fixed for the transaction, and because the optimizer may ask for the
     * same aggregation pushdown several times while it explores a plan.
     */
    private DuckLakeRowCount rowCount(DuckLakeTableHandle handle)
    {
        return rowCounts.computeIfAbsent(handle.tableId(), tableId -> metastore.rowCount(handle.snapshotId(), tableId));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        if (handle.rowCount().isPresent()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(1))
                    .build();
        }
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount(handle).rowCount()));

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
