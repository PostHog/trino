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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
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
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedSummary;
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
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import static io.trino.plugin.ducklake.util.DuckLakeViewSql.withCatalogPlaceholder;
import static io.trino.plugin.ducklake.util.PartitionTransforms.IDENTITY_TRANSFORM;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
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
    private static final Logger log = Logger.get(DuckLakeMetadata.class);

    private static final String COMMENT_TAG_KEY = "comment";
    private static final String VIEW_DEFINITION_TAG_KEY = "trino_view_definition";
    private static final String TRINO_VIEW_DIALECT = "trino";

    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;
    private final boolean addFilesProcedureEnabled;
    private final int addFilesMaxConcurrentFileReads;
    private final DuckLakeRangeReplacement rangeReplacement;
    private final Map<Long, DuckLakeRowCount> rowCounts = new ConcurrentHashMap<>();
    private final Map<TableSnapshot, DuckLakeInlinedSummary> inlinedSummaries = new ConcurrentHashMap<>();
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
            ParquetReaderOptions parquetReaderOptions,
            boolean addFilesProcedureEnabled,
            int addFilesMaxConcurrentFileReads,
            DuckLakeRangeReplacement rangeReplacement)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.addFilesProcedureEnabled = addFilesProcedureEnabled;
        this.addFilesMaxConcurrentFileReads = addFilesMaxConcurrentFileReads;
        this.rangeReplacement = requireNonNull(rangeReplacement, "rangeReplacement is null");
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
        // pin the transaction to a snapshot first, which also verifies the catalog format version.
        // Everything this statement read, it read there, so that is the snapshot the commit has to
        // be checked for conflicts against.
        long readSnapshotId = snapshotId();

        class Result
        {
            T value;
            long snapshot;
        }

        Result result = metastore.commit(readSnapshotId, commit -> {
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
            commit.insertSchemaRow(schema.schemaId(), target, schema.path(), schema.pathIsRelative());
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
        // Rows and deletions a DuckDB writer stored inline in the catalog database are read along
        // with the data files. Changing them is not supported, which the statements that would are
        // told by verifyNoInlinedData.
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
        commit(commit -> createTable(commit, tableMetadata, saveMode));
    }

    /**
     * Creates the catalog rows of a new table and returns its identifier and location, or null when
     * the table already exists and {@code IGNORE} leaves it alone. Shared by {@code CREATE TABLE}
     * and {@code CREATE TABLE AS}, which then writes data into it.
     */
    @Nullable
    private NewTable createTable(DuckLakeCommit commit, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();
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
                // the table being replaced ends in the snapshot the new one begins in, so the name
                // resolves to the old table or to the new one, never to nothing
                case REPLACE -> endTable(commit, existing.get().tableId());
            }
        }
        List<DuckLakeWriteColumn> columns = DuckLakeColumns.assignColumnIds(tableMetadata.getColumns());
        return insertTable(
                commit,
                schema,
                tableName,
                columns,
                columnComments(tableMetadata.getColumns(), columns),
                tableMetadata.getComment(),
                resolvePartitioning(columns, DuckLakeTableProperties.getPartitioning(tableMetadata.getProperties())));
    }

    /**
     * Writes the catalog rows that describe a table: the table itself, its columns, the comments on
     * both, and its partitioning scheme.
     */
    private NewTable insertTable(
            DuckLakeCommit commit,
            DuckLakeCommit.SchemaIdentity schema,
            SchemaTableName tableName,
            List<DuckLakeWriteColumn> columns,
            Map<Long, String> columnComments,
            Optional<String> comment,
            List<DuckLakePartitioning.Field> partitionFields)
    {
        long tableId = commit.allocateCatalogId();
        String tablePath = directoryName(tableName.getTableName());
        commit.insertTableRow(tableId, schema.schemaId(), tableName.getTableName(), tablePath, Optional.empty());

        for (DuckLakeColumnRow row : DuckLakeColumns.toColumnRows(columns)) {
            commit.insertColumn(tableId, row);
        }
        columnComments.forEach((columnId, columnComment) -> commit.setColumnTag(tableId, columnId, COMMENT_TAG_KEY, Optional.of(columnComment)));
        comment.ifPresent(value -> commit.setTableTag(tableId, COMMENT_TAG_KEY, Optional.of(value)));
        Optional<DuckLakePartitioning> partitioning = insertPartitioning(commit, tableId, partitionFields);
        commit.recordCreatedTable(tableName.getSchemaName(), tableName.getTableName(), tableId);

        String schemaLocation = PathResolver.resolve(dataPath, schema.path(), schema.pathIsRelative());
        return new NewTable(tableId, columns, PathResolver.resolve(schemaLocation, tablePath, true), partitioning);
    }

    /**
     * The comment of each column that has one, by column identifier.
     */
    private static Map<Long, String> columnComments(List<ColumnMetadata> columns, List<DuckLakeWriteColumn> writeColumns)
    {
        ImmutableMap.Builder<Long, String> comments = ImmutableMap.builder();
        for (int index = 0; index < columns.size(); index++) {
            Optional<String> comment = columns.get(index).getComment();
            if (comment.isPresent()) {
                comments.put(writeColumns.get(index).columnId(), comment.get());
            }
        }
        return comments.buildOrThrow();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        commit(commit -> {
            endTable(commit, handle.tableId());
            return null;
        });
    }

    /**
     * Ends every row describing a table, so that it is gone from this snapshot on. The data files
     * stay in place, still visible to readers of the snapshots they belonged to.
     */
    private static void endTable(DuckLakeCommit commit, long tableId)
    {
        commit.endTable(tableId);
        commit.endTableContents(tableId);
        commit.deleteTableStats(tableId);
        commit.recordDroppedTable(tableId);
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
            renameTable(commit, table, targetSchema.schemaId(), newTableName);
            return null;
        });
    }

    /**
     * Gives a table a new name, keeping its identifier, location and UUID: the row naming it ends
     * and a row with the new name begins in this snapshot, as DuckDB renames a table.
     */
    private static void renameTable(DuckLakeCommit commit, DuckLakeCommit.TableIdentity table, long schemaId, SchemaTableName newTableName)
    {
        commit.endTable(table.tableId());
        // the location keeps the original name, so already written data files stay reachable
        commit.insertTableRow(table.tableId(), schemaId, newTableName.getTableName(), table.path(), table.tableUuid());
        commit.recordCreatedTable(newTableName.getSchemaName(), newTableName.getTableName(), table.tableId());
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
        verifyNoInlinedRows(handle, "adding a column");
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
        verifyNoInlinedRows(handle, "dropping a column");
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
        verifyNoInlinedRows(handle, "renaming a column");
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
        verifyNoInlinedRows(handle, "changing the type of a column");
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
        verifyNoInlinedRows(handle, "dropping a NOT NULL constraint");
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
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> properties,
            RetryMode retryMode)
    {
        if (procedureName.equalsIgnoreCase(DuckLakePublishAsProcedure.NAME)) {
            return Optional.of(publishAsHandle(accessControl, (DuckLakeTableHandle) tableHandle, properties));
        }
        if (!procedureName.equalsIgnoreCase(DuckLakeAddFilesProcedure.NAME)) {
            return Optional.empty();
        }
        if (!addFilesProcedureEnabled) {
            throw new TrinoException(StandardErrorCode.PERMISSION_DENIED, "add_files procedure is disabled");
        }
        if (!metastore.fileRegistrationSupported()) {
            throw new TrinoException(NOT_SUPPORTED, "File registration requires a DuckLake catalog with name mapping support");
        }
        DuckLakeTableHandle table = (DuckLakeTableHandle) tableHandle;
        accessControl.checkCanInsertIntoTable(null, table.schemaTableName(), Optional.empty());
        boolean replace = Boolean.TRUE.equals(properties.get("replace"));
        Optional<DuckLakeReplacementRange> range = replacementRange(table, properties);
        if (replace && range.isPresent()) {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "replace and replace_range_column are mutually exclusive");
        }
        if (replace || range.isPresent()) {
            accessControl.checkCanDeleteFromTable(null, table.schemaTableName(), Optional.empty());
        }
        Object rawFiles = properties.get("files");
        if (!(rawFiles instanceof List<?> values) || values.size() > 10000 || (!replace && range.isEmpty() && values.isEmpty())) {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "files must contain 1 to 10000 Parquet locations (an empty replacement is allowed)");
        }
        ImmutableList.Builder<String> files = ImmutableList.builder();
        Set<String> unique = new HashSet<>();
        for (Object value : values) {
            if (!(value instanceof String path)) {
                throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "files must contain distinct non-null locations");
            }
            DuckLakeFileImporter.validateLocation(dataPath, path);
            if (!unique.add(canonicalFileLocation(path))) {
                throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "files must contain distinct non-null locations");
            }
            files.add(path);
        }
        Object rawImportId = properties.get("import_id");
        if (!(rawImportId instanceof String importId) || !importId.matches("[A-Za-z0-9_-]{1,128}")) {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "import_id must contain 1 to 128 letters, digits, underscores or hyphens");
        }
        return Optional.of(new DuckLakeAddFilesHandle(
                table,
                files.build().stream().sorted().collect(toImmutableList()),
                importId,
                Boolean.TRUE.equals(properties.get("allow_missing")),
                Boolean.TRUE.equals(properties.get("ignore_extra_columns")),
                Boolean.TRUE.equals(properties.get("hive_partitioning")),
                replace,
                range));
    }

    /**
     * Plans {@code publish_as}, which renames the table to the target name and drops the table
     * holding that name, if there is one, in the same snapshot. The caller needs the permissions
     * of both statements.
     */
    private DuckLakePublishAsHandle publishAsHandle(ConnectorAccessControl accessControl, DuckLakeTableHandle table, Map<String, Object> properties)
    {
        SchemaTableName target = publishTarget(table, properties.get("target"));
        accessControl.checkCanRenameTable(null, table.schemaTableName(), target);
        if (metastore.findView(table.snapshotId(), target.getSchemaName(), target.getTableName()).isPresent()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "target is a view, which publish_as cannot replace: " + target);
        }
        OptionalLong replacedTableId = metastore.findTable(table.snapshotId(), target.getSchemaName(), target.getTableName())
                .map(replaced -> OptionalLong.of(replaced.tableId()))
                .orElseGet(OptionalLong::empty);
        if (replacedTableId.isPresent()) {
            accessControl.checkCanDropTable(null, target);
        }
        return new DuckLakePublishAsHandle(table, target, replacedTableId);
    }

    /**
     * The name {@code publish_as} gives the table: a table name of the schema the table is in,
     * matched without regard to case, as the engine matches the identifiers of a statement.
     */
    private static SchemaTableName publishTarget(DuckLakeTableHandle table, Object value)
    {
        if (!(value instanceof String name)) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "target must be specified");
        }
        if (name.isBlank()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "target must be a table name, but is empty");
        }
        if (name.contains(".")) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "target must be an unqualified table name, which is published in the schema of the table: " + name);
        }
        SchemaTableName target = new SchemaTableName(table.schemaName(), name);
        if (target.equals(table.schemaTableName())) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "target must name another table than the one published: " + target.getTableName());
        }
        return target;
    }

    private Optional<DuckLakeReplacementRange> replacementRange(DuckLakeTableHandle table, Map<String, Object> properties)
    {
        String name = (String) properties.get("replace_range_column");
        String start = (String) properties.get("replace_range_start");
        String end = (String) properties.get("replace_range_end");
        if (name == null && start == null && end == null) {
            return Optional.empty();
        }
        if (name == null || start == null || end == null) {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "replace_range_column, replace_range_start and replace_range_end must be specified together");
        }
        DuckLakeColumnHandle column = columnHandles(table).stream().filter(value -> value.name().equalsIgnoreCase(name)).findFirst()
                .orElseThrow(() -> new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "Replacement range column does not exist: " + name));
        return Optional.of(new DuckLakeReplacementRange(column, start, end));
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return switch (tableExecuteHandle) {
            case DuckLakeAddFilesHandle handle -> addFiles(session, handle);
            case DuckLakePublishAsHandle handle -> publishAs(handle);
            default -> throw new IllegalArgumentException("Unknown table procedure handle: " + tableExecuteHandle);
        };
    }

    private Map<String, Long> addFiles(ConnectorSession session, DuckLakeAddFilesHandle handle)
    {
        DuckLakeTableHandle table = handle.table();
        String fingerprint = DuckLakeFileImporter.fingerprint(handle);
        String receiptKey = "trino_import_" + handle.importId();
        // A retry of an applied import is recognized before anything is read, so it succeeds even
        // when the schema, the table contents or the files have changed since.
        Optional<String> receipt = metastore.tag(table.snapshotId(), table.tableId(), receiptKey);
        if (receipt.isPresent()) {
            verifyReceipt(receipt.orElseThrow(), fingerprint);
            return ImmutableMap.of("added_data_files", 0L);
        }
        if (handle.replace() || handle.replacementRange().isPresent()) {
            verifyNoInlinedData(table, "replacing rows");
        }
        List<DuckLakeWriteColumn> columns = DuckLakeColumns.fromCatalog(metastore.columns(table.snapshotId(), table.tableId()));
        DuckLakeWriteTarget target = new DuckLakeWriteTarget(table.schemaTableName(), table.tableId(), table.tableLocation(), columns, writePartitioningOf(table));
        List<DuckLakeFileImporter.ImportedFile> imported = DuckLakeFileImporter.readFiles(
                fileSystemFactory.create(session), dataPath, handle, columns, target.partitioning(), parquetReaderOptions, fileFormatDataSourceStats, addFilesMaxConcurrentFileReads);
        handle.replacementRange().ifPresent(range -> range.validateImportedFiles(imported));
        Optional<DuckLakeRangeReplacement.Plan> rangePlan = handle.replacementRange().map(range -> rangeReplacement.plan(session, table, range));
        List<RewrittenDeletes> rangeDeletes = rangePlan.map(plan -> rewriteDeleteFiles(session, target, plan.positions())).orElse(ImmutableList.of());
        Optional<Long> added = commit(commit -> {
            DuckLakeCommit.TableIdentity current = commit.findTable(table.schemaName(), table.tableName())
                    .orElseThrow(() -> new TrinoException(StandardErrorCode.TABLE_NOT_FOUND, "Import target no longer exists"));
            if (current.tableId() != table.tableId()) {
                throw new DuckLakeCommit.ConcurrentModificationFailure("Import target was replaced while reading files");
            }
            Optional<String> concurrentReceipt = commit.tableTag(table.tableId(), receiptKey);
            if (concurrentReceipt.isPresent()) {
                verifyReceipt(concurrentReceipt.orElseThrow(), fingerprint);
                return Optional.empty();
            }
            verifyFilesNotRecorded(commit, table, handle.files());
            if (handle.replace()) {
                removeAllRows(commit, table.tableId());
                commit.recordDelete(table.tableId());
            }
            rangePlan.ifPresent(plan -> {
                Set<Long> changed = ImmutableSet.<Long>builder().addAll(plan.wholeFiles()).addAll(plan.positions().keySet()).build();
                commit.verifyDataFilesUnchanged(table.tableId(), changed);
                commit.verifyDeleteFilesUnchanged(table.tableId(), plan.positions().keySet(), rangeDeletes.stream()
                        .flatMap(value -> value.replacedDeleteFileId().stream()).collect(toImmutableSet()));
                removeDataFiles(commit, table.tableId(), ImmutableList.copyOf(plan.wholeFiles()));
                applyDeletes(commit, target, rangeDeletes);
                // A replacement depends on every row in the interval, including the absence of
                // rows in an empty interval. Record both sides even for a delete-only batch.
                commit.recordDelete(table.tableId());
            });
            Map<String, Long> mappings = new LinkedHashMap<>();
            Map<List<DuckLakeCommit.NameMappingColumn>, Long> mappingIds = new LinkedHashMap<>();
            for (DuckLakeFileImporter.ImportedFile file : imported) {
                long mappingId = mappingIds.computeIfAbsent(file.mapping(), mapping -> {
                    long id = commit.allocateFileId();
                    commit.insertNameMapping(id, table.tableId(), mapping);
                    return id;
                });
                mappings.put(file.file().path(), mappingId);
            }
            addDataFiles(commit, target, imported.stream().map(DuckLakeFileImporter.ImportedFile::file).collect(toImmutableList()), mappings);
            commit.setTableTag(table.tableId(), receiptKey, Optional.of(fingerprint));
            commit.recordInsert(table.tableId());
            return Optional.of((long) imported.size());
        });
        if (added.isEmpty()) {
            // a concurrent retry applied the import first, so nothing references these files
            deleteUnreferencedDeleteFiles(session, target, rangeDeletes);
            return ImmutableMap.of("added_data_files", 0L);
        }
        return ImmutableMap.of("added_data_files", added.orElseThrow());
    }

    private static void verifyReceipt(String receipt, String fingerprint)
    {
        if (!receipt.equals(fingerprint)) {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "import_id was already used with different files or options");
        }
    }

    /**
     * Fails when a file to register is recorded anywhere in the catalog, in any snapshot and for
     * any table, or waits to be deleted, or lies in the directory of another table.
     * <p>
     * DuckDB deletes a file once the snapshots of a record ending it expire, even while another
     * record still references it, so a file recorded twice is eventually lost. A file in the
     * directory of another table holds rows of that table, which the caller may not be allowed to
     * read.
     */
    private void verifyFilesNotRecorded(DuckLakeCommit commit, DuckLakeTableHandle table, List<String> files)
    {
        ListMultimap<Long, String> tableLocations = ArrayListMultimap.create();
        Map<String, Long> visibleTableDirectories = new HashMap<>();
        for (DuckLakeCommit.TableLocation location : commit.tableLocations()) {
            String schemaLocation = PathResolver.resolve(dataPath, location.schemaPath(), location.schemaPathIsRelative());
            String tableLocation = PathResolver.resolve(schemaLocation, location.path(), location.pathIsRelative());
            tableLocations.put(location.tableId(), tableLocation);
            if (location.visible()) {
                visibleTableDirectories.put(canonicalDirectory(tableLocation), location.tableId());
            }
        }
        Map<String, String> filesByLocation = new HashMap<>();
        for (String file : files) {
            String location = canonicalFileLocation(file);
            filesByLocation.put(location, file);
            // the table whose directory holds the file most closely owns it
            Optional<Long> owner = visibleTableDirectories.entrySet().stream()
                    .filter(directory -> location.startsWith(directory.getKey()))
                    .max(Comparator.comparingInt(directory -> directory.getKey().length()))
                    .map(Map.Entry::getValue);
            if (owner.isPresent() && owner.get() != table.tableId()) {
                throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "Imported files cannot be in the directory of another table: " + file);
            }
        }
        Set<String> fileNames = files.stream()
                .map(file -> file.substring(file.lastIndexOf('/') + 1))
                .collect(toImmutableSet());
        for (DuckLakeCommit.RecordedFile recorded : commit.recordedFiles(fileNames)) {
            List<String> bases = recorded.tableId().isPresent() ? tableLocations.get(recorded.tableId().orElseThrow()) : ImmutableList.of(dataPath);
            for (String base : bases) {
                String location;
                try {
                    location = canonicalFileLocation(PathResolver.resolve(base, recorded.path(), recorded.pathIsRelative()));
                }
                catch (IllegalArgumentException _) {
                    // a location this connector cannot parse is not one of the files to register
                    continue;
                }
                String file = filesByLocation.get(location);
                if (file != null) {
                    throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "File is already registered in the catalog, or was and has not been deleted yet: " + file);
                }
            }
        }
    }

    private void deleteUnreferencedDeleteFiles(ConnectorSession session, DuckLakeWriteTarget target, List<RewrittenDeletes> rewrittenDeletes)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        for (RewrittenDeletes rewritten : rewrittenDeletes) {
            if (rewritten.newDeleteFile().isEmpty()) {
                continue;
            }
            String location = PathResolver.resolve(target.tableLocation(), rewritten.newDeleteFile().orElseThrow().path(), true);
            try {
                fileSystem.deleteFile(Location.of(location));
            }
            catch (IOException e) {
                log.warn(e, "Could not delete unreferenced delete file %s", location);
            }
        }
    }

    /**
     * Renames the table to the target name and retires the table holding that name, if any, in one
     * snapshot, so that the name resolves to the old table or to the new one and never to nothing.
     * The replaced table is retired as {@code DROP TABLE} retires it, so a reader of an earlier
     * snapshot still reads it, from files that stay in place until those snapshots expire.
     * <p>
     * Both names are resolved again against the snapshot the commit lands on, and the statement
     * fails rather than rename or drop a table other than the ones it was planned and authorized
     * for.
     */
    private Map<String, Long> publishAs(DuckLakePublishAsHandle handle)
    {
        DuckLakeTableHandle table = handle.table();
        SchemaTableName target = handle.target();
        commit(commit -> {
            DuckLakeCommit.TableIdentity published = commit.findTable(table.schemaName(), table.tableName())
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Table %s was dropped or renamed while publishing it".formatted(table.schemaTableName())));
            if (published.tableId() != table.tableId()) {
                throw new DuckLakeCommit.ConcurrentModificationFailure("Table %s was replaced while publishing it".formatted(table.schemaTableName()));
            }
            if (commit.findView(target.getSchemaName(), target.getTableName()).isPresent()) {
                throw new DuckLakeCommit.ConcurrentModificationFailure("View %s was created while publishing as it".formatted(target));
            }
            OptionalLong replacedTableId = commit.findTable(target.getSchemaName(), target.getTableName())
                    .map(replaced -> OptionalLong.of(replaced.tableId()))
                    .orElseGet(OptionalLong::empty);
            if (!replacedTableId.equals(handle.replacedTableId())) {
                throw new DuckLakeCommit.ConcurrentModificationFailure("Table %s was created, dropped or replaced while publishing as it".formatted(target));
            }
            // dropped first, matching the order DuckDB records a DROP TABLE and a rename committed together
            replacedTableId.ifPresent(replaced -> endTable(commit, replaced));
            renameTable(commit, published, published.schemaId(), target);
            return null;
        });
        return ImmutableMap.of("replaced_tables", handle.replacedTableId().isPresent() ? 1L : 0L);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        SchemaTableName tableName = tableMetadata.getTable();
        if (replace) {
            // The name still resolves to the table being replaced, which keeps its rows until the
            // statement finishes. Creating the new table now would make the name resolve to an
            // empty table in the meantime, so the whole statement is left to a single commit in
            // finishCreateTable.
            //
            // A name a view holds is one no table can take, and the commit will say so. It is
            // worth saying here too, because everything the statement selects is written before
            // that commit runs, and none of it could be used.
            if (metastore.findView(snapshotId(), tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + tableName);
            }
            List<DuckLakeWriteColumn> columns = DuckLakeColumns.assignColumnIds(tableMetadata.getColumns());
            return new DuckLakeReplaceTarget(
                    tableName,
                    newTableLocation(tableName),
                    columns,
                    resolvePartitioning(columns, DuckLakeTableProperties.getPartitioning(tableMetadata.getProperties())),
                    columnComments(tableMetadata.getColumns(), columns),
                    tableMetadata.getComment());
        }
        // the table is created in its own snapshot, so that the rows written afterwards land in a
        // table that already exists, exactly as an insert into an existing table would
        return commit(commit -> {
            NewTable table = createTable(commit, tableMetadata, SaveMode.FAIL);
            return new DuckLakeWriteTarget(tableName, table.tableId(), table.location(), table.columns(), table.partitioning());
        });
    }

    /**
     * Where a table of the given name keeps its data files. The directory is named after the table
     * itself, so it can be resolved before the table exists — which the replacing form of
     * {@code CREATE TABLE AS} needs, because it writes its rows before the commit that creates the
     * table it writes them into.
     */
    private String newTableLocation(SchemaTableName tableName)
    {
        DuckLakeSchemaEntry schema = metastore.listSchemas(snapshotId()).stream()
                .filter(entry -> entry.schemaName().equalsIgnoreCase(tableName.getSchemaName()))
                .findFirst()
                .orElseThrow(() -> new SchemaNotFoundException(tableName.getSchemaName()));
        String schemaLocation = PathResolver.resolve(dataPath, schema.path(), schema.pathIsRelative());
        return PathResolver.resolve(schemaLocation, directoryName(tableName.getTableName()), true);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        if (tableHandle instanceof DuckLakeReplaceTarget replaceTarget) {
            return finishReplaceTable(replaceTarget, fragments);
        }
        return finishWrite((DuckLakeWriteTarget) tableHandle, fragments);
    }

    /**
     * Ends the table being replaced, creates the one that takes its name, and registers the rows
     * written for it — all in one commit, so a reader sees the rows the table held before or the
     * rows it holds now, and never a table that exists but holds nothing.
     * <p>
     * The new table is a table of its own, with a new identifier, as it is in DuckDB. Nothing of
     * the table it replaces carries over: a comment or a partitioning scheme the new definition
     * does not state is gone with the old table.
     */
    private Optional<ConnectorOutputMetadata> finishReplaceTable(DuckLakeReplaceTarget replaceTarget, Collection<Slice> fragments)
    {
        List<DuckLakeDataFile> dataFiles = parseDataFiles(fragments);
        SchemaTableName tableName = replaceTarget.tableName();
        commit(commit -> {
            DuckLakeCommit.SchemaIdentity schema = commit.findSchema(tableName.getSchemaName())
                    .orElseThrow(() -> new SchemaNotFoundException(tableName.getSchemaName()));
            if (commit.findView(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + tableName);
            }
            commit.findTable(tableName.getSchemaName(), tableName.getTableName())
                    .ifPresent(existing -> endTable(commit, existing.tableId()));
            NewTable table = insertTable(
                    commit,
                    schema,
                    tableName,
                    replaceTarget.columns(),
                    replaceTarget.columnComments(),
                    replaceTarget.comment(),
                    replaceTarget.partitionFields());
            if (!dataFiles.isEmpty()) {
                DuckLakeWriteTarget target = new DuckLakeWriteTarget(tableName, table.tableId(), table.location(), table.columns(), table.partitioning());
                addDataFiles(commit, target, dataFiles);
                commit.recordInsert(table.tableId());
            }
            return null;
        });
        return Optional.empty();
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
        List<DuckLakeDataFile> dataFiles = parseDataFiles(fragments);
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
     * The data files the workers report having written, one per fragment.
     */
    private List<DuckLakeDataFile> parseDataFiles(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> dataFileCodec.fromJson(fragment.getBytes()))
                .collect(toImmutableList());
    }

    /**
     * Adds data files to a table, numbering their rows from the table's next row identifier and
     * updating the statistics the catalog keeps for the whole table.
     */
    private void addDataFiles(DuckLakeCommit commit, DuckLakeWriteTarget target, List<DuckLakeDataFile> dataFiles)
    {
        addDataFiles(commit, target, dataFiles, Map.of());
    }

    private void addDataFiles(DuckLakeCommit commit, DuckLakeWriteTarget target, List<DuckLakeDataFile> dataFiles, Map<String, Long> mappings)
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

        ImmutableList.Builder<DuckLakeCommit.NewDataFile> newFiles = ImmutableList.builderWithExpectedSize(dataFiles.size());
        for (DuckLakeDataFile dataFile : dataFiles) {
            long dataFileId = commit.allocateFileId();
            newFiles.add(new DuckLakeCommit.NewDataFile(
                    dataFileId,
                    new DuckLakeCommit.DataFileRow(
                            importedFilePath(target.tableLocation(), dataFile.path(), mappings.containsKey(dataFile.path())),
                            dataFile.recordCount(),
                            dataFile.fileSizeBytes(),
                            dataFile.footerSize(),
                            nextRowId,
                            target.partitioning().filter(partitioning -> dataFile.partitionValues().size() == partitioning.fields().size()).map(DuckLakePartitioning::partitionId).map(OptionalLong::of).orElse(OptionalLong.empty()),
                            !mappings.containsKey(dataFile.path()) || Location.of(dataFile.path()).scheme().orElse("").equals("local"),
                            mappings.containsKey(dataFile.path()) ? OptionalLong.of(mappings.get(dataFile.path())) : OptionalLong.empty()),
                    dataFile.partitionValues(),
                    dataFile.columnStatistics()));
            nextRowId = Math.addExact(nextRowId, dataFile.recordCount());
            recordCount = Math.addExact(recordCount, dataFile.recordCount());
            fileSizeBytes = Math.addExact(fileSizeBytes, dataFile.fileSizeBytes());

            for (DuckLakeFileColumnStatsRow fileStats : dataFile.columnStatistics()) {
                DuckLakeWriteColumn column = columnsById.get(fileStats.columnId());
                if (column != null) {
                    mergedStats.merge(
                            fileStats.columnId(),
                            toTableColumnStats(column, fileStats),
                            (existing, added) -> mergeColumnStats(column.type(), existing, added));
                }
            }
        }

        commit.insertDataFiles(target.tableId(), newFiles.build());
        commit.writeTableStats(target.tableId(), new DuckLakeCommit.TableStatsRow(recordCount, nextRowId, fileSizeBytes));
        commit.writeTableColumnStats(target.tableId(), ImmutableList.copyOf(mergedStats.values()));
    }

    private static String importedFilePath(String tableLocation, String file, boolean imported)
    {
        // Local locations are relative to the configured filesystem root, which DuckDB does not
        // know. Store a table-relative path so both engines resolve the same physical file.
        if (imported && Location.of(file).scheme().orElse("").equals("local")) {
            return Path.of(Location.of(tableLocation).path()).relativize(Path.of(Location.of(file).path())).toString();
        }
        return file;
    }

    private static String canonicalFileLocation(String file)
    {
        if (Location.of(file).scheme().orElse("").equals("local")) {
            return "local:///" + Path.of(Location.of(file).path()).normalize();
        }
        return file;
    }

    private static String canonicalDirectory(String directory)
    {
        String location = canonicalFileLocation(directory);
        return location.endsWith("/") ? location : location + "/";
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
        // DELETE, UPDATE and MERGE all come through here
        verifyNoInlinedData(handle, "modifying rows");
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
    public Optional<ConnectorOutputMetadata> finishMerge(
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
            return Optional.empty();
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
        return Optional.empty();
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
        verifyNoInlinedData(tableHandle, "deleting rows");
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
        verifyNoInlinedData(tableHandle, "deleting rows");
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
        verifyNoInlinedData(handle, "truncating it");
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
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, SaveMode saveMode)
    {
        if (!viewProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support view properties");
        }
        // A DEFINER view records which user its query runs as, and Trino reads that back here as a
        // permission grant. The definition lives in the catalog database, which the owner of the
        // catalog can write to directly, so a stored owner is not trustworthy input. Reject the
        // request rather than store a grant this connector cannot vouch for.
        if (!definition.isRunAsInvoker()) {
            throw new TrinoException(NOT_SUPPORTED, "Run-as-definer views are not supported for DuckLake catalogs: " + viewName + ". Add SECURITY INVOKER.");
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
                if (saveMode == SaveMode.IGNORE) {
                    return null;
                }
                if (saveMode == SaveMode.FAIL) {
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
                    storedViewSql(definition),
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
                .flatMap(view -> toViewDefinition(view, true));
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (DuckLakeViewEntry view : metastore.listViews(snapshotId(), schemaName)) {
            SchemaTableName viewName = new SchemaTableName(view.schemaName(), view.viewName());
            try {
                toViewDefinition(view).ifPresent(definition -> views.put(viewName, definition));
            }
            catch (TrinoException e) {
                // A listing covers every view in the schema, so one unreadable definition is left
                // out rather than failing the call for the rest of them.
                log.warn(e, "Skipping view %s with an unreadable definition", viewName);
            }
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
     * <p>
     * The definition is read as a run-as-invoker view whatever it records. The owner and the run
     * mode are stored in the catalog database, which the owner of the catalog can write to, so a
     * stored owner is not a grant this connector can rely on. Coercing on read also covers a view
     * written before this rule, and a definition edited in place to name a different user.
     */
    private Optional<ConnectorViewDefinition> toViewDefinition(DuckLakeViewEntry view)
    {
        return toViewDefinition(view, false);
    }

    /**
     * @param reportCoercion logs a definition that is read as run-as-invoker although it records
     *         a different run mode. Reading is where the stored owner is refused, so this log is the one
     *         piece of evidence of a rewritten tag that the owner of the catalog cannot delete. Listing
     *         every view of a schema is frequent, so only the single-view path asks for it.
     */
    private Optional<ConnectorViewDefinition> toViewDefinition(DuckLakeViewEntry view, boolean reportCoercion)
    {
        if (!view.dialect().equalsIgnoreCase(TRINO_VIEW_DIALECT)) {
            return Optional.empty();
        }
        return metastore.tag(snapshotId(), view.viewId(), VIEW_DEFINITION_TAG_KEY)
                .map(definition -> decodeViewDefinition(view, definition))
                .map(definition -> runAsInvoker(view, definition, reportCoercion));
    }

    /**
     * Decodes a stored definition, reporting a tag that is not readable as a metadata error of this
     * connector. The tag is a value the catalog holds rather than one the connector wrote last, so
     * it can be unreadable, and a bare codec exception would surface as an internal error.
     */
    private ConnectorViewDefinition decodeViewDefinition(DuckLakeViewEntry view, String definition)
    {
        try {
            return viewDefinitionCodec.fromJson(definition);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Unreadable definition for view %s.%s".formatted(view.schemaName(), view.viewName()), e);
        }
    }

    private static ConnectorViewDefinition runAsInvoker(DuckLakeViewEntry view, ConnectorViewDefinition definition, boolean reportCoercion)
    {
        if (definition.isRunAsInvoker()) {
            return definition;
        }
        if (reportCoercion) {
            log.warn("Reading view %s.%s as SECURITY INVOKER although its stored definition runs as %s",
                    view.schemaName(),
                    view.viewName(),
                    definition.getOwner().orElse("<no owner>"));
        }
        return new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                definition.getComment(),
                Optional.empty(),
                true,
                definition.getPath());
    }

    private void requireViewSupport()
    {
        if (!metastore.viewsSupported()) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "This DuckLake catalog was created before views were added to the format");
        }
    }

    /**
     * The view's query in the form DuckLake stores it, naming the catalog it reads by the
     * placeholder the format defines rather than by the name this catalog is mounted under here.
     * Trino keeps reading the query from the tag beside the view, so the placeholder is written
     * for the engines that read the column instead — DuckDB attaches the catalog under a name of
     * its own, and a literal one would leave the view unbindable there.
     */
    private static String storedViewSql(ConnectorViewDefinition definition)
    {
        return definition.getCatalog()
                .map(catalog -> withCatalogPlaceholder(definition.getOriginalSql(), catalog))
                .orElseGet(definition::getOriginalSql);
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
     * Resolves the partition keys of a table against its columns, checking that each transform can
     * be applied to the column it reads.
     * <p>
     * This writes nothing, so a statement that has to settle how it lays its rows out before the
     * commit that records the layout can still do so.
     */
    private static List<DuckLakePartitioning.Field> resolvePartitioning(List<DuckLakeWriteColumn> columns, List<String> partitionKeys)
    {
        ImmutableList.Builder<DuckLakePartitioning.Field> fields = ImmutableList.builder();
        for (String partitionKey : partitionKeys) {
            DuckLakeTableProperties.PartitionKey key = DuckLakeTableProperties.parsePartitionKey(partitionKey);
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
            fields.add(new DuckLakePartitioning.Field(channel, column.columnId(), column.name(), key.transform()));
        }
        return fields.build();
    }

    /**
     * Records the partitioning of a table being created or altered, which the data files written
     * from now on are filed under.
     */
    private static Optional<DuckLakePartitioning> insertPartitioning(DuckLakeCommit commit, long tableId, List<DuckLakePartitioning.Field> fields)
    {
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<DuckLakePartitionColumn> catalogColumns = ImmutableList.builder();
        for (int index = 0; index < fields.size(); index++) {
            DuckLakePartitioning.Field field = fields.get(index);
            catalogColumns.add(new DuckLakePartitionColumn(index, field.columnId(), field.transform()));
        }
        return Optional.of(new DuckLakePartitioning(commit.insertPartitioning(tableId, catalogColumns.build()), fields));
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        verifyNoInlinedRows(handle, "changing its partitioning");
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
            insertPartitioning(commit, handle.tableId(), resolvePartitioning(columns, partitionKeys));
            commit.recordAlteredTable(handle.tableId());
            return null;
        });
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        Set<DuckLakeColumnHandle> columns = assignments.values().stream()
                .map(DuckLakeColumnHandle.class::cast)
                .collect(toImmutableSet());
        if (handle.projectedColumns().equals(Optional.of(columns))) {
            return Optional.empty();
        }
        List<Assignment> newAssignments = assignments.entrySet().stream()
                .map(entry -> new Assignment(entry.getKey(), entry.getValue(), ((DuckLakeColumnHandle) entry.getValue()).type()))
                .collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(handle.withProjectedColumns(columns), projections, newAssignments, false));
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
            // Inlined rows carry no partition values to prune by, so while the table holds any, no
            // predicate is enforced and the engine filters every row.
            Map<Long, DuckLakePartitionColumn> partitionColumns = inlinedSummary(handle).hasRows() ? ImmutableMap.of() : enforceablePartitionColumns(handle);
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> enforceableDomains = ImmutableMap.builder();
            ImmutableMap.Builder<DuckLakeColumnHandle, Domain> unenforceableDomains = ImmutableMap.builder();
            for (Map.Entry<DuckLakeColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
                DuckLakeColumnHandle column = entry.getKey();
                DuckLakePartitionColumn partitionColumn = partitionColumns.get(column.columnId());
                if (partitionColumn != null && isEnforceableType(column.type())
                        && (!isTemporalType(column.type()) || everyPartitionValueDecidesTheColumn(handle, column, partitionColumn))) {
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
                handle.rowCount(),
                handle.projectedColumns());

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
     * The values are read through the same code as {@link DuckLakeSplitManager#getSplits}, at the
     * same snapshot, so a predicate enforced here is a predicate splits can be pruned by there.
     * The query behind it costs one row per distinct value of the key, not one per data file, so
     * a table of many files filed by few days is answered by few rows.
     */
    private boolean everyPartitionValueDecidesTheColumn(
            DuckLakeTableHandle handle,
            DuckLakeColumnHandle column,
            DuckLakePartitionColumn partitionColumn)
    {
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(partitionColumn);
        return metastore.distinctPartitionValues(handle.snapshotId(), handle.tableId(), partitionColumn.partitionKeyIndex()).stream()
                .allMatch(partitionValues -> PartitionTransforms.partitionDomain(column, transforms, partitionValues).isPresent());
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
     * Whether the table holds anything stored inline in the catalog database in the handle's
     * snapshot, read once per table and snapshot.
     */
    private DuckLakeInlinedSummary inlinedSummary(DuckLakeTableHandle handle)
    {
        return inlinedSummaries.computeIfAbsent(
                new TableSnapshot(handle.tableId(), handle.snapshotId()),
                key -> metastore.inlinedSummary(key.snapshotId(), key.tableId()));
    }

    private record TableSnapshot(long tableId, long snapshotId) {}

    /**
     * Refuses a statement that would change the rows of a table holding rows or deletions stored
     * inline in the catalog database. Such a statement would neither see nor change them: a
     * DELETE would leave the inlined rows in place, and an UPDATE would rewrite only the rows of
     * data files.
     */
    private void verifyNoInlinedData(DuckLakeTableHandle handle, String operation)
    {
        DuckLakeInlinedSummary summary = inlinedSummary(handle);
        if (summary.hasRows()) {
            throw inlinedDataNotSupported(handle, "inlined data", operation);
        }
        if (summary.hasFileDeletions()) {
            throw inlinedDataNotSupported(handle, "rows deleted inline", operation);
        }
    }

    /**
     * Refuses a change to the columns or layout of a table holding inlined rows. DuckDB keeps the
     * inlined rows of every schema version of a table in a table of their own, which it creates as
     * it changes the schema, and a change made here would not create one.
     */
    private void verifyNoInlinedRows(DuckLakeTableHandle handle, String operation)
    {
        if (inlinedSummary(handle).hasRows()) {
            throw inlinedDataNotSupported(handle, "inlined data", operation);
        }
    }

    private static TrinoException inlinedDataNotSupported(DuckLakeTableHandle handle, String what, String operation)
    {
        return new TrinoException(
                DUCKLAKE_UNSUPPORTED_FEATURE,
                "Table %s has %s stored in the DuckLake catalog database, and %s is not supported for such a table. Flush inlined data to Parquet with DuckDB first (CALL ducklake_flush_inlined_data(...))"
                        .formatted(handle.schemaTableName(), what, operation));
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
