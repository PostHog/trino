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
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeColumns;
import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
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
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
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
import static io.trino.plugin.ducklake.util.PartitionTransforms.IDENTITY_TRANSFORM;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
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
    private static final String COMMENT_TAG_KEY = "comment";

    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;
    private final Map<Long, DuckLakeRowCount> rowCounts = new ConcurrentHashMap<>();
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec;
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
            TrinoFileSystemFactory fileSystemFactory,
            DuckLakeWriterFactory writerFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.mergeFragmentCodec = requireNonNull(mergeFragmentCodec, "mergeFragmentCodec is null");
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();
        commit(commit -> {
            NewTable table = createTable(commit, tableName, tableMetadata.getColumns(), saveMode);
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
    private NewTable createTable(DuckLakeCommit commit, SchemaTableName tableName, List<ColumnMetadata> columns, SaveMode saveMode)
    {
        DuckLakeCommit.SchemaIdentity schema = commit.findSchema(tableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(tableName.getSchemaName()));
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
        commit.recordCreatedTable(tableName.getSchemaName(), tableName.getTableName(), tableId);

        String schemaLocation = PathResolver.resolve(dataPath, schema.path(), true);
        return new NewTable(tableId, writeColumns, PathResolver.resolve(schemaLocation, tablePath, true));
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
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName tableName = tableMetadata.getTable();
        // the table is created in its own snapshot, so that the rows written afterwards land in a
        // table that already exists, exactly as an insert into an existing table would
        return commit(commit -> {
            NewTable table = createTable(commit, tableName, tableMetadata.getColumns(), SaveMode.FAIL);
            tableMetadata.getComment().ifPresent(comment -> commit.setTableTag(table.tableId(), COMMENT_TAG_KEY, Optional.of(comment)));
            return new DuckLakeWriteTarget(tableName, table.tableId(), table.location(), table.columns(), Optional.empty());
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
                partitioningOf(handle));
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
                        partitioningOf(handle)));
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
        // only a delete of every row can be answered by dropping files; anything narrower needs
        // the positions of the rows that go, which the engine produces through a merge
        if (tableHandle.enforcedConstraint().isAll() && tableHandle.unenforcedConstraint().isAll()) {
            return Optional.of(tableHandle);
        }
        return Optional.empty();
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DuckLakeTableHandle tableHandle = (DuckLakeTableHandle) handle;
        return OptionalLong.of(commit(commit -> {
            long removed = removeAllRows(commit, tableHandle.tableId());
            commit.recordDelete(tableHandle.tableId());
            return removed;
        }));
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
     * The partitioning to write a table with, taken from the scheme recorded in the catalog.
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
            DuckLakeWritePartitioner.validateTransform(partitionColumn.transform(), column.name(), column.type());
            fields.add(new DuckLakePartitioning.Field(channel, column.columnId(), column.name(), partitionColumn.transform()));
        }
        return Optional.of(new DuckLakePartitioning(partitionInfo.get().partitionId(), fields.build()));
    }

    /**
     * The directory a schema or table stores its files in. DuckDB names it after the object, which
     * keeps the layout readable; names that cannot be a single path segment fall back to a
     * generated one.
     */
    private static String directoryName(String name)
    {
        if (name.isEmpty() || name.equals(".") || name.equals("..") || name.contains("/") || name.contains("\\")) {
            return java.util.UUID.randomUUID() + "/";
        }
        return name + "/";
    }

    private record NewTable(long tableId, List<DuckLakeWriteColumn> columns, String location) {}

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
