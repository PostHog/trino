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
package io.trino.plugin.ducklake.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.ducklake.DuckLakeConfig;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_FAILED;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Reads DuckLake catalog metadata from the {@code ducklake_*} tables in a PostgreSQL database.
 */
public class JdbcDuckLakeMetastore
{
    private static final String VISIBLE = "begin_snapshot <= :snapshot AND (end_snapshot IS NULL OR end_snapshot > :snapshot)";
    private static final String UNDEFINED_TABLE_SQL_STATE = "42P01";
    private static final String SERIALIZATION_FAILURE_SQL_STATE = "40001";
    private static final String DEADLOCK_DETECTED_SQL_STATE = "40P01";
    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
    private static final int MAX_COMMIT_ATTEMPTS = 10;
    private static final long COMMIT_RETRY_BASE_DELAY_MILLIS = 20;
    private static final long MAX_COMMIT_RETRY_DELAY_MILLIS = 1000;

    private final Jdbi jdbi;
    private final String metadataSchema;

    private volatile Boolean dataFileHasPartialMax;
    private volatile Boolean inlinedDataTablesRegistryExists;
    private volatile Boolean nameMappingTableExists;
    private volatile Boolean viewTableExists;
    private volatile Boolean nameMappingHasIsPartition;

    @Inject
    public JdbcDuckLakeMetastore(org.jdbi.v3.core.ConnectionFactory connectionFactory, DuckLakeConfig config)
    {
        this.jdbi = Jdbi.create(requireNonNull(connectionFactory, "connectionFactory is null"));
        this.metadataSchema = config.getMetadataSchema();
    }

    public long currentSnapshotId()
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery("SELECT snapshot_id FROM " + table("ducklake_snapshot") + " ORDER BY snapshot_id DESC LIMIT 1")
                    .mapTo(Long.class)
                    .findOne()
                    .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "No snapshots found in DuckLake catalog"));
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Runs the action against a new snapshot and commits it atomically.
     * <p>
     * DuckLake orders all changes to a catalog on a single snapshot chain, so a commit conflicts
     * with any other commit that started from the same snapshot. Conflicts are detected by the
     * database rather than avoided by locking: the transaction runs at {@code SERIALIZABLE}, and
     * the snapshot table's primary key rejects a second commit claiming the same snapshot
     * identifier. Either way the action is discarded and replayed against the newer state, which
     * is safe because it only reads catalog rows and writes them through this commit — the data
     * files it registers were written before the commit began and are unaffected by a replay.
     */
    public <T> T commit(DuckLakeCommitAction<T> action)
    {
        RuntimeException conflict = null;
        for (int attempt = 0; attempt < MAX_COMMIT_ATTEMPTS; attempt++) {
            try {
                return jdbi.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
                    DuckLakeCommit commit = new DuckLakeCommit(handle, metadataSchema, snapshotState(handle));
                    T result = action.run(commit);
                    commit.writeSnapshot();
                    return result;
                });
            }
            catch (JdbiException e) {
                if (!isRetriableConflict(e)) {
                    throw metastoreError(e);
                }
                conflict = e;
            }
            catch (DuckLakeCommit.ConcurrentModificationFailure e) {
                throw e;
            }
            sleepBeforeRetry(attempt);
        }
        throw new TrinoException(DUCKLAKE_COMMIT_FAILED, "Failed to commit to the DuckLake catalog after %s attempts because of concurrent updates".formatted(MAX_COMMIT_ATTEMPTS), conflict);
    }

    private DuckLakeCommit.SnapshotState snapshotState(Handle handle)
    {
        return handle.createQuery(
                        """
                        SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
                        FROM %s ORDER BY snapshot_id DESC LIMIT 1""".formatted(table("ducklake_snapshot")))
                .map((rs, _) -> new DuckLakeCommit.SnapshotState(
                        rs.getLong("snapshot_id"),
                        rs.getLong("schema_version"),
                        rs.getLong("next_catalog_id"),
                        rs.getLong("next_file_id")))
                .findOne()
                .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "No snapshots found in DuckLake catalog"));
    }

    /**
     * Recognizes the two ways a concurrent commit surfaces: a serialization failure raised by the
     * database, and a unique violation from two commits claiming the same snapshot identifier.
     */
    private static boolean isRetriableConflict(Throwable throwable)
    {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException) {
                String state = sqlException.getSQLState();
                if (SERIALIZATION_FAILURE_SQL_STATE.equals(state) || DEADLOCK_DETECTED_SQL_STATE.equals(state) || UNIQUE_VIOLATION_SQL_STATE.equals(state)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void sleepBeforeRetry(int attempt)
    {
        try {
            Thread.sleep(Math.min(COMMIT_RETRY_BASE_DELAY_MILLIS << attempt, MAX_COMMIT_RETRY_DELAY_MILLIS));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(DUCKLAKE_COMMIT_FAILED, "Interrupted while retrying a DuckLake commit", e);
        }
    }

    public Optional<String> formatVersion()
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery("SELECT value FROM " + table("ducklake_metadata") + " WHERE key = 'version'")
                    .mapTo(String.class)
                    .findOne();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeSchemaEntry> listSchemas(long snapshotId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT schema_id, schema_name, path, path_is_relative
                            FROM %s
                            WHERE %s
                            ORDER BY schema_name""".formatted(table("ducklake_schema"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .map((rs, _) -> new DuckLakeSchemaEntry(
                            rs.getLong("schema_id"),
                            rs.getString("schema_name"),
                            stringOrEmpty(rs, "path"),
                            rs.getBoolean("path_is_relative")))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeTableEntry> listTables(long snapshotId, Optional<String> schemaName)
    {
        try (Handle handle = jdbi.open()) {
            String sql =
                    """
                    SELECT t.table_id, t.schema_id, s.schema_name, t.table_name, t.path, t.path_is_relative,
                        s.path AS schema_path, s.path_is_relative AS schema_path_is_relative
                    FROM %s t
                    JOIN %s s ON t.schema_id = s.schema_id
                    WHERE %s AND %s
                    """.formatted(table("ducklake_table"), table("ducklake_schema"), visible("t"), visible("s"));
            if (schemaName.isPresent()) {
                sql += " AND lower(s.schema_name) = :schemaName";
            }
            sql += " ORDER BY s.schema_name, t.table_name";
            var query = handle.createQuery(sql).bind("snapshot", snapshotId);
            schemaName.ifPresent(name -> query.bind("schemaName", name.toLowerCase(ENGLISH)));
            return query.map(JdbcDuckLakeMetastore::tableEntry).list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Finds the table matching the given Trino (lowercase) schema and table name. Names are
     * matched case-insensitively because Trino lowercases unquoted identifiers while DuckLake
     * preserves the case the table was created with.
     */
    public Optional<DuckLakeTableEntry> findTable(long snapshotId, String schemaName, String tableName)
    {
        List<DuckLakeTableEntry> matches;
        try (Handle handle = jdbi.open()) {
            matches = handle.createQuery(
                            """
                            SELECT t.table_id, t.schema_id, s.schema_name, t.table_name, t.path, t.path_is_relative,
                                s.path AS schema_path, s.path_is_relative AS schema_path_is_relative
                            FROM %s t
                            JOIN %s s ON t.schema_id = s.schema_id
                            WHERE %s AND %s AND lower(s.schema_name) = :schemaName AND lower(t.table_name) = :tableName
                            """.formatted(table("ducklake_table"), table("ducklake_schema"), visible("t"), visible("s")))
                    .bind("snapshot", snapshotId)
                    .bind("schemaName", schemaName.toLowerCase(ENGLISH))
                    .bind("tableName", tableName.toLowerCase(ENGLISH))
                    .map(JdbcDuckLakeMetastore::tableEntry)
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
        if (matches.size() > 1) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Ambiguous table name '%s.%s': multiple tables differ only in case".formatted(schemaName, tableName));
        }
        return matches.stream().findFirst();
    }

    public List<DuckLakeColumnEntry> columns(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT column_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column
                            FROM %s
                            WHERE table_id = :tableId AND %s
                            ORDER BY parent_column NULLS FIRST, column_order""".formatted(table("ducklake_column"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeColumnEntry(
                            rs.getLong("column_id"),
                            rs.getLong("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            optionalLong(rs, "parent_column"),
                            rs.getBoolean("nulls_allowed"),
                            Optional.ofNullable(rs.getString("initial_default")),
                            Optional.ofNullable(rs.getString("default_value"))))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Returns the columns of every table visible at the snapshot, optionally restricted to a
     * single schema given by its Trino (lowercase) name, grouped by table. Columns of each table
     * are ordered like {@link #columns}, so nested types can be reconstructed.
     */
    public List<DuckLakeTableColumnsEntry> columnsForAllTables(long snapshotId, Optional<String> schemaName)
    {
        record ColumnRow(long tableId, String schemaName, String tableName, DuckLakeColumnEntry column) {}

        List<ColumnRow> rows;
        try (Handle handle = jdbi.open()) {
            String sql =
                    """
                    SELECT t.table_id, s.schema_name, t.table_name,
                        c.column_id, c.column_order, c.column_name, c.column_type, c.initial_default, c.default_value, c.nulls_allowed, c.parent_column
                    FROM %s c
                    JOIN %s t ON c.table_id = t.table_id
                    JOIN %s s ON t.schema_id = s.schema_id
                    WHERE %s AND %s AND %s
                    """.formatted(table("ducklake_column"), table("ducklake_table"), table("ducklake_schema"), visible("c"), visible("t"), visible("s"));
            if (schemaName.isPresent()) {
                sql += " AND lower(s.schema_name) = :schemaName";
            }
            sql += " ORDER BY s.schema_name, t.table_name, t.table_id, c.parent_column NULLS FIRST, c.column_order";
            var query = handle.createQuery(sql).bind("snapshot", snapshotId);
            schemaName.ifPresent(name -> query.bind("schemaName", name.toLowerCase(ENGLISH)));
            rows = query
                    .map((rs, _) -> new ColumnRow(
                            rs.getLong("table_id"),
                            rs.getString("schema_name"),
                            rs.getString("table_name"),
                            new DuckLakeColumnEntry(
                                    rs.getLong("column_id"),
                                    rs.getLong("column_order"),
                                    rs.getString("column_name"),
                                    rs.getString("column_type"),
                                    optionalLong(rs, "parent_column"),
                                    rs.getBoolean("nulls_allowed"),
                                    Optional.ofNullable(rs.getString("initial_default")),
                                    Optional.ofNullable(rs.getString("default_value")))))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }

        Map<Long, ColumnRow> firstRowByTableId = new LinkedHashMap<>();
        Map<Long, ImmutableList.Builder<DuckLakeColumnEntry>> columnsByTableId = new LinkedHashMap<>();
        for (ColumnRow row : rows) {
            firstRowByTableId.putIfAbsent(row.tableId(), row);
            columnsByTableId.computeIfAbsent(row.tableId(), _ -> ImmutableList.builder()).add(row.column());
        }
        return firstRowByTableId.values().stream()
                .map(row -> new DuckLakeTableColumnsEntry(
                        row.tableId(),
                        row.schemaName(),
                        row.tableName(),
                        columnsByTableId.get(row.tableId()).build()))
                .collect(toImmutableList());
    }

    public List<DuckLakeDataFileEntry> dataFiles(long snapshotId, long tableId)
    {
        String partialMaxColumn = "NULL AS partial_max";
        if (dataFileHasPartialMax()) {
            partialMaxColumn = "f.partial_max";
        }
        try (Handle handle = jdbi.open()) {
            Map<Long, DuckLakeDataFileEntry> filesById = new LinkedHashMap<>();
            Map<Long, Map<Integer, Optional<String>>> partitionValuesByFileId = new LinkedHashMap<>();
            handle.createQuery(
                            """
                            SELECT f.data_file_id, f.path, f.path_is_relative, f.file_format, f.record_count, f.file_size_bytes,
                                f.footer_size, f.row_id_start, f.partition_id, f.encryption_key, f.mapping_id, %s,
                                v.partition_key_index, v.partition_value
                            FROM %s f
                            LEFT JOIN %s v ON f.data_file_id = v.data_file_id AND f.table_id = v.table_id
                            WHERE f.table_id = :tableId AND %s
                            ORDER BY f.data_file_id, v.partition_key_index""".formatted(
                                    partialMaxColumn,
                                    table("ducklake_data_file"),
                                    table("ducklake_file_partition_value"),
                                    visible("f")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> {
                        long dataFileId = rs.getLong("data_file_id");
                        filesById.computeIfAbsent(dataFileId, _ -> dataFileEntry(rs, dataFileId));
                        int partitionKeyIndex = rs.getInt("partition_key_index");
                        if (!rs.wasNull()) {
                            partitionValuesByFileId
                                    .computeIfAbsent(dataFileId, _ -> new LinkedHashMap<>())
                                    .put(partitionKeyIndex, Optional.ofNullable(rs.getString("partition_value")));
                        }
                        return dataFileId;
                    })
                    .list();
            return filesById.values().stream()
                    .map(file -> withPartitionValues(file, partitionValuesByFileId.getOrDefault(file.dataFileId(), Map.of())))
                    .collect(toImmutableList());
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Returns the number of rows of a table in a snapshot, computed from the catalog alone. The
     * result also says whether that number is exactly what a scan of the table would return, which
     * is what lets a {@code count(*)} be answered from it. It is not exact when the table holds a
     * file the connector refuses to read, or a data file only partly visible in the snapshot; both
     * are conditions the split manager checks per file, and reporting them here keeps a
     * {@code count(*)} failing wherever a scan would fail.
     */
    public DuckLakeRowCount rowCount(long snapshotId, long tableId)
    {
        // Files the split manager rejects are counted rather than located, because the count only
        // decides whether to answer from the catalog at all; the scan that runs instead reports
        // which file it was.
        //
        // 1 = 0 rather than the false literal, which not every catalog database spells the same way
        String partiallyVisibleCondition = "1 = 0";
        if (dataFileHasPartialMax()) {
            partiallyVisibleCondition = "(f.partial_max IS NOT NULL AND f.partial_max > :snapshot)";
        }
        try (Handle handle = jdbi.open()) {
            DuckLakeRowCount dataFiles = handle.createQuery(
                            """
                            SELECT
                                coalesce(sum(f.record_count), 0) AS record_count,
                                coalesce(sum(CASE WHEN lower(f.file_format) <> 'parquet'
                                        OR f.encryption_key IS NOT NULL
                                        OR %s THEN 1 ELSE 0 END), 0) AS unreadable_count
                            FROM %s f
                            WHERE f.table_id = :tableId AND %s""".formatted(
                                    partiallyVisibleCondition,
                                    table("ducklake_data_file"),
                                    visible("f")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeRowCount(rs.getLong("record_count"), rs.getLong("unreadable_count") == 0))
                    .one();

            // The rows to subtract come from the delete files joined to their data file, so that one
            // left behind for a data file that is no longer visible does not remove rows that were
            // never counted. The files to reject are looked for without that join, because the
            // split manager checks every visible delete file whether or not it applies to one.
            // Several visible delete files for one data file is such a rejection, and the count
            // could not be trusted there either because they may delete the same row twice.
            DuckLakeRowCount deleteFiles = handle.createQuery(
                            """
                            SELECT
                                (SELECT coalesce(sum(j.delete_count), 0)
                                    FROM %s j
                                    JOIN %s f ON f.table_id = j.table_id AND f.data_file_id = j.data_file_id
                                    WHERE j.table_id = :tableId AND %s AND %s) AS delete_count,
                                coalesce(sum(CASE WHEN lower(d.format) <> 'parquet'
                                        OR d.encryption_key IS NOT NULL THEN 1 ELSE 0 END), 0)
                                    + (count(*) - count(DISTINCT d.data_file_id)) AS unreadable_count
                            FROM %s d
                            WHERE d.table_id = :tableId AND %s""".formatted(
                                    table("ducklake_delete_file"),
                                    table("ducklake_data_file"),
                                    visible("j"),
                                    visible("f"),
                                    table("ducklake_delete_file"),
                                    visible("d")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeRowCount(rs.getLong("delete_count"), rs.getLong("unreadable_count") == 0))
                    .one();

            return new DuckLakeRowCount(
                    dataFiles.rowCount() - deleteFiles.rowCount(),
                    dataFiles.exact() && deleteFiles.exact());
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeDeleteFileEntry> deleteFiles(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT delete_file_id, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key
                            FROM %s
                            WHERE table_id = :tableId AND %s
                            ORDER BY delete_file_id""".formatted(table("ducklake_delete_file"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeDeleteFileEntry(
                            rs.getLong("delete_file_id"),
                            rs.getLong("data_file_id"),
                            rs.getString("path"),
                            rs.getBoolean("path_is_relative"),
                            rs.getString("format"),
                            rs.getLong("delete_count"),
                            rs.getLong("file_size_bytes"),
                            optionalLong(rs, "footer_size"),
                            Optional.ofNullable(rs.getString("encryption_key"))))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Returns the name mappings with the given ids, keyed by mapping id. A data file referencing
     * a mapping stores its columns under the Parquet names given by the mapping instead of
     * carrying DuckLake field ids. Mappings that are not present in the catalog are omitted from
     * the result.
     */
    public Map<Long, DuckLakeNameMapping> nameMappings(Set<Long> mappingIds)
    {
        if (mappingIds.isEmpty() || !nameMappingTableExists()) {
            return ImmutableMap.of();
        }

        // catalogs written before the is_partition column was added cannot map Hive partition values
        String isPartitionColumn = nameMappingHasIsPartition() ? "is_partition" : "false AS is_partition";

        record NameMappingRow(long mappingId, long columnId, String sourceName, long targetFieldId, OptionalLong parentColumn, boolean isPartition) {}

        List<NameMappingRow> rows;
        try (Handle handle = jdbi.open()) {
            rows = handle.createQuery(
                            """
                            SELECT mapping_id, column_id, source_name, target_field_id, parent_column, %s
                            FROM %s
                            WHERE mapping_id IN (<mappingIds>)
                            ORDER BY mapping_id, column_id""".formatted(isPartitionColumn, table("ducklake_name_mapping")))
                    .bindList("mappingIds", ImmutableList.copyOf(mappingIds))
                    .map((rs, _) -> new NameMappingRow(
                            rs.getLong("mapping_id"),
                            rs.getLong("column_id"),
                            rs.getString("source_name"),
                            rs.getLong("target_field_id"),
                            optionalLong(rs, "parent_column"),
                            rs.getBoolean("is_partition")))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }

        // a mapping entry for a nested field references the entry of its enclosing column
        Map<Long, Set<Long>> columnIdsWithChildren = new HashMap<>();
        for (NameMappingRow row : rows) {
            row.parentColumn().ifPresent(parentColumn -> columnIdsWithChildren
                    .computeIfAbsent(row.mappingId(), _ -> new HashSet<>())
                    .add(parentColumn));
        }

        Map<Long, Map<Long, DuckLakeNameMappingEntry>> entriesByMappingId = new LinkedHashMap<>();
        for (NameMappingRow row : rows) {
            if (row.parentColumn().isPresent()) {
                // nested entries are only reachable through their top-level column
                continue;
            }
            boolean hasNestedFields = columnIdsWithChildren.getOrDefault(row.mappingId(), Set.of()).contains(row.columnId());
            DuckLakeNameMappingEntry entry = new DuckLakeNameMappingEntry(row.sourceName(), row.isPartition(), hasNestedFields);
            DuckLakeNameMappingEntry existing = entriesByMappingId
                    .computeIfAbsent(row.mappingId(), _ -> new LinkedHashMap<>())
                    .putIfAbsent(row.targetFieldId(), entry);
            if (existing != null) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Name mapping %s maps field %s more than once".formatted(row.mappingId(), row.targetFieldId()));
            }
        }
        return entriesByMappingId.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> new DuckLakeNameMapping(entry.getValue())));
    }

    /**
     * Returns the partitioning scheme of the table visible at the snapshot, if any.
     */
    public Optional<DuckLakePartitionInfo> partitionInfo(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            Map<Long, ImmutableList.Builder<DuckLakePartitionColumn>> columnsByPartitionId = new LinkedHashMap<>();
            handle.createQuery(
                            """
                            SELECT i.partition_id, c.partition_key_index, c.column_id, c.transform
                            FROM %s i
                            JOIN %s c ON i.partition_id = c.partition_id AND i.table_id = c.table_id
                            WHERE i.table_id = :tableId AND %s
                            ORDER BY c.partition_key_index""".formatted(table("ducklake_partition_info"), table("ducklake_partition_column"), visible("i")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> {
                        long partitionId = rs.getLong("partition_id");
                        columnsByPartitionId
                                .computeIfAbsent(partitionId, _ -> ImmutableList.builder())
                                .add(new DuckLakePartitionColumn(
                                        rs.getInt("partition_key_index"),
                                        rs.getLong("column_id"),
                                        rs.getString("transform")));
                        return partitionId;
                    })
                    .list();
            if (columnsByPartitionId.isEmpty()) {
                return Optional.empty();
            }
            if (columnsByPartitionId.size() > 1) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Multiple partitioning schemes are visible for table %s at snapshot %s".formatted(tableId, snapshotId));
            }
            Map.Entry<Long, ImmutableList.Builder<DuckLakePartitionColumn>> entry = columnsByPartitionId.entrySet().iterator().next();
            return Optional.of(new DuckLakePartitionInfo(entry.getKey(), entry.getValue().build()));
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Returns true when every data file of the table visible at the snapshot was written with
     * the given partitioning scheme, so partition values of all files can be interpreted with it.
     */
    public boolean allDataFilesUsePartition(long snapshotId, long tableId, long partitionId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT count(*)
                            FROM %s f
                            WHERE f.table_id = :tableId AND %s
                                AND (f.partition_id IS NULL OR f.partition_id <> :partitionId)""".formatted(table("ducklake_data_file"), visible("f")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .bind("partitionId", partitionId)
                    .mapTo(Long.class)
                    .one() == 0;
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeFileColumnStats> fileColumnStats(long tableId, Set<Long> columnIds)
    {
        if (columnIds.isEmpty()) {
            return ImmutableList.of();
        }
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT data_file_id, column_id, value_count, null_count, min_value, max_value, contains_nan
                            FROM %s
                            WHERE table_id = :tableId AND column_id IN (<columnIds>)""".formatted(table("ducklake_file_column_stats")))
                    .bind("tableId", tableId)
                    .bindList("columnIds", ImmutableList.copyOf(columnIds))
                    .map((rs, _) -> new DuckLakeFileColumnStats(
                            rs.getLong("data_file_id"),
                            rs.getLong("column_id"),
                            optionalLong(rs, "value_count"),
                            optionalLong(rs, "null_count"),
                            Optional.ofNullable(rs.getString("min_value")),
                            Optional.ofNullable(rs.getString("max_value")),
                            optionalBoolean(rs, "contains_nan")))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * The views visible at the snapshot, optionally restricted to one schema given by its Trino
     * (lowercase) name.
     */
    public List<DuckLakeViewEntry> listViews(long snapshotId, Optional<String> schemaName)
    {
        if (!viewTableExists()) {
            // catalogs written before DuckLake had views have no table to read
            return ImmutableList.of();
        }
        try (Handle handle = jdbi.open()) {
            String sql =
                    """
                    SELECT v.view_id, v.schema_id, s.schema_name, v.view_name, v.dialect, v.sql, v.column_aliases
                    FROM %s v
                    JOIN %s s ON v.schema_id = s.schema_id
                    WHERE %s AND %s
                    """.formatted(table("ducklake_view"), table("ducklake_schema"), visible("v"), visible("s"));
            if (schemaName.isPresent()) {
                sql += " AND lower(s.schema_name) = :schemaName";
            }
            sql += " ORDER BY s.schema_name, v.view_name";
            var query = handle.createQuery(sql).bind("snapshot", snapshotId);
            schemaName.ifPresent(name -> query.bind("schemaName", name.toLowerCase(ENGLISH)));
            return query.map((rs, _) -> new DuckLakeViewEntry(
                            rs.getLong("view_id"),
                            rs.getLong("schema_id"),
                            rs.getString("schema_name"),
                            rs.getString("view_name"),
                            stringOrEmpty(rs, "dialect"),
                            stringOrEmpty(rs, "sql"),
                            stringOrEmpty(rs, "column_aliases")))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Finds the view matching the given Trino (lowercase) schema and view name.
     */
    public Optional<DuckLakeViewEntry> findView(long snapshotId, String schemaName, String viewName)
    {
        return listViews(snapshotId, Optional.of(schemaName)).stream()
                .filter(view -> view.viewName().equalsIgnoreCase(viewName))
                .findFirst();
    }

    public boolean viewsSupported()
    {
        return viewTableExists();
    }

    /**
     * The value of one tag of an object, which is how DuckLake records a comment and anything else
     * an engine wants to keep beside a schema, table or view.
     */
    public Optional<String> tag(long snapshotId, long objectId, String key)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT value FROM %s
                            WHERE object_id = :objectId AND key = :key AND %s""".formatted(table("ducklake_tag"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .bind("objectId", objectId)
                    .bind("key", key)
                    .mapTo(String.class)
                    .findFirst();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * The comments recorded for a table and for its columns. DuckLake keeps them as tags keyed by
     * {@code comment}, versioned by snapshot like every other row.
     */
    public Optional<String> tableComment(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT value FROM %s
                            WHERE object_id = :tableId AND key = 'comment' AND %s""".formatted(table("ducklake_tag"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .mapTo(String.class)
                    .findFirst();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public Map<Long, String> columnComments(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            Map<Long, String> comments = new LinkedHashMap<>();
            handle.createQuery(
                            """
                            SELECT column_id, value FROM %s
                            WHERE table_id = :tableId AND key = 'comment' AND %s""".formatted(table("ducklake_column_tag"), VISIBLE))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .map((rs, _) -> comments.put(rs.getLong("column_id"), rs.getString("value")))
                    .list();
            return ImmutableMap.copyOf(comments);
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public Optional<DuckLakeTableStats> tableStatistics(long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery("SELECT record_count, file_size_bytes FROM " + table("ducklake_table_stats") + " WHERE table_id = :tableId")
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeTableStats(
                            optionalLong(rs, "record_count"),
                            optionalLong(rs, "file_size_bytes")))
                    .findOne();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeTableColumnStats> tableColumnStatistics(long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT column_id, contains_null, contains_nan, min_value, max_value
                            FROM %s
                            WHERE table_id = :tableId""".formatted(table("ducklake_table_column_stats")))
                    .bind("tableId", tableId)
                    .map((rs, _) -> new DuckLakeTableColumnStats(
                            rs.getLong("column_id"),
                            optionalBoolean(rs, "contains_null"),
                            optionalBoolean(rs, "contains_nan"),
                            Optional.ofNullable(rs.getString("min_value")),
                            Optional.ofNullable(rs.getString("max_value"))))
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Returns true when the table has data stored inline in the catalog database that is visible
     * at the given snapshot. Such rows are not backed by Parquet files and are not supported.
     */
    public boolean hasInlinedData(long snapshotId, long tableId)
    {
        if (!inlinedDataTablesRegistryExists()) {
            // older DuckLake catalogs have no ducklake_inlined_data_tables table and cannot inline data
            return false;
        }
        List<String> inlinedTableNames;
        try (Handle handle = jdbi.open()) {
            inlinedTableNames = handle.createQuery("SELECT table_name FROM " + table("ducklake_inlined_data_tables") + " WHERE table_id = :tableId")
                    .bind("tableId", tableId)
                    .mapTo(String.class)
                    .list();
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
        for (String inlinedTableName : inlinedTableNames) {
            try (Handle handle = jdbi.open()) {
                boolean hasRows = handle.createQuery("SELECT 1 FROM %s WHERE %s LIMIT 1".formatted(table(inlinedTableName), VISIBLE))
                        .bind("snapshot", snapshotId)
                        .mapTo(Long.class)
                        .findOne()
                        .isPresent();
                if (hasRows) {
                    return true;
                }
            }
            catch (JdbiException e) {
                if (!isUndefinedTable(e)) {
                    throw metastoreError(e);
                }
                // the inlined data table is registered but does not exist (e.g. already cleaned up); treat as empty
            }
        }
        return false;
    }

    /**
     * Returns true when a {@link SQLException} in the cause chain reports the PostgreSQL
     * "undefined table" error, meaning the queried table does not exist.
     */
    static boolean isUndefinedTable(Throwable throwable)
    {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException && UNDEFINED_TABLE_SQL_STATE.equals(sqlException.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    private boolean inlinedDataTablesRegistryExists()
    {
        Boolean registryExists = inlinedDataTablesRegistryExists;
        if (registryExists == null) {
            registryExists = tableExists("ducklake_inlined_data_tables");
            inlinedDataTablesRegistryExists = registryExists;
        }
        return registryExists;
    }

    private boolean viewTableExists()
    {
        Boolean tableExists = viewTableExists;
        if (tableExists == null) {
            tableExists = tableExists("ducklake_view");
            viewTableExists = tableExists;
        }
        return tableExists;
    }

    private boolean nameMappingTableExists()
    {
        Boolean tableExists = nameMappingTableExists;
        if (tableExists == null) {
            tableExists = tableExists("ducklake_name_mapping");
            nameMappingTableExists = tableExists;
        }
        return tableExists;
    }

    private boolean nameMappingHasIsPartition()
    {
        Boolean hasIsPartition = nameMappingHasIsPartition;
        if (hasIsPartition == null) {
            hasIsPartition = columnExists("ducklake_name_mapping", "is_partition");
            nameMappingHasIsPartition = hasIsPartition;
        }
        return hasIsPartition;
    }

    private boolean dataFileHasPartialMax()
    {
        Boolean hasPartialMax = dataFileHasPartialMax;
        if (hasPartialMax == null) {
            hasPartialMax = columnExists("ducklake_data_file", "partial_max");
            dataFileHasPartialMax = hasPartialMax;
        }
        return hasPartialMax;
    }

    private boolean tableExists(String tableName)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT count(*) FROM information_schema.tables
                            WHERE table_schema = :schema AND table_name = :tableName""")
                    .bind("schema", metadataSchema)
                    .bind("tableName", tableName)
                    .mapTo(Long.class)
                    .one() > 0;
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    private boolean columnExists(String tableName, String columnName)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT count(*) FROM information_schema.columns
                            WHERE table_schema = :schema AND table_name = :tableName AND column_name = :columnName""")
                    .bind("schema", metadataSchema)
                    .bind("tableName", tableName)
                    .bind("columnName", columnName)
                    .mapTo(Long.class)
                    .one() > 0;
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    private static DuckLakeTableEntry tableEntry(ResultSet resultSet, org.jdbi.v3.core.statement.StatementContext context)
            throws SQLException
    {
        return new DuckLakeTableEntry(
                resultSet.getLong("table_id"),
                resultSet.getLong("schema_id"),
                resultSet.getString("schema_name"),
                resultSet.getString("table_name"),
                stringOrEmpty(resultSet, "path"),
                resultSet.getBoolean("path_is_relative"),
                stringOrEmpty(resultSet, "schema_path"),
                resultSet.getBoolean("schema_path_is_relative"));
    }

    private static DuckLakeDataFileEntry dataFileEntry(ResultSet resultSet, long dataFileId)
    {
        try {
            return new DuckLakeDataFileEntry(
                    dataFileId,
                    resultSet.getString("path"),
                    resultSet.getBoolean("path_is_relative"),
                    resultSet.getString("file_format"),
                    resultSet.getLong("record_count"),
                    resultSet.getLong("file_size_bytes"),
                    optionalLong(resultSet, "footer_size"),
                    optionalLong(resultSet, "row_id_start"),
                    optionalLong(resultSet, "partition_id"),
                    Optional.ofNullable(resultSet.getString("encryption_key")),
                    optionalLong(resultSet, "mapping_id"),
                    optionalLong(resultSet, "partial_max"),
                    Map.of());
        }
        catch (SQLException e) {
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to read DuckLake metadata: " + e.getMessage(), e);
        }
    }

    private static DuckLakeDataFileEntry withPartitionValues(DuckLakeDataFileEntry file, Map<Integer, Optional<String>> partitionValues)
    {
        return new DuckLakeDataFileEntry(
                file.dataFileId(),
                file.path(),
                file.pathIsRelative(),
                file.fileFormat(),
                file.recordCount(),
                file.fileSizeBytes(),
                file.footerSize(),
                file.rowIdStart(),
                file.partitionId(),
                file.encryptionKey(),
                file.mappingId(),
                file.partialMax(),
                partitionValues);
    }

    private String table(String tableName)
    {
        return "\"%s\".\"%s\"".formatted(metadataSchema.replace("\"", "\"\""), tableName.replace("\"", "\"\""));
    }

    private static String visible(String alias)
    {
        return "%s.begin_snapshot <= :snapshot AND (%s.end_snapshot IS NULL OR %s.end_snapshot > :snapshot)".formatted(alias, alias, alias);
    }

    private static String stringOrEmpty(ResultSet resultSet, String columnName)
            throws SQLException
    {
        String value = resultSet.getString(columnName);
        if (value == null) {
            return "";
        }
        return value;
    }

    static OptionalLong optionalLong(ResultSet resultSet, String columnName)
            throws SQLException
    {
        long value = resultSet.getLong(columnName);
        if (resultSet.wasNull()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(value);
    }

    static Optional<Boolean> optionalBoolean(ResultSet resultSet, String columnName)
            throws SQLException
    {
        boolean value = resultSet.getBoolean(columnName);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    private static TrinoException metastoreError(JdbiException exception)
    {
        return new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to access DuckLake catalog: " + exception.getMessage(), exception);
    }
}
