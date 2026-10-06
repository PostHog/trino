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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import jakarta.annotation.Nullable;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_FAILED;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_CONCURRENT_MODIFICATION;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Reads DuckLake catalog metadata from the {@code ducklake_*} tables in a PostgreSQL database.
 */
public class JdbcDuckLakeMetastore
{
    private static final String VISIBLE = "begin_snapshot <= :snapshot AND (end_snapshot IS NULL OR end_snapshot > :snapshot)";
    /**
     * The columns every inlined data table starts with, ahead of the columns of the DuckLake table.
     */
    private static final List<String> INLINED_SYSTEM_COLUMNS = ImmutableList.of("row_id", "begin_snapshot", "end_snapshot");
    private static final String UNDEFINED_TABLE_SQL_STATE = "42P01";
    private static final String SERIALIZATION_FAILURE_SQL_STATE = "40001";
    private static final String DEADLOCK_DETECTED_SQL_STATE = "40P01";
    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";
    /**
     * How often the wait between commit attempts is doubled. Waiting longer than this does not help
     * a writer that keeps losing the race, and the bound holds whatever backoff is configured.
     */
    private static final int MAX_COMMIT_RETRY_DOUBLINGS = 5;

    private final Jdbi jdbi;
    private final String metadataSchema;
    private final int maxCommitRetries;
    private final long commitRetryBackoffMillis;

    private volatile Boolean dataFileHasPartialMax;
    private volatile Boolean deleteFileHasPartialMax;
    private volatile Boolean schemaVersionsTableExists;
    private volatile Boolean schemaVersionsHasTableId;
    private volatile Boolean inlinedDataTablesRegistryExists;
    private volatile Boolean nameMappingTableExists;
    private volatile Boolean sortInfoTableExists;
    private volatile Boolean viewTableExists;
    private volatile Boolean nameMappingHasIsPartition;

    @Inject
    public JdbcDuckLakeMetastore(ConnectionFactory connectionFactory, DuckLakeConfig config)
    {
        this.jdbi = Jdbi.create(requireNonNull(connectionFactory, "connectionFactory is null"));
        this.metadataSchema = config.getMetadataSchema();
        this.maxCommitRetries = config.getCommitMaxRetries();
        this.commitRetryBackoffMillis = config.getCommitRetryBackoff().toMillis();
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
     * Runs the action against a new snapshot and commits it atomically, re-basing it onto whatever
     * another writer committed in the meantime.
     * <p>
     * DuckLake orders all changes to a catalog on a single snapshot chain, so every commit has to
     * claim the snapshot following the newest one. Losing that race is ordinary rather than
     * exceptional here, because a DuckDB writer commits to the same catalog: the database reports
     * it — as a serialization failure, or as a unique violation on the snapshot table's primary
     * key — and the action is discarded and replayed against the newer state. A replay is safe
     * because the action only reads catalog rows and writes them through this commit; the data
     * files it registers were written to object storage before the commit began, and the discarded
     * attempt left no rows behind.
     * <p>
     * Re-basing is only allowed where DuckLake allows it. {@code readSnapshotId} is the snapshot the
     * statement was planned and read against, and the commit refuses to land if anything committed
     * after it changed an object the statement's own result depends on. The refusal carries
     * {@code DUCKLAKE_COMMIT_CONFLICT} and is not retried: only replanning the statement can help.
     */
    public <T> T commit(long readSnapshotId, DuckLakeCommitAction<T> action)
    {
        // read outside the transaction, so that the commit itself needs one connection only
        boolean sortInfoSupported = sortInfoTableExists();
        RuntimeException conflict = null;
        for (int attempt = 0; attempt <= maxCommitRetries; attempt++) {
            try {
                return jdbi.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
                    DuckLakeCommit commit = new DuckLakeCommit(handle, metadataSchema, snapshotState(handle), sortInfoSupported);
                    T result = action.run(commit);
                    commit.verifyNoConflictSince(readSnapshotId);
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
            if (attempt < maxCommitRetries) {
                sleepBeforeRetry(attempt);
            }
        }
        throw new TrinoException(
                DUCKLAKE_COMMIT_FAILED,
                "Failed to commit to the DuckLake catalog after %s retries because of concurrent updates; raise ducklake.commit.max-retries if this is common".formatted(maxCommitRetries),
                conflict);
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

    private void sleepBeforeRetry(int attempt)
    {
        try {
            Thread.sleep(commitRetryBackoffMillis << Math.min(attempt, MAX_COMMIT_RETRY_DOUBLINGS));
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
            return columns(handle, snapshotId, tableId);
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    private List<DuckLakeColumnEntry> columns(Handle handle, long snapshotId, long tableId)
    {
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
     * file the connector refuses to read, a data or delete file only partly visible in the
     * snapshot, or rows of a data file deleted inline, whose deletions may repeat those of a delete
     * file; all are conditions the split manager checks per file, and reporting them here keeps a
     * {@code count(*)} failing wherever a scan would fail.
     * <p>
     * Rows stored inline in the catalog database are counted too. Everything is read in one
     * transaction, because DuckDB moves inlined rows into Parquet files by deleting them from the
     * catalog database while it adds the files under the snapshots the rows were written in: read
     * at two different moments, the same rows could be counted twice or not at all.
     */
    public DuckLakeRowCount rowCount(long snapshotId, long tableId)
    {
        boolean inlinedDataSupported = inlinedDataTablesRegistryExists();
        // Files the split manager rejects are counted rather than located, because the count only
        // decides whether to answer from the catalog at all; the scan that runs instead reports
        // which file it was.
        //
        // 1 = 0 rather than the false literal, which not every catalog database spells the same way
        String partiallyVisibleCondition = "1 = 0";
        if (dataFileHasPartialMax()) {
            partiallyVisibleCondition = "(f.partial_max IS NOT NULL AND f.partial_max > :snapshot)";
        }
        String partiallyVisibleDeleteCondition = "1 = 0";
        if (deleteFileHasPartialMax()) {
            partiallyVisibleDeleteCondition = "(d.partial_max IS NOT NULL AND d.partial_max > :snapshot)";
        }
        String dataFileCondition = partiallyVisibleCondition;
        String deleteFileCondition = partiallyVisibleDeleteCondition;
        try {
            return jdbi.inTransaction(TransactionIsolationLevel.REPEATABLE_READ, handle -> {
                DuckLakeRowCount dataFiles = handle.createQuery(
                                """
                                SELECT
                                    coalesce(sum(f.record_count), 0) AS record_count,
                                    coalesce(sum(CASE WHEN lower(f.file_format) <> 'parquet'
                                            OR f.encryption_key IS NOT NULL
                                            OR %s THEN 1 ELSE 0 END), 0) AS unreadable_count
                                FROM %s f
                                WHERE f.table_id = :tableId AND %s""".formatted(
                                        dataFileCondition,
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
                                            OR d.encryption_key IS NOT NULL
                                            OR %s THEN 1 ELSE 0 END), 0)
                                        + (count(*) - count(DISTINCT d.data_file_id)) AS unreadable_count
                                FROM %s d
                                WHERE d.table_id = :tableId AND %s""".formatted(
                                        table("ducklake_delete_file"),
                                        table("ducklake_data_file"),
                                        visible("j"),
                                        visible("f"),
                                        deleteFileCondition,
                                        table("ducklake_delete_file"),
                                        visible("d")))
                        .bind("snapshot", snapshotId)
                        .bind("tableId", tableId)
                        .map((rs, _) -> new DuckLakeRowCount(rs.getLong("delete_count"), rs.getLong("unreadable_count") == 0))
                        .one();

                long inlinedRows = 0;
                if (inlinedDataSupported) {
                    for (String inlinedTableName : existingInlinedDataTableNames(handle, tableId)) {
                        inlinedRows += handle.createQuery("SELECT count(*) FROM %s WHERE %s".formatted(table(inlinedTableName), VISIBLE))
                                .bind("snapshot", snapshotId)
                                .mapTo(Long.class)
                                .one();
                    }
                }
                // A position deleted inline may also be recorded by a delete file of the same data
                // file, so subtracting both may count a row twice. The result is the number DuckDB
                // reports, which subtracts both, but it is not exact enough to answer a count with.
                long inlinedDeletions = inlinedFileDeletionCount(handle, snapshotId, tableId);

                return new DuckLakeRowCount(
                        dataFiles.rowCount() - deleteFiles.rowCount() - inlinedDeletions + inlinedRows,
                        dataFiles.exact() && deleteFiles.exact() && inlinedDeletions == 0);
            });
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    public List<DuckLakeDeleteFileEntry> deleteFiles(long snapshotId, long tableId)
    {
        try (Handle handle = jdbi.open()) {
            return deleteFiles(handle, snapshotId, tableId);
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * The delete files of a table visible in the snapshot together with the positions of its data
     * files deleted inline, read in one transaction. See {@link DuckLakeDeletions}.
     */
    public DuckLakeDeletions deletions(long snapshotId, long tableId)
    {
        try {
            return jdbi.inTransaction(TransactionIsolationLevel.REPEATABLE_READ, handle -> new DuckLakeDeletions(
                    deleteFiles(handle, snapshotId, tableId),
                    inlinedFileDeletions(handle, snapshotId, tableId)));
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    private List<DuckLakeDeleteFileEntry> deleteFiles(Handle handle, long snapshotId, long tableId)
    {
        String partialMaxColumn = "NULL AS partial_max";
        if (deleteFileHasPartialMax()) {
            partialMaxColumn = "partial_max";
        }
        return handle.createQuery(
                        """
                        SELECT delete_file_id, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key, %s
                        FROM %s
                        WHERE table_id = :tableId AND %s
                        ORDER BY delete_file_id""".formatted(partialMaxColumn, table("ducklake_delete_file"), VISIBLE))
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
                        Optional.ofNullable(rs.getString("encryption_key")),
                        optionalLong(rs, "partial_max")))
                .list();
    }

    /**
     * The positions of rows of the table's data files that DuckDB deleted by recording them in
     * {@code ducklake_inlined_delete_<table id>} instead of writing a delete file, by data file
     * identifier. A position there is the index of the row within its data file, like the
     * positions a delete file holds, and a deletion is never ended: it applies from the snapshot
     * that recorded it on, for as long as the data file it names is visible.
     */
    private Map<Long, long[]> inlinedFileDeletions(Handle handle, long snapshotId, long tableId)
    {
        String deletionTable = inlinedFileDeletionTableName(tableId);
        if (!relationExists(handle, deletionTable)) {
            return ImmutableMap.of();
        }
        Map<Long, LongArrayList> positionsByDataFile = new HashMap<>();
        handle.createQuery(
                        """
                        SELECT DISTINCT file_id, row_id
                        FROM %s
                        WHERE begin_snapshot <= :snapshot
                        ORDER BY file_id, row_id""".formatted(table(deletionTable)))
                .bind("snapshot", snapshotId)
                .map((rs, _) -> {
                    long dataFileId = rs.getLong("file_id");
                    long position = rs.getLong("row_id");
                    if (rs.wasNull() || position < 0) {
                        throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Inlined deletion of data file %s of table %s has an invalid position".formatted(dataFileId, tableId));
                    }
                    positionsByDataFile.computeIfAbsent(dataFileId, _ -> new LongArrayList()).add(position);
                    return dataFileId;
                })
                .list();
        return positionsByDataFile.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toLongArray()));
    }

    /**
     * The number of inline deletions applying to the data files of the table visible in the
     * snapshot.
     */
    private long inlinedFileDeletionCount(Handle handle, long snapshotId, long tableId)
    {
        String deletionTable = inlinedFileDeletionTableName(tableId);
        if (!relationExists(handle, deletionTable)) {
            return 0;
        }
        return handle.createQuery(
                        """
                        SELECT count(*) FROM (
                            SELECT DISTINCT d.file_id, d.row_id
                            FROM %s d
                            JOIN %s f ON f.data_file_id = d.file_id
                            WHERE d.begin_snapshot <= :snapshot AND f.table_id = :tableId AND %s) deletions""".formatted(
                                table(deletionTable),
                                table("ducklake_data_file"),
                                visible("f")))
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .mapTo(Long.class)
                .one();
    }

    private static String inlinedFileDeletionTableName(long tableId)
    {
        return "ducklake_inlined_delete_" + tableId;
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

    /**
     * Returns the distinct partition values one key of the partitioning scheme takes across the
     * data files of the table visible at the snapshot, each in the shape
     * {@link DuckLakeDataFileEntry#partitionValues()} carries so that both are read the same way.
     * A file that records no value under the key contributes the empty map.
     * <p>
     * The result holds one entry per distinct value rather than one per file, which is what makes
     * it affordable to ask about a table of many files.
     */
    public List<Map<Integer, Optional<String>>> distinctPartitionValues(long snapshotId, long tableId, int partitionKeyIndex)
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT DISTINCT v.partition_key_index, v.partition_value
                            FROM %s f
                            LEFT JOIN %s v ON f.data_file_id = v.data_file_id AND f.table_id = v.table_id
                                AND v.partition_key_index = :partitionKeyIndex
                            WHERE f.table_id = :tableId AND %s""".formatted(
                                    table("ducklake_data_file"),
                                    table("ducklake_file_partition_value"),
                                    visible("f")))
                    .bind("snapshot", snapshotId)
                    .bind("tableId", tableId)
                    .bind("partitionKeyIndex", partitionKeyIndex)
                    .map((rs, _) -> {
                        int keyIndex = rs.getInt("partition_key_index");
                        if (rs.wasNull()) {
                            // a file that records nothing under the key says nothing about its rows
                            return Map.<Integer, Optional<String>>of();
                        }
                        return Map.of(keyIndex, Optional.ofNullable(rs.getString("partition_value")));
                    })
                    .list();
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
     * Whether the catalog can hold inlined rows at all, which catalogs written before DuckLake
     * inlined data cannot.
     */
    public boolean inlinedDataSupported()
    {
        return inlinedDataTablesRegistryExists();
    }

    /**
     * Whether the table holds rows or deletions stored inline in the catalog database that are
     * visible in the snapshot. Both are read in one transaction.
     */
    public DuckLakeInlinedSummary inlinedSummary(long snapshotId, long tableId)
    {
        boolean inlinedDataSupported = inlinedDataTablesRegistryExists();
        try {
            return jdbi.inTransaction(TransactionIsolationLevel.REPEATABLE_READ, handle -> {
                boolean hasRows = false;
                if (inlinedDataSupported) {
                    for (String inlinedTableName : existingInlinedDataTableNames(handle, tableId)) {
                        if (hasVisibleRows(handle, inlinedTableName, snapshotId)) {
                            hasRows = true;
                            break;
                        }
                    }
                }
                return new DuckLakeInlinedSummary(hasRows, inlinedFileDeletionCount(handle, snapshotId, tableId) > 0);
            });
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * The inlined data tables of the table holding rows visible in the snapshot, with the columns
     * each of them stores, read in one transaction together with the newest snapshot of the
     * catalog. See {@link DuckLakeInlinedData}.
     * <p>
     * DuckDB creates an inlined data table for every schema version of a table it inlines rows
     * under, and names its columns as the table named them in that version. The columns are tied
     * to the DuckLake column identifiers through the columns the table had in the first snapshot of
     * that schema version, which is how DuckDB reads them too. An inlined data table whose columns
     * are not exactly those is rejected rather than guessed at, since a guess could hand the values
     * of one column to another. An inlined data table that is registered but does not exist holds
     * nothing; DuckDB drops the ones it emptied.
     */
    public DuckLakeInlinedData inlinedData(long snapshotId, long tableId)
    {
        if (!inlinedDataTablesRegistryExists()) {
            // older DuckLake catalogs have no ducklake_inlined_data_tables table and cannot inline data
            return DuckLakeInlinedData.none(snapshotId);
        }
        try {
            return jdbi.inTransaction(TransactionIsolationLevel.REPEATABLE_READ, handle -> {
                long watermark = handle.createQuery("SELECT max(snapshot_id) FROM " + table("ducklake_snapshot"))
                        .mapTo(Long.class)
                        .one();
                ImmutableList.Builder<DuckLakeInlinedDataTable> tables = ImmutableList.builder();
                for (InlinedDataTableRegistration registration : inlinedDataTableRegistrations(handle, tableId)) {
                    if (!relationExists(handle, registration.tableName()) || !hasVisibleRows(handle, registration.tableName(), snapshotId)) {
                        continue;
                    }
                    tables.add(new DuckLakeInlinedDataTable(
                            registration.tableName(),
                            registration.schemaVersion(),
                            inlinedColumns(handle, tableId, registration)));
                }
                return new DuckLakeInlinedData(watermark, tables.build());
            });
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    /**
     * Whether DuckDB moved inlined rows of the table into Parquet files in a snapshot newer than
     * the given one. Such a flush deletes the rows from the catalog database, so rows listed as
     * inlined before it may since have gone from there, to Parquet files a reader of the earlier
     * listing does not know about.
     */
    public boolean inlinedDataFlushedAfter(long tableId, long watermarkSnapshotId)
    {
        try (Handle handle = jdbi.open()) {
            return inlinedDataFlushedAfter(handle, tableId, watermarkSnapshotId);
        }
        catch (JdbiException e) {
            throw metastoreError(e);
        }
    }

    private boolean inlinedDataFlushedAfter(Handle handle, long tableId, long watermarkSnapshotId)
    {
        // the patterns only narrow down the rows to look at; the entries are matched exactly below
        return handle.createQuery(
                        """
                        SELECT changes_made FROM %s
                        WHERE snapshot_id > :watermark
                            AND (changes_made LIKE '%%inline_flush:%%' OR changes_made LIKE '%%flushed_inlined:%%')""".formatted(table("ducklake_snapshot_changes")))
                .bind("watermark", watermarkSnapshotId)
                .mapTo(String.class)
                .list()
                .stream()
                .anyMatch(changes -> DuckLakeSnapshotChanges.recordsInlinedDataFlush(changes, tableId));
    }

    /**
     * Opens the rows of an inlined data table visible in the snapshot, reading the given columns.
     * The rows are read in a transaction of their own, which first makes sure that no flush of the
     * table was committed after {@code watermarkSnapshotId}, the snapshot the inlined data tables
     * were listed in: a flush in between would have moved the rows to Parquet files the scan does
     * not read, and the scan would silently miss them. Reading both in one transaction is what
     * makes the check hold for the rows read.
     */
    public InlinedRows openInlinedRows(long tableId, long snapshotId, long watermarkSnapshotId, String inlinedTableName, List<String> columnNames, int fetchSize)
    {
        Handle handle = jdbi.open();
        try {
            handle.begin();
            // a single snapshot of the catalog database for the check and the rows it vouches for
            handle.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            if (inlinedDataFlushedAfter(handle, tableId, watermarkSnapshotId)) {
                throw new TrinoException(
                        DUCKLAKE_CONCURRENT_MODIFICATION,
                        "Inlined data of DuckLake table %s was flushed to Parquet files while the query was reading it; run the query again".formatted(tableId));
            }
            if (!relationExists(handle, inlinedTableName)) {
                // registered but gone: DuckDB drops an inlined data table only once it is empty
                closeQuietly(handle);
                return InlinedRows.EMPTY;
            }
            String projection = columnNames.isEmpty()
                    ? "1"
                    : columnNames.stream().map(JdbcDuckLakeMetastore::quoted).collect(joining(", "));
            Connection connection = handle.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT %s FROM %s WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)".formatted(projection, table(inlinedTableName)));
            try {
                statement.setFetchSize(fetchSize);
                statement.setLong(1, snapshotId);
                statement.setLong(2, snapshotId);
                return new InlinedRows(handle, statement, statement.executeQuery());
            }
            catch (SQLException | RuntimeException e) {
                statement.close();
                throw e;
            }
        }
        catch (SQLException e) {
            closeQuietly(handle);
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to read inlined data from DuckLake catalog: " + e.getMessage(), e);
        }
        catch (JdbiException e) {
            closeQuietly(handle);
            throw metastoreError(e);
        }
        catch (RuntimeException e) {
            closeQuietly(handle);
            throw e;
        }
    }

    private static void closeQuietly(Handle handle)
    {
        try (handle) {
            if (handle.isInTransaction()) {
                handle.rollback();
            }
        }
        catch (RuntimeException _) {
            // the connection is discarded or reset by the pool either way
        }
    }

    /**
     * Rows of an inlined data table being read, holding the transaction they are read in until
     * closed.
     */
    public static final class InlinedRows
            implements AutoCloseable
    {
        static final InlinedRows EMPTY = new InlinedRows(null, null, null);

        @Nullable
        private final Handle handle;
        @Nullable
        private final Statement statement;
        @Nullable
        private final ResultSet resultSet;

        private InlinedRows(@Nullable Handle handle, @Nullable Statement statement, @Nullable ResultSet resultSet)
        {
            this.handle = handle;
            this.statement = statement;
            this.resultSet = resultSet;
        }

        /**
         * Moves to the next row, returning false once there is none.
         */
        public boolean next()
                throws SQLException
        {
            return resultSet != null && resultSet.next();
        }

        /**
         * The current row, whose columns are numbered from one in the order they were requested.
         */
        public ResultSet row()
        {
            return requireNonNull(resultSet, "no rows");
        }

        @Override
        public void close()
        {
            if (handle == null) {
                return;
            }
            try {
                if (statement != null) {
                    // closing the statement closes its result set
                    statement.close();
                }
            }
            catch (SQLException _) {
                // the transaction is rolled back below, which releases whatever the statement held
            }
            closeQuietly(handle);
        }
    }

    private record InlinedDataTableRegistration(String tableName, long schemaVersion) {}

    private List<InlinedDataTableRegistration> inlinedDataTableRegistrations(Handle handle, long tableId)
    {
        return handle.createQuery(
                        """
                        SELECT table_name, schema_version FROM %s
                        WHERE table_id = :tableId
                        ORDER BY schema_version""".formatted(table("ducklake_inlined_data_tables")))
                .bind("tableId", tableId)
                .map((rs, _) -> new InlinedDataTableRegistration(rs.getString("table_name"), rs.getLong("schema_version")))
                .list();
    }

    private List<String> existingInlinedDataTableNames(Handle handle, long tableId)
    {
        return inlinedDataTableRegistrations(handle, tableId).stream()
                .map(InlinedDataTableRegistration::tableName)
                .filter(tableName -> relationExists(handle, tableName))
                .collect(toImmutableList());
    }

    private boolean hasVisibleRows(Handle handle, String inlinedTableName, long snapshotId)
    {
        return handle.createQuery("SELECT 1 FROM %s WHERE %s LIMIT 1".formatted(table(inlinedTableName), VISIBLE))
                .bind("snapshot", snapshotId)
                .mapTo(Long.class)
                .findOne()
                .isPresent();
    }

    /**
     * Ties the columns of an inlined data table to the DuckLake columns they hold the values of.
     */
    private List<DuckLakeInlinedColumn> inlinedColumns(Handle handle, long tableId, InlinedDataTableRegistration registration)
    {
        record StoredColumn(String name, String type) {}

        List<StoredColumn> stored = handle.createQuery(
                        """
                        SELECT column_name, udt_name FROM information_schema.columns
                        WHERE table_schema = :schema AND table_name = :tableName
                        ORDER BY ordinal_position""")
                .bind("schema", metadataSchema)
                .bind("tableName", registration.tableName())
                .map((rs, _) -> new StoredColumn(rs.getString("column_name"), rs.getString("udt_name")))
                .list();
        List<String> systemColumns = stored.stream()
                .limit(INLINED_SYSTEM_COLUMNS.size())
                .map(StoredColumn::name)
                .collect(toImmutableList());
        if (!systemColumns.equals(INLINED_SYSTEM_COLUMNS)) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Inlined data table %s of table %s does not start with the columns %s".formatted(registration.tableName(), tableId, INLINED_SYSTEM_COLUMNS));
        }
        List<StoredColumn> dataColumns = stored.subList(INLINED_SYSTEM_COLUMNS.size(), stored.size());

        long versionSnapshot = schemaVersionSnapshot(handle, tableId, registration.schemaVersion());
        Map<String, Long> columnIdsByName = new LinkedHashMap<>();
        for (DuckLakeColumnEntry column : columns(handle, versionSnapshot, tableId)) {
            if (column.parentColumn().isEmpty() && columnIdsByName.put(column.columnName(), column.columnId()) != null) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Table %s had two columns named '%s' at snapshot %s".formatted(tableId, column.columnName(), versionSnapshot));
            }
        }
        Set<String> storedNames = dataColumns.stream()
                .map(StoredColumn::name)
                .collect(toImmutableSet());
        if (!storedNames.equals(columnIdsByName.keySet()) || storedNames.size() != dataColumns.size()) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Inlined data table %s of table %s stores the columns %s, but the table had the columns %s in schema version %s".formatted(
                    registration.tableName(),
                    tableId,
                    dataColumns.stream().map(StoredColumn::name).toList(),
                    columnIdsByName.keySet(),
                    registration.schemaVersion()));
        }
        return dataColumns.stream()
                .map(column -> new DuckLakeInlinedColumn(columnIdsByName.get(column.name()), column.name(), column.type()))
                .collect(toImmutableList());
    }

    /**
     * A snapshot in which the table has the columns it had in the given schema version, found the
     * way DuckDB finds it: the snapshot {@code ducklake_schema_versions} records for the table and
     * version, and failing that, for catalogs that record schema versions without the table they
     * belong to or not at all, a snapshot of that schema version, and finally the snapshot the table
     * was created in.
     */
    private long schemaVersionSnapshot(Handle handle, long tableId, long schemaVersion)
    {
        if (schemaVersionsTableExists() && schemaVersionsHasTableId()) {
            Optional<Long> snapshot = handle.createQuery(
                            """
                            SELECT min(begin_snapshot) FROM %s
                            WHERE table_id = :tableId AND schema_version = :schemaVersion""".formatted(table("ducklake_schema_versions")))
                    .bind("tableId", tableId)
                    .bind("schemaVersion", schemaVersion)
                    .mapTo(Long.class)
                    .findOne();
            if (snapshot.isPresent()) {
                return snapshot.get();
            }
        }
        Optional<Long> snapshot = handle.createQuery("SELECT min(snapshot_id) FROM %s WHERE schema_version = :schemaVersion".formatted(table("ducklake_snapshot")))
                .bind("schemaVersion", schemaVersion)
                .mapTo(Long.class)
                .findOne();
        if (snapshot.isPresent()) {
            return snapshot.get();
        }
        return handle.createQuery("SELECT min(begin_snapshot) FROM %s WHERE table_id = :tableId".formatted(table("ducklake_table")))
                .bind("tableId", tableId)
                .mapTo(Long.class)
                .findOne()
                .orElseThrow(() -> new TrinoException(DUCKLAKE_INVALID_METADATA, "Cannot find the columns of table %s in schema version %s".formatted(tableId, schemaVersion)));
    }

    private boolean relationExists(Handle handle, String tableName)
    {
        return handle.createQuery("SELECT to_regclass(:name) IS NOT NULL")
                .bind("name", table(tableName))
                .mapTo(Boolean.class)
                .one();
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

    private boolean schemaVersionsTableExists()
    {
        Boolean tableExists = schemaVersionsTableExists;
        if (tableExists == null) {
            tableExists = tableExists("ducklake_schema_versions");
            schemaVersionsTableExists = tableExists;
        }
        return tableExists;
    }

    /**
     * Whether {@code ducklake_schema_versions} records which table each schema version belongs to,
     * which catalogs written before DuckLake versioned the schema of each table separately do not.
     */
    private boolean schemaVersionsHasTableId()
    {
        Boolean hasTableId = schemaVersionsHasTableId;
        if (hasTableId == null) {
            hasTableId = columnExists("ducklake_schema_versions", "table_id");
            schemaVersionsHasTableId = hasTableId;
        }
        return hasTableId;
    }

    private boolean deleteFileHasPartialMax()
    {
        Boolean hasPartialMax = deleteFileHasPartialMax;
        if (hasPartialMax == null) {
            hasPartialMax = columnExists("ducklake_delete_file", "partial_max");
            deleteFileHasPartialMax = hasPartialMax;
        }
        return hasPartialMax;
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

    /**
     * Whether the catalog records sort orders. {@code ducklake_sort_info} was added to the format
     * in version 0.4, so a catalog written against an older one does not have the table at all.
     */
    private boolean sortInfoTableExists()
    {
        Boolean tableExists = sortInfoTableExists;
        if (tableExists == null) {
            tableExists = tableExists("ducklake_sort_info");
            sortInfoTableExists = tableExists;
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

    private static DuckLakeTableEntry tableEntry(ResultSet resultSet, StatementContext context)
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

    private static String quoted(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
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
