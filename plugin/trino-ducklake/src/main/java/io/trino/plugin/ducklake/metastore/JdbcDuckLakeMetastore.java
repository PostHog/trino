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
import com.google.inject.Inject;
import io.trino.plugin.ducklake.DuckLakeConfig;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

    private final Jdbi jdbi;
    private final String metadataSchema;

    private volatile Boolean dataFileHasPartialMax;
    private volatile Boolean inlinedDataTablesRegistryExists;

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
                    WHERE t.%s AND s.%s
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
                            WHERE t.%s AND s.%s AND lower(s.schema_name) = :schemaName AND lower(t.table_name) = :tableName
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
                    WHERE c.%s AND t.%s AND s.%s
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
                            WHERE f.table_id = :tableId AND f.%s
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
                            WHERE i.table_id = :tableId AND i.%s
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
                            WHERE f.table_id = :tableId AND f.%s
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
            try (Handle handle = jdbi.open()) {
                registryExists = handle.createQuery(
                                """
                                SELECT count(*) FROM information_schema.tables
                                WHERE table_schema = :schema AND table_name = 'ducklake_inlined_data_tables'""")
                        .bind("schema", metadataSchema)
                        .mapTo(Long.class)
                        .one() > 0;
            }
            catch (JdbiException e) {
                throw metastoreError(e);
            }
            inlinedDataTablesRegistryExists = registryExists;
        }
        return registryExists;
    }

    private boolean dataFileHasPartialMax()
    {
        Boolean hasPartialMax = dataFileHasPartialMax;
        if (hasPartialMax == null) {
            try (Handle handle = jdbi.open()) {
                hasPartialMax = handle.createQuery(
                                """
                                SELECT count(*) FROM information_schema.columns
                                WHERE table_schema = :schema AND table_name = 'ducklake_data_file' AND column_name = 'partial_max'""")
                        .bind("schema", metadataSchema)
                        .mapTo(Long.class)
                        .one() > 0;
            }
            catch (JdbiException e) {
                throw metastoreError(e);
            }
            dataFileHasPartialMax = hasPartialMax;
        }
        return hasPartialMax;
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
        return "begin_snapshot <= :snapshot AND (" + alias + ".end_snapshot IS NULL OR " + alias + ".end_snapshot > :snapshot)";
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

    private static OptionalLong optionalLong(ResultSet resultSet, String columnName)
            throws SQLException
    {
        long value = resultSet.getLong(columnName);
        if (resultSet.wasNull()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(value);
    }

    private static Optional<Boolean> optionalBoolean(ResultSet resultSet, String columnName)
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
