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
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.Handle;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Builds one DuckLake snapshot inside a single database transaction.
 * <p>
 * Every change to a DuckLake catalog creates a snapshot. Rows of the {@code ducklake_*} tables
 * carry the snapshot range they are visible in: a row is created with {@code begin_snapshot} set
 * to the snapshot that introduced it and stays visible until a later snapshot sets its
 * {@code end_snapshot}. Updating an object therefore means ending its current row and inserting a
 * replacement carrying the same identifier, which keeps older snapshots readable.
 * <p>
 * Object identifiers come from two counters carried on the snapshot: schemas, tables, views and
 * partitioning schemes draw from {@code next_catalog_id}, data and delete files draw from
 * {@code next_file_id}. Column identifiers are numbered per table instead and are not drawn from
 * either counter.
 */
public final class DuckLakeCommit
{
    private final Handle handle;
    private final String metadataSchema;
    private final long baseSnapshotId;
    private final long snapshotId;
    private final long baseSchemaVersion;

    private long nextCatalogId;
    private long nextFileId;
    private long schemaVersion;
    private final List<String> changes = new ArrayList<>();
    private final Set<Long> tablesWithSchemaChange = new LinkedHashSet<>();

    DuckLakeCommit(Handle handle, String metadataSchema, SnapshotState state)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.metadataSchema = requireNonNull(metadataSchema, "metadataSchema is null");
        this.baseSnapshotId = state.snapshotId();
        this.snapshotId = state.snapshotId() + 1;
        this.baseSchemaVersion = state.schemaVersion();
        this.schemaVersion = state.schemaVersion();
        this.nextCatalogId = state.nextCatalogId();
        this.nextFileId = state.nextFileId();
    }

    /**
     * The snapshot this commit creates. Rows written by the commit become visible at it.
     */
    public long snapshotId()
    {
        return snapshotId;
    }

    /**
     * The newest snapshot that existed when the commit started. Reads that decide what the commit
     * changes must use it, so that the commit is computed against a single consistent state.
     */
    public long baseSnapshotId()
    {
        return baseSnapshotId;
    }

    public long allocateCatalogId()
    {
        return nextCatalogId++;
    }

    public long allocateFileId()
    {
        return nextFileId++;
    }

    /**
     * Records a change for {@code ducklake_snapshot_changes}. DuckDB reads these to invalidate its
     * caches, so the vocabulary has to match the one it writes.
     */
    public void recordChange(String change)
    {
        changes.add(requireNonNull(change, "change is null"));
    }

    public void recordCreatedSchema(String schemaName)
    {
        recordChange("created_schema:" + quoteName(schemaName));
        bumpSchemaVersion();
    }

    public void recordDroppedSchema(long schemaId)
    {
        recordChange("dropped_schema:" + schemaId);
        bumpSchemaVersion();
    }

    public void recordCreatedTable(String schemaName, String tableName, long tableId)
    {
        recordChange("created_table:" + quoteName(schemaName) + "." + quoteName(tableName));
        bumpSchemaVersion();
        tablesWithSchemaChange.add(tableId);
    }

    public void recordAlteredTable(long tableId)
    {
        recordChange("altered_table:" + tableId);
        bumpSchemaVersion();
        tablesWithSchemaChange.add(tableId);
    }

    public void recordDroppedTable(long tableId)
    {
        recordChange("dropped_table:" + tableId);
        bumpSchemaVersion();
    }

    public void recordCreatedView(String schemaName, String viewName)
    {
        recordChange("created_view:" + quoteName(schemaName) + "." + quoteName(viewName));
        bumpSchemaVersion();
    }

    public void recordDroppedView(long viewId)
    {
        recordChange("dropped_view:" + viewId);
        bumpSchemaVersion();
    }

    public void recordInsert(long tableId)
    {
        recordChange("inserted_into_table:" + tableId);
    }

    public void recordDelete(long tableId)
    {
        recordChange("deleted_from_table:" + tableId);
    }

    /**
     * Records a change that only alters a table's metadata, such as a comment, without changing
     * the columns readers have to resolve. DuckDB bumps the catalog schema version for these but
     * does not record a new schema version for the table itself.
     */
    public void recordTableMetadataChange(long tableId)
    {
        recordChange("altered_table:" + tableId);
        bumpSchemaVersion();
    }

    private void bumpSchemaVersion()
    {
        if (schemaVersion == baseSchemaVersion) {
            schemaVersion = baseSchemaVersion + 1;
        }
    }

    // ------------------------------------------------------------------
    // schemas
    // ------------------------------------------------------------------

    public long insertSchema(String schemaName, String path)
    {
        long schemaId = allocateCatalogId();
        insertSchemaRow(schemaId, schemaName, path, true);
        return schemaId;
    }

    public void insertSchemaRow(long schemaId, String schemaName, String path, boolean pathIsRelative)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
                        VALUES (:schemaId, %s, :snapshot, NULL, :schemaName, :path, :pathIsRelative)""".formatted(table("ducklake_schema"), randomUuid()))
                .bind("schemaId", schemaId)
                .bind("snapshot", snapshotId)
                .bind("schemaName", schemaName)
                .bind("path", path)
                .bind("pathIsRelative", pathIsRelative)
                .execute();
    }

    public void endSchema(long schemaId)
    {
        endRows("ducklake_schema", "schema_id = :objectId", schemaId);
    }

    // ------------------------------------------------------------------
    // tables
    // ------------------------------------------------------------------

    public void insertTableRow(long tableId, long schemaId, String tableName, String path, Optional<String> tableUuid)
    {
        String uuidExpression = tableUuid.map(_ -> ":tableUuid" + uuidCast()).orElseGet(DuckLakeCommit::randomUuid);
        var update = handle.createUpdate(
                        """
                        INSERT INTO %s (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
                        VALUES (:tableId, %s, :snapshot, NULL, :schemaId, :tableName, :path, true)""".formatted(table("ducklake_table"), uuidExpression))
                .bind("tableId", tableId)
                .bind("snapshot", snapshotId)
                .bind("schemaId", schemaId)
                .bind("tableName", tableName)
                .bind("path", path);
        tableUuid.ifPresent(uuid -> update.bind("tableUuid", uuid));
        update.execute();
    }

    public void endTable(long tableId)
    {
        endRows("ducklake_table", "table_id = :objectId", tableId);
    }

    /**
     * Ends every row describing the table other than the table row itself: its columns, its
     * partitioning scheme, its data and delete files and its comments. Used when the table is
     * dropped, so that no part of it stays visible at later snapshots.
     */
    public void endTableContents(long tableId)
    {
        endRows("ducklake_column", "table_id = :objectId", tableId);
        endRows("ducklake_partition_info", "table_id = :objectId", tableId);
        endRows("ducklake_data_file", "table_id = :objectId", tableId);
        endRows("ducklake_delete_file", "table_id = :objectId", tableId);
        endRows("ducklake_column_tag", "table_id = :objectId", tableId);
        endRows("ducklake_tag", "object_id = :objectId", tableId);
    }

    public Optional<TableIdentity> findTable(String schemaName, String tableName)
    {
        return handle.createQuery(
                        """
                        SELECT t.table_id, t.schema_id, t.table_name, t.path, t.table_uuid
                        FROM %s t
                        JOIN %s s ON t.schema_id = s.schema_id
                        WHERE %s AND %s AND lower(s.schema_name) = :schemaName AND lower(t.table_name) = :tableName""".formatted(
                                table("ducklake_table"),
                                table("ducklake_schema"),
                                visible("t"),
                                visible("s")))
                .bind("snapshot", baseSnapshotId)
                .bind("schemaName", schemaName.toLowerCase(ENGLISH))
                .bind("tableName", tableName.toLowerCase(ENGLISH))
                .map((rs, _) -> new TableIdentity(
                        rs.getLong("table_id"),
                        rs.getLong("schema_id"),
                        rs.getString("table_name"),
                        Optional.ofNullable(rs.getString("path")).orElse(""),
                        Optional.ofNullable(rs.getString("table_uuid"))))
                .findFirst();
    }

    public Optional<SchemaIdentity> findSchema(String schemaName)
    {
        return handle.createQuery(
                        """
                        SELECT schema_id, schema_name, path, path_is_relative
                        FROM %s
                        WHERE %s AND lower(schema_name) = :schemaName""".formatted(table("ducklake_schema"), visibleUnaliased()))
                .bind("snapshot", baseSnapshotId)
                .bind("schemaName", schemaName.toLowerCase(ENGLISH))
                .map((rs, _) -> new SchemaIdentity(
                        rs.getLong("schema_id"),
                        rs.getString("schema_name"),
                        Optional.ofNullable(rs.getString("path")).orElse(""),
                        rs.getBoolean("path_is_relative")))
                .findFirst();
    }

    public boolean schemaHasRelations(long schemaId)
    {
        long count = handle.createQuery(
                        """
                        SELECT (SELECT count(*) FROM %s WHERE schema_id = :schemaId AND %s)
                             + (SELECT count(*) FROM %s WHERE schema_id = :schemaId AND %s)""".formatted(
                                table("ducklake_table"),
                                visibleUnaliased(),
                                table("ducklake_view"),
                                visibleUnaliased()))
                .bind("snapshot", baseSnapshotId)
                .bind("schemaId", schemaId)
                .mapTo(Long.class)
                .one();
        return count > 0;
    }

    // ------------------------------------------------------------------
    // columns
    // ------------------------------------------------------------------

    public void insertColumn(long tableId, DuckLakeColumnRow column)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type,
                            initial_default, default_value, nulls_allowed, parent_column)
                        VALUES (:columnId, :snapshot, NULL, :tableId, :columnOrder, :columnName, :columnType,
                            :initialDefault, :defaultValue, :nullsAllowed, :parentColumn)""".formatted(table("ducklake_column")))
                .bind("columnId", column.columnId())
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .bind("columnOrder", column.columnOrder())
                .bind("columnName", column.columnName())
                .bind("columnType", column.columnType())
                .bind("initialDefault", column.initialDefault().orElse(null))
                .bind("defaultValue", column.defaultValue().orElse(null))
                .bind("nullsAllowed", column.nullsAllowed())
                .bind("parentColumn", column.parentColumn().isPresent() ? column.parentColumn().orElseThrow() : null)
                .execute();
    }

    public void endColumn(long tableId, long columnId)
    {
        handle.createUpdate(
                        "UPDATE %s SET end_snapshot = :snapshot WHERE table_id = :tableId AND column_id = :columnId AND end_snapshot IS NULL".formatted(table("ducklake_column")))
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .bind("columnId", columnId)
                .execute();
    }

    public List<DuckLakeColumnRow> columns(long tableId)
    {
        return handle.createQuery(
                        """
                        SELECT column_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column
                        FROM %s
                        WHERE table_id = :tableId AND %s
                        ORDER BY parent_column NULLS FIRST, column_order""".formatted(table("ducklake_column"), visibleUnaliased()))
                .bind("snapshot", baseSnapshotId)
                .bind("tableId", tableId)
                .map((rs, _) -> new DuckLakeColumnRow(
                        rs.getLong("column_id"),
                        rs.getLong("column_order"),
                        rs.getString("column_name"),
                        rs.getString("column_type"),
                        JdbcDuckLakeMetastore.optionalLong(rs, "parent_column"),
                        rs.getBoolean("nulls_allowed"),
                        Optional.ofNullable(rs.getString("initial_default")),
                        Optional.ofNullable(rs.getString("default_value"))))
                .list();
    }

    /**
     * The largest column identifier the table has ever used, including columns that were dropped.
     * New columns must be numbered above it, because data files written before a column was
     * dropped still carry its identifier.
     */
    public long maxColumnId(long tableId)
    {
        return handle.createQuery("SELECT coalesce(max(column_id), 0) FROM " + table("ducklake_column") + " WHERE table_id = :tableId")
                .bind("tableId", tableId)
                .mapTo(Long.class)
                .one();
    }

    // ------------------------------------------------------------------
    // comments
    // ------------------------------------------------------------------

    public void setTableTag(long tableId, String key, Optional<String> value)
    {
        handle.createUpdate(
                        "UPDATE %s SET end_snapshot = :snapshot WHERE object_id = :objectId AND key = :key AND end_snapshot IS NULL".formatted(table("ducklake_tag")))
                .bind("snapshot", snapshotId)
                .bind("objectId", tableId)
                .bind("key", key)
                .execute();
        if (value.isEmpty()) {
            return;
        }
        handle.createUpdate(
                        """
                        INSERT INTO %s (object_id, begin_snapshot, end_snapshot, key, value)
                        VALUES (:objectId, :snapshot, NULL, :key, :value)""".formatted(table("ducklake_tag")))
                .bind("objectId", tableId)
                .bind("snapshot", snapshotId)
                .bind("key", key)
                .bind("value", value.get())
                .execute();
    }

    public void setColumnTag(long tableId, long columnId, String key, Optional<String> value)
    {
        handle.createUpdate(
                        """
                        UPDATE %s SET end_snapshot = :snapshot
                        WHERE table_id = :tableId AND column_id = :columnId AND key = :key AND end_snapshot IS NULL""".formatted(table("ducklake_column_tag")))
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .bind("columnId", columnId)
                .bind("key", key)
                .execute();
        if (value.isEmpty()) {
            return;
        }
        handle.createUpdate(
                        """
                        INSERT INTO %s (table_id, column_id, begin_snapshot, end_snapshot, key, value)
                        VALUES (:tableId, :columnId, :snapshot, NULL, :key, :value)""".formatted(table("ducklake_column_tag")))
                .bind("tableId", tableId)
                .bind("columnId", columnId)
                .bind("snapshot", snapshotId)
                .bind("key", key)
                .bind("value", value.get())
                .execute();
    }

    // ------------------------------------------------------------------
    // views
    // ------------------------------------------------------------------

    public void insertViewRow(long viewId, long schemaId, String viewName, String dialect, String sql, String columnAliases)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases)
                        VALUES (:viewId, %s, :snapshot, NULL, :schemaId, :viewName, :dialect, :sql, :columnAliases)""".formatted(table("ducklake_view"), randomUuid()))
                .bind("viewId", viewId)
                .bind("snapshot", snapshotId)
                .bind("schemaId", schemaId)
                .bind("viewName", viewName)
                .bind("dialect", dialect)
                .bind("sql", sql)
                .bind("columnAliases", columnAliases)
                .execute();
    }

    public void endView(long viewId)
    {
        endRows("ducklake_view", "view_id = :objectId", viewId);
    }

    public Optional<ViewIdentity> findView(String schemaName, String viewName)
    {
        return handle.createQuery(
                        """
                        SELECT v.view_id, v.dialect
                        FROM %s v
                        JOIN %s s ON v.schema_id = s.schema_id
                        WHERE %s AND %s AND lower(s.schema_name) = :schemaName AND lower(v.view_name) = :viewName""".formatted(
                                table("ducklake_view"),
                                table("ducklake_schema"),
                                visible("v"),
                                visible("s")))
                .bind("snapshot", baseSnapshotId)
                .bind("schemaName", schemaName.toLowerCase(ENGLISH))
                .bind("viewName", viewName.toLowerCase(ENGLISH))
                .map((rs, _) -> new ViewIdentity(rs.getLong("view_id"), Optional.ofNullable(rs.getString("dialect")).orElse("")))
                .findFirst();
    }

    // ------------------------------------------------------------------
    // partitioning
    // ------------------------------------------------------------------

    public void endPartitioning(long tableId)
    {
        endRows("ducklake_partition_info", "table_id = :objectId", tableId);
    }

    public long insertPartitioning(long tableId, List<DuckLakePartitionColumn> columns)
    {
        long partitionId = allocateCatalogId();
        handle.createUpdate(
                        """
                        INSERT INTO %s (partition_id, table_id, begin_snapshot, end_snapshot)
                        VALUES (:partitionId, :tableId, :snapshot, NULL)""".formatted(table("ducklake_partition_info")))
                .bind("partitionId", partitionId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshotId)
                .execute();
        for (DuckLakePartitionColumn column : columns) {
            handle.createUpdate(
                            """
                            INSERT INTO %s (partition_id, table_id, partition_key_index, column_id, transform)
                            VALUES (:partitionId, :tableId, :keyIndex, :columnId, :transform)""".formatted(table("ducklake_partition_column")))
                    .bind("partitionId", partitionId)
                    .bind("tableId", tableId)
                    .bind("keyIndex", column.partitionKeyIndex())
                    .bind("columnId", column.columnId())
                    .bind("transform", column.transform())
                    .execute();
        }
        return partitionId;
    }

    // ------------------------------------------------------------------
    // data and delete files
    // ------------------------------------------------------------------

    public void insertDataFile(long tableId, long dataFileId, DataFileRow file)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative,
                            file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id)
                        VALUES (:dataFileId, :tableId, :snapshot, NULL, NULL, :path, true,
                            'parquet', :recordCount, :fileSizeBytes, :footerSize, :rowIdStart, :partitionId, NULL, NULL)""".formatted(table("ducklake_data_file")))
                .bind("dataFileId", dataFileId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshotId)
                .bind("path", file.path())
                .bind("recordCount", file.recordCount())
                .bind("fileSizeBytes", file.fileSizeBytes())
                .bind("footerSize", file.footerSize())
                .bind("rowIdStart", file.rowIdStart())
                .bind("partitionId", file.partitionId().isPresent() ? file.partitionId().orElseThrow() : null)
                .execute();
    }

    public void insertFilePartitionValue(long tableId, long dataFileId, int partitionKeyIndex, Optional<String> value)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (data_file_id, table_id, partition_key_index, partition_value)
                        VALUES (:dataFileId, :tableId, :keyIndex, :value)""".formatted(table("ducklake_file_partition_value")))
                .bind("dataFileId", dataFileId)
                .bind("tableId", tableId)
                .bind("keyIndex", partitionKeyIndex)
                .bind("value", value.orElse(null))
                .execute();
    }

    public void insertFileColumnStats(long tableId, long dataFileId, DuckLakeFileColumnStatsRow stats)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan)
                        VALUES (:dataFileId, :tableId, :columnId, :columnSizeBytes, :valueCount, :nullCount, :minValue, :maxValue, :containsNan)""".formatted(
                                table("ducklake_file_column_stats")))
                .bind("dataFileId", dataFileId)
                .bind("tableId", tableId)
                .bind("columnId", stats.columnId())
                .bind("columnSizeBytes", stats.columnSizeBytes().isPresent() ? stats.columnSizeBytes().orElseThrow() : null)
                .bind("valueCount", stats.valueCount().isPresent() ? stats.valueCount().orElseThrow() : null)
                .bind("nullCount", stats.nullCount().isPresent() ? stats.nullCount().orElseThrow() : null)
                .bind("minValue", stats.minValue().orElse(null))
                .bind("maxValue", stats.maxValue().orElse(null))
                .bind("containsNan", stats.containsNan().orElse(null))
                .execute();
    }

    public void endDataFiles(long tableId, List<Long> dataFileIds)
    {
        if (dataFileIds.isEmpty()) {
            return;
        }
        handle.createUpdate(
                        """
                        UPDATE %s SET end_snapshot = :snapshot
                        WHERE table_id = :tableId AND data_file_id IN (<dataFileIds>) AND end_snapshot IS NULL""".formatted(table("ducklake_data_file")))
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .bindList("dataFileIds", dataFileIds)
                .execute();
    }

    public void insertDeleteFile(long tableId, long deleteFileId, long dataFileId, String path, long deleteCount, long fileSizeBytes, long footerSize)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative,
                            format, delete_count, file_size_bytes, footer_size, encryption_key)
                        VALUES (:deleteFileId, :tableId, :snapshot, NULL, :dataFileId, :path, true,
                            'parquet', :deleteCount, :fileSizeBytes, :footerSize, NULL)""".formatted(table("ducklake_delete_file")))
                .bind("deleteFileId", deleteFileId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshotId)
                .bind("dataFileId", dataFileId)
                .bind("path", path)
                .bind("deleteCount", deleteCount)
                .bind("fileSizeBytes", fileSizeBytes)
                .bind("footerSize", footerSize)
                .execute();
    }

    public void endDeleteFilesFor(long tableId, List<Long> dataFileIds)
    {
        if (dataFileIds.isEmpty()) {
            return;
        }
        handle.createUpdate(
                        """
                        UPDATE %s SET end_snapshot = :snapshot
                        WHERE table_id = :tableId AND data_file_id IN (<dataFileIds>) AND end_snapshot IS NULL""".formatted(table("ducklake_delete_file")))
                .bind("snapshot", snapshotId)
                .bind("tableId", tableId)
                .bindList("dataFileIds", dataFileIds)
                .execute();
    }

    /**
     * The data files of the table visible at the base snapshot, with the delete file currently
     * applying to each. Used by statements that rewrite or remove whole files.
     */
    public List<VisibleDataFile> visibleDataFiles(long tableId)
    {
        return handle.createQuery(
                        """
                        SELECT f.data_file_id, f.path, f.path_is_relative, f.record_count, f.file_size_bytes,
                            d.delete_file_id, d.delete_count, d.file_size_bytes AS delete_file_size_bytes
                        FROM %s f
                        LEFT JOIN %s d ON f.data_file_id = d.data_file_id AND f.table_id = d.table_id AND %s
                        WHERE f.table_id = :tableId AND %s
                        ORDER BY f.data_file_id""".formatted(
                                table("ducklake_data_file"),
                                table("ducklake_delete_file"),
                                visible("d"),
                                visible("f")))
                .bind("snapshot", baseSnapshotId)
                .bind("tableId", tableId)
                .map((rs, _) -> new VisibleDataFile(
                        rs.getLong("data_file_id"),
                        rs.getString("path"),
                        rs.getBoolean("path_is_relative"),
                        rs.getLong("record_count"),
                        rs.getLong("file_size_bytes"),
                        JdbcDuckLakeMetastore.optionalLong(rs, "delete_file_id"),
                        JdbcDuckLakeMetastore.optionalLong(rs, "delete_count"),
                        JdbcDuckLakeMetastore.optionalLong(rs, "delete_file_size_bytes")))
                .list();
    }

    /**
     * Verifies that every given data file is still visible at the base snapshot. A statement that
     * rewrites files reads them before the commit starts; if a concurrent commit has replaced any
     * of them in the meantime the rewrite would resurrect rows, so the commit has to fail.
     */
    public void verifyDataFilesUnchanged(long tableId, Set<Long> dataFileIds)
    {
        if (dataFileIds.isEmpty()) {
            return;
        }
        long visible = handle.createQuery(
                        """
                        SELECT count(*) FROM %s
                        WHERE table_id = :tableId AND data_file_id IN (<dataFileIds>) AND %s""".formatted(table("ducklake_data_file"), visibleUnaliased()))
                .bind("snapshot", baseSnapshotId)
                .bind("tableId", tableId)
                .bindList("dataFileIds", ImmutableList.copyOf(dataFileIds))
                .mapTo(Long.class)
                .one();
        if (visible != dataFileIds.size()) {
            throw new ConcurrentModificationFailure("data files of table %s were modified by another transaction".formatted(tableId));
        }
    }

    /**
     * Verifies that the delete files the statement read are still the ones applying to their data
     * files. A concurrent delete that added positions to the same data file would otherwise be
     * lost when this commit replaces the delete file.
     */
    public void verifyDeleteFilesUnchanged(long tableId, Set<Long> dataFileIds, Set<Long> expectedDeleteFileIds)
    {
        if (dataFileIds.isEmpty()) {
            return;
        }
        List<Long> current = handle.createQuery(
                        """
                        SELECT delete_file_id FROM %s
                        WHERE table_id = :tableId AND data_file_id IN (<dataFileIds>) AND %s""".formatted(table("ducklake_delete_file"), visibleUnaliased()))
                .bind("snapshot", baseSnapshotId)
                .bind("tableId", tableId)
                .bindList("dataFileIds", ImmutableList.copyOf(dataFileIds))
                .mapTo(Long.class)
                .list();
        if (!Set.copyOf(current).equals(expectedDeleteFileIds)) {
            throw new ConcurrentModificationFailure("deletes of table %s were modified by another transaction".formatted(tableId));
        }
    }

    public void scheduleFileForDeletion(long dataFileId, String pathFromDataRoot)
    {
        handle.createUpdate(
                        """
                        INSERT INTO %s (data_file_id, path, path_is_relative, schedule_start)
                        VALUES (:dataFileId, :path, true, CURRENT_TIMESTAMP)""".formatted(table("ducklake_files_scheduled_for_deletion")))
                .bind("dataFileId", dataFileId)
                .bind("path", pathFromDataRoot)
                .execute();
    }

    // ------------------------------------------------------------------
    // statistics
    // ------------------------------------------------------------------

    public Optional<TableStatsRow> tableStats(long tableId)
    {
        return handle.createQuery("SELECT record_count, next_row_id, file_size_bytes FROM " + table("ducklake_table_stats") + " WHERE table_id = :tableId")
                .bind("tableId", tableId)
                .map((rs, _) -> new TableStatsRow(rs.getLong("record_count"), rs.getLong("next_row_id"), rs.getLong("file_size_bytes")))
                .findFirst();
    }

    public void writeTableStats(long tableId, TableStatsRow stats)
    {
        int updated = handle.createUpdate(
                        """
                        UPDATE %s SET record_count = :recordCount, next_row_id = :nextRowId, file_size_bytes = :fileSizeBytes
                        WHERE table_id = :tableId""".formatted(table("ducklake_table_stats")))
                .bind("tableId", tableId)
                .bind("recordCount", stats.recordCount())
                .bind("nextRowId", stats.nextRowId())
                .bind("fileSizeBytes", stats.fileSizeBytes())
                .execute();
        if (updated == 0) {
            handle.createUpdate(
                            """
                            INSERT INTO %s (table_id, record_count, next_row_id, file_size_bytes)
                            VALUES (:tableId, :recordCount, :nextRowId, :fileSizeBytes)""".formatted(table("ducklake_table_stats")))
                    .bind("tableId", tableId)
                    .bind("recordCount", stats.recordCount())
                    .bind("nextRowId", stats.nextRowId())
                    .bind("fileSizeBytes", stats.fileSizeBytes())
                    .execute();
        }
    }

    public List<DuckLakeTableColumnStats> tableColumnStats(long tableId)
    {
        return handle.createQuery(
                        """
                        SELECT column_id, contains_null, contains_nan, min_value, max_value
                        FROM %s WHERE table_id = :tableId""".formatted(table("ducklake_table_column_stats")))
                .bind("tableId", tableId)
                .map((rs, _) -> new DuckLakeTableColumnStats(
                        rs.getLong("column_id"),
                        JdbcDuckLakeMetastore.optionalBoolean(rs, "contains_null"),
                        JdbcDuckLakeMetastore.optionalBoolean(rs, "contains_nan"),
                        Optional.ofNullable(rs.getString("min_value")),
                        Optional.ofNullable(rs.getString("max_value"))))
                .list();
    }

    public void writeTableColumnStats(long tableId, DuckLakeTableColumnStats stats)
    {
        int updated = handle.createUpdate(
                        """
                        UPDATE %s SET contains_null = :containsNull, contains_nan = :containsNan, min_value = :minValue, max_value = :maxValue
                        WHERE table_id = :tableId AND column_id = :columnId""".formatted(table("ducklake_table_column_stats")))
                .bind("tableId", tableId)
                .bind("columnId", stats.columnId())
                .bind("containsNull", stats.containsNull().orElse(null))
                .bind("containsNan", stats.containsNan().orElse(null))
                .bind("minValue", stats.minValue().orElse(null))
                .bind("maxValue", stats.maxValue().orElse(null))
                .execute();
        if (updated == 0) {
            handle.createUpdate(
                            """
                            INSERT INTO %s (table_id, column_id, contains_null, contains_nan, min_value, max_value)
                            VALUES (:tableId, :columnId, :containsNull, :containsNan, :minValue, :maxValue)""".formatted(table("ducklake_table_column_stats")))
                    .bind("tableId", tableId)
                    .bind("columnId", stats.columnId())
                    .bind("containsNull", stats.containsNull().orElse(null))
                    .bind("containsNan", stats.containsNan().orElse(null))
                    .bind("minValue", stats.minValue().orElse(null))
                    .bind("maxValue", stats.maxValue().orElse(null))
                    .execute();
        }
    }

    public void deleteTableStats(long tableId)
    {
        handle.execute("DELETE FROM " + table("ducklake_table_stats") + " WHERE table_id = ?", tableId);
        handle.execute("DELETE FROM " + table("ducklake_table_column_stats") + " WHERE table_id = ?", tableId);
    }

    public void deleteTableColumnStats(long tableId, long columnId)
    {
        handle.execute("DELETE FROM " + table("ducklake_table_column_stats") + " WHERE table_id = ? AND column_id = ?", tableId, columnId);
    }

    // ------------------------------------------------------------------
    // finishing the snapshot
    // ------------------------------------------------------------------

    /**
     * The snapshot a reader should continue from. A commit whose action turned out to change
     * nothing writes no snapshot, so the base one is still the newest.
     */
    public long effectiveSnapshotId()
    {
        if (changes.isEmpty()) {
            return baseSnapshotId;
        }
        return snapshotId;
    }

    void writeSnapshot()
    {
        if (changes.isEmpty()) {
            return;
        }
        handle.createUpdate(
                        """
                        INSERT INTO %s (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                        VALUES (:snapshotId, CURRENT_TIMESTAMP, :schemaVersion, :nextCatalogId, :nextFileId)""".formatted(table("ducklake_snapshot")))
                .bind("snapshotId", snapshotId)
                .bind("schemaVersion", schemaVersion)
                .bind("nextCatalogId", nextCatalogId)
                .bind("nextFileId", nextFileId)
                .execute();
        handle.createUpdate(
                        """
                        INSERT INTO %s (snapshot_id, changes_made, author, commit_message, commit_extra_info)
                        VALUES (:snapshotId, :changes, NULL, NULL, NULL)""".formatted(table("ducklake_snapshot_changes")))
                .bind("snapshotId", snapshotId)
                .bind("changes", join(",", changes))
                .execute();
        for (long tableId : tablesWithSchemaChange) {
            handle.createUpdate(
                            """
                            INSERT INTO %s (begin_snapshot, schema_version, table_id)
                            VALUES (:snapshotId, :schemaVersion, :tableId)""".formatted(table("ducklake_schema_versions")))
                    .bind("snapshotId", snapshotId)
                    .bind("schemaVersion", schemaVersion)
                    .bind("tableId", tableId)
                    .execute();
        }
    }

    private void endRows(String tableName, String predicate, long objectId)
    {
        handle.createUpdate("UPDATE %s SET end_snapshot = :snapshot WHERE %s AND end_snapshot IS NULL".formatted(table(tableName), predicate))
                .bind("snapshot", snapshotId)
                .bind("objectId", objectId)
                .execute();
    }

    private String table(String tableName)
    {
        return "\"%s\".\"%s\"".formatted(metadataSchema.replace("\"", "\"\""), tableName.replace("\"", "\"\""));
    }

    private static String visible(String alias)
    {
        return "%s.begin_snapshot <= :snapshot AND (%s.end_snapshot IS NULL OR %s.end_snapshot > :snapshot)".formatted(alias, alias, alias);
    }

    private static String visibleUnaliased()
    {
        return "begin_snapshot <= :snapshot AND (end_snapshot IS NULL OR end_snapshot > :snapshot)";
    }

    /**
     * DuckDB stores a UUID for every schema, table and view. Nothing reads them back, so they are
     * generated by the database rather than carried through the connector.
     */
    private static String randomUuid()
    {
        return "cast(md5(random()::text || clock_timestamp()::text) AS uuid)";
    }

    private static String uuidCast()
    {
        return "::uuid";
    }

    private static String quoteName(String name)
    {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public record TableIdentity(long tableId, long schemaId, String tableName, String path, Optional<String> tableUuid) {}

    public record SchemaIdentity(long schemaId, String schemaName, String path, boolean pathIsRelative) {}

    public record ViewIdentity(long viewId, String dialect) {}

    public record TableStatsRow(long recordCount, long nextRowId, long fileSizeBytes) {}

    public record DataFileRow(String path, long recordCount, long fileSizeBytes, long footerSize, long rowIdStart, OptionalLong partitionId)
    {
        public DataFileRow
        {
            requireNonNull(path, "path is null");
            requireNonNull(partitionId, "partitionId is null");
        }
    }

    public record VisibleDataFile(
            long dataFileId,
            String path,
            boolean pathIsRelative,
            long recordCount,
            long fileSizeBytes,
            OptionalLong deleteFileId,
            OptionalLong deleteCount,
            OptionalLong deleteFileSizeBytes)
    {
        public VisibleDataFile
        {
            requireNonNull(path, "path is null");
        }

        public long visibleRecordCount()
        {
            return recordCount - deleteCount.orElse(0);
        }
    }

    /**
     * Signals that the state a statement was planned against changed before it could commit.
     * Retrying the commit cannot help, because the statement has to be replanned.
     */
    public static class ConcurrentModificationFailure
            extends TrinoException
    {
        public ConcurrentModificationFailure(String message)
        {
            super(DUCKLAKE_INVALID_METADATA, message);
        }
    }

    record SnapshotState(long snapshotId, long schemaVersion, long nextCatalogId, long nextFileId) {}
}
