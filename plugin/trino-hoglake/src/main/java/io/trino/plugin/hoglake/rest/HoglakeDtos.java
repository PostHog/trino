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
package io.trino.plugin.hoglake.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hoglake.HoglakeDeletionVector;

import java.util.List;
import java.util.Map;

/**
 * Wire DTOs for the hoglake REST API (openapi/hoglake.yaml, snake_case
 * wire). Read planning and initial table creation/append operations.
 */
public final class HoglakeDtos
{
    private HoglakeDtos() {}

    public record ColumnDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("type_params") Map<String, Object> typeParams,
            @JsonProperty("nullable") boolean nullable,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) @JsonProperty("children") List<ColumnDefinition> children,
            @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("comment") String comment)
    {
        public ColumnDefinition
        {
            children = children == null ? List.of() : List.copyOf(children);
        }

        public ColumnDefinition(String name, String type, Map<String, Object> typeParams, boolean nullable, List<ColumnDefinition> children)
        {
            this(name, type, typeParams, nullable, children, null);
        }

        public ColumnDefinition withComment(String comment)
        {
            return new ColumnDefinition(name, type, typeParams, nullable, children, comment);
        }

        public boolean hasComments()
        {
            return comment != null || children.stream().anyMatch(ColumnDefinition::hasComments);
        }

        public ColumnDefinition(String name, String type, Map<String, Object> typeParams, boolean nullable)
        {
            this(name, type, typeParams, nullable, List.of());
        }
    }

    public record CreateTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ColumnDefinition> columns) {}

    public record FileRegistration(
            @JsonProperty("path") String path,
            @JsonProperty("record_count") long recordCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("footer_size") long footerSize,
            @JsonInclude(JsonInclude.Include.NON_EMPTY) @JsonProperty("partition_values") List<String> partitionValues)
    {
        public FileRegistration
        {
            partitionValues = partitionValues == null ? List.of() : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(partitionValues));
        }

        public FileRegistration(String path, long recordCount, long fileSizeBytes, long footerSize)
        {
            this(path, recordCount, fileSizeBytes, footerSize, List.of());
        }
    }

    public record PartitionField(
            @JsonProperty("source_field_id") long sourceFieldId,
            @JsonProperty("transform") String transform,
            @JsonProperty("transform_param") Integer transformParam) {}

    public record SortField(
            @JsonProperty("source_field_id") long sourceFieldId,
            @JsonProperty("direction") String direction,
            @JsonProperty("null_order") String nullOrder) {}

    public record Append(
            @JsonProperty("namespace") String namespace,
            @JsonProperty("table") String table,
            @JsonProperty("expected_table_uuid") String expectedTableUuid,
            @JsonProperty("files") List<FileRegistration> files) {}

    public record CommitResult(
            @JsonProperty("snapshot_id") long snapshotId) {}

    public record CommitReceipt(
            @JsonProperty("operation_id") String operationId,
            @JsonProperty("snapshot_id") long snapshotId) {}

    public record DeleteRegistration(@JsonProperty("data_file_id") long dataFileId, @JsonProperty("path") String path, @JsonProperty("delete_count") long deleteCount, @JsonProperty("file_size_bytes") long fileSizeBytes) {}

    public record Deletes(@JsonProperty("namespace") String namespace, @JsonProperty("table") String table, @JsonProperty("expected_table_uuid") String expectedTableUuid, @JsonProperty("files") List<DeleteRegistration> files) {}

    public record Commit(
            @JsonProperty("read_snapshot") long readSnapshot,
            @JsonProperty("appends") List<Append> appends,
            @JsonProperty("deletes") List<Deletes> deletes,
            @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty("idempotency_key") String operationId)
    {
        public Commit(long readSnapshot, List<Append> appends, String operationId)
        {
            this(readSnapshot, appends, List.of(), operationId);
        }

        public Commit(long readSnapshot, List<Append> appends)
        {
            this(readSnapshot, appends, List.of(), null);
        }
    }

    public record Catalog(
            @JsonProperty("name") String name,
            @JsonProperty("data_path") String dataPath,
            @JsonProperty("head_snapshot_id") long headSnapshotId,
            @JsonProperty("schema_version") long schemaVersion,
            @JsonProperty("capabilities") List<String> capabilities)
    {
        public Catalog(String name, String dataPath, long headSnapshotId, long schemaVersion)
        {
            this(name, dataPath, headSnapshotId, schemaVersion, List.of());
        }
    }

    public record ReplacementTarget(
            @JsonProperty("expected_table_uuid") String expectedTableUuid,
            @JsonProperty("read_snapshot") long readSnapshot) {}

    public record TableCreation(
            @JsonProperty("operation_id") String operationId,
            @JsonProperty("table_uuid") String tableUuid,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("write_path") String writePath,
            @JsonProperty("state") String state,
            @JsonProperty("snapshot_id") Long snapshotId,
            @JsonProperty("reason") String reason) {}

    public record Namespace(
            @JsonProperty("name") String name,
            @JsonProperty("namespace_id") Long namespaceId) {}

    public record TableSummary(
            @JsonProperty("name") String name,
            @JsonProperty("table_uuid") String tableUuid) {}

    public record Column(
            @JsonProperty("field_id") long fieldId,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("type_params") Map<String, Object> typeParams,
            @JsonProperty("nullable") Boolean nullable,
            @JsonProperty("children") List<Column> children,
            @JsonProperty("comment") String comment)
    {
        public Column(long fieldId, int ordinal, String name, String type, Map<String, Object> typeParams, Boolean nullable, List<Column> children)
        {
            this(fieldId, ordinal, name, type, typeParams, nullable, children, null);
        }

        public Column(long fieldId, int ordinal, String name, String type, Map<String, Object> typeParams, Boolean nullable)
        {
            this(fieldId, ordinal, name, type, typeParams, nullable, List.of());
        }

        public Column
        {
            children = children == null ? List.of() : List.copyOf(children);
        }

        public boolean isNullable()
        {
            return nullable == null || nullable;
        }
    }

    public record Table(
            @JsonProperty("name") String name,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("table_uuid") String tableUuid,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("record_count") long recordCount,
            @JsonProperty("file_count") long fileCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("partition_spec") Map<String, Object> partitionSpec,
            @JsonProperty("sort_spec") Map<String, Object> sortSpec,
            @JsonProperty("comment") String comment,
            @JsonProperty("properties") Map<String, String> properties)
    {
        public Table
        {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }

        public Table(String name, String namespace, String tableUuid, List<Column> columns, long recordCount, long fileCount, long fileSizeBytes, Map<String, Object> partitionSpec, Map<String, Object> sortSpec)
        {
            this(name, namespace, tableUuid, columns, recordCount, fileCount, fileSizeBytes, partitionSpec, sortSpec, null, Map.of());
        }

        public Table(String name, String namespace, String tableUuid, List<Column> columns, long recordCount, long fileCount, long fileSizeBytes, Map<String, Object> partitionSpec)
        {
            this(name, namespace, tableUuid, columns, recordCount, fileCount, fileSizeBytes, partitionSpec, null);
        }

        public Table(String name, String namespace, String tableUuid, List<Column> columns, long recordCount, long fileCount, long fileSizeBytes)
        {
            this(name, namespace, tableUuid, columns, recordCount, fileCount, fileSizeBytes, null, null);
        }
    }

    public record DataFile(
            @JsonProperty("data_file_id") long dataFileId,
            @JsonProperty("path") String path,
            @JsonProperty("file_format") String fileFormat,
            @JsonProperty("record_count") long recordCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("footer_size") Long footerSize,
            @JsonProperty("row_id_start") long rowIdStart,
            @JsonProperty("stats_state") String statsState,
            @JsonProperty("begin_snapshot") long beginSnapshot) {}

    public record DeleteFile(
            @JsonProperty("delete_file_id") long deleteFileId,
            @JsonProperty("data_file_id") long dataFileId,
            @JsonProperty("path") String path,
            @JsonProperty("file_format") String fileFormat,
            @JsonProperty("delete_count") long deleteCount,
            @JsonProperty("file_size_bytes") long fileSizeBytes,
            @JsonProperty("begin_snapshot") long beginSnapshot)
    {
        /**
         * The format the connector reads, for callers that do not carry the
         * wire's {@code file_format} (the scan response always does).
         */
        public DeleteFile(
                long deleteFileId,
                long dataFileId,
                String path,
                long deleteCount,
                long fileSizeBytes,
                long beginSnapshot)
        {
            this(deleteFileId, dataFileId, path, HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT, deleteCount, fileSizeBytes, beginSnapshot);
        }
    }

    /**
     * One /scan entry: a data file paired with its live deletion vector, if any.
     */
    public record ScanFile(
            @JsonProperty("data_file") DataFile dataFile,
            @JsonProperty("delete_file") DeleteFile deleteFile) {}
}
