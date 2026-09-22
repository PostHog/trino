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
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hoglake.rest.HoglakeDtos.PartitionField;
import io.trino.plugin.hoglake.rest.HoglakeDtos.SortField;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record HoglakeWriteHandle(
        @JsonProperty("namespace") String namespace,
        @JsonProperty("table") String table,
        @JsonProperty("tableUuid") String tableUuid,
        @JsonProperty("snapshot") long snapshot,
        @JsonProperty("dataPath") String dataPath,
        @JsonProperty("columns") List<HoglakeColumnHandle> columns,
        @JsonProperty("inputColumns") List<HoglakeColumnHandle> inputColumns,
        @JsonProperty("creationOperation") Optional<String> creationOperation,
        @JsonProperty("insertOperation") Optional<String> insertOperation,
        @JsonProperty("partitionFields") List<PartitionField> partitionFields,
        @JsonProperty("sortFields") List<SortField> sortFields,
        @JsonProperty("claimUploads") boolean claimUploads)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    public HoglakeWriteHandle(String namespace, String table, String tableUuid, long snapshot, String dataPath, List<HoglakeColumnHandle> columns, List<HoglakeColumnHandle> inputColumns, Optional<String> creationOperation)
    {
        this(namespace, table, tableUuid, snapshot, dataPath, columns, inputColumns, creationOperation, Optional.empty());
    }

    public HoglakeWriteHandle(String namespace, String table, String tableUuid, long snapshot, String dataPath, List<HoglakeColumnHandle> columns, List<HoglakeColumnHandle> inputColumns, Optional<String> creationOperation, Optional<String> insertOperation)
    {
        this(namespace, table, tableUuid, snapshot, dataPath, columns, inputColumns, creationOperation, insertOperation, List.of());
    }

    public HoglakeWriteHandle(String namespace, String table, String tableUuid, long snapshot, String dataPath, List<HoglakeColumnHandle> columns, List<HoglakeColumnHandle> inputColumns, Optional<String> creationOperation, Optional<String> insertOperation, List<PartitionField> partitionFields)
    {
        this(namespace, table, tableUuid, snapshot, dataPath, columns, inputColumns, creationOperation, insertOperation, partitionFields, List.of());
    }

    public HoglakeWriteHandle(String namespace, String table, String tableUuid, long snapshot, String dataPath, List<HoglakeColumnHandle> columns, List<HoglakeColumnHandle> inputColumns, Optional<String> creationOperation, Optional<String> insertOperation, List<PartitionField> partitionFields, List<SortField> sortFields)
    {
        this(namespace, table, tableUuid, snapshot, dataPath, columns, inputColumns, creationOperation, insertOperation, partitionFields, sortFields, false);
    }

    @JsonCreator
    public HoglakeWriteHandle
    {
        requireNonNull(namespace, "namespace is null");
        requireNonNull(table, "table is null");
        requireNonNull(tableUuid, "tableUuid is null");
        requireNonNull(dataPath, "dataPath is null");
        requireNonNull(insertOperation, "insertOperation is null");
        requireNonNull(creationOperation, "creationOperation is null");
        partitionFields = ImmutableList.copyOf(partitionFields);
        sortFields = ImmutableList.copyOf(sortFields);
        columns = ImmutableList.copyOf(columns);
        inputColumns = ImmutableList.copyOf(inputColumns);
    }
}
