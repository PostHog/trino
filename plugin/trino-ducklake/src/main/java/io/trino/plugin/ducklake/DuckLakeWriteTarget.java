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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Everything the workers need to write into one table: where its files go, which columns they
 * hold, and how the rows are laid out across files.
 * <p>
 * The same description serves {@code INSERT} and {@code CREATE TABLE AS}. For the latter the table
 * already exists in the catalog by the time rows are written — the statement creates it in its own
 * snapshot first — so nothing distinguishes the two on the write side.
 */
public record DuckLakeWriteTarget(
        @JsonProperty SchemaTableName tableName,
        @JsonProperty long tableId,
        @JsonProperty String tableLocation,
        @JsonProperty List<DuckLakeWriteColumn> columns,
        @JsonProperty Optional<DuckLakePartitioning> partitioning)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    @JsonCreator
    public DuckLakeWriteTarget
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        columns = ImmutableList.copyOf(columns);
        requireNonNull(partitioning, "partitioning is null");
    }

    @Override
    public String toString()
    {
        return tableName.toString();
    }
}
