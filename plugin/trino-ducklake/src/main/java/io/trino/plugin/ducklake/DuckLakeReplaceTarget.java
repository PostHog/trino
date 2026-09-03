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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The table a {@code CREATE OR REPLACE TABLE ... AS SELECT} is about to define, which does not
 * exist in the catalog yet.
 * <p>
 * The name it takes still resolves to the table being replaced, holding the rows it has always
 * held. Creating the new table in its own snapshot first, the way {@link DuckLakeWriteTarget} does,
 * would leave the name resolving to an empty table until the rows landed. So the definition travels
 * with the write instead, and the commit that registers the data files is the one that ends the old
 * table and creates this one.
 * <p>
 * Nothing here identifies a catalog object: the table identifier and the identifier of its
 * partitioning scheme are drawn by that commit. Only what the workers need to write the files is
 * settled in advance, which is the column layout and the location the files go to.
 */
public record DuckLakeReplaceTarget(
        @JsonProperty SchemaTableName tableName,
        @JsonProperty String tableLocation,
        @JsonProperty List<DuckLakeWriteColumn> columns,
        @JsonProperty List<DuckLakePartitioning.Field> partitionFields,
        @JsonProperty Map<Long, String> columnComments,
        @JsonProperty Optional<String> comment)
        implements ConnectorOutputTableHandle
{
    @JsonCreator
    public DuckLakeReplaceTarget
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        columns = ImmutableList.copyOf(columns);
        partitionFields = ImmutableList.copyOf(partitionFields);
        columnComments = ImmutableMap.copyOf(columnComments);
        requireNonNull(comment, "comment is null");
    }

    @Override
    public String toString()
    {
        return tableName.toString();
    }
}
