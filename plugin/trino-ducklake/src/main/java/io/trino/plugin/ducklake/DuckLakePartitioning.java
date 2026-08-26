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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * How a table's rows are laid out across data files.
 * <p>
 * Each key is a column of the table and a transform applied to it. {@code identity} files rows by
 * the value itself; the temporal transforms file them by a component of a date or timestamp, so
 * that a predicate on the source column can skip whole files.
 */
public record DuckLakePartitioning(@JsonProperty long partitionId, @JsonProperty List<Field> fields)
{
    @JsonCreator
    public DuckLakePartitioning
    {
        fields = ImmutableList.copyOf(fields);
    }

    /**
     * One partition key: which column of the row it reads, under which name it is filed, and how
     * the value is derived.
     */
    public record Field(
            @JsonProperty int sourceChannel,
            @JsonProperty long columnId,
            @JsonProperty String columnName,
            @JsonProperty String transform)
    {
        @JsonCreator
        public Field
        {
            requireNonNull(columnName, "columnName is null");
            requireNonNull(transform, "transform is null");
        }
    }
}
