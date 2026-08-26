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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * A column of a table being written, with the identifiers of its nested fields.
 * <p>
 * DuckLake numbers every field of a nested type, not just the top-level column, and readers match
 * the fields of a Parquet file to the catalog by those identifiers. The writer therefore needs the
 * whole column tree rather than the top-level handle the read path uses.
 */
public record DuckLakeWriteColumn(
        @JsonProperty long columnId,
        @JsonProperty String name,
        @JsonProperty String duckLakeType,
        @JsonProperty Type type,
        @JsonProperty boolean nullsAllowed,
        @JsonProperty List<DuckLakeWriteColumn> children)
{
    @JsonCreator
    public DuckLakeWriteColumn
    {
        requireNonNull(name, "name is null");
        requireNonNull(duckLakeType, "duckLakeType is null");
        requireNonNull(type, "type is null");
        children = ImmutableList.copyOf(children);
    }

    /**
     * Every field of the column tree in depth-first order, the order data file statistics are
     * keyed by. Only fields holding values of their own carry statistics, but the identifiers are
     * assigned across all of them.
     */
    @JsonIgnore
    public List<DuckLakeWriteColumn> flatten()
    {
        ImmutableList.Builder<DuckLakeWriteColumn> fields = ImmutableList.builder();
        flattenInto(fields);
        return fields.build();
    }

    private void flattenInto(ImmutableList.Builder<DuckLakeWriteColumn> fields)
    {
        fields.add(this);
        for (DuckLakeWriteColumn child : children) {
            child.flattenInto(fields);
        }
    }

    public static List<DuckLakeWriteColumn> flatten(List<DuckLakeWriteColumn> columns)
    {
        return columns.stream()
                .flatMap(column -> column.flatten().stream())
                .collect(toImmutableList());
    }
}
