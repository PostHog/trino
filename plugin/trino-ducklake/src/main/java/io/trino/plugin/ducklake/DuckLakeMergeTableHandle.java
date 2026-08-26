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
import io.trino.spi.connector.ConnectorMergeTableHandle;

import static java.util.Objects.requireNonNull;

/**
 * The target of a row-level change. Rows a merge writes go into new data files exactly as an
 * insert would, so the write target is the same; the table handle is kept alongside it because
 * finishing the merge has to reason about the files the rows being removed came from.
 */
public record DuckLakeMergeTableHandle(
        @JsonProperty DuckLakeTableHandle tableHandle,
        @JsonProperty DuckLakeWriteTarget writeTarget)
        implements ConnectorMergeTableHandle
{
    @JsonCreator
    public DuckLakeMergeTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(writeTarget, "writeTarget is null");
    }

    @Override
    public DuckLakeTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
