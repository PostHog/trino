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
import io.trino.spi.connector.ConnectorTableExecuteHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DuckLakeAddFilesHandle(
        @JsonProperty DuckLakeTableHandle table,
        @JsonProperty List<String> files,
        @JsonProperty String importId,
        @JsonProperty boolean allowMissing,
        @JsonProperty boolean ignoreExtraColumns,
        @JsonProperty boolean hivePartitioning,
        @JsonProperty boolean replace,
        @JsonProperty Optional<DuckLakeReplacementRange> replacementRange)
        implements ConnectorTableExecuteHandle
{
    @JsonCreator
    public DuckLakeAddFilesHandle
    {
        requireNonNull(table, "table is null");
        files = ImmutableList.copyOf(files);
        requireNonNull(importId, "importId is null");
        requireNonNull(replacementRange, "replacementRange is null");
    }
}
