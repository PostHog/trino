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
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStatsRow;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A data file a worker has written, described well enough for the coordinator to register it in
 * the catalog. Instances are serialized as JSON and returned as page sink fragments.
 * <p>
 * The path is relative to the table's location, which is how DuckLake stores it, so moving a
 * table's files does not require rewriting the catalog.
 */
public record DuckLakeDataFile(
        @JsonProperty String path,
        @JsonProperty long recordCount,
        @JsonProperty long fileSizeBytes,
        @JsonProperty long footerSize,
        @JsonProperty List<Optional<String>> partitionValues,
        @JsonProperty List<DuckLakeFileColumnStatsRow> columnStatistics)
{
    @JsonCreator
    public DuckLakeDataFile
    {
        requireNonNull(path, "path is null");
        partitionValues = ImmutableList.copyOf(partitionValues);
        columnStatistics = ImmutableList.copyOf(columnStatistics);
    }
}
