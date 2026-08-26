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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Statistics of one column of one data file, as written to {@code ducklake_file_column_stats}.
 * Values are the DuckLake string representation of the column type, the same encoding the read
 * path parses for file pruning.
 * <p>
 * Instances travel from the workers that write the data files to the coordinator that commits
 * them, so they are serialized as JSON.
 */
public record DuckLakeFileColumnStatsRow(
        @JsonProperty long columnId,
        @JsonProperty OptionalLong columnSizeBytes,
        @JsonProperty OptionalLong valueCount,
        @JsonProperty OptionalLong nullCount,
        @JsonProperty Optional<String> minValue,
        @JsonProperty Optional<String> maxValue,
        @JsonProperty Optional<Boolean> containsNan)
{
    @JsonCreator
    public DuckLakeFileColumnStatsRow
    {
        requireNonNull(columnSizeBytes, "columnSizeBytes is null");
        requireNonNull(valueCount, "valueCount is null");
        requireNonNull(nullCount, "nullCount is null");
        requireNonNull(minValue, "minValue is null");
        requireNonNull(maxValue, "maxValue is null");
        requireNonNull(containsNan, "containsNan is null");
    }
}
