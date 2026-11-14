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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.connector.ConnectorTableExecuteHandle;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static java.util.Objects.requireNonNull;

/**
 * A planned {@code add_files}. The table handle pins the snapshot the commit reads, and
 * {@code partitionFields} is the live spec at that snapshot, empty or a single {@code day} field.
 */
public record HoglakeAddFilesHandle(
        @JsonProperty("table") HoglakeTableHandle table,
        @JsonProperty("location") Optional<String> location,
        @JsonProperty("files") List<String> files,
        @JsonProperty("filePrefix") Optional<String> filePrefix,
        @JsonProperty("importId") String importId,
        @JsonProperty("expectedRows") Optional<Long> expectedRows,
        @JsonProperty("allowMissing") boolean allowMissing,
        @JsonProperty("deleteOnFailure") boolean deleteOnFailure,
        @JsonProperty("replace") boolean replace,
        @JsonProperty("range") Optional<ReplacementRange> range,
        @JsonProperty("partitionFields") List<HoglakeDtos.PartitionField> partitionFields)
        implements ConnectorTableExecuteHandle
{
    @JsonCreator
    public HoglakeAddFilesHandle
    {
        requireNonNull(table, "table is null");
        requireNonNull(location, "location is null");
        files = List.copyOf(requireNonNull(files, "files is null"));
        requireNonNull(filePrefix, "filePrefix is null");
        requireNonNull(importId, "importId is null");
        requireNonNull(expectedRows, "expectedRows is null");
        requireNonNull(range, "range is null");
        partitionFields = List.copyOf(requireNonNull(partitionFields, "partitionFields is null"));
    }

    /**
     * The days {@code [startDay, endDay)} since the epoch, on UTC day boundaries, of a column
     * partitioned by {@code day}.
     */
    public record ReplacementRange(
            @JsonProperty("column") String column,
            @JsonProperty("startDay") long startDay,
            @JsonProperty("endDay") long endDay)
    {
        @JsonCreator
        public ReplacementRange
        {
            requireNonNull(column, "column is null");
        }

        @JsonIgnore
        public List<String> partitionValues()
        {
            return LongStream.range(startDay, endDay).mapToObj(HoglakePartitioning::dayValue).toList();
        }
    }

    @JsonIgnore
    public boolean replacement()
    {
        return replace || range.isPresent();
    }
}
