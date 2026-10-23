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
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * The non-NULL values in a typed, half-open interval of a top-level column.
 */
public record DuckLakeReplacementRange(
        @JsonProperty DuckLakeColumnHandle column,
        @JsonProperty String start,
        @JsonProperty String end)
{
    @JsonCreator
    public DuckLakeReplacementRange
    {
        requireNonNull(column, "column is null");
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");
        if (!column.type().isOrderable() || column.isInt128()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Replacement range requires an exactly ordered scalar column");
        }
        // Validate bounds before any files are inspected or a delete file is written.
        interval(column, start, end);
    }

    public Domain domain()
    {
        return interval(column, start, end);
    }

    private static Domain interval(DuckLakeColumnHandle column, String start, String end)
    {
        Object low = StatsValueParser.parse(column.type(), start)
                .orElseThrow(() -> new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid replacement range start for " + column.name()));
        Object high = StatsValueParser.parse(column.type(), end)
                .orElseThrow(() -> new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid replacement range end for " + column.name()));
        try {
            return Domain.create(ValueSet.ofRanges(Range.range(column.type(), low, true, high, false)), false);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Replacement range start must be less than end", e);
        }
    }

    public void validateImportedFiles(List<DuckLakeFileImporter.ImportedFile> files)
    {
        Domain range = domain();
        for (DuckLakeFileImporter.ImportedFile imported : files) {
            DuckLakeDataFile file = imported.file();
            if (file.recordCount() == 0) {
                continue;
            }
            Optional<Domain> values = file.columnStatistics().stream()
                    .filter(stats -> stats.columnId() == column.columnId())
                    .findFirst()
                    .flatMap(stats -> DuckLakeSplitManager.statisticsDomain(column.type(), new DuckLakeFileColumnStats(
                            0, stats.columnId(), stats.valueCount(), stats.nullCount(), stats.minValue(), stats.maxValue(), stats.containsNan()), file.recordCount()));
            if (values.isEmpty() || !range.contains(values.orElseThrow())) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Replacement file must have non-NULL values entirely inside the replacement range, proven by footer statistics: " + file.path());
            }
        }
    }
}
