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
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.util.DuckLakePartitionValues;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
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
        if (!isExactlyOrderedScalar(column)) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Replacement range requires an exactly ordered scalar column, but %s has type %s".formatted(column.name(), column.type()));
        }
        // Validate bounds before any files are inspected or a delete file is written.
        interval(column, start, end);
    }

    /**
     * Floating point columns are excluded because NaN has no place in the interval and footers
     * cannot bound it, so no file could ever be proven to lie inside the range.
     */
    private static boolean isExactlyOrderedScalar(DuckLakeColumnHandle column)
    {
        Type type = column.type();
        return type.isOrderable()
                && !type.equals(REAL)
                && !type.equals(DOUBLE)
                && !(type instanceof ArrayType)
                && !(type instanceof MapType)
                && !(type instanceof RowType)
                && !column.isInt128();
    }

    /**
     * The bounds in the catalog representation of their values, so that an equivalent spelling
     * of a bound, or the same bound after the column was widened, has the same representation.
     */
    public List<String> canonicalBounds()
    {
        return ImmutableList.of(canonical(column, start), canonical(column, end));
    }

    private static String canonical(DuckLakeColumnHandle column, String value)
    {
        Object parsed = StatsValueParser.parse(column.type(), value).orElseThrow();
        BlockBuilder builder = column.type().createBlockBuilder(null, 1);
        TypeUtils.writeNativeValue(column.type(), builder, parsed);
        try {
            return DuckLakePartitionValues.format(column.type(), builder.build(), 0).orElseThrow();
        }
        catch (TrinoException _) {
            // a type without a catalog representation keeps the bound as it was written
            return value;
        }
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
