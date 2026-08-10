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
package io.trino.plugin.ducklake.util;

import io.trino.plugin.ducklake.DuckLakeColumnHandle;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;

/**
 * Computes the {@link Domain} a data file's partition values imply for a source column.
 * DuckLake partition transforms use DuckDB semantics: {@code year} is the calendar year,
 * {@code month} the month of year (1-12), {@code day} the day of month, and {@code hour}
 * the hour of day, all stored as decimal strings in {@code ducklake_file_partition_value};
 * {@code identity} stores the column value itself as a string ({@code NULL} for null).
 */
public final class PartitionTransforms
{
    public static final String IDENTITY_TRANSFORM = "identity";
    private static final String YEAR_TRANSFORM = "year";
    private static final String MONTH_TRANSFORM = "month";
    private static final String DAY_TRANSFORM = "day";
    private static final String HOUR_TRANSFORM = "hour";

    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;

    private PartitionTransforms() {}

    /**
     * Returns the domain the partition values of a file imply for the column, or
     * {@link Optional#empty()} when the values do not constrain the column reliably.
     * The returned domain is exact for identity transforms and a superset of the file's
     * actual values for temporal transforms, so intersecting it with a query predicate
     * can never prune a file containing matching rows.
     */
    public static Optional<Domain> partitionDomain(
            DuckLakeColumnHandle column,
            List<DuckLakePartitionColumn> columnTransforms,
            Map<Integer, Optional<String>> partitionValues)
    {
        try {
            return computeDomain(column, columnTransforms, partitionValues);
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Optional<Domain> computeDomain(
            DuckLakeColumnHandle column,
            List<DuckLakePartitionColumn> columnTransforms,
            Map<Integer, Optional<String>> partitionValues)
    {
        Type type = column.type();
        if (column.isInt128()) {
            // int128 values are stored lossily as doubles in Parquet, so the exact catalog
            // value may disagree with the value the engine compares against
            return Optional.empty();
        }

        for (DuckLakePartitionColumn transform : columnTransforms) {
            if (!transform.transform().toLowerCase(Locale.ENGLISH).equals(IDENTITY_TRANSFORM)) {
                continue;
            }
            Optional<String> value = partitionValues.get(transform.partitionKeyIndex());
            if (value == null) {
                return Optional.empty();
            }
            if (value.isEmpty()) {
                return Optional.of(Domain.onlyNull(type));
            }
            return StatsValueParser.parse(type, value.get())
                    .map(nativeValue -> Domain.create(ValueSet.of(type, nativeValue), false));
        }

        boolean isDate = type.equals(DATE);
        boolean isShortTimestamp = type instanceof TimestampType timestampType && timestampType.isShort();
        if (!isDate && !isShortTimestamp) {
            // temporal transforms over other types (e.g. timestamp with time zone, where the
            // transform output depends on the writer's time zone) are not used for pruning
            return Optional.empty();
        }

        Integer year = null;
        Integer month = null;
        Integer day = null;
        Integer hour = null;
        for (DuckLakePartitionColumn transform : columnTransforms) {
            Optional<String> value = partitionValues.get(transform.partitionKeyIndex());
            if (value == null) {
                continue;
            }
            String transformName = transform.transform().toLowerCase(Locale.ENGLISH);
            if (!transformName.equals(YEAR_TRANSFORM) && !transformName.equals(MONTH_TRANSFORM) && !transformName.equals(DAY_TRANSFORM) && !transformName.equals(HOUR_TRANSFORM)) {
                continue;
            }
            if (value.isEmpty()) {
                // the transform of a NULL value is NULL, so all rows of the file are null
                return Optional.of(Domain.onlyNull(type));
            }
            int parsed = Integer.parseInt(value.get());
            switch (transformName) {
                case YEAR_TRANSFORM -> year = parsed;
                case MONTH_TRANSFORM -> month = parsed;
                case DAY_TRANSFORM -> day = parsed;
                case HOUR_TRANSFORM -> hour = parsed;
                default -> throw new IllegalStateException("Unexpected transform: " + transformName);
            }
        }
        if (year == null) {
            // month/day/hour without a year do not describe a contiguous range
            return Optional.empty();
        }

        LocalDateTime start = LocalDateTime.of(year, 1, 1, 0, 0);
        ChronoUnit unit = ChronoUnit.YEARS;
        if (month != null) {
            start = start.withMonth(month);
            unit = ChronoUnit.MONTHS;
            if (day != null) {
                start = start.withDayOfMonth(day);
                unit = ChronoUnit.DAYS;
                if (hour != null && isShortTimestamp) {
                    start = start.withHour(hour);
                    unit = ChronoUnit.HOURS;
                }
            }
        }
        LocalDateTime end = start.plus(1, unit);

        Range range;
        if (isDate) {
            range = Range.range(type, start.toLocalDate().toEpochDay(), true, end.toLocalDate().toEpochDay(), false);
        }
        else {
            range = Range.range(type, epochMicros(start), true, epochMicros(end), false);
        }
        return Optional.of(Domain.create(ValueSet.ofRanges(range), false));
    }

    private static long epochMicros(LocalDateTime timestamp)
    {
        long seconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        long micros = timestamp.getNano() / NANOSECONDS_PER_MICROSECOND;
        return Math.addExact(Math.multiplyExact(seconds, MICROSECONDS_PER_SECOND), micros);
    }
}
