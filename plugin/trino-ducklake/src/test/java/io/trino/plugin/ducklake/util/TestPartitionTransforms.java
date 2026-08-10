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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.DuckLakeColumnHandle;
import io.trino.plugin.ducklake.metastore.DuckLakePartitionColumn;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPartitionTransforms
{
    @Test
    void testIdentity()
    {
        DuckLakeColumnHandle column = column("id", "int32", INTEGER);
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(new DuckLakePartitionColumn(0, 1, "identity"));

        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("7"))))
                .contains(Domain.create(ValueSet.of(INTEGER, 7L), false));
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.empty())))
                .contains(Domain.onlyNull(INTEGER));
        // missing partition value cannot constrain the column
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of())).isEmpty();
        // unparseable partition value cannot constrain the column
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("bogus")))).isEmpty();
    }

    @Test
    void testYearMonthOverTimestamp()
    {
        DuckLakeColumnHandle column = column("ts", "timestamp", TIMESTAMP_MICROS);
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(
                new DuckLakePartitionColumn(0, 1, "year"),
                new DuckLakePartitionColumn(1, 1, "month"));

        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("2024"), 1, Optional.of("2"))))
                .contains(timestampRange(LocalDateTime.of(2024, 2, 1, 0, 0), LocalDateTime.of(2024, 3, 1, 0, 0)));
        // year only
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("2024"))))
                .contains(timestampRange(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2025, 1, 1, 0, 0)));
        // null timestamp rows land in the null partition
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.empty(), 1, Optional.empty())))
                .contains(Domain.onlyNull(TIMESTAMP_MICROS));
        // month without year does not describe a contiguous range
        assertThat(PartitionTransforms.partitionDomain(column, ImmutableList.of(new DuckLakePartitionColumn(0, 1, "month")), ImmutableMap.of(0, Optional.of("2"))))
                .isEmpty();
    }

    @Test
    void testYearMonthDayOverDate()
    {
        DuckLakeColumnHandle column = column("d", "date", DATE);
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(
                new DuckLakePartitionColumn(0, 1, "year"),
                new DuckLakePartitionColumn(1, 1, "month"),
                new DuckLakePartitionColumn(2, 1, "day"));

        Map<Integer, Optional<String>> values = ImmutableMap.of(0, Optional.of("2024"), 1, Optional.of("2"), 2, Optional.of("29"));
        assertThat(PartitionTransforms.partitionDomain(column, transforms, values))
                .contains(dateRange(LocalDate.of(2024, 2, 29), LocalDate.of(2024, 3, 1)));
        // invalid date components cannot constrain the column
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("2023"), 1, Optional.of("2"), 2, Optional.of("29"))))
                .isEmpty();
    }

    @Test
    void testTimestampWithTimeZoneIsNotPruned()
    {
        // the transform output depends on the writer's time zone, so it is not used for pruning
        DuckLakeColumnHandle column = column("tz", "timestamptz", TIMESTAMP_TZ_MICROS);
        List<DuckLakePartitionColumn> transforms = ImmutableList.of(new DuckLakePartitionColumn(0, 1, "year"));
        assertThat(PartitionTransforms.partitionDomain(column, transforms, ImmutableMap.of(0, Optional.of("2024")))).isEmpty();
    }

    private static Domain timestampRange(LocalDateTime startInclusive, LocalDateTime endExclusive)
    {
        return Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_MICROS, epochMicros(startInclusive), true, epochMicros(endExclusive), false)),
                false);
    }

    private static Domain dateRange(LocalDate startInclusive, LocalDate endExclusive)
    {
        return Domain.create(
                ValueSet.ofRanges(Range.range(DATE, startInclusive.toEpochDay(), true, endExclusive.toEpochDay(), false)),
                false);
    }

    private static long epochMicros(LocalDateTime timestamp)
    {
        return timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + timestamp.getNano() / 1_000;
    }

    private static DuckLakeColumnHandle column(String name, String duckLakeType, Type type)
    {
        return new DuckLakeColumnHandle(1, name, duckLakeType, type, true, Optional.empty());
    }
}
