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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.OptionalInt;

import static io.trino.plugin.ducklake.util.DuckDbTemporalText.parseEpochDay;
import static io.trino.plugin.ducklake.util.DuckDbTemporalText.parseNanoOfDay;
import static io.trino.plugin.ducklake.util.DuckDbTemporalText.parseTimestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The text DuckDB casts dates, times and timestamps to, which is how DuckLake stores them for
 * inlined rows in a PostgreSQL catalog. Each case is a string DuckDB writes.
 */
final class TestDuckDbTemporalText
{
    @Test
    void testDate()
    {
        assertThat(parseEpochDay("2024-01-02")).isEqualTo(LocalDate.of(2024, 1, 2).toEpochDay());
        assertThat(parseEpochDay("1970-01-01")).isEqualTo(0);
        assertThat(parseEpochDay("12345-01-01")).isEqualTo(LocalDate.of(12345, 1, 1).toEpochDay());
        // DuckDB counts years before 1 AD backwards: 0001 (BC) is the year 0
        assertThat(parseEpochDay("0001-12-31 (BC)")).isEqualTo(LocalDate.of(0, 12, 31).toEpochDay());
        assertThat(parseEpochDay("0044-03-15 (BC)")).isEqualTo(LocalDate.of(-43, 3, 15).toEpochDay());
        // the way PostgreSQL writes it
        assertThat(parseEpochDay("0044-03-15 BC")).isEqualTo(LocalDate.of(-43, 3, 15).toEpochDay());

        assertThatThrownBy(() -> parseEpochDay("infinity")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseEpochDay("-infinity")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseEpochDay("2024-02-30")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseEpochDay("24-01-02")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTime()
    {
        assertThat(parseNanoOfDay("00:00:00")).isEqualTo(0);
        assertThat(parseNanoOfDay("01:02:03.5")).isEqualTo(3_723_500_000_000L);
        assertThat(parseNanoOfDay("23:59:59.999999")).isEqualTo(86_399_999_999_000L);
        assertThatThrownBy(() -> parseNanoOfDay("24:00:00")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseNanoOfDay("1:02:03")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTimestamp()
    {
        assertTimestamp("2024-01-02 03:04:05", LocalDateTime.of(2024, 1, 2, 3, 4, 5), 0, OptionalInt.empty());
        // DuckDB removes trailing zeros from the fraction
        assertTimestamp("2024-01-02 03:04:05.12", LocalDateTime.of(2024, 1, 2, 3, 4, 5), 120_000_000, OptionalInt.empty());
        assertTimestamp("2024-01-02 03:04:05.000000001", LocalDateTime.of(2024, 1, 2, 3, 4, 5), 1, OptionalInt.empty());
        assertTimestamp("1969-12-31 23:59:59.999999", LocalDateTime.of(1969, 12, 31, 23, 59, 59), 999_999_000, OptionalInt.empty());
        // DuckDB writes the era right after the date, PostgreSQL at the end
        assertTimestamp("0001-01-01 (BC) 00:00:00", LocalDateTime.of(0, 1, 1, 0, 0, 0), 0, OptionalInt.empty());
        assertTimestamp("0044-03-15 (BC) 12:30:00.5", LocalDateTime.of(-43, 3, 15, 12, 30, 0), 500_000_000, OptionalInt.empty());
        assertTimestamp("0044-03-15 (BC) 12:30:00+01", LocalDateTime.of(-43, 3, 15, 12, 30, 0), 0, OptionalInt.of(3600));
        assertTimestamp("0001-01-01 00:00:00 BC", LocalDateTime.of(0, 1, 1, 0, 0, 0), 0, OptionalInt.empty());
        // a timestamp with time zone carries the offset of the zone DuckDB wrote it in
        assertTimestamp("2024-01-01 21:34:05+00", LocalDateTime.of(2024, 1, 1, 21, 34, 5), 0, OptionalInt.of(0));
        assertTimestamp("2024-01-01 22:04:05-05", LocalDateTime.of(2024, 1, 1, 22, 4, 5), 0, OptionalInt.of(-5 * 3600));
        assertTimestamp("2024-01-02 03:04:05.5+05:30", LocalDateTime.of(2024, 1, 2, 3, 4, 5), 500_000_000, OptionalInt.of(5 * 3600 + 30 * 60));
        assertThat(parseTimestamp("2024-01-02 03:04:05+05:30").utcEpochSecond())
                .isEqualTo(LocalDateTime.of(2024, 1, 1, 21, 34, 5).toEpochSecond(ZoneOffset.UTC));

        assertThatThrownBy(() -> parseTimestamp("infinity")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseTimestamp("2024-01-02")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseTimestamp("2024-01-02 03:04:05.1234567890")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseTimestamp("2024-01-02 03:04:05+05:99")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseTimestamp("0001-01-01 (BC) 00:00:00 BC")).isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertTimestamp(String text, LocalDateTime expected, int nanoOfSecond, OptionalInt offsetSeconds)
    {
        DuckDbTemporalText.Timestamp timestamp = parseTimestamp(text);
        assertThat(timestamp.epochSecond()).isEqualTo(expected.toEpochSecond(ZoneOffset.UTC));
        assertThat(timestamp.nanoOfSecond()).isEqualTo(nanoOfSecond);
        assertThat(timestamp.offsetSeconds()).isEqualTo(offsetSeconds);
    }
}
