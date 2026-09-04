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

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.UuidType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStatsValueParser
{
    @Test
    void testBoolean()
    {
        // file statistics store 0/1, identity partition values store true/false
        assertThat(StatsValueParser.parse(BOOLEAN, "0")).contains(false);
        assertThat(StatsValueParser.parse(BOOLEAN, "1")).contains(true);
        assertThat(StatsValueParser.parse(BOOLEAN, "false")).contains(false);
        assertThat(StatsValueParser.parse(BOOLEAN, "true")).contains(true);
        assertThat(StatsValueParser.parse(BOOLEAN, "yes")).isEmpty();
    }

    @Test
    void testIntegers()
    {
        assertThat(StatsValueParser.parse(INTEGER, "-5")).contains(-5L);
        assertThat(StatsValueParser.parse(BIGINT, "123456789012")).contains(123456789012L);
        assertThat(StatsValueParser.parse(INTEGER, "not a number")).isEmpty();
        assertThat(StatsValueParser.parse(INTEGER, "123456789012")).isEmpty();
    }

    @Test
    void testFloatingPoint()
    {
        assertThat(StatsValueParser.parse(DOUBLE, "-2.75")).contains(-2.75);
        assertThat(StatsValueParser.parse(DOUBLE, "100.0")).contains(100.0);
        assertThat(StatsValueParser.parse(REAL, "1.5")).contains((long) Float.floatToRawIntBits(1.5f));
        assertThat(StatsValueParser.parse(DOUBLE, "nan")).isEmpty();
        assertThat(StatsValueParser.parse(DOUBLE, "inf")).isEmpty();
    }

    @Test
    void testDecimal()
    {
        assertThat(StatsValueParser.parse(DecimalType.createDecimalType(10, 2), "12.34")).contains(1234L);
        assertThat(StatsValueParser.parse(DecimalType.createDecimalType(30, 5), "5.00000")).contains(Int128.valueOf(500000));
        assertThat(StatsValueParser.parse(DecimalType.createDecimalType(30, 5), "1234567890123456789012345.12345"))
                .contains(Int128.valueOf("123456789012345678901234512345"));
        // value with more fractional digits than the scale cannot be represented exactly
        assertThat(StatsValueParser.parse(DecimalType.createDecimalType(10, 2), "1.234")).isEmpty();
    }

    @Test
    void testVarchar()
    {
        assertThat(StatsValueParser.parse(VARCHAR, "zebra with spaces 'quote")).contains(utf8Slice("zebra with spaces 'quote"));
    }

    @Test
    void testDate()
    {
        assertThat(StatsValueParser.parse(DATE, "2024-05-01")).contains(19844L);
        assertThat(StatsValueParser.parse(DATE, "not a date")).isEmpty();
        // a year past four digits is signed as this connector writes it, and bare as DuckDB does
        assertThat(StatsValueParser.parse(DATE, "+10000-01-01")).contains(2932897L);
        assertThat(StatsValueParser.parse(DATE, "10000-01-01")).isEmpty();
    }

    /**
     * The forms DuckDB writes for a date or a timestamp that names no instant. Reading one back
     * would be a guess, so it parses as unknown and leaves the file it partitions unprunable.
     */
    @Test
    void testTemporalValuesDuckDbWritesThatNameNoInstant()
    {
        assertThat(StatsValueParser.parse(DATE, "infinity")).isEmpty();
        assertThat(StatsValueParser.parse(DATE, "-infinity")).isEmpty();
        assertThat(StatsValueParser.parse(DATE, "0001-01-01 (BC)")).isEmpty();
        assertThat(StatsValueParser.parse(TIMESTAMP_MICROS, "infinity")).isEmpty();
        assertThat(StatsValueParser.parse(TIMESTAMP_MICROS, "-infinity")).isEmpty();
        assertThat(StatsValueParser.parse(TIMESTAMP_MICROS, "0001-01-01 00:00:00 (BC)")).isEmpty();
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "infinity")).isEmpty();
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "-infinity")).isEmpty();
    }

    @Test
    void testTimestamp()
    {
        assertThat(StatsValueParser.parse(TIMESTAMP_MICROS, "1970-01-01 00:00:01")).contains(1_000_000L);
        assertThat(StatsValueParser.parse(TIMESTAMP_MICROS, "2024-05-01 12:34:56.789012")).contains(1714566896789012L);
        assertThat(StatsValueParser.parse(TIMESTAMP_NANOS, "2024-05-01 12:34:56.789012345")).isEmpty();
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "1970-01-01 00:00:01+00"))
                .contains(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000, 0, UTC_KEY));
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "2024-05-01 10:34:56.789012+00"))
                .contains(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1714559696789L, 12_000_000, UTC_KEY));
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "2024-05-01 12:34:56.789012+02"))
                .contains(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1714559696789L, 12_000_000, UTC_KEY));
        // a writer in a zone offset by half an hour records the offset with its minutes
        assertThat(StatsValueParser.parse(TIMESTAMP_TZ_MICROS, "2024-05-01 16:04:56.789012+05:30"))
                .contains(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1714559696789L, 12_000_000, UTC_KEY));
    }

    @Test
    void testUuid()
    {
        assertThat(StatsValueParser.parse(UuidType.UUID, "11111111-2222-3333-4444-555555555555"))
                .contains(javaUuidToTrinoUuid(UUID.fromString("11111111-2222-3333-4444-555555555555")));
    }
}
