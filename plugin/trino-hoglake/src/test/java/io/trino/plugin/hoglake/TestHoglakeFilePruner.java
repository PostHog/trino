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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Planning-time pruning from catalog bounds. Every bound is parsed from the
 * JSON text the server would send, so the tests see the same tokens the
 * wire delivers: exact integers past 2^53, the infinity sentinels, and ISO
 * temporals with their trailing zero units elided.
 */
final class TestHoglakeFilePruner
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final HoglakeColumnHandle ID = new HoglakeColumnHandle("id", 1, BIGINT, true);
    private static final HoglakeColumnHandle NAME = new HoglakeColumnHandle("name", 2, VARCHAR, true);

    @Test
    void testNoStatisticsNeverPrunes()
    {
        TupleDomain<HoglakeColumnHandle> constraint = equalTo(ID, 5L);
        // Absent: pending or failed statistics, or a scan that did not ask.
        assertThat(HoglakeFilePruner.mayContain(constraint, file(null))).isTrue();
        // Empty: provided, but nothing recorded for the requested columns.
        assertThat(HoglakeFilePruner.mayContain(constraint, file(List.of()))).isTrue();
        // An entry for another column says nothing about this one.
        assertThat(HoglakeFilePruner.mayContain(constraint, file(List.of(stats(9, 10, 0, "1", "2"))))).isTrue();
    }

    @Test
    void testBigintBounds()
    {
        assertOutside(ID, 5L, "10", "20");
        assertOutside(ID, 25L, "10", "20");
        assertInside(ID, 10L, "10", "20");
        assertInside(ID, 20L, "10", "20");
        assertInside(ID, 15L, "10", "20");
    }

    @Test
    void testIntegersBeyondDoublePrecision()
    {
        // 2^53 + 1 and 2^53 are the same double: routed through one, the
        // bound would admit 2^53 and the file would stay.
        assertOutside(ID, 9007199254740992L, "9007199254740993", "9223372036854775807");
        assertInside(ID, Long.MAX_VALUE, "9007199254740993", "9223372036854775807");
    }

    @Test
    void testIntegerBounds()
    {
        HoglakeColumnHandle column = new HoglakeColumnHandle("count", 3, INTEGER, true);
        assertOutside(column, 3L, "-5", "2");
        assertInside(column, -5L, "-5", "2");
        // A long-width token under an int column was not produced by it.
        assertInside(column, 3L, "-5", "2147483648");
    }

    @Test
    void testFloatingPointBounds()
    {
        HoglakeColumnHandle price = new HoglakeColumnHandle("price", 4, DOUBLE, true);
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 3.5), file(List.of(stats(4, 10, 0, 0L, "0.5", "2.5"))))).isFalse();
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 2.5), file(List.of(stats(4, 10, 0, 0L, "0.5", "2.5"))))).isTrue();
        // The infinity sentinels are bounds like any other.
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 1e300), file(List.of(stats(4, 10, 0, 0L, "\"-Infinity\"", "2.5"))))).isFalse();
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 1e300), file(List.of(stats(4, 10, 0, 0L, "\"-Infinity\"", "\"Infinity\""))))).isTrue();

        HoglakeColumnHandle ratio = new HoglakeColumnHandle("ratio", 5, REAL, true);
        long threePointFive = floatToRawIntBits(3.5f);
        assertThat(HoglakeFilePruner.mayContain(equalTo(ratio, threePointFive), file(List.of(stats(5, 10, 0, 0L, "0.1", "2.5"))))).isFalse();
        assertThat(HoglakeFilePruner.mayContain(equalTo(ratio, (long) floatToRawIntBits(0.1f)), file(List.of(stats(5, 10, 0, 0L, "0.1", "2.5"))))).isTrue();
    }

    @Test
    void testFloatingPointWithUnknownNaNsNeverPrunes()
    {
        // Bounds exclude NaN; without a NaN count of zero the column may hold
        // values outside them. The hydrator never reports a NaN count.
        HoglakeColumnHandle price = new HoglakeColumnHandle("price", 4, DOUBLE, true);
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 3.5), file(List.of(stats(4, 10, 0, "0.5", "2.5"))))).isTrue();
        assertThat(HoglakeFilePruner.mayContain(equalTo(price, 3.5), file(List.of(stats(4, 10, 0, 2L, "0.5", "2.5"))))).isTrue();
    }

    @Test
    void testBooleanDateAndVarcharBounds()
    {
        HoglakeColumnHandle flag = new HoglakeColumnHandle("flag", 6, BOOLEAN, true);
        assertOutside(flag, true, "false", "false");
        assertInside(flag, false, "false", "true");

        HoglakeColumnHandle day = new HoglakeColumnHandle("day", 7, DATE, true);
        assertOutside(day, LocalDate.parse("2026-09-06").toEpochDay(), "\"2026-09-01\"", "\"2026-09-05\"");
        assertInside(day, LocalDate.parse("2026-09-05").toEpochDay(), "\"2026-09-01\"", "\"2026-09-05\"");

        // Byte order, as the bounds are: a writer-truncated upper bound
        // ("ab" rounded up to "ac") still bounds every value.
        assertOutside(NAME, utf8Slice("ad"), "\"aa\"", "\"ac\"");
        assertInside(NAME, utf8Slice("abzzz"), "\"aa\"", "\"ac\"");
        assertInside(NAME, utf8Slice("🦔"), "\"aa\"", "\"🦔\"");
    }

    @Test
    void testTimestampBoundsWithElidedUnits()
    {
        HoglakeColumnHandle timestamp = new HoglakeColumnHandle("ts", 8, TIMESTAMP_MICROS, true);
        // A whole-minute bound elides its seconds: "2026-09-05T00:00".
        assertOutside(timestamp, epochMicros("2026-09-04T23:59:59.999999"), "\"2026-09-05T00:00\"", "\"2026-09-05T23:59:59.999999\"");
        assertInside(timestamp, epochMicros("2026-09-05T00:00:00"), "\"2026-09-05T00:00\"", "\"2026-09-05T23:59:59.999999\"");
        assertOutside(timestamp, epochMicros("2026-09-06T00:00:00"), "\"2026-09-05T00:00\"", "\"2026-09-05T23:59:59.999999\"");

        HoglakeColumnHandle instant = new HoglakeColumnHandle("at", 9, TIMESTAMP_TZ_MICROS, true);
        // A predicate in another zone compares by instant.
        LongTimestampWithTimeZone justBefore = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                LocalDateTime.parse("2026-09-04T23:59:59.999").toInstant(ZoneOffset.UTC).toEpochMilli(), 999_000_000, getTimeZoneKey("America/New_York"));
        LongTimestampWithTimeZone start = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                LocalDateTime.parse("2026-09-05T00:00").toInstant(ZoneOffset.UTC).toEpochMilli(), 0, getTimeZoneKey("Europe/London"));
        assertOutside(instant, justBefore, "\"2026-09-05T00:00Z\"", "\"2026-09-05T12:00:00.5Z\"");
        assertInside(instant, start, "\"2026-09-05T00:00Z\"", "\"2026-09-05T12:00:00.5Z\"");
    }

    @Test
    void testOneOrBothNullBoundsNeverPrune()
    {
        // A null bound means "do not prune on this column", whichever end.
        assertInside(ID, 5L, "null", "20");
        assertInside(ID, 25L, "10", "null");
        assertInside(ID, 25L, "null", "null");
        assertThat(HoglakeFilePruner.mayContain(equalTo(ID, 25L), file(List.of(new HoglakeDtos.ScanColumnStats(1, 10, 0, null, null, null))))).isTrue();
    }

    @Test
    void testAllNullColumns()
    {
        // value_count includes nulls: an all-null column bounds no value.
        HoglakeDtos.DataFile allNull = file(List.of(stats(1, 10, 10, "null", "null")));
        assertThat(HoglakeFilePruner.mayContain(equalTo(ID, 5L), allNull)).isFalse();
        assertThat(HoglakeFilePruner.mayContain(TupleDomain.withColumnDomains(Map.of(ID, Domain.notNull(BIGINT))), allNull)).isFalse();
        assertThat(HoglakeFilePruner.mayContain(TupleDomain.withColumnDomains(Map.of(ID, Domain.onlyNull(BIGINT))), allNull)).isTrue();
        assertThat(HoglakeFilePruner.mayContain(TupleDomain.withColumnDomains(Map.of(ID, Domain.create(ValueSet.of(BIGINT, 5L), true))), allNull)).isTrue();

        // Nulls beside bounded values stay visible to IS NULL.
        HoglakeDtos.DataFile someNull = file(List.of(stats(1, 10, 3, "10", "20")));
        assertThat(HoglakeFilePruner.mayContain(TupleDomain.withColumnDomains(Map.of(ID, Domain.onlyNull(BIGINT))), someNull)).isTrue();
        HoglakeDtos.DataFile noNull = file(List.of(stats(1, 10, 0, "10", "20")));
        assertThat(HoglakeFilePruner.mayContain(TupleDomain.withColumnDomains(Map.of(ID, Domain.onlyNull(BIGINT))), noNull)).isFalse();
    }

    @Test
    void testUnsupportedTypesNeverPrune()
    {
        HoglakeColumnHandle uuid = new HoglakeColumnHandle("uuid", 10, UUID, true);
        HoglakeColumnHandle unsigned = new HoglakeColumnHandle("unsigned", 11, BIGINT, true, List.of(), "uint32");
        HoglakeColumnHandle decimal = new HoglakeColumnHandle("amount", 12, DecimalType.createDecimalType(10, 2), true);
        assertThat(HoglakeFilePruner.isPrunable(uuid)).isFalse();
        assertThat(HoglakeFilePruner.isPrunable(unsigned)).isFalse();
        assertThat(HoglakeFilePruner.isPrunable(decimal)).isFalse();
        assertThat(HoglakeFilePruner.isPrunable(HoglakeColumnHandle.ROW_ID)).isFalse();
        // Bounds that would exclude the value do not prune an unsigned column.
        assertThat(HoglakeFilePruner.mayContain(equalTo(unsigned, 5L), file(List.of(stats(11, 10, 0, "10", "20"))))).isTrue();
        assertThat(HoglakeFilePruner.prunableFieldIds(TupleDomain.withColumnDomains(Map.of(
                ID, Domain.singleValue(BIGINT, 5L),
                unsigned, Domain.singleValue(BIGINT, 5L),
                NAME, Domain.all(VARCHAR))))).containsExactly(1L);
    }

    @Test
    void testUndecodableBoundsNeverPrune()
    {
        assertInside(ID, 5L, "\"ten\"", "20");
        assertInside(ID, 5L, "10.5", "20");
        assertInside(ID, 5L, "true", "20");
        HoglakeColumnHandle day = new HoglakeColumnHandle("day", 7, DATE, true);
        assertInside(day, 0L, "\"not-a-date\"", "\"2026-09-05\"");
        HoglakeColumnHandle timestamp = new HoglakeColumnHandle("ts", 8, TIMESTAMP_MICROS, true);
        // Sub-microsecond digits are not a microsecond column's bound.
        assertInside(timestamp, 0L, "\"2026-09-05T00:00:00.0000001\"", "\"2026-09-06T00:00\"");
        // An inverted pair is not a range at all.
        assertInside(ID, 5L, "20", "10");
    }

    @Test
    void testEveryConstrainedColumnMustOverlap()
    {
        HoglakeDtos.DataFile file = file(List.of(stats(1, 10, 0, "10", "20"), stats(2, 10, 0, "\"a\"", "\"c\"")));
        TupleDomain<HoglakeColumnHandle> idInsideNameOutside = TupleDomain.withColumnDomains(Map.of(
                ID, Domain.singleValue(BIGINT, 15L),
                NAME, Domain.singleValue(VARCHAR, utf8Slice("z"))));
        assertThat(HoglakeFilePruner.mayContain(idInsideNameOutside, file)).isFalse();
        TupleDomain<HoglakeColumnHandle> range = TupleDomain.withColumnDomains(Map.of(
                ID, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 20L)), false)));
        assertThat(HoglakeFilePruner.mayContain(range, file)).isFalse();
    }

    private static void assertOutside(HoglakeColumnHandle column, Object value, String lower, String upper)
    {
        assertThat(HoglakeFilePruner.mayContain(equalTo(column, value), file(List.of(stats(column.fieldId(), 10, 0, zeroNaNs(column.type()), lower, upper)))))
                .describedAs("%s = %s against [%s, %s]", column.name(), value, lower, upper)
                .isFalse();
    }

    private static void assertInside(HoglakeColumnHandle column, Object value, String lower, String upper)
    {
        assertThat(HoglakeFilePruner.mayContain(equalTo(column, value), file(List.of(stats(column.fieldId(), 10, 0, zeroNaNs(column.type()), lower, upper)))))
                .describedAs("%s = %s against [%s, %s]", column.name(), value, lower, upper)
                .isTrue();
    }

    private static Long zeroNaNs(Type type)
    {
        if (type.equals(DOUBLE) || type.equals(REAL)) {
            return 0L;
        }
        return null;
    }

    private static TupleDomain<HoglakeColumnHandle> equalTo(HoglakeColumnHandle column, Object value)
    {
        return TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(column.type(), value)));
    }

    private static long epochMicros(String localDateTime)
    {
        LocalDateTime value = LocalDateTime.parse(localDateTime);
        return value.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + value.getNano() / 1_000;
    }

    private static HoglakeDtos.ScanColumnStats stats(long fieldId, long valueCount, long nullCount, String lower, String upper)
    {
        return stats(fieldId, valueCount, nullCount, null, lower, upper);
    }

    private static HoglakeDtos.ScanColumnStats stats(long fieldId, long valueCount, long nullCount, Long nanCount, String lower, String upper)
    {
        return new HoglakeDtos.ScanColumnStats(fieldId, valueCount, nullCount, nanCount, json(lower), json(upper));
    }

    private static JsonNode json(String token)
    {
        try {
            return MAPPER.readTree(token);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static HoglakeDtos.DataFile file(List<HoglakeDtos.ScanColumnStats> columnStats)
    {
        return new HoglakeDtos.DataFile(10, "s3://lake/t/a.parquet", "parquet", 10, 1024, 100L, 0, "provided", 3, List.of(), columnStats);
    }
}
