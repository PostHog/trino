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
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

/**
 * Parses the string values DuckLake stores in its catalog (column statistics
 * {@code min_value}/{@code max_value} and identity partition values) into Trino native values.
 * The strings are written by DuckDB using its default {@code VARCHAR} casts, for example
 * {@code 2024-05-01 12:34:56.789012} for timestamps and {@code 2024-05-01 10:34:56.789012+00}
 * for timestamps with time zone.
 *
 * <p>Any value that cannot be parsed reliably yields {@link Optional#empty()}, so callers
 * treat it as "unknown" and never prune based on it.
 */
public final class StatsValueParser
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = new DateTimeFormatterBuilder()
            .append(TIMESTAMP_FORMATTER)
            .appendOffset("+HH:mm", "+00")
            .toFormatter();

    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;
    private static final long PICOSECONDS_PER_NANOSECOND = 1_000;
    private static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;
    private static final long MICROSECONDS_PER_MILLISECOND = 1_000;

    private StatsValueParser() {}

    /**
     * Parses a DuckLake catalog string value to the Trino native representation of the given type,
     * or {@link Optional#empty()} when the value cannot be represented reliably.
     */
    public static Optional<Object> parse(Type type, String value)
    {
        try {
            return Optional.ofNullable(parseValue(type, value));
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Object parseValue(Type type, String value)
    {
        if (BOOLEAN.equals(type)) {
            return switch (value) {
                case "true", "1" -> true;
                case "false", "0" -> false;
                default -> null;
            };
        }
        if (TINYINT.equals(type)) {
            return (long) Byte.parseByte(value);
        }
        if (SMALLINT.equals(type)) {
            return (long) Short.parseShort(value);
        }
        if (INTEGER.equals(type)) {
            return (long) Integer.parseInt(value);
        }
        if (BIGINT.equals(type)) {
            return Long.parseLong(value);
        }
        if (REAL.equals(type)) {
            float parsed = Float.parseFloat(value);
            if (!Float.isFinite(parsed)) {
                return null;
            }
            return (long) Float.floatToRawIntBits(parsed);
        }
        if (DOUBLE.equals(type)) {
            double parsed = Double.parseDouble(value);
            if (!Double.isFinite(parsed)) {
                return null;
            }
            return parsed;
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal parsed = new BigDecimal(value).setScale(decimalType.getScale());
            Int128 unscaled = Int128.valueOf(parsed.unscaledValue());
            if (Decimals.overflows(unscaled, decimalType.getPrecision())) {
                return null;
            }
            if (decimalType.isShort()) {
                return unscaled.toLongExact();
            }
            return unscaled;
        }
        if (type instanceof VarcharType) {
            return utf8Slice(value);
        }
        if (DATE.equals(type)) {
            return LocalDate.parse(value).toEpochDay();
        }
        if (type instanceof TimestampType timestampType) {
            if (!timestampType.isShort()) {
                return null;
            }
            LocalDateTime timestamp = LocalDateTime.parse(value, TIMESTAMP_FORMATTER);
            return epochMicros(timestamp);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() != 6) {
                return null;
            }
            OffsetDateTime timestamp = OffsetDateTime.parse(value, TIMESTAMP_TZ_FORMATTER);
            long micros = epochMicros(timestamp.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                    floorDiv(micros, MICROSECONDS_PER_MILLISECOND),
                    (int) (floorMod(micros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND),
                    UTC_KEY);
        }
        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() > 6) {
                return null;
            }
            LocalTime time = LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
            return time.toNanoOfDay() * PICOSECONDS_PER_NANOSECOND;
        }
        if (io.trino.spi.type.UuidType.UUID.equals(type)) {
            return javaUuidToTrinoUuid(UUID.fromString(value));
        }
        return null;
    }

    private static long epochMicros(LocalDateTime timestamp)
    {
        long seconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        long micros = timestamp.getNano() / NANOSECONDS_PER_MICROSECOND;
        return Math.addExact(Math.multiplyExact(seconds, MICROSECONDS_PER_SECOND), micros);
    }
}
