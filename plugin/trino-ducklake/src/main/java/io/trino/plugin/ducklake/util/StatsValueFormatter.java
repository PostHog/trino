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
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

/**
 * Renders the values of a Parquet column statistic as the catalog strings DuckLake stores, in the
 * format DuckDB's {@code VARCHAR} casts produce. This is the inverse of {@link StatsValueParser}:
 * everything written here has to parse back to the same value, and DuckDB has to read it as the
 * value the file actually holds.
 * <p>
 * Values arrive in the physical representation of the Parquet column, so the type passed in is the
 * one {@link DuckLakeParquetSchema} chose to write the column with rather than the type the column
 * is declared as. A value that cannot be rendered faithfully yields {@link Optional#empty()},
 * which leaves the statistic unset rather than recording something a reader could prune on
 * incorrectly.
 */
public final class StatsValueFormatter
{
    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long MILLISECONDS_PER_SECOND = 1_000;
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;
    private static final long MICROSECONDS_PER_MILLISECOND = 1_000;

    private StatsValueFormatter() {}

    public static Optional<String> format(Type writerType, Object value)
    {
        try {
            return Optional.ofNullable(formatValue(writerType, value));
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static String formatValue(Type writerType, Object value)
    {
        if (BOOLEAN.equals(writerType)) {
            return Boolean.toString((Boolean) value);
        }
        if (TINYINT.equals(writerType) || SMALLINT.equals(writerType) || INTEGER.equals(writerType) || BIGINT.equals(writerType)) {
            return Long.toString(((Number) value).longValue());
        }
        if (REAL.equals(writerType)) {
            float number = ((Number) value).floatValue();
            if (!Float.isFinite(number)) {
                return null;
            }
            return Float.toString(number);
        }
        if (DOUBLE.equals(writerType)) {
            double number = ((Number) value).doubleValue();
            if (!Double.isFinite(number)) {
                return null;
            }
            return Double.toString(number);
        }
        if (writerType instanceof DecimalType decimalType) {
            BigInteger unscaled;
            if (value instanceof Binary binary) {
                unscaled = new BigInteger(binary.getBytes());
            }
            else {
                unscaled = BigInteger.valueOf(((Number) value).longValue());
            }
            return new BigDecimal(unscaled, decimalType.getScale()).toPlainString();
        }
        if (writerType instanceof VarcharType) {
            return new String(((Binary) value).getBytes(), StandardCharsets.UTF_8);
        }
        if (VARBINARY.equals(writerType)) {
            return HexFormat.of().formatHex(((Binary) value).getBytes());
        }
        if (io.trino.spi.type.UuidType.UUID.equals(writerType)) {
            ByteBuffer buffer = ByteBuffer.wrap(((Binary) value).getBytes());
            return new UUID(buffer.getLong(), buffer.getLong()).toString();
        }
        if (DATE.equals(writerType)) {
            return LocalDate.ofEpochDay(((Number) value).longValue()).toString();
        }
        if (writerType instanceof TimeType) {
            return formatTime(LocalTime.ofNanoOfDay(((Number) value).longValue() * NANOSECONDS_PER_MICROSECOND));
        }
        if (writerType instanceof TimestampType timestampType) {
            return formatTimestamp(timestampFrom(((Number) value).longValue(), timestampType.getPrecision()));
        }
        if (writerType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return formatTimestamp(timestampFrom(((Number) value).longValue(), timestampWithTimeZoneType.getPrecision())) + "+00";
        }
        return null;
    }

    /**
     * Interprets a Parquet timestamp value. The unit follows the precision the column is written
     * with, which {@link DuckLakeParquetSchema} keeps in step with the Parquet annotation.
     */
    private static LocalDateTime timestampFrom(long value, int precision)
    {
        long unitsPerSecond = MICROSECONDS_PER_SECOND;
        long nanosPerUnit = NANOSECONDS_PER_MICROSECOND;
        if (precision <= 3) {
            unitsPerSecond = MILLISECONDS_PER_SECOND;
            nanosPerUnit = NANOSECONDS_PER_SECOND / MILLISECONDS_PER_SECOND;
        }
        else if (precision > 6) {
            unitsPerSecond = NANOSECONDS_PER_SECOND;
            nanosPerUnit = 1;
        }
        long seconds = floorDiv(value, unitsPerSecond);
        long nanos = floorMod(value, unitsPerSecond) * nanosPerUnit;
        return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
    }

    /**
     * DuckDB always prints seconds and trims the fraction to whole milliseconds or microseconds,
     * so {@code 2024-01-02 03:04:00} keeps its seconds and {@code .123000} prints as {@code .123}.
     */
    private static String formatTimestamp(LocalDateTime timestamp)
    {
        String time = formatTime(timestamp.toLocalTime());
        if (time == null) {
            return null;
        }
        return timestamp.toLocalDate() + " " + time;
    }

    private static String formatTime(LocalTime time)
    {
        StringBuilder formatted = new StringBuilder(15);
        appendTwoDigits(formatted, time.getHour());
        formatted.append(':');
        appendTwoDigits(formatted, time.getMinute());
        formatted.append(':');
        appendTwoDigits(formatted, time.getSecond());
        long nanos = time.getNano();
        if (nanos == 0) {
            return formatted.toString();
        }
        if (nanos % NANOSECONDS_PER_MICROSECOND != 0) {
            // sub-microsecond values have no DuckLake catalog representation
            return null;
        }
        long micros = nanos / NANOSECONDS_PER_MICROSECOND;
        formatted.append('.');
        if (micros % MICROSECONDS_PER_MILLISECOND == 0) {
            appendPaddedDigits(formatted, micros / MICROSECONDS_PER_MILLISECOND, 3);
        }
        else {
            appendPaddedDigits(formatted, micros, 6);
        }
        return formatted.toString();
    }

    private static void appendTwoDigits(StringBuilder builder, int value)
    {
        if (value < 10) {
            builder.append('0');
        }
        builder.append(value);
    }

    private static void appendPaddedDigits(StringBuilder builder, long value, int digits)
    {
        String text = Long.toString(value);
        builder.append("0".repeat(digits - text.length()));
        builder.append(text);
    }
}
