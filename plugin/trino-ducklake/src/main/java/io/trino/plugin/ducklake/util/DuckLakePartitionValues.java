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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Optional;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

/**
 * Renders the partition value of a row as DuckLake records it, and as the directory the file is
 * written into.
 * <p>
 * The catalog holds the value as the string DuckDB's {@code VARCHAR} cast produces, which is what
 * the read path parses back when pruning files. The directory is Hive-style, percent-escaped so
 * that any value is a single path segment, and matches the layout DuckDB writes — the path is
 * decorative, since the catalog is what readers resolve, but keeping it identical makes a lake
 * written by either engine look the same.
 */
public final class DuckLakePartitionValues
{
    private static final String NULL_DIRECTORY_VALUE = "__HIVE_DEFAULT_PARTITION__";
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;
    private static final long PICOSECONDS_PER_NANOSECOND = 1_000;
    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long MICROSECONDS_PER_MILLISECOND = 1_000;
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    private static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;

    private DuckLakePartitionValues() {}

    /**
     * The catalog representation of one partition value, or {@link Optional#empty()} for a null.
     */
    public static Optional<String> format(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return Optional.empty();
        }
        return Optional.of(formatValue(type, block, position));
    }

    private static String formatValue(Type type, Block block, int position)
    {
        if (BOOLEAN.equals(type)) {
            return Boolean.toString(BOOLEAN.getBoolean(block, position));
        }
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
            return Long.toString(type.getLong(block, position));
        }
        if (REAL.equals(type)) {
            return Float.toString(Float.intBitsToFloat(REAL.getInt(block, position)));
        }
        if (DOUBLE.equals(type)) {
            return Double.toString(DOUBLE.getDouble(block, position));
        }
        if (type instanceof DecimalType decimalType) {
            BigInteger unscaled;
            if (decimalType.isShort()) {
                unscaled = BigInteger.valueOf(decimalType.getLong(block, position));
            }
            else {
                unscaled = ((Int128) decimalType.getObject(block, position)).toBigInteger();
            }
            return new BigDecimal(unscaled, decimalType.getScale()).toPlainString();
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (VARBINARY.equals(type)) {
            return HexFormat.of().formatHex(VARBINARY.getSlice(block, position).getBytes());
        }
        if (io.trino.spi.type.UuidType.UUID.equals(type)) {
            Slice slice = type.getSlice(block, position);
            return trinoUuidToJavaUuid(slice).toString();
        }
        if (DATE.equals(type)) {
            return LocalDate.ofEpochDay(DATE.getLong(block, position)).toString();
        }
        if (type instanceof TimeType) {
            long picos = type.getLong(block, position);
            return formatTime(LocalTime.ofNanoOfDay(floorDiv(picos, PICOSECONDS_PER_NANOSECOND)));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return formatTimestamp(fromEpochMicrosAndPicos(timestampType.getLong(block, position), 0));
            }
            LongTimestamp timestamp = (LongTimestamp) timestampType.getObject(block, position);
            return formatTimestamp(fromEpochMicrosAndPicos(timestamp.getEpochMicros(), timestamp.getPicosOfMicro()));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.isShort()) {
                long millis = unpackMillisUtc(timestampWithTimeZoneType.getLong(block, position));
                return formatTimestamp(fromEpochMicrosAndPicos(millis * MICROSECONDS_PER_MILLISECOND, 0)) + "+00";
            }
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) timestampWithTimeZoneType.getObject(block, position);
            long micros = timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND
                    + timestamp.getPicosOfMilli() / PICOSECONDS_PER_MICROSECOND;
            return formatTimestamp(fromEpochMicrosAndPicos(micros, 0)) + "+00";
        }
        throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake cannot partition on a column of type " + type);
    }

    /**
     * Whether values of the type have a catalog representation that round-trips, which is what
     * makes a column usable as a partition key.
     */
    public static boolean isPartitionable(Type type)
    {
        return BOOLEAN.equals(type)
                || TINYINT.equals(type)
                || SMALLINT.equals(type)
                || INTEGER.equals(type)
                || BIGINT.equals(type)
                || REAL.equals(type)
                || DOUBLE.equals(type)
                || DATE.equals(type)
                || VARBINARY.equals(type)
                || io.trino.spi.type.UuidType.UUID.equals(type)
                || type instanceof DecimalType
                || type instanceof VarcharType
                || type instanceof TimeType
                || type instanceof TimestampType
                || type instanceof TimestampWithTimeZoneType;
    }

    /**
     * The directory segment a value is written under, following the Hive layout DuckDB uses.
     */
    public static String directorySegment(String columnName, Optional<String> value)
    {
        return escape(columnName) + "=" + escape(value.orElse(NULL_DIRECTORY_VALUE)) + "/";
    }

    /**
     * Percent-escapes everything that is not unreserved, so that a value containing a separator,
     * a space or an equals sign still forms a single path segment.
     */
    private static String escape(String value)
    {
        StringBuilder escaped = new StringBuilder(value.length());
        for (byte encoded : value.getBytes(StandardCharsets.UTF_8)) {
            int character = encoded & 0xFF;
            if (isUnreserved(character)) {
                escaped.append((char) character);
            }
            else {
                escaped.append('%');
                escaped.append(HexFormat.of().withUpperCase().toHexDigits((byte) character));
            }
        }
        return escaped.toString();
    }

    private static boolean isUnreserved(int character)
    {
        return (character >= 'a' && character <= 'z')
                || (character >= 'A' && character <= 'Z')
                || (character >= '0' && character <= '9')
                || character == '-' || character == '_' || character == '.' || character == '~';
    }

    private static LocalDateTime fromEpochMicrosAndPicos(long micros, int picosOfMicro)
    {
        long seconds = floorDiv(micros, MICROSECONDS_PER_SECOND);
        long nanos = floorMod(micros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND
                + picosOfMicro / PICOSECONDS_PER_NANOSECOND;
        return LocalDateTime.ofEpochSecond(seconds, (int) Math.min(nanos, NANOSECONDS_PER_SECOND - 1), ZoneOffset.UTC);
    }

    private static String formatTimestamp(LocalDateTime timestamp)
    {
        return timestamp.toLocalDate() + " " + formatTime(timestamp.toLocalTime());
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
