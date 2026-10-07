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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.addExact;
import static java.lang.Math.floorMod;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Reads the values of inlined rows, which a DuckDB writer stores in columns of the catalog
 * database, into the Trino type of the DuckLake column they belong to.
 * <p>
 * DuckDB stores a value in the PostgreSQL type closest to it that loses nothing: integers,
 * floating point numbers, decimals, booleans, UUIDs and times natively, strings and blobs as
 * {@code bytea} because PostgreSQL text cannot hold every string DuckDB can, and the integers and
 * temporal values whose range PostgreSQL cannot hold as the text DuckDB casts them to. DuckDB reads
 * them back by casting each stored value to the DuckLake type of its column, and these readers do
 * the same. A type the column could hold that is not stored in one of these ways, and a value that
 * has no Trino counterpart, such as an infinite date, fail the read rather than return something
 * else; so do values of nested types, which DuckDB stores as the text of a DuckDB literal.
 */
public final class DuckLakeInlinedValues
{
    private static final Set<String> INTEGER_TYPES = ImmutableSet.of("int2", "int4", "int8");
    private static final Set<String> TEXT_TYPES = ImmutableSet.of("varchar", "text");
    private static final Set<String> FLOAT_TYPES = ImmutableSet.of("float4", "float8");
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000L;
    private static final long NANOSECONDS_PER_MILLISECOND = 1_000_000L;
    private static final long MICROSECONDS_PER_SECOND = 1_000_000L;
    private static final long MILLISECONDS_PER_SECOND = 1_000L;
    private static final long PICOSECONDS_PER_NANOSECOND = 1_000L;

    private DuckLakeInlinedValues() {}

    /**
     * Writes the value of one column of the current row to the output, or a null.
     */
    public interface ValueReader
    {
        void read(ResultSet row, int column, BlockBuilder output)
                throws SQLException;
    }

    /**
     * The reader of a column holding values of {@code type}, stored in the catalog database in a
     * column of {@code postgresType}, as {@code information_schema.columns.udt_name} names it.
     */
    public static ValueReader valueReader(String columnName, Type type, String postgresType)
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(type, "type is null");
        String stored = postgresType.toLowerCase(Locale.ENGLISH);

        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // DuckDB stores these as the text of a DuckDB literal, which no Trino type parses
            return (row, column, output) -> {
                if (row.getObject(column) != null) {
                    throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Column '%s' of type %s has inlined values, which are not supported for nested types. Flush inlined data to Parquet with DuckDB first".formatted(columnName, type));
                }
                output.appendNull();
            };
        }
        if (type.equals(BOOLEAN) && stored.equals("bool")) {
            return (row, column, output) -> {
                boolean value = row.getBoolean(column);
                if (row.wasNull()) {
                    output.appendNull();
                    return;
                }
                BOOLEAN.writeBoolean(output, value);
            };
        }
        if ((type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) && INTEGER_TYPES.contains(stored)) {
            long min = integerMin(type);
            long max = integerMax(type);
            return (row, column, output) -> {
                long value = row.getLong(column);
                if (row.wasNull()) {
                    output.appendNull();
                    return;
                }
                if (value < min || value > max) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value %s of column '%s' is out of range for %s".formatted(value, columnName, type));
                }
                type.writeLong(output, value);
            };
        }
        if (type instanceof DecimalType decimalType && (stored.equals("numeric") || INTEGER_TYPES.contains(stored) || TEXT_TYPES.contains(stored))) {
            return (row, column, output) -> {
                BigDecimal value = readDecimal(row, column, stored, columnName);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                writeDecimal(output, decimalType, value, columnName);
            };
        }
        if (type.equals(REAL) && stored.equals("float4")) {
            return (row, column, output) -> {
                float value = row.getFloat(column);
                if (row.wasNull()) {
                    output.appendNull();
                    return;
                }
                REAL.writeLong(output, floatToRawIntBits(value));
            };
        }
        if (type.equals(DOUBLE) && FLOAT_TYPES.contains(stored)) {
            return (row, column, output) -> {
                // a float4 value widens to the double of the same value, as DuckDB casts it
                double value = stored.equals("float4") ? row.getFloat(column) : row.getDouble(column);
                if (row.wasNull()) {
                    output.appendNull();
                    return;
                }
                DOUBLE.writeDouble(output, value);
            };
        }
        if (type instanceof VarcharType && stored.equals("bytea")) {
            // DuckDB reinterprets the stored bytes as its string, without checking them
            return (row, column, output) -> {
                byte[] value = row.getBytes(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                type.writeSlice(output, Slices.wrappedBuffer(value));
            };
        }
        if (type instanceof VarcharType && TEXT_TYPES.contains(stored)) {
            return (row, column, output) -> {
                String value = row.getString(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                type.writeSlice(output, utf8Slice(value));
            };
        }
        if (type.equals(VARBINARY) && stored.equals("bytea")) {
            return (row, column, output) -> {
                byte[] value = row.getBytes(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                VARBINARY.writeSlice(output, Slices.wrappedBuffer(value));
            };
        }
        if (type.equals(UuidType.UUID) && stored.equals("uuid")) {
            return (row, column, output) -> {
                UUID value = row.getObject(column, UUID.class);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                UuidType.UUID.writeSlice(output, javaUuidToTrinoUuid(value));
            };
        }
        if (type.equals(DATE) && (stored.equals("date") || TEXT_TYPES.contains(stored))) {
            return (row, column, output) -> {
                String value = row.getString(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                DATE.writeLong(output, parse(columnName, type, value, () -> DuckDbTemporalText.parseEpochDay(value)));
            };
        }
        if (type instanceof TimeType timeType && (stored.equals("time") || TEXT_TYPES.contains(stored))) {
            long nanosecondsPerUnit = nanosecondsPerUnit(timeType.getPrecision());
            return (row, column, output) -> {
                String value = row.getString(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                long nanoOfDay = parse(columnName, type, value, () -> DuckDbTemporalText.parseNanoOfDay(value));
                requirePrecision(nanoOfDay, nanosecondsPerUnit, columnName, type, value);
                timeType.writeLong(output, nanoOfDay * PICOSECONDS_PER_NANOSECOND);
            };
        }
        if (type instanceof TimestampType timestampType && (stored.equals("timestamp") || TEXT_TYPES.contains(stored))) {
            long nanosecondsPerUnit = nanosecondsPerUnit(timestampType.getPrecision());
            return (row, column, output) -> {
                String value = row.getString(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                DuckDbTemporalText.Timestamp timestamp = parse(columnName, type, value, () -> DuckDbTemporalText.parseTimestamp(value));
                if (timestamp.offsetSeconds().isPresent()) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value '%s' of column '%s' of type %s carries a time zone".formatted(value, columnName, type));
                }
                requirePrecision(timestamp.nanoOfSecond(), nanosecondsPerUnit, columnName, type, value);
                long epochMicros = epochMicros(timestamp.epochSecond(), timestamp.nanoOfSecond(), columnName, type, value);
                if (timestampType.isShort()) {
                    timestampType.writeLong(output, epochMicros);
                }
                else {
                    int picosOfMicro = toIntExactPicos((timestamp.nanoOfSecond() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND);
                    timestampType.writeObject(output, new LongTimestamp(epochMicros, picosOfMicro));
                }
            };
        }
        if (type instanceof TimestampWithTimeZoneType timestampType && (stored.equals("timestamptz") || TEXT_TYPES.contains(stored))) {
            long nanosecondsPerUnit = nanosecondsPerUnit(timestampType.getPrecision());
            return (row, column, output) -> {
                String value = row.getString(column);
                if (value == null) {
                    output.appendNull();
                    return;
                }
                DuckDbTemporalText.Timestamp timestamp = parse(columnName, type, value, () -> DuckDbTemporalText.parseTimestamp(value));
                if (timestamp.offsetSeconds().isEmpty()) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value '%s' of column '%s' of type %s carries no time zone offset".formatted(value, columnName, type));
                }
                requirePrecision(timestamp.nanoOfSecond(), nanosecondsPerUnit, columnName, type, value);
                long epochMillis;
                try {
                    epochMillis = addExact(multiplyExact(timestamp.utcEpochSecond(), MILLISECONDS_PER_SECOND), timestamp.nanoOfSecond() / NANOSECONDS_PER_MILLISECOND);
                }
                catch (ArithmeticException e) {
                    throw outOfRange(columnName, type, value, e);
                }
                if (timestampType.isShort()) {
                    timestampType.writeLong(output, packDateTimeWithZone(epochMillis, UTC_KEY));
                }
                else {
                    int picosOfMilli = toIntExactPicos((timestamp.nanoOfSecond() % NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND);
                    timestampType.writeObject(output, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
                }
            };
        }
        throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Column '%s' of type %s has inlined values stored as PostgreSQL type %s, which is not supported".formatted(columnName, type, postgresType));
    }

    private static BigDecimal readDecimal(ResultSet row, int column, String stored, String columnName)
            throws SQLException
    {
        if (stored.equals("numeric")) {
            return row.getBigDecimal(column);
        }
        if (INTEGER_TYPES.contains(stored)) {
            long value = row.getLong(column);
            return row.wasNull() ? null : BigDecimal.valueOf(value);
        }
        // DuckDB writes UBIGINT and HUGEINT values as the digits of the integer
        String value = row.getString(column);
        if (value == null) {
            return null;
        }
        try {
            return new BigDecimal(new BigInteger(value));
        }
        catch (NumberFormatException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value '%s' of column '%s' is not an integer".formatted(value, columnName), e);
        }
    }

    private static void writeDecimal(BlockBuilder output, DecimalType type, BigDecimal value, String columnName)
    {
        BigInteger unscaled;
        try {
            unscaled = value.setScale(type.getScale()).unscaledValue();
        }
        catch (ArithmeticException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value %s of column '%s' has more fractional digits than %s".formatted(value, columnName, type), e);
        }
        if (unscaled.abs().compareTo(BigInteger.TEN.pow(type.getPrecision())) >= 0) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value %s of column '%s' is out of range for %s".formatted(value, columnName, type));
        }
        if (type.isShort()) {
            type.writeLong(output, unscaled.longValueExact());
        }
        else {
            Int128 decimal = Int128.valueOf(unscaled);
            if (Decimals.overflows(decimal, type.getPrecision())) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value %s of column '%s' is out of range for %s".formatted(value, columnName, type));
            }
            type.writeObject(output, decimal);
        }
    }

    private static long epochMicros(long epochSecond, int nanoOfSecond, String columnName, Type type, String value)
    {
        try {
            return addExact(multiplyExact(epochSecond, MICROSECONDS_PER_SECOND), nanoOfSecond / NANOSECONDS_PER_MICROSECOND);
        }
        catch (ArithmeticException e) {
            throw outOfRange(columnName, type, value, e);
        }
    }

    private static TrinoException outOfRange(String columnName, Type type, String value, ArithmeticException cause)
    {
        return new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value '%s' of column '%s' is out of range for %s".formatted(value, columnName, type), cause);
    }

    private static int toIntExactPicos(long picos)
    {
        return toIntExact(picos);
    }

    /**
     * The nanoseconds in one unit of the last digit a value of the precision holds.
     */
    private static long nanosecondsPerUnit(int precision)
    {
        if (precision >= 9) {
            return 1;
        }
        long nanoseconds = 1;
        for (int digit = precision; digit < 9; digit++) {
            nanoseconds *= 10;
        }
        return nanoseconds;
    }

    private static void requirePrecision(long nanoseconds, long nanosecondsPerUnit, String columnName, Type type, String value)
    {
        if (floorMod(nanoseconds, nanosecondsPerUnit) != 0) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Inlined value '%s' of column '%s' is more precise than %s".formatted(value, columnName, type));
        }
    }

    private interface Parser<T>
    {
        T parse();
    }

    private static <T> T parse(String columnName, Type type, String value, Parser<T> parser)
    {
        try {
            return parser.parse();
        }
        catch (IllegalArgumentException e) {
            if (value.toLowerCase(Locale.ENGLISH).contains("infinity")) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Column '%s' of type %s has the inlined value '%s', which Trino cannot represent".formatted(columnName, type, value), e);
            }
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Cannot read inlined value '%s' of column '%s' as %s".formatted(value, columnName, type), e);
        }
    }

    private static long integerMin(Type type)
    {
        if (type.equals(TINYINT)) {
            return Byte.MIN_VALUE;
        }
        if (type.equals(SMALLINT)) {
            return Short.MIN_VALUE;
        }
        if (type.equals(INTEGER)) {
            return Integer.MIN_VALUE;
        }
        return Long.MIN_VALUE;
    }

    private static long integerMax(Type type)
    {
        if (type.equals(TINYINT)) {
            return Byte.MAX_VALUE;
        }
        if (type.equals(SMALLINT)) {
            return Short.MAX_VALUE;
        }
        if (type.equals(INTEGER)) {
            return Integer.MAX_VALUE;
        }
        return Long.MAX_VALUE;
    }
}
