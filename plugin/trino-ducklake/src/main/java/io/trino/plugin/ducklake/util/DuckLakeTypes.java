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
import com.google.common.collect.ListMultimap;
import io.trino.metastore.HiveType;
import io.trino.metastore.type.TypeInfo;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Locale;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveType.HIVE_BINARY;
import static io.trino.metastore.HiveType.HIVE_BOOLEAN;
import static io.trino.metastore.HiveType.HIVE_BYTE;
import static io.trino.metastore.HiveType.HIVE_DATE;
import static io.trino.metastore.HiveType.HIVE_DOUBLE;
import static io.trino.metastore.HiveType.HIVE_FLOAT;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_SHORT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.HiveType.HIVE_TIMESTAMP;
import static io.trino.metastore.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class DuckLakeTypes
{
    public static final DecimalType UNSIGNED_BIGINT_DECIMAL_TYPE = DecimalType.createDecimalType(20, 0);
    public static final DecimalType INT128_DECIMAL_TYPE = DecimalType.createDecimalType(38, 0);

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private DuckLakeTypes() {}

    /**
     * Parses a scalar DuckLake type string (as stored in {@code ducklake_column.column_type})
     * to the corresponding Trino type. Nested types ({@code list}, {@code struct}, {@code map})
     * are handled by {@link #toTrinoType(DuckLakeColumnEntry, ListMultimap)}.
     */
    public static Type toTrinoType(String duckLakeType)
    {
        String type = duckLakeType.trim().toLowerCase(Locale.ENGLISH);
        if ((type.startsWith("decimal(") || type.startsWith("numeric(")) && type.endsWith(")")) {
            return parseDecimal(duckLakeType, type);
        }
        return switch (type) {
            case "boolean", "bool" -> BOOLEAN;
            case "int8", "tinyint" -> TINYINT;
            case "int16", "smallint" -> SMALLINT;
            case "int32", "integer", "int", "int4" -> INTEGER;
            case "int64", "bigint", "long" -> BIGINT;
            case "uint8", "utinyint" -> SMALLINT;
            case "uint16", "usmallint" -> INTEGER;
            case "uint32", "uinteger" -> BIGINT;
            case "uint64", "ubigint" -> UNSIGNED_BIGINT_DECIMAL_TYPE;
            case "int128", "hugeint" -> INT128_DECIMAL_TYPE;
            case "float32", "float", "real" -> REAL;
            case "float64", "double" -> DOUBLE;
            case "varchar", "text", "string" -> VARCHAR;
            case "json" -> VARCHAR;
            case "blob", "bytea", "varbinary", "binary" -> VARBINARY;
            case "uuid" -> UUID;
            case "date" -> DATE;
            case "time" -> TimeType.TIME_MICROS;
            case "timestamp", "timestamp_us", "datetime" -> TimestampType.TIMESTAMP_MICROS;
            case "timestamp_s" -> TimestampType.TIMESTAMP_SECONDS;
            case "timestamp_ms" -> TimestampType.TIMESTAMP_MILLIS;
            case "timestamp_ns" -> TimestampType.TIMESTAMP_NANOS;
            case "timestamptz", "timestamp with time zone" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            default -> throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Unsupported DuckLake type: " + duckLakeType);
        };
    }

    /**
     * Resolves the Trino type of a column, reconstructing nested types from the
     * {@code parent_column} relationships of {@code ducklake_column}.
     */
    public static Type toTrinoType(DuckLakeColumnEntry column, ListMultimap<Long, DuckLakeColumnEntry> childrenByParent)
    {
        String type = column.columnType().trim().toLowerCase(Locale.ENGLISH);
        List<DuckLakeColumnEntry> children = childrenByParent.get(column.columnId());
        switch (type) {
            case "list" -> {
                if (children.size() != 1) {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Expected exactly one child column for list column '%s', found %s".formatted(column.columnName(), children.size()));
                }
                return new ArrayType(toTrinoType(children.getFirst(), childrenByParent));
            }
            case "map" -> {
                if (children.size() != 2) {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Expected exactly two child columns for map column '%s', found %s".formatted(column.columnName(), children.size()));
                }
                Type keyType = toTrinoType(children.getFirst(), childrenByParent);
                Type valueType = toTrinoType(children.getLast(), childrenByParent);
                return new MapType(keyType, valueType, TYPE_OPERATORS);
            }
            case "struct" -> {
                if (children.isEmpty()) {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Struct column '%s' has no child columns".formatted(column.columnName()));
                }
                List<RowType.Field> fields = children.stream()
                        .map(child -> RowType.field(child.columnName(), toTrinoType(child, childrenByParent)))
                        .collect(toImmutableList());
                return RowType.from(fields);
            }
            default -> {
                return toTrinoType(column.columnType());
            }
        }
    }

    /**
     * The DuckLake type a Trino type is stored as, as written to {@code ducklake_column.column_type}.
     * Nested types yield the name of the container; their element types are stored on the child
     * columns.
     * <p>
     * Types whose values DuckLake can hold exactly but whose declared precision it cannot express
     * are stored at the next precision that holds them, so a {@code TIMESTAMP(1)} column becomes a
     * millisecond timestamp. Types DuckLake cannot represent at all, such as bounded
     * {@code VARCHAR}, are rejected rather than silently stored as something else.
     */
    public static String toDuckLakeType(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return "boolean";
        }
        if (TINYINT.equals(type)) {
            return "int8";
        }
        if (SMALLINT.equals(type)) {
            return "int16";
        }
        if (INTEGER.equals(type)) {
            return "int32";
        }
        if (BIGINT.equals(type)) {
            return "int64";
        }
        if (REAL.equals(type)) {
            return "float32";
        }
        if (DOUBLE.equals(type)) {
            return "float64";
        }
        if (type instanceof DecimalType decimalType) {
            return "decimal(%s,%s)".formatted(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof VarcharType varcharType) {
            if (!varcharType.isUnbounded()) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake does not support a maximum length on VARCHAR columns; use unbounded VARCHAR instead of " + type);
            }
            return "varchar";
        }
        if (VARBINARY.equals(type)) {
            return "blob";
        }
        if (UUID.equals(type)) {
            return "uuid";
        }
        if (DATE.equals(type)) {
            return "date";
        }
        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() > 6) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake stores times with microsecond precision; " + type + " is not supported");
            }
            return "time";
        }
        if (type instanceof TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (precision == 0) {
                return "timestamp_s";
            }
            if (precision <= 3) {
                return "timestamp_ms";
            }
            if (precision <= 6) {
                return "timestamp";
            }
            return "timestamp_ns";
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() > 6) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake stores times with time zone with microsecond precision; " + type + " is not supported");
            }
            return "timestamptz";
        }
        if (type instanceof ArrayType) {
            return "list";
        }
        if (type instanceof MapType) {
            return "map";
        }
        if (type instanceof RowType) {
            return "struct";
        }
        throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake does not support columns of type " + type);
    }

    public static boolean isUnsignedBigint(String duckLakeType)
    {
        String type = duckLakeType.trim().toLowerCase(Locale.ENGLISH);
        return type.equals("uint64") || type.equals("ubigint");
    }

    public static boolean isInt128(String duckLakeType)
    {
        String type = duckLakeType.trim().toLowerCase(Locale.ENGLISH);
        return type.equals("int128") || type.equals("hugeint");
    }

    public static boolean isUnsignedInteger(String duckLakeType)
    {
        String type = duckLakeType.trim().toLowerCase(Locale.ENGLISH);
        return type.equals("uint32") || type.equals("uinteger");
    }

    /**
     * Translates a Trino type to a Hive type for use in {@link io.trino.plugin.hive.HiveColumnHandle}.
     * The Hive type is only nominal on the DuckLake read path (the Parquet reader resolves columns
     * by name and decodes into the Trino type), so types without an exact Hive equivalent are mapped
     * to their closest physical representation.
     */
    public static HiveType toHiveType(Type type)
    {
        return HiveType.fromTypeInfo(translate(type));
    }

    private static TypeInfo translate(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN.getTypeInfo();
        }
        if (TINYINT.equals(type)) {
            return HIVE_BYTE.getTypeInfo();
        }
        if (SMALLINT.equals(type)) {
            return HIVE_SHORT.getTypeInfo();
        }
        if (INTEGER.equals(type)) {
            return HIVE_INT.getTypeInfo();
        }
        if (BIGINT.equals(type)) {
            return HIVE_LONG.getTypeInfo();
        }
        if (REAL.equals(type)) {
            return HIVE_FLOAT.getTypeInfo();
        }
        if (DOUBLE.equals(type)) {
            return HIVE_DOUBLE.getTypeInfo();
        }
        if (type instanceof VarcharType) {
            return HIVE_STRING.getTypeInfo();
        }
        if (VARBINARY.equals(type) || UUID.equals(type)) {
            return HIVE_BINARY.getTypeInfo();
        }
        if (DATE.equals(type)) {
            return HIVE_DATE.getTypeInfo();
        }
        if (type instanceof DecimalType decimalType) {
            return getDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof TimeType) {
            return HIVE_LONG.getTypeInfo();
        }
        if (type instanceof ArrayType arrayType) {
            return getListTypeInfo(translate(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return getMapTypeInfo(translate(mapType.getKeyType()), translate(mapType.getValueType()));
        }
        if (type instanceof RowType rowType) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            ImmutableList.Builder<TypeInfo> fieldTypes = ImmutableList.builder();
            for (RowType.Field field : rowType.getFields()) {
                fieldNames.add(field.getName().orElseThrow(() -> new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Anonymous row fields are not supported: " + rowType)));
                fieldTypes.add(translate(field.getType()));
            }
            return getStructTypeInfo(fieldNames.build(), fieldTypes.build());
        }
        throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Unsupported type: " + type);
    }

    private static Type parseDecimal(String original, String normalized)
    {
        int open = normalized.indexOf('(');
        String arguments = normalized.substring(open + 1, normalized.length() - 1);
        String[] parts = arguments.split(",");
        if (parts.length != 2) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Invalid DuckLake decimal type: " + original);
        }
        try {
            int precision = Integer.parseInt(parts[0].trim());
            int scale = Integer.parseInt(parts[1].trim());
            return DecimalType.createDecimalType(precision, scale);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Invalid DuckLake decimal type: " + original, e);
        }
    }
}
