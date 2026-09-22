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

import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.VariantType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * The hoglake &lt;-&gt; Trino type mapping. Hoglake's column vocabulary is the
 * closed set from openapi/hoglake.yaml (ColumnDef.type); time-bearing
 * types are fixed at microsecond precision by the catalog, hence the
 * (6)s on the Trino side.
 *
 * <pre>
 *   long        BIGINT                     int        INTEGER
 *   double      DOUBLE                     float      REAL
 *   boolean     BOOLEAN                    string     VARCHAR
 *   binary      VARBINARY                  date       DATE
 *   time        TIME(6)                    timestamp  TIMESTAMP(6)
 *   timestamptz TIMESTAMP(6) WITH TZ       uuid       UUID
 *   decimal(p,s) DECIMAL(p,s)  (type_params {"precision":p,"scale":s})
 * </pre>
 */
public final class HoglakeTypes
{
    private HoglakeTypes() {}

    /**
     * Map a hoglake wire type (+ optional type_params) to the Trino type.
     */
    public static Type toTrinoType(String hoglakeType, Map<String, Object> typeParams)
    {
        return switch (hoglakeType) {
            case "boolean" -> BooleanType.BOOLEAN;
            case "uint8" -> SmallintType.SMALLINT;
            case "uint16" -> IntegerType.INTEGER;
            case "uint32" -> BigintType.BIGINT;
            case "uint64" -> DecimalType.createDecimalType(20, 0);
            case "json" -> VarcharType.VARCHAR;
            case "timestamp_s", "timestamp_ms" -> TimestampType.TIMESTAMP_MICROS;
            case "int8" -> TinyintType.TINYINT;
            case "int16" -> SmallintType.SMALLINT;
            case "timestamp_ns" -> TimestampType.TIMESTAMP_NANOS;
            case "variant" -> VariantType.VARIANT;
            case "int" -> IntegerType.INTEGER;
            case "long" -> BigintType.BIGINT;
            case "float" -> RealType.REAL;
            case "double" -> DoubleType.DOUBLE;
            case "string" -> VarcharType.VARCHAR;
            case "binary" -> VarbinaryType.VARBINARY;
            case "date" -> DateType.DATE;
            case "time" -> TimeType.TIME_MICROS;
            case "timestamp" -> TimestampType.TIMESTAMP_MICROS;
            case "timestamptz" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            case "uuid" -> UuidType.UUID;
            case "decimal" -> decimalType(typeParams);
            default -> throw new TrinoException(
                    NOT_SUPPORTED,
                    "Unsupported hoglake type: " + hoglakeType);
        };
    }

    public static Type toTrinoType(HoglakeDtos.Column column)
    {
        return switch (column.type()) {
            case "list" -> new ArrayType(toTrinoType(column.children().getFirst()));
            case "map" -> new MapType(toTrinoType(column.children().get(0)), toTrinoType(column.children().get(1)), new TypeOperators());
            case "struct" -> RowType.from(column.children().stream().map(child -> RowType.field(child.name(), toTrinoType(child))).toList());
            default -> toTrinoType(column.type(), column.typeParams());
        };
    }

    public static HoglakeDtos.ColumnDefinition columnDefinition(String name, Type type, boolean nullable)
    {
        Map<String, Object> params = null;
        List<HoglakeDtos.ColumnDefinition> children = List.of();
        if (type instanceof DecimalType decimal) {
            params = Map.of("precision", decimal.getPrecision(), "scale", decimal.getScale());
        }
        if (type instanceof ArrayType array) {
            children = List.of(columnDefinition("element", array.getElementType(), true));
        }
        if (type instanceof MapType map) {
            children = List.of(columnDefinition("key", map.getKeyType(), false), columnDefinition("value", map.getValueType(), true));
        }
        if (type instanceof RowType row) {
            children = row.getFields().stream().map(field -> columnDefinition(
                    field.getName().orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Hoglake ROW fields must be named")), field.getType(), true)).toList();
        }
        return new HoglakeDtos.ColumnDefinition(name, toHoglakeType(type), params, nullable, children);
    }

    public static boolean requiresRecursiveWriteSchema(Type type)
    {
        return type instanceof ArrayType || type instanceof MapType || type instanceof RowType ||
                type.equals(TinyintType.TINYINT) || type.equals(SmallintType.SMALLINT) ||
                type.equals(TimestampType.TIMESTAMP_NANOS) || type.equals(VariantType.VARIANT);
    }

    public static boolean canPromote(Type source, Type target)
    {
        return (source.equals(TinyintType.TINYINT) && (target.equals(SmallintType.SMALLINT) || target.equals(IntegerType.INTEGER) || target.equals(BigintType.BIGINT))) ||
                (source.equals(SmallintType.SMALLINT) && (target.equals(IntegerType.INTEGER) || target.equals(BigintType.BIGINT))) ||
                (source.equals(IntegerType.INTEGER) && target.equals(BigintType.BIGINT)) ||
                (source.equals(RealType.REAL) && target.equals(DoubleType.DOUBLE));
    }

    private static Type decimalType(Map<String, Object> typeParams)
    {
        int precision = intParam(typeParams, "precision");
        int scale = intParam(typeParams, "scale");
        try {
            return DecimalType.createDecimalType(precision, scale);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Invalid hoglake decimal parameters: precision=" + precision + ", scale=" + scale,
                    e);
        }
    }

    private static int intParam(Map<String, Object> typeParams, String name)
    {
        Object value = typeParams == null ? null : typeParams.get(name);
        if (!(value instanceof Number number)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "hoglake decimal type requires integer type_params." + name + ", got: " + value);
        }
        return number.intValue();
    }

    /**
     * Map a Trino type back to the hoglake wire type. Types outside Hoglake's
     * scalar vocabulary are rejected. Metadata widens lower temporal precisions
     * to microseconds before creating tables.
     */
    public static String toHoglakeType(Type type)
    {
        if (type instanceof ArrayType) {
            return "list";
        }
        if (type instanceof MapType) {
            return "map";
        }
        if (type instanceof RowType) {
            return "struct";
        }
        if (type.equals(VariantType.VARIANT)) {
            return "variant";
        }
        if (type.equals(TinyintType.TINYINT)) {
            return "int8";
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return "int16";
        }
        if (type.equals(TimestampType.TIMESTAMP_NANOS)) {
            return "timestamp_ns";
        }
        if (type.equals(BooleanType.BOOLEAN)) {
            return "boolean";
        }
        if (type.equals(IntegerType.INTEGER)) {
            return "int";
        }
        if (type.equals(BigintType.BIGINT)) {
            return "long";
        }
        if (type.equals(RealType.REAL)) {
            return "float";
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return "double";
        }
        if (type instanceof VarcharType) {
            return "string";
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return "binary";
        }
        if (type.equals(DateType.DATE)) {
            return "date";
        }
        if (type.equals(TimeType.TIME_MICROS)) {
            return "time";
        }
        if (type.equals(TimestampType.TIMESTAMP_MICROS)) {
            return "timestamp";
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS)) {
            return "timestamptz";
        }
        if (type.equals(UuidType.UUID)) {
            return "uuid";
        }
        if (type instanceof DecimalType) {
            return "decimal";
        }
        throw new TrinoException(NOT_SUPPORTED, "Trino type has no hoglake equivalent: " + type);
    }
}
