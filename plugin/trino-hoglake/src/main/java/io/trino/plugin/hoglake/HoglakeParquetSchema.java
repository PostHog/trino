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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public record HoglakeParquetSchema(MessageType messageType, Map<List<String>, Type> primitiveTypes)
{
    public static HoglakeParquetSchema create(List<HoglakeColumnHandle> columns)
    {
        Types.MessageTypeBuilder message = Types.buildMessage();
        ImmutableMap.Builder<List<String>, Type> types = ImmutableMap.builder();
        for (HoglakeColumnHandle column : columns) {
            message.addField(field(column, List.of(), types));
        }
        return new HoglakeParquetSchema(message.named("hoglake_schema"), types.buildOrThrow());
    }

    private static org.apache.parquet.schema.Type field(HoglakeColumnHandle column, List<String> parent, ImmutableMap.Builder<List<String>, Type> types)
    {
        List<String> path = new ArrayList<>(parent);
        path.add(column.name());
        var repetition = column.nullable() ? OPTIONAL : REQUIRED;
        String wire = column.hoglakeType();
        if (wire.equals("struct")) {
            var group = Types.buildGroup(repetition);
            column.children().forEach(child -> group.addField(field(child, path, types)));
            return group.id(toIntExact(column.fieldId())).named(column.name());
        }
        if (wire.equals("list") || wire.equals("map")) {
            boolean list = wire.equals("list");
            List<String> repeatedPath = new ArrayList<>(path);
            String repeatedName = list ? "list" : "key_value";
            repeatedPath.add(repeatedName);
            var repeated = Types.buildGroup(REPEATED);
            column.children().forEach(child -> repeated.addField(field(child, repeatedPath, types)));
            return Types.buildGroup(repetition)
                    .as(list ? LogicalTypeAnnotation.listType() : LogicalTypeAnnotation.mapType())
                    .addField(repeated.named(repeatedName))
                    .id(toIntExact(column.fieldId())).named(column.name());
        }
        if (wire.equals("variant")) {
            return Types.buildGroup(repetition).as(LogicalTypeAnnotation.variantType((byte) 1))
                    .addField(Types.required(BINARY).named("metadata"))
                    .addField(Types.required(BINARY).named("value"))
                    .id(toIntExact(column.fieldId())).named(column.name());
        }
        Types.PrimitiveBuilder<PrimitiveType> field = switch (wire) {
            case "boolean" -> Types.primitive(BOOLEAN, repetition);
            case "uint8" -> Types.primitive(INT32, repetition).as(LogicalTypeAnnotation.intType(8, false));
            case "uint16" -> Types.primitive(INT32, repetition).as(LogicalTypeAnnotation.intType(16, false));
            case "uint32" -> Types.primitive(INT64, repetition);
            case "uint64" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.intType(64, false));
            case "json" -> Types.primitive(BINARY, repetition).as(LogicalTypeAnnotation.jsonType());
            case "timestamp_s", "timestamp_ms" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS));
            case "int8" -> Types.primitive(INT32, repetition).as(LogicalTypeAnnotation.intType(8, true));
            case "int16" -> Types.primitive(INT32, repetition).as(LogicalTypeAnnotation.intType(16, true));
            case "timestamp_ns" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, NANOS));
            case "int" -> Types.primitive(INT32, repetition);
            case "long" -> Types.primitive(INT64, repetition);
            case "float" -> Types.primitive(FLOAT, repetition);
            case "double" -> Types.primitive(DOUBLE, repetition);
            case "string" -> Types.primitive(BINARY, repetition).as(LogicalTypeAnnotation.stringType());
            case "binary" -> Types.primitive(BINARY, repetition);
            case "date" -> Types.primitive(INT32, repetition).as(LogicalTypeAnnotation.dateType());
            case "time" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.timeType(false, MICROS));
            case "timestamp" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.timestampType(false, MICROS));
            case "timestamptz" -> Types.primitive(INT64, repetition).as(LogicalTypeAnnotation.timestampType(true, MICROS));
            case "uuid" -> Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(16).as(LogicalTypeAnnotation.uuidType());
            case "decimal" -> {
                DecimalType decimal = (DecimalType) column.type();
                Types.PrimitiveBuilder<PrimitiveType> primitive;
                if (decimal.getPrecision() <= 9) {
                    primitive = Types.primitive(INT32, repetition);
                }
                else if (decimal.isShort()) {
                    primitive = Types.primitive(INT64, repetition);
                }
                else {
                    primitive = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(16);
                }
                yield primitive.as(LogicalTypeAnnotation.decimalType(decimal.getScale(), decimal.getPrecision()));
            }
            default -> throw new IllegalArgumentException("Unsupported column: " + column);
        };
        types.put(List.copyOf(path), wire.equals("timestamp_s") || wire.equals("timestamp_ms")
                ? TimestampType.TIMESTAMP_MILLIS
                : HoglakeUnsigned.physicalType(column));
        return field.id(toIntExact(column.fieldId())).named(column.name());
    }
}
