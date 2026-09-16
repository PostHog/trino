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
import io.trino.spi.type.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public record HoglakeParquetSchema(MessageType messageType, Map<List<String>, Type> primitiveTypes)
{
    public static HoglakeParquetSchema create(List<HoglakeColumnHandle> columns)
    {
        Types.MessageTypeBuilder message = Types.buildMessage();
        ImmutableMap.Builder<List<String>, Type> types = ImmutableMap.builder();
        for (HoglakeColumnHandle column : columns) {
            Types.PrimitiveBuilder<PrimitiveType> field = switch (HoglakeTypes.toHoglakeType(column.type())) {
                case "boolean" -> Types.optional(BOOLEAN);
                case "int" -> Types.optional(INT32);
                case "long" -> Types.optional(INT64);
                case "float" -> Types.optional(FLOAT);
                case "double" -> Types.optional(DOUBLE);
                case "string" -> Types.optional(BINARY).as(LogicalTypeAnnotation.stringType());
                case "binary" -> Types.optional(BINARY);
                case "date" -> Types.optional(INT32).as(LogicalTypeAnnotation.dateType());
                case "time" -> Types.optional(INT64).as(LogicalTypeAnnotation.timeType(false, MICROS));
                case "timestamp" -> Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(false, MICROS));
                case "timestamptz" -> Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, MICROS));
                case "uuid" -> Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(LogicalTypeAnnotation.uuidType());
                case "decimal" -> {
                    DecimalType decimal = (DecimalType) column.type();
                    Types.PrimitiveBuilder<PrimitiveType> primitive;
                    if (decimal.getPrecision() <= 9) {
                        primitive = Types.optional(INT32);
                    }
                    else if (decimal.isShort()) {
                        primitive = Types.optional(INT64);
                    }
                    else {
                        primitive = Types.optional(FIXED_LEN_BYTE_ARRAY).length(16);
                    }
                    yield primitive.as(LogicalTypeAnnotation.decimalType(decimal.getScale(), decimal.getPrecision()));
                }
                default -> throw new IllegalArgumentException("Unsupported column: " + column);
            };
            message.addField(field.id(toIntExact(column.fieldId())).named(column.name()));
            types.put(List.of(column.name()), column.type());
        }
        return new HoglakeParquetSchema(message.named("hoglake_schema"), types.buildOrThrow());
    }
}
