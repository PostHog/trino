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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.variant.VariantShreddingSchema.ArrayValue;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectField;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedType;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_VARIANT_SHREDDING;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeVariantShredding
{
    @Test
    void testDeclaration()
    {
        Map<String, Object> shredding = Map.of("type", "object", "fields", List.of(Map.of("name", "a", "type", "int64")));
        assertThat(HoglakeVariantShredding.declaration(catalogColumn("variant", Map.of("shredding", shredding))))
                .hasValueSatisfying(json -> assertThat(HoglakeVariantShredding.schema(column(json)).value())
                        .isEqualTo(object(field("a", primitive(ShreddedType.INT64)))));
        // A column of another type, and a column without a declaration, have no layout
        assertThat(HoglakeVariantShredding.declaration(catalogColumn("string", Map.of("shredding", shredding)))).isEmpty();
        assertThat(HoglakeVariantShredding.declaration(catalogColumn("variant", null))).isEmpty();
        assertThat(HoglakeVariantShredding.declaration(catalogColumn("variant", Map.of("other", 1)))).isEmpty();
        Map<String, Object> nullShredding = new HashMap<>();
        nullShredding.put("shredding", null);
        assertThat(HoglakeVariantShredding.declaration(catalogColumn("variant", nullShredding))).isEmpty();
    }

    @Test
    void testLayout()
    {
        assertThat(HoglakeVariantShredding.schema(column(
                """
                {"type": "object", "fields": [
                    {"name": "$browser", "type": "string"},
                    {"name": "payload", "type": "variant"},
                    {"name": "flag", "type": "boolean"},
                    {"name": "tiny", "type": "int8"},
                    {"name": "small", "type": "int16"},
                    {"name": "count", "type": "int32"},
                    {"name": "id", "type": "int64"},
                    {"name": "ratio", "type": "float"},
                    {"name": "score", "type": "double"},
                    {"name": "price", "type": "decimal4", "precision": 9, "scale": 2},
                    {"name": "total", "type": "decimal8", "precision": 18, "scale": 4},
                    {"name": "huge", "type": "decimal16", "precision": 38, "scale": 0},
                    {"name": "day", "type": "date"},
                    {"name": "at", "type": "time"},
                    {"name": "local", "type": "timestamp"},
                    {"name": "local_ns", "type": "timestamp_ns"},
                    {"name": "instant", "type": "timestamptz"},
                    {"name": "instant_ns", "type": "timestamptz_ns"},
                    {"name": "bytes", "type": "binary"},
                    {"name": "uuid", "type": "uuid"},
                    {"name": "tags", "type": "array", "element": {"type": "string"}},
                    {"name": "$set", "type": "object", "fields": [{"name": "plan", "type": "string"}]}]}
                """)).value())
                .isEqualTo(object(
                        field("$browser", primitive(ShreddedType.STRING)),
                        field("payload", new ShreddedValue(Optional.empty())),
                        field("flag", primitive(ShreddedType.BOOLEAN)),
                        field("tiny", primitive(ShreddedType.INT8)),
                        field("small", primitive(ShreddedType.INT16)),
                        field("count", primitive(ShreddedType.INT32)),
                        field("id", primitive(ShreddedType.INT64)),
                        field("ratio", primitive(ShreddedType.FLOAT)),
                        field("score", primitive(ShreddedType.DOUBLE)),
                        field("price", new ShreddedValue(Optional.of(PrimitiveValue.decimal(ShreddedType.DECIMAL4, 9, 2)))),
                        field("total", new ShreddedValue(Optional.of(PrimitiveValue.decimal(ShreddedType.DECIMAL8, 18, 4)))),
                        field("huge", new ShreddedValue(Optional.of(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 38, 0)))),
                        field("day", primitive(ShreddedType.DATE)),
                        field("at", primitive(ShreddedType.TIME_MICROS)),
                        field("local", primitive(ShreddedType.TIMESTAMP_MICROS)),
                        field("local_ns", primitive(ShreddedType.TIMESTAMP_NANOS)),
                        field("instant", primitive(ShreddedType.TIMESTAMP_TZ_MICROS)),
                        field("instant_ns", primitive(ShreddedType.TIMESTAMP_TZ_NANOS)),
                        field("bytes", primitive(ShreddedType.BINARY)),
                        field("uuid", primitive(ShreddedType.UUID)),
                        field("tags", new ShreddedValue(Optional.of(new ArrayValue(primitive(ShreddedType.STRING))))),
                        field("$set", object(field("plan", primitive(ShreddedType.STRING))))));
        assertThat(HoglakeVariantShredding.schema(column("{\"type\": \"string\"}")).value()).isEqualTo(primitive(ShreddedType.STRING));
    }

    @Test
    void testInvalidDeclarations()
    {
        assertInvalid("[]", "$ is not a JSON object");
        assertInvalid("{\"fields\": []}", "$ has no type");
        assertInvalid("{\"type\": 1}", "$ has no type");
        assertInvalid("{\"type\": \"text\"}", "$ has an unknown type: text");
        assertInvalid("{\"type\": \"string\", \"nullable\": true}", "$ has an unknown key: nullable");
        assertInvalid("{\"type\": \"object\"}", "$ has no fields");
        assertInvalid("{\"type\": \"object\", \"fields\": []}", "$ has no fields");
        assertInvalid("{\"type\": \"object\", \"fields\": {\"a\": {\"type\": \"string\"}}}", "$ has no fields");
        assertInvalid("{\"type\": \"object\", \"fields\": [{\"type\": \"string\"}]}", "$ has a field without a name");
        assertInvalid("{\"type\": \"object\", \"fields\": [{\"name\": \"a\"}]}", "$.a has no type");
        assertInvalid("{\"type\": \"object\", \"fields\": [{\"name\": \"a\", \"type\": \"string\", \"element\": {\"type\": \"string\"}}]}", "$.a has an unknown key: element");
        assertInvalid("{\"type\": \"array\"}", "$ has no element");
        assertInvalid("{\"type\": \"array\", \"element\": {\"type\": \"array\", \"element\": {\"type\": \"decimal\"}}}", "$[*][*] has an unknown type: decimal");
        assertInvalid("{\"type\": \"decimal8\", \"scale\": 2}", "$ has no integer precision");
        assertInvalid("{\"type\": \"decimal8\", \"precision\": 1.5, \"scale\": 0}", "$ has no integer precision");
        assertInvalid("{\"type\": \"decimal8\", \"precision\": \"18\", \"scale\": 0}", "$ has no integer precision");
        assertInvalid("{\"type\": \"decimal4\", \"precision\": 10, \"scale\": 2}", "$ has precision 10 and scale 2, which decimal4 does not hold");
        assertInvalid("{\"type\": \"decimal16\", \"precision\": 39, \"scale\": 2}", "$ has precision 39 and scale 2, which decimal16 does not hold");
        assertInvalid("{\"type\": \"decimal8\", \"precision\": 5, \"scale\": 6}", "$ has precision 5 and scale 6, which decimal8 does not hold");

        // The rules of the Parquet library
        assertInvalid(
                "{\"type\": \"object\", \"fields\": [{\"name\": \"plan\", \"type\": \"string\"}, {\"name\": \"Plan\", \"type\": \"string\"}]}",
                "Shredded VARIANT object $ has fields that differ only by case: plan and Plan");
        assertInvalid(
                "{\"type\": \"object\", \"fields\": [{\"name\": \"a\", \"type\": \"string\"}, {\"name\": \"a\", \"type\": \"int64\"}]}",
                "Shredded VARIANT object $ has duplicate field a");
        assertInvalid(
                "{\"type\": \"object\", \"fields\": [{\"name\": \"\", \"type\": \"string\"}]}",
                "Shredded VARIANT object $ has a field with an empty name");
    }

    @Test
    void testLimits()
    {
        String deepest = "{\"type\": \"string\"}";
        for (int level = 0; level < HoglakeVariantShredding.MAX_DEPTH; level++) {
            deepest = "{\"type\": \"array\", \"element\": " + deepest + "}";
        }
        HoglakeVariantShredding.schema(column(deepest));
        assertInvalid("{\"type\": \"array\", \"element\": " + deepest + "}", "$" + "[*]".repeat(HoglakeVariantShredding.MAX_DEPTH) + " is nested more than 16 levels deep");

        HoglakeVariantShredding.schema(column(objectWithFields(HoglakeVariantShredding.MAX_FIELDS)));
        assertInvalid(objectWithFields(HoglakeVariantShredding.MAX_FIELDS + 1), "$ has more than 1000 fields");
    }

    private static String objectWithFields(int count)
    {
        return IntStream.range(0, count)
                .mapToObj(field -> "{\"name\": \"k%s\", \"type\": \"string\"}".formatted(field))
                .collect(Collectors.joining(", ", "{\"type\": \"object\", \"fields\": [", "]}"));
    }

    @Test
    void testNestedColumns()
    {
        // Hoglake reads shredded files only in top-level columns
        HoglakeColumnHandle field = new HoglakeColumnHandle("x", 2, VARIANT, true, List.of(), "variant", null, List.of(), Optional.of("{\"type\": \"string\"}"));
        HoglakeColumnHandle struct = new HoglakeColumnHandle("r", 1, RowType.from(List.of(RowType.field("x", VARIANT))), true, List.of(field), "struct", null);
        assertThatThrownBy(() -> HoglakeVariantShredding.checkWritable(List.of(struct)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Hoglake writes shredded VARIANT values only in top-level columns: x");
        assertThatThrownBy(() -> HoglakeParquetSchema.create(List.of(struct)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Hoglake writes shredded VARIANT values only in top-level columns: x");
        assertThatThrownBy(() -> new HoglakeColumnHandle("id", 1, BIGINT, true, List.of(), "long", null, List.of(), Optional.of("{\"type\": \"string\"}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only a VARIANT column has a shredded layout: id");
    }

    @Test
    void testParquetSchema()
    {
        HoglakeColumnHandle id = new HoglakeColumnHandle("id", 1, BIGINT, false, List.of(), "long", null);
        HoglakeColumnHandle properties = column("{\"type\": \"object\", \"fields\": [{\"name\": \"$browser\", \"type\": \"string\"}]}");
        HoglakeParquetSchema schema = HoglakeParquetSchema.create(List.of(id, properties));
        // The field id is on the group, and the columns of the group have none
        assertThat(schema.messageType().toString()).isEqualTo(
                """
                message hoglake_schema {
                  required int64 id = 1;
                  optional group v (VARIANT(1)) = 7 {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                      required group $browser {
                        optional binary value;
                        optional binary typed_value (STRING);
                      }
                    }
                  }
                }
                """);
        assertThat(schema.primitiveTypes().keySet()).containsExactly(List.of("id"));
    }

    private static void assertInvalid(String declaration, String message)
    {
        assertThatThrownBy(() -> HoglakeVariantShredding.schema(column(declaration)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_INVALID_VARIANT_SHREDDING.toErrorCode()))
                .hasMessage("Invalid type_params.shredding of column v: " + message);
        assertThatThrownBy(() -> HoglakeVariantShredding.checkWritable(List.of(column(declaration))))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_INVALID_VARIANT_SHREDDING.toErrorCode()));
    }

    private static HoglakeColumnHandle column(String declaration)
    {
        return new HoglakeColumnHandle("v", 7, VARIANT, true, List.of(), "variant", null, List.of(), Optional.of(declaration));
    }

    private static HoglakeDtos.Column catalogColumn(String type, Map<String, Object> typeParams)
    {
        return new HoglakeDtos.Column(7, 0, "v", type, typeParams, true);
    }

    private static ShreddedValue primitive(ShreddedType type)
    {
        return new ShreddedValue(Optional.of(PrimitiveValue.of(type)));
    }

    private static ShreddedValue object(ObjectField... fields)
    {
        return new ShreddedValue(Optional.of(new ObjectValue(ImmutableList.copyOf(fields))));
    }

    private static ObjectField field(String name, ShreddedValue value)
    {
        return new ObjectField(name, value);
    }
}
