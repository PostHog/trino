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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.parquet.variant.VariantShreddingSchema;
import io.trino.parquet.variant.VariantShreddingSchema.ArrayValue;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectField;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedType;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;

import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_VARIANT_SHREDDING;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static java.util.Objects.requireNonNull;

/**
 * The shredded layout of a VARIANT column, which its catalog column declares in
 * {@code type_params.shredding}. Writes split each value into the columns of the
 * layout. Reads use the layout of each data file, so a declaration never affects
 * them. A declaration is a tree of JSON objects:
 *
 * <pre>
 * {"type": "object", "fields": [
 *     {"name": "$browser", "type": "string"},
 *     {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
 *     {"name": "tags", "type": "array", "element": {"type": "string"}},
 *     {"name": "payload", "type": "variant"}]}
 * </pre>
 *
 * <p>The primitive types are the Variant types of the shredding specification:
 * {@code boolean}, {@code int8}, {@code int16}, {@code int32}, {@code int64},
 * {@code float}, {@code double}, {@code decimal4}, {@code decimal8} and
 * {@code decimal16} (with a precision and a scale), {@code date}, {@code time},
 * {@code timestamp}, {@code timestamp_ns}, {@code timestamptz},
 * {@code timestamptz_ns}, {@code binary}, {@code string} and {@code uuid}. A
 * {@code variant} field is shredded without a type, into its own value column.
 * Values of other types stay in the value columns. Only top-level columns can be
 * shredded, because Hoglake reads shredded files only in top-level columns.
 */
final class HoglakeVariantShredding
{
    static final int MAX_DEPTH = 16;
    static final int MAX_FIELDS = 1000;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, ShreddedType> PRIMITIVE_TYPES = ImmutableMap.<String, ShreddedType>builder()
            .put("boolean", ShreddedType.BOOLEAN)
            .put("int8", ShreddedType.INT8)
            .put("int16", ShreddedType.INT16)
            .put("int32", ShreddedType.INT32)
            .put("int64", ShreddedType.INT64)
            .put("float", ShreddedType.FLOAT)
            .put("double", ShreddedType.DOUBLE)
            .put("date", ShreddedType.DATE)
            .put("time", ShreddedType.TIME_MICROS)
            .put("timestamp", ShreddedType.TIMESTAMP_MICROS)
            .put("timestamp_ns", ShreddedType.TIMESTAMP_NANOS)
            .put("timestamptz", ShreddedType.TIMESTAMP_TZ_MICROS)
            .put("timestamptz_ns", ShreddedType.TIMESTAMP_TZ_NANOS)
            .put("binary", ShreddedType.BINARY)
            .put("string", ShreddedType.STRING)
            .put("uuid", ShreddedType.UUID)
            .buildOrThrow();
    private static final Map<String, ShreddedType> DECIMAL_TYPES = ImmutableMap.of(
            "decimal4", ShreddedType.DECIMAL4,
            "decimal8", ShreddedType.DECIMAL8,
            "decimal16", ShreddedType.DECIMAL16);

    private HoglakeVariantShredding() {}

    /**
     * The declaration of a catalog column, as JSON text, or empty if the column is not
     * a VARIANT or does not declare a layout.
     */
    static Optional<String> declaration(HoglakeDtos.Column column)
    {
        if (!column.type().equals("variant") || column.typeParams() == null || column.typeParams().get("shredding") == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(MAPPER.writeValueAsString(column.typeParams().get("shredding")));
        }
        catch (JsonProcessingException e) {
            // The parameters were parsed from JSON
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Fails if a column, or a field of a struct, list or map column, has a declaration
     * that Hoglake cannot write.
     */
    static void checkWritable(List<HoglakeColumnHandle> columns)
    {
        for (HoglakeColumnHandle column : columns) {
            if (column.variantShredding().isPresent()) {
                schema(column);
            }
            column.children().forEach(HoglakeVariantShredding::checkNotShredded);
        }
    }

    private static void checkNotShredded(HoglakeColumnHandle field)
    {
        if (field.variantShredding().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake writes shredded VARIANT values only in top-level columns: " + field.name());
        }
        field.children().forEach(HoglakeVariantShredding::checkNotShredded);
    }

    /**
     * The layout of a top-level column with a declaration.
     */
    static VariantShreddingSchema schema(HoglakeColumnHandle column)
    {
        JsonNode declaration;
        try {
            declaration = MAPPER.readTree(column.variantShredding().orElseThrow());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(HOGLAKE_INVALID_VARIANT_SHREDDING, "Invalid type_params.shredding of column %s: not JSON".formatted(column.name()), e);
        }
        ShreddedValue value = new Parser(column).value(declaration, "$", 0, ImmutableSet.of());
        try {
            return VariantShreddingSchema.of(value);
        }
        catch (IllegalArgumentException e) {
            throw invalid(column, e.getMessage());
        }
    }

    private static TrinoException invalid(HoglakeColumnHandle column, String message)
    {
        return new TrinoException(HOGLAKE_INVALID_VARIANT_SHREDDING, "Invalid type_params.shredding of column %s: %s".formatted(column.name(), message));
    }

    private static final class Parser
    {
        private final HoglakeColumnHandle column;
        private int fields;

        private Parser(HoglakeColumnHandle column)
        {
            this.column = requireNonNull(column, "column is null");
        }

        /**
         * @param keys the keys that the JSON object can have besides those of its type
         */
        private ShreddedValue value(JsonNode node, String path, int depth, Set<String> keys)
        {
            if (!node.isObject()) {
                throw invalid(path, "is not a JSON object");
            }
            JsonNode typeNode = node.get("type");
            if (typeNode == null || !typeNode.isTextual()) {
                throw invalid(path, "has no type");
            }
            String type = typeNode.textValue();
            return switch (type) {
                case "variant" -> {
                    checkKeys(node, path, keys);
                    yield new ShreddedValue(Optional.empty());
                }
                case "object" -> {
                    checkKeys(node, path, keys, "fields");
                    checkDepth(path, depth);
                    JsonNode fieldsNode = node.get("fields");
                    if (fieldsNode == null || !fieldsNode.isArray() || fieldsNode.isEmpty()) {
                        throw invalid(path, "has no fields");
                    }
                    ImmutableList.Builder<ObjectField> objectFields = ImmutableList.builder();
                    for (JsonNode field : fieldsNode) {
                        fields++;
                        if (fields > MAX_FIELDS) {
                            throw invalid("$", "has more than %s fields".formatted(MAX_FIELDS));
                        }
                        JsonNode name = field.get("name");
                        if (!field.isObject() || name == null || !name.isTextual()) {
                            throw invalid(path, "has a field without a name");
                        }
                        String fieldPath = path + "." + name.textValue();
                        objectFields.add(new ObjectField(name.textValue(), value(field, fieldPath, depth + 1, ImmutableSet.of("name"))));
                    }
                    yield new ShreddedValue(Optional.of(new ObjectValue(objectFields.build())));
                }
                case "array" -> {
                    checkKeys(node, path, keys, "element");
                    checkDepth(path, depth);
                    JsonNode element = node.get("element");
                    if (element == null) {
                        throw invalid(path, "has no element");
                    }
                    yield new ShreddedValue(Optional.of(new ArrayValue(value(element, path + "[*]", depth + 1, ImmutableSet.of()))));
                }
                default -> new ShreddedValue(Optional.of(primitive(node, path, type, keys)));
            };
        }

        private PrimitiveValue primitive(JsonNode node, String path, String type, Set<String> keys)
        {
            ShreddedType primitive = PRIMITIVE_TYPES.get(type);
            if (primitive != null) {
                checkKeys(node, path, keys);
                return PrimitiveValue.of(primitive);
            }
            ShreddedType decimal = DECIMAL_TYPES.get(type);
            if (decimal == null) {
                throw invalid(path, "has an unknown type: " + type);
            }
            checkKeys(node, path, keys, "precision", "scale");
            int precision = intValue(node, path, "precision");
            int scale = intValue(node, path, "scale");
            if (precision < 1 || precision > MAX_PRECISION || scale < 0 || scale > precision) {
                throw invalid(path, "has precision %s and scale %s, which %s does not hold".formatted(precision, scale, type));
            }
            try {
                return PrimitiveValue.decimal(decimal, precision, scale);
            }
            catch (IllegalArgumentException e) {
                // The precision is larger than the width of the decimal type holds
                throw invalid(path, "has precision %s and scale %s, which %s does not hold".formatted(precision, scale, type));
            }
        }

        private int intValue(JsonNode node, String path, String key)
        {
            JsonNode value = node.get(key);
            if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
                throw invalid(path, "has no integer " + key);
            }
            return value.intValue();
        }

        private void checkKeys(JsonNode node, String path, Set<String> keys, String... typeKeys)
        {
            Set<String> allowed = ImmutableSet.<String>builder().add("type").addAll(keys).add(typeKeys).build();
            for (Iterator<String> names = node.fieldNames(); names.hasNext(); ) {
                String name = names.next();
                if (!allowed.contains(name)) {
                    throw invalid(path, "has an unknown key: " + name);
                }
            }
        }

        private void checkDepth(String path, int depth)
        {
            if (depth >= MAX_DEPTH) {
                throw invalid(path, "is nested more than %s levels deep".formatted(MAX_DEPTH));
            }
        }

        private TrinoException invalid(String path, String message)
        {
            return HoglakeVariantShredding.invalid(column, path + " " + message);
        }
    }
}
