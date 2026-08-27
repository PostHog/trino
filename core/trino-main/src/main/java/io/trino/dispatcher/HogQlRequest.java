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
package io.trino.dispatcher;

import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.json.JsonCodec;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.ArrayValue;
import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.NullValue;
import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.ObjectValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.HogQlTypedValue.Value;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

record HogQlRequest(
        String query,
        int protocolVersion,
        HogQlLanguageVersion languageVersion,
        Map<String, HogQlTypedValue> parameters,
        Map<String, HogQlTypedValue> variables,
        Map<String, HogQlTypedValue> filters,
        Map<String, HogQlTypedValue> modifiers,
        OptionalLong catalogGeneration)
{
    private static final JsonCodec<JsonNode> JSON_CODEC = jsonCodec(JsonNode.class);
    private static final Set<String> FIELDS = Set.of(
            "query",
            "protocolVersion",
            "languageVersion",
            "parameters",
            "variables",
            "filters",
            "modifiers",
            "catalogGeneration");
    private static final Set<String> TYPED_VALUE_FIELDS = Set.of("type", "value");
    private static final int MAX_VALUE_DEPTH = 64;

    HogQlRequest
    {
        query = requireNonNull(query, "query is null");
        languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        parameters = Map.copyOf(requireNonNull(parameters, "parameters is null"));
        variables = Map.copyOf(requireNonNull(variables, "variables is null"));
        filters = Map.copyOf(requireNonNull(filters, "filters is null"));
        modifiers = Map.copyOf(requireNonNull(modifiers, "modifiers is null"));
        catalogGeneration = requireNonNull(catalogGeneration, "catalogGeneration is null");
    }

    static HogQlRequest fromJson(String requestBody)
    {
        JsonNode root = JSON_CODEC.fromJson(requireNonNull(requestBody, "requestBody is null"));
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("HogQL request must be an object");
        }
        rejectUnknownFields(root, FIELDS, "unknown HogQL request field");

        return new HogQlRequest(
                requiredText(root, "query"),
                requiredInt(root, "protocolVersion"),
                languageVersion(root),
                typedValues(root, "parameters"),
                typedValues(root, "variables"),
                typedValues(root, "filters"),
                typedValues(root, "modifiers"),
                catalogGeneration(root));
    }

    HogQlCompileEnvelope toCompileEnvelope()
    {
        return new HogQlCompileEnvelope(
                query,
                protocolVersion,
                languageVersion,
                parameters,
                variables,
                filters,
                modifiers,
                catalogGeneration);
    }

    private static HogQlLanguageVersion languageVersion(JsonNode root)
    {
        try {
            return HogQlLanguageVersion.valueOf(requiredText(root, "languageVersion"));
        }
        catch (RuntimeException _) {
            throw new IllegalArgumentException("invalid HogQL language version");
        }
    }

    private static OptionalLong catalogGeneration(JsonNode root)
    {
        JsonNode value = root.get("catalogGeneration");
        if (value == null) {
            return OptionalLong.empty();
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() <= 0) {
            throw new IllegalArgumentException("invalid HogQL catalog generation");
        }
        return OptionalLong.of(value.longValue());
    }

    private static Map<String, HogQlTypedValue> typedValues(JsonNode root, String field)
    {
        JsonNode values = root.get(field);
        if (values == null) {
            return Map.of();
        }
        if (!values.isObject()) {
            throw new IllegalArgumentException("invalid HogQL semantic field");
        }

        Map<String, HogQlTypedValue> result = new LinkedHashMap<>();
        Iterator<String> names = values.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            result.put(name, typedValue(values.get(name)));
        }
        return Map.copyOf(result);
    }

    private static HogQlTypedValue typedValue(JsonNode node)
    {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("invalid HogQL typed value");
        }
        rejectUnknownFields(node, TYPED_VALUE_FIELDS, "unknown HogQL typed value field");
        if (!node.has("value")) {
            throw new IllegalArgumentException("missing HogQL typed value");
        }
        return new HogQlTypedValue(requiredText(node, "type"), value(node.get("value"), 0));
    }

    private static Value value(JsonNode node, int depth)
    {
        if (depth > MAX_VALUE_DEPTH) {
            throw new IllegalArgumentException("HogQL typed value is too deeply nested");
        }
        if (node == null || node.isNull()) {
            return NullValue.NULL;
        }
        if (node.isBoolean()) {
            return new BooleanValue(node.booleanValue());
        }
        if (node.isNumber()) {
            return new NumberValue(node.asText());
        }
        if (node.isTextual()) {
            return new StringValue(node.textValue());
        }
        if (node.isArray()) {
            List<Value> values = new ArrayList<>(node.size());
            node.elements().forEachRemaining(element -> values.add(value(element, depth + 1)));
            return new ArrayValue(values);
        }
        if (node.isObject()) {
            Map<String, Value> values = new LinkedHashMap<>();
            Iterator<String> names = node.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                values.put(name, value(node.get(name), depth + 1));
            }
            return new ObjectValue(values);
        }
        throw new IllegalArgumentException("invalid HogQL typed value payload");
    }

    private static String requiredText(JsonNode root, String field)
    {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("missing HogQL text field");
        }
        return value.textValue();
    }

    private static int requiredInt(JsonNode root, String field)
    {
        JsonNode value = root.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new IllegalArgumentException("missing HogQL integer field");
        }
        return value.intValue();
    }

    private static void rejectUnknownFields(JsonNode object, Set<String> fields, String message)
    {
        Iterator<String> names = object.fieldNames();
        while (names.hasNext()) {
            if (!fields.contains(names.next())) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
