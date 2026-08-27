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
package io.trino.hogql.compiler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.hogql.compiler.HogQlTypedValue.ArrayValue;
import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.NullValue;
import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.ObjectValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.HogQlTypedValue.Value;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticModifierDefault;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_BINDING_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_COMPILER_INTERNAL_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlModifierResolver
{
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private HogQlModifierResolver() {}

    public static List<HogQlModifierBinding> resolve(
            PinnedSnapshot pinnedSnapshot,
            Map<String, HogQlTypedValue> suppliedModifiers,
            SourceSpan querySpan)
    {
        requireNonNull(pinnedSnapshot, "pinnedSnapshot is null");
        requireNonNull(suppliedModifiers, "suppliedModifiers is null");
        requireNonNull(querySpan, "querySpan is null");

        Map<String, SemanticModifierDefault> definitions = new HashMap<>();
        for (SemanticModifierDefault definition : pinnedSnapshot.snapshot().modifierDefaults()) {
            definitions.put(canonical(definition.name()), definition);
        }

        Map<String, HogQlTypedValue> suppliedByCanonicalName = new LinkedHashMap<>();
        suppliedModifiers.entrySet().stream()
                .sorted(Comparator.comparing(entry -> canonical(entry.getKey())))
                .forEach(entry -> {
                    String name = canonical(entry.getKey());
                    if (suppliedByCanonicalName.put(name, entry.getValue()) != null) {
                        throw bindingError(querySpan, "Duplicate HogQL modifier: " + entry.getKey());
                    }
                    SemanticModifierDefault definition = definitions.get(name);
                    if (definition == null) {
                        throw bindingError(querySpan, "Unknown HogQL modifier: " + entry.getKey());
                    }
                    if (!definition.defaultValue().typeSignature().equalsIgnoreCase(entry.getValue().type())) {
                        throw bindingError(querySpan, "HogQL modifier has an incompatible type: " + definition.name());
                    }
                });

        List<HogQlModifierBinding> bindings = new ArrayList<>();
        for (SemanticModifierDefault definition : pinnedSnapshot.snapshot().modifierDefaults()) {
            Optional<HogQlTypedValue> supplied = Optional.ofNullable(suppliedByCanonicalName.get(canonical(definition.name())));
            switch (definition.behavior()) {
                case TRINO_SESSION_PROPERTY -> bindings.add(new HogQlModifierBinding(
                        definition.name(),
                        Optional.of(definition.sessionProperty()),
                        supplied.orElseGet(() -> typedValue(definition.defaultValue()))));
                case COMPILER -> {
                    if (supplied.isPresent()) {
                        throw unsupportedError(querySpan, "HogQL compiler modifier is not implemented: " + definition.name());
                    }
                }
                case SAFE_NOOP -> supplied.ifPresent(value -> bindings.add(new HogQlModifierBinding(
                        definition.name(),
                        Optional.empty(),
                        value)));
                case UNSUPPORTED -> {
                    if (supplied.isPresent()) {
                        throw unsupportedError(querySpan, "HogQL modifier is not supported: " + definition.name());
                    }
                }
            }
        }
        return List.copyOf(bindings);
    }

    private static HogQlTypedValue typedValue(TypedLiteral literal)
    {
        Value value = switch (literal.encoding()) {
            case NULL -> NullValue.NULL;
            case STRING -> new StringValue(literal.value());
            case BOOLEAN -> new BooleanValue(Boolean.parseBoolean(literal.value()));
            case INTEGER, DECIMAL, FLOAT -> new NumberValue(literal.value());
            case JSON -> jsonValue(literal.value());
            case BASE64 -> new StringValue(HexFormat.of().formatHex(Base64.getDecoder().decode(literal.value())));
        };
        return new HogQlTypedValue(literal.typeSignature(), value);
    }

    private static Value jsonValue(String json)
    {
        try {
            return jsonValue(JSON_MAPPER.readTree(json));
        }
        catch (JsonProcessingException _) {
            throw new TrinoException(HOGQL_COMPILER_INTERNAL_ERROR, "Invalid HogQL modifier default");
        }
    }

    private static Value jsonValue(JsonNode node)
    {
        if (node.isNull()) {
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
            node.forEach(element -> values.add(jsonValue(element)));
            return new ArrayValue(values);
        }
        if (node.isObject()) {
            Map<String, Value> values = new LinkedHashMap<>();
            node.properties().forEach(entry -> values.put(entry.getKey(), jsonValue(entry.getValue())));
            return new ObjectValue(values);
        }
        throw new TrinoException(HOGQL_COMPILER_INTERNAL_ERROR, "Invalid HogQL modifier default");
    }

    private static String canonical(String name)
    {
        return requireNonNull(name, "name is null").toLowerCase(Locale.ENGLISH);
    }

    private static TrinoException bindingError(SourceSpan span, String message)
    {
        return new TrinoException(HOGQL_BINDING_ERROR, Optional.of(new Location(span.startLine(), span.startColumn())), message, null);
    }

    private static TrinoException unsupportedError(SourceSpan span, String message)
    {
        return new TrinoException(HOGQL_UNSUPPORTED_FEATURE, Optional.of(new Location(span.startLine(), span.startColumn())), message, null);
    }
}
