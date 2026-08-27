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

import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record HogQlCompileEnvelope(
        String query,
        int protocolVersion,
        HogQlLanguageVersion languageVersion,
        Map<String, HogQlTypedValue> parameters,
        Map<String, HogQlTypedValue> variables,
        Map<String, HogQlTypedValue> filters,
        Map<String, HogQlTypedValue> modifiers,
        OptionalLong catalogGeneration)
{
    public static final int PROTOCOL_VERSION = 1;

    private static final Set<String> SEMANTIC_FIELDS = Set.of("parameters", "variables", "filters", "modifiers");

    public HogQlCompileEnvelope
    {
        query = requireNonNull(query, "query is null");
        if (query.isBlank()) {
            throw new IllegalArgumentException("query is empty");
        }
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported HogQL protocol version");
        }
        languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        if (!languageVersion.equals(HogQlLanguageContract.current().languageVersion())) {
            throw new IllegalArgumentException("unsupported HogQL language version");
        }
        parameters = immutableValues("parameters", parameters);
        variables = immutableValues("variables", variables);
        filters = immutableValues("filters", filters);
        modifiers = immutableValues("modifiers", modifiers);
        catalogGeneration = requireNonNull(catalogGeneration, "catalogGeneration is null");
        catalogGeneration.ifPresent(generation -> {
            if (generation <= 0) {
                throw new IllegalArgumentException("catalog generation must be positive");
            }
        });
    }

    public static HogQlCompileEnvelope fromSemanticFields(
            String query,
            int protocolVersion,
            HogQlLanguageVersion languageVersion,
            Map<String, Map<String, HogQlTypedValue>> semanticFields,
            OptionalLong catalogGeneration)
    {
        requireNonNull(semanticFields, "semanticFields is null");
        if (!SEMANTIC_FIELDS.containsAll(semanticFields.keySet())) {
            throw new IllegalArgumentException("unknown HogQL semantic field");
        }
        return new HogQlCompileEnvelope(
                query,
                protocolVersion,
                languageVersion,
                semanticFields.getOrDefault("parameters", Map.of()),
                semanticFields.getOrDefault("variables", Map.of()),
                semanticFields.getOrDefault("filters", Map.of()),
                semanticFields.getOrDefault("modifiers", Map.of()),
                catalogGeneration);
    }

    private static Map<String, HogQlTypedValue> immutableValues(String field, Map<String, HogQlTypedValue> values)
    {
        requireNonNull(values, field + " is null");
        for (Map.Entry<String, HogQlTypedValue> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new IllegalArgumentException(field + " contains an empty name");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(field + " contains a missing typed value");
            }
        }
        return Map.copyOf(values);
    }

    @Override
    public String toString()
    {
        return "HogQlCompileEnvelope[protocolVersion=%s, languageVersion=%s, query=<redacted>, parameters=%s, variables=%s, filters=%s, modifiers=%s, catalogGenerationPresent=%s]"
                .formatted(
                        protocolVersion,
                        languageVersion,
                        parameters.size(),
                        variables.size(),
                        filters.size(),
                        modifiers.size(),
                        catalogGeneration.isPresent());
    }
}
