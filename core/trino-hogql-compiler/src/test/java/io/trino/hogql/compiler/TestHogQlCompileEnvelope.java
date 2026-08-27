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

import io.trino.hogql.compiler.HogQlTypedValue.ArrayValue;
import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.ObjectValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.HogQlTypedValue.Value;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestHogQlCompileEnvelope
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageContract.current().languageVersion();

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 2, Integer.MAX_VALUE})
    public void testRejectsUnknownProtocolVersions(int protocolVersion)
    {
        assertThatThrownBy(() -> envelope(protocolVersion, LANGUAGE_VERSION, Map.of(), Map.of(), Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported HogQL protocol version");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.0.0", "1.0.1", "2.0.0"})
    public void testRejectsUnknownLanguageVersions(String languageVersion)
    {
        assertThatThrownBy(() -> envelope(HogQlCompileEnvelope.PROTOCOL_VERSION, HogQlLanguageVersion.valueOf(languageVersion), Map.of(), Map.of(), Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported HogQL language version");
    }

    @ParameterizedTest
    @MethodSource("ambiguousTypedValues")
    public void testRejectsAmbiguousTypedValues(Supplier<Object> invalidValue, String sensitiveValue)
    {
        assertThatThrownBy(invalidValue::get)
                .isInstanceOfAny(IllegalArgumentException.class, NullPointerException.class)
                .message()
                .doesNotContain(sensitiveValue);
    }

    @ParameterizedTest
    @ValueSource(strings = {"actions", "cohorts", "savedQueries"})
    public void testRejectsUnknownSemanticFields(String semanticField)
    {
        Map<String, Map<String, HogQlTypedValue>> fields = Map.of(
                semanticField, Map.of("input", new HogQlTypedValue("varchar", new StringValue("sensitive-value"))));

        assertThatThrownBy(() -> HogQlCompileEnvelope.fromSemanticFields(
                "SELECT 1",
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                LANGUAGE_VERSION,
                fields,
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown HogQL semantic field");
    }

    @ParameterizedTest
    @MethodSource("sensitiveValues")
    public void testTypedValuesAreRedacted(Value value, String sensitiveValue)
    {
        HogQlTypedValue typedValue = new HogQlTypedValue("varchar", value);

        assertThat(value.toString()).doesNotContain(sensitiveValue);
        assertThat(typedValue.toString()).doesNotContain(sensitiveValue);
    }

    @ParameterizedTest
    @ValueSource(strings = {"parameters", "variables", "filters", "modifiers"})
    public void testEnvelopeRenderingRedactsEveryValueCollection(String semanticField)
    {
        String sensitiveValue = "sensitive-value-" + semanticField;
        Map<String, Map<String, HogQlTypedValue>> fields = Map.of(
                semanticField, Map.of("input", new HogQlTypedValue("varchar", new StringValue(sensitiveValue))));
        HogQlCompileEnvelope envelope = HogQlCompileEnvelope.fromSemanticFields(
                "SELECT 'sensitive-query'",
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                LANGUAGE_VERSION,
                fields,
                OptionalLong.of(42));

        assertThat(envelope.toString())
                .doesNotContain(sensitiveValue, "sensitive-query")
                .contains("query=<redacted>");
    }

    private static Stream<Arguments> ambiguousTypedValues()
    {
        String sensitiveValue = "sensitive-value";
        List<Value> arrayWithMissingValue = new ArrayList<>();
        arrayWithMissingValue.add(new StringValue(sensitiveValue));
        arrayWithMissingValue.add(null);
        Map<String, Value> objectWithMissingValue = new HashMap<>();
        objectWithMissingValue.put("input", new StringValue(sensitiveValue));
        objectWithMissingValue.put("missing", null);
        return Stream.of(
                arguments((Supplier<Object>) () -> new HogQlTypedValue(null, new StringValue(sensitiveValue)), sensitiveValue),
                arguments((Supplier<Object>) () -> new HogQlTypedValue("", new StringValue(sensitiveValue)), sensitiveValue),
                arguments((Supplier<Object>) () -> new HogQlTypedValue("varchar", null), sensitiveValue),
                arguments((Supplier<Object>) () -> new ArrayValue(arrayWithMissingValue), sensitiveValue),
                arguments((Supplier<Object>) () -> new ObjectValue(objectWithMissingValue), sensitiveValue));
    }

    private static Stream<Arguments> sensitiveValues()
    {
        return Stream.of(
                arguments(new StringValue("sensitive-string"), "sensitive-string"),
                arguments(new NumberValue("98765432101234567890"), "98765432101234567890"),
                arguments(new ArrayValue(List.of(new StringValue("sensitive-array"))), "sensitive-array"),
                arguments(new ObjectValue(Map.of("sensitive-key", new StringValue("sensitive-object"))), "sensitive-object"));
    }

    private static HogQlCompileEnvelope envelope(
            int protocolVersion,
            HogQlLanguageVersion languageVersion,
            Map<String, HogQlTypedValue> parameters,
            Map<String, HogQlTypedValue> variables,
            Map<String, HogQlTypedValue> filters,
            Map<String, HogQlTypedValue> modifiers)
    {
        return new HogQlCompileEnvelope(
                "SELECT 1",
                protocolVersion,
                languageVersion,
                parameters,
                variables,
                filters,
                modifiers,
                OptionalLong.empty());
    }
}
