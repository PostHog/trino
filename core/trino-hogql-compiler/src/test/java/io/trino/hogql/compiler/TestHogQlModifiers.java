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

import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ModifierBehavior;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticModifierDefault;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Query;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_BINDING_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlModifiers
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final SemanticModifierDefault SESSION_MODIFIER = modifier(
            "sampling",
            ModifierBehavior.TRINO_SESSION_PROPERTY,
            List.of(new PhysicalIdentifier("hogql", false), new PhysicalIdentifier("sampling", false)));
    private static final SemanticModifierDefault COMPILER_MODIFIER = modifier("compilerMode", ModifierBehavior.COMPILER, List.of());
    private static final SemanticModifierDefault NOOP_MODIFIER = modifier("legacyMode", ModifierBehavior.SAFE_NOOP, List.of());
    private static final SemanticModifierDefault UNSUPPORTED_MODIFIER = modifier("futureMode", ModifierBehavior.UNSUPPORTED, List.of());
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = snapshot(List.of(
            SESSION_MODIFIER,
            COMPILER_MODIFIER,
            NOOP_MODIFIER,
            UNSUPPORTED_MODIFIER));

    @Test
    public void testExplicitModifierPinsLiteralQueryAndOverridesDefault()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlCompilationResult result = new HogQlCompiler().compile(
                envelope("SELECT 1", Map.of(
                        "SAMPLING", new HogQlTypedValue("BOOLEAN", new BooleanValue(true)),
                        "legacyMode", new HogQlTypedValue("boolean", new BooleanValue(true)))),
                Optional.of(context(pins)));

        assertThat(pins).hasValue(1);
        assertThat(result.catalogGeneration()).hasValue(7);
        assertThat(result.modifierBindings()).hasSize(2);
        assertThat(result.modifierBindings()).filteredOn(binding -> binding.modifierName().equals("sampling")).singleElement().satisfies(binding -> {
            assertThat(binding.sessionProperty().orElseThrow()).extracting(PhysicalIdentifier::value).containsExactly("hogql", "sampling");
            assertThat(binding.value()).isEqualTo(new HogQlTypedValue("BOOLEAN", new BooleanValue(true)));
        });
        assertThat(result.modifierBindings()).filteredOn(binding -> binding.modifierName().equals("legacyMode")).singleElement().satisfies(binding -> {
            assertThat(binding.sessionProperty()).isEmpty();
            assertThat(binding.value()).isEqualTo(new HogQlTypedValue("boolean", new BooleanValue(true)));
        });
        assertThat(((Query) result.statement()).getSessionProperties()).isEmpty();
    }

    @Test
    public void testPinnedQueryAppliesSessionDefaultAndLeavesOtherDefaultsInert()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(
                envelope("SELECT 1", Map.of("legacyMode", new HogQlTypedValue("boolean", new BooleanValue(true)))),
                Optional.of(context(new AtomicInteger())));

        assertThat(result.modifierBindings()).hasSize(2);
        assertThat(result.modifierBindings()).filteredOn(binding -> binding.modifierName().equals("sampling")).singleElement()
                .extracting(HogQlModifierBinding::value)
                .isEqualTo(new HogQlTypedValue("boolean", new BooleanValue(false)));
    }

    @Test
    public void testUnmodifiedLiteralQueryDoesNotPinSnapshot()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlCompilationResult result = new HogQlCompiler().compile(
                envelope("SELECT 1", Map.of()),
                Optional.of(context(pins)));

        assertThat(pins).hasValue(0);
        assertThat(result.catalogGeneration()).isEmpty();
        assertThat(result.modifierBindings()).isEmpty();
    }

    @Test
    public void testExplicitModifierRequiresSnapshotContext()
    {
        assertThatThrownBy(() -> new HogQlCompiler().compile(envelope(
                "SELECT 1",
                Map.of("sampling", new HogQlTypedValue("boolean", new BooleanValue(true))))))
                .isInstanceOf(HogQlSemanticCatalogException.class)
                .hasMessageContaining("required for modifiers");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidModifiers")
    public void testRejectsInvalidExplicitModifiers(String name, Map<String, HogQlTypedValue> modifiers, ErrorCode errorCode, String message)
    {
        assertThatThrownBy(() -> new HogQlCompiler().compile(
                envelope("SELECT 1", modifiers),
                Optional.of(context(new AtomicInteger()))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(errorCode);
                    assertThat(exception).hasMessageContaining(message);
                });
    }

    private static Stream<Arguments> invalidModifiers()
    {
        HogQlTypedValue booleanValue = new HogQlTypedValue("boolean", new BooleanValue(true));
        return Stream.of(
                Arguments.of("unknown", Map.of("missing", booleanValue), HOGQL_BINDING_ERROR.toErrorCode(), "Unknown HogQL modifier"),
                Arguments.of("wrong type", Map.of("sampling", new HogQlTypedValue("varchar", new StringValue("true"))), HOGQL_BINDING_ERROR.toErrorCode(), "incompatible type"),
                Arguments.of("compiler", Map.of("compilerMode", booleanValue), HOGQL_UNSUPPORTED_FEATURE.toErrorCode(), "not implemented"),
                Arguments.of("unsupported", Map.of("futureMode", booleanValue), HOGQL_UNSUPPORTED_FEATURE.toErrorCode(), "not supported"),
                Arguments.of("duplicate canonical name", Map.of("sampling", booleanValue, "SAMPLING", booleanValue), HOGQL_BINDING_ERROR.toErrorCode(), "Duplicate HogQL modifier"));
    }

    private static HogQlSemanticCatalogContext context(AtomicInteger pins)
    {
        return new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(SNAPSHOT);
        });
    }

    private static HogQlCompileEnvelope envelope(String query, Map<String, HogQlTypedValue> modifiers)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                modifiers,
                OptionalLong.empty());
    }

    private static SemanticModifierDefault modifier(String name, ModifierBehavior behavior, List<PhysicalIdentifier> sessionProperty)
    {
        return new SemanticModifierDefault(
                name,
                behavior,
                new TypedLiteral("boolean", LiteralEncoding.BOOLEAN, "false"),
                sessionProperty);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(List<SemanticModifierDefault> modifiers)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                7,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                modifiers,
                List.of(),
                List.of(),
                List.of());
    }
}
