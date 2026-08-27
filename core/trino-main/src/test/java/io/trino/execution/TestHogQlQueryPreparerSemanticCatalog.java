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
package io.trino.execution;

import io.trino.Session;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlErrorCode;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.BooleanValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ModifierBehavior;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticModifierDefault;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.TrinoException;
import io.trino.sql.SessionPropertyResolver;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Table;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QuerySubmission.hogQl;
import static io.trino.hogql.HogQlCompilationObserver.NOOP;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlQueryPreparerSemanticCatalog
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = new HogQlSemanticCatalogSnapshot(
            HogQlSemanticCatalogSnapshot.SCHEMA_VERSION,
            HogQlLanguageContract.current().languageVersion(),
            CATALOG,
            7,
            List.of(new LogicalTableDefinition(
                    "events",
                    new PhysicalQualifiedName(CATALOG, new PhysicalIdentifier("default", false), new PhysicalIdentifier("raw_events", false)),
                    List.of(new LogicalFieldDefinition("event", new PhysicalIdentifier("event_name", false), "varchar", LogicalType.STRING, false, true)),
                    List.of(),
                    List.of())));
    private static final HogQlSemanticCatalogSnapshot MODIFIER_SNAPSHOT = new HogQlSemanticCatalogSnapshot(
            HogQlSemanticCatalogSnapshot.PROTOCOL_VERSION,
            HogQlSemanticCatalogSnapshot.SCHEMA_VERSION,
            HogQlLanguageContract.current().languageVersion(),
            CATALOG,
            7,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(new SemanticModifierDefault(
                            "sampling",
                            ModifierBehavior.TRINO_SESSION_PROPERTY,
                            new TypedLiteral("boolean", LiteralEncoding.BOOLEAN, "false"),
                            List.of(new PhysicalIdentifier("hogql_sampling", false))),
                    new SemanticModifierDefault(
                            "legacyMode",
                            ModifierBehavior.SAFE_NOOP,
                            new TypedLiteral("boolean", LiteralEncoding.BOOLEAN, "false"),
                            List.of())),
            List.of(),
            List.of(),
            List.of());

    @Test
    public void testSessionCatalogPinsSemanticSnapshotAndResolvesLogicalTable()
    {
        AtomicReference<PinRequest> pinRequest = new AtomicReference<>();
        QueryPreparer queryPreparer = new QueryPreparer(
                new SqlParser(),
                new HogQlCompiler(),
                NOOP,
                Optional.of(request -> {
                    pinRequest.set(request);
                    return new PinnedSnapshot(SNAPSHOT);
                }));

        PreparedQuery preparedQuery = queryPreparer.prepareQuery(
                testSessionBuilder().setCatalog("analytics").build(),
                hogQl(envelope("SELECT event FROM events", OptionalLong.of(7))));

        assertThat(pinRequest.get()).isEqualTo(new PinRequest(CATALOG, HogQlLanguageContract.current().languageVersion(), OptionalLong.of(7)));
        QuerySpecification query = (QuerySpecification) ((Query) preparedQuery.getStatement()).getQueryBody();
        assertThat(((Table) query.getFrom().orElseThrow()).getName().getOriginalParts())
                .extracting(Identifier::getValue)
                .containsExactly("analytics", "default", "raw_events");
    }

    @Test
    public void testMissingSessionCatalogOrProviderDoesNotFetchMetadata()
    {
        AtomicInteger pins = new AtomicInteger();
        QueryPreparer withProvider = new QueryPreparer(
                new SqlParser(),
                new HogQlCompiler(),
                NOOP,
                Optional.of(_ -> {
                    pins.incrementAndGet();
                    return new PinnedSnapshot(SNAPSHOT);
                }));
        QueryPreparer withoutProvider = new QueryPreparer(new SqlParser(), new HogQlCompiler(), NOOP, Optional.empty());

        withProvider.prepareQuery(testSessionBuilder().build(), hogQl(envelope("SELECT 1", OptionalLong.empty())));
        withoutProvider.prepareQuery(testSessionBuilder().setCatalog("analytics").build(), hogQl(envelope("SELECT 1", OptionalLong.empty())));

        assertThat(pins).hasValue(0);
    }

    @Test
    public void testModifierOverrideIsAppliedOutsideStatementAst()
    {
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            SessionPropertyManager sessionPropertyManager = queryRunner.getSessionPropertyManager();
            sessionPropertyManager.addSystemSessionProperty(booleanProperty("hogql_sampling", "HogQL sampling", false, false));
            Session session = testSessionBuilder(sessionPropertyManager).setCatalog("analytics").build();
            QueryPreparer queryPreparer = new QueryPreparer(
                    new SqlParser(),
                    new HogQlCompiler(),
                    NOOP,
                    Optional.of(_ -> new PinnedSnapshot(MODIFIER_SNAPSHOT)));

            PreparedQuery preparedQuery = queryPreparer.prepareQuery(
                    session,
                    hogQl(envelope(
                            "SELECT 1",
                            OptionalLong.empty(),
                            Map.of("sampling", new HogQlTypedValue("boolean", new BooleanValue(true))))));

            assertThat(((Query) preparedQuery.getStatement()).getSessionProperties()).isEmpty();
            assertThat(preparedQuery.getSessionPropertyOverrides()).singleElement()
                    .extracting(property -> property.getName().toString())
                    .isEqualTo("hogql_sampling");

            SessionPropertyResolver resolver = new SessionPropertyResolver(
                    new SessionPropertyEvaluator(
                            queryRunner.getPlannerContext(),
                            queryRunner.getAccessControl(),
                            sessionPropertyManager,
                            new SqlEnvironmentConfig()),
                    queryRunner.getAccessControl());
            Session overridden = resolver.getSessionPropertiesApplier(preparedQuery).apply(session);

            assertThat(overridden.getSystemProperty("hogql_sampling", Boolean.class)).isTrue();
        }
    }

    @Test
    public void testInvalidModifierValueFailsAsRedactedBindingError()
    {
        QueryPreparer queryPreparer = new QueryPreparer(
                new SqlParser(),
                new HogQlCompiler(),
                NOOP,
                Optional.of(_ -> new PinnedSnapshot(MODIFIER_SNAPSHOT)));

        assertThatThrownBy(() -> queryPreparer.prepareQuery(
                testSessionBuilder().setCatalog("analytics").build(),
                hogQl(envelope(
                        "SELECT 1",
                        OptionalLong.empty(),
                        Map.of("sampling", new HogQlTypedValue("boolean", new StringValue("secret-value")))))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_BINDING_ERROR.toErrorCode());
                    assertThat(exception.getLocation()).hasValueSatisfying(location -> {
                        assertThat(location.lineNumber()).isEqualTo(1);
                        assertThat(location.columnNumber()).isEqualTo(1);
                    });
                    assertThat(exception).hasMessageContaining("Invalid HogQL modifier binding: sampling");
                    assertThat(exception).hasMessageNotContaining("secret-value");
                });
    }

    @Test
    public void testInvalidSafeNoopValueFailsAsRedactedBindingError()
    {
        QueryPreparer queryPreparer = new QueryPreparer(
                new SqlParser(),
                new HogQlCompiler(),
                NOOP,
                Optional.of(_ -> new PinnedSnapshot(MODIFIER_SNAPSHOT)));

        assertThatThrownBy(() -> queryPreparer.prepareQuery(
                testSessionBuilder().setCatalog("analytics").build(),
                hogQl(envelope(
                        "SELECT 1",
                        OptionalLong.empty(),
                        Map.of("legacyMode", new HogQlTypedValue("boolean", new StringValue("secret-value")))))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_BINDING_ERROR.toErrorCode());
                    assertThat(exception.getLocation()).hasValueSatisfying(location -> {
                        assertThat(location.lineNumber()).isEqualTo(1);
                        assertThat(location.columnNumber()).isEqualTo(1);
                    });
                    assertThat(exception).hasMessageContaining("Invalid HogQL modifier binding: legacyMode");
                    assertThat(exception).hasMessageNotContaining("secret-value");
                });
    }

    private static HogQlCompileEnvelope envelope(String query, OptionalLong generation)
    {
        return envelope(query, generation, Map.of());
    }

    private static HogQlCompileEnvelope envelope(String query, OptionalLong generation, Map<String, HogQlTypedValue> modifiers)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                modifiers,
                generation);
    }
}
