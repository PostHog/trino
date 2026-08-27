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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ActionReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CohortReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.MaterializedViewReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PredicateRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ReferencedField;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlSemanticEntityExpansion
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = snapshot();

    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @ParameterizedTest
    @ValueSource(strings = {"'Paid event'", "17"})
    public void testExpandsActionPredicateByNameOrId(String action)
    {
        assertThat(compile("SELECT event FROM events e WHERE matchesAction(" + action + ")").statement())
                .isEqualTo(sqlParser.createStatement(
                        "SELECT event_name AS event FROM analytics.data.raw_events e " +
                                "WHERE e.event_name = CAST('purchase' AS varchar)"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"IN COHORT 23", "NOT IN COHORT 'Active people'"})
    public void testExpandsCohortMembershipAsInSubquery(String predicate)
    {
        assertThat(compile("SELECT event FROM events WHERE personId " + predicate).statement())
                .isEqualTo(sqlParser.createStatement(
                        "SELECT event_name AS event FROM analytics.data.raw_events " +
                                "WHERE person_id " + (predicate.startsWith("NOT") ? "NOT " : "") +
                                "IN (SELECT DISTINCT \"personId\" FROM analytics.data.active_people)"));
    }

    @Test
    public void testExpandsActionRelationMembership()
    {
        assertThat(compile("SELECT event FROM events WHERE matchesAction(18)").statement())
                .isEqualTo(sqlParser.createStatement(
                        "SELECT event_name AS event FROM analytics.data.raw_events " +
                                "WHERE person_id IN (SELECT DISTINCT \"personId\" FROM analytics.data.active_people)"));
    }

    @Test
    public void testExpandsCohortPredicate()
    {
        assertThat(compile("SELECT event FROM events WHERE event NOT IN COHORT 24").statement())
                .isEqualTo(sqlParser.createStatement(
                        "SELECT event_name AS event FROM analytics.data.raw_events " +
                                "WHERE NOT (raw_events.event_name = CAST('purchase' AS varchar))"));
    }

    @Test
    public void testRejectsUnknownActionWithTypedResolutionError()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compile("SELECT event FROM events WHERE matchesAction('Missing action')"));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception).hasMessageContaining("Unknown HogQL action");
    }

    @ParameterizedTest
    @MethodSource("invalidCohortQueries")
    public void testRejectsInvalidCohortUseWithTypedCompatibilityError(String query, String message)
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compile(query));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception).hasMessageContaining(message);
    }

    private static Stream<Arguments> invalidCohortQueries()
    {
        return Stream.of(
                Arguments.of(
                        "SELECT event FROM events WHERE personId IN COHORT event",
                        "HogQL cohort reference must be a string or integer literal"),
                Arguments.of(
                        "SELECT event FROM events WHERE event IN COHORT 23",
                        "HogQL cohort membership source does not match the catalog"));
    }

    private HogQlCompilationResult compile(String query)
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));
        return compiler.compile(new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.of(11)), Optional.of(context));
    }

    private static HogQlSemanticCatalogSnapshot snapshot()
    {
        LogicalTableDefinition events = new LogicalTableDefinition(
                "events",
                physicalName("raw_events"),
                List.of(
                        field("event", "event_name"),
                        field("personId", "person_id")),
                List.of(),
                List.of());
        MaterializedViewReference activePeople = new MaterializedViewReference(
                "active_people",
                physicalName("active_people"),
                List.of(new ReferencedField("personId", "varchar", LogicalType.STRING, false, true)));
        PredicateRepresentation purchasePredicate = new PredicateRepresentation(new OperatorRecipe(
                SemanticOperator.EQUAL,
                List.of(
                        new FieldReferenceRecipe("events", "event"),
                        new LiteralRecipe(new TypedLiteral("varchar", LiteralEncoding.STRING, "purchase")))));
        RelationMembershipRepresentation activeMembership = new RelationMembershipRepresentation(new RelationMembershipRecipe(
                new RelationReference(RelationKind.MATERIALIZED_VIEW, "active_people"),
                "personId",
                "personId"));
        ActionReference paidEvent = new ActionReference(
                "Paid event",
                "17",
                "events",
                purchasePredicate);
        ActionReference precomputedAction = new ActionReference(
                "Precomputed action",
                "18",
                "events",
                activeMembership);
        CohortReference activePeopleCohort = new CohortReference(
                "Active people",
                "23",
                "events",
                activeMembership);
        CohortReference purchasers = new CohortReference(
                "Purchasers",
                "24",
                "events",
                purchasePredicate);
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                11,
                List.of(events),
                List.of(),
                List.of(),
                List.of(),
                List.of(activePeople),
                List.of(),
                List.of(),
                List.of(),
                List.of(paidEvent, precomputedAction),
                List.of(activePeopleCohort, purchasers));
    }

    private static LogicalFieldDefinition field(String name, String physicalName)
    {
        return new LogicalFieldDefinition(name, new PhysicalIdentifier(physicalName, false), "varchar", LogicalType.STRING, false, true);
    }

    private static PhysicalQualifiedName physicalName(String table)
    {
        return new PhysicalQualifiedName(CATALOG, new PhysicalIdentifier("data", false), new PhysicalIdentifier(table, false));
    }
}
