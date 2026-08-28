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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ArgumentReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionArgument;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyProjectionDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyLookupRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipJoinSide;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ScopedFieldReferenceRecipe;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlRelationshipExpansion
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = snapshot();

    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testExpandsRelationshipAndLazyProjectionThroughOneJoin()
    {
        HogQlCompilationResult result = compile("SELECT person.name, e.personProfile.name, e.personProfile.plan FROM events e");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT \"__hogql_lazy_1\".full_name AS name, " +
                        "\"__hogql_lazy_1\".full_name AS name, " +
                        "CAST(\"__hogql_lazy_1\".properties_map[CAST(CAST('plan' AS varchar) AS varchar)] AS varchar) AS plan " +
                        "FROM analytics.data.raw_events e " +
                        "LEFT JOIN analytics.data.raw_persons \"__hogql_lazy_1\" " +
                        "ON e.person_id = \"__hogql_lazy_1\".person_id " +
                        "AND e.workspace_id = \"__hogql_lazy_1\".workspace_id"));
    }

    @Test
    public void testExpandsOnlyTheDeclaredPathThroughRelationshipCycle()
    {
        HogQlCompilationResult result = compile("SELECT personEvent.event FROM events");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT \"__hogql_lazy_2\".event_name AS event " +
                        "FROM analytics.data.raw_events " +
                        "LEFT JOIN analytics.data.raw_persons \"__hogql_lazy_1\" " +
                        "ON raw_events.person_id = \"__hogql_lazy_1\".person_id " +
                        "AND raw_events.workspace_id = \"__hogql_lazy_1\".workspace_id " +
                        "LEFT JOIN analytics.data.raw_events \"__hogql_lazy_2\" " +
                        "ON \"__hogql_lazy_1\".event_id = \"__hogql_lazy_2\".event_id"));
    }

    @Test
    public void testExpandsJsonObjectPropertyLookup()
    {
        HogQlCompilationResult result = compile("SELECT jsonProperties.plan FROM persons");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT CAST(CAST(json_parse(properties_json) AS map(varchar, json))[CAST('plan' AS varchar)] AS varchar) AS plan " +
                        "FROM analytics.data.raw_persons"));
    }

    @Test
    public void testPrunesUnusedLazyStarProjectionBeforeExpansion()
    {
        HogQlCompilationResult result = compile("SELECT sub.event FROM (SELECT e.personProfile.*, e.* FROM events e) sub");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT sub.\"event\" AS event FROM (SELECT e.event_name AS \"event\" FROM analytics.data.raw_events e) sub"));
    }

    @Test
    public void testRetainsDemandedLazyStarProjectionAsStockJoin()
    {
        HogQlCompilationResult result = compile("SELECT sub.name FROM (SELECT e.personProfile.*, e.* FROM events e) sub");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT sub.\"name\" AS name FROM (" +
                        "SELECT \"__hogql_lazy_1\".full_name AS \"name\" " +
                        "FROM analytics.data.raw_events e " +
                        "LEFT JOIN analytics.data.raw_persons \"__hogql_lazy_1\" " +
                        "ON e.person_id = \"__hogql_lazy_1\".person_id " +
                        "AND e.workspace_id = \"__hogql_lazy_1\".workspace_id) sub"));
    }

    @ParameterizedTest
    @MethodSource("nonProjectionRelationshipQueries")
    public void testExpandsRelationshipDemandedOutsideProjection(String hogql, String trinoSql)
    {
        assertThat(compile(hogql).statement()).isEqualTo(sqlParser.createStatement(trinoSql));
    }

    @Test
    public void testFailsExplicitlyWhenRelationshipIsDemandedInsideJoinCriteria()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compile("SELECT e.event FROM events e JOIN persons p ON e.person.name = p.name"));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception).hasMessageContaining("HogQL relationship paths are not supported inside explicit join criteria");
    }

    private static Stream<Arguments> nonProjectionRelationshipQueries()
    {
        String join = "FROM analytics.data.raw_events " +
                "LEFT JOIN analytics.data.raw_persons \"__hogql_lazy_1\" " +
                "ON raw_events.person_id = \"__hogql_lazy_1\".person_id " +
                "AND raw_events.workspace_id = \"__hogql_lazy_1\".workspace_id ";
        return Stream.of(
                Arguments.of(
                        "SELECT event FROM events WHERE person.name IS NOT NULL",
                        "SELECT event_name AS event " + join + "WHERE \"__hogql_lazy_1\".full_name IS NOT NULL"),
                Arguments.of(
                        "SELECT event FROM events ORDER BY person.name",
                        "SELECT event_name AS event " + join + "ORDER BY \"__hogql_lazy_1\".full_name"));
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
                OptionalLong.of(7)), Optional.of(context));
    }

    private static HogQlSemanticCatalogSnapshot snapshot()
    {
        LogicalTableDefinition events = new LogicalTableDefinition(
                "events",
                physicalName("raw_events"),
                List.of(
                        field("event", "event_name"),
                        field("eventId", "event_id"),
                        field("personId", "person_id"),
                        field("workspaceId", "workspace_id")),
                List.of(),
                List.of(new RelationshipDefinition(
                        "person",
                        "persons",
                        RelationshipCardinality.MANY_TO_ONE,
                        List.of(new JoinKey("personId", "personId")),
                        Optional.of(new OperatorRecipe(
                                SemanticOperator.EQUAL,
                                List.of(
                                        new ScopedFieldReferenceRecipe(RelationshipJoinSide.SOURCE, "workspaceId"),
                                        new ScopedFieldReferenceRecipe(RelationshipJoinSide.TARGET, "workspaceId")))))));
        LogicalTableDefinition persons = new LogicalTableDefinition(
                "persons",
                physicalName("raw_persons"),
                List.of(
                        field("personId", "person_id"),
                        field("workspaceId", "workspace_id"),
                        field("eventId", "event_id"),
                        field("name", "full_name"),
                        new LogicalFieldDefinition("propertiesJson", new PhysicalIdentifier("properties_json", false), "varchar", LogicalType.STRING, true, false),
                        new LogicalFieldDefinition("propertiesMap", new PhysicalIdentifier("properties_map", false), "map(varchar, varchar)", LogicalType.MAP, true, false)),
                List.of(
                        new PropertyDefinition(
                                "properties",
                                "propertiesMap",
                                PropertyStorage.MAP,
                                LogicalType.STRING,
                                true,
                                Optional.of("varchar"),
                                Optional.of("varchar"),
                                Optional.of(new OperatorRecipe(
                                        SemanticOperator.SUBSCRIPT,
                                        List.of(
                                                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_SOURCE),
                                                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_KEY))))),
                        new PropertyDefinition(
                                "jsonProperties",
                                "propertiesJson",
                                PropertyStorage.JSON_OBJECT,
                                LogicalType.STRING,
                                true,
                                Optional.of("varchar"),
                                Optional.of("varchar"),
                                Optional.of(new OperatorRecipe(
                                        SemanticOperator.JSON_OBJECT_LOOKUP,
                                        List.of(
                                                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_SOURCE),
                                                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_KEY)))))),
                List.of(new RelationshipDefinition(
                        "event",
                        "events",
                        RelationshipCardinality.MANY_TO_ONE,
                        List.of(new JoinKey("eventId", "eventId")))));
        List<LazyTableDefinition> lazyTables = List.of(
                new LazyTableDefinition(
                        "events",
                        "personProfile",
                        List.of("person"),
                        List.of(
                                new LazyProjectionDefinition("name", "varchar", LogicalType.STRING, true, true, new FieldReferenceRecipe("persons", "name")),
                                new LazyProjectionDefinition(
                                        "plan",
                                        "varchar",
                                        LogicalType.STRING,
                                        true,
                                        true,
                                        new PropertyLookupRecipe(
                                                "persons",
                                                "properties",
                                                new LiteralRecipe(new TypedLiteral("varchar", LiteralEncoding.STRING, "plan")))))),
                new LazyTableDefinition(
                        "events",
                        "personEvent",
                        List.of("person", "event"),
                        List.of(new LazyProjectionDefinition("event", "varchar", LogicalType.STRING, true, true, new FieldReferenceRecipe("events", "event")))));
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                7,
                List.of(events, persons),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                lazyTables,
                List.of(),
                List.of());
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
