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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.hogql.compiler.HogQlCompilationResult;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlSemanticCatalogContext;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ActionReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ArgumentReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionArgument;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyProjectionDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PredicateRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.sql.SqlFormatter;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlProjectionPruning
        extends BasePlanTest
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier(TEST_CATALOG_NAME, false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = testingSnapshot();

    private final HogQlCompiler compiler = new HogQlCompiler();

    public TestHogQlProjectionPruning()
    {
        super(ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "false",
                JOIN_REORDERING_STRATEGY, "NONE"));
    }

    @Test
    public void testUnusedLazyProjectionDoesNotReachStockPlan()
    {
        Plan plan = plan(compile("SELECT sub.event FROM (SELECT e.personProfile.*, e.* FROM events e) sub"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(1);
        assertThat(nodes(plan, JoinNode.class)).isEmpty();
    }

    @Test
    public void testUsedLazyProjectionRemainsVisibleToStockPredicatePushdown()
    {
        Plan plan = plan(compile(
                "SELECT sub.name FROM (SELECT e.personProfile.*, e.* FROM events e) sub " +
                        "WHERE sub.name = 'Customer#000000001'"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(2);
        List<JoinNode> joins = nodes(plan, JoinNode.class);
        assertThat(joins).hasSize(1);
        JoinNode join = joins.getFirst();
        assertThat(join.getType()).isEqualTo(INNER);
        assertThat(PlanNodeSearcher.searchFrom(join.getRight())
                .whereIsInstanceOfAny(FilterNode.class)
                .findFirst()).isPresent();
    }

    @Test
    public void testUnusedLazyProjectionIsPrunedThroughChainedCtes()
    {
        Plan plan = plan(compile(
                "WITH first_source(aliasedName, aliasedEvent, aliasedPersonId) AS (SELECT e.personProfile.*, e.* FROM events e), " +
                        "second_source AS (SELECT * FROM first_source) " +
                        "SELECT aliasedEvent FROM second_source"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(1);
        assertThat(nodes(plan, JoinNode.class)).isEmpty();
    }

    @Test
    public void testDemandedLazyProjectionRemainsInPlanThroughChainedCtes()
    {
        Plan plan = plan(compile(
                "WITH first_source(aliasedName, aliasedEvent, aliasedPersonId) AS (SELECT e.personProfile.*, e.* FROM events e), " +
                        "second_source AS (SELECT * FROM first_source) " +
                        "SELECT aliasedName FROM second_source WHERE aliasedName = 'Customer#000000001'"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(2);
        assertThat(nodes(plan, JoinNode.class)).hasSize(1);
    }

    @Test
    public void testUnusedLazyProjectionIsPrunedThroughDerivedCteChain()
    {
        Plan plan = plan(compile(
                "WITH first_source AS (SELECT e.personProfile.*, e.* FROM events e), " +
                        "second_source AS (SELECT derived.* FROM (SELECT * FROM first_source) " +
                        "derived(aliasedName, aliasedEvent, aliasedPersonId)) " +
                        "SELECT aliasedEvent FROM second_source"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(1);
        assertThat(nodes(plan, JoinNode.class)).isEmpty();
    }

    @Test
    public void testDemandedLazyProjectionRemainsThroughDerivedCteChain()
    {
        Plan plan = plan(compile(
                "WITH first_source AS (SELECT e.personProfile.*, e.* FROM events e), " +
                        "second_source AS (SELECT derived.* FROM (SELECT * FROM first_source) " +
                        "derived(aliasedName, aliasedEvent, aliasedPersonId)) " +
                        "SELECT aliasedName FROM second_source WHERE aliasedName = 'Customer#000000001'"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(2);
        assertThat(nodes(plan, JoinNode.class)).hasSize(1);
    }

    @Test
    public void testVarcharJsonPropertyPredicateReachesStockPlanner()
    {
        Plan plan = plan(compile("SELECT properties.plan FROM persons WHERE properties.plan = 'pro'"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(1);
        assertThat(nodes(plan, FilterNode.class)).hasSize(1);
    }

    @Test
    public void testV0ActionPredicateReachesStockPlanner()
    {
        Plan plan = plan(compileV0("SELECT event FROM events WHERE matchesAction(42)"));

        assertThat(nodes(plan, TableScanNode.class)).hasSize(1);
        assertThat(nodes(plan, FilterNode.class)).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNION ALL", "INTERSECT ALL", "EXCEPT ALL"})
    public void testSetOperationDemandIsMappedByBranchPosition(String operator)
    {
        Plan plan = plan(compile(
                "WITH source AS (" +
                        "SELECT CAST(e.event AS String) AS selected, e.personProfile.name AS discarded FROM events e " +
                        operator + " " +
                        "SELECT e.personProfile.name AS rightSelected, CAST(e.event AS String) AS rightDiscarded FROM events e) " +
                        "SELECT selected FROM source"), CREATED);

        List<SetOperationNode> setOperations = nodes(plan, SetOperationNode.class);
        assertThat(setOperations).hasSize(1);
        SetOperationNode setOperation = setOperations.getFirst();
        assertThat(nodes(setOperation.getSources().getFirst(), TableScanNode.class)).hasSize(1);
        assertThat(nodes(setOperation.getSources().getFirst(), JoinNode.class)).isEmpty();
        assertThat(nodes(setOperation.getSources().getLast(), TableScanNode.class)).hasSize(2);
        assertThat(nodes(setOperation.getSources().getLast(), JoinNode.class)).hasSize(1);
        assertThat(nodes(plan, TableScanNode.class)).hasSize(3);
        assertThat(nodes(plan, JoinNode.class)).hasSize(1);
    }

    private String compile(String query)
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));
        HogQlCompilationResult result = compiler.compile(new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.of(1)), Optional.of(context));
        return SqlFormatter.formatSql(result.statement());
    }

    private String compileV0(String query)
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));
        HogQlCompilationResult result = compiler.compileV0(new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.of(1)), Optional.of(context));
        return SqlFormatter.formatSql(result.statement());
    }

    private static <T extends PlanNode> List<T> nodes(Plan plan, Class<T> type)
    {
        return nodes(plan.getRoot(), type);
    }

    private static <T extends PlanNode> List<T> nodes(PlanNode root, Class<T> type)
    {
        return PlanNodeSearcher.searchFrom(root)
                .whereIsInstanceOfAny(type)
                .findAll().stream()
                .map(type::cast)
                .toList();
    }

    static HogQlSemanticCatalogSnapshot testingSnapshot()
    {
        LogicalTableDefinition events = new LogicalTableDefinition(
                "events",
                physicalName("orders"),
                List.of(
                        field("event", "orderkey", "bigint", LogicalType.INTEGER),
                        field("personId", "custkey", "bigint", LogicalType.INTEGER)),
                List.of(),
                List.of(new RelationshipDefinition(
                        "person",
                        "persons",
                        RelationshipCardinality.MANY_TO_ONE,
                        List.of(new JoinKey("personId", "personId")))));
        LogicalTableDefinition persons = new LogicalTableDefinition(
                "persons",
                physicalName("customer"),
                List.of(
                        field("personId", "custkey", "bigint", LogicalType.INTEGER),
                        field("name", "name", "varchar", LogicalType.STRING),
                        field("properties", "comment", "varchar", LogicalType.STRING)),
                List.of(new PropertyDefinition(
                        "properties",
                        "properties",
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
                List.of());
        LazyTableDefinition personProfile = new LazyTableDefinition(
                "events",
                "personProfile",
                List.of("person"),
                List.of(new LazyProjectionDefinition(
                        "name",
                        "varchar",
                        LogicalType.STRING,
                        false,
                        true,
                        new FieldReferenceRecipe("persons", "name"))));
        return new HogQlSemanticCatalogSnapshot(
                HogQlSemanticCatalogSnapshot.PROTOCOL_VERSION,
                HogQlSemanticCatalogSnapshot.SCHEMA_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                1,
                List.of(events, persons),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(personProfile),
                List.of(new ActionReference(
                        "Synthetic order",
                        "42",
                        "events",
                        new PredicateRepresentation(new OperatorRecipe(
                                SemanticOperator.EQUAL,
                                List.of(
                                        new FieldReferenceRecipe("events", "event"),
                                        new LiteralRecipe(new TypedLiteral("bigint", LiteralEncoding.INTEGER, "1"))))))),
                List.of());
    }

    private static LogicalFieldDefinition field(String name, String physicalName, String type, LogicalType logicalType)
    {
        return new LogicalFieldDefinition(name, new PhysicalIdentifier(physicalName, false), type, logicalType, false, true);
    }

    private static PhysicalQualifiedName physicalName(String table)
    {
        return new PhysicalQualifiedName(CATALOG, new PhysicalIdentifier("tiny", false), new PhysicalIdentifier(table, false));
    }
}
