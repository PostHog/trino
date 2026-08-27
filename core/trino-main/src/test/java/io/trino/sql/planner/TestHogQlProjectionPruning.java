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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyProjectionDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.sql.SqlFormatter;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlProjectionPruning
        extends BasePlanTest
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier(TEST_CATALOG_NAME, false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = snapshot();

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

    private static <T extends PlanNode> List<T> nodes(Plan plan, Class<T> type)
    {
        return PlanNodeSearcher.searchFrom(plan.getRoot())
                .whereIsInstanceOfAny(type)
                .findAll().stream()
                .map(type::cast)
                .toList();
    }

    private static HogQlSemanticCatalogSnapshot snapshot()
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
                        field("name", "name", "varchar", LogicalType.STRING)),
                List.of(),
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
                List.of(),
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
