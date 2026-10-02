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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/// Pins the form in which a connector receives `CAST(v['k'] AS varchar)` over a
/// VARIANT column, and shows that a connector can replace `v` with a column
/// that holds only the paths the projection reads, while the engine still
/// evaluates the subscript and the cast.
public class TestVariantSubscriptProjectionPushdown
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final SchemaTableName TEST_TABLE = new SchemaTableName(TEST_SCHEMA, "test_table");
    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final ColumnHandle VARIANT_COLUMN = new MockConnectorColumnHandle("v", VARIANT);
    private static final ColumnHandle PRUNED_VARIANT_COLUMN = new MockConnectorColumnHandle("v_pruned", VARIANT);

    // Connector expressions name builtin functions in lowercase, including mangled operator names
    private static final FunctionName SUBSCRIPT_FUNCTION_NAME = new FunctionName("$operator$subscript");
    private static final ResolvedFunction VARIANT_SUBSCRIPT = new TestingFunctionResolution().resolveOperator(SUBSCRIPT, ImmutableList.of(VARIANT, VARCHAR));

    @Test
    public void testSubscriptCastReachesConnector()
    {
        List<List<ConnectorExpression>> projections = new CopyOnWriteArrayList<>();
        try (PlanTester planTester = createPlanTester(projections)) {
            // The engine translates the returned projection back to an expression, which
            // resolves the lowercase operator name
            assertPlan(
                    planTester,
                    "SELECT CAST(v['k'] AS varchar) FROM test_table",
                    output(project(
                            ImmutableMap.of("value", expression(castSubscript(new Reference(VARIANT, "pruned")))),
                            tableScan(
                                    new MockConnectorTableHandle(TEST_TABLE, TupleDomain.all(), Optional.of(ImmutableList.of(PRUNED_VARIANT_COLUMN)))::equals,
                                    TupleDomain.all(),
                                    ImmutableMap.of("pruned", PRUNED_VARIANT_COLUMN::equals)))));
        }

        // The engine offers the projection on v once. Later optimizer passes offer the returned
        // projection on the pruned column again, and the connector must decline it.
        List<ConnectorExpression> projectionOnVariant = ImmutableList.of(castSubscriptProjection(new Variable("v", VARIANT)));
        List<ConnectorExpression> projectionOnPruned = ImmutableList.of(castSubscriptProjection(new Variable("pruned", VARIANT)));
        assertThat(projections).containsOnlyOnce(projectionOnVariant);
        assertThat(projections.subList(projections.indexOf(projectionOnVariant) + 1, projections.size()))
                .isNotEmpty()
                .containsOnly(projectionOnPruned);
    }

    @Test
    public void testFilterBlocksSubscriptCastPushdown()
    {
        // PushProjectionIntoTableScan only matches a projection directly on a table scan. When
        // the connector uses the predicate only to skip data and leaves the whole filter to the
        // engine, it is never offered the projection.
        List<List<ConnectorExpression>> projections = new CopyOnWriteArrayList<>();
        try (PlanTester planTester = createPlanTester(projections)) {
            assertPlan(
                    planTester,
                    "SELECT CAST(v['k'] AS varchar) FROM test_table WHERE id > 1",
                    output(project(
                            ImmutableMap.of("value", expression(castSubscript(new Reference(VARIANT, "v")))),
                            filter(
                                    comparison(GREATER_THAN, new Reference(BIGINT, "id"), new io.trino.sql.ir.Constant(BIGINT, 1L)),
                                    tableScan(TEST_TABLE.getTableName(), ImmutableMap.of("id", "id", "v", "v"))))));
        }

        assertThat(projections).isEmpty();
    }

    private static PlanTester createPlanTester(List<List<ConnectorExpression>> projections)
    {
        PlanTester planTester = PlanTester.create(MOCK_SESSION);
        planTester.createCatalog(
                MOCK_CATALOG,
                MockConnectorFactory.builder()
                        .withGetTableHandle((_, name) -> new MockConnectorTableHandle(name))
                        .withGetColumns(_ -> ImmutableList.of(
                                new ColumnMetadata("id", BIGINT),
                                new ColumnMetadata("v", VARIANT)))
                        .withApplyFilter(TestVariantSubscriptProjectionPushdown::applyFilter)
                        .withApplyProjection((_, handle, projectionList, assignments) -> {
                            projections.add(ImmutableList.copyOf(projectionList));
                            return applyProjection(handle, projectionList, assignments);
                        })
                        .build(),
                ImmutableMap.of());
        return planTester;
    }

    /// Uses the predicate to prune data, but leaves the whole filter to the engine.
    private static Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
        TupleDomain<ColumnHandle> predicate = table.getConstraint().intersect(constraint.getSummary());
        if (predicate.equals(table.getConstraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                new MockConnectorTableHandle(table.getTableName(), predicate, table.getColumns()),
                constraint.getSummary(),
                constraint.getExpression(),
                false));
    }

    /// Replaces `v` in `CAST(v[<key>] AS <type>)` with a column pruned to the paths that the
    /// projection reads, and returns the same expression on that column. Declines any other
    /// projections.
    private static Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (projections.size() != 1 ||
                !(projections.getFirst() instanceof Call cast) ||
                !cast.getFunctionName().equals(CAST_FUNCTION_NAME) ||
                !(cast.getArguments().getFirst() instanceof Call subscript) ||
                !subscript.getFunctionName().equals(SUBSCRIPT_FUNCTION_NAME) ||
                !(subscript.getArguments().getFirst() instanceof Variable base) ||
                !VARIANT_COLUMN.equals(assignments.get(base.getName()))) {
            return Optional.empty();
        }

        MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
        Variable pruned = new Variable("pruned", VARIANT);
        ConnectorExpression projection = new Call(
                cast.getType(),
                cast.getFunctionName(),
                ImmutableList.of(new Call(
                        subscript.getType(),
                        subscript.getFunctionName(),
                        ImmutableList.of(pruned, subscript.getArguments().get(1)))));
        return Optional.of(new ProjectionApplicationResult<>(
                new MockConnectorTableHandle(table.getTableName(), table.getConstraint(), Optional.of(ImmutableList.of(PRUNED_VARIANT_COLUMN))),
                ImmutableList.of(projection),
                ImmutableList.of(new Assignment(pruned.getName(), PRUNED_VARIANT_COLUMN, VARIANT)),
                false));
    }

    private static ConnectorExpression castSubscriptProjection(Variable variant)
    {
        return new Call(
                VARCHAR,
                CAST_FUNCTION_NAME,
                ImmutableList.of(new Call(
                        VARIANT,
                        SUBSCRIPT_FUNCTION_NAME,
                        ImmutableList.of(variant, new Constant(utf8Slice("k"), VARCHAR)))));
    }

    private static Expression castSubscript(Reference variant)
    {
        return new Cast(
                new io.trino.sql.ir.Call(VARIANT_SUBSCRIPT, ImmutableList.of(variant, new io.trino.sql.ir.Constant(VARCHAR, utf8Slice("k")))),
                VARCHAR);
    }

    private static void assertPlan(PlanTester planTester, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        planTester.inTransaction(transactionSession -> {
            Plan actualPlan = planTester.createPlan(transactionSession, sql);
            PlanAssert.assertPlan(transactionSession, planTester.getPlannerContext().getMetadata(), planTester.getPlannerContext().getFunctionManager(), planTester.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }
}
