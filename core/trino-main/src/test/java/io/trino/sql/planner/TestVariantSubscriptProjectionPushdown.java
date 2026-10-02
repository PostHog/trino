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
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
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
/// evaluates the subscript and the cast, also above a filter.
public class TestVariantSubscriptProjectionPushdown
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final SchemaTableName TEST_TABLE = new SchemaTableName(TEST_SCHEMA, "test_table");
    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(MOCK_CATALOG).setSchema(TEST_SCHEMA).build();

    private static final ColumnHandle ID_COLUMN = new MockConnectorColumnHandle("id", BIGINT);
    private static final ColumnHandle VARIANT_COLUMN = new MockConnectorColumnHandle("v", VARIANT);
    private static final ColumnHandle PRUNED_VARIANT_COLUMN = new MockConnectorColumnHandle("v_pruned", VARIANT);
    private static final ColumnHandle COMPUTED_COLUMN = new MockConnectorColumnHandle("k_value", VARCHAR);

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
    public void testSubscriptCastPushdownPastFilter()
    {
        // The connector uses the predicate only to skip data and leaves the whole filter to the
        // engine. The pruned column replaces v in both the project and the filter, which stay
        // where they are.
        List<List<ConnectorExpression>> projections = new CopyOnWriteArrayList<>();
        try (PlanTester planTester = createPlanTester(projections, false)) {
            assertPlan(
                    planTester,
                    "SELECT CAST(v['k'] AS varchar) FROM test_table WHERE id > 1",
                    output(project(
                            ImmutableMap.of("value", expression(castSubscript(new Reference(VARIANT, "pruned")))),
                            filter(
                                    comparison(GREATER_THAN, new Reference(BIGINT, "id"), new io.trino.sql.ir.Constant(BIGINT, 1L)),
                                    tableScan(isTestTable(), TupleDomain.all(), ImmutableMap.of("id", ID_COLUMN::equals, "pruned", PRUNED_VARIANT_COLUMN::equals))))));

            assertPlan(
                    planTester,
                    "SELECT id FROM test_table WHERE CAST(v['k'] AS varchar) = 'x'",
                    output(project(filter(
                            comparison(EQUAL, castSubscript(new Reference(VARIANT, "pruned")), new io.trino.sql.ir.Constant(VARCHAR, utf8Slice("x"))),
                            tableScan(isTestTable(), TupleDomain.all(), ImmutableMap.of("id", ID_COLUMN::equals, "pruned", PRUNED_VARIANT_COLUMN::equals))))));
        }
        // The filter's expressions are offered with the project's
        assertThat(projections).contains(ImmutableList.of(
                new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, ImmutableList.of(new Constant(1L, BIGINT), new Variable("id", BIGINT))),
                castSubscriptProjection(new Variable("v", VARIANT))));
    }

    @Test
    public void testConnectorCannotComputeProjectionBelowFilter()
    {
        // A connector that computes the subscript in the table scan would evaluate it on rows
        // that the filter removes, so the projection is not pushed past the filter
        try (PlanTester planTester = createPlanTester(new CopyOnWriteArrayList<>(), true)) {
            assertPlan(
                    planTester,
                    "SELECT CAST(v['k'] AS varchar) FROM test_table WHERE id > 1",
                    output(project(
                            ImmutableMap.of("value", expression(castSubscript(new Reference(VARIANT, "v")))),
                            filter(
                                    comparison(GREATER_THAN, new Reference(BIGINT, "id"), new io.trino.sql.ir.Constant(BIGINT, 1L)),
                                    tableScan(TEST_TABLE.getTableName(), ImmutableMap.of("id", "id", "v", "v"))))));

            // Without a filter, the connector computes it
            assertPlan(
                    planTester,
                    "SELECT CAST(v['k'] AS varchar) FROM test_table",
                    output(tableScan(isTestTable(), TupleDomain.all(), ImmutableMap.of("value", COMPUTED_COLUMN::equals))));
        }
    }

    private static PlanTester createPlanTester(List<List<ConnectorExpression>> projections)
    {
        return createPlanTester(projections, false);
    }

    private static PlanTester createPlanTester(List<List<ConnectorExpression>> projections, boolean computeInTableScan)
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
                            if (computeInTableScan) {
                                return computeProjection(handle, projectionList, assignments);
                            }
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

    /// Replaces `v` in `v[<key>]` with a column pruned to the paths that the projections
    /// read, and returns the same expressions on that column. Declines projections that
    /// read `v` in another way, or do not read it.
    private static Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        Variable pruned = new Variable("pruned", VARIANT);
        List<ConnectorExpression> newProjections = projections.stream()
                .map(projection -> replaceSubscriptBase(projection, assignments, pruned))
                .collect(toImmutableList());
        if (newProjections.equals(projections) || newProjections.stream().anyMatch(projection -> readsColumn(projection, VARIANT_COLUMN, assignments))) {
            return Optional.empty();
        }

        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.<Assignment>builder()
                .add(new Assignment(pruned.getName(), PRUNED_VARIANT_COLUMN, VARIANT));
        newProjections.stream()
                .flatMap(projection -> variables(projection).stream())
                .filter(variable -> !variable.equals(pruned))
                .distinct()
                .forEach(variable -> newAssignments.add(new Assignment(variable.getName(), assignments.get(variable.getName()), variable.getType())));
        List<Assignment> resultAssignments = newAssignments.build();
        MockConnectorTableHandle table = (MockConnectorTableHandle) handle;
        return Optional.of(new ProjectionApplicationResult<>(
                new MockConnectorTableHandle(table.getTableName(), table.getConstraint(), Optional.of(resultAssignments.stream().map(Assignment::getColumn).collect(toImmutableList()))),
                newProjections,
                resultAssignments,
                false));
    }

    /// Computes `CAST(v['k'] AS varchar)` in the table scan, as a column of its own, and
    /// replaces each other column with a copy, as a connector that also prunes columns.
    private static Optional<ProjectionApplicationResult<ConnectorTableHandle>> computeProjection(
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        ConnectorExpression computed = castSubscriptProjection(new Variable("v", VARIANT));
        if (!projections.contains(computed) || !VARIANT_COLUMN.equals(assignments.get("v"))) {
            return Optional.empty();
        }
        Variable value = new Variable("k_value", VARCHAR);
        List<ConnectorExpression> newProjections = projections.stream()
                .map(projection -> projection.equals(computed) ? value : copyColumns(projection))
                .collect(toImmutableList());
        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.<Assignment>builder()
                .add(new Assignment(value.getName(), COMPUTED_COLUMN, VARCHAR));
        newProjections.stream()
                .flatMap(projection -> variables(projection).stream())
                .filter(variable -> !variable.equals(value))
                .distinct()
                .forEach(variable -> newAssignments.add(new Assignment(variable.getName(), new MockConnectorColumnHandle(variable.getName(), variable.getType()), variable.getType())));
        return Optional.of(new ProjectionApplicationResult<>(handle, newProjections, newAssignments.build(), false));
    }

    private static ConnectorExpression copyColumns(ConnectorExpression expression)
    {
        return switch (expression) {
            case Variable variable -> new Variable(variable.getName() + "_copy", variable.getType());
            case Call call -> new Call(call.getType(), call.getFunctionName(), call.getArguments().stream()
                    .map(TestVariantSubscriptProjectionPushdown::copyColumns)
                    .collect(toImmutableList()));
            default -> expression;
        };
    }

    private static ConnectorExpression replaceSubscriptBase(ConnectorExpression expression, Map<String, ColumnHandle> assignments, Variable pruned)
    {
        if (expression instanceof Call call &&
                call.getFunctionName().equals(SUBSCRIPT_FUNCTION_NAME) &&
                call.getArguments().getFirst() instanceof Variable base &&
                VARIANT_COLUMN.equals(assignments.get(base.getName()))) {
            return new Call(call.getType(), call.getFunctionName(), ImmutableList.of(pruned, call.getArguments().get(1)));
        }
        if (expression instanceof Call call) {
            return new Call(call.getType(), call.getFunctionName(), call.getArguments().stream()
                    .map(argument -> replaceSubscriptBase(argument, assignments, pruned))
                    .collect(toImmutableList()));
        }
        return expression;
    }

    private static boolean readsColumn(ConnectorExpression expression, ColumnHandle column, Map<String, ColumnHandle> assignments)
    {
        return variables(expression).stream().anyMatch(variable -> column.equals(assignments.get(variable.getName())));
    }

    private static List<Variable> variables(ConnectorExpression expression)
    {
        if (expression instanceof Variable variable) {
            return ImmutableList.of(variable);
        }
        return expression.getChildren().stream()
                .flatMap(child -> variables(child).stream())
                .collect(toImmutableList());
    }

    private static Predicate<ConnectorTableHandle> isTestTable()
    {
        return handle -> ((MockConnectorTableHandle) handle).getTableName().equals(TEST_TABLE);
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
