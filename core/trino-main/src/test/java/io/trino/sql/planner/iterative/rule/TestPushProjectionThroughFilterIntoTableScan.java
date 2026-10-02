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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPushProjectionThroughFilterIntoTableScan
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Session MOCK_SESSION = testSessionBuilder().setCatalog(TEST_CATALOG_NAME).setSchema(TEST_SCHEMA).build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction VARIANT_SUBSCRIPT = FUNCTIONS.resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(VARIANT, VARCHAR));
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    private static final ColumnHandle COLUMN_V = new TpchColumnHandle("v", VARIANT);
    private static final ColumnHandle COLUMN_A = new TpchColumnHandle("a", BIGINT);
    private static final ColumnHandle COLUMN_B = new TpchColumnHandle("b", BIGINT);
    private static final ColumnHandle REPLACED_COLUMN_V = new TpchColumnHandle("v_replaced", VARIANT);
    private static final ColumnHandle REPLACED_COLUMN_A = new TpchColumnHandle("a_replaced", BIGINT);
    private static final ColumnHandle REPLACED_COLUMN_B = new TpchColumnHandle("b_replaced", BIGINT);

    private static final Expression B_IS_POSITIVE = comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L));

    @Test
    public void testReplacesColumnInFilterAndProject()
    {
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_V, REPLACED_COLUMN_V))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, B_IS_POSITIVE))
                    .matches(project(
                            ImmutableMap.of("x", expression(subscript(new Reference(VARIANT, "v_replaced")))),
                            filter(
                                    B_IS_POSITIVE,
                                    tableScan(
                                            _ -> true,
                                            TupleDomain.all(),
                                            ImmutableMap.of("v_replaced", REPLACED_COLUMN_V::equals, "b", COLUMN_B::equals),
                                            // The statistics of the kept column remain
                                            statistics -> statistics.orElseThrow().getOutputRowCount() == 42 &&
                                                    statistics.orElseThrow().getSymbolsWithKnownStatistics().stream().map(Symbol::name).toList().equals(List.of("b"))))));
        }
    }

    @Test
    public void testReplacesColumnUnderItsName()
    {
        ColumnHandle newColumn = new MockConnectorColumnHandle("v", VARIANT);
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_V, newColumn))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, B_IS_POSITIVE))
                    .matches(project(
                            ImmutableMap.of("x", expression(subscript(new Reference(VARIANT, "new_v")))),
                            filter(
                                    B_IS_POSITIVE,
                                    tableScan(_ -> true, TupleDomain.all(), ImmutableMap.of("new_v", newColumn::equals, "b", COLUMN_B::equals)))));
        }
    }

    @Test
    public void testDoesNotFireWithoutReplacedColumn()
    {
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of())) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, B_IS_POSITIVE))
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireWithoutVariantColumn()
    {
        // The connector would replace column a, but the table scan reads no VARIANT column
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_A, REPLACED_COLUMN_A))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> {
                        Symbol a = p.symbol("a", BIGINT);
                        Symbol b = p.symbol("b", BIGINT);
                        return p.project(
                                Assignments.of(p.symbol("x", BIGINT), new io.trino.sql.ir.Call(ADD_BIGINT, ImmutableList.of(a.toSymbolReference(), new Constant(BIGINT, 1L)))),
                                p.filter(
                                        B_IS_POSITIVE,
                                        p.tableScan(
                                                ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE),
                                                ImmutableList.of(a, b),
                                                ImmutableMap.of(a, COLUMN_A, b, COLUMN_B))));
                    })
                    .doesNotFire();
        }
    }

    @Test
    public void testDoesNotFireWhenDynamicFilterReadsReplacedColumn()
    {
        // A dynamic filter is not offered to the connector, so it would still read the replaced column
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_B, REPLACED_COLUMN_B))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, new Logical(Logical.Operator.AND, ImmutableList.of(
                            B_IS_POSITIVE,
                            DynamicFilters.createDynamicFilterExpression(
                                    ruleTester.getMetadata(),
                                    getCharVarcharCoercion(MOCK_SESSION),
                                    new DynamicFilterId("df"),
                                    BIGINT,
                                    new Reference(BIGINT, "b"))))))
                    .doesNotFire();
        }
    }

    @Test
    public void testKeepsNondeterministicFilter()
    {
        // The engine does not offer random(), and the filter keeps it unchanged. Nothing reads b.
        Expression predicate = comparison(GREATER_THAN, new io.trino.sql.ir.Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.5));
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_V, REPLACED_COLUMN_V))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, predicate))
                    .matches(project(
                            ImmutableMap.of("x", expression(subscript(new Reference(VARIANT, "v_replaced")))),
                            filter(
                                    predicate,
                                    tableScan(_ -> true, TupleDomain.all(), ImmutableMap.of("v_replaced", REPLACED_COLUMN_V::equals)))));
        }
    }

    @Test
    public void testDoesNotOfferEngineLocalVariables()
    {
        // The engine binds a local variable in a filter that it cannot translate as a whole, such as
        // the operand of BETWEEN SYMMETRIC. The connector fails on a variable that is not a column.
        Symbol local = new Symbol(BIGINT, "local");
        Expression predicate = new Let(
                local,
                new io.trino.sql.ir.Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "b"), new Constant(BIGINT, 1L))),
                new Logical(Logical.Operator.OR, ImmutableList.of(
                        comparison(GREATER_THAN, local.toSymbolReference(), new Reference(BIGINT, "b")),
                        comparison(LESS_THAN, local.toSymbolReference(), new Constant(BIGINT, 0L)))));
        try (RuleTester ruleTester = createRuleTester(ImmutableMap.of(COLUMN_V, REPLACED_COLUMN_V))) {
            ruleTester.assertThat(new PushProjectionThroughFilterIntoTableScan(ruleTester.getPlannerContext()))
                    .withSession(MOCK_SESSION)
                    .on(p -> plan(ruleTester, p, predicate))
                    .matches(project(
                            ImmutableMap.of("x", expression(subscript(new Reference(VARIANT, "v_replaced")))),
                            node(
                                    FilterNode.class,
                                    tableScan(_ -> true, TupleDomain.all(), ImmutableMap.of("v_replaced", REPLACED_COLUMN_V::equals, "b", COLUMN_B::equals)))));
        }
    }

    /// Project `x := v['k']` on a filter with the predicate, on a scan of `v` and `b`.
    private static PlanNode plan(RuleTester ruleTester, PlanBuilder p, Expression predicate)
    {
        Symbol v = p.symbol("v", VARIANT);
        Symbol b = p.symbol("b", BIGINT);
        return p.project(
                Assignments.of(p.symbol("x", VARIANT), subscript(v.toSymbolReference())),
                p.filter(
                        predicate,
                        p.tableScan(tableScan -> tableScan
                                .setTableHandle(ruleTester.getCurrentCatalogTableHandle(TEST_SCHEMA, TEST_TABLE))
                                .setSymbols(ImmutableList.of(v, b))
                                .setAssignments(ImmutableMap.of(v, COLUMN_V, b, COLUMN_B))
                                .setStatistics(Optional.of(PlanNodeStatsEstimate.builder()
                                        .setOutputRowCount(42)
                                        .addSymbolStatistics(b, SymbolStatsEstimate.builder().setNullsFraction(0).setDistinctValuesCount(7).build())
                                        .build())))));
    }

    private static Expression subscript(Expression variant)
    {
        return new io.trino.sql.ir.Call(VARIANT_SUBSCRIPT, ImmutableList.of(variant, new Constant(VARCHAR, utf8Slice("k"))));
    }

    private static RuleTester createRuleTester(Map<ColumnHandle, ColumnHandle> replacedColumns)
    {
        MockConnectorFactory factory = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of(TEST_SCHEMA))
                .withListTables((_, schema) -> TEST_SCHEMA.equals(schema) ? ImmutableList.of(TEST_TABLE) : ImmutableList.of())
                .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("v", VARIANT), new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT)))
                .withApplyProjection((_, handle, projections, assignments) -> applyProjection(handle, projections, assignments, replacedColumns))
                .build();
        return RuleTester.builder().withDefaultCatalogConnectorFactory(factory).build();
    }

    /// Returns the same expressions, with the columns in `replacedColumns` replaced. Like
    /// some connectors, fails on a variable that is not a column.
    private static Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments,
            Map<ColumnHandle, ColumnHandle> replacedColumns)
    {
        projections.stream()
                .flatMap(projection -> variables(projection).stream())
                .forEach(variable -> checkArgument(assignments.containsKey(variable.getName()), "Variable is not a column: %s", variable));

        Map<String, Assignment> newAssignments = new LinkedHashMap<>();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(projection -> replaceColumns(projection, assignments, replacedColumns, newAssignments))
                .collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(handle, newProjections, ImmutableList.copyOf(newAssignments.values()), false));
    }

    private static ConnectorExpression replaceColumns(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Map<ColumnHandle, ColumnHandle> replacedColumns,
            Map<String, Assignment> newAssignments)
    {
        return switch (expression) {
            case Variable variable -> {
                ColumnHandle column = assignments.get(variable.getName());
                if (!replacedColumns.containsKey(column)) {
                    newAssignments.putIfAbsent(variable.getName(), new Assignment(variable.getName(), column, variable.getType()));
                    yield variable;
                }
                ColumnHandle replacement = replacedColumns.get(column);
                String name = switch (replacement) {
                    case TpchColumnHandle tpchColumn -> tpchColumn.columnName();
                    case MockConnectorColumnHandle mockColumn -> mockColumn.name();
                    default -> throw new IllegalArgumentException("Unexpected column: " + replacement);
                };
                newAssignments.putIfAbsent(name, new Assignment(name, replacement, variable.getType()));
                yield new Variable(name, variable.getType());
            }
            case Call call -> new Call(call.getType(), call.getFunctionName(), call.getArguments().stream()
                    .map(argument -> replaceColumns(argument, assignments, replacedColumns, newAssignments))
                    .collect(toImmutableList()));
            default -> expression;
        };
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
}
