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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Lambda;
import io.trino.spi.expression.Variable;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.trino.sql.planner.ReferenceAwareExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Function.identity;

/// Offers the connector the projections of a project and of a filter on a table
/// scan, and lets it replace the columns that they read, such as a column that
/// holds only the parts of each value that the expressions read.
///
/// [PushProjectionIntoTableScan] matches only a projection directly on a table
/// scan, so a connector that leaves the filter to the engine is never offered the
/// projection. Unlike that rule, this rule does not move any expression below the
/// filter, because an expression can fail on a row that the filter removes. It
/// accepts the result only if each returned expression is its input with only
/// variables replaced, and then replaces those variables in the filter and the
/// project.
///
/// Today only a VARIANT column can be replaced this way, by a column that holds
/// the paths that subscripts read, so the rule offers the expressions only for a
/// table scan that reads a VARIANT column. Planning a projection can be expensive
/// for other connectors, which would only return results that the rule declines.
public class PushProjectionThroughFilterIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Capture<FilterNode> FILTER = newCapture();
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project().with(source().matching(
            filter().capturedAs(FILTER).with(source().matching(
                    tableScan().capturedAs(TABLE_SCAN)))));

    private final PlannerContext plannerContext;

    public PushProjectionThroughFilterIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        FilterNode filter = captures.get(FILTER);
        TableScanNode tableScan = captures.get(TABLE_SCAN);
        Session session = context.getSession();

        if (tableScan.getAssignments().keySet().stream().noneMatch(symbol -> symbol.type().equals(VARIANT))) {
            return Result.empty();
        }

        Map<String, Symbol> inputVariableMappings = tableScan.getAssignments().keySet().stream()
                .collect(toImmutableMap(Symbol::name, identity()));

        // The connector is not offered dynamic filters, which must keep their form, or
        // non-deterministic calls, which the engine does not translate, such as the random()
        // filter of TABLESAMPLE BERNOULLI. If an expression that is not offered reads a column
        // that the connector replaces, the check below declines the result. Dynamic filters
        // are planned after the optimizers that run this rule, so a filter has none today.
        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = Stream.concat(
                        extractConjuncts(filter.getPredicate()).stream().filter(conjunct -> !isDynamicFilter(conjunct)),
                        project.getAssignments().expressions().stream())
                .flatMap(expression -> extractPartialTranslations(expression, session).entrySet().stream())
                // Constant expressions should not be pushed to the connector
                .filter(entry -> !(entry.getValue() instanceof Constant))
                // A translation can read a variable that the engine binds itself, such as a
                // local variable of BETWEEN SYMMETRIC, which the connector does not know
                .filter(entry -> inputVariableMappings.keySet().containsAll(freeVariables(entry.getValue(), ImmutableSet.of())))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue, (first, _) -> first));
        if (partialTranslations.isEmpty()) {
            return Result.empty();
        }
        List<NodeRef<Expression>> nodes = ImmutableList.copyOf(partialTranslations.keySet());
        List<ConnectorExpression> connectorProjections = ImmutableList.copyOf(partialTranslations.values());

        Map<String, ColumnHandle> assignments = inputVariableMappings.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> tableScan.getAssignments().get(entry.getValue())));

        Optional<ProjectionApplicationResult<TableHandle>> result = plannerContext.getMetadata().applyProjection(session, tableScan.getTable(), connectorProjections, assignments);
        if (result.isEmpty()) {
            return Result.empty();
        }
        List<ConnectorExpression> newConnectorProjections = result.get().getProjections();
        checkState(newConnectorProjections.size() == connectorProjections.size(),
                "Mismatch between input and output projections from the connector: expected %s but got %s",
                connectorProjections.size(),
                newConnectorProjections.size());

        // The connector must not compute any expression in the table scan, which would
        // evaluate it before the filter
        Map<String, String> replacements = new HashMap<>();
        for (int i = 0; i < connectorProjections.size(); i++) {
            if (!replacesOnlyVariables(connectorProjections.get(i), newConnectorProjections.get(i), ImmutableSet.of(), replacements)) {
                return Result.empty();
            }
        }
        if (ImmutableSet.copyOf(replacements.values()).size() != replacements.size()) {
            return Result.empty();
        }

        // A column that the connector keeps under its name keeps its symbol
        List<Symbol> newScanOutputs = new ArrayList<>();
        Map<Symbol, ColumnHandle> newScanAssignments = new HashMap<>();
        Map<String, Symbol> variableMappings = new HashMap<>();
        Set<Symbol> replacedSymbols = new HashSet<>();
        for (Assignment assignment : result.get().getAssignments()) {
            Symbol symbol;
            if (assignment.getColumn().equals(assignments.get(assignment.getVariable()))) {
                symbol = inputVariableMappings.get(assignment.getVariable());
            }
            else {
                symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());
            }
            if (variableMappings.putIfAbsent(assignment.getVariable(), symbol) != null) {
                return Result.empty();
            }
            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
        }
        replacements.forEach((input, output) -> {
            if (!variableMappings.containsKey(output) || !variableMappings.get(output).equals(inputVariableMappings.get(input))) {
                replacedSymbols.add(inputVariableMappings.get(input));
            }
        });
        // Without a replaced column, the result would give the same plan again
        if (replacedSymbols.isEmpty() || !variableMappings.keySet().containsAll(replacements.values())) {
            return Result.empty();
        }

        // Only the expressions that read a replaced column change
        ImmutableMap.Builder<NodeRef<Expression>, Expression> newNodes = ImmutableMap.builder();
        for (int i = 0; i < nodes.size(); i++) {
            boolean keepsSymbols = freeVariables(newConnectorProjections.get(i), ImmutableSet.of()).stream()
                    .allMatch(name -> variableMappings.get(name).equals(inputVariableMappings.get(name)));
            if (keepsSymbols && newConnectorProjections.get(i).equals(connectorProjections.get(i))) {
                continue;
            }
            Expression translated = ConnectorExpressionTranslator.translate(session, newConnectorProjections.get(i), plannerContext, variableMappings, context.getSymbolAllocator());
            translated = LambdaCaptureDesugaringRewriter.rewrite(translated, context.getSymbolAllocator());
            // Keep the optimized form of the expression, to avoid an optimizer loop
            newNodes.put(nodes.get(i), plannerContext.getExpressionOptimizer().process(translated, session, context.getSymbolAllocator(), ImmutableMap.of()).orElse(translated));
        }
        Map<NodeRef<Expression>, Expression> replacedNodes = newNodes.buildOrThrow();
        Expression newPredicate = replaceExpression(filter.getPredicate(), replacedNodes);
        Assignments.Builder newProjectAssignments = Assignments.builder();
        project.getAssignments().entrySet().forEach(entry -> newProjectAssignments.put(entry.getKey(), replaceExpression(entry.getValue(), replacedNodes)));
        Assignments newAssignments = newProjectAssignments.build();

        // Each column that the filter and the project read must come from the new table
        // scan. A replaced column stays read where the engine did not offer its expression,
        // such as in a dynamic filter.
        Set<Symbol> scanSymbols = ImmutableSet.copyOf(tableScan.getOutputSymbols());
        Set<Symbol> newScanSymbols = ImmutableSet.copyOf(newScanOutputs);
        boolean missingColumn = Stream.concat(Stream.of(newPredicate), newAssignments.expressions().stream())
                .flatMap(expression -> SymbolsExtractor.extractUnique(expression).stream())
                .anyMatch(symbol -> scanSymbols.contains(symbol) && !newScanSymbols.contains(symbol));
        if (missingColumn) {
            return Result.empty();
        }

        verifyTablePartitioning(context, tableScan, result.get().getHandle());
        return Result.ofPlanNode(new ProjectNode(
                project.getId(),
                new FilterNode(
                        filter.getId(),
                        new TableScanNode(
                                tableScan.getId(),
                                result.get().getHandle(),
                                newScanOutputs,
                                newScanAssignments,
                                tableScan.getEnforcedConstraint().filter((column, _) -> newScanAssignments.containsValue(column)),
                                tableScan.getStatistics().map(statistics -> keptStatistics(statistics, newScanOutputs)),
                                tableScan.isUpdateTarget(),
                                tableScan.getUseConnectorNodePartitioning()),
                        newPredicate),
                newAssignments));
    }

    /// The statistics of the columns that the table scan keeps under their symbols.
    private static PlanNodeStatsEstimate keptStatistics(PlanNodeStatsEstimate statistics, List<Symbol> outputs)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(statistics.getOutputRowCount());
        outputs.stream()
                .filter(symbol -> statistics.getSymbolsWithKnownStatistics().contains(symbol))
                .forEach(symbol -> builder.addSymbolStatistics(symbol, statistics.getSymbolStatistics(symbol)));
        return builder.build();
    }

    private static Set<String> freeVariables(ConnectorExpression expression, Set<String> boundVariables)
    {
        return switch (expression) {
            case Variable variable when boundVariables.contains(variable.getName()) -> ImmutableSet.of();
            case Variable variable -> ImmutableSet.of(variable.getName());
            case Lambda lambda -> freeVariables(lambda.getBody(), withArguments(boundVariables, lambda));
            default -> expression.getChildren().stream()
                    .flatMap(child -> freeVariables(child, boundVariables).stream())
                    .collect(toImmutableSet());
        };
    }

    /// Whether `output` is `input` with only free variables replaced, each by one
    /// variable of the same type, and records the replacements.
    private static boolean replacesOnlyVariables(ConnectorExpression input, ConnectorExpression output, Set<String> boundVariables, Map<String, String> replacements)
    {
        return switch (input) {
            case Variable variable when boundVariables.contains(variable.getName()) -> variable.equals(output);
            case Variable variable -> output instanceof Variable replacement &&
                    replacement.getType().equals(variable.getType()) &&
                    !boundVariables.contains(replacement.getName()) &&
                    replacements.computeIfAbsent(variable.getName(), _ -> replacement.getName()).equals(replacement.getName());
            case Constant constant -> constant.equals(output);
            case Call call -> output instanceof Call other &&
                    other.getType().equals(call.getType()) &&
                    other.getFunctionName().equals(call.getFunctionName()) &&
                    other.getArguments().size() == call.getArguments().size() &&
                    IntStream.range(0, call.getArguments().size())
                            .allMatch(index -> replacesOnlyVariables(call.getArguments().get(index), other.getArguments().get(index), boundVariables, replacements));
            case FieldDereference dereference -> output instanceof FieldDereference other &&
                    other.getType().equals(dereference.getType()) &&
                    other.getField() == dereference.getField() &&
                    replacesOnlyVariables(dereference.getTarget(), other.getTarget(), boundVariables, replacements);
            case Lambda lambda -> output instanceof Lambda other &&
                    other.getType().equals(lambda.getType()) &&
                    other.getArguments().equals(lambda.getArguments()) &&
                    replacesOnlyVariables(lambda.getBody(), other.getBody(), withArguments(boundVariables, lambda), replacements);
            default -> false;
        };
    }

    private static Set<String> withArguments(Set<String> boundVariables, Lambda lambda)
    {
        return ImmutableSet.<String>builder()
                .addAll(boundVariables)
                .addAll(lambda.getArguments().stream().map(Variable::getName).collect(toImmutableList()))
                .build();
    }

    // The table scan partitioning must not change after AddExchanges, as in PushProjectionIntoTableScan
    private void verifyTablePartitioning(Context context, TableScanNode oldTableScan, TableHandle newTable)
    {
        if (oldTableScan.getUseConnectorNodePartitioning().isEmpty()) {
            return;
        }
        Optional<TablePartitioning> oldTablePartitioning = plannerContext.getMetadata().getTableProperties(context.getSession(), oldTableScan.getTable()).getTablePartitioning();
        Optional<TablePartitioning> newTablePartitioning = plannerContext.getMetadata().getTableProperties(context.getSession(), newTable).getTablePartitioning();
        verify(newTablePartitioning.equals(oldTablePartitioning), "Partitioning must not change after projection is pushed down");
    }
}
