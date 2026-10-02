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
package io.trino.plugin.hoglake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Lambda;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;

/**
 * Pushdown of subscripts on VARIANT columns, such as {@code v['k']},
 * {@code v['a']['b']}, and {@code v['a'][1]}, with or without a cast.
 *
 * <p>The connector replaces such a column with one that holds only the paths
 * that the subscripts read, and returns the same projections on that column.
 * The engine still evaluates the subscripts and the casts, so the results, and
 * the errors in the values they read, do not change; the page source reads
 * fewer shredded columns. As with any column pruning, corrupt data in the
 * columns that are not read is no longer detected. A column that a projection
 * also uses in another way is not replaced.
 */
final class HoglakeVariantProjections
{
    // Connector expressions name builtin functions in lowercase, including operators
    private static final FunctionName SUBSCRIPT = new FunctionName("$operator$subscript");

    private HoglakeVariantProjections() {}

    public static Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        // The engine can offer a variable that it binds itself, such as a local
        // variable of an expression that it does not translate as a whole
        if (!projections.stream().flatMap(projection -> freeVariables(projection, ImmutableSet.of()).stream()).allMatch(variable -> assignments.containsKey(variable.getName()))) {
            return Optional.empty();
        }

        Map<String, List<List<HoglakeVariantPathStep>>> pathsByVariable = new LinkedHashMap<>();
        Set<String> wholeVariables = new HashSet<>();
        for (ConnectorExpression projection : projections) {
            collectPaths(projection, ImmutableSet.of(), assignments, pathsByVariable, wholeVariables);
        }
        pathsByVariable.keySet().removeAll(wholeVariables);
        // The engine offers the returned projections again, on the replaced columns
        if (pathsByVariable.isEmpty()) {
            return Optional.empty();
        }

        // A new name must not be the name of a column or of a lambda argument
        Set<String> usedNames = new HashSet<>(assignments.keySet());
        projections.forEach(projection -> collectNames(projection, usedNames));
        Map<String, Variable> replacements = new LinkedHashMap<>();
        Map<String, Assignment> newAssignments = new LinkedHashMap<>();
        pathsByVariable.forEach((name, paths) -> {
            HoglakeColumnHandle column = (HoglakeColumnHandle) assignments.get(name);
            Variable pruned = new Variable(freshName(name + "_pruned", usedNames), VARIANT);
            replacements.put(name, pruned);
            newAssignments.put(pruned.getName(), new Assignment(pruned.getName(), column.withVariantPaths(paths.stream().distinct().toList()), VARIANT));
        });
        List<ConnectorExpression> newProjections = projections.stream()
                .map(projection -> replaceVariables(projection, ImmutableSet.of(), replacements))
                .collect(toImmutableList());
        for (ConnectorExpression projection : newProjections) {
            for (Variable variable : freeVariables(projection, ImmutableSet.of())) {
                newAssignments.computeIfAbsent(variable.getName(), name -> new Assignment(name, assignments.get(name), variable.getType()));
            }
        }
        return Optional.of(new ProjectionApplicationResult<>(table, newProjections, ImmutableList.copyOf(newAssignments.values()), false));
    }

    /**
     * Records the path of each subscript chain on a VARIANT column, and each
     * other use of a column.
     */
    private static void collectPaths(
            ConnectorExpression expression,
            Set<String> boundVariables,
            Map<String, ColumnHandle> assignments,
            Map<String, List<List<HoglakeVariantPathStep>>> pathsByVariable,
            Set<String> wholeVariables)
    {
        Optional<SubscriptChain> chain = subscriptChain(expression);
        if (chain.isPresent() &&
                !boundVariables.contains(chain.get().column().getName()) &&
                assignments.get(chain.get().column().getName()) instanceof HoglakeColumnHandle column &&
                column.variantPaths().isEmpty()) {
            pathsByVariable.computeIfAbsent(chain.get().column().getName(), _ -> new ArrayList<>()).add(chain.get().path());
            return;
        }
        switch (expression) {
            case Variable variable -> {
                if (!boundVariables.contains(variable.getName())) {
                    wholeVariables.add(variable.getName());
                }
            }
            case Lambda lambda -> collectPaths(lambda.getBody(), withArguments(boundVariables, lambda), assignments, pathsByVariable, wholeVariables);
            default -> expression.getChildren().forEach(child -> collectPaths(child, boundVariables, assignments, pathsByVariable, wholeVariables));
        }
    }

    /**
     * The column and path of a chain of VARIANT subscripts with constant keys
     * and indexes, such as {@code v['a'][1]}.
     */
    private static Optional<SubscriptChain> subscriptChain(ConnectorExpression expression)
    {
        List<HoglakeVariantPathStep> path = new ArrayList<>();
        ConnectorExpression current = expression;
        while (current instanceof Call call &&
                call.getFunctionName().equals(SUBSCRIPT) &&
                call.getType().equals(VARIANT) &&
                call.getArguments().size() == 2 &&
                call.getArguments().get(1) instanceof Constant subscript &&
                subscript.getValue() != null) {
            if (subscript.getType() instanceof VarcharType) {
                path.addFirst(HoglakeVariantPathStep.objectKey(((Slice) subscript.getValue()).toStringUtf8()));
            }
            else if (subscript.getType().equals(BIGINT)) {
                path.addFirst(HoglakeVariantPathStep.arrayElement());
            }
            else {
                return Optional.empty();
            }
            current = call.getArguments().getFirst();
        }
        if (path.isEmpty() || !(current instanceof Variable column) || !column.getType().equals(VARIANT)) {
            return Optional.empty();
        }
        return Optional.of(new SubscriptChain(column, ImmutableList.copyOf(path)));
    }

    private static ConnectorExpression replaceVariables(ConnectorExpression expression, Set<String> boundVariables, Map<String, Variable> replacements)
    {
        return switch (expression) {
            case Variable variable when !boundVariables.contains(variable.getName()) && replacements.containsKey(variable.getName()) -> replacements.get(variable.getName());
            case Call call -> new Call(
                    call.getType(),
                    call.getFunctionName(),
                    call.getArguments().stream()
                            .map(argument -> replaceVariables(argument, boundVariables, replacements))
                            .collect(toImmutableList()));
            case FieldDereference dereference -> new FieldDereference(dereference.getType(), replaceVariables(dereference.getTarget(), boundVariables, replacements), dereference.getField());
            case Lambda lambda -> new Lambda(lambda.getType(), lambda.getArguments(), replaceVariables(lambda.getBody(), withArguments(boundVariables, lambda), replacements));
            default -> expression;
        };
    }

    private static List<Variable> freeVariables(ConnectorExpression expression, Set<String> boundVariables)
    {
        return switch (expression) {
            case Variable variable when boundVariables.contains(variable.getName()) -> List.of();
            case Variable variable -> List.of(variable);
            case Lambda lambda -> freeVariables(lambda.getBody(), withArguments(boundVariables, lambda));
            default -> expression.getChildren().stream()
                    .flatMap(child -> freeVariables(child, boundVariables).stream())
                    .collect(toImmutableList());
        };
    }

    private static void collectNames(ConnectorExpression expression, Set<String> names)
    {
        switch (expression) {
            case Variable variable -> names.add(variable.getName());
            case Lambda lambda -> {
                lambda.getArguments().forEach(argument -> names.add(argument.getName()));
                collectNames(lambda.getBody(), names);
            }
            default -> expression.getChildren().forEach(child -> collectNames(child, names));
        }
    }

    private static String freshName(String name, Set<String> usedNames)
    {
        String fresh = name;
        for (int suffix = 0; !usedNames.add(fresh); suffix++) {
            fresh = name + "_" + suffix;
        }
        return fresh;
    }

    private static Set<String> withArguments(Set<String> boundVariables, Lambda lambda)
    {
        return ImmutableSet.<String>builder()
                .addAll(boundVariables)
                .addAll(lambda.getArguments().stream().map(Variable::getName).iterator())
                .build();
    }

    private record SubscriptChain(Variable column, List<HoglakeVariantPathStep> path) {}
}
