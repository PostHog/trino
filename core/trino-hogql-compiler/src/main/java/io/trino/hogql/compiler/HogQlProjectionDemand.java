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

import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinUsing;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowFrame;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

final class HogQlProjectionDemand
{
    private final boolean all;
    private final Set<String> unqualified;
    private final Set<String> allQualifiers;
    private final Map<String, Set<String>> qualified;

    private HogQlProjectionDemand(boolean all, Set<String> unqualified, Set<String> allQualifiers, Map<String, Set<String>> qualified)
    {
        this.all = all;
        this.unqualified = Set.copyOf(requireNonNull(unqualified, "unqualified is null"));
        this.allQualifiers = Set.copyOf(requireNonNull(allQualifiers, "allQualifiers is null"));
        requireNonNull(qualified, "qualified is null");
        Map<String, Set<String>> copied = new HashMap<>();
        qualified.forEach((key, value) -> copied.put(key, Set.copyOf(value)));
        this.qualified = Map.copyOf(copied);
    }

    public static HogQlProjectionDemand collect(HogQlQuery query)
    {
        Builder builder = new Builder();
        if (query.body() instanceof SelectQueryBody select) {
            select.projections().forEach(projection -> collect(projection, builder));
            select.where().ifPresent(expression -> collect(expression, builder));
            select.groupBy().forEach(expression -> collect(expression, builder));
            select.having().ifPresent(expression -> collect(expression, builder));
            select.windows().forEach(window -> collect(window.specification(), builder));
            select.from().ifPresent(relation -> collectJoinCriteria(relation, builder));
        }
        query.orderBy().forEach(sortItem -> collect(sortItem.expression(), builder));
        query.limit().ifPresent(expression -> collect(expression, builder));
        query.offset().ifPresent(expression -> collect(expression, builder));
        return builder.build();
    }

    public static HogQlProjectionDemand preserveAll()
    {
        return new HogQlProjectionDemand(true, Set.of(), Set.of(), Map.of());
    }

    public RequiredOutputs forAlias(Identifier alias)
    {
        String qualifier = canonical(alias.value());
        if (all || allQualifiers.contains(qualifier)) {
            return RequiredOutputs.allOutputs();
        }
        Set<String> names = new HashSet<>(unqualified);
        names.addAll(qualified.getOrDefault(qualifier, Set.of()));
        return new RequiredOutputs(false, names);
    }

    public RequiredOutputs unqualified()
    {
        if (all) {
            return RequiredOutputs.allOutputs();
        }
        return new RequiredOutputs(false, unqualified);
    }

    private static void collect(Projection projection, Builder builder)
    {
        switch (projection) {
            case ColumnsList columns -> columns.expressions().forEach(expression -> collect(expression, builder));
            case ColumnsRegex _ -> builder.all = true;
            case ExpressionProjection expression -> collect(expression.expression(), builder);
            case Star star -> {
                if (star.qualifier().isEmpty()) {
                    builder.all = true;
                }
                else {
                    builder.allQualifiers.add(canonical(star.qualifier().getFirst().value()));
                }
                star.replacements().forEach(replacement -> collect(replacement.expression(), builder));
            }
        }
    }

    private static void collectJoinCriteria(Relation relation, Builder builder)
    {
        switch (relation) {
            case HogQlQuery.AliasedRelation alias -> collectJoinCriteria(alias.relation(), builder);
            case HogQlQuery.CommonTableReference _, HogQlQuery.SubqueryRelation _, HogQlQuery.TablePlaceholder _, HogQlQuery.TableReference _, HogQlQuery.ValuesRelation _ -> {}
            case JoinRelation join -> {
                collectJoinCriteria(join.left(), builder);
                collectJoinCriteria(join.right(), builder);
                join.criteria().ifPresent(criteria -> {
                    switch (criteria) {
                        case JoinOn on -> collect(on.expression(), builder);
                        case JoinUsing using -> using.columns().forEach(builder::addUnqualified);
                    }
                });
            }
        }
    }

    private static void collect(Expression expression, Builder builder)
    {
        switch (expression) {
            case ArrayExpression array -> array.values().forEach(value -> collect(value, builder));
            case BetweenExpression between -> {
                collect(between.value(), builder);
                collect(between.min(), builder);
                collect(between.max(), builder);
            }
            case BinaryExpression binary -> {
                collect(binary.left(), builder);
                collect(binary.right(), builder);
            }
            case CaseExpression caseExpression -> {
                caseExpression.operand().ifPresent(value -> collect(value, builder));
                caseExpression.whenClauses().forEach(when -> {
                    collect(when.operand(), builder);
                    collect(when.result(), builder);
                });
                caseExpression.defaultValue().ifPresent(value -> collect(value, builder));
            }
            case CastExpression cast -> collect(cast.value(), builder);
            case ColumnReference reference -> builder.add(reference);
            case FunctionCall function -> {
                function.arguments().forEach(argument -> collect(argument, builder));
                function.orderBy().forEach(sortItem -> collect(sortItem.expression(), builder));
                function.filter().ifPresent(filter -> collect(filter, builder));
                function.window().ifPresent(window -> collect(window, builder));
            }
            case InCohortExpression cohort -> collect(cohort.value(), builder);
            case InExpression in -> {
                collect(in.value(), builder);
                in.values().forEach(value -> collect(value, builder));
            }
            case InSubqueryExpression in -> {
                collect(in.value(), builder);
                builder.all = true;
            }
            case IntervalExpression interval -> collect(interval.value(), builder);
            case IsNullExpression isNull -> collect(isNull.value(), builder);
            case Literal _, Placeholder _ -> {}
            case MemberAccessExpression member -> collect(member.base(), builder);
            case SubscriptExpression subscript -> {
                collect(subscript.base(), builder);
                collect(subscript.index(), builder);
            }
            case TupleExpression tuple -> tuple.values().forEach(value -> collect(value, builder));
            case UnaryExpression unary -> collect(unary.operand(), builder);
        }
    }

    private static void collect(Window window, Builder builder)
    {
        switch (window) {
            case WindowReference _ -> {}
            case WindowSpecification specification -> {
                specification.partitionBy().forEach(expression -> collect(expression, builder));
                specification.orderBy().forEach(sortItem -> collect(sortItem.expression(), builder));
                specification.frame().ifPresent(frame -> collect(frame, builder));
            }
        }
    }

    private static void collect(WindowFrame frame, Builder builder)
    {
        frame.start().value().ifPresent(value -> collect(value, builder));
        frame.end().flatMap(HogQlQuery.FrameBound::value).ifPresent(value -> collect(value, builder));
    }

    private static String canonical(String value)
    {
        return value.toLowerCase(java.util.Locale.ENGLISH);
    }

    public record RequiredOutputs(boolean all, Set<String> names)
    {
        public RequiredOutputs
        {
            names = Set.copyOf(requireNonNull(names, "names is null"));
        }

        public static RequiredOutputs allOutputs()
        {
            return new RequiredOutputs(true, Set.of());
        }

        public boolean includes(String name)
        {
            return all || names.contains(canonical(name));
        }
    }

    private static final class Builder
    {
        private boolean all;
        private final Set<String> unqualified = new HashSet<>();
        private final Set<String> allQualifiers = new HashSet<>();
        private final Map<String, Set<String>> qualified = new HashMap<>();

        public void add(ColumnReference reference)
        {
            if (reference.parts().size() == 1) {
                unqualified.add(canonical(reference.parts().getFirst().value()));
                return;
            }
            String qualifier = canonical(reference.parts().getFirst().value());
            String field = canonical(reference.parts().get(1).value());
            qualified.computeIfAbsent(qualifier, _ -> new HashSet<>()).add(field);
        }

        public void addUnqualified(Identifier identifier)
        {
            unqualified.add(canonical(identifier.value()));
        }

        public HogQlProjectionDemand build()
        {
            return new HogQlProjectionDemand(all, unqualified, allQualifiers, qualified);
        }
    }
}
