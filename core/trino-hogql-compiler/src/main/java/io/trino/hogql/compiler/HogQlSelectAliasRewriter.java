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
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FrameBound;
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
import io.trino.hogql.parser.tree.HogQlQuery.LambdaExpression;
import io.trino.hogql.parser.tree.HogQlQuery.LimitBy;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.PivotAggregation;
import io.trino.hogql.parser.tree.HogQlQuery.PivotRelation;
import io.trino.hogql.parser.tree.HogQlQuery.PivotValueGroup;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.StarReplacement;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnnestRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowDefinition;
import io.trino.hogql.parser.tree.HogQlQuery.WindowFrame;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class HogQlSelectAliasRewriter
{
    private HogQlSelectAliasRewriter() {}

    public static HogQlQuery rewrite(HogQlQuery query)
    {
        List<CommonTableExpression> commonTables = query.with().stream()
                .map(commonTable -> new CommonTableExpression(
                        commonTable.name(),
                        commonTable.columnAliases(),
                        rewrite(commonTable.query()),
                        commonTable.span()))
                .toList();
        return switch (query.body()) {
            case SelectQueryBody select -> rewriteSelect(query, commonTables, select);
            case SetOperation set -> new HogQlQuery(
                    commonTables,
                    new SetOperation(
                            set.type(),
                            set.distinct(),
                            rewrite(set.left()),
                            rewrite(set.right()),
                            set.leftParenthesized(),
                            set.rightParenthesized(),
                            set.operatorSpan(),
                            set.span()),
                    query.orderBy().stream().map(item -> rewriteSortItem(item, Map.of())).toList(),
                    query.limit().map(expression -> rewriteExpression(expression, Map.of())),
                    query.offset().map(expression -> rewriteExpression(expression, Map.of())),
                    query.span());
        };
    }

    private static HogQlQuery rewriteSelect(HogQlQuery query, List<CommonTableExpression> commonTables, SelectQueryBody select)
    {
        Map<AliasKey, Expression> aliases = new HashMap<>();
        Set<AliasKey> ambiguous = new HashSet<>();
        List<Projection> projections = select.projections().stream()
                .map(projection -> rewriteProjection(projection, aliases, ambiguous))
                .toList();
        Map<AliasKey, Expression> visibleAliases = Map.copyOf(aliases);
        return new HogQlQuery(
                commonTables,
                new SelectQueryBody(
                        select.distinct(),
                        projections,
                        select.from().map(HogQlSelectAliasRewriter::rewriteRelation),
                        select.where().map(expression -> rewriteExpression(expression, visibleAliases)),
                        select.groupBy().stream().map(expression -> rewriteExpression(expression, visibleAliases)).toList(),
                        select.having().map(expression -> rewriteExpression(expression, visibleAliases)),
                        select.windows().stream().map(window -> rewriteWindowDefinition(window, visibleAliases)).toList(),
                        select.limitBy().map(limitBy -> new LimitBy(
                                rewriteExpression(limitBy.limit(), visibleAliases),
                                limitBy.offset().map(expression -> rewriteExpression(expression, visibleAliases)),
                                limitBy.partitionBy().stream().map(expression -> rewriteExpression(expression, visibleAliases)).toList(),
                                limitBy.span())),
                        select.span()),
                query.orderBy().stream().map(item -> rewriteSortItem(item, visibleAliases)).toList(),
                query.limit().map(expression -> rewriteExpression(expression, visibleAliases)),
                query.offset().map(expression -> rewriteExpression(expression, visibleAliases)),
                query.span());
    }

    private static Projection rewriteProjection(Projection projection, Map<AliasKey, Expression> aliases, Set<AliasKey> ambiguous)
    {
        return switch (projection) {
            case ColumnsList columns -> new ColumnsList(
                    columns.expressions().stream().map(expression -> rewriteExpression(expression, aliases)).toList(),
                    columns.span());
            case ColumnsRegex columns -> columns;
            case ExpressionProjection expression -> {
                Expression rewritten = rewriteExpression(expression.expression(), aliases);
                expression.alias().ifPresent(alias -> registerAlias(alias, rewritten, aliases, ambiguous));
                yield new ExpressionProjection(rewritten, expression.alias());
            }
            case Star star -> new Star(
                    star.qualifier(),
                    star.exclusions(),
                    star.replacements().stream()
                            .map(replacement -> new StarReplacement(
                                    rewriteExpression(replacement.expression(), aliases),
                                    replacement.target(),
                                    replacement.span()))
                            .toList(),
                    star.span());
        };
    }

    private static void registerAlias(Identifier alias, Expression expression, Map<AliasKey, Expression> aliases, Set<AliasKey> ambiguous)
    {
        AliasKey key = AliasKey.of(alias);
        if (!ambiguous.add(key)) {
            aliases.remove(key);
            return;
        }
        aliases.put(key, expression);
    }

    private static Relation rewriteRelation(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> new AliasedRelation(rewriteRelation(alias.relation()), alias.alias(), alias.columnAliases(), alias.span());
            case CommonTableReference commonTable -> commonTable;
            case JoinRelation join -> new JoinRelation(
                    join.type(),
                    rewriteRelation(join.left()),
                    rewriteRelation(join.right()),
                    join.criteria().map(criteria -> switch (criteria) {
                        case JoinOn on -> new JoinOn(rewriteExpression(on.expression(), Map.of()), on.span());
                        case JoinUsing using -> using;
                    }),
                    join.span());
            case PivotRelation pivot -> new PivotRelation(
                    rewriteRelation(pivot.input()),
                    pivot.aggregations().stream()
                            .map(aggregation -> new PivotAggregation(
                                    rewriteExpression(aggregation.expression(), Map.of()),
                                    aggregation.alias(),
                                    aggregation.span()))
                            .toList(),
                    pivot.pivotColumns().stream().map(expression -> rewriteExpression(expression, Map.of())).toList(),
                    pivot.valueGroups().stream()
                            .map(group -> new PivotValueGroup(
                                    group.values().stream().map(expression -> rewriteExpression(expression, Map.of())).toList(),
                                    group.alias(),
                                    group.span()))
                            .toList(),
                    pivot.groupBy().stream().map(expression -> rewriteExpression(expression, Map.of())).toList(),
                    pivot.span());
            case SubqueryRelation subquery -> new SubqueryRelation(rewrite(subquery.query()), subquery.span());
            case TablePlaceholder table -> table;
            case TableReference table -> table;
            case UnnestRelation unnest -> new UnnestRelation(
                    unnest.expressions().stream().map(expression -> rewriteExpression(expression, Map.of())).toList(),
                    unnest.alias(),
                    unnest.columnAliases(),
                    unnest.span());
            case ValuesRelation values -> new ValuesRelation(
                    values.rows().stream()
                            .map(row -> row.stream().map(expression -> rewriteExpression(expression, Map.of())).toList())
                            .toList(),
                    values.span());
        };
    }

    private static Expression rewriteExpression(Expression expression, Map<AliasKey, Expression> aliases)
    {
        return switch (expression) {
            case ArrayExpression array -> new ArrayExpression(array.values().stream().map(value -> rewriteExpression(value, aliases)).toList(), array.span());
            case BetweenExpression between -> new BetweenExpression(
                    rewriteExpression(between.value(), aliases),
                    rewriteExpression(between.min(), aliases),
                    rewriteExpression(between.max(), aliases),
                    between.negated(),
                    between.predicateSpan(),
                    between.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    rewriteExpression(binary.left(), aliases),
                    rewriteExpression(binary.right(), aliases),
                    binary.span());
            case CaseExpression caseExpression -> new CaseExpression(
                    caseExpression.operand().map(value -> rewriteExpression(value, aliases)),
                    caseExpression.whenClauses().stream()
                            .map(when -> new CaseWhen(
                                    rewriteExpression(when.operand(), aliases),
                                    rewriteExpression(when.result(), aliases),
                                    when.span()))
                            .toList(),
                    caseExpression.defaultValue().map(value -> rewriteExpression(value, aliases)),
                    caseExpression.span());
            case CastExpression cast -> new CastExpression(rewriteExpression(cast.value(), aliases), cast.type(), cast.safe(), cast.typeDialect(), cast.span());
            case ColumnReference reference -> alias(reference, aliases).orElse(reference);
            case FunctionCall function -> new FunctionCall(
                    function.nameParts(),
                    function.arguments().stream().map(argument -> rewriteExpression(argument, aliases)).toList(),
                    function.distinct(),
                    function.orderBy().stream().map(item -> rewriteSortItem(item, aliases)).toList(),
                    function.filter().map(filter -> rewriteExpression(filter, aliases)),
                    function.nullTreatment(),
                    function.window().map(window -> rewriteWindow(window, aliases)),
                    function.span());
            case InCohortExpression in -> new InCohortExpression(
                    rewriteExpression(in.value(), aliases),
                    rewriteExpression(in.cohort(), aliases),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InExpression in -> new InExpression(
                    rewriteExpression(in.value(), aliases),
                    in.values().stream().map(value -> rewriteExpression(value, aliases)).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InSubqueryExpression in -> new InSubqueryExpression(
                    rewriteExpression(in.value(), aliases),
                    rewrite(in.query()),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IntervalExpression interval -> new IntervalExpression(rewriteExpression(interval.value(), aliases), interval.unit(), interval.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    rewriteExpression(isNull.value(), aliases),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case LambdaExpression lambda -> {
                Map<AliasKey, Expression> visibleAliases = new HashMap<>(aliases);
                lambda.arguments().forEach(argument -> visibleAliases.remove(AliasKey.of(argument)));
                yield new LambdaExpression(lambda.arguments(), rewriteExpression(lambda.body(), visibleAliases), lambda.span());
            }
            case Literal literal -> literal;
            case MemberAccessExpression member -> new MemberAccessExpression(rewriteExpression(member.base(), aliases), member.member(), member.span());
            case Placeholder placeholder -> placeholder;
            case ScalarSubqueryExpression subquery -> new ScalarSubqueryExpression(rewrite(subquery.query()), subquery.span());
            case SubscriptExpression subscript -> new SubscriptExpression(
                    rewriteExpression(subscript.base(), aliases),
                    rewriteExpression(subscript.index(), aliases),
                    subscript.span());
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(value -> rewriteExpression(value, aliases)).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), rewriteExpression(unary.operand(), aliases), unary.span());
        };
    }

    private static Optional<Expression> alias(ColumnReference reference, Map<AliasKey, Expression> aliases)
    {
        if (reference.parts().size() != 1) {
            return Optional.empty();
        }
        return Optional.ofNullable(aliases.get(AliasKey.of(reference.parts().getFirst())));
    }

    private static SortItem rewriteSortItem(SortItem item, Map<AliasKey, Expression> aliases)
    {
        return new SortItem(rewriteExpression(item.expression(), aliases), item.direction(), item.nullPlacement(), item.span());
    }

    private static WindowDefinition rewriteWindowDefinition(WindowDefinition definition, Map<AliasKey, Expression> aliases)
    {
        return new WindowDefinition(definition.name(), rewriteWindowSpecification(definition.specification(), aliases), definition.span());
    }

    private static Window rewriteWindow(Window window, Map<AliasKey, Expression> aliases)
    {
        return switch (window) {
            case WindowReference reference -> reference;
            case WindowSpecification specification -> rewriteWindowSpecification(specification, aliases);
        };
    }

    private static WindowSpecification rewriteWindowSpecification(WindowSpecification window, Map<AliasKey, Expression> aliases)
    {
        return new WindowSpecification(
                window.partitionBy().stream().map(expression -> rewriteExpression(expression, aliases)).toList(),
                window.orderBy().stream().map(item -> rewriteSortItem(item, aliases)).toList(),
                window.frame().map(frame -> rewriteWindowFrame(frame, aliases)),
                window.span());
    }

    private static WindowFrame rewriteWindowFrame(WindowFrame frame, Map<AliasKey, Expression> aliases)
    {
        return new WindowFrame(
                frame.type(),
                rewriteFrameBound(frame.start(), aliases),
                frame.end().map(bound -> rewriteFrameBound(bound, aliases)),
                frame.span());
    }

    private static FrameBound rewriteFrameBound(FrameBound bound, Map<AliasKey, Expression> aliases)
    {
        return new FrameBound(bound.type(), bound.value().map(value -> rewriteExpression(value, aliases)), bound.span());
    }

    private record AliasKey(String value, boolean delimited)
    {
        private static AliasKey of(Identifier identifier)
        {
            return new AliasKey(identifier.delimited() ? identifier.value() : identifier.value().toLowerCase(Locale.ENGLISH), identifier.delimited());
        }
    }
}
