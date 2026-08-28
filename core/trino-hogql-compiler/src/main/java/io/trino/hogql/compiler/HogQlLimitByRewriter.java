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
import io.trino.hogql.parser.tree.HogQlQuery.BinaryOperator;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.LimitBy;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;

final class HogQlLimitByRewriter
{
    private static final String BASE_ALIAS = "__hogql_limit_by_base";
    private static final String RANKED_ALIAS = "__hogql_limit_by_ranked";
    private static final String ROW_NUMBER = "__hogql_limit_by_row_number";

    private HogQlLimitByRewriter() {}

    public static HogQlQuery rewrite(HogQlQuery query)
    {
        if (!(query.body() instanceof SelectQueryBody select) || select.limitBy().isEmpty()) {
            return query;
        }
        LimitBy limitBy = select.limitBy().orElseThrow();
        List<OutputColumn> outputs = outputs(select.projections(), limitBy.span());
        Map<String, OutputColumn> outputsByName = outputsByName(outputs);

        List<Projection> baseProjections = outputs.stream()
                .map(output -> new ExpressionProjection(output.expression(), Optional.of(output.innerName())))
                .map(Projection.class::cast)
                .toList();
        HogQlQuery baseQuery = new HogQlQuery(
                List.of(),
                new SelectQueryBody(
                        select.distinct(),
                        baseProjections,
                        select.from(),
                        select.where(),
                        select.groupBy(),
                        select.having(),
                        select.windows(),
                        Optional.empty(),
                        select.span()),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                query.span());

        Identifier baseAlias = identifier(BASE_ALIAS, limitBy.span());
        List<Projection> rankedProjections = new ArrayList<>();
        outputs.forEach(output -> rankedProjections.add(new ExpressionProjection(
                column(baseAlias, output.innerName(), output.expression().span()),
                Optional.of(output.innerName()))));
        WindowSpecification rankingWindow = new WindowSpecification(
                limitBy.partitionBy().stream()
                        .map(expression -> remap(expression, outputsByName, baseAlias))
                        .toList(),
                query.orderBy().stream()
                        .map(sortItem -> remap(sortItem, outputsByName, baseAlias))
                        .toList(),
                Optional.empty(),
                limitBy.span());
        FunctionCall rowNumber = new FunctionCall(
                List.of(identifier("row_number", limitBy.span())),
                List.of(),
                false,
                List.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(rankingWindow),
                limitBy.span());
        rankedProjections.add(new ExpressionProjection(rowNumber, Optional.of(identifier(ROW_NUMBER, limitBy.span()))));
        HogQlQuery rankedQuery = new HogQlQuery(
                List.of(),
                new SelectQueryBody(
                        false,
                        rankedProjections,
                        Optional.of(new HogQlQuery.AliasedRelation(new SubqueryRelation(baseQuery, query.span()), baseAlias, query.span())),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        select.span()),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                query.span());

        Identifier rankedAlias = identifier(RANKED_ALIAS, limitBy.span());
        List<Projection> outerProjections = outputs.stream()
                .map(output -> new ExpressionProjection(
                        column(rankedAlias, output.innerName(), output.expression().span()),
                        Optional.of(output.outputName())))
                .map(Projection.class::cast)
                .toList();
        Expression rowNumberReference = column(rankedAlias, identifier(ROW_NUMBER, limitBy.span()), limitBy.span());
        Expression upperBound = limitBy.offset()
                .<Expression>map(offset -> new BinaryExpression(BinaryOperator.ADD, offset, limitBy.limit(), limitBy.span()))
                .orElse(limitBy.limit());
        Expression filter = new BinaryExpression(BinaryOperator.LESS_THAN_OR_EQUAL, rowNumberReference, upperBound, limitBy.span());
        if (limitBy.offset().isPresent()) {
            filter = new BinaryExpression(
                    BinaryOperator.AND,
                    new BinaryExpression(BinaryOperator.GREATER_THAN, rowNumberReference, limitBy.offset().orElseThrow(), limitBy.span()),
                    filter,
                    limitBy.span());
        }
        List<SortItem> outerOrderBy = query.orderBy().stream()
                .map(sortItem -> remap(sortItem, outputsByName, rankedAlias))
                .toList();
        return new HogQlQuery(
                query.with(),
                new SelectQueryBody(
                        false,
                        outerProjections,
                        Optional.of(new HogQlQuery.AliasedRelation(new SubqueryRelation(rankedQuery, query.span()), rankedAlias, query.span())),
                        Optional.of(filter),
                        List.of(),
                        Optional.empty(),
                        List.of(),
                        Optional.empty(),
                        select.span()),
                outerOrderBy,
                query.limit(),
                query.offset(),
                query.span());
    }

    private static List<OutputColumn> outputs(List<Projection> projections, SourceSpan span)
    {
        List<OutputColumn> outputs = new ArrayList<>();
        for (Projection projection : projections) {
            if (!(projection instanceof ExpressionProjection expression)) {
                throw unsupported(span, "HogQL LIMIT BY requires explicit output columns");
            }
            Identifier outputName = expression.alias()
                    .orElseGet(() -> {
                        if (expression.expression() instanceof ColumnReference reference) {
                            return reference.parts().getLast();
                        }
                        throw unsupported(expression.span(), "HogQL LIMIT BY expressions require output aliases");
                    });
            outputs.add(new OutputColumn(
                    expression.expression(),
                    outputName,
                    identifier("__hogql_limit_by_column_" + outputs.size(), expression.span())));
        }
        return List.copyOf(outputs);
    }

    private static Map<String, OutputColumn> outputsByName(List<OutputColumn> outputs)
    {
        Map<String, OutputColumn> byName = new HashMap<>();
        Set<String> duplicates = new HashSet<>();
        for (OutputColumn output : outputs) {
            String name = canonical(output.outputName().value());
            if (byName.putIfAbsent(name, output) != null) {
                duplicates.add(name);
            }
        }
        duplicates.forEach(byName::remove);
        return Map.copyOf(byName);
    }

    private static SortItem remap(SortItem sortItem, Map<String, OutputColumn> outputs, Identifier relationAlias)
    {
        return new SortItem(
                remap(sortItem.expression(), outputs, relationAlias),
                sortItem.direction(),
                sortItem.nullPlacement(),
                sortItem.span());
    }

    private static Expression remap(Expression expression, Map<String, OutputColumn> outputs, Identifier relationAlias)
    {
        return switch (expression) {
            case ArrayExpression array -> new ArrayExpression(array.values().stream().map(value -> remap(value, outputs, relationAlias)).toList(), array.span());
            case BetweenExpression between -> new BetweenExpression(
                    remap(between.value(), outputs, relationAlias),
                    remap(between.min(), outputs, relationAlias),
                    remap(between.max(), outputs, relationAlias),
                    between.negated(),
                    between.predicateSpan(),
                    between.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    remap(binary.left(), outputs, relationAlias),
                    remap(binary.right(), outputs, relationAlias),
                    binary.span());
            case CaseExpression caseExpression -> new CaseExpression(
                    caseExpression.operand().map(value -> remap(value, outputs, relationAlias)),
                    caseExpression.whenClauses().stream()
                            .map(when -> new CaseWhen(
                                    remap(when.operand(), outputs, relationAlias),
                                    remap(when.result(), outputs, relationAlias),
                                    when.span()))
                            .toList(),
                    caseExpression.defaultValue().map(value -> remap(value, outputs, relationAlias)),
                    caseExpression.span());
            case CastExpression cast -> new CastExpression(remap(cast.value(), outputs, relationAlias), cast.type(), cast.safe(), cast.typeDialect(), cast.span());
            case ColumnReference reference -> remap(reference, outputs, relationAlias);
            case FunctionCall function -> new FunctionCall(
                    function.nameParts(),
                    function.arguments().stream().map(argument -> remap(argument, outputs, relationAlias)).toList(),
                    function.distinct(),
                    function.orderBy().stream().map(item -> remap(item, outputs, relationAlias)).toList(),
                    function.filter().map(filter -> remap(filter, outputs, relationAlias)),
                    function.nullTreatment(),
                    function.window().map(_ -> {
                        throw unsupported(function.span(), "HogQL LIMIT BY does not support window expressions in its keys or ordering");
                    }),
                    function.span());
            case InCohortExpression in -> new InCohortExpression(
                    remap(in.value(), outputs, relationAlias),
                    remap(in.cohort(), outputs, relationAlias),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InExpression in -> new InExpression(
                    remap(in.value(), outputs, relationAlias),
                    in.values().stream().map(value -> remap(value, outputs, relationAlias)).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case HogQlQuery.InSubqueryExpression in -> throw unsupported(in.span(), "HogQL LIMIT BY does not support subqueries in its keys or ordering");
            case IntervalExpression interval -> new IntervalExpression(remap(interval.value(), outputs, relationAlias), interval.unit(), interval.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    remap(isNull.value(), outputs, relationAlias),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case HogQlQuery.LambdaExpression lambda -> throw unsupported(lambda.span(), "HogQL LIMIT BY does not support lambdas in its keys or ordering");
            case Literal literal -> literal;
            case MemberAccessExpression member -> new MemberAccessExpression(remap(member.base(), outputs, relationAlias), member.member(), member.span());
            case Placeholder placeholder -> placeholder;
            case ScalarSubqueryExpression subquery -> throw unsupported(subquery.span(), "HogQL LIMIT BY does not support subqueries in its keys or ordering");
            case SubscriptExpression subscript -> new SubscriptExpression(
                    remap(subscript.base(), outputs, relationAlias),
                    remap(subscript.index(), outputs, relationAlias),
                    subscript.span());
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(value -> remap(value, outputs, relationAlias)).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), remap(unary.operand(), outputs, relationAlias), unary.span());
        };
    }

    private static Expression remap(ColumnReference reference, Map<String, OutputColumn> outputs, Identifier relationAlias)
    {
        if (reference.parts().size() == 1) {
            OutputColumn output = outputs.get(canonical(reference.parts().getFirst().value()));
            if (output != null) {
                return column(relationAlias, output.innerName(), reference.span());
            }
        }
        Optional<OutputColumn> exact = outputs.values().stream()
                .filter(output -> output.expression() instanceof ColumnReference)
                .filter(output -> sameReference(reference, (ColumnReference) output.expression()))
                .findFirst();
        if (exact.isPresent()) {
            return column(relationAlias, exact.orElseThrow().innerName(), reference.span());
        }
        throw unsupported(reference.span(), "HogQL LIMIT BY keys and ordering must reference projected outputs");
    }

    private static boolean sameReference(ColumnReference left, ColumnReference right)
    {
        if (left.parts().size() != right.parts().size()) {
            return false;
        }
        for (int index = 0; index < left.parts().size(); index++) {
            Identifier leftPart = left.parts().get(index);
            Identifier rightPart = right.parts().get(index);
            if (leftPart.delimited() != rightPart.delimited() || !canonical(leftPart.value()).equals(canonical(rightPart.value()))) {
                return false;
            }
        }
        return true;
    }

    private static ColumnReference column(Identifier relation, Identifier column, SourceSpan span)
    {
        return new ColumnReference(List.of(relation, column), span);
    }

    private static Identifier identifier(String value, SourceSpan span)
    {
        return new Identifier(value, false, span);
    }

    private static String canonical(String value)
    {
        return value.toLowerCase(Locale.ENGLISH);
    }

    private static TrinoException unsupported(SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_UNSUPPORTED_FEATURE,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }

    private record OutputColumn(Expression expression, Identifier outputName, Identifier innerName) {}
}
