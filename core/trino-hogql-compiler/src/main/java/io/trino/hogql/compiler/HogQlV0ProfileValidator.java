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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.InCohortExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.hogql.parser.tree.HogQlQuery.Window;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.Locale;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlV0ProfileValidator
{
    private HogQlV0ProfileValidator() {}

    public static void validate(HogQlQuery query, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        requireNonNull(query, "query is null");
        requireNonNull(snapshot, "snapshot is null");
        query.with().forEach(commonTable -> validate(commonTable.query(), snapshot));
        switch (query.body()) {
            case SelectQueryBody select -> {
                select.projections().forEach(projection -> validate(projection, snapshot));
                select.from().ifPresent(relation -> validate(relation, snapshot));
                select.where().ifPresent(expression -> validate(expression, snapshot));
                select.groupBy().forEach(expression -> validate(expression, snapshot));
                select.having().ifPresent(expression -> validate(expression, snapshot));
                select.windows().forEach(window -> validate(window.specification(), snapshot));
            }
            case SetOperation setOperation -> {
                validate(setOperation.left(), snapshot);
                validate(setOperation.right(), snapshot);
            }
        }
        query.orderBy().forEach(sortItem -> validate(sortItem.expression(), snapshot));
        query.limit().ifPresent(expression -> validate(expression, snapshot));
        query.offset().ifPresent(expression -> validate(expression, snapshot));
    }

    private static void validate(Projection projection, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        switch (projection) {
            case ColumnsList columns -> columns.expressions().forEach(expression -> validate(expression, snapshot));
            case ColumnsRegex _ -> {}
            case ExpressionProjection expression -> validate(expression.expression(), snapshot);
            case Star star -> star.replacements().forEach(replacement -> validate(replacement.expression(), snapshot));
        }
    }

    private static void validate(Relation relation, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        switch (relation) {
            case AliasedRelation alias -> validate(alias.relation(), snapshot);
            case CommonTableReference _, TablePlaceholder _ -> {}
            case JoinRelation join -> {
                validate(join.left(), snapshot);
                validate(join.right(), snapshot);
                join.criteria().filter(JoinOn.class::isInstance)
                        .map(JoinOn.class::cast)
                        .ifPresent(on -> validate(on.expression(), snapshot));
            }
            case HogQlQuery.PivotRelation pivot -> throw unsupported(pivot.span(), "PIVOT is outside the HogQL v0 profile");
            case SubqueryRelation subquery -> validate(subquery.query(), snapshot);
            case TableReference table -> validateTable(table, snapshot);
            case ValuesRelation values -> values.rows().forEach(row -> row.forEach(expression -> validate(expression, snapshot)));
        }
    }

    private static void validateTable(TableReference table, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        if (table.parts().size() != 1 || snapshot.isEmpty()) {
            return;
        }
        String name = canonical(table.parts().getFirst().value());
        HogQlSemanticCatalogSnapshot catalog = snapshot.orElseThrow();
        if (catalog.logicalTable(name).isPresent() && !name.equals("events") && !name.equals("persons")) {
            throw unsupported(table.span(), "Logical table " + table.parts().getFirst().value() + " is outside the HogQL v0 profile");
        }
        boolean deferredRelation = catalog.virtualTables().stream().anyMatch(tableDefinition -> canonical(tableDefinition.name()).equals(name)) ||
                catalog.savedQueries().stream().anyMatch(query -> canonical(query.name()).equals(name)) ||
                catalog.materializedViews().stream().anyMatch(view -> canonical(view.name()).equals(name));
        if (deferredRelation) {
            throw unsupported(table.span(), "Semantic relation " + table.parts().getFirst().value() + " is outside the HogQL v0 profile");
        }
    }

    private static void validate(Expression expression, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        switch (expression) {
            case ArrayExpression array -> array.values().forEach(value -> validate(value, snapshot));
            case BetweenExpression between -> {
                validate(between.value(), snapshot);
                validate(between.min(), snapshot);
                validate(between.max(), snapshot);
            }
            case BinaryExpression binary -> {
                validate(binary.left(), snapshot);
                validate(binary.right(), snapshot);
            }
            case CaseExpression caseExpression -> {
                caseExpression.operand().ifPresent(value -> validate(value, snapshot));
                caseExpression.whenClauses().forEach(when -> {
                    validate(when.operand(), snapshot);
                    validate(when.result(), snapshot);
                });
                caseExpression.defaultValue().ifPresent(value -> validate(value, snapshot));
            }
            case CastExpression cast -> validate(cast.value(), snapshot);
            case ColumnReference _, Literal _, Placeholder _ -> {}
            case FunctionCall function -> {
                if (function.nameParts().size() != 1) {
                    throw unsupported(function.span(), "Qualified functions are outside the HogQL v0 profile");
                }
                if (canonical(function.name().value()).equals("matchesaction")) {
                    throw unsupported(function.span(), "Actions are outside the HogQL v0 profile");
                }
                function.arguments().forEach(argument -> validate(argument, snapshot));
                function.orderBy().forEach(sortItem -> validate(sortItem.expression(), snapshot));
                function.filter().ifPresent(filter -> validate(filter, snapshot));
                function.window().ifPresent(window -> validate(window, snapshot));
            }
            case InCohortExpression in -> throw unsupported(in.span(), "Cohorts are outside the HogQL v0 profile");
            case InExpression in -> {
                validate(in.value(), snapshot);
                in.values().forEach(value -> validate(value, snapshot));
            }
            case InSubqueryExpression in -> {
                validate(in.value(), snapshot);
                validate(in.query(), snapshot);
            }
            case IntervalExpression interval -> validate(interval.value(), snapshot);
            case IsNullExpression isNull -> validate(isNull.value(), snapshot);
            case MemberAccessExpression memberAccess -> validate(memberAccess.base(), snapshot);
            case ScalarSubqueryExpression subquery -> validate(subquery.query(), snapshot);
            case SubscriptExpression subscript -> {
                validate(subscript.base(), snapshot);
                validate(subscript.index(), snapshot);
            }
            case TupleExpression tuple -> tuple.values().forEach(value -> validate(value, snapshot));
            case UnaryExpression unary -> validate(unary.operand(), snapshot);
        }
    }

    private static void validate(Window window, Optional<HogQlSemanticCatalogSnapshot> snapshot)
    {
        switch (window) {
            case WindowReference _ -> {}
            case WindowSpecification specification -> {
                specification.partitionBy().forEach(expression -> validate(expression, snapshot));
                specification.orderBy().forEach(sortItem -> validate(sortItem.expression(), snapshot));
                specification.frame().ifPresent(frame -> {
                    frame.start().value().ifPresent(value -> validate(value, snapshot));
                    frame.end().flatMap(HogQlQuery.FrameBound::value).ifPresent(value -> validate(value, snapshot));
                });
            }
        }
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
}
