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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinUsing;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
import io.trino.hogql.parser.tree.HogQlQuery.SelectQueryBody;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.TablePlaceholder;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlFunctionResolver
{
    private final Map<String, FunctionCapabilityDefinition> functions;

    private HogQlFunctionResolver(PinnedSnapshot snapshot)
    {
        functions = snapshot.snapshot().functions().stream()
                .collect(Collectors.toUnmodifiableMap(function -> canonical(function.name()), Function.identity()));
    }

    public static HogQlQuery resolve(PinnedSnapshot snapshot, HogQlQuery query)
    {
        requireNonNull(snapshot, "snapshot is null");
        requireNonNull(query, "query is null");
        return new HogQlFunctionResolver(snapshot).resolveQuery(query);
    }

    private HogQlQuery resolveQuery(HogQlQuery query)
    {
        List<CommonTableExpression> commonTables = query.with().stream()
                .map(commonTable -> new CommonTableExpression(
                        commonTable.name(),
                        commonTable.columnAliases(),
                        resolveQuery(commonTable.query()),
                        commonTable.span()))
                .toList();
        HogQlQuery.QueryBody body = switch (query.body()) {
            case SelectQueryBody select -> new SelectQueryBody(
                    select.distinct(),
                    select.projections().stream().map(this::resolveProjection).toList(),
                    select.from().map(this::resolveRelation),
                    select.where().map(this::resolveExpression),
                    select.groupBy().stream().map(this::resolveExpression).toList(),
                    select.having().map(this::resolveExpression),
                    select.span());
            case SetOperation set -> new SetOperation(
                    set.type(),
                    set.distinct(),
                    resolveQuery(set.left()),
                    resolveQuery(set.right()),
                    set.leftParenthesized(),
                    set.rightParenthesized(),
                    set.operatorSpan(),
                    set.span());
        };
        return new HogQlQuery(
                commonTables,
                body,
                resolveSortItems(query.orderBy()),
                query.limit().map(this::resolveExpression),
                query.offset().map(this::resolveExpression),
                query.span());
    }

    private Projection resolveProjection(Projection projection)
    {
        return switch (projection) {
            case ExpressionProjection expression -> new ExpressionProjection(resolveExpression(expression.expression()), expression.alias());
            case Star star -> star;
        };
    }

    private Relation resolveRelation(Relation relation)
    {
        return switch (relation) {
            case AliasedRelation alias -> new AliasedRelation(resolveRelation(alias.relation()), alias.alias(), alias.columnAliases(), alias.span());
            case CommonTableReference commonTable -> commonTable;
            case JoinRelation join -> new JoinRelation(
                    join.type(),
                    resolveRelation(join.left()),
                    resolveRelation(join.right()),
                    join.criteria().map(criteria -> switch (criteria) {
                        case JoinOn on -> new JoinOn(resolveExpression(on.expression()), on.span());
                        case JoinUsing using -> using;
                    }),
                    join.span());
            case SubqueryRelation subquery -> new SubqueryRelation(resolveQuery(subquery.query()), subquery.span());
            case TablePlaceholder placeholder -> placeholder;
            case TableReference table -> table;
            case ValuesRelation values -> new ValuesRelation(
                    values.rows().stream()
                            .map(row -> row.stream().map(this::resolveExpression).toList())
                            .toList(),
                    values.span());
        };
    }

    private List<SortItem> resolveSortItems(List<SortItem> sortItems)
    {
        return sortItems.stream()
                .map(sortItem -> new SortItem(resolveExpression(sortItem.expression()), sortItem.direction(), sortItem.nullPlacement(), sortItem.span()))
                .toList();
    }

    private Expression resolveExpression(Expression expression)
    {
        return switch (expression) {
            case ArrayExpression array -> new ArrayExpression(array.values().stream().map(this::resolveExpression).toList(), array.span());
            case BetweenExpression between -> new BetweenExpression(
                    resolveExpression(between.value()),
                    resolveExpression(between.min()),
                    resolveExpression(between.max()),
                    between.negated(),
                    between.predicateSpan(),
                    between.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    resolveExpression(binary.left()),
                    resolveExpression(binary.right()),
                    binary.span());
            case CaseExpression caseExpression -> new CaseExpression(
                    caseExpression.operand().map(this::resolveExpression),
                    caseExpression.whenClauses().stream()
                            .map(when -> new CaseWhen(resolveExpression(when.operand()), resolveExpression(when.result()), when.span()))
                            .toList(),
                    caseExpression.defaultValue().map(this::resolveExpression),
                    caseExpression.span());
            case CastExpression cast -> new CastExpression(resolveExpression(cast.value()), cast.type(), cast.safe(), cast.span());
            case ColumnReference reference -> reference;
            case FunctionCall function -> resolveFunction(function, false);
            case InExpression in -> new InExpression(
                    resolveExpression(in.value()),
                    in.values().stream().map(this::resolveExpression).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    resolveExpression(isNull.value()),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case Literal literal -> literal;
            case Placeholder placeholder -> placeholder;
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private FunctionCall resolveFunction(FunctionCall function, boolean windowInvocation)
    {
        String name = function.name().value();
        FunctionCapabilityDefinition capability = functions.get(canonical(name));
        if (capability == null) {
            throw resolutionError(function, "Unknown HogQL function: " + name);
        }
        if (capability.implementation() == FunctionImplementation.REWRITE) {
            throw unsupportedError(function, "HogQL function " + name + " requires a compiler rewrite");
        }
        if (capability.kind() == FunctionKind.TABLE) {
            throw unsupportedError(function, "HogQL table function " + name + " cannot be used as an expression");
        }
        if (windowInvocation && !capability.supportsWindow()) {
            throw unsupportedError(function, "HogQL function " + name + " does not support OVER");
        }
        if (!windowInvocation && capability.kind() == FunctionKind.WINDOW) {
            throw unsupportedError(function, "HogQL window function " + name + " requires an OVER clause");
        }
        if (!matchesArity(capability.signatures(), function.arguments().size())) {
            throw resolutionError(function, "HogQL function " + name + " does not accept " + function.arguments().size() + " arguments");
        }
        if (function.distinct() && !capability.supportsDistinct()) {
            throw unsupportedError(function, "HogQL function " + name + " does not support DISTINCT");
        }
        if (!function.orderBy().isEmpty() && !capability.supportsOrderBy()) {
            throw unsupportedError(function, "HogQL function " + name + " does not support ORDER BY");
        }
        if (function.filter().isPresent() && !capability.supportsFilter()) {
            throw unsupportedError(function, "HogQL function " + name + " does not support FILTER");
        }
        return new FunctionCall(
                capability.trinoName().stream()
                        .map(identifier -> identifier(identifier, function.span()))
                        .toList(),
                function.arguments().stream().map(this::resolveExpression).toList(),
                function.distinct(),
                resolveSortItems(function.orderBy()),
                function.filter().map(this::resolveExpression),
                function.span());
    }

    private static boolean matchesArity(List<FunctionSignature> signatures, int arity)
    {
        return signatures.stream().anyMatch(signature -> signature.variadic()
                ? arity >= Math.max(0, signature.argumentTypes().size() - 1)
                : arity == signature.argumentTypes().size());
    }

    private static Identifier identifier(PhysicalIdentifier identifier, HogQlQuery.SourceSpan span)
    {
        return new Identifier(identifier.value(), identifier.delimited(), span);
    }

    private static String canonical(String value)
    {
        return value.toLowerCase(Locale.ENGLISH);
    }

    private static TrinoException resolutionError(FunctionCall function, String message)
    {
        return error(HOGQL_RESOLUTION_ERROR, function, message);
    }

    private static TrinoException unsupportedError(FunctionCall function, String message)
    {
        return error(HOGQL_UNSUPPORTED_FEATURE, function, message);
    }

    private static TrinoException error(HogQlErrorCode errorCode, FunctionCall function, String message)
    {
        return new TrinoException(
                errorCode,
                Optional.of(new Location(function.span().startLine(), function.span().startColumn())),
                message,
                null);
    }
}
