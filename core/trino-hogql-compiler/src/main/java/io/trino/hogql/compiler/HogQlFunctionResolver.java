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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionRewrite;
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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
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
import io.trino.hogql.parser.tree.HogQlQuery.LambdaExpression;
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
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;

final class HogQlFunctionResolver
{
    private static final String MATCHES_ACTION = "matchesaction";

    private final Map<String, FunctionCapabilityDefinition> functions;
    private final boolean semanticCatalogAvailable;

    private HogQlFunctionResolver(Optional<PinnedSnapshot> snapshot)
    {
        Map<String, FunctionCapabilityDefinition> functions = new LinkedHashMap<>(HogQlV0FunctionRegistry.functions());
        snapshot.stream()
                .flatMap(pinned -> pinned.snapshot().functions().stream())
                .forEach(function -> functions.putIfAbsent(canonical(function.name()), function));
        this.functions = Map.copyOf(functions);
        semanticCatalogAvailable = snapshot.isPresent();
    }

    public static HogQlQuery resolve(PinnedSnapshot snapshot, HogQlQuery query)
    {
        requireNonNull(snapshot, "snapshot is null");
        requireNonNull(query, "query is null");
        return new HogQlFunctionResolver(Optional.of(snapshot)).resolveQuery(query);
    }

    public static HogQlQuery resolve(HogQlQuery query)
    {
        requireNonNull(query, "query is null");
        return new HogQlFunctionResolver(Optional.empty()).resolveQuery(query);
    }

    public static HogQlQuery resolveV0(HogQlQuery query)
    {
        requireNonNull(query, "query is null");
        return new HogQlFunctionResolver(Optional.empty()).resolveQuery(query);
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
                    select.windows().stream().map(this::resolveWindowDefinition).toList(),
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
            case ColumnsList columns -> new ColumnsList(columns.expressions().stream().map(this::resolveExpression).toList(), columns.span());
            case ColumnsRegex columns -> columns;
            case ExpressionProjection expression -> new ExpressionProjection(resolveExpression(expression.expression()), expression.alias());
            case Star star -> new Star(
                    star.qualifier(),
                    star.exclusions(),
                    star.replacements().stream()
                            .map(replacement -> new StarReplacement(resolveExpression(replacement.expression()), replacement.target(), replacement.span()))
                            .toList(),
                    star.span());
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
            case PivotRelation pivot -> new PivotRelation(
                    resolveRelation(pivot.input()),
                    pivot.aggregations().stream()
                            .map(aggregation -> new PivotAggregation(
                                    resolveExpression(aggregation.expression()),
                                    aggregation.alias(),
                                    aggregation.span()))
                            .toList(),
                    pivot.pivotColumns().stream().map(this::resolveExpression).toList(),
                    pivot.valueGroups().stream()
                            .map(group -> new PivotValueGroup(
                                    group.values().stream().map(this::resolveExpression).toList(),
                                    group.alias(),
                                    group.span()))
                            .toList(),
                    pivot.groupBy().stream().map(this::resolveExpression).toList(),
                    pivot.span());
            case SubqueryRelation subquery -> new SubqueryRelation(resolveQuery(subquery.query()), subquery.span());
            case TablePlaceholder placeholder -> placeholder;
            case TableReference table -> table;
            case UnnestRelation unnest -> new UnnestRelation(
                    unnest.expressions().stream().map(this::resolveExpression).toList(),
                    unnest.alias(),
                    unnest.columnAliases(),
                    unnest.span());
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
            case CastExpression cast -> new CastExpression(resolveExpression(cast.value()), cast.type(), cast.safe(), cast.typeDialect(), cast.span());
            case ColumnReference reference -> reference;
            case FunctionCall function -> resolveFunction(function, function.window().isPresent());
            case InCohortExpression in -> new InCohortExpression(
                    resolveExpression(in.value()),
                    resolveExpression(in.cohort()),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InExpression in -> new InExpression(
                    resolveExpression(in.value()),
                    in.values().stream().map(this::resolveExpression).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case InSubqueryExpression in -> new InSubqueryExpression(
                    resolveExpression(in.value()),
                    resolveQuery(in.query()),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IntervalExpression interval -> new IntervalExpression(resolveExpression(interval.value()), interval.unit(), interval.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    resolveExpression(isNull.value()),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case LambdaExpression lambda -> new LambdaExpression(lambda.arguments(), resolveExpression(lambda.body()), lambda.span());
            case Literal literal -> literal;
            case MemberAccessExpression memberAccess -> new MemberAccessExpression(
                    resolveExpression(memberAccess.base()),
                    memberAccess.member(),
                    memberAccess.span());
            case Placeholder placeholder -> placeholder;
            case ScalarSubqueryExpression subquery -> new ScalarSubqueryExpression(resolveQuery(subquery.query()), subquery.span());
            case SubscriptExpression subscript -> new SubscriptExpression(
                    resolveExpression(subscript.base()),
                    resolveExpression(subscript.index()),
                    subscript.span());
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private Expression resolveFunction(FunctionCall function, boolean windowInvocation)
    {
        String name = function.name().value();
        if (function.nameParts().size() != 1) {
            throw unsupportedError(function, "Qualified HogQL functions are not supported");
        }
        if (semanticCatalogAvailable && function.nameParts().size() == 1 && canonical(name).equals(MATCHES_ACTION)) {
            return new FunctionCall(
                    function.nameParts(),
                    function.arguments().stream().map(this::resolveExpression).toList(),
                    function.distinct(),
                    resolveSortItems(function.orderBy()),
                    function.filter().map(this::resolveExpression),
                    function.nullTreatment(),
                    function.window().map(this::resolveWindow),
                    function.span());
        }
        FunctionCapabilityDefinition capability = functions.get(canonical(name));
        if (capability == null) {
            throw resolutionError(function, "Unknown HogQL function: " + name);
        }
        if (capability.implementation() == FunctionImplementation.REWRITE && function.nullTreatment().isPresent()) {
            throw unsupportedError(function, "HogQL function " + name + " does not support null treatment");
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
        List<Expression> arguments = function.arguments().stream().map(this::resolveExpression).toList();
        if (capability.implementation() == FunctionImplementation.REWRITE) {
            FunctionRewrite rewrite = capability.rewrite()
                    .orElseThrow(() -> unsupportedError(function, "HogQL function " + name + " has no compiler rewrite"));
            boolean negated = switch (rewrite) {
                case IS_NULL -> false;
                case IS_NOT_NULL -> true;
            };
            return new IsNullExpression(arguments.getFirst(), negated, function.span(), function.span());
        }
        return new FunctionCall(
                capability.trinoName().stream()
                        .map(identifier -> identifier(identifier, function.span()))
                        .toList(),
                arguments,
                function.distinct(),
                resolveSortItems(function.orderBy()),
                function.filter().map(this::resolveExpression),
                function.nullTreatment(),
                function.window().map(this::resolveWindow),
                function.span());
    }

    private WindowDefinition resolveWindowDefinition(WindowDefinition definition)
    {
        return new WindowDefinition(definition.name(), (WindowSpecification) resolveWindow(definition.specification()), definition.span());
    }

    private Window resolveWindow(Window window)
    {
        return switch (window) {
            case WindowReference reference -> reference;
            case WindowSpecification specification -> new WindowSpecification(
                    specification.partitionBy().stream().map(this::resolveExpression).toList(),
                    resolveSortItems(specification.orderBy()),
                    specification.frame().map(this::resolveWindowFrame),
                    specification.span());
        };
    }

    private WindowFrame resolveWindowFrame(WindowFrame frame)
    {
        return new WindowFrame(
                frame.type(),
                new HogQlQuery.FrameBound(
                        frame.start().type(),
                        frame.start().value().map(this::resolveExpression),
                        frame.start().span()),
                frame.end().map(bound -> new HogQlQuery.FrameBound(
                        bound.type(),
                        bound.value().map(this::resolveExpression),
                        bound.span())),
                frame.span());
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
