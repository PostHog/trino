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

import java.util.ArrayList;
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
    private ArrayList<UnnestRelation> activeArrayJoins;

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
        if (query.body() instanceof SelectQueryBody select) {
            return resolveSelectQuery(query, commonTables, select);
        }
        SetOperation set = (SetOperation) query.body();
        return new HogQlQuery(
                commonTables,
                new SetOperation(
                    set.type(),
                    set.distinct(),
                    resolveQuery(set.left()),
                    resolveQuery(set.right()),
                    set.leftParenthesized(),
                    set.rightParenthesized(),
                    set.operatorSpan(),
                    set.span()),
                resolveSortItems(query.orderBy()),
                query.limit().map(this::resolveExpression),
                query.offset().map(this::resolveExpression),
                query.span());
    }

    private HogQlQuery resolveSelectQuery(HogQlQuery query, List<CommonTableExpression> commonTables, SelectQueryBody select)
    {
        ArrayList<UnnestRelation> parentArrayJoins = activeArrayJoins;
        activeArrayJoins = new ArrayList<>();
        try {
            Optional<Relation> from = select.from().map(this::resolveRelation);
            List<Projection> projections = select.projections().stream().map(this::resolveProjection).toList();
            Optional<Expression> where = select.where().map(this::resolveExpression);
            List<Expression> groupBy = select.groupBy().stream().map(this::resolveExpression).toList();
            Optional<Expression> having = select.having().map(this::resolveExpression);
            List<WindowDefinition> windows = select.windows().stream().map(this::resolveWindowDefinition).toList();
            List<SortItem> orderBy = resolveSortItems(query.orderBy());
            Optional<Expression> limit = query.limit().map(this::resolveExpression);
            Optional<Expression> offset = query.offset().map(this::resolveExpression);
            for (UnnestRelation unnest : activeArrayJoins) {
                from = Optional.of(from
                        .<Relation>map(left -> new JoinRelation(HogQlQuery.JoinType.CROSS, left, unnest, Optional.empty(), unnest.span()))
                        .orElse(unnest));
            }
            return new HogQlQuery(
                    commonTables,
                    new SelectQueryBody(select.distinct(), projections, from, where, groupBy, having, windows, select.span()),
                    orderBy,
                    limit,
                    offset,
                    query.span());
        }
        finally {
            activeArrayJoins = parentArrayJoins;
        }
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
        if (canonical(name).equals("arrayjoin")) {
            return resolveArrayJoin(function);
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
            return rewrite(function, rewrite, arguments);
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

    private Expression resolveArrayJoin(FunctionCall function)
    {
        if (function.arguments().size() != 1) {
            throw resolutionError(function, "HogQL function " + function.name().value() + " does not accept " + function.arguments().size() + " arguments");
        }
        if (function.distinct() || !function.orderBy().isEmpty() || function.filter().isPresent() ||
                function.nullTreatment().isPresent() || function.window().isPresent()) {
            throw unsupportedError(function, "HogQL arrayJoin does not accept function modifiers");
        }
        if (activeArrayJoins == null) {
            throw unsupportedError(function, "HogQL arrayJoin requires a SELECT query context");
        }
        int index = activeArrayJoins.size();
        Identifier alias = new Identifier("__hogql_array_join_" + index, false, function.span());
        Identifier column = new Identifier("__hogql_value_" + index, false, function.span());
        activeArrayJoins.add(new UnnestRelation(
                List.of(resolveExpression(function.arguments().getFirst())),
                alias,
                List.of(column),
                function.span()));
        return new ColumnReference(List.of(alias, column), function.span());
    }

    private Expression rewrite(FunctionCall function, FunctionRewrite rewrite, List<Expression> arguments)
    {
        HogQlQuery.SourceSpan span = function.span();
        return switch (rewrite) {
            case IS_NULL -> new IsNullExpression(arguments.getFirst(), false, span, span);
            case IS_NOT_NULL -> new IsNullExpression(arguments.getFirst(), true, span, span);
            case CAST_DATE -> cast(arguments.getFirst(), "date", span);
            case CAST_DOUBLE -> cast(arguments.getFirst(), "double", span);
            case CAST_SMALLINT -> cast(arguments.getFirst(), "smallint", span);
            case FLOAT_OR_ZERO -> coalesce(tryCast(arguments.getFirst(), "double", span), new Literal(HogQlQuery.LiteralKind.FLOAT, "0.0", span), span);
            case FLOAT_OR_DEFAULT -> coalesce(tryCast(arguments.getFirst(), "double", span), cast(arguments.get(1), "double", span), span);
            case DECIMAL_CAST -> tryCast(arguments.getFirst(), decimalType(function, arguments.get(1)), span);
            case INT_DIV -> intDiv(arguments, span);
            case ARRAY_ELEMENT -> call("element_at", arguments, span);
            case ARRAY_FILTER -> call("filter", List.of(arguments.get(1), arguments.getFirst()), span);
            case ARRAY_FIRST -> call(
                    "element_at",
                    List.of(call("filter", List.of(arguments.get(1), arguments.getFirst()), span), integerLiteral("1", span)),
                    span);
            case ARRAY_MAP -> call("transform", List.of(arguments.get(1), arguments.getFirst()), span);
            case ARRAY_SUM -> arraySum(arguments.getFirst(), span);
            case RANGE -> range(arguments, span);
            case TUPLE_ELEMENT -> new SubscriptExpression(arguments.getFirst(), arguments.get(1), span);
            case SPLIT_CHAR -> call("split", List.of(arguments.get(1), arguments.getFirst()), span);
            case HAS -> has(arguments, span);
            case ASSUME_NOT_NULL -> arguments.getFirst();
            case EMPTY -> empty(arguments.getFirst(), true, span);
            case NOT_EMPTY -> empty(arguments.getFirst(), false, span);
            case EQUALS -> new BinaryExpression(HogQlQuery.BinaryOperator.EQUAL, arguments.getFirst(), arguments.get(1), span);
            case PLUS -> new BinaryExpression(HogQlQuery.BinaryOperator.ADD, arguments.getFirst(), arguments.get(1), span);
            case MINUS -> new BinaryExpression(HogQlQuery.BinaryOperator.SUBTRACT, arguments.getFirst(), arguments.get(1), span);
            case NOT_EQUALS -> new BinaryExpression(HogQlQuery.BinaryOperator.NOT_EQUAL, arguments.getFirst(), arguments.get(1), span);
            case MULTIPLY, MULTIPLY_DECIMAL -> new BinaryExpression(HogQlQuery.BinaryOperator.MULTIPLY, arguments.getFirst(), arguments.get(1), span);
            case DIVIDE_DECIMAL -> new BinaryExpression(HogQlQuery.BinaryOperator.DIVIDE, arguments.getFirst(), arguments.get(1), span);
            case IN_ARRAY -> call("contains", List.of(arguments.get(1), arguments.getFirst()), span);
            case TUPLE -> new TupleExpression(arguments, span);
            case SUBTRACT_MONTHS -> dateAdd(
                    "month",
                    List.of(arguments.getFirst(), new UnaryExpression(HogQlQuery.UnaryOperator.NEGATE, arguments.get(1), span)),
                    span);
            case INTERVAL_MONTH -> new IntervalExpression(arguments.getFirst(), HogQlQuery.IntervalUnit.MONTH, span);
            case START_WEEK -> startOfWeek(arguments.getFirst(), span);
            case SPLIT_STRING -> call("split", List.of(arguments.get(1), arguments.getFirst()), span);
            case CAST_BIGINT -> cast(arguments.getFirst(), "bigint", span);
            case CAST_TIMESTAMP -> arguments.size() == 1
                    ? cast(arguments.getFirst(), "timestamp(0)", span)
                    : call("with_timezone", List.of(cast(arguments.getFirst(), "timestamp(0)", span), arguments.get(1)), span);
            case CAST_VARCHAR -> cast(arguments.getFirst(), "varchar", span);
            case DATE_TRUNC_DAY -> dateTrunc("day", arguments.getFirst(), span);
            case DATE_TRUNC_HOUR -> dateTrunc("hour", arguments.getFirst(), span);
            case DATE_TRUNC_MONTH -> dateTrunc("month", arguments.getFirst(), span);
            case DATE_TRUNC_WEEK -> dateTrunc("week", arguments.getFirst(), span);
            case COUNT_IF -> aggregate(function, "count", List.of(), false, arguments.getFirst(), span);
            case SUM_IF -> aggregate(function, "sum", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case MAX_IF -> aggregate(function, "max", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case UNIQ_IF -> aggregate(function, "approx_distinct", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case UNIQ_EXACT -> aggregate(function, "count", List.of(arguments.getFirst()), true, null, span);
            case GROUP_UNIQ_ARRAY -> aggregate(function, "array_agg", List.of(arguments.getFirst()), true, null, span);
            case ARG_MAX_IF -> aggregate(function, "max_by", List.of(arguments.getFirst(), arguments.get(1)), false, arguments.get(2), span);
            case ARG_MIN_IF -> aggregate(function, "min_by", List.of(arguments.getFirst(), arguments.get(1)), false, arguments.get(2), span);
            case ANY_IF -> aggregate(function, "arbitrary", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case MIN_IF -> aggregate(function, "min", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case AVG_IF -> aggregate(function, "avg", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case GROUP_ARRAY_IF -> arguments.size() == 2
                    ? aggregate(function, "array_agg", List.of(arguments.getFirst()), false, arguments.get(1), span)
                    : call(
                            "slice",
                            List.of(
                                    aggregate(function, "array_agg", List.of(arguments.getFirst()), false, arguments.get(1), span),
                                    integerLiteral("1", span),
                                    arguments.get(2)),
                            span);
            case UNIQ_EXACT_IF -> aggregate(function, "count", List.of(arguments.getFirst()), true, arguments.get(1), span);
            case GROUP_UNIQ_ARRAY_IF -> aggregate(function, "array_agg", List.of(arguments.getFirst()), true, arguments.get(1), span);
            case COUNT_DISTINCT -> aggregate(function, "count", List.of(arguments.getFirst()), true, null, span);
            case MULTI_IF -> multiIf(function, arguments);
            case JSON_EXTRACT_STRING -> jsonExtractScalar(function, arguments, "varchar", new Literal(HogQlQuery.LiteralKind.STRING, "", span));
            case JSON_EXTRACT_INT -> jsonExtractScalar(function, arguments, "bigint", new Literal(HogQlQuery.LiteralKind.INTEGER, "0", span));
            case JSON_EXTRACT_FLOAT -> jsonExtractScalar(function, arguments, "double", new Literal(HogQlQuery.LiteralKind.FLOAT, "0.0", span));
            case JSON_EXTRACT_BOOL -> jsonExtractScalar(function, arguments, "boolean", new Literal(HogQlQuery.LiteralKind.BOOLEAN, "false", span));
            case JSON_EXTRACT_UINT -> jsonExtractScalar(function, arguments, "bigint", integerLiteral("0", span));
            case JSON_EXTRACT_ARRAY_RAW -> jsonExtractArrayRaw(function, arguments);
            case JSON_EXTRACT_KEYS -> call(
                    "map_keys",
                    List.of(typedJsonMapFromJson(jsonValue(function, arguments), "json", span)),
                    span);
            case JSON_EXTRACT_RAW -> coalesce(
                    call("json_format", List.of(call("json_extract", List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))), span)), span),
                    new Literal(HogQlQuery.LiteralKind.STRING, "", span),
                    span);
            case JSON_EXTRACT_TYPED -> jsonExtractTyped(function, arguments);
            case JSON_KEYS_AND_VALUES -> jsonExtractKeysAndValues(function, arguments);
            case JSON_KEYS_AND_VALUES_RAW -> jsonExtractKeysAndValuesRaw(function, arguments);
            case JSON_LENGTH -> coalesce(
                    call("json_size", List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))), span),
                    new Literal(HogQlQuery.LiteralKind.INTEGER, "0", span),
                    span);
            case TODAY -> cast(call("now", List.of(), span), "date", span);
            case INTERVAL_DAY -> new IntervalExpression(arguments.getFirst(), HogQlQuery.IntervalUnit.DAY, span);
            case ADD_DAYS -> dateAdd("day", arguments, span);
            case ADD_MONTHS -> dateAdd("month", arguments, span);
            case DATE_ADD -> arguments.size() == 2
                    ? new BinaryExpression(HogQlQuery.BinaryOperator.ADD, arguments.getFirst(), arguments.get(1), span)
                    : call("date_add", arguments, span);
            case TO_UNIX_TIMESTAMP -> cast(call("to_unixtime", arguments, span), "bigint", span);
            case PARSE_TIMESTAMP -> new CastExpression(arguments.getFirst(), new Identifier("timestamp(3)", false, span), true, span);
            case NOT -> new UnaryExpression(HogQlQuery.UnaryOperator.NOT, arguments.getFirst(), span);
            case AND -> and(arguments, span);
            case GREATER -> new BinaryExpression(HogQlQuery.BinaryOperator.GREATER_THAN, arguments.getFirst(), arguments.get(1), span);
            case GREATER_OR_EQUAL -> new BinaryExpression(HogQlQuery.BinaryOperator.GREATER_THAN_OR_EQUAL, arguments.getFirst(), arguments.get(1), span);
            case LESS_OR_EQUAL -> new BinaryExpression(HogQlQuery.BinaryOperator.LESS_THAN_OR_EQUAL, arguments.getFirst(), arguments.get(1), span);
            case LIKE -> new BinaryExpression(HogQlQuery.BinaryOperator.LIKE, arguments.getFirst(), arguments.get(1), span);
            case REGEX_EXTRACT -> regexExtract(function, arguments);
            case REGEX_EXTRACT_ALL -> regexExtractAll(function, arguments);
            case REGEX_REPLACE_ALL -> regexReplaceAll(function, arguments);
            case REGEX_REPLACE_ONE -> regexReplaceOne(function, arguments);
            case ARRAY_SLICE -> call("slice", arguments, span);
            case ARRAY_SORT -> arraySort(function, arguments);
            case ARRAY_ENUMERATE -> arrayEnumerate(arguments.getFirst(), span);
            case SUBTRACT_YEARS -> dateAdd(
                    "year",
                    List.of(arguments.getFirst(), new UnaryExpression(HogQlQuery.UnaryOperator.NEGATE, arguments.get(1), span)),
                    span);
            case INT_OR_ZERO -> coalesce(tryCast(arguments.getFirst(), "bigint", span), integerLiteral("0", span), span);
            case CAST_UUID -> tryCast(arguments.getFirst(), "uuid", span);
            case TO_JSON_STRING -> call("json_format", List.of(cast(arguments.getFirst(), "json", span)), span);
            case JSON_HAS -> new IsNullExpression(
                    call("json_extract", List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))), span),
                    true,
                    span,
                    span);
            case JSON_VALUE -> call("json_extract_scalar", arguments, span);
            case SURVEY_RESPONSE -> surveyResponse(function, arguments);
            case MD5 -> call("md5", List.of(call("to_utf8", List.of(cast(arguments.getFirst(), "varchar", span)), span)), span);
            case MEDIAN_IF -> aggregate(
                    function,
                    "approx_percentile",
                    List.of(arguments.getFirst(), new Literal(HogQlQuery.LiteralKind.FLOAT, "0.5", span)),
                    false,
                    arguments.get(1),
                    span);
            case QUANTILE, QUANTILE_EXACT -> aggregate(function, "approx_percentile", arguments, false, null, span);
            case QUANTILE_IF -> aggregate(
                    function,
                    "approx_percentile",
                    List.of(arguments.getFirst(), arguments.get(2)),
                    false,
                    arguments.get(1),
                    span);
            case DATE_PART -> datePart(function, arguments);
        };
    }

    private static Expression datePart(FunctionCall function, List<Expression> arguments)
    {
        Literal unit = stringLiteral(function, arguments.getFirst(), "date part");
        String trinoFunction = switch (unit.value().toLowerCase(Locale.ENGLISH)) {
            case "year" -> "year";
            case "quarter" -> "quarter";
            case "month" -> "month";
            case "week" -> "week";
            case "day" -> "day";
            case "dow", "dayofweek" -> "day_of_week";
            case "doy", "dayofyear" -> "day_of_year";
            case "hour" -> "hour";
            case "minute" -> "minute";
            case "second" -> "second";
            default -> throw unsupportedError(function, "Unsupported HogQL date part: " + unit.value());
        };
        return call(trinoFunction, List.of(arguments.get(1)), function.span());
    }

    private static Expression and(List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        Expression result = arguments.getFirst();
        for (int index = 1; index < arguments.size(); index++) {
            result = new BinaryExpression(HogQlQuery.BinaryOperator.AND, result, arguments.get(index), span);
        }
        return result;
    }

    private static Expression regexExtract(FunctionCall function, List<Expression> arguments)
    {
        Literal pattern = stringLiteral(function, arguments.get(1), "regular expression");
        Literal group = new Literal(
                HogQlQuery.LiteralKind.INTEGER,
                hasCapturingGroup(pattern.value()) ? "1" : "0",
                function.span());
        Expression extracted = call("regexp_extract", List.of(arguments.getFirst(), pattern, group), function.span());
        return coalesce(extracted, new Literal(HogQlQuery.LiteralKind.STRING, "", function.span()), function.span());
    }

    private static Expression regexExtractAll(FunctionCall function, List<Expression> arguments)
    {
        Literal pattern = stringLiteral(function, arguments.get(1), "regular expression");
        Literal group = integerLiteral(hasCapturingGroup(pattern.value()) ? "1" : "0", function.span());
        return call("regexp_extract_all", List.of(arguments.getFirst(), pattern, group), function.span());
    }

    private static Expression regexReplaceAll(FunctionCall function, List<Expression> arguments)
    {
        Literal pattern = stringLiteral(function, arguments.get(1), "regular expression");
        Literal replacement = stringLiteral(function, arguments.get(2), "regular expression replacement");
        String trinoReplacement = regexReplacement(replacement.value(), 0);
        return call(
                "regexp_replace",
                List.of(arguments.getFirst(), pattern, new Literal(HogQlQuery.LiteralKind.STRING, trinoReplacement, replacement.span())),
                function.span());
    }

    private static Expression regexReplaceOne(FunctionCall function, List<Expression> arguments)
    {
        Literal pattern = stringLiteral(function, arguments.get(1), "regular expression");
        Literal replacement = stringLiteral(function, arguments.get(2), "regular expression replacement");
        Literal firstPattern = new Literal(
                HogQlQuery.LiteralKind.STRING,
                "(?s)^(.*?)(" + pattern.value() + ")",
                pattern.span());
        Literal firstReplacement = new Literal(
                HogQlQuery.LiteralKind.STRING,
                "$1" + regexReplacement(replacement.value(), 2),
                replacement.span());
        return call("regexp_replace", List.of(arguments.getFirst(), firstPattern, firstReplacement), function.span());
    }

    private static String regexReplacement(String replacement, int groupOffset)
    {
        StringBuilder result = new StringBuilder(replacement.length());
        for (int index = 0; index < replacement.length(); index++) {
            char current = replacement.charAt(index);
            if (current == '\\' && index + 1 < replacement.length() && Character.isDigit(replacement.charAt(index + 1))) {
                result.append('$').append(Character.digit(replacement.charAt(++index), 10) + groupOffset);
            }
            else {
                result.append(current);
            }
        }
        return result.toString();
    }

    private static Literal stringLiteral(FunctionCall function, Expression expression, String description)
    {
        if (expression instanceof Literal literal && literal.kind() == HogQlQuery.LiteralKind.STRING) {
            return literal;
        }
        throw unsupportedError(function, "HogQL " + description + " must be a string literal");
    }

    private static boolean hasCapturingGroup(String pattern)
    {
        boolean escaped = false;
        boolean characterClass = false;
        for (int index = 0; index < pattern.length(); index++) {
            char current = pattern.charAt(index);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == '[') {
                characterClass = true;
                continue;
            }
            if (current == ']' && characterClass) {
                characterClass = false;
                continue;
            }
            if (current != '(' || characterClass) {
                continue;
            }
            if (index + 1 >= pattern.length() || pattern.charAt(index + 1) != '?') {
                return true;
            }
            if (index + 2 < pattern.length() && pattern.charAt(index + 2) == '<' &&
                    (index + 3 >= pattern.length() || (pattern.charAt(index + 3) != '=' && pattern.charAt(index + 3) != '!'))) {
                return true;
            }
        }
        return false;
    }

    private static FunctionCall dateAdd(String unit, List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        return call(
                "date_add",
                List.of(new Literal(HogQlQuery.LiteralKind.STRING, unit, span), arguments.get(1), arguments.getFirst()),
                span);
    }

    private FunctionCall aggregate(FunctionCall source, String name, List<Expression> arguments, boolean distinct, Expression filter, HogQlQuery.SourceSpan span)
    {
        return new FunctionCall(
                List.of(new Identifier(name, false, span)),
                arguments,
                distinct,
                List.of(),
                Optional.ofNullable(filter),
                Optional.empty(),
                source.window().map(this::resolveWindow),
                span);
    }

    private static CaseExpression multiIf(FunctionCall function, List<Expression> arguments)
    {
        if (arguments.size() % 2 == 0) {
            throw resolutionError(function, "HogQL function " + function.name().value() + " requires condition/result pairs followed by a default value");
        }
        List<CaseWhen> clauses = new ArrayList<>((arguments.size() - 1) / 2);
        for (int index = 0; index < arguments.size() - 1; index += 2) {
            clauses.add(new CaseWhen(arguments.get(index), arguments.get(index + 1), function.span()));
        }
        return new CaseExpression(Optional.empty(), clauses, Optional.of(arguments.getLast()), function.span());
    }

    private static Expression jsonExtractScalar(FunctionCall function, List<Expression> arguments, String type, Literal defaultValue)
    {
        HogQlQuery.SourceSpan span = function.span();
        Expression extracted = call(
                "json_extract_scalar",
                List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))),
                span);
        if (!type.equals("varchar")) {
            extracted = new CastExpression(extracted, new Identifier(type, false, span), true, span);
        }
        return coalesce(extracted, defaultValue, span);
    }

    private static Expression surveyResponse(FunctionCall function, List<Expression> arguments)
    {
        HogQlQuery.SourceSpan span = function.span();
        int questionIndex = surveyQuestionIndex(function, arguments.getFirst());
        if (!(arguments.get(1) instanceof Literal questionId) || questionId.kind() != HogQlQuery.LiteralKind.STRING || questionId.value().isEmpty()) {
            throw unsupportedError(function, "HogQL getSurveyResponse question ID must be a non-empty string literal");
        }
        Expression properties = new ColumnReference(List.of(new Identifier("properties", false, span)), span);
        Literal empty = new Literal(HogQlQuery.LiteralKind.STRING, "", span);
        Expression idResponse = call(
                "nullif",
                List.of(jsonExtractScalar(function, List.of(properties, stringLiteral("$survey_response_" + questionId.value(), span)), "varchar", empty), empty),
                span);
        String indexKey = questionIndex == 0 ? "$survey_response" : "$survey_response_" + questionIndex;
        Expression indexResponse = call(
                "nullif",
                List.of(jsonExtractScalar(function, List.of(properties, stringLiteral(indexKey, span)), "varchar", empty), empty),
                span);
        return call("coalesce", List.of(idResponse, indexResponse), span);
    }

    private static int surveyQuestionIndex(FunctionCall function, Expression expression)
    {
        if (!(expression instanceof Literal literal) ||
                (literal.kind() != HogQlQuery.LiteralKind.INTEGER && literal.kind() != HogQlQuery.LiteralKind.STRING)) {
            throw unsupportedError(function, "HogQL getSurveyResponse question index must be an integer literal");
        }
        try {
            return Integer.parseInt(literal.value());
        }
        catch (NumberFormatException _) {
            throw resolutionError(function, "HogQL getSurveyResponse question index is outside the supported range");
        }
    }

    private static Expression jsonExtractTyped(FunctionCall function, List<Expression> arguments)
    {
        String type = jsonType(function, arguments.get(1));
        return switch (type) {
            case "Map(String, Float64)" -> typedJsonMap(arguments.getFirst(), "double", function.span());
            default -> throw unsupportedError(function, "Unsupported HogQL JSON extraction type: " + type);
        };
    }

    private static Expression jsonExtractKeysAndValues(FunctionCall function, List<Expression> arguments)
    {
        String type = jsonType(function, arguments.get(1));
        if (!type.equals("Float64")) {
            throw unsupportedError(function, "Unsupported HogQL JSON key/value type: " + type);
        }
        return call("map_entries", List.of(typedJsonMap(arguments.getFirst(), "double", function.span())), function.span());
    }

    private static Expression jsonExtractKeysAndValuesRaw(FunctionCall function, List<Expression> arguments)
    {
        HogQlQuery.SourceSpan span = function.span();
        Identifier key = new Identifier("key", false, span);
        Identifier item = new Identifier("value", false, span);
        LambdaExpression formatValue = new LambdaExpression(
                List.of(key, item),
                call("json_format", List.of(new ColumnReference(List.of(item), span)), span),
                span);
        Expression formatted = call(
                "transform_values",
                List.of(typedJsonMapFromJson(jsonValue(function, arguments), "json", span), formatValue),
                span);
        return call("map_entries", List.of(formatted), span);
    }

    private static Expression jsonExtractArrayRaw(FunctionCall function, List<Expression> arguments)
    {
        HogQlQuery.SourceSpan span = function.span();
        String type = "array(json)";
        Expression converted = tryCast(jsonValue(function, arguments), type, span);
        Expression empty = cast(new ArrayExpression(List.of(), span), type, span);
        Expression values = coalesce(converted, empty, span);
        Identifier item = new Identifier("_hogql_json_item", false, span);
        LambdaExpression format = new LambdaExpression(
                List.of(item),
                call("json_format", List.of(new ColumnReference(List.of(item), span)), span),
                span);
        return call("transform", List.of(values, format), span);
    }

    private static Expression typedJsonMap(Expression value, String valueType, HogQlQuery.SourceSpan span)
    {
        return typedJsonMapFromJson(call("json_parse", List.of(value), span), valueType, span);
    }

    private static Expression typedJsonMapFromJson(Expression parsed, String valueType, HogQlQuery.SourceSpan span)
    {
        String type = "map(varchar," + valueType + ")";
        Expression converted = new CastExpression(parsed, new Identifier(type, false, span), true, span);
        Expression empty = new CastExpression(
                call("map", List.of(new ArrayExpression(List.of(), span), new ArrayExpression(List.of(), span)), span),
                new Identifier(type, false, span),
                false,
                span);
        return coalesce(converted, empty, span);
    }

    private static Expression jsonValue(FunctionCall function, List<Expression> arguments)
    {
        if (arguments.size() == 1) {
            return call("json_parse", List.of(arguments.getFirst()), function.span());
        }
        return call(
                "json_extract",
                List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))),
                function.span());
    }

    private static String jsonType(FunctionCall function, Expression expression)
    {
        if (expression instanceof Literal literal && literal.kind() == HogQlQuery.LiteralKind.STRING) {
            return literal.value();
        }
        throw unsupportedError(function, "HogQL JSON extraction type must be a string literal");
    }

    private static Expression jsonPath(FunctionCall function, List<Expression> segments)
    {
        StringBuilder path = new StringBuilder("$");
        List<Expression> pathParts = new ArrayList<>();
        for (Expression segment : segments) {
            if (!(segment instanceof Literal literal)) {
                pathParts.add(stringLiteral(path.toString(), function.span()));
                path.setLength(0);
                pathParts.add(stringLiteral("[", function.span()));
                pathParts.add(call("json_format", List.of(cast(segment, "json", function.span())), function.span()));
                pathParts.add(stringLiteral("]", function.span()));
                continue;
            }
            switch (literal.kind()) {
                case STRING -> path.append("[\"")
                        .append(literal.value().replace("\\", "\\\\").replace("\"", "\\\""))
                        .append("\"]");
                case INTEGER -> path.append('[').append(literal.value()).append(']');
                default -> throw unsupportedError(function, "HogQL JSON path segments must be string or integer literals");
            }
        }
        if (pathParts.isEmpty()) {
            return stringLiteral(path.toString(), function.span());
        }
        if (!path.isEmpty()) {
            pathParts.add(stringLiteral(path.toString(), function.span()));
        }
        return call("concat", pathParts, function.span());
    }

    private static FunctionCall coalesce(Expression value, Expression defaultValue, HogQlQuery.SourceSpan span)
    {
        return call("coalesce", List.of(value, defaultValue), span);
    }

    private static FunctionCall call(String name, List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        return new FunctionCall(
                new Identifier(name, false, span),
                arguments,
                false,
                List.of(),
                Optional.empty(),
                span);
    }

    private static CastExpression cast(Expression value, String type, HogQlQuery.SourceSpan span)
    {
        return new CastExpression(value, new Identifier(type, false, span), false, span);
    }

    private static CastExpression tryCast(Expression value, String type, HogQlQuery.SourceSpan span)
    {
        return new CastExpression(value, new Identifier(type, false, span), true, span);
    }

    private static String decimalType(FunctionCall function, Expression scaleExpression)
    {
        if (!(scaleExpression instanceof Literal literal) || literal.kind() != HogQlQuery.LiteralKind.INTEGER) {
            throw unsupportedError(function, "HogQL decimal scale must be an integer literal");
        }
        int scale;
        try {
            scale = Integer.parseInt(literal.value());
        }
        catch (NumberFormatException _) {
            throw resolutionError(function, "HogQL decimal scale is outside the supported range");
        }
        if (scale < 0 || scale > 18) {
            throw resolutionError(function, "HogQL Decimal64 scale must be between 0 and 18");
        }
        return "decimal(18," + scale + ")";
    }

    private static Expression intDiv(List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        Expression dividend = cast(arguments.getFirst(), "bigint", span);
        Expression divisor = cast(arguments.get(1), "bigint", span);
        Literal zero = new Literal(HogQlQuery.LiteralKind.INTEGER, "0", span);
        Literal one = new Literal(HogQlQuery.LiteralKind.INTEGER, "1", span);
        Expression hasRemainder = new BinaryExpression(
                HogQlQuery.BinaryOperator.NOT_EQUAL,
                new BinaryExpression(HogQlQuery.BinaryOperator.MODULO, dividend, divisor, span),
                zero,
                span);
        Expression signsDiffer = new BinaryExpression(
                HogQlQuery.BinaryOperator.OR,
                new BinaryExpression(
                        HogQlQuery.BinaryOperator.AND,
                        new BinaryExpression(HogQlQuery.BinaryOperator.LESS_THAN, dividend, zero, span),
                        new BinaryExpression(HogQlQuery.BinaryOperator.GREATER_THAN, divisor, zero, span),
                        span),
                new BinaryExpression(
                        HogQlQuery.BinaryOperator.AND,
                        new BinaryExpression(HogQlQuery.BinaryOperator.GREATER_THAN, dividend, zero, span),
                        new BinaryExpression(HogQlQuery.BinaryOperator.LESS_THAN, divisor, zero, span),
                        span),
                span);
        Expression adjustment = call(
                "if",
                List.of(new BinaryExpression(HogQlQuery.BinaryOperator.AND, hasRemainder, signsDiffer, span), one, zero),
                span);
        return new BinaryExpression(
                HogQlQuery.BinaryOperator.SUBTRACT,
                new BinaryExpression(HogQlQuery.BinaryOperator.DIVIDE, dividend, divisor, span),
                adjustment,
                span);
    }

    private static Expression arraySum(Expression array, HogQlQuery.SourceSpan span)
    {
        Identifier sum = new Identifier("_hogql_sum", false, span);
        Identifier item = new Identifier("_hogql_item", false, span);
        Expression sumReference = new ColumnReference(List.of(sum), span);
        LambdaExpression add = new LambdaExpression(
                List.of(sum, item),
                new BinaryExpression(
                        HogQlQuery.BinaryOperator.ADD,
                        sumReference,
                        new ColumnReference(List.of(item), span),
                        span),
                span);
        LambdaExpression finish = new LambdaExpression(List.of(sum), sumReference, span);
        return call("reduce", List.of(array, integerLiteral("0", span), add, finish), span);
    }

    private static Literal integerLiteral(String value, HogQlQuery.SourceSpan span)
    {
        return new Literal(HogQlQuery.LiteralKind.INTEGER, value, span);
    }

    private static Literal stringLiteral(String value, HogQlQuery.SourceSpan span)
    {
        return new Literal(HogQlQuery.LiteralKind.STRING, value, span);
    }

    private static Expression range(List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        Expression start = arguments.size() == 1 ? integerLiteral("0", span) : arguments.getFirst();
        Expression end = arguments.getLast();
        Expression isEmpty = new BinaryExpression(HogQlQuery.BinaryOperator.LESS_THAN_OR_EQUAL, end, start, span);
        Expression empty = cast(new ArrayExpression(List.of(), span), "array(bigint)", span);
        Expression sequence = call(
                "sequence",
                List.of(start, new BinaryExpression(HogQlQuery.BinaryOperator.SUBTRACT, end, integerLiteral("1", span), span)),
                span);
        return call("if", List.of(isEmpty, empty, sequence), span);
    }

    private static Expression arrayEnumerate(Expression array, HogQlQuery.SourceSpan span)
    {
        Expression size = call("cardinality", List.of(array), span);
        Expression empty = cast(new ArrayExpression(List.of(), span), "array(bigint)", span);
        Expression sequence = call("sequence", List.of(integerLiteral("1", span), size), span);
        return call(
                "if",
                List.of(
                        new BinaryExpression(HogQlQuery.BinaryOperator.EQUAL, size, integerLiteral("0", span), span),
                        empty,
                        sequence),
                span);
    }

    private static Expression arraySort(FunctionCall function, List<Expression> arguments)
    {
        if (arguments.size() == 1) {
            return call("array_sort", arguments, function.span());
        }
        if (!(arguments.getFirst() instanceof LambdaExpression lambda) || lambda.arguments().size() != 1) {
            throw unsupportedError(function, "HogQL arraySort key must be a single-argument lambda");
        }
        HogQlQuery.SourceSpan span = function.span();
        Identifier left = new Identifier("__hogql_array_sort_left", false, span);
        Identifier right = new Identifier("__hogql_array_sort_right", false, span);
        String parameter = canonical(lambda.arguments().getFirst().value());
        Expression leftKey = substituteArraySortParameter(function, lambda.body(), parameter, new ColumnReference(List.of(left), span));
        Expression rightKey = substituteArraySortParameter(function, lambda.body(), parameter, new ColumnReference(List.of(right), span));
        Expression comparator = new CaseExpression(
                Optional.empty(),
                List.of(
                        new CaseWhen(
                                new BinaryExpression(HogQlQuery.BinaryOperator.LESS_THAN, leftKey, rightKey, span),
                                new UnaryExpression(HogQlQuery.UnaryOperator.NEGATE, integerLiteral("1", span), span),
                                span),
                        new CaseWhen(
                                new BinaryExpression(HogQlQuery.BinaryOperator.GREATER_THAN, leftKey, rightKey, span),
                                integerLiteral("1", span),
                                span)),
                Optional.of(integerLiteral("0", span)),
                span);
        return call(
                "array_sort",
                List.of(
                        arguments.get(1),
                        new LambdaExpression(List.of(left, right), comparator, span)),
                span);
    }

    private static Expression substituteArraySortParameter(FunctionCall function, Expression expression, String parameter, Expression replacement)
    {
        return switch (expression) {
            case ColumnReference reference -> reference.parts().size() == 1 && canonical(reference.parts().getFirst().value()).equals(parameter)
                    ? replacement
                    : reference;
            case SubscriptExpression subscript -> new SubscriptExpression(
                    substituteArraySortParameter(function, subscript.base(), parameter, replacement),
                    substituteArraySortParameter(function, subscript.index(), parameter, replacement),
                    subscript.span());
            case MemberAccessExpression member -> new MemberAccessExpression(
                    substituteArraySortParameter(function, member.base(), parameter, replacement),
                    member.member(),
                    member.span());
            case CastExpression cast -> new CastExpression(
                    substituteArraySortParameter(function, cast.value(), parameter, replacement),
                    cast.type(),
                    cast.safe(),
                    cast.typeDialect(),
                    cast.span());
            case UnaryExpression unary -> new UnaryExpression(
                    unary.operator(),
                    substituteArraySortParameter(function, unary.operand(), parameter, replacement),
                    unary.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    substituteArraySortParameter(function, binary.left(), parameter, replacement),
                    substituteArraySortParameter(function, binary.right(), parameter, replacement),
                    binary.span());
            case Literal literal -> literal;
            default -> throw unsupportedError(function, "HogQL arraySort key expression is outside the supported subset");
        };
    }

    private static Expression has(List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        Expression array = arguments.getFirst();
        Expression value = arguments.get(1);
        Identifier item = new Identifier("_hogql_item", false, span);
        LambdaExpression isNull = new LambdaExpression(
                List.of(item),
                new IsNullExpression(new ColumnReference(List.of(item), span), false, span, span),
                span);
        Expression findNull = call("any_match", List.of(array, isNull), span);
        Expression contains = coalesce(
                call("contains", List.of(array, value), span),
                new Literal(HogQlQuery.LiteralKind.BOOLEAN, "false", span),
                span);
        return call(
                "if",
                List.of(new IsNullExpression(value, false, span, span), findNull, contains),
                span);
    }

    private static Expression empty(Expression value, boolean expectedEmpty, HogQlQuery.SourceSpan span)
    {
        Expression length = coalesce(
                call("length", List.of(cast(value, "varchar", span)), span),
                integerLiteral("0", span),
                span);
        return new BinaryExpression(
                expectedEmpty ? HogQlQuery.BinaryOperator.EQUAL : HogQlQuery.BinaryOperator.GREATER_THAN,
                length,
                integerLiteral("0", span),
                span);
    }

    private static Expression startOfWeek(Expression value, HogQlQuery.SourceSpan span)
    {
        Expression shifted = call("date_add", List.of(
                new Literal(HogQlQuery.LiteralKind.STRING, "day", span),
                integerLiteral("1", span),
                value), span);
        Expression monday = dateTrunc("week", shifted, span);
        return call("date_add", List.of(
                new Literal(HogQlQuery.LiteralKind.STRING, "day", span),
                integerLiteral("-1", span),
                monday), span);
    }

    private static FunctionCall dateTrunc(String unit, Expression value, HogQlQuery.SourceSpan span)
    {
        return new FunctionCall(
                new Identifier("date_trunc", false, span),
                List.of(new Literal(HogQlQuery.LiteralKind.STRING, unit, span), value),
                false,
                List.of(),
                Optional.empty(),
                span);
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
