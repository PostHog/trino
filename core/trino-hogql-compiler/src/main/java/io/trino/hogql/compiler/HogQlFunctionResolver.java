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

    private static Expression rewrite(FunctionCall function, FunctionRewrite rewrite, List<Expression> arguments)
    {
        HogQlQuery.SourceSpan span = function.span();
        return switch (rewrite) {
            case IS_NULL -> new IsNullExpression(arguments.getFirst(), false, span, span);
            case IS_NOT_NULL -> new IsNullExpression(arguments.getFirst(), true, span, span);
            case CAST_DATE -> cast(arguments.getFirst(), "date", span);
            case CAST_DOUBLE -> cast(arguments.getFirst(), "double", span);
            case CAST_BIGINT -> cast(arguments.getFirst(), "bigint", span);
            case CAST_TIMESTAMP -> arguments.size() == 1
                    ? cast(arguments.getFirst(), "timestamp(0)", span)
                    : call("with_timezone", List.of(cast(arguments.getFirst(), "timestamp(0)", span), arguments.get(1)), span);
            case CAST_VARCHAR -> cast(arguments.getFirst(), "varchar", span);
            case DATE_TRUNC_DAY -> dateTrunc("day", arguments.getFirst(), span);
            case DATE_TRUNC_HOUR -> dateTrunc("hour", arguments.getFirst(), span);
            case DATE_TRUNC_MONTH -> dateTrunc("month", arguments.getFirst(), span);
            case DATE_TRUNC_WEEK -> dateTrunc("week", arguments.getFirst(), span);
            case COUNT_IF -> aggregate("count", List.of(), false, arguments.getFirst(), span);
            case SUM_IF -> aggregate("sum", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case MAX_IF -> aggregate("max", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case UNIQ_IF -> aggregate("approx_distinct", List.of(arguments.getFirst()), false, arguments.get(1), span);
            case UNIQ_EXACT -> aggregate("count", List.of(arguments.getFirst()), true, null, span);
            case GROUP_UNIQ_ARRAY -> aggregate("array_agg", List.of(arguments.getFirst()), true, null, span);
            case MULTI_IF -> multiIf(function, arguments);
            case JSON_EXTRACT_STRING -> jsonExtractScalar(function, arguments, "varchar", new Literal(HogQlQuery.LiteralKind.STRING, "", span));
            case JSON_EXTRACT_INT -> jsonExtractScalar(function, arguments, "bigint", new Literal(HogQlQuery.LiteralKind.INTEGER, "0", span));
            case JSON_EXTRACT_FLOAT -> jsonExtractScalar(function, arguments, "double", new Literal(HogQlQuery.LiteralKind.FLOAT, "0.0", span));
            case JSON_EXTRACT_RAW -> coalesce(
                    call("json_format", List.of(call("json_extract", List.of(arguments.getFirst(), jsonPath(function, arguments.subList(1, arguments.size()))), span)), span),
                    new Literal(HogQlQuery.LiteralKind.STRING, "", span),
                    span);
            case JSON_EXTRACT_TYPED -> jsonExtractTyped(function, arguments);
            case JSON_KEYS_AND_VALUES -> jsonExtractKeysAndValues(function, arguments);
            case JSON_KEYS_AND_VALUES_RAW -> jsonExtractKeysAndValuesRaw(function, arguments.getFirst());
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
        };
    }

    private static FunctionCall dateAdd(String unit, List<Expression> arguments, HogQlQuery.SourceSpan span)
    {
        return call(
                "date_add",
                List.of(new Literal(HogQlQuery.LiteralKind.STRING, unit, span), arguments.get(1), arguments.getFirst()),
                span);
    }

    private static FunctionCall aggregate(String name, List<Expression> arguments, boolean distinct, Expression filter, HogQlQuery.SourceSpan span)
    {
        return new FunctionCall(
                new Identifier(name, false, span),
                arguments,
                distinct,
                List.of(),
                Optional.ofNullable(filter),
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

    private static Expression jsonExtractKeysAndValuesRaw(FunctionCall function, Expression value)
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
                List.of(typedJsonMap(value, "json", span), formatValue),
                span);
        return call("map_entries", List.of(formatted), span);
    }

    private static Expression typedJsonMap(Expression value, String valueType, HogQlQuery.SourceSpan span)
    {
        String type = "map(varchar," + valueType + ")";
        Expression parsed = call("json_parse", List.of(value), span);
        Expression converted = new CastExpression(parsed, new Identifier(type, false, span), true, span);
        Expression empty = new CastExpression(
                call("map", List.of(new ArrayExpression(List.of(), span), new ArrayExpression(List.of(), span)), span),
                new Identifier(type, false, span),
                false,
                span);
        return coalesce(converted, empty, span);
    }

    private static String jsonType(FunctionCall function, Expression expression)
    {
        if (expression instanceof Literal literal && literal.kind() == HogQlQuery.LiteralKind.STRING) {
            return literal.value();
        }
        throw unsupportedError(function, "HogQL JSON extraction type must be a string literal");
    }

    private static Literal jsonPath(FunctionCall function, List<Expression> segments)
    {
        StringBuilder path = new StringBuilder("$");
        for (Expression segment : segments) {
            if (!(segment instanceof Literal literal)) {
                throw unsupportedError(function, "HogQL JSON path segments must be string or integer literals");
            }
            switch (literal.kind()) {
                case STRING -> path.append("[\"")
                        .append(literal.value().replace("\\", "\\\\").replace("\"", "\\\""))
                        .append("\"]");
                case INTEGER -> path.append('[').append(literal.value()).append(']');
                default -> throw unsupportedError(function, "HogQL JSON path segments must be string or integer literals");
            }
        }
        return new Literal(HogQlQuery.LiteralKind.STRING, path.toString(), function.span());
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
