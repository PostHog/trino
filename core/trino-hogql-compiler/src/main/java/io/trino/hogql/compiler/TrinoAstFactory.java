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
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CastTypeDialect;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
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
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.Relation;
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
import io.trino.hogql.parser.tree.HogQlQuery.WindowDefinition;
import io.trino.hogql.parser.tree.HogQlQuery.WindowFrame;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SimpleIntervalQualifier;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;

final class TrinoAstFactory
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private TrinoAstFactory() {}

    public static Statement createStatement(HogQlQuery query, Map<SourceSpan, Integer> parameterIds)
    {
        return createQuery(query, parameterIds);
    }

    private static Query createQuery(HogQlQuery query, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(query.span());
        QueryBody queryBody = createQueryBody(query, parameterIds);
        boolean setOperation = query.body() instanceof SetOperation;
        return new Query(
                location,
                List.of(),
                List.of(),
                createWith(query, parameterIds),
                queryBody,
                setOperation ? createOrderBy(query, parameterIds) : Optional.empty(),
                setOperation ? query.offset().map(offset -> new Offset(location(offset.span()), createExpression(offset, parameterIds))) : Optional.empty(),
                setOperation ? query.limit().map(limit -> new Limit(location(limit.span()), createExpression(limit, parameterIds))) : Optional.empty());
    }

    private static QueryBody createQueryBody(HogQlQuery query, Map<SourceSpan, Integer> parameterIds)
    {
        return switch (query.body()) {
            case SelectQueryBody select -> createQuerySpecification(query, select, parameterIds);
            case SetOperation setOperation -> createSetOperation(setOperation, parameterIds);
        };
    }

    private static QuerySpecification createQuerySpecification(
            HogQlQuery query,
            SelectQueryBody select,
            Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(select.span());
        QuerySpecification querySpecification = new QuerySpecification(
                location,
                new Select(location, select.distinct(), select.projections().stream()
                        .flatMap(projection -> createSelectItems(projection, parameterIds).stream())
                        .toList()),
                select.from().map(relation -> createRelation(relation, parameterIds)),
                select.where().map(expression -> createExpression(expression, parameterIds)),
                createGroupBy(select.groupBy(), parameterIds),
                select.having().map(expression -> createExpression(expression, parameterIds)),
                select.windows().stream()
                        .map(window -> createWindowDefinition(window, parameterIds))
                        .toList(),
                createOrderBy(query, parameterIds),
                query.offset().map(offset -> new Offset(location(offset.span()), createExpression(offset, parameterIds))),
                query.limit().map(limit -> new Limit(location(limit.span()), createExpression(limit, parameterIds))));
        return querySpecification;
    }

    private static QueryBody createSetOperation(SetOperation setOperation, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(setOperation.operatorSpan());
        QueryBody left = createSetOperand(setOperation.left(), setOperation.leftParenthesized(), parameterIds);
        QueryBody right = createSetOperand(setOperation.right(), setOperation.rightParenthesized(), parameterIds);
        return switch (setOperation.type()) {
            case EXCEPT -> new Except(location, left, right, setOperation.distinct(), Optional.empty());
            case INTERSECT -> new Intersect(location, List.of(left, right), setOperation.distinct(), Optional.empty());
            case UNION -> new Union(location, List.of(left, right), setOperation.distinct(), Optional.empty());
        };
    }

    private static QueryBody createSetOperand(
            HogQlQuery query,
            boolean parenthesized,
            Map<SourceSpan, Integer> parameterIds)
    {
        if (parenthesized || !query.with().isEmpty() || !query.orderBy().isEmpty() || query.limit().isPresent() || query.offset().isPresent()) {
            return new TableSubquery(location(query.span()), createQuery(query, parameterIds));
        }
        return createQueryBody(query, parameterIds);
    }

    private static Optional<With> createWith(HogQlQuery query, Map<SourceSpan, Integer> parameterIds)
    {
        if (query.with().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new With(
                location(query.span()),
                false,
                query.with().stream()
                        .map(commonTable -> createWithQuery(commonTable, parameterIds))
                        .toList()));
    }

    private static WithQuery createWithQuery(CommonTableExpression commonTable, Map<SourceSpan, Integer> parameterIds)
    {
        return new WithQuery(
                location(commonTable.span()),
                createIdentifier(commonTable.name()),
                createQuery(commonTable.query(), parameterIds),
                commonTable.columnAliases().isEmpty()
                        ? Optional.empty()
                        : Optional.of(commonTable.columnAliases().stream()
                                      .map(TrinoAstFactory::createIdentifier)
                                      .toList()));
    }

    private static Optional<OrderBy> createOrderBy(HogQlQuery query, Map<SourceSpan, Integer> parameterIds)
    {
        return createOrderBy(query.orderBy(), parameterIds);
    }

    private static Optional<OrderBy> createOrderBy(List<HogQlQuery.SortItem> sortItems, Map<SourceSpan, Integer> parameterIds)
    {
        if (sortItems.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new OrderBy(
                location(sortItems.getFirst().span()),
                sortItems.stream()
                        .map(sortItem -> new io.trino.sql.tree.SortItem(
                                location(sortItem.span()),
                                createExpression(sortItem.expression(), parameterIds),
                                switch (sortItem.direction()) {
                                    case ASCENDING -> Ordering.ASCENDING;
                                    case DESCENDING -> Ordering.DESCENDING;
                                },
                                switch (sortItem.nullPlacement()) {
                                    case FIRST -> NullOrdering.FIRST;
                                    case LAST -> NullOrdering.LAST;
                                    case UNDEFINED -> NullOrdering.UNDEFINED;
                                }))
                        .toList()));
    }

    private static Optional<GroupBy> createGroupBy(List<HogQlQuery.Expression> expressions, Map<SourceSpan, Integer> parameterIds)
    {
        if (expressions.isEmpty()) {
            return Optional.empty();
        }
        NodeLocation location = location(expressions.getFirst().span());
        return Optional.of(new GroupBy(
                location,
                false,
                List.of(new SimpleGroupBy(
                        location,
                        expressions.stream()
                                .map(expression -> createExpression(expression, parameterIds))
                                .toList()))));
    }

    private static List<SelectItem> createSelectItems(Projection projection, Map<SourceSpan, Integer> parameterIds)
    {
        return switch (projection) {
            case ColumnsList columns -> columns.expressions().stream()
                    .map(expression -> new SingleColumn(
                            location(expression.span()),
                            createExpression(expression, parameterIds),
                            Optional.empty()))
                    .map(SelectItem.class::cast)
                    .toList();
            case ColumnsRegex columns -> throw unsupportedColumns(columns.span());
            case Star star -> List.of(createAllColumns(star));
            case ExpressionProjection expression -> List.of(new SingleColumn(
                    location(expression.span()),
                    createExpression(expression.expression(), parameterIds),
                    expression.alias().map(TrinoAstFactory::createIdentifier)));
        };
    }

    private static AllColumns createAllColumns(Star star)
    {
        if (!star.replacements().isEmpty()) {
            Identifier target = star.replacements().getFirst().target();
            throw unsupportedSemanticExpression(
                    target.span(),
                    "HogQL star replacement requires a logical relation from the semantic catalog: " + target.value());
        }
        if (!star.exclusions().isEmpty()) {
            ColumnReference exclusion = star.exclusions().getFirst();
            throw unsupportedSemanticExpression(
                    exclusion.span(),
                    "HogQL star exclusions require a logical relation from the semantic catalog: " +
                            String.join(".", exclusion.parts().stream().map(Identifier::value).toList()));
        }
        Optional<Expression> target = star.qualifier().isEmpty()
                ? Optional.empty()
                : Optional.of(createColumnReference(new ColumnReference(star.qualifier(), star.span())));
        return new AllColumns(location(star.span()), target, List.of());
    }

    private static TrinoException unsupportedColumns(SourceSpan span)
    {
        return unsupportedSemanticExpression(span, "HogQL COLUMNS requires a logical relation from the semantic catalog");
    }

    private static Expression createExpression(HogQlQuery.Expression expression, Map<SourceSpan, Integer> parameterIds)
    {
        return switch (expression) {
            case ArrayExpression array -> new Array(
                    location(array.span()),
                    array.values().stream()
                            .map(value -> createExpression(value, parameterIds))
                            .toList());
            case BetweenExpression between -> createBetweenExpression(between, parameterIds);
            case BinaryExpression binary -> createBinaryExpression(binary, parameterIds);
            case CaseExpression caseExpression -> createCaseExpression(caseExpression, parameterIds);
            case CastExpression cast -> new Cast(
                    location(cast.span()),
                    createExpression(cast.value(), parameterIds),
                    createCastType(cast.type(), cast.typeDialect()),
                    cast.safe());
            case ColumnReference reference -> createColumnReference(reference);
            case FunctionCall function -> createFunctionCall(function, parameterIds);
            case InCohortExpression in -> throw unsupportedSemanticExpression(in.span(), "HogQL IN COHORT requires a semantic catalog snapshot");
            case InExpression in -> createInExpression(in, parameterIds);
            case InSubqueryExpression in -> createInSubqueryExpression(in, parameterIds);
            case IntervalExpression interval -> createIntervalExpression(interval, parameterIds);
            case IsNullExpression isNull -> new Predicated(
                    location(isNull.predicateSpan()),
                    createExpression(isNull.value(), parameterIds),
                    new IsNullPredicate(location(isNull.predicateSpan()), isNull.negated()));
            case Literal literal -> createLiteral(literal);
            case MemberAccessExpression memberAccess -> new DereferenceExpression(
                    location(memberAccess.span()),
                    createExpression(memberAccess.base(), parameterIds),
                    createIdentifier(memberAccess.member()));
            case Placeholder placeholder -> new Parameter(location(placeholder.span()), parameterIds.get(placeholder.span()));
            case SubscriptExpression subscript -> new io.trino.sql.tree.SubscriptExpression(
                    location(subscript.span()),
                    createExpression(subscript.base(), parameterIds),
                    createExpression(subscript.index(), parameterIds));
            case TupleExpression tuple -> new Row(
                    location(tuple.span()),
                    tuple.values().stream()
                            .map(value -> new Row.Field(location(value.span()), Optional.empty(), createExpression(value, parameterIds)))
                            .toList());
            case UnaryExpression unary -> switch (unary.operator()) {
                case NEGATE -> ArithmeticUnaryExpression.negative(location(unary.span()), createExpression(unary.operand(), parameterIds));
                case NOT -> new NotExpression(location(unary.span()), createExpression(unary.operand(), parameterIds));
                case POSITIVE -> ArithmeticUnaryExpression.positive(location(unary.span()), createExpression(unary.operand(), parameterIds));
            };
        };
    }

    private static Expression createIntervalExpression(IntervalExpression interval, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(interval.span());
        int multiplier = switch (interval.unit()) {
            case WEEK -> 7;
            case QUARTER -> 3;
            default -> 1;
        };
        IntervalField field = switch (interval.unit()) {
            case SECOND -> new IntervalField.Second(OptionalInt.empty());
            case MINUTE -> new IntervalField.Minute();
            case HOUR -> new IntervalField.Hour();
            case DAY, WEEK -> new IntervalField.Day();
            case MONTH, QUARTER -> new IntervalField.Month();
            case YEAR -> new IntervalField.Year();
        };
        IntervalLiteral unitInterval = new IntervalLiteral(
                location,
                Integer.toString(multiplier),
                IntervalLiteral.Sign.POSITIVE,
                new SimpleIntervalQualifier(location, OptionalInt.empty(), field));
        return new ArithmeticBinaryExpression(
                location,
                ArithmeticBinaryExpression.Operator.MULTIPLY,
                createExpression(interval.value(), parameterIds),
                unitInterval);
    }

    private static io.trino.sql.tree.DataType createCastType(Identifier type, CastTypeDialect typeDialect)
    {
        if (typeDialect == CastTypeDialect.TRINO) {
            return SQL_PARSER.createType(type.value());
        }
        try {
            return SQL_PARSER.createType(HogQlCastTypeTranslator.translate(type.value()));
        }
        catch (IllegalArgumentException exception) {
            throw unsupportedSemanticExpression(
                    type.span(),
                    "HogQL cast type cannot be represented exactly in Trino: " + type.value() + " (" + exception.getMessage() + ")");
        }
    }

    private static Expression createFunctionCall(FunctionCall function, Map<SourceSpan, Integer> parameterIds)
    {
        if (function.nameParts().size() == 1 && function.name().value().equalsIgnoreCase("matchesAction")) {
            throw unsupportedSemanticExpression(function.span(), "HogQL matchesAction requires a semantic catalog snapshot and events relation");
        }
        return new io.trino.sql.tree.FunctionCall(
                location(function.span()),
                QualifiedName.of(function.nameParts().stream()
                        .map(TrinoAstFactory::createIdentifier)
                        .toList()),
                function.window().map(window -> createWindow(window, parameterIds)),
                function.filter().map(filter -> createExpression(filter, parameterIds)),
                createOrderBy(function.orderBy(), parameterIds),
                function.distinct(),
                function.nullTreatment().map(_ -> io.trino.sql.tree.FunctionCall.NullTreatment.IGNORE),
                Optional.empty(),
                function.arguments().stream()
                        .map(argument -> new CallArgument(
                                location(argument.span()),
                                Optional.empty(),
                                createExpression(argument, parameterIds)))
                        .toList());
    }

    private static io.trino.sql.tree.WindowDefinition createWindowDefinition(
            WindowDefinition definition,
            Map<SourceSpan, Integer> parameterIds)
    {
        return new io.trino.sql.tree.WindowDefinition(
                location(definition.span()),
                createIdentifier(definition.name()),
                (io.trino.sql.tree.WindowSpecification) createWindow(definition.specification(), parameterIds));
    }

    private static io.trino.sql.tree.Window createWindow(Window window, Map<SourceSpan, Integer> parameterIds)
    {
        return switch (window) {
            case WindowReference reference -> new io.trino.sql.tree.WindowReference(
                    location(reference.span()),
                    createIdentifier(reference.name()));
            case WindowSpecification specification -> new io.trino.sql.tree.WindowSpecification(
                    location(specification.span()),
                    Optional.empty(),
                    specification.partitionBy().stream()
                            .map(expression -> createExpression(expression, parameterIds))
                            .toList(),
                    createOrderBy(specification.orderBy(), parameterIds),
                    specification.frame().map(frame -> createWindowFrame(frame, parameterIds)));
        };
    }

    private static io.trino.sql.tree.WindowFrame createWindowFrame(
            WindowFrame frame,
            Map<SourceSpan, Integer> parameterIds)
    {
        return new io.trino.sql.tree.WindowFrame(
                location(frame.span()),
                switch (frame.type()) {
                    case RANGE -> io.trino.sql.tree.WindowFrame.Type.RANGE;
                    case ROWS -> io.trino.sql.tree.WindowFrame.Type.ROWS;
                },
                createFrameBound(frame.start(), parameterIds),
                frame.end().map(bound -> createFrameBound(bound, parameterIds)),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                List.of());
    }

    private static io.trino.sql.tree.FrameBound createFrameBound(
            FrameBound bound,
            Map<SourceSpan, Integer> parameterIds)
    {
        io.trino.sql.tree.FrameBound.Type type = switch (bound.type()) {
            case CURRENT_ROW -> io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
            case FOLLOWING -> io.trino.sql.tree.FrameBound.Type.FOLLOWING;
            case PRECEDING -> io.trino.sql.tree.FrameBound.Type.PRECEDING;
            case UNBOUNDED_FOLLOWING -> io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
            case UNBOUNDED_PRECEDING -> io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
        };
        return bound.value()
                .map(value -> new io.trino.sql.tree.FrameBound(
                        location(bound.span()),
                        type,
                        createExpression(value, parameterIds)))
                .orElseGet(() -> new io.trino.sql.tree.FrameBound(location(bound.span()), type));
    }

    private static Expression createCaseExpression(CaseExpression caseExpression, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(caseExpression.span());
        List<WhenClause> whenClauses = caseExpression.whenClauses().stream()
                .map(when -> new WhenClause(
                        location(when.span()),
                        createExpression(when.operand(), parameterIds),
                        createExpression(when.result(), parameterIds)))
                .toList();
        Optional<Expression> defaultValue = caseExpression.defaultValue().map(value -> createExpression(value, parameterIds));
        return caseExpression.operand()
                .<Expression>map(operand -> new SimpleCaseExpression(location, createExpression(operand, parameterIds), whenClauses, defaultValue))
                .orElseGet(() -> new SearchedCaseExpression(location, whenClauses, defaultValue));
    }

    private static Expression createBetweenExpression(BetweenExpression between, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(between.predicateSpan());
        return new Predicated(
                location,
                createExpression(between.value(), parameterIds),
                new BetweenPredicate(
                        location,
                        between.negated(),
                        Optional.empty(),
                        createExpression(between.min(), parameterIds),
                        createExpression(between.max(), parameterIds)));
    }

    private static Expression createInExpression(InExpression in, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(in.predicateSpan());
        return new Predicated(
                location,
                createExpression(in.value(), parameterIds),
                new InPredicate(
                        location,
                        in.negated(),
                        new InListExpression(
                                location,
                                in.values().stream()
                                        .map(value -> createExpression(value, parameterIds))
                                        .toList())));
    }

    private static Expression createInSubqueryExpression(InSubqueryExpression in, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(in.predicateSpan());
        return new Predicated(
                location,
                createExpression(in.value(), parameterIds),
                new InPredicate(
                        location,
                        in.negated(),
                        new io.trino.sql.tree.SubqueryExpression(location, createQuery(in.query(), parameterIds))));
    }

    private static Expression createBinaryExpression(BinaryExpression binary, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation location = location(binary.span());
        Expression left = createExpression(binary.left(), parameterIds);
        Expression right = createExpression(binary.right(), parameterIds);
        return switch (binary.operator()) {
            case ADD -> new ArithmeticBinaryExpression(location, ArithmeticBinaryExpression.Operator.ADD, left, right);
            case SUBTRACT -> new ArithmeticBinaryExpression(location, ArithmeticBinaryExpression.Operator.SUBTRACT, left, right);
            case MULTIPLY -> new ArithmeticBinaryExpression(location, ArithmeticBinaryExpression.Operator.MULTIPLY, left, right);
            case DIVIDE -> new ArithmeticBinaryExpression(location, ArithmeticBinaryExpression.Operator.DIVIDE, left, right);
            case MODULO -> new ArithmeticBinaryExpression(location, ArithmeticBinaryExpression.Operator.MODULO, left, right);
            case AND -> new LogicalExpression(location, LogicalExpression.Operator.AND, List.of(left, right));
            case OR -> new LogicalExpression(location, LogicalExpression.Operator.OR, List.of(left, right));
            case EQUAL -> comparison(location, ComparisonPredicate.Operator.EQUAL, left, right);
            case NOT_EQUAL -> comparison(location, ComparisonPredicate.Operator.NOT_EQUAL, left, right);
            case LESS_THAN -> comparison(location, ComparisonPredicate.Operator.LESS_THAN, left, right);
            case LESS_THAN_OR_EQUAL -> comparison(location, ComparisonPredicate.Operator.LESS_THAN_OR_EQUAL, left, right);
            case GREATER_THAN -> comparison(location, ComparisonPredicate.Operator.GREATER_THAN, left, right);
            case GREATER_THAN_OR_EQUAL -> comparison(location, ComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL, left, right);
        };
    }

    private static Expression comparison(NodeLocation location, ComparisonPredicate.Operator operator, Expression left, Expression right)
    {
        return new Predicated(location, left, new ComparisonPredicate(location, operator, right));
    }

    private static Expression createLiteral(Literal literal)
    {
        NodeLocation location = location(literal.span());
        return switch (literal.kind()) {
            case BOOLEAN -> new BooleanLiteral(location, literal.value());
            case INTEGER -> new LongLiteral(location, literal.value());
            case NULL -> new NullLiteral(location);
            case STRING -> new StringLiteral(location, literal.value());
        };
    }

    private static Expression createColumnReference(ColumnReference reference)
    {
        List<Identifier> parts = reference.parts();
        Expression expression = createIdentifier(parts.getFirst());
        for (Identifier part : parts.subList(1, parts.size())) {
            expression = new DereferenceExpression(location(reference.span()), expression, createIdentifier(part));
        }
        return expression;
    }

    private static io.trino.sql.tree.Relation createRelation(Relation relation, Map<SourceSpan, Integer> parameterIds)
    {
        return switch (relation) {
            case AliasedRelation alias -> new io.trino.sql.tree.AliasedRelation(
                    location(alias.span()),
                    createRelation(alias.relation(), parameterIds),
                    createIdentifier(alias.alias()),
                    alias.columnAliases().isEmpty()
                            ? null
                            : alias.columnAliases().stream()
                              .map(TrinoAstFactory::createIdentifier)
                              .toList());
            case CommonTableReference commonTable -> new Table(
                    location(commonTable.span()),
                    QualifiedName.of(List.of(createIdentifier(commonTable.name()))));
            case JoinRelation join -> new io.trino.sql.tree.Join(
                    location(join.span()),
                    switch (join.type()) {
                        case CROSS -> io.trino.sql.tree.Join.Type.CROSS;
                        case INNER -> io.trino.sql.tree.Join.Type.INNER;
                        case LEFT -> io.trino.sql.tree.Join.Type.LEFT;
                        case RIGHT -> io.trino.sql.tree.Join.Type.RIGHT;
                        case FULL -> io.trino.sql.tree.Join.Type.FULL;
                    },
                    createRelation(join.left(), parameterIds),
                    createRelation(join.right(), parameterIds),
                    join.criteria().map(criteria -> switch (criteria) {
                        case JoinOn on -> new io.trino.sql.tree.JoinOn(createExpression(on.expression(), parameterIds));
                        case JoinUsing using -> new io.trino.sql.tree.JoinUsing(using.columns().stream()
                                .map(TrinoAstFactory::createIdentifier)
                                .toList());
                    }));
            case SubqueryRelation subquery -> new TableSubquery(location(subquery.span()), createQuery(subquery.query(), parameterIds));
            case TablePlaceholder _ -> throw new IllegalArgumentException("table placeholder was not validated");
            case TableReference table -> createTable(table);
            case ValuesRelation values -> createValuesRelation(values, parameterIds);
        };
    }

    private static TableSubquery createValuesRelation(ValuesRelation values, Map<SourceSpan, Integer> parameterIds)
    {
        NodeLocation valuesLocation = new NodeLocation(values.span().startLine(), values.span().startColumn() + 1);
        Values body = new Values(
                valuesLocation,
                values.rows().stream()
                        .map(row -> createValuesRow(row, parameterIds))
                        .toList());
        Query query = new Query(
                valuesLocation,
                List.of(),
                List.of(),
                Optional.empty(),
                body,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new TableSubquery(location(values.span()), query);
    }

    private static Expression createValuesRow(List<HogQlQuery.Expression> row, Map<SourceSpan, Integer> parameterIds)
    {
        if (row.size() == 1) {
            return createExpression(row.getFirst(), parameterIds);
        }
        return new Row(
                location(row.getFirst().span()),
                row.stream()
                        .map(value -> new Row.Field(location(value.span()), Optional.empty(), createExpression(value, parameterIds)))
                        .toList());
    }

    private static Table createTable(TableReference table)
    {
        List<io.trino.sql.tree.Identifier> parts = table.parts().stream()
                .map(TrinoAstFactory::createIdentifier)
                .toList();
        return new Table(location(table.span()), QualifiedName.of(parts));
    }

    private static io.trino.sql.tree.Identifier createIdentifier(Identifier identifier)
    {
        return new io.trino.sql.tree.Identifier(location(identifier.span()), identifier.value(), identifier.delimited());
    }

    private static NodeLocation location(SourceSpan span)
    {
        return new NodeLocation(span.startLine(), span.startColumn());
    }

    private static TrinoException unsupportedSemanticExpression(SourceSpan span, String message)
    {
        return new TrinoException(
                HOGQL_UNSUPPORTED_FEATURE,
                Optional.of(new Location(span.startLine(), span.startColumn())),
                message,
                null);
    }
}
