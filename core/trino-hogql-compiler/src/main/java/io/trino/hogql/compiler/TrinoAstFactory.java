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
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.WhenClause;

import java.util.List;
import java.util.Optional;

final class TrinoAstFactory
{
    private TrinoAstFactory() {}

    public static Statement createStatement(HogQlQuery query)
    {
        NodeLocation location = location(query.span());
        QuerySpecification querySpecification = new QuerySpecification(
                location,
                new Select(location, false, query.projections().stream()
                        .map(TrinoAstFactory::createSelectItem)
                        .toList()),
                query.from().map(TrinoAstFactory::createTable),
                query.where().map(TrinoAstFactory::createExpression),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new Query(
                location,
                List.of(),
                List.of(),
                Optional.empty(),
                querySpecification,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static SelectItem createSelectItem(Projection projection)
    {
        return switch (projection) {
            case Star star -> new AllColumns(location(star.span()));
            case ExpressionProjection expression -> new SingleColumn(
                    location(expression.span()),
                    createExpression(expression.expression()),
                    expression.alias().map(TrinoAstFactory::createIdentifier));
        };
    }

    private static Expression createExpression(HogQlQuery.Expression expression)
    {
        return switch (expression) {
            case ArrayExpression array -> new Array(
                    location(array.span()),
                    array.values().stream()
                            .map(TrinoAstFactory::createExpression)
                            .toList());
            case BetweenExpression between -> createBetweenExpression(between);
            case BinaryExpression binary -> createBinaryExpression(binary);
            case CaseExpression caseExpression -> createCaseExpression(caseExpression);
            case CastExpression cast -> new Cast(
                    location(cast.span()),
                    createExpression(cast.value()),
                    new GenericDataType(location(cast.type().span()), createIdentifier(cast.type()), List.of()),
                    cast.safe());
            case ColumnReference reference -> createColumnReference(reference);
            case FunctionCall function -> new io.trino.sql.tree.FunctionCall(
                    location(function.span()),
                    QualifiedName.of(List.of(createIdentifier(function.name()))),
                    function.arguments().stream()
                            .map(TrinoAstFactory::createExpression)
                            .toList());
            case InExpression in -> createInExpression(in);
            case IsNullExpression isNull -> new Predicated(
                    location(isNull.predicateSpan()),
                    createExpression(isNull.value()),
                    new IsNullPredicate(location(isNull.predicateSpan()), isNull.negated()));
            case Literal literal -> createLiteral(literal);
            case TupleExpression tuple -> new Row(
                    location(tuple.span()),
                    tuple.values().stream()
                            .map(value -> new Row.Field(location(value.span()), Optional.empty(), createExpression(value)))
                            .toList());
            case UnaryExpression unary -> switch (unary.operator()) {
                case NEGATE -> ArithmeticUnaryExpression.negative(location(unary.span()), createExpression(unary.operand()));
                case NOT -> new NotExpression(location(unary.span()), createExpression(unary.operand()));
                case POSITIVE -> ArithmeticUnaryExpression.positive(location(unary.span()), createExpression(unary.operand()));
            };
        };
    }

    private static Expression createCaseExpression(CaseExpression caseExpression)
    {
        NodeLocation location = location(caseExpression.span());
        List<WhenClause> whenClauses = caseExpression.whenClauses().stream()
                .map(when -> new WhenClause(
                        location(when.span()),
                        createExpression(when.operand()),
                        createExpression(when.result())))
                .toList();
        Optional<Expression> defaultValue = caseExpression.defaultValue().map(TrinoAstFactory::createExpression);
        return caseExpression.operand()
                .<Expression>map(operand -> new SimpleCaseExpression(location, createExpression(operand), whenClauses, defaultValue))
                .orElseGet(() -> new SearchedCaseExpression(location, whenClauses, defaultValue));
    }

    private static Expression createBetweenExpression(BetweenExpression between)
    {
        NodeLocation location = location(between.predicateSpan());
        return new Predicated(
                location,
                createExpression(between.value()),
                new BetweenPredicate(
                        location,
                        between.negated(),
                        Optional.empty(),
                        createExpression(between.min()),
                        createExpression(between.max())));
    }

    private static Expression createInExpression(InExpression in)
    {
        NodeLocation location = location(in.predicateSpan());
        return new Predicated(
                location,
                createExpression(in.value()),
                new InPredicate(
                        location,
                        in.negated(),
                        new InListExpression(
                                location,
                                in.values().stream()
                                        .map(TrinoAstFactory::createExpression)
                                        .toList())));
    }

    private static Expression createBinaryExpression(BinaryExpression binary)
    {
        NodeLocation location = location(binary.span());
        Expression left = createExpression(binary.left());
        Expression right = createExpression(binary.right());
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
}
