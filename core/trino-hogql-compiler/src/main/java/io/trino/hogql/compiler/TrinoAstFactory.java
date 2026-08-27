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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;

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
                Optional.empty(),
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
                    Optional.empty());
        };
    }

    private static Expression createExpression(HogQlQuery.Expression expression)
    {
        return switch (expression) {
            case ColumnReference reference -> createColumnReference(reference);
            case Literal literal -> createLiteral(literal);
        };
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
        return new NodeLocation(span.line(), span.column());
    }
}
