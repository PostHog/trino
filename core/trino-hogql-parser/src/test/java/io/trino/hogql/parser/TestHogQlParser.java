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
package io.trino.hogql.parser;

import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.NullPlacement;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.SortDirection;
import io.trino.hogql.parser.tree.HogQlSyntaxTree;
import io.trino.hogql.parser.tree.HogQlSyntaxTree.Element;
import io.trino.hogql.parser.tree.HogQlSyntaxTree.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlParser
{
    private final HogQlParser parser = new HogQlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT",
            "SELECT * FROM first JOIN second",
            "SELECT count(*) OVER ()",
            "SELECT function(1)(value)",
            "DROP TABLE events",
            "SELECT 1; SELECT 2",
            "SELECT * FROM",
            "SELECT {1 + 2}",
            "SELECT 1 GROUP BY ALL",
    })
    public void testRejectsSyntaxWithoutAnAstMapping(String hogql)
    {
        assertThatThrownBy(() -> parser.parseStatement(hogql))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageStartingWith("line ");
    }

    @Test
    public void testUsesCanonicalGrammarForExpressionsAndWhereClause()
    {
        HogQlQuery query = parser.parseStatement("SELECT event + 1 * 2 FROM events WHERE event >= 3 AND NOT false");

        assertThat(query.projections()).hasSize(1);
        assertThat(query.from()).isPresent();
        assertThat(query.where()).isPresent();
    }

    @Test
    public void testPreservesNamedPlaceholderNamesAndSourceSpans()
    {
        HogQlQuery query = parser.parseStatement("SELECT {later},\n {first} + {later}");
        Placeholder later = (Placeholder) ((ExpressionProjection) query.projections().getFirst()).expression();
        BinaryExpression addition = (BinaryExpression) ((ExpressionProjection) query.projections().get(1)).expression();
        Placeholder first = (Placeholder) addition.left();
        Placeholder repeated = (Placeholder) addition.right();

        assertThat(later.name()).isEqualTo("later");
        assertThat(later.span()).isEqualTo(new HogQlQuery.SourceSpan(7, 14, 1, 8, 1, 15));
        assertThat(first.name()).isEqualTo("first");
        assertThat(first.span()).isEqualTo(new HogQlQuery.SourceSpan(17, 24, 2, 2, 2, 9));
        assertThat(repeated.name()).isEqualTo("later");
        assertThat(repeated.span()).isEqualTo(new HogQlQuery.SourceSpan(27, 34, 2, 12, 2, 19));
    }

    @Test
    public void testBuildsDistinctOrderingAndPaginationAst()
    {
        HogQlQuery query = parser.parseStatement("SELECT DISTINCT event FROM events ORDER BY event DESC NULLS FIRST LIMIT 10 OFFSET 2");

        assertThat(query.distinct()).isTrue();
        assertThat(query.orderBy()).singleElement().satisfies(sortItem -> {
            assertThat(sortItem.direction()).isEqualTo(SortDirection.DESCENDING);
            assertThat(sortItem.nullPlacement()).isEqualTo(NullPlacement.FIRST);
        });
        assertThat(query.limit()).get().extracting(HogQlQuery.Literal.class::cast).extracting(HogQlQuery.Literal::value).isEqualTo("10");
        assertThat(query.offset()).get().extracting(HogQlQuery.Literal.class::cast).extracting(HogQlQuery.Literal::value).isEqualTo("2");
    }

    @Test
    public void testBuildsOrdinaryGroupingHavingAndAggregateFunctionAst()
    {
        HogQlQuery query = parser.parseStatement("SELECT country, count(DISTINCT person_id) FROM events GROUP BY country, lower(source) HAVING count(*) > 1");

        assertThat(query.groupBy()).hasSize(2);
        assertThat(query.having()).isPresent();
        FunctionCall count = (FunctionCall) ((ExpressionProjection) query.projections().get(1)).expression();
        assertThat(count.name().value()).isEqualTo("count");
        assertThat(count.distinct()).isTrue();
        assertThat(count.arguments()).singleElement().isInstanceOf(ColumnReference.class);
    }

    @Test
    public void testSourceSpansUseUnicodeCodePointOffsetsAndEndPositions()
    {
        HogQlQuery query = parser.parseStatement("SELECT '😀',\n event");
        ExpressionProjection projection = (ExpressionProjection) query.projections().get(1);
        ColumnReference reference = (ColumnReference) projection.expression();

        assertThat(query.span()).isEqualTo(new HogQlQuery.SourceSpan(0, 18, 1, 1, 2, 7));
        assertThat(reference.span()).isEqualTo(new HogQlQuery.SourceSpan(13, 18, 2, 2, 2, 7));
    }

    @Test
    public void testSyntaxTreeRepresentsReadOnlyGrammarBeforeLowering()
    {
        String hogql = "WITH x AS (SELECT 1) SELECT * FROM x UNION ALL SELECT event FROM events ORDER BY event LIMIT 10 OFFSET 2;";

        HogQlSyntaxTree syntaxTree = parser.parseSyntax(hogql);

        assertThat(syntaxTree.languageClass()).isEqualTo(HogQlSyntaxTree.LanguageClass.READ_ONLY_QUERY);
        assertThat(syntaxTree.root().span()).isEqualTo(new HogQlQuery.SourceSpan(0, hogql.length(), 1, 1, 1, hogql.length() + 1));
        assertThat(nodes(syntaxTree.root()))
                .extracting(Node::rule)
                .contains("withClause", "selectSetStmt", "subsequentSelectSetClause", "orderByClause", "limitAndOffsetClauseOptional");
        assertThat(nodes(syntaxTree.root()))
                .extracting(Node::alternative)
                .contains(java.util.Optional.of("WithExprSubquery"), java.util.Optional.of("ColumnExprIdentifier"));
    }

    @Test
    public void testSyntaxTreeClassifiesHogQlXAsNonQuerySyntax()
    {
        HogQlSyntaxTree syntaxTree = parser.parseSyntax("<Table />");

        assertThat(syntaxTree.languageClass()).isEqualTo(HogQlSyntaxTree.LanguageClass.HOGQLX);
        assertThat(nodes(syntaxTree.root()))
                .extracting(Node::rule)
                .contains("hogqlxTagElement");
    }

    @Test
    public void testSyntaxTreeClassifiesBlockLambdaAsProcedural()
    {
        HogQlSyntaxTree syntaxTree = parser.parseSyntax("SELECT value -> { RETURN value }");

        assertThat(syntaxTree.languageClass()).isEqualTo(HogQlSyntaxTree.LanguageClass.PROCEDURAL);
        assertThat(nodes(syntaxTree.root()))
                .extracting(Node::rule)
                .contains("block", "returnStmt");
    }

    @Test
    public void testParsesCanonicalExpressionEntryPointWithoutAQueryWrapper()
    {
        HogQlSyntaxTree syntaxTree = parser.parseExpressionSyntax("value + 1");

        assertThat(syntaxTree.root().rule()).isEqualTo("expression");
        assertThat(syntaxTree.root().span()).isEqualTo(new HogQlQuery.SourceSpan(0, 9, 1, 1, 1, 10));
        assertThatThrownBy(() -> parser.parseExpressionSyntax("value + 1 trailing"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("unexpected trailing input");
    }

    private static Stream<Node> nodes(Element element)
    {
        if (!(element instanceof Node node)) {
            return Stream.empty();
        }
        return Stream.concat(Stream.of(node), node.children().stream().flatMap(TestHogQlParser::nodes));
    }
}
