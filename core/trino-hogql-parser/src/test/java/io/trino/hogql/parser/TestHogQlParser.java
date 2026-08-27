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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
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
            "SELECT count(*)",
            "SELECT * FROM first JOIN second",
            "DROP TABLE events",
            "SELECT 1; SELECT 2",
            "SELECT * FROM",
            "SELECT {value}",
            "SELECT 1 GROUP BY 1",
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

    private static Stream<Node> nodes(Element element)
    {
        if (!(element instanceof Node node)) {
            return Stream.empty();
        }
        return Stream.concat(Stream.of(node), node.children().stream().flatMap(TestHogQlParser::nodes));
    }
}
