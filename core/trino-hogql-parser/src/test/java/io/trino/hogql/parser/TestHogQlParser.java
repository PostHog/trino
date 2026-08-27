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
import io.trino.hogql.parser.tree.HogQlQuery.AliasedRelation;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinType;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.NullPlacement;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperationType;
import io.trino.hogql.parser.tree.HogQlQuery.SortDirection;
import io.trino.hogql.parser.tree.HogQlQuery.SubqueryRelation;
import io.trino.hogql.parser.tree.HogQlQuery.SubscriptExpression;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ValuesRelation;
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
            "SELECT * FROM first NATURAL JOIN second",
            "SELECT * FROM first ANY INNER JOIN second ON first.id = second.id",
            "SELECT * FROM first LEFT SEMI JOIN second ON first.id = second.id",
            "SELECT * FROM first ASOF JOIN second ON first.id = second.id",
            "SELECT * FROM first POSITIONAL JOIN second",
            "SELECT * FROM first, second",
            "SELECT function(1)(value)",
            "SELECT tuple_value.0",
            "SELECT array_value[1:2]",
            "SELECT array_value?.[1]",
            "SELECT tuple_value?.1",
            "DROP TABLE events",
            "SELECT 1; SELECT 2",
            "SELECT * FROM",
            "SELECT {1 + 2}",
            "SELECT 1 GROUP BY ALL",
            "WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x",
            "WITH x AS MATERIALIZED (SELECT 1) SELECT * FROM x",
            "WITH x AS NOT MATERIALIZED (SELECT 1) SELECT * FROM x",
            "WITH x USING KEY (id) AS (SELECT 1) SELECT * FROM x",
            "WITH 1 AS x SELECT x",
            "WITH x AS (SELECT * FROM x) SELECT * FROM x",
            "WITH x AS (SELECT 1), y AS (WITH x AS (SELECT * FROM x) SELECT * FROM x) SELECT * FROM y",
            "WITH x AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x",
            "SELECT * FROM events e JOIN (SELECT * FROM persons WHERE personId = e.person_id) p ON true",
            "SELECT * FROM events e JOIN (SELECT person_id) p ON true",
            "SELECT 1 UNION BY NAME SELECT 1",
            "SELECT 1 UNION ALL BY NAME SELECT 1",
            "SELECT 1 INTERSECT BY NAME SELECT 1",
            "SELECT 1 EXCEPT BY NAME SELECT 1",
            "SELECT * FROM (VALUES (1, 2), (3)) AS data(first, second)",
            "SELECT * FROM (VALUES (1, 2)) AS data(first)",
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
    public void testBuildsCollectionAccessWithSourceSpans()
    {
        HogQlQuery query = parser.parseStatement("SELECT [10,{element}][{index}], (1, 'two').2, {payload}.plan");

        SubscriptExpression arrayAccess = (SubscriptExpression) ((ExpressionProjection) query.projections().getFirst()).expression();
        assertThat(arrayAccess.base()).isInstanceOf(HogQlQuery.ArrayExpression.class);
        assertThat(arrayAccess.index()).isInstanceOf(Placeholder.class);
        assertThat(arrayAccess.span()).isEqualTo(new HogQlQuery.SourceSpan(7, 30, 1, 8, 1, 31));
        assertThat(arrayAccess.index().span()).isEqualTo(new HogQlQuery.SourceSpan(22, 29, 1, 23, 1, 30));

        SubscriptExpression tupleAccess = (SubscriptExpression) ((ExpressionProjection) query.projections().get(1)).expression();
        assertThat(tupleAccess.base()).isInstanceOf(TupleExpression.class);
        assertThat(tupleAccess.index()).isInstanceOf(HogQlQuery.Literal.class);
        assertThat(tupleAccess.span()).isEqualTo(new HogQlQuery.SourceSpan(32, 44, 1, 33, 1, 45));
        assertThat(tupleAccess.index().span()).isEqualTo(new HogQlQuery.SourceSpan(43, 44, 1, 44, 1, 45));

        MemberAccessExpression memberAccess = (MemberAccessExpression) ((ExpressionProjection) query.projections().get(2)).expression();
        assertThat(memberAccess.base()).isInstanceOf(Placeholder.class);
        assertThat(memberAccess.member().value()).isEqualTo("plan");
        assertThat(memberAccess.span()).isEqualTo(new HogQlQuery.SourceSpan(46, 60, 1, 47, 1, 61));
        assertThat(memberAccess.member().span()).isEqualTo(new HogQlQuery.SourceSpan(56, 60, 1, 57, 1, 61));
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
    public void testBuildsAliasedJoinAstWithSourceSpans()
    {
        HogQlQuery query = parser.parseStatement("SELECT e.id\nFROM events AS e\nLEFT JOIN persons AS p ON e.person_id = p.id");

        JoinRelation join = (JoinRelation) query.from().orElseThrow();
        assertThat(join.type()).isEqualTo(JoinType.LEFT);
        assertThat(join.span()).isEqualTo(new HogQlQuery.SourceSpan(17, 73, 2, 6, 3, 45));
        AliasedRelation left = (AliasedRelation) join.left();
        assertThat(left.alias().value()).isEqualTo("e");
        assertThat(left.span()).isEqualTo(new HogQlQuery.SourceSpan(17, 28, 2, 6, 2, 17));
        JoinOn criteria = (JoinOn) join.criteria().orElseThrow();
        assertThat(criteria.span()).isEqualTo(new HogQlQuery.SourceSpan(52, 73, 3, 24, 3, 45));
    }

    @Test
    public void testBuildsCteAndDerivedTableAstWithSourceSpans()
    {
        String hogql = "WITH base(id) AS (SELECT {cte}), next AS (SELECT id FROM base)\n" +
                "SELECT d.id FROM (SELECT id FROM next WHERE id = {inner}) AS d WHERE d.id = {outer}";

        HogQlQuery query = parser.parseStatement(hogql);

        assertThat(query.with()).hasSize(2);
        assertThat(query.with().getFirst().name().value()).isEqualTo("base");
        assertThat(query.with().getFirst().columnAliases()).singleElement().satisfies(alias -> assertThat(alias.value()).isEqualTo("id"));
        assertThat(query.with().getFirst().span()).isEqualTo(new HogQlQuery.SourceSpan(5, 31, 1, 6, 1, 32));
        assertThat(query.with().get(1).query().from()).get().isInstanceOf(CommonTableReference.class);
        AliasedRelation derived = (AliasedRelation) query.from().orElseThrow();
        assertThat(derived.alias().value()).isEqualTo("d");
        SubqueryRelation subquery = (SubqueryRelation) derived.relation();
        assertThat(subquery.query().from()).get().isInstanceOf(CommonTableReference.class);
        assertThat(subquery.span()).isEqualTo(new HogQlQuery.SourceSpan(80, 120, 2, 18, 2, 58));
    }

    @Test
    public void testBuildsSetOperationAstWithPrecedenceAndSourceSpans()
    {
        HogQlQuery query = parser.parseStatement("SELECT {left}\nUNION ALL\nSELECT {middle}\nINTERSECT\nSELECT {right}");

        SetOperation union = (SetOperation) query.body();
        assertThat(union.type()).isEqualTo(SetOperationType.UNION);
        assertThat(union.distinct()).isFalse();
        assertThat(union.operatorSpan()).isEqualTo(new HogQlQuery.SourceSpan(14, 23, 2, 1, 2, 10));
        SetOperation intersect = (SetOperation) union.right().body();
        assertThat(intersect.type()).isEqualTo(SetOperationType.INTERSECT);
        assertThat(intersect.distinct()).isTrue();
        assertThat(intersect.operatorSpan()).isEqualTo(new HogQlQuery.SourceSpan(40, 49, 4, 1, 4, 10));
    }

    @Test
    public void testBuildsValuesRelationWithColumnSchema()
    {
        HogQlQuery query = parser.parseStatement("SELECT *\nFROM (VALUES (1, {first}), (2, {second})) AS data(id, label)");

        AliasedRelation alias = (AliasedRelation) query.from().orElseThrow();
        assertThat(alias.columnAliases())
                .extracting(HogQlQuery.Identifier::value)
                .containsExactly("id", "label");
        ValuesRelation values = (ValuesRelation) alias.relation();
        assertThat(values.rows()).hasSize(2).allSatisfy(row -> assertThat(row).hasSize(2));
        assertThat(values.span().startLine()).isEqualTo(2);
        assertThat(values.span().startColumn()).isEqualTo(6);
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
