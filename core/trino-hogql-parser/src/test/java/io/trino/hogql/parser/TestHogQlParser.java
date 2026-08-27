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
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsList;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnsRegex;
import io.trino.hogql.parser.tree.HogQlQuery.CommonTableReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.InSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IntervalUnit;
import io.trino.hogql.parser.tree.HogQlQuery.JoinOn;
import io.trino.hogql.parser.tree.HogQlQuery.JoinRelation;
import io.trino.hogql.parser.tree.HogQlQuery.JoinType;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.MemberAccessExpression;
import io.trino.hogql.parser.tree.HogQlQuery.NullPlacement;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.ScalarSubqueryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperation;
import io.trino.hogql.parser.tree.HogQlQuery.SetOperationType;
import io.trino.hogql.parser.tree.HogQlQuery.SortDirection;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.StarReplacement;
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
    public void testBuildsCanonicalIntervalExpressions()
    {
        HogQlQuery query = parser.parseStatement("SELECT INTERVAL 1 WEEK, INTERVAL event QUARTER, INTERVAL '5 months'");

        assertThat(query.projections()).extracting(projection -> ((IntervalExpression) ((ExpressionProjection) projection).expression()).unit())
                .containsExactly(IntervalUnit.WEEK, IntervalUnit.QUARTER, IntervalUnit.MONTH);
        IntervalExpression stringInterval = (IntervalExpression) ((ExpressionProjection) query.projections().get(2)).expression();
        assertThat(stringInterval.value()).isInstanceOfSatisfying(Literal.class, value -> {
            assertThat(value.kind()).isEqualTo(HogQlQuery.LiteralKind.INTEGER);
            assertThat(value.value()).isEqualTo("5");
        });
    }

    @Test
    public void testRejectsInvalidCombinedStringIntervals()
    {
        assertThatThrownBy(() -> parser.parseStatement("SELECT INTERVAL 'twenty days'"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("Unsupported interval count: 'twenty' is not a valid integer");
        assertThatThrownBy(() -> parser.parseStatement("SELECT INTERVAL '9223372036854775808 day'"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("Unsupported interval count: '9223372036854775808' is too large");
        assertThatThrownBy(() -> parser.parseStatement("SELECT INTERVAL '1 SECOND'"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("Unsupported interval unit: SECOND");
        assertThatThrownBy(() -> parser.parseStatement("SELECT INTERVAL '1 dayss'"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("Unsupported interval unit: dayss");
        assertThatThrownBy(() -> parser.parseStatement("SELECT INTERVAL 'x'"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("Unsupported interval type: must be in the format '<count> <unit>'");
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
    public void testBuildsQualifiedStarWithExclusionsAndSourceSpans()
    {
        HogQlQuery query = parser.parseStatement("SELECT \"Event Alias\".* EXCLUDE (\"event\", personId) FROM events AS \"Event Alias\"");

        Star star = (Star) query.projections().getFirst();
        assertThat(star.qualifier()).singleElement().satisfies(qualifier -> {
            assertThat(qualifier.value()).isEqualTo("Event Alias");
            assertThat(qualifier.delimited()).isTrue();
            assertThat(qualifier.span()).isEqualTo(new HogQlQuery.SourceSpan(7, 20, 1, 8, 1, 21));
        });
        assertThat(star.exclusions()).extracting(exclusion -> exclusion.parts().getFirst().value()).containsExactly("event", "personId");
        assertThat(star.exclusions()).extracting(exclusion -> exclusion.parts().getFirst().delimited()).containsExactly(true, false);
        assertThat(star.exclusions().getFirst().span()).isEqualTo(new HogQlQuery.SourceSpan(32, 39, 1, 33, 1, 40));
        assertThat(star.exclusions().get(1).span()).isEqualTo(new HogQlQuery.SourceSpan(41, 49, 1, 42, 1, 50));
        assertThat(star.span()).isEqualTo(new HogQlQuery.SourceSpan(7, 50, 1, 8, 1, 51));
    }

    @Test
    public void testPreservesQualifiedStarExclusionPath()
    {
        Star star = (Star) parser.parseStatement("SELECT * EXCLUDE (analytics.events.event) FROM events").projections().getFirst();

        assertThat(star.exclusions()).singleElement().satisfies(exclusion ->
                assertThat(exclusion.parts()).extracting(HogQlQuery.Identifier::value).containsExactly("analytics", "events", "event"));
    }

    @Test
    public void testBuildsColumnsSelectorsAndReplacementAst()
    {
        HogQlQuery query = parser.parseStatement(
                "SELECT COLUMNS('^(event|person)'), COLUMNS(event, personId + 1), " +
                        "COLUMNS(events.* REPLACE (personId AS event, event AS \"personId\")) FROM events");

        ColumnsRegex regex = (ColumnsRegex) query.projections().getFirst();
        assertThat(regex.pattern()).isEqualTo("^(event|person)");
        assertThat(regex.patternSpan()).isEqualTo(new HogQlQuery.SourceSpan(15, 32, 1, 16, 1, 33));

        ColumnsList explicit = (ColumnsList) query.projections().get(1);
        assertThat(explicit.expressions()).hasSize(2);
        assertThat(explicit.expressions().getFirst()).isInstanceOf(ColumnReference.class);
        assertThat(explicit.expressions().get(1)).isInstanceOf(BinaryExpression.class);

        Star star = (Star) query.projections().get(2);
        assertThat(star.qualifier()).extracting(HogQlQuery.Identifier::value).containsExactly("events");
        assertThat(star.replacements()).extracting(replacement -> replacement.target().value()).containsExactly("event", "personId");
        assertThat(star.replacements()).extracting(replacement -> replacement.target().delimited()).containsExactly(false, true);
        assertThat(star.replacements()).extracting(StarReplacement::expression).allSatisfy(expression ->
                assertThat(expression).isInstanceOf(ColumnReference.class));
    }

    @Test
    public void testPreservesParameterizedAndNestedCastTypeSyntax()
    {
        HogQlQuery query = parser.parseStatement(
                "SELECT CAST(value AS Nullable(Decimal(18, 4))), " +
                        "TRY_CAST(value AS Tuple(id Int64, payload Array(String)))");

        CastExpression decimal = (CastExpression) ((ExpressionProjection) query.projections().getFirst()).expression();
        CastExpression tuple = (CastExpression) ((ExpressionProjection) query.projections().get(1)).expression();

        assertThat(decimal.type().value()).isEqualTo("Nullable(Decimal(18, 4))");
        assertThat(decimal.type().span()).isEqualTo(new HogQlQuery.SourceSpan(21, 45, 1, 22, 1, 46));
        assertThat(decimal.safe()).isFalse();
        assertThat(decimal.typeDialect()).isEqualTo(HogQlQuery.CastTypeDialect.HOGQL);
        assertThat(tuple.type().value()).isEqualTo("Tuple(id Int64, payload Array(String))");
        assertThat(tuple.safe()).isTrue();
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
    public void testBuildsPropertyAccessPaths()
    {
        HogQlQuery query = parser.parseStatement("SELECT properties.browser, e.properties.browser FROM events e");

        ColumnReference unqualified = (ColumnReference) ((ExpressionProjection) query.projections().getFirst()).expression();
        assertThat(unqualified.parts()).extracting(HogQlQuery.Identifier::value).containsExactly("properties", "browser");
        assertThat(unqualified.span()).isEqualTo(new HogQlQuery.SourceSpan(7, 25, 1, 8, 1, 26));

        ColumnReference qualified = (ColumnReference) ((ExpressionProjection) query.projections().get(1)).expression();
        assertThat(qualified.parts()).extracting(HogQlQuery.Identifier::value).containsExactly("e", "properties", "browser");
        assertThat(qualified.span()).isEqualTo(new HogQlQuery.SourceSpan(27, 47, 1, 28, 1, 48));
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
    public void testBuildsCorrelatedInSubqueryWithSourceSpans()
    {
        HogQlQuery query = parser.parseStatement(
                """
                SELECT o.orderkey
                FROM orders o
                WHERE o.custkey IN (
                    SELECT c.custkey
                    FROM customer c
                    WHERE c.custkey = o.custkey
                )
                """);

        InSubqueryExpression predicate = (InSubqueryExpression) query.where().orElseThrow();
        BinaryExpression correlation = (BinaryExpression) predicate.query().where().orElseThrow();
        ColumnReference outerReference = (ColumnReference) correlation.right();

        assertThat(predicate.predicateSpan()).isEqualTo(new HogQlQuery.SourceSpan(48, 127, 3, 17, 7, 2));
        assertThat(predicate.query().span()).isEqualTo(new HogQlQuery.SourceSpan(57, 125, 4, 5, 6, 32));
        assertThat(outerReference.parts()).extracting(HogQlQuery.Identifier::value).containsExactly("o", "custkey");
        assertThat(outerReference.span()).isEqualTo(new HogQlQuery.SourceSpan(116, 125, 6, 23, 6, 32));
    }

    @Test
    public void testBuildsCorrelatedScalarSubqueryWithSourceSpans()
    {
        HogQlQuery query = parser.parseStatement(
                "SELECT o.orderkey, (SELECT c.custkey FROM customer c WHERE c.custkey = o.custkey) AS matched FROM orders o");

        ScalarSubqueryExpression subquery = (ScalarSubqueryExpression) ((ExpressionProjection) query.projections().get(1)).expression();
        BinaryExpression correlation = (BinaryExpression) subquery.query().where().orElseThrow();
        ColumnReference outerReference = (ColumnReference) correlation.right();

        assertThat(subquery.span()).isEqualTo(new HogQlQuery.SourceSpan(19, 81, 1, 20, 1, 82));
        assertThat(subquery.query().span()).isEqualTo(new HogQlQuery.SourceSpan(20, 80, 1, 21, 1, 81));
        assertThat(outerReference.parts()).extracting(HogQlQuery.Identifier::value).containsExactly("o", "custkey");
        assertThat(outerReference.span()).isEqualTo(new HogQlQuery.SourceSpan(71, 80, 1, 72, 1, 81));
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
