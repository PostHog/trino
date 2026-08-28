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

import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Pivot;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WithQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestHogQlCompiler
{
    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT 1",
            "select 'event'",
            "SELECT TRUE, FALSE, NULL",
            "SELECT * FROM events",
            "SELECT event, properties FROM ducklake.default.events",
            "SELECT * FROM \"MiXeD\".\"Event Table\"",
            "SELECT event + 1 * 2 FROM events WHERE event >= 3 AND NOT false",
            "SELECT lower(event) AS lowered FROM events",
            "SELECT 01",
            "SELECT -1, +2",
    })
    public void testLowersToEquivalentStockTrinoAst(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testQuotesHogQlPropertyIdentifiersForTrino()
    {
        assertThat(compiler.compile("SELECT properties.$user_id, properties.$host FROM events"))
                .isEqualTo(sqlParser.createStatement("SELECT properties.\"$user_id\", properties.\"$host\" FROM events"));
    }

    @Test
    public void testLowersDistinctOrderingAndClickHousePaginationOrder()
    {
        Statement statement = compiler.compile("SELECT DISTINCT event FROM events ORDER BY event DESC NULLS FIRST LIMIT 10 OFFSET 2");

        assertThat(statement).isEqualTo(sqlParser.createStatement("SELECT DISTINCT event FROM events ORDER BY event DESC NULLS FIRST OFFSET 2 LIMIT 10"));
    }

    @Test
    public void testLowersLimitByToPartitionedRowNumber()
    {
        Statement statement = compiler.compile(
                "SELECT event, created_at AS ts FROM events ORDER BY ts DESC LIMIT 1, 2 BY event LIMIT 10");

        assertThat(statement).isEqualTo(sqlParser.createStatement(
                "SELECT __hogql_limit_by_ranked.__hogql_limit_by_column_0 AS event, " +
                        "__hogql_limit_by_ranked.__hogql_limit_by_column_1 AS ts " +
                        "FROM (" +
                        "SELECT __hogql_limit_by_base.__hogql_limit_by_column_0 AS __hogql_limit_by_column_0, " +
                        "__hogql_limit_by_base.__hogql_limit_by_column_1 AS __hogql_limit_by_column_1, " +
                        "row_number() OVER (PARTITION BY __hogql_limit_by_base.__hogql_limit_by_column_0 " +
                        "ORDER BY __hogql_limit_by_base.__hogql_limit_by_column_1 DESC) AS __hogql_limit_by_row_number " +
                        "FROM (SELECT event AS __hogql_limit_by_column_0, created_at AS __hogql_limit_by_column_1 FROM events) " +
                        "AS __hogql_limit_by_base" +
                        ") AS __hogql_limit_by_ranked " +
                        "WHERE __hogql_limit_by_ranked.__hogql_limit_by_row_number > 1 " +
                        "AND __hogql_limit_by_ranked.__hogql_limit_by_row_number <= 1 + 2 " +
                        "ORDER BY __hogql_limit_by_ranked.__hogql_limit_by_column_1 DESC LIMIT 10"));
    }

    @Test
    public void testWrapsSetOperandsWithLocalClauses()
    {
        Statement statement = compiler.compile("SELECT 1 ORDER BY 1 LIMIT 1 UNION ALL SELECT 2");

        assertThat(statement).isEqualTo(sqlParser.createStatement("(SELECT 1 ORDER BY 1 LIMIT 1) UNION ALL SELECT 2"));
    }

    @Test
    public void testPreservesUnqualifiedOutputReferencesInDerivedSetQueries()
    {
        Statement statement = compiler.compile(
                "SELECT * FROM (SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value) AS nested");

        assertThat(statement).isEqualTo(sqlParser.createStatement(
                "SELECT * FROM (SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value) AS nested"));
    }

    @Test
    public void testLowersNumbersTableFunction()
    {
        Statement statement = compiler.compile("SELECT number FROM numbers(5)");

        assertThat(statement).isEqualTo(sqlParser.createStatement(
                "SELECT number FROM UNNEST(if(5 <= 0, CAST(ARRAY[] AS array(bigint)), sequence(0, 5 - 1))) AS numbers(number)"));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
                                            SELECT [1, 2, 3]                 | SELECT ARRAY[1, 2, 3]
                                            SELECT ARRAY[]                   | SELECT ARRAY[]
                                            SELECT (1, 'example')            | SELECT (1, 'example')
                                            SELECT (1,)                       | SELECT ROW(1)
                                            SELECT [1, 2, 3][1]              | SELECT ARRAY[1, 2, 3][1]
                                            SELECT (1, 'example').2          | SELECT ROW(1, 'example')[2]
                                            SELECT attributes['plan']        | SELECT attributes['plan']
                                            SELECT nested[position[1]]       | SELECT nested[position[1]]
                                            SELECT [payload][1].plan          | SELECT ARRAY[payload][1].plan
                                            SELECT value BETWEEN 1 AND 10    | SELECT value BETWEEN 1 AND 10
                                            SELECT value NOT BETWEEN 1 AND 10| SELECT value NOT BETWEEN 1 AND 10
                                            SELECT value IS NULL             | SELECT value IS NULL
                                            SELECT value IS NOT NULL         | SELECT value IS NOT NULL
                                            SELECT value IN (1, 2, 3)        | SELECT value IN (1, 2, 3)
                                            SELECT value IN (1)              | SELECT value IN (1)
                                            SELECT value NOT IN (1, 2, 3)    | SELECT value NOT IN (1, 2, 3)
                                            SELECT value IN [1, 2, 3]        | SELECT value IN (1, 2, 3)
                                            SELECT value NOT IN [1]          | SELECT value NOT IN (1)
                                            SELECT value IN []               | SELECT IF(value IS NULL, NULL, false)
                                            SELECT value NOT IN []           | SELECT IF(value IS NULL, NULL, true)
                                            SELECT value ?? fallback         | SELECT coalesce(value, fallback)
                                            SELECT first ?? second ?? third   | SELECT coalesce(coalesce(first, second), third)
                                            SELECT value LIKE 'pro%'         | SELECT value LIKE 'pro%'
                                            SELECT value NOT LIKE 'pro%'     | SELECT value NOT LIKE 'pro%'
                                            SELECT value ILIKE 'Pro%'        | SELECT lower(value) LIKE lower('Pro%')
                                            SELECT value NOT ILIKE 'Pro%'    | SELECT lower(value) NOT LIKE lower('Pro%')
                                            SELECT 1.5                       | SELECT 1.5E0
                                            SELECT .5                        | SELECT .5E0
                                            SELECT 1.                        | SELECT 1E0
                                            SELECT -1.5                      | SELECT -1.5E0
                                            SELECT +1.5                      | SELECT +1.5E0
                                            SELECT 1e3                       | SELECT 1E3
                                            SELECT map(x -> x + 1, [1])      | SELECT map(x -> x + 1, ARRAY[1])
                                            SELECT map((x, y) -> x + y, [1]) | SELECT map((x, y) -> x + y, ARRAY[1])
                                            SELECT map(lambda x: x + 1, [1]) | SELECT map(x -> x + 1, ARRAY[1])
                                            """)
    public void testLowersCollectionAndPredicateExpressions(String hogql, String trinoSql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(trinoSql));
    }

    @Test
    public void testLowersConcatenationOperator()
    {
        assertThat(compiler.compile("SELECT first || second"))
                .isEqualTo(sqlParser.createStatement("SELECT first || second"));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
                                            SELECT item FROM events ARRAY JOIN attributes AS item                  | SELECT item FROM events CROSS JOIN UNNEST(attributes) AS __hogql_array_join(item)
                                            SELECT item FROM events LEFT ARRAY JOIN attributes AS item             | SELECT item FROM events LEFT JOIN UNNEST(attributes) AS __hogql_array_join(item) ON TRUE
                                            SELECT item, key FROM events ARRAY JOIN attributes AS item, keys AS key | SELECT item, key FROM events CROSS JOIN UNNEST(attributes, keys) AS __hogql_array_join(item, key)
                                            SELECT item ARRAY JOIN [1] AS item                                      | SELECT item FROM UNNEST(ARRAY[1]) AS __hogql_array_join(item)
                                            """)
    public void testLowersArrayJoin(String hogql, String trinoSql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(trinoSql));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
                                            SELECT INTERVAL 2 SECOND     | SELECT 2 * INTERVAL '1' SECOND
                                            SELECT INTERVAL value MINUTE | SELECT value * INTERVAL '1' MINUTE
                                            SELECT INTERVAL 2 HOUR       | SELECT 2 * INTERVAL '1' HOUR
                                            SELECT INTERVAL 2 DAY        | SELECT 2 * INTERVAL '1' DAY
                                            SELECT INTERVAL 2 WEEK       | SELECT 2 * INTERVAL '7' DAY
                                            SELECT INTERVAL 2 MONTH      | SELECT 2 * INTERVAL '1' MONTH
                                            SELECT INTERVAL 2 QUARTER    | SELECT 2 * INTERVAL '3' MONTH
                                            SELECT INTERVAL value YEAR   | SELECT value * INTERVAL '1' YEAR
                                            SELECT INTERVAL '5 months'   | SELECT 5 * INTERVAL '1' MONTH
                                            """)
    public void testLowersCanonicalIntervals(String hogql, String trinoSql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(trinoSql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testBindsPlaceholdersInsideCollectionSubscriptsBySourceOrder()
    {
        HogQlCompilationResult result = compiler.compile(
                "SELECT [{element}][{index}], ({left}, {right}).2, {object}.field",
                Map.of(
                        "element", typedValue("element"),
                        "index", typedValue("index"),
                        "left", typedValue("left"),
                        "right", typedValue("right"),
                        "object", typedValue("object")));

        assertThat(result.parameterNames()).containsExactly("element", "index", "left", "right", "object");
        assertThat(parameters(result.statement()))
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void testPreservesCollectionSubscriptSourceLocations()
    {
        Query query = (Query) compiler.compile("SELECT [10, 20][2], (1, 'two').2");
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        SubscriptExpression arrayAccess = (SubscriptExpression) ((SingleColumn) querySpecification.getSelect().getSelectItems().getFirst()).getExpression();
        SubscriptExpression tupleAccess = (SubscriptExpression) ((SingleColumn) querySpecification.getSelect().getSelectItems().get(1)).getExpression();

        assertThat(arrayAccess.getLocation()).contains(new NodeLocation(1, 8));
        assertThat(arrayAccess.getIndex().getLocation()).contains(new NodeLocation(1, 17));
        assertThat(tupleAccess.getLocation()).contains(new NodeLocation(1, 21));
        assertThat(tupleAccess.getIndex().getLocation()).contains(new NodeLocation(1, 32));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT CASE WHEN enabled THEN 1 ELSE 0 END",
            "SELECT CASE value WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
    })
    public void testLowersCaseAndCastExpressions(String hogql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(hogql));
    }

    @ParameterizedTest
    @MethodSource("representableHogQlCastTypes")
    public void testLowersRepresentableHogQlCastTypes(String hogql, String trinoSql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(trinoSql));
    }

    @ParameterizedTest
    @MethodSource("unsupportedHogQlCastTypes")
    public void testRejectsHogQlCastTypesWithoutExactTrinoRepresentation(String type, String reason)
    {
        String hogql = "SELECT CAST(value AS " + type + ")";

        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> compiler.compile(hogql));

        assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, hogql.indexOf(type) + 1));
        assertThat(exception)
                .hasMessageContaining("HogQL cast type cannot be represented exactly in Trino")
                .hasMessageContaining(reason);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT event, count(*) AS total FROM events GROUP BY event HAVING count(*) > 1",
            "SELECT lower(event), count(DISTINCT person_id), sum(revenue) FROM events GROUP BY lower(event) HAVING sum(revenue) > 10",
            "SELECT count(*) FILTER (WHERE enabled) FROM events",
            "SELECT array_agg(event ORDER BY timestamp DESC) FROM events",
    })
    public void testLowersOrdinaryGroupingHavingAndAggregateFunctions(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testBindsPlaceholdersAcrossGroupingHavingAndAggregateModifiers()
    {
        String hogql = "SELECT array_agg({value} ORDER BY {sort}) FILTER (WHERE {filter}) FROM events " +
                "GROUP BY {group} HAVING sum({having}) > {threshold}";
        Map<String, HogQlTypedValue> bindings = Map.of(
                "value", typedValue("value"),
                "sort", typedValue("sort"),
                "filter", typedValue("filter"),
                "group", typedValue("group"),
                "having", typedValue("having"),
                "threshold", typedValue("threshold"));

        HogQlCompilationResult result = compiler.compile(hogql, bindings);

        assertThat(result.parameterNames()).containsExactly("value", "sort", "filter", "group", "having", "threshold");
        assertThat(parameters(result.statement()))
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2, 3, 4, 5);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT e.id FROM events AS e",
            "SELECT date.id FROM events AS date",
            "SELECT \"e\".id FROM events AS \"e\"",
            "SELECT e.id FROM events e JOIN persons p ON e.person_id = p.id",
            "SELECT * FROM events LEFT OUTER JOIN persons USING (id)",
            "SELECT * FROM events RIGHT JOIN persons ON events.id = persons.id",
            "SELECT * FROM events FULL OUTER JOIN persons USING (id, team_id)",
            "SELECT * FROM events CROSS JOIN persons",
            "SELECT * FROM (events CROSS JOIN persons)",
    })
    public void testLowersAliasesAndStockJoins(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testLowersUnparenthesizedUsing()
    {
        assertThat(compiler.compile("SELECT * FROM events JOIN persons USING id"))
                .isEqualTo(sqlParser.createStatement("SELECT * FROM events JOIN persons USING (id)"));
    }

    @Test
    public void testLowersPivotToEquivalentStockAstWithSourceLocations()
    {
        String hogql = "SELECT * FROM orders PIVOT (sum(totalprice) AS total FOR (orderstatus, custkey) " +
                "IN (('F', 1) AS filled, ('O', 2) AS open) GROUP BY clerk)";

        Query query = (Query) compiler.compile(hogql);
        Pivot pivot = (Pivot) ((QuerySpecification) query.getQueryBody()).getFrom().orElseThrow();

        assertThat(query).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(query))).isEqualTo(query);
        assertThat(pivot.getLocation()).contains(new NodeLocation(1, hogql.indexOf("orders") + 1));
        assertThat(pivot.getAggregations()).singleElement().satisfies(aggregation -> {
            assertThat(aggregation.getLocation()).contains(new NodeLocation(1, hogql.indexOf("sum(totalprice)") + 1));
            assertThat(aggregation.getAlias()).get().satisfies(alias ->
                    assertThat(alias.getLocation()).contains(new NodeLocation(1, hogql.indexOf("total FOR") + 1)));
        });
        assertThat(pivot.getPivotColumns()).hasSize(2);
        assertThat(pivot.getValueGroups()).hasSize(2);
        assertThat(pivot.getValueGroups().getFirst().getLocation())
                .contains(new NodeLocation(1, hogql.indexOf("('F', 1)") + 1));
        assertThat(pivot.getValueGroups().getFirst().getAlias()).get().satisfies(alias ->
                assertThat(alias.getLocation()).contains(new NodeLocation(1, hogql.indexOf("filled") + 1)));
        assertThat(pivot.getGroupBy()).isPresent();
    }

    @Test
    public void testBindsPlaceholdersInsidePivot()
    {
        HogQlCompilationResult result = compiler.compile(
                "SELECT * FROM orders PIVOT (sum({amount}) FOR orderstatus IN ({status}))",
                Map.of("amount", typedValue("amount"), "status", typedValue("status")));

        assertThat(result.parameterNames()).containsExactly("amount", "status");
        assertThat(parameters(result.statement())).extracting(Parameter::getId).containsExactly(0, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "WITH base AS (SELECT 1 AS id) SELECT id FROM base",
            "WITH base(id) AS (SELECT 1), next AS (SELECT id FROM base) SELECT id FROM next",
            "WITH base AS (WITH base AS (SELECT 1 AS id) SELECT id FROM base) SELECT id FROM base",
            "SELECT derived.id FROM (SELECT id FROM events) AS derived",
            "WITH base AS (SELECT id FROM events) SELECT derived.id FROM (SELECT id FROM base) derived",
    })
    public void testLowersCtesAndDerivedTables(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testLowersCorrelatedInSubqueryWithSourceLocations()
    {
        String hogql =
                """
                SELECT o.orderkey
                FROM orders o
                WHERE o.custkey IN (
                    SELECT c.custkey
                    FROM customer c
                    WHERE c.custkey = o.custkey
                )
                """;

        Query query = (Query) compiler.compile(hogql);
        Predicated where = (Predicated) ((QuerySpecification) query.getQueryBody()).getWhere().orElseThrow();
        InPredicate predicate = (InPredicate) where.getPredicate();
        SubqueryExpression subquery = (SubqueryExpression) predicate.getValueList();

        assertThat(query).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(where.getLocation()).contains(new NodeLocation(3, 17));
        assertThat(subquery.getLocation()).contains(new NodeLocation(4, 5));
        assertThat(subquery.getQuery().getLocation()).contains(new NodeLocation(4, 5));
    }

    @Test
    public void testLowersCorrelatedScalarSubqueryWithSourceLocations()
    {
        String hogql = "SELECT o.orderkey, (SELECT c.custkey FROM customer c WHERE c.custkey = o.custkey) AS matched FROM orders o";

        Query query = (Query) compiler.compile(hogql);
        SingleColumn projection = (SingleColumn) ((QuerySpecification) query.getQueryBody()).getSelect().getSelectItems().get(1);
        SubqueryExpression subquery = (SubqueryExpression) projection.getExpression();

        assertThat(query).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(subquery.getLocation()).contains(new NodeLocation(1, 20));
        assertThat(subquery.getQuery().getLocation()).contains(new NodeLocation(1, 21));
    }

    @Test
    public void testBindsPlaceholdersAcrossCteAndDerivedQueryScopes()
    {
        HogQlCompilationResult result = compiler.compile(
                "WITH base AS (SELECT {cte}) SELECT {outer} FROM (SELECT {derived} FROM base) nested WHERE {where}",
                Map.of(
                        "cte", typedValue("cte"),
                        "outer", typedValue("outer"),
                        "derived", typedValue("derived"),
                        "where", typedValue("where")));

        assertThat(result.parameterNames()).containsExactly("cte", "outer", "derived", "where");
        assertThat(parameters(result.statement()))
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2, 3);
    }

    @Test
    public void testPreservesCteAndDerivedTableSourceLocations()
    {
        Query query = (Query) compiler.compile("WITH base AS (SELECT id FROM events)\nSELECT d.id FROM (SELECT id FROM base) AS d");
        WithQuery commonTable = query.getWith().orElseThrow().getQueries().getFirst();
        AliasedRelation aliasedRelation = (AliasedRelation) ((QuerySpecification) query.getQueryBody()).getFrom().orElseThrow();
        TableSubquery derivedTable = (TableSubquery) aliasedRelation.getRelation();

        assertThat(query.getWith().orElseThrow().getLocation()).contains(new NodeLocation(1, 1));
        assertThat(commonTable.getLocation()).contains(new NodeLocation(1, 6));
        assertThat(commonTable.getQuery().getLocation()).contains(new NodeLocation(1, 15));
        assertThat(aliasedRelation.getLocation()).contains(new NodeLocation(2, 18));
        assertThat(derivedTable.getLocation()).contains(new NodeLocation(2, 18));
        assertThat(derivedTable.getQuery().getLocation()).contains(new NodeLocation(2, 19));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT 1 UNION SELECT 2",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 UNION DISTINCT SELECT 2",
            "SELECT 1 INTERSECT SELECT 2",
            "SELECT 1 INTERSECT ALL SELECT 2",
            "SELECT 1 INTERSECT DISTINCT SELECT 2",
            "SELECT 1 EXCEPT SELECT 2",
            "SELECT 1 EXCEPT ALL SELECT 2",
            "SELECT 1 UNION SELECT 2 INTERSECT SELECT 2",
            "SELECT 1 EXCEPT SELECT 2 UNION SELECT 3",
            "SELECT 1 UNION (SELECT 2 EXCEPT SELECT 3)",
            "WITH base AS (SELECT 1 AS id) SELECT id FROM base UNION SELECT id FROM base",
            "WITH base AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM base",
            "SELECT * FROM (SELECT 1 UNION ALL SELECT 2) AS data",
            "SELECT * FROM (VALUES (1, 'first'), (2, 'second')) AS data(id, label)",
    })
    public void testLowersSetOperationsAndValuesToStockTrinoAst(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testLowersSetOperationOrderLimitAndOffset()
    {
        Statement statement = compiler.compile("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 3 OFFSET 1");

        assertThat(statement).isEqualTo(sqlParser.createStatement("SELECT 1 UNION SELECT 2 ORDER BY 1 OFFSET 1 ROW LIMIT 3"));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @Test
    public void testBindsPlaceholdersAcrossSetBranchesAndValuesRows()
    {
        HogQlCompilationResult result = compiler.compile(
                "SELECT {left} UNION ALL SELECT {right} FROM (VALUES ({first}), ({second})) AS data(value)",
                Map.of(
                        "left", typedValue("left"),
                        "right", typedValue("right"),
                        "first", typedValue("first"),
                        "second", typedValue("second")));

        assertThat(result.parameterNames()).containsExactly("left", "right", "first", "second");
        assertThat(parameters(result.statement()))
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2, 3);
    }

    @Test
    public void testPreservesSetOperationAndValuesSourceLocations()
    {
        Query query = (Query) compiler.compile("SELECT 1\nUNION ALL\nSELECT * FROM (VALUES (2), (3)) data(value)");
        Union union = (Union) query.getQueryBody();
        QuerySpecification right = (QuerySpecification) union.getRelations().get(1);
        AliasedRelation alias = (AliasedRelation) right.getFrom().orElseThrow();
        TableSubquery valuesSubquery = (TableSubquery) alias.getRelation();
        Values values = (Values) valuesSubquery.getQuery().getQueryBody();

        assertThat(union.getLocation()).contains(new NodeLocation(2, 1));
        assertThat(alias.getLocation()).contains(new NodeLocation(3, 15));
        assertThat(valuesSubquery.getLocation()).contains(new NodeLocation(3, 15));
        assertThat(values.getLocation()).contains(new NodeLocation(3, 16));
    }

    @Test
    public void testPreservesAliasedJoinSourceLocations()
    {
        Query query = (Query) compiler.compile("SELECT e.id\nFROM events AS e\nLEFT JOIN persons AS p ON e.person_id = p.id");
        Join join = (Join) ((QuerySpecification) query.getQueryBody()).getFrom().orElseThrow();
        AliasedRelation left = (AliasedRelation) join.getLeft();
        JoinOn criteria = (JoinOn) join.getCriteria().orElseThrow();

        assertThat(join.getLocation()).contains(new NodeLocation(2, 6));
        assertThat(left.getLocation()).contains(new NodeLocation(2, 6));
        assertThat(left.getAlias().getLocation()).contains(new NodeLocation(2, 16));
        assertThat(criteria.getExpression().getLocation()).contains(new NodeLocation(3, 27));
    }

    @Test
    public void testBindsPlaceholdersInJoinCriteriaBySourceOrder()
    {
        HogQlCompilationResult result = compiler.compile(
                "SELECT {projection} FROM events e JOIN persons p ON e.id = {join_value} WHERE {where_value}",
                Map.of(
                        "projection", typedValue("projection"),
                        "join_value", typedValue("join"),
                        "where_value", typedValue("where")));

        assertThat(result.parameterNames()).containsExactly("projection", "join_value", "where_value");
        assertThat(parameters(result.statement()))
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT value BETWEEN 1 AND 10",
            "SELECT value NOT BETWEEN 1 AND 10",
            "SELECT value IS NULL",
            "SELECT value IS NOT NULL",
            "SELECT value IN (1, 2, 3)",
            "SELECT value NOT IN (1, 2, 3)",
    })
    public void testPreservesPredicateSourceLocations(String hogql)
    {
        Query query = (Query) compiler.compile(hogql);
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        SingleColumn column = (SingleColumn) querySpecification.getSelect().getSelectItems().getFirst();
        Predicated predicated = (Predicated) column.getExpression();

        assertThat(predicated.getLocation()).contains(new NodeLocation(1, 14));
        assertThat(predicated.getPredicate().getLocation()).contains(new NodeLocation(1, 14));
    }

    @Test
    public void testPreservesQuotedIdentifiersAndSourceLocations()
    {
        Statement statement = compiler.compile(
                """
                SELECT
                    `Event Name`,
                    *
                FROM
                    `Analytics`.`Event Table`
                """);

        Query query = (Query) statement;
        QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
        SingleColumn column = (SingleColumn) querySpecification.getSelect().getSelectItems().getFirst();
        Identifier columnIdentifier = (Identifier) column.getExpression();
        AllColumns allColumns = (AllColumns) querySpecification.getSelect().getSelectItems().get(1);
        Table table = (Table) querySpecification.getFrom().orElseThrow();

        assertThat(query.getLocation()).contains(new NodeLocation(1, 1));
        assertThat(column.getLocation()).contains(new NodeLocation(2, 5));
        assertThat(columnIdentifier.getValue()).isEqualTo("Event Name");
        assertThat(columnIdentifier.isDelimited()).isTrue();
        assertThat(allColumns.getLocation()).contains(new NodeLocation(3, 5));
        assertThat(table.getLocation()).contains(new NodeLocation(5, 5));
        assertThat(table.getName().getOriginalParts())
                .extracting(Identifier::getValue)
                .containsExactly("Analytics", "Event Table");
        assertThat(table.getName().getOriginalParts())
                .allMatch(Identifier::isDelimited);
    }

    @Test
    public void testLowersNamedPlaceholdersToOrderedPositionalParameters()
    {
        String firstValue = "sensitive-first-value";
        String laterValue = "sensitive-later-value";
        Map<String, HogQlTypedValue> bindings = new LinkedHashMap<>();
        bindings.put("first", typedValue(firstValue));
        bindings.put("later", typedValue(laterValue));

        HogQlCompilationResult result = compiler.compile("SELECT {later},\n {first} + {later}", bindings);
        List<Parameter> parameters = parameters(result.statement());

        assertThat(result.parameterNames()).containsExactly("later", "first", "later");
        assertThat(parameters)
                .extracting(Parameter::getId)
                .containsExactly(0, 1, 2);
        assertThat(parameters)
                .extracting(Parameter::getLocation)
                .containsExactly(
                        Optional.of(new NodeLocation(1, 8)),
                        Optional.of(new NodeLocation(2, 2)),
                        Optional.of(new NodeLocation(2, 12)));
        assertThat(SqlFormatter.formatSql(result.statement()))
                .contains("?")
                .doesNotContain(firstValue, laterValue, "{first}", "{later}");
    }

    @ParameterizedTest
    @MethodSource("bindingErrors")
    public void testReportsStableSourceLocatedBindingErrors(
            String hogql,
            Map<String, HogQlTypedValue> bindings,
            String expectedMessage,
            Location expectedLocation,
            String sensitiveValue)
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile(hogql, bindings));

        assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_BINDING_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(expectedLocation);
        assertThat(exception)
                .hasMessage(expectedMessage)
                .message()
                .doesNotContain(sensitiveValue);
    }

    @Test
    public void testReturnsOnlyStockTrinoTreeNodes()
    {
        Statement statement = compiler.compile("SELECT event, * FROM ducklake.default.events");
        Deque<Node> pending = new ArrayDeque<>();
        pending.add(statement);

        while (!pending.isEmpty()) {
            Node node = pending.removeFirst();
            assertThat(node.getClass().getPackageName()).isEqualTo("io.trino.sql.tree");
            pending.addAll(node.getChildren());
        }
    }

    @Test
    public void testReportsSourceLocatedHogQlSyntaxError()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile("SELECT 1\nGROUP BY ALL"));

        assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_SYNTAX_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(2, 1));
        assertThat(exception).hasMessageStartingWith("line 2:1:");
    }

    private static Stream<Arguments> bindingErrors()
    {
        return Stream.of(
                arguments(
                        "SELECT\n {missing}",
                        Map.of(),
                        "line 2:2: Missing HogQL parameter bindings: missing",
                        new Location(2, 2),
                        "sensitive-missing-value"),
                arguments(
                        "SELECT 1",
                        Map.of("extra", typedValue("sensitive-extra-value")),
                        "line 1:1: Unused HogQL parameter bindings: extra",
                        new Location(1, 1),
                        "sensitive-extra-value"),
                arguments(
                        "SELECT *\nFROM {source}",
                        Map.of("source", typedValue("sensitive-table-value")),
                        "line 2:6: HogQL parameter placeholders are not supported in table positions: source",
                        new Location(2, 6),
                        "sensitive-table-value"));
    }

    private static Stream<Arguments> representableHogQlCastTypes()
    {
        return Stream.of(
                arguments("SELECT CAST(value AS Int8)", "SELECT CAST(value AS tinyint)"),
                arguments("SELECT CAST(value AS Int16)", "SELECT CAST(value AS smallint)"),
                arguments("SELECT CAST(value AS Int32)", "SELECT CAST(value AS integer)"),
                arguments("SELECT CAST(value AS Int64)", "SELECT CAST(value AS bigint)"),
                arguments("SELECT CAST(value AS Int)", "SELECT CAST(value AS integer)"),
                arguments("SELECT CAST(value AS Integer)", "SELECT CAST(value AS integer)"),
                arguments("SELECT CAST(value AS Float32)", "SELECT CAST(value AS real)"),
                arguments("SELECT CAST(value AS Float64)", "SELECT CAST(value AS double)"),
                arguments("SELECT CAST(value AS Float)", "SELECT CAST(value AS real)"),
                arguments("SELECT CAST(value AS Real)", "SELECT CAST(value AS real)"),
                arguments("SELECT CAST(value AS String)", "SELECT CAST(value AS varchar)"),
                arguments("SELECT TRY_CAST(value AS VARCHAR)", "SELECT TRY_CAST(value AS varchar)"),
                arguments("SELECT CAST(value AS Bool)", "SELECT CAST(value AS boolean)"),
                arguments("SELECT CAST(value AS Date)", "SELECT CAST(value AS date)"),
                arguments("SELECT CAST(value AS UUID)", "SELECT CAST(value AS uuid)"),
                arguments("SELECT CAST(value AS JSON)", "SELECT CAST(value AS json)"),
                arguments("SELECT CAST(value AS Decimal(18, 4))", "SELECT CAST(value AS decimal(18, 4))"),
                arguments("SELECT CAST(value AS Decimal32(2))", "SELECT CAST(value AS decimal(9, 2))"),
                arguments("SELECT CAST(value AS Decimal64(4))", "SELECT CAST(value AS decimal(18, 4))"),
                arguments("SELECT CAST(value AS Decimal128(8))", "SELECT CAST(value AS decimal(38, 8))"),
                arguments("SELECT TRY_CAST(value AS Nullable(Decimal(18, 4)))", "SELECT TRY_CAST(value AS decimal(18, 4))"),
                arguments("SELECT CAST(value AS Array(Nullable(Int32)))", "SELECT CAST(value AS array(integer))"),
                arguments("SELECT CAST(value AS Map(String, Float64))", "SELECT CAST(value AS map(varchar, double))"),
                arguments("SELECT CAST(value AS Tuple(Int64, String))", "SELECT CAST(value AS row(bigint, varchar))"),
                arguments("SELECT CAST(value AS Tuple(id Int64, label String))", "SELECT CAST(value AS row(id bigint, label varchar))"),
                arguments("SELECT CAST(value AS Timestamp(6))", "SELECT CAST(value AS timestamp(6))"),
                arguments("SELECT CAST(value AS Timestamp WITH TIME ZONE)", "SELECT CAST(value AS timestamp(3) with time zone)"),
                arguments("SELECT CAST(value AS Time(12))", "SELECT CAST(value AS time(12))"),
                arguments("SELECT CAST(value AS Interval Day To Second)", "SELECT CAST(value AS interval day to second)"),
                arguments("SELECT CAST(value AS Interval Year To Month)", "SELECT CAST(value AS interval year to month)"));
    }

    private static Stream<Arguments> unsupportedHogQlCastTypes()
    {
        return Stream.of(
                arguments("UInt8", "unsigned integer range"),
                arguments("UInt16", "unsigned integer range"),
                arguments("UInt32", "unsigned integer range"),
                arguments("UInt64", "unsigned integer range"),
                arguments("UInt128", "unsigned integer range"),
                arguments("UInt256", "unsigned integer range"),
                arguments("Int128", "integer width exceeds Trino bigint"),
                arguments("Int256", "integer width exceeds Trino bigint"),
                arguments("Decimal256(2)", "decimal precision exceeds Trino"),
                arguments("Decimal(39, 2)", "precision <= 38"),
                arguments("Decimal(4, 5)", "scale <= precision"),
                arguments("Nullable(Array(Int32))", "Nullable cannot wrap"),
                arguments("Nullable(Map(String, Int32))", "Nullable cannot wrap"),
                arguments("Nullable(Tuple(Int32))", "Nullable cannot wrap"),
                arguments("FixedString(16)", "padding semantics"),
                arguments("Date32", "Date32 range"),
                arguments("DateTime", "time-zone and range semantics"),
                arguments("DateTime64(3, 'UTC')", "time-zone and range semantics"),
                arguments("Timestamp(13)", "precision exceeds 12"),
                arguments("Time(13)", "precision exceeds 12"),
                arguments("Timestamp WITH LOCAL TIME ZONE", "WITH LOCAL TIME ZONE semantics"),
                arguments("Interval", "interval qualifier is required"));
    }

    private static HogQlTypedValue typedValue(String value)
    {
        return new HogQlTypedValue("varchar", new StringValue(value));
    }

    private static List<Parameter> parameters(Statement statement)
    {
        Deque<Node> pending = new ArrayDeque<>();
        List<Parameter> parameters = new ArrayList<>();
        pending.add(statement);
        while (!pending.isEmpty()) {
            Node node = pending.removeFirst();
            if (node instanceof Parameter parameter) {
                parameters.add(parameter);
            }
            pending.addAll(node.getChildren());
        }
        return parameters.stream()
                .sorted(Comparator.comparingInt(Parameter::getId))
                .toList();
    }
}
