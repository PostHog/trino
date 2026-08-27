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
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
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
    public void testLowersDistinctOrderingAndClickHousePaginationOrder()
    {
        Statement statement = compiler.compile("SELECT DISTINCT event FROM events ORDER BY event DESC NULLS FIRST LIMIT 10 OFFSET 2");

        assertThat(statement).isEqualTo(sqlParser.createStatement("SELECT DISTINCT event FROM events ORDER BY event DESC NULLS FIRST OFFSET 2 LIMIT 10"));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
                                            SELECT [1, 2, 3]                 | SELECT ARRAY[1, 2, 3]
                                            SELECT ARRAY[]                   | SELECT ARRAY[]
                                            SELECT (1, 'example')            | SELECT (1, 'example')
                                            SELECT (1,)                       | SELECT ROW(1)
                                            SELECT value BETWEEN 1 AND 10    | SELECT value BETWEEN 1 AND 10
                                            SELECT value NOT BETWEEN 1 AND 10| SELECT value NOT BETWEEN 1 AND 10
                                            SELECT value IS NULL             | SELECT value IS NULL
                                            SELECT value IS NOT NULL         | SELECT value IS NOT NULL
                                            SELECT value IN (1, 2, 3)        | SELECT value IN (1, 2, 3)
                                            SELECT value IN (1)              | SELECT value IN (1)
                                            SELECT value NOT IN (1, 2, 3)    | SELECT value NOT IN (1, 2, 3)
                                            """)
    public void testLowersCollectionAndPredicateExpressions(String hogql, String trinoSql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(trinoSql));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT CASE WHEN enabled THEN 1 ELSE 0 END",
            "SELECT CASE value WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
            "SELECT CAST(value AS INT)",
            "SELECT TRY_CAST(value AS VARCHAR)",
    })
    public void testLowersCaseAndCastExpressions(String hogql)
    {
        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(hogql));
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
