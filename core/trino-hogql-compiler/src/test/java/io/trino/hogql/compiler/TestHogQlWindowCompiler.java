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
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlWindowCompiler
{
    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT row_number() OVER ()",
            "SELECT sum(value) FILTER (WHERE enabled) OVER (PARTITION BY team_id ORDER BY timestamp DESC NULLS LAST)",
            "SELECT avg(value) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            "SELECT first_value(value) OVER (ORDER BY timestamp RANGE UNBOUNDED PRECEDING)",
            "SELECT row_number() OVER recent FROM events WINDOW recent AS (PARTITION BY team_id ORDER BY timestamp)",
            "SELECT sum(value) OVER first_window, avg(value) OVER second_window FROM events " +
                    "WINDOW first_window AS (ROWS CURRENT ROW), second_window AS (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
    })
    public void testLowersWindowsToEquivalentStockTrinoAst(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ROWS UNBOUNDED PRECEDING",
            "ROWS 2 PRECEDING",
            "ROWS CURRENT ROW",
            "ROWS 2 FOLLOWING",
            "ROWS UNBOUNDED FOLLOWING",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            "RANGE BETWEEN 2 PRECEDING AND 3 FOLLOWING",
    })
    public void testLowersEveryCanonicalFrameBound(String frame)
    {
        String hogql = "SELECT sum(value) OVER (ORDER BY timestamp " + frame + ")";

        assertThat(compiler.compile(hogql)).isEqualTo(sqlParser.createStatement(hogql));
    }

    @Test
    public void testLowersCanonicalWindowNullTreatmentOrder()
    {
        assertThat(compiler.compile("SELECT first_value(value) OVER (ORDER BY timestamp) IGNORE NULLS"))
                .isEqualTo(sqlParser.createStatement("SELECT first_value(value) IGNORE NULLS OVER (ORDER BY timestamp)"));
    }

    @Test
    public void testBindsPlaceholdersAcrossWindowComponents()
    {
        String hogql = "SELECT sum({value}) FILTER (WHERE {filter}) OVER " +
                "(PARTITION BY {partition} ORDER BY {sort} ROWS BETWEEN {lower} PRECEDING AND {upper} FOLLOWING)";
        Map<String, HogQlTypedValue> bindings = Map.<String, HogQlTypedValue>of(
                "value", typedValue("value"),
                "filter", typedValue("filter"),
                "partition", typedValue("partition"),
                "sort", typedValue("sort"),
                "lower", typedValue("lower"),
                "upper", typedValue("upper"));

        HogQlCompilationResult result = compiler.compile(hogql, bindings);

        assertThat(result.parameterNames()).containsExactly("value", "filter", "partition", "sort", "lower", "upper");
        assertThat(parameters(result.statement())).extracting(Parameter::getId).containsExactly(0, 1, 2, 3, 4, 5);
    }

    private static HogQlTypedValue typedValue(String value)
    {
        return new HogQlTypedValue("varchar", new StringValue(value));
    }

    private static List<Parameter> parameters(Statement statement)
    {
        List<Parameter> parameters = new ArrayList<>();
        Deque<io.trino.sql.tree.Node> nodes = new ArrayDeque<>();
        nodes.add(statement);
        while (!nodes.isEmpty()) {
            io.trino.sql.tree.Node node = nodes.removeFirst();
            if (node instanceof Parameter parameter) {
                parameters.add(parameter);
            }
            nodes.addAll(node.getChildren());
        }
        return parameters.stream()
                .sorted(Comparator.comparingInt(Parameter::getId))
                .toList();
    }
}
