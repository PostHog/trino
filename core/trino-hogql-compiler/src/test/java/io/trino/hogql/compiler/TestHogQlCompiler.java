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

import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    })
    public void testLowersToEquivalentStockTrinoAst(String hogql)
    {
        Statement statement = compiler.compile(hogql);

        assertThat(statement).isEqualTo(sqlParser.createStatement(hogql));
        assertThat(sqlParser.createStatement(SqlFormatter.formatSql(statement))).isEqualTo(statement);
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
    public void testReportsParsingFailureAsTrinoSyntaxError()
    {
        assertThatThrownBy(() -> compiler.compile("SELECT 1\nWHERE TRUE"))
                .isInstanceOf(ParsingException.class)
                .hasMessageStartingWith("line 2:1:");
    }
}
