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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckLakeCountDistinct
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        catalog.executeInDuckDb(
                "CALL lake.set_option('parquet_row_group_size', 2048)",
                "CREATE TABLE events (person_id VARCHAR, event_id BIGINT)",
                // Many repeated values, unique values, nulls, and duplicates across files.
                "INSERT INTO events SELECT CASE WHEN range % 17 = 0 THEN NULL WHEN range % 3 = 0 THEN 'hot' ELSE 'person-' || range END, range FROM range(0, 12000)",
                "INSERT INTO events SELECT CASE WHEN range % 17 = 0 THEN NULL WHEN range % 3 = 0 THEN 'hot' ELSE 'person-' || range END, range FROM range(6000, 18000)",
                "CREATE TABLE empty_events (person_id VARCHAR)",
                "CREATE TABLE null_events AS SELECT NULL::VARCHAR AS person_id FROM range(20)",
                "CREATE TABLE uuid_events AS SELECT (CASE WHEN range % 13 = 0 THEN NULL ELSE md5((range % 3000)::VARCHAR) END)::UUID AS person_id FROM range(12000)",
                "CREATE TABLE deleted_events AS SELECT * FROM events",
                "DELETE FROM deleted_events WHERE event_id % 5 = 0",
                "CREATE TABLE evolved_events (event_id BIGINT)",
                "INSERT INTO evolved_events VALUES (1), (2)",
                "ALTER TABLE evolved_events ADD COLUMN person_id VARCHAR",
                "INSERT INTO evolved_events VALUES (3, 'a'), (4, 'a'), (5, 'b')");
        return DuckLakeQueryRunner.builder(catalog)
                .setExtraProperties(ImmutableMap.of("optimizer.fuse-count-distinct", "true"))
                .build();
    }

    @Test
    void testCountDistinctUsesFusedOperator()
            throws SQLException
    {
        String sql = "SELECT COUNT(DISTINCT person_id) AS distinct_persons FROM events WHERE person_id IS NOT NULL";
        long expected = duckDbCount(sql);
        assertQueryStats(
                getSession(),
                sql,
                stats -> assertThat(stats.getOperatorSummaries())
                        .extracting(OperatorStats::getOperatorType)
                        .contains("CountDistinctOperator"),
                result -> assertThat(result.getOnlyValue()).isEqualTo(expected));
    }

    @Test
    void testFusionDisabled()
            throws SQLException
    {
        String sql = "SELECT count(DISTINCT person_id) FROM events";
        long expected = duckDbCount(sql);
        for (Session session : new Session[] {
                Session.builder(getSession()).setSystemProperty("fuse_count_distinct", "false").build(),
                Session.builder(getSession()).setSystemProperty("spill_enabled", "true").build(),
        }) {
            assertQueryStats(
                    session,
                    sql,
                    stats -> assertThat(stats.getOperatorSummaries())
                            .extracting(OperatorStats::getOperatorType)
                            .doesNotContain("CountDistinctOperator"),
                    result -> assertThat(result.getOnlyValue()).isEqualTo(expected));
        }
    }

    @Test
    void testExactResultsAcrossFilesAndSplits()
            throws SQLException
    {
        for (String table : new String[] {"events", "empty_events", "null_events", "uuid_events", "deleted_events", "evolved_events"}) {
            for (String predicate : new String[] {"", " WHERE person_id IS NOT NULL"}) {
                String sql = "SELECT count(DISTINCT person_id) FROM " + table + predicate;
                long expected = duckDbCount(sql);
                assertQuery(sql, "VALUES " + expected);
                Session smallSplits = Session.builder(getSession())
                        .setCatalogSessionProperty("ducklake", "max_split_size", "1kB")
                        .build();
                assertQuery(smallSplits, sql, "VALUES " + expected);
            }
        }
    }

    @Test
    void testCountOfDistinctRowsIncludesNull()
            throws SQLException
    {
        String sql = "SELECT count(*) FROM (SELECT DISTINCT person_id FROM events)";
        assertQuery(sql, "VALUES " + duckDbCount(sql));
        assertQuery("SELECT count(*) FROM (SELECT DISTINCT person_id FROM null_events)", "VALUES 1");
        assertQuery("SELECT count(*) FROM (SELECT DISTINCT person_id FROM empty_events)", "VALUES 0");
    }

    @Test
    void testOtherAggregationShapes()
            throws SQLException
    {
        for (String sql : new String[] {
                "SELECT count(DISTINCT person_id) FROM events WHERE event_id < 9000",
                "SELECT count(DISTINCT person_id) FILTER (WHERE event_id % 2 = 0) FROM events",
                "SELECT count(person_id) FROM events",
                "SELECT count(*) FROM events",
        }) {
            assertQuery(sql, "VALUES " + duckDbCount(sql));
        }
        assertQuery("SELECT person_id, count(*) FROM evolved_events GROUP BY person_id", "VALUES (NULL, 2), ('a', 2), ('b', 1)");
        assertQuery("SELECT count(DISTINCT person_id), count(*) FROM evolved_events", "VALUES (2, 5)");
        assertQuery("SELECT count(DISTINCT person_id), count(DISTINCT event_id) FROM evolved_events", "VALUES (2, 5)");
    }

    private long duckDbCount(String sql)
            throws SQLException
    {
        try (var connection = catalog.openDuckDbConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }
}
