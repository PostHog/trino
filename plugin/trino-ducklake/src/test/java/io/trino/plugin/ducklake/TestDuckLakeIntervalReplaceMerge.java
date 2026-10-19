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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * The single-statement interval replace a SQLMesh client issues instead of DELETE + INSERT.
 * <p>
 * The connector publishes every statement as its own DuckLake snapshot, and its transaction
 * commit is a no-op, so a DELETE and the INSERT after it can never commit together. One MERGE
 * is one snapshot: these tests pin that the shape below replaces exactly the interval, and that
 * a MERGE which fails leaves the interval exactly as it was.
 */
final class TestDuckLakeIntervalReplaceMerge
        extends AbstractTestQueryFramework
{
    private static final String DAY = "2026-10-14";
    private static final String COLUMNS = "id BIGINT, \"timestamp\" TIMESTAMP(6) WITH TIME ZONE, v VARCHAR, "
            + "m MAP(VARCHAR, VARCHAR), r ROW(a BIGINT, b VARCHAR), arr ARRAY(VARCHAR)";
    // (id, timestamp, v) rows become full rows of the table: m, r and arr are derived from them
    private static final String WIDEN = "SELECT id, ts, v, MAP(ARRAY['k'], ARRAY[v]), CAST(ROW(id, v) AS ROW(a BIGINT, b VARCHAR)), ARRAY[v] FROM (%s) AS w(id, ts, v)";

    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
        }
        catch (SQLException e) {
            abort("Failed to create a DuckLake catalog with DuckDB (extension download requires network access): " + e);
        }
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    /**
     * The replace, exactly as the client renders it, for this table and the interval
     * [DAY 00:00, DAY 23:59:59.999999], from a model query producing {@code newRows}: one delete
     * marker per distinct minute already in the interval, UNION ALL the new rows keyed by their
     * own minute plus 2^40 so they can never match. MAP, ROW and ARRAY are there because each
     * delete marker carries a NULL of every column, and those must stay outside the DISTINCT.
     */
    private static String replaceInterval(String table, String newRows)
    {
        String low = "CAST('" + DAY + " 00:00:00+00:00' AS TIMESTAMP(6) WITH TIME ZONE)";
        String high = "CAST('" + DAY + " 23:59:59.999999+00:00' AS TIMESTAMP(6) WITH TIME ZONE)";
        return """
               MERGE INTO %1$s AS __t
               USING (
                 SELECT
                   CAST(NULL AS BIGINT) AS "id",
                   CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS "timestamp",
                   CAST(NULL AS VARCHAR) AS "v",
                   CAST(NULL AS MAP(VARCHAR, VARCHAR)) AS "m",
                   CAST(NULL AS ROW(a BIGINT, b VARCHAR)) AS "r",
                   CAST(NULL AS ARRAY(VARCHAR)) AS "arr",
                   __k.__sqlmesh_key,
                   FALSE AS __sqlmesh_new
                 FROM (
                   SELECT DISTINCT
                     CAST(FLOOR(CAST(TO_UNIXTIME("__m"."timestamp") AS DOUBLE) / 60) AS BIGINT) AS __sqlmesh_key
                   FROM %1$s AS __m
                   WHERE "__m"."timestamp" BETWEEN %2$s AND %3$s
                 ) AS __k
                 UNION ALL
                 SELECT
                   "__q"."id", "__q"."timestamp", "__q"."v", "__q"."m", "__q"."r", "__q"."arr",
                   CAST(FLOOR(CAST(TO_UNIXTIME("__q"."timestamp") AS DOUBLE) / 60) AS BIGINT) + 1099511627776 AS __sqlmesh_key,
                   TRUE AS __sqlmesh_new
                 FROM (
                   SELECT
                     CAST("id" AS BIGINT) AS "id",
                     CAST("timestamp" AS TIMESTAMP(6) WITH TIME ZONE) AS "timestamp",
                     CAST("v" AS VARCHAR) AS "v",
                     CAST("m" AS MAP(VARCHAR, VARCHAR)) AS "m",
                     CAST("r" AS ROW(a BIGINT, b VARCHAR)) AS "r",
                     CAST("arr" AS ARRAY(VARCHAR)) AS "arr"
                   FROM (%4$s) AS _subquery(id, "timestamp", v, m, r, arr)
                   WHERE "timestamp" BETWEEN %2$s AND %3$s
                 ) AS __q
               ) AS __s
               ON __s.__sqlmesh_key = CAST(FLOOR(CAST(TO_UNIXTIME("__t"."timestamp") AS DOUBLE) / 60) AS BIGINT)
               AND "__t"."timestamp" BETWEEN %2$s AND %3$s
               WHEN MATCHED THEN DELETE
               WHEN NOT MATCHED AND __s.__sqlmesh_new THEN INSERT ("id", "timestamp", "v", "m", "r", "arr")
                 VALUES ("__s"."id", "__s"."timestamp", "__s"."v", "__s"."m", "__s"."r", "__s"."arr")
               """.formatted(table, low, high, WIDEN.formatted(newRows));
    }

    private String createTable()
    {
        String table = "interval_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (" + COLUMNS + ")");
        return table;
    }

    private String createTableWithThreeDays()
    {
        String table = createTable();
        // Several rows share a minute, so one delete marker matches many target rows.
        assertUpdate(
                "INSERT INTO " + table + " " + WIDEN.formatted(
                        """
                        VALUES
                          (BIGINT '1', TIMESTAMP '2026-10-13 12:00:00.000000 UTC', 'before'),
                          (2, TIMESTAMP '2026-10-14 00:00:00.000000 UTC', 'old'),
                          (3, TIMESTAMP '2026-10-14 00:00:30.000000 UTC', 'old'),
                          (4, TIMESTAMP '2026-10-14 00:00:59.999999 UTC', 'old'),
                          (5, TIMESTAMP '2026-10-14 13:37:00.000000 UTC', 'old'),
                          (6, TIMESTAMP '2026-10-14 23:59:59.999999 UTC', 'old'),
                          (7, TIMESTAMP '2026-10-15 00:00:00.000000 UTC', 'after')"""),
                7);
        return table;
    }

    /**
     * Snapshots that changed {@code table}. Counted per table, not catalog-wide: test methods run
     * concurrently and each commits its own snapshots.
     */
    private long snapshotsChanging(String table)
    {
        String tableId = catalog.scalar("SELECT table_id::VARCHAR FROM ducklake_table_info('lake') WHERE table_name = '" + table + "'");
        return Long.parseLong(catalog.scalar(
                """
                SELECT count(*)::VARCHAR FROM ducklake_snapshots('lake')
                WHERE list_contains(coalesce(changes['tables_inserted_into'], []), '%1$s')
                   OR list_contains(coalesce(changes['tables_deleted_from'], []), '%1$s')
                   OR list_contains(coalesce(changes['inlined_insert'], []), '%1$s')
                   OR list_contains(coalesce(changes['inlined_delete'], []), '%1$s')
                """.formatted(tableId)));
    }

    private List<String> duckDbRows(String table)
    {
        return catalog.rows("SELECT id::VARCHAR || ' ' || v || ' ' || m['k'] || ' ' || r.b || ' ' || arr[1] FROM " + table + " ORDER BY id");
    }

    @Test
    void testReplacesExactlyTheIntervalInOneSnapshot()
    {
        String table = createTableWithThreeDays();
        try {
            long before = snapshotsChanging(table);
            assertUpdate(
                    replaceInterval(table,
                            """
                            VALUES
                              (BIGINT '20', TIMESTAMP '2026-10-14 00:00:10.000000 UTC', 'new'),
                              (BIGINT '21', TIMESTAMP '2026-10-14 00:00:10.000000 UTC', 'new'),
                              (BIGINT '22', TIMESTAMP '2026-10-14 18:00:00.000000 UTC', 'new'),
                              (BIGINT '99', TIMESTAMP '2026-10-16 00:00:00.000000 UTC', 'outside')"""),
                    5 + 3);
            assertThat(snapshotsChanging(table) - before).isEqualTo(1);
            assertQuery(
                    "SELECT id, v, m['k'], r.a, arr[1] FROM " + table + " ORDER BY id",
                    "VALUES (1, 'before', 'before', 1, 'before'), (7, 'after', 'after', 7, 'after'), "
                            + "(20, 'new', 'new', 20, 'new'), (21, 'new', 'new', 21, 'new'), (22, 'new', 'new', 22, 'new')");
            assertThat(duckDbRows(table)).isEqualTo(List.of(
                    "1 before before before before",
                    "7 after after after after",
                    "20 new new new new",
                    "21 new new new new",
                    "22 new new new new"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testEmptySourceEmptiesTheIntervalOnly()
    {
        String table = createTableWithThreeDays();
        try {
            assertUpdate(
                    replaceInterval(table, "SELECT BIGINT '0', TIMESTAMP '2026-10-14 01:00:00.000000 UTC', 'x' WHERE false"),
                    5);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 1, 7");
            assertThat(duckDbRows(table)).isEqualTo(List.of("1 before before before before", "7 after after after after"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testEmptyIntervalOnlyInserts()
    {
        String table = createTable();
        try {
            assertUpdate(
                    replaceInterval(table, "VALUES (BIGINT '30', TIMESTAMP '2026-10-14 05:00:00.000000 UTC', 'new')"),
                    1);
            assertQuery("SELECT id, v FROM " + table, "VALUES (30, 'new')");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testFailedReplaceChangesNothing()
    {
        String table = createTableWithThreeDays();
        try {
            long before = snapshotsChanging(table);
            // The second new row fails while the MERGE runs, after its delete markers were read.
            assertThatThrownBy(() -> getQueryRunner().execute(replaceInterval(table,
                    """
                    SELECT id, ts, v FROM (VALUES
                      (BIGINT '40', TIMESTAMP '2026-10-14 02:00:00.000000 UTC', '1'),
                      (BIGINT '41', TIMESTAMP '2026-10-14 03:00:00.000000 UTC', 'not a number')) AS t(id, ts, v)
                    WHERE CAST(v AS BIGINT) > 0""")))
                    .hasMessageContaining("Cannot cast");
            assertThat(snapshotsChanging(table)).isEqualTo(before);
            assertQuery(
                    "SELECT id, v FROM " + table + " ORDER BY id",
                    "VALUES (1, 'before'), (2, 'old'), (3, 'old'), (4, 'old'), (5, 'old'), (6, 'old'), (7, 'after')");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testPlanPrunesTheTargetAndKeysTheJoin()
    {
        String table = createTableWithThreeDays();
        try {
            MaterializedResult plan = computeActual("EXPLAIN " + replaceInterval(table, "VALUES (BIGINT '50', TIMESTAMP '2026-10-14 06:00:00.000000 UTC', 'new')"));
            String text = (String) plan.getOnlyValue();
            // a hash join on the minute key, not a nested-loop (cross) join
            assertThat(text).doesNotContain("CrossJoin");
            assertThat(text).containsPattern("(Right|Left|Inner)Join\\[criteria = \\(");
            // the MERGE target scan is pruned to the interval, not the whole table
            assertThat(text).containsPattern("ScanFilter\\[table = [^\\]]*" + table + "[^\\]]*, filterPredicate = \\(\\(timestamp\\(6\\) with time zone '" + DAY);
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }
}
