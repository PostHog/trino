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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.PASSWORD;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Data files holding the rows of several snapshots, which DuckDB writes when it merges adjacent
 * files and when it flushes inlined rows. Such a file is registered from the oldest snapshot of
 * its rows on, replacing the files or rows it was written from, so a transaction reading an older
 * snapshot than the newest of them sees the file and must leave out the rows it cannot see. That
 * is what happens when a compaction or a flush commits while a query runs, which every test here
 * stages: a Trino transaction is pinned to a snapshot, DuckDB then inserts more rows and rewrites
 * the table's files, and the transaction reads the table. Every expectation is what DuckDB returns
 * for the same table at the same snapshot.
 * <p>
 * The tests run one at a time, because the snapshot a transaction is pinned to is taken as the
 * newest snapshot of the catalog when it begins, which another test writing to the catalog would
 * move.
 */
@Execution(SAME_THREAD)
final class TestDuckLakePartialDataFiles
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            createFixtures(catalog);
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    private static void createFixtures(TestingDuckLakeCatalog catalog)
            throws SQLException
    {
        catalog.executeInDuckDb(
                // read to pin the snapshot of a transaction; never changed
                "CREATE TABLE snapshot_pin (id INTEGER)",
                "INSERT INTO snapshot_pin VALUES (1), (2), (3)",

                // partitioned like a table of events, each insert writing a file per partition
                "CREATE TABLE merged_events (id BIGINT, p INTEGER, v VARCHAR, ts TIMESTAMP)",
                "ALTER TABLE merged_events SET PARTITIONED BY (p)",
                "INSERT INTO merged_events SELECT range, range % 2, 'a' || range, TIMESTAMP '2026-09-25 00:00:00' + to_seconds(range) FROM range(0, 1000)",
                "INSERT INTO merged_events SELECT range, range % 2, 'b' || range, TIMESTAMP '2026-09-25 00:00:00' + to_seconds(range) FROM range(1000, 2000)",

                "CREATE TABLE merged_writes (id BIGINT, v VARCHAR)",
                "INSERT INTO merged_writes SELECT range, 'a' || range FROM range(0, 100)",
                "INSERT INTO merged_writes SELECT range, 'b' || range FROM range(100, 200)",

                // inlined rows, some of them deleted, flushed later by a test
                "CALL lake.set_option('data_inlining_row_limit', 100)",
                "CREATE TABLE flushed_events (id INTEGER, v VARCHAR)",
                "INSERT INTO flushed_events VALUES (1, 'one'), (2, 'two'), (3, 'three')",
                "INSERT INTO flushed_events VALUES (4, 'four'), (5, 'five')",
                "DELETE FROM flushed_events WHERE id = 2",
                "CALL lake.set_option('data_inlining_row_limit', 0)");
    }

    /**
     * Files of three inserts merged into one while a transaction reads the snapshot of the second.
     * The transaction reads the rows of the first two inserts from the merged file, and only those.
     */
    @Test
    void testMergedFileReadAtAnOlderSnapshot()
            throws SQLException
    {
        readPinnedToSnapshot(() -> catalog.executeInDuckDb(
                "INSERT INTO merged_events SELECT range, range % 2, 'c' || range, TIMESTAMP '2026-09-25 00:00:00' + to_seconds(range) FROM range(2000, 3000)",
                "CALL ducklake_merge_adjacent_files('lake', 'merged_events')"), (session, snapshot) -> {
            assertPartialFiles("merged_events", snapshot, 2);

            assertQuery(session, "SELECT count(*) FROM merged_events", "VALUES 2000");
            assertQuery(session, "SELECT count(*), min(id), max(id), sum(id) FROM merged_events", "VALUES (2000, 0, 1999, 1999000)");
            assertMatchesDuckDb(session, snapshot, "SELECT id, p, v FROM merged_events ORDER BY id");
            // projections without the column the rows are told apart by, and without any column
            assertMatchesDuckDb(session, snapshot, "SELECT v FROM merged_events ORDER BY v");
            assertMatchesDuckDb(session, snapshot, "SELECT count(*) FROM merged_events");
            assertMatchesDuckDb(session, snapshot, "SELECT p, count(*) FROM merged_events GROUP BY p ORDER BY p");
            // predicates on data and partition columns, pushed into the reader and pruning files
            assertMatchesDuckDb(session, snapshot, "SELECT id FROM merged_events WHERE id >= 1990 ORDER BY id");
            assertMatchesDuckDb(session, snapshot, "SELECT count(*) FROM merged_events WHERE id >= 1500");
            assertMatchesDuckDb(session, snapshot, "SELECT count(*) FROM merged_events WHERE p = 1");
            assertMatchesDuckDb(session, snapshot, "SELECT count(*) FROM merged_events WHERE v LIKE 'c%'");
            assertQuery(session, "SELECT max(ts), count(*) FROM merged_events WHERE ts > TIMESTAMP '2026-09-25 00:30:00'", "VALUES (TIMESTAMP '2026-09-25 00:33:19', 199)");
            // a join, whose dynamic filter reaches the reader
            assertQuery(
                    session,
                    "SELECT count(*), sum(e.id) FROM merged_events e JOIN (VALUES BIGINT '5', 1500, 2500) k(id) ON e.id = k.id",
                    "VALUES (2, 1505)");
        });

        // the newest snapshot holds every row
        assertQuery("SELECT count(*), min(id), max(id) FROM merged_events", "VALUES (3000, 0, 2999)");
        assertThat(computeActual("SELECT id FROM merged_events WHERE v LIKE 'c%'").getRowCount()).isEqualTo(1000);
    }

    /**
     * Inlined rows inserted and deleted on both sides of the snapshot a transaction reads, then
     * flushed into a data file and a delete file while it reads. Each file holds the rows, and the
     * deletions, of every snapshot, each tagged with its own: the transaction applies the
     * deletions it can see to the rows it can see, and the positions of the deletions are those of
     * the rows in the file, including the rows it does not see.
     */
    @Test
    void testFlushedRowsAndDeletionsReadAtAnOlderSnapshot()
            throws SQLException
    {
        readPinnedToSnapshot(() -> catalog.executeInDuckDb(
                // the connection writes Parquet files unless told to inline
                "CALL lake.set_option('data_inlining_row_limit', 100)",
                "DELETE FROM flushed_events WHERE id = 4",
                "INSERT INTO flushed_events VALUES (6, 'six'), (7, 'seven')",
                "CALL lake.set_option('data_inlining_row_limit', 0)",
                "CALL ducklake_flush_inlined_data('lake', table_name => 'flushed_events')"), (session, snapshot) -> {
            assertPartialFiles("flushed_events", snapshot, 1);
            assertQuery(session, "SELECT id, v FROM flushed_events", "VALUES (1, 'one'), (3, 'three'), (4, 'four'), (5, 'five')");
            assertQuery(session, "SELECT count(*) FROM flushed_events", "VALUES 4");
            assertQuery(session, "SELECT v FROM flushed_events WHERE id > 3", "VALUES 'four', 'five'");
            assertMatchesDuckDb(session, snapshot, "SELECT id, v FROM flushed_events ORDER BY id");
            assertMatchesDuckDb(session, snapshot, "SELECT count(*) FROM flushed_events");
        });

        assertQuery("SELECT id, v FROM flushed_events", "VALUES (1, 'one'), (3, 'three'), (5, 'five'), (6, 'six'), (7, 'seven')");
        assertQuery("SELECT count(*) FROM flushed_events", "VALUES 5");
    }

    /**
     * A merged file is read at an older snapshot, and then changed at the newest one, where every
     * row of it is visible: the change applies to its rows by their position in the file. A
     * change cannot run in a transaction pinned to an older snapshot, because writes are
     * autocommit only, and one that reads such a file in a race with the merge conflicts at commit
     * with the insert of the rows it did not see.
     */
    @Test
    void testRowLevelChangesOfMergedFile()
            throws SQLException
    {
        readPinnedToSnapshot(() -> catalog.executeInDuckDb(
                "INSERT INTO merged_writes SELECT range, 'c' || range FROM range(200, 300)",
                "CALL ducklake_merge_adjacent_files('lake', 'merged_writes')"), (session, snapshot) -> {
            assertPartialFiles("merged_writes", snapshot, 1);
            assertQuery(session, "SELECT count(*) FROM merged_writes WHERE id % 10 = 0", "VALUES 20");
        });
        assertQuery("SELECT count(*) FROM merged_writes", "VALUES 300");

        // the merged file is read whole now, and its rows are changed by their position in it
        assertUpdate("DELETE FROM merged_writes WHERE id % 10 = 0", 30);
        assertUpdate("UPDATE merged_writes SET v = 'u' || v WHERE id % 10 = 5", 30);
        assertMatchesDuckDb(getSession(), "SELECT id, v FROM merged_writes ORDER BY id");
        assertQuery("SELECT count(*), sum(id) FROM merged_writes", "VALUES (270, 40500)");
    }

    private interface CatalogChange
    {
        void apply()
                throws SQLException;
    }

    private interface PinnedRead
    {
        void read(Session session, long snapshot)
                throws SQLException;
    }

    /**
     * Pins a transaction to the newest snapshot, lets {@code change} write to the catalog, and
     * reads in the transaction.
     */
    private void readPinnedToSnapshot(CatalogChange change, PinnedRead read)
            throws SQLException
    {
        long snapshot = newestSnapshot();
        inTransaction(session -> {
            // the transaction reads the snapshot current when it first reads the catalog
            assertQuery(session, "SELECT id FROM snapshot_pin", "VALUES 1, 2, 3");
            try {
                assertThat(newestSnapshot()).isEqualTo(snapshot);
                change.apply();
                assertThat(newestSnapshot()).isGreaterThan(snapshot);
                read.read(session, snapshot);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Checks that the table now has only the given number of data files, each also holding rows
     * newer than the snapshot, so that the reads that follow exercise the filter.
     */
    private void assertPartialFiles(String table, long snapshot, int expectedFiles)
            throws SQLException
    {
        long tableId = metastoreLong("SELECT table_id FROM ducklake_table WHERE table_name = '%s' AND end_snapshot IS NULL".formatted(table));
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL".formatted(tableId)))
                .as(metastoreRows("SELECT data_file_id, begin_snapshot, end_snapshot, partial_max, record_count, path FROM ducklake_data_file WHERE table_id = %s".formatted(tableId)))
                .isEqualTo(expectedFiles);
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL AND begin_snapshot <= %s AND partial_max > %s".formatted(tableId, snapshot, snapshot)))
                .isEqualTo(expectedFiles);
    }

    private void assertMatchesDuckDb(Session session, long snapshot, @Language("SQL") String sql)
    {
        assertThat(trinoValues(session, sql)).isEqualTo(catalog.rows(atSnapshot(sql, snapshot)));
    }

    private void assertMatchesDuckDb(Session session, @Language("SQL") String sql)
    {
        assertThat(trinoValues(session, sql)).isEqualTo(catalog.rows(sql));
    }

    private List<String> trinoValues(Session session, @Language("SQL") String sql)
    {
        MaterializedResult result = computeActual(session, sql);
        ImmutableList.Builder<String> values = ImmutableList.builder();
        result.getMaterializedRows().forEach(row -> row.getFields().forEach(value -> values.add(value == null ? "<null>" : value.toString())));
        return values.build();
    }

    /**
     * The query reading every table at the snapshot, in DuckDB's time travel syntax. The tables
     * here are named once each in the queries that use it.
     */
    private static String atSnapshot(String sql, long snapshot)
    {
        return sql.replaceAll("FROM (\\w+)", "FROM $1 AT (VERSION => %s)".formatted(snapshot));
    }

    private long newestSnapshot()
            throws SQLException
    {
        return metastoreLong("SELECT max(snapshot_id) FROM ducklake_snapshot");
    }

    private String metastoreRows(@Language("SQL") String sql)
            throws SQLException
    {
        StringBuilder rows = new StringBuilder();
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                for (int column = 1; column <= resultSet.getMetaData().getColumnCount(); column++) {
                    rows.append(resultSet.getString(column)).append(' ');
                }
                rows.append('\n');
            }
        }
        return rows.toString();
    }

    private long metastoreLong(@Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next()).isTrue();
            long value = resultSet.getLong(1);
            assertThat(resultSet.next()).isFalse();
            return value;
        }
    }
}
