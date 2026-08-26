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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.SystemSessionProperties.ALLOW_PUSHDOWN_INTO_CONNECTORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Covers counting rows without reading them: {@code count(*)} answered from the catalog, and the
 * scans that fall back to listing files but still read none of them.
 */
final class TestDuckLakeCount
        extends AbstractTestQueryFramework
{
    /**
     * Larger than any fixture file, so every file is read by a single split whose record count is
     * the exact number of rows of the file.
     */
    private static final String WHOLE_FILE_SPLIT_SIZE = "1GB";
    /**
     * Small enough to split a fixture file into several byte ranges, none of which knows how many
     * rows it holds.
     */
    private static final String TINY_SPLIT_SIZE = "1kB";

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
                // two files, so that counting spans more than one catalog row
                "CREATE TABLE counts (id INTEGER, v VARCHAR)",
                "INSERT INTO counts SELECT range, 'v' || range FROM range(0, 12)",
                "INSERT INTO counts SELECT range, 'v' || range FROM range(12, 20)",

                "CREATE TABLE counts_empty (id INTEGER, v VARCHAR)",

                // deletes leave the record counts of the data files untouched and add delete files
                "CREATE TABLE counts_deleted (id INTEGER, v VARCHAR)",
                "INSERT INTO counts_deleted SELECT range, 'v' || range FROM range(0, 12)",
                "INSERT INTO counts_deleted SELECT range, 'v' || range FROM range(12, 20)",
                "DELETE FROM counts_deleted WHERE id % 3 = 0",

                // partitioned by an identity column, so a predicate on it is enforced by pruning
                // files and the scan is left with no predicate of its own
                "CREATE TABLE counts_partitioned (id INTEGER, v VARCHAR)",
                "ALTER TABLE counts_partitioned SET PARTITIONED BY (id)",
                "INSERT INTO counts_partitioned VALUES (1, 'a'), (1, 'b'), (2, 'c')",

                // files the connector refuses to read, made unreadable in the catalog below
                "CREATE TABLE counts_unsupported_format (id INTEGER, v VARCHAR)",
                "INSERT INTO counts_unsupported_format VALUES (1, 'a'), (2, 'b')",
                "CREATE TABLE counts_double_delete (id INTEGER, v VARCHAR)",
                "INSERT INTO counts_double_delete SELECT range, 'v' || range FROM range(0, 10)",
                "DELETE FROM counts_double_delete WHERE id = 1",

                // many rows in one file, so that a small split size cuts it into byte ranges
                "CALL lake.set_option('parquet_row_group_size', 2048)",
                "CREATE TABLE counts_row_groups (id BIGINT, v VARCHAR)",
                "INSERT INTO counts_row_groups SELECT range, 'v' || range FROM range(0, 20000)");

        // A format the connector does not read. Counting the rows of such a file from the catalog
        // would succeed where reading it fails, so the count declines to answer instead.
        updateCatalog(
                catalog,
                """
                UPDATE ducklake_data_file f SET file_format = 'orc'
                FROM ducklake_table t
                WHERE t.table_id = f.table_id AND t.table_name = 'counts_unsupported_format'""");

        // A second delete file for the same data file. The connector refuses such a catalog, and
        // the rows they delete may overlap, so the count cannot be trusted either.
        updateCatalog(
                catalog,
                """
                INSERT INTO ducklake_delete_file
                    (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative,
                        format, delete_count, file_size_bytes, footer_size, encryption_key)
                SELECT (SELECT max(delete_file_id) + 1 FROM ducklake_delete_file), d.table_id, d.begin_snapshot,
                    d.end_snapshot, d.data_file_id, d.path, d.path_is_relative, d.format, d.delete_count,
                    d.file_size_bytes, d.footer_size, d.encryption_key
                FROM ducklake_delete_file d
                JOIN ducklake_table t ON t.table_id = d.table_id
                WHERE t.table_name = 'counts_double_delete'
                LIMIT 1""");
    }

    private static void updateCatalog(TestingDuckLakeCatalog catalog, @Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    @Test
    void testCountIsAnsweredFromTheCatalog()
    {
        assertCountReadsNoData("SELECT count(*) FROM counts", 20);
        assertCountReadsNoData("SELECT count(1) FROM counts", 20);
        assertCountReadsNoData("SELECT count(*) FROM counts_row_groups", 20000);
    }

    /**
     * The pushed down count travels in the table handle, which the plan prints, so the plan shows
     * whether the count was answered from the catalog or left for the scan to work out.
     */
    @Test
    void testPushdownIsVisibleInThePlan()
    {
        assertThat(explain("SELECT count(*) FROM counts")).containsPattern("TableScan\\[table = ducklake:main\\.counts@\\d+ rows=20\\]");
        assertThat(explain("SELECT count(*) FROM counts_deleted")).contains(" rows=13]");
        assertThat(explain("SELECT count(*) FROM counts_empty")).contains(" rows=0]");

        // aggregates the catalog cannot answer leave the scan to read the table
        assertThat(explain("SELECT count(v) FROM counts")).doesNotContain(" rows=");
        assertThat(explain("SELECT count(DISTINCT id) FROM counts")).doesNotContain(" rows=");
        assertThat(explain("SELECT count(*) FILTER (WHERE id < 5) FROM counts")).doesNotContain(" rows=");
        assertThat(explain("SELECT sum(id) FROM counts")).doesNotContain(" rows=");
        assertThat(explain("SELECT id, count(*) FROM counts_partitioned GROUP BY id")).doesNotContain(" rows=");
        assertThat(explain("SELECT count(*) FROM counts WHERE v = 'v3'")).doesNotContain(" rows=");
        assertThat(explain("SELECT count(*) FROM counts_partitioned WHERE id = 1")).doesNotContain(" rows=");
    }

    @Test
    void testCountOfEmptyTable()
    {
        assertCountReadsNoData("SELECT count(*) FROM counts_empty", 0);
    }

    @Test
    void testCountSubtractsDeletedRows()
    {
        // ids 0, 3, 6, 9, 12, 15 and 18 are deleted
        assertCountReadsNoData("SELECT count(*) FROM counts_deleted", 13);
        assertThat(computeScalar("SELECT count(*) FROM counts_deleted"))
                .isEqualTo(computeScalar("SELECT count(id) FROM counts_deleted"));
    }

    /**
     * Without the aggregation pushdown the scan still runs, and reads nothing: a split that
     * projects no column is answered from the record count the catalog holds for its file.
     */
    @Test
    void testCountWithoutPushdownStillReadsNoData()
    {
        Session session = Session.builder(wholeFileSplits())
                .setSystemProperty(ALLOW_PUSHDOWN_INTO_CONNECTORS, "false")
                .build();
        assertCountReadsNoData(session, "SELECT count(*) FROM counts", 20);
        assertCountReadsNoData(session, "SELECT count(*) FROM counts_deleted", 13);
        assertCountReadsNoData(session, "SELECT count(*) FROM counts_row_groups", 20000);
    }

    /**
     * A predicate the connector enforces is applied by leaving files out of the scan, so the
     * remaining files are still counted whole.
     */
    @Test
    void testCountWithEnforcedPredicateReadsNoData()
    {
        assertCountReadsNoData("SELECT count(*) FROM counts_partitioned WHERE id = 1", 2);
        assertCountReadsNoData("SELECT count(*) FROM counts_partitioned WHERE id = 3", 0);
        assertCountReadsNoData("SELECT count(*) FROM counts_partitioned", 3);
    }

    /**
     * A file cut into byte ranges only knows a share of its rows per range, so those splits read
     * the file rather than counting from the catalog. The count still has to come out right.
     */
    @Test
    void testCountOverSplitFiles()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ALLOW_PUSHDOWN_INTO_CONNECTORS, "false")
                .setCatalogSessionProperty("ducklake", "max_split_size", TINY_SPLIT_SIZE)
                .build();
        assertQueryStats(
                session,
                "SELECT count(*) FROM counts_row_groups",
                queryStats -> assertThat(queryStats.getPhysicalInputDataSize().toBytes()).isPositive(),
                result -> assertThat(onlyValue(result)).isEqualTo(20000L));
    }

    /**
     * A count read from the catalog must not answer where reading the table would fail, or the
     * same table would count fine and scan not at all.
     */
    @Test
    void testCountFailsWhereTheTableCannotBeRead()
    {
        assertQueryFails("SELECT * FROM counts_unsupported_format", ".*has unsupported format 'orc'.*");
        assertQueryFails("SELECT count(*) FROM counts_unsupported_format", ".*has unsupported format 'orc'.*");

        assertQueryFails("SELECT * FROM counts_double_delete", "Multiple delete files are visible for data file .*");
        assertQueryFails("SELECT count(*) FROM counts_double_delete", "Multiple delete files are visible for data file .*");
    }

    @Test
    void testAggregatesThatAreNotAnsweredFromTheCatalog()
    {
        // count of a column ignores nulls, so it has to read the column
        assertQuery("SELECT count(v) FROM counts", "VALUES 20");
        assertQuery("SELECT count(DISTINCT id) FROM counts", "VALUES 20");
        assertQuery("SELECT count(*) FILTER (WHERE id < 5) FROM counts", "VALUES 5");
        assertQuery("SELECT sum(id) FROM counts", "VALUES 190");
        assertQuery("SELECT count(*), sum(id) FROM counts", "VALUES (20, 190)");
        assertQuery("SELECT id, count(*) FROM counts_partitioned GROUP BY id ORDER BY id", "VALUES (1, 2), (2, 1)");
    }

    /**
     * A predicate the connector cannot enforce leaves rows to filter, so the scan reads the column
     * it filters on instead of counting from the catalog.
     */
    @Test
    void testCountWithUnenforcedPredicate()
    {
        assertQueryStats(
                getSession(),
                "SELECT count(*) FROM counts WHERE v = 'v3'",
                queryStats -> assertThat(queryStats.getPhysicalInputDataSize().toBytes()).isPositive(),
                result -> assertThat(onlyValue(result)).isEqualTo(1L));
        assertQuery("SELECT count(*) FROM counts WHERE id >= 10", "VALUES 10");
    }

    @Test
    void testCountMatchesDuckDb()
            throws SQLException
    {
        for (String table : new String[] {"counts", "counts_empty", "counts_deleted", "counts_partitioned", "counts_row_groups"}) {
            assertThat(computeScalar("SELECT count(*) FROM " + table))
                    .describedAs("count of %s", table)
                    .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*)::VARCHAR FROM " + table)));
        }
    }

    private void assertCountReadsNoData(@Language("SQL") String sql, long expectedCount)
    {
        assertCountReadsNoData(wholeFileSplits(), sql, expectedCount);
    }

    private void assertCountReadsNoData(Session session, @Language("SQL") String sql, long expectedCount)
    {
        assertQueryStats(
                session,
                sql,
                queryStats -> assertThat(queryStats.getPhysicalInputDataSize().toBytes())
                        .describedAs("bytes read by %s", sql)
                        .isEqualTo(0),
                result -> assertThat(onlyValue(result)).isEqualTo(expectedCount));
    }

    private String explain(@Language("SQL") String sql)
    {
        return (String) computeScalar("EXPLAIN " + sql);
    }

    private Session wholeFileSplits()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", "max_split_size", WHOLE_FILE_SPLIT_SIZE)
                .build();
    }

    private static Object onlyValue(MaterializedResult result)
    {
        assertThat(result.getRowCount()).isEqualTo(1);
        return result.getOnlyValue();
    }

    private String duckDbScalar(@Language("SQL") String sql)
            throws SQLException
    {
        try (var connection = catalog.openDuckDbConnection();
                var statement = connection.createStatement();
                var resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next()).isTrue();
            return resultSet.getString(1);
        }
    }
}
