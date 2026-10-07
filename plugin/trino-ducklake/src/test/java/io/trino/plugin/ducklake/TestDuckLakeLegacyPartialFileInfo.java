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
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.PASSWORD;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * A catalog of DuckLake specification 0.3 describes a data file holding the rows of several
 * snapshots in the {@code partial_file_info} text column of {@code ducklake_data_file}, which
 * DuckLake 1.0 replaced with the {@code partial_max} column. A flush wrote
 * {@code partial_max:<snapshot>}, and a merge of adjacent files wrote
 * {@code <snapshot>:<row count>|...}. DuckDB converts the column when it upgrades the catalog, so
 * the catalog here is written by DuckDB and converted back.
 * <p>
 * A transaction is pinned to a snapshot before DuckDB flushes and merges, as a query running
 * while they commit would be, and reads after the conversion. DuckDB cannot be asked for the
 * expected rows: attaching the converted catalog would upgrade it again.
 */
final class TestDuckLakeLegacyPartialFileInfo
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            catalog.executeInDuckDb(
                    "CREATE TABLE legacy_merged (id INTEGER)",
                    "INSERT INTO legacy_merged SELECT range FROM range(0, 10)",
                    "INSERT INTO legacy_merged SELECT range FROM range(10, 20)",

                    "CALL lake.set_option('data_inlining_row_limit', 100)",
                    "CREATE TABLE legacy_flushed (id INTEGER)",
                    "INSERT INTO legacy_flushed VALUES (1), (2)",
                    "CALL lake.set_option('data_inlining_row_limit', 0)");
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    @Test
    void testPartialFileInfo()
    {
        long[] pinned = new long[1];
        // the transaction ends with the refused read, which aborts it
        assertThatThrownBy(() -> inTransaction(session -> {
            try {
                // takes the snapshot of the transaction without listing any file, so that the
                // connector looks at the columns of the catalog only once it has been converted
                computeActual(session, "SHOW COLUMNS FROM legacy_merged");
                long snapshot = metastoreLongs("SELECT max(snapshot_id) FROM ducklake_snapshot").getFirst();
                pinned[0] = snapshot;

                catalog.executeInDuckDb(
                        "CALL lake.set_option('data_inlining_row_limit', 100)",
                        "INSERT INTO legacy_flushed VALUES (3)",
                        "CALL lake.set_option('data_inlining_row_limit', 0)",
                        "CALL ducklake_flush_inlined_data('lake', table_name => 'legacy_flushed')",
                        "INSERT INTO legacy_merged SELECT range FROM range(20, 30)");
                // what DuckLake 0.3 recorded for a merge: the rows of the file visible from the
                // snapshot of each file merged on
                List<Long> sources = metastoreLongs(
                        """
                        SELECT begin_snapshot FROM ducklake_data_file
                        WHERE end_snapshot IS NULL AND table_id = (SELECT table_id FROM ducklake_table WHERE table_name = 'legacy_merged')
                        ORDER BY begin_snapshot""");
                assertThat(sources).hasSize(3);
                String splits = "%s:10|%s:20|%s:30".formatted(sources.get(0), sources.get(1), sources.get(2));
                catalog.executeInDuckDb("CALL ducklake_merge_adjacent_files('lake', 'legacy_merged')");
                assertThat(metastoreLongs("SELECT count(*) FROM ducklake_data_file WHERE partial_max > %s AND end_snapshot IS NULL".formatted(snapshot)).getFirst())
                        .isEqualTo(2);

                catalog.executeInMetastore(
                        "ALTER TABLE ducklake_data_file ADD COLUMN partial_file_info VARCHAR",
                        "UPDATE ducklake_data_file SET partial_file_info = 'partial_max:' || partial_max WHERE partial_max IS NOT NULL",
                        ("UPDATE ducklake_data_file SET partial_file_info = '%s' WHERE partial_max IS NOT NULL AND end_snapshot IS NULL " +
                                "AND table_id = (SELECT table_id FROM ducklake_table WHERE table_name = 'legacy_merged')").formatted(splits),
                        "ALTER TABLE ducklake_data_file DROP COLUMN partial_max");

                // the flushed file leaves out the row inserted after the snapshot
                assertQuery(session, "SELECT id FROM legacy_flushed", "VALUES 1, 2");
                assertQuery(session, "SELECT count(*) FROM legacy_flushed", "VALUES 2");
                // the snapshot sees some rows of the merged file only, which is refused rather than
                // read whole; a count is not answered from the catalog either
                computeActual(session, "SELECT count(*) FROM legacy_merged");
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }))
                .hasMessageMatching("Data file .* of table \\d+ holds rows added after snapshot \\d+, recorded in the partial_file_info of an older DuckLake catalog, which is not supported. Upgrade the catalog with DuckDB");
        assertThat(pinned[0]).isPositive();

        // the newest snapshot sees every row of both files
        assertQuery("SELECT id FROM legacy_flushed", "VALUES 1, 2, 3");
        assertQuery("SELECT count(*), sum(id) FROM legacy_merged", "VALUES (30, 435)");
    }

    private List<Long> metastoreLongs(@Language("SQL") String sql)
            throws SQLException
    {
        List<Long> values = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                values.add(resultSet.getLong(1));
            }
        }
        return values;
    }
}
