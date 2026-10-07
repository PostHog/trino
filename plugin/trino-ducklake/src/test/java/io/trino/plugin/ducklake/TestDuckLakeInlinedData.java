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
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedData;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_CONCURRENT_MODIFICATION;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.PASSWORD;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Tables holding rows a DuckDB writer stored inline in the catalog database, which DuckLake does
 * for small inserts while {@code data_inlining_row_limit} is set, and rows of data files deleted
 * inline. Every expectation is what DuckDB itself returns for the same table.
 */
final class TestDuckLakeInlinedData
        extends AbstractTestQueryFramework
{
    /**
     * Inserts of more rows than this are written to Parquet; smaller ones are inlined, and so are
     * deletions of at most this many rows of a data file.
     */
    private static final int INLINING_ROW_LIMIT = 10;
    private static final String MODIFY_REFUSAL = "Table main\\.%s has %s stored in the DuckLake catalog database, and %s is not supported for such a table\\. Flush inlined data to Parquet with DuckDB first.*";

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
                "CALL lake.set_option('data_inlining_row_limit', %s)".formatted(INLINING_ROW_LIMIT),

                // every row is inlined
                "CREATE TABLE inlined_only (id INTEGER, v VARCHAR)",
                "INSERT INTO inlined_only VALUES (1, 'one'), (2, 'two'), (3, NULL)",

                // a Parquet file followed by two inlined inserts
                "CREATE TABLE inlined_mixed (id BIGINT, v VARCHAR)",
                "INSERT INTO inlined_mixed SELECT range, 'p' || range FROM range(0, 100)",
                "INSERT INTO inlined_mixed VALUES (100, 'i100'), (101, 'i101')",
                "INSERT INTO inlined_mixed VALUES (102, 'i102')",

                // inlined rows deleted and updated in later snapshots
                "CREATE TABLE inlined_deleted (id INTEGER)",
                "INSERT INTO inlined_deleted VALUES (1), (2), (3), (4), (5)",
                "DELETE FROM inlined_deleted WHERE id IN (2, 4)",
                "UPDATE inlined_deleted SET id = 30 WHERE id = 3",

                // Each schema version keeps its inlined rows in a table of its own, named after the
                // columns of that version. The last rename gives a new column the name a dropped
                // column had, so reading by name instead of by column identifier would hand the
                // dropped column's values to the new one.
                "CREATE TABLE inlined_evolution (a INTEGER, b VARCHAR, c INTEGER)",
                "INSERT INTO inlined_evolution VALUES (1, 'x', 10)",
                "ALTER TABLE inlined_evolution ADD COLUMN d VARCHAR",
                "INSERT INTO inlined_evolution VALUES (2, 'y', 20, 'd2')",
                "ALTER TABLE inlined_evolution RENAME COLUMN b TO b2",
                "INSERT INTO inlined_evolution VALUES (3, 'z', 30, 'd3')",
                "ALTER TABLE inlined_evolution DROP COLUMN c",
                "INSERT INTO inlined_evolution VALUES (4, 'w', 'd4')",
                "ALTER TABLE inlined_evolution RENAME COLUMN d TO c",
                "INSERT INTO inlined_evolution VALUES (5, 'v', 'c5')",
                "ALTER TABLE inlined_evolution ALTER COLUMN a SET DATA TYPE BIGINT",
                "INSERT INTO inlined_evolution VALUES (6000000000, 'u', 'c6')",

                // every scalar type the connector reads, with values at the edges of how DuckDB
                // writes them to the catalog database
                """
                CREATE TABLE inlined_types (
                    c_boolean BOOLEAN,
                    c_tinyint TINYINT,
                    c_smallint SMALLINT,
                    c_integer INTEGER,
                    c_bigint BIGINT,
                    c_utinyint UTINYINT,
                    c_usmallint USMALLINT,
                    c_uinteger UINTEGER,
                    c_uint64 UBIGINT,
                    c_hugeint HUGEINT,
                    c_decimal_short DECIMAL(10,2),
                    c_decimal_long DECIMAL(30,5),
                    c_real FLOAT,
                    c_double DOUBLE,
                    c_varchar VARCHAR,
                    c_json JSON,
                    c_blob BLOB,
                    c_uuid UUID,
                    c_date DATE,
                    c_time TIME,
                    c_timestamp TIMESTAMP,
                    c_timestamp_s TIMESTAMP_S,
                    c_timestamp_ms TIMESTAMP_MS,
                    c_timestamp_ns TIMESTAMP_NS,
                    c_timestamptz TIMESTAMPTZ)
                """,
                """
                INSERT INTO inlined_types VALUES (
                    true, 127, 32767, 2147483647, 9223372036854775807,
                    255, 65535, 4294967295, 18446744073709551615,
                    12345678901234567890123456789012345678,
                    123.45, 1234567890123456789012345.12345,
                    1.5, 2.5, 'hello', '{"a": [1, 2]}', '\\xDE\\xAD'::BLOB,
                    '11111111-2222-3333-4444-555555555555',
                    DATE '2024-05-01', TIME '12:34:56.789012',
                    TIMESTAMP '2024-05-01 12:34:56.789012',
                    TIMESTAMP_S '2024-05-01 12:34:56',
                    TIMESTAMP_MS '2024-05-01 12:34:56.789',
                    TIMESTAMP_NS '2024-05-01 12:34:56.789012345',
                    TIMESTAMPTZ '2024-05-01 12:34:56.789012+02')
                """,
                """
                INSERT INTO inlined_types VALUES (
                    false, -128, -32768, -2147483648, -9223372036854775808,
                    0, 0, 0, 0,
                    -170141183460469231731687303715884105,
                    -0.01, -0.00001,
                    -0.0, -1e300, 'it''s ä € 😀', '[]', ''::BLOB,
                    '00000000-0000-0000-0000-000000000000',
                    DATE '0044-03-15 (BC)', TIME '00:00:00',
                    TIMESTAMP '1969-12-31 23:59:59.999999',
                    make_timestamp(-43, 3, 15, 12, 30, 0)::TIMESTAMP_S,
                    make_timestamp(-43, 3, 15, 12, 30, 0.5)::TIMESTAMP_MS,
                    TIMESTAMP_NS '1969-12-31 23:59:59.999999999',
                    TIMESTAMPTZ '2024-05-01 12:34:56.000001+05:30')
                """,
                """
                INSERT INTO inlined_types VALUES (
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                // the same values written to Parquet, which the connector already reads like DuckDB
                "CALL lake.set_option('data_inlining_row_limit', 0)",
                "CREATE TABLE parquet_types AS SELECT * FROM inlined_types",
                "CALL lake.set_option('data_inlining_row_limit', %s)".formatted(INLINING_ROW_LIMIT),

                // DuckDB stores nested values inline as the text of a DuckDB literal
                "CREATE TABLE inlined_nested (id INTEGER, l INTEGER[], s STRUCT(a INTEGER))",
                "INSERT INTO inlined_nested VALUES (1, NULL, NULL)",
                "CREATE TABLE inlined_nested_values (id INTEGER, l INTEGER[])",
                "INSERT INTO inlined_nested_values VALUES (1, [1, 2])",

                // inlined rows have no partition values to prune by
                "CREATE TABLE inlined_partitioned (id INTEGER, p INTEGER)",
                "ALTER TABLE inlined_partitioned SET PARTITIONED BY (p)",
                "INSERT INTO inlined_partitioned SELECT range, range % 2 FROM range(0, 100)",
                "INSERT INTO inlined_partitioned VALUES (1000, 1), (1001, 0)",

                // rows of a data file deleted inline, before and after a delete file was written
                "CREATE TABLE inlined_file_deletes (i INTEGER)",
                "INSERT INTO inlined_file_deletes SELECT range FROM range(0, 100)",
                "DELETE FROM inlined_file_deletes WHERE i IN (5, 7)",
                "DELETE FROM inlined_file_deletes WHERE i >= 50",
                "DELETE FROM inlined_file_deletes WHERE i = 9",

                "CREATE TABLE inlined_insert (id INTEGER, v VARCHAR)",
                "INSERT INTO inlined_insert VALUES (1, 'duckdb')",

                "CREATE TABLE inlined_drop (id INTEGER)",
                "INSERT INTO inlined_drop VALUES (1)",

                "CREATE TABLE inlined_pinned (id INTEGER)",
                "INSERT INTO inlined_pinned VALUES (1), (2)",

                "CREATE TABLE inlined_flushed (id INTEGER, v VARCHAR)",
                "INSERT INTO inlined_flushed VALUES (1, 'one'), (2, 'two')",
                "DELETE FROM inlined_flushed WHERE id = 2",
                "INSERT INTO inlined_flushed VALUES (3, 'three')",

                "CREATE TABLE inlined_race (id INTEGER)",
                "INSERT INTO inlined_race VALUES (1), (2)",

                // flushed by a test as another table than the one it reads
                "CREATE TABLE inlined_other (id INTEGER)",
                "INSERT INTO inlined_other VALUES (1)",

                // read to pin the snapshot of a transaction; never changed
                "CREATE TABLE snapshot_pin (id INTEGER)",
                "INSERT INTO snapshot_pin VALUES (1), (2), (3)",

                "CREATE TABLE inlined_dropped (id INTEGER)",
                "INSERT INTO inlined_dropped VALUES (1), (2)",

                // flushed by a test while a transaction reads an older snapshot
                "CREATE TABLE inlined_flushed_deletes (id INTEGER)",
                "INSERT INTO inlined_flushed_deletes VALUES (1), (2), (3), (4), (5)",
                "DELETE FROM inlined_flushed_deletes WHERE id = 2",

                // a delete file DuckDB replaces by a later DELETE of more rows than it inlines
                "CREATE TABLE merged_deletes (i INTEGER)",
                "INSERT INTO merged_deletes SELECT range FROM range(0, 100)",
                "DELETE FROM merged_deletes WHERE i < 20");
    }

    @Test
    void testInlinedRowsOnly()
            throws SQLException
    {
        // the rows are in the catalog database, not in a data file
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s".formatted(tableId("inlined_only")))).isZero();
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_inlined_data_tables WHERE table_id = %s".formatted(tableId("inlined_only")))).isPositive();
        assertQuery("SELECT id, v FROM inlined_only", "VALUES (1, 'one'), (2, 'two'), (3, NULL)");
        assertQuery("SELECT v FROM inlined_only WHERE id = 2", "VALUES 'two'");
        assertQuery("SELECT count(*) FROM inlined_only", "VALUES 3");
        assertMatchesDuckDb("SELECT id, v FROM inlined_only ORDER BY id");
    }

    @Test
    void testParquetAndInlinedRows()
            throws SQLException
    {
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s".formatted(tableId("inlined_mixed")))).isEqualTo(1);
        assertQuery("SELECT count(*), sum(id), max(v) FROM inlined_mixed", "VALUES (103, 5253, 'p99')");
        assertQuery("SELECT id, v FROM inlined_mixed WHERE id >= 99", "VALUES (99, 'p99'), (100, 'i100'), (101, 'i101'), (102, 'i102')");
        assertQuery("SELECT count(*) FROM inlined_mixed WHERE v LIKE 'i%'", "VALUES 3");
        assertMatchesDuckDb("SELECT id, v FROM inlined_mixed ORDER BY id");
    }

    @Test
    void testDeletedInlinedRowsAreNotRead()
            throws SQLException
    {
        assertQuery("SELECT id FROM inlined_deleted", "VALUES 1, 5, 30");
        assertQuery("SELECT count(*) FROM inlined_deleted", "VALUES 3");
        assertMatchesDuckDb("SELECT id FROM inlined_deleted ORDER BY id");
    }

    @Test
    void testSchemaEvolution()
            throws SQLException
    {
        assertThat(query("SELECT a, b2, c FROM inlined_evolution"))
                .matches(
                        """
                        VALUES
                            (BIGINT '1', VARCHAR 'x', CAST(NULL AS varchar)),
                            (2, 'y', 'd2'),
                            (3, 'z', 'd3'),
                            (4, 'w', 'd4'),
                            (5, 'v', 'c5'),
                            (6000000000, 'u', 'c6')
                        """);
        assertQuery("SELECT a FROM inlined_evolution WHERE c IS NULL", "VALUES 1");
        assertMatchesDuckDb("SELECT a, b2, c FROM inlined_evolution ORDER BY a");
    }

    @Test
    void testTypesMatchParquet()
    {
        // the 128-bit integer column is left out: DuckDB writes it to Parquet as a DOUBLE, so only
        // its inlined values are exact
        String columns =
                """
                c_boolean, c_tinyint, c_smallint, c_integer, c_bigint, c_utinyint, c_usmallint, c_uinteger, c_uint64,
                c_decimal_short, c_decimal_long, c_real, c_double, c_varchar, c_json, c_blob, c_uuid, c_date, c_time,
                c_timestamp, c_timestamp_s, c_timestamp_ms, c_timestamp_ns, c_timestamptz""";
        assertThat(computeActual("SELECT %s FROM inlined_types EXCEPT SELECT %s FROM parquet_types".formatted(columns, columns)).getRowCount()).isZero();
        assertThat(computeActual("SELECT %s FROM parquet_types EXCEPT SELECT %s FROM inlined_types".formatted(columns, columns)).getRowCount()).isZero();
        assertQuery("SELECT count(*) FROM inlined_types", "VALUES 3");
        assertQuery("SELECT count(*) FROM inlined_types WHERE c_boolean IS NULL AND c_timestamptz IS NULL AND c_varchar IS NULL", "VALUES 1");
    }

    @Test
    void testTypes()
            throws SQLException
    {
        assertThat(query("SELECT * FROM inlined_types WHERE c_boolean"))
                .matches(
                        """
                        VALUES (
                            true,
                            TINYINT '127',
                            SMALLINT '32767',
                            2147483647,
                            BIGINT '9223372036854775807',
                            SMALLINT '255',
                            65535,
                            BIGINT '4294967295',
                            CAST('18446744073709551615' AS decimal(20, 0)),
                            CAST('12345678901234567890123456789012345678' AS decimal(38, 0)),
                            CAST('123.45' AS decimal(10, 2)),
                            CAST('1234567890123456789012345.12345' AS decimal(30, 5)),
                            REAL '1.5',
                            DOUBLE '2.5',
                            VARCHAR 'hello',
                            VARCHAR '{"a": [1, 2]}',
                            X'DEAD',
                            UUID '11111111-2222-3333-4444-555555555555',
                            DATE '2024-05-01',
                            TIME '12:34:56.789012',
                            TIMESTAMP '2024-05-01 12:34:56.789012',
                            TIMESTAMP '2024-05-01 12:34:56',
                            TIMESTAMP '2024-05-01 12:34:56.789',
                            TIMESTAMP '2024-05-01 12:34:56.789012345',
                            TIMESTAMP '2024-05-01 10:34:56.789012 UTC')
                        """);
        assertThat(query("SELECT c_hugeint, c_varchar, c_date, c_timestamp, c_timestamp_ns, c_timestamptz FROM inlined_types WHERE NOT c_boolean"))
                .matches(
                        """
                        VALUES (
                            CAST('-170141183460469231731687303715884105' AS decimal(38, 0)),
                            VARCHAR 'it''s ä € 😀',
                            DATE '-0043-03-15',
                            TIMESTAMP '1969-12-31 23:59:59.999999',
                            TIMESTAMP '1969-12-31 23:59:59.999999999',
                            TIMESTAMP '2024-05-01 07:04:56.000001 UTC')
                        """);
        // the 128-bit integer is read exactly from the catalog database, as DuckDB reads it
        assertThat((String) computeScalar("SELECT CAST(c_hugeint AS varchar) FROM inlined_types WHERE c_boolean"))
                .isEqualTo(catalog.scalar("SELECT c_hugeint::VARCHAR FROM inlined_types WHERE c_boolean"));
        assertThat((long) computeScalar("SELECT date_diff('day', DATE '1970-01-01', c_date) FROM inlined_types WHERE NOT c_boolean"))
                .isEqualTo(Long.parseLong(catalog.scalar("SELECT (c_date - DATE '1970-01-01')::VARCHAR FROM inlined_types WHERE NOT c_boolean")));
    }

    @Test
    void testNestedTypes()
    {
        // a nested column holding only NULLs reads as such
        assertThat(query("SELECT id, l, s FROM inlined_nested"))
                .matches("VALUES (1, CAST(NULL AS array(integer)), CAST(NULL AS row(a integer)))");
        // the text DuckDB stores a nested value as is not read, so a query that needs one fails
        assertQuery("SELECT id FROM inlined_nested_values", "VALUES 1");
        assertQueryFails(
                "SELECT l FROM inlined_nested_values",
                "Column 'l' of type array\\(integer\\) has inlined values, which are not supported for nested types\\. Flush inlined data to Parquet with DuckDB first");
    }

    @Test
    void testPartitionPredicateOnInlinedRows()
            throws SQLException
    {
        // the predicate on the partition column cannot be enforced by pruning data files, because
        // the inlined rows have no partition value; the engine filters them
        assertQuery("SELECT count(*) FROM inlined_partitioned WHERE p = 1", "VALUES 51");
        assertQuery("SELECT id FROM inlined_partitioned WHERE p = 1 AND id >= 1000", "VALUES 1000");
        assertQuery("SELECT id FROM inlined_partitioned WHERE p = 0 AND id >= 1000", "VALUES 1001");
        assertMatchesDuckDb("SELECT p, count(*) FROM inlined_partitioned GROUP BY p ORDER BY p");
    }

    @Test
    void testRowsDeletedInline()
            throws SQLException
    {
        // the deletions are recorded inline, then in a delete file, then inline again
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_inlined_delete_%s".formatted(tableId("inlined_file_deletes")))).isPositive();
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_delete_file WHERE table_id = %s AND end_snapshot IS NULL".formatted(tableId("inlined_file_deletes")))).isEqualTo(1);

        assertQuery("SELECT count(*) FROM inlined_file_deletes", "VALUES 47");
        assertQuery("SELECT count(*) FROM inlined_file_deletes WHERE i IN (5, 7, 9, 50, 99)", "VALUES 0");
        assertQuery("SELECT min(i), max(i), sum(i) FROM inlined_file_deletes", "VALUES (0, 49, 1204)");
        assertMatchesDuckDb("SELECT i FROM inlined_file_deletes ORDER BY i");
        assertThat((long) computeScalar("SELECT count(*) FROM inlined_file_deletes"))
                .isEqualTo(Long.parseLong(catalog.scalar("SELECT count(*) FROM inlined_file_deletes")));
    }

    @Test
    void testCountIncludesInlinedRows()
    {
        assertQuery("SELECT count(*) FROM inlined_mixed", "VALUES 103");
        assertQuery("SELECT count(*) FROM inlined_deleted", "VALUES 3");
        assertQuery("SELECT count(*) FROM inlined_only", "VALUES 3");
        assertQuery("SELECT count(*) FROM inlined_file_deletes", "VALUES 47");
        // and so do the statistics the planner sees
        MaterializedResult statistics = computeActual("SHOW STATS FOR inlined_mixed");
        assertThat(statistics.getMaterializedRows().getLast().getField(4)).isEqualTo(103.0);
    }

    @Test
    void testModifyingInlinedRowsIsRefused()
    {
        String rows = "inlined data";
        assertQueryFails("DELETE FROM inlined_mixed WHERE id = 100", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "(deleting|modifying) rows"));
        assertQueryFails("DELETE FROM inlined_mixed", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "deleting rows"));
        assertQueryFails("UPDATE inlined_mixed SET v = 'x' WHERE id = 1", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "modifying rows"));
        assertQueryFails(
                "MERGE INTO inlined_mixed t USING (VALUES 1) s(id) ON t.id = s.id WHEN MATCHED THEN DELETE",
                MODIFY_REFUSAL.formatted("inlined_mixed", rows, "modifying rows"));
        assertQueryFails("TRUNCATE TABLE inlined_mixed", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "truncating it"));
        assertQueryFails("ALTER TABLE inlined_mixed ADD COLUMN extra integer", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "adding a column"));
        assertQueryFails("ALTER TABLE inlined_mixed DROP COLUMN v", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "dropping a column"));
        assertQueryFails("ALTER TABLE inlined_mixed RENAME COLUMN v TO w", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "renaming a column"));
        assertQueryFails("ALTER TABLE inlined_mixed ALTER COLUMN id SET DATA TYPE bigint", MODIFY_REFUSAL.formatted("inlined_mixed", rows, "changing the type of a column"));

        // a data file with rows deleted inline cannot be modified either
        String deletions = "rows deleted inline";
        assertQueryFails("DELETE FROM inlined_file_deletes WHERE i = 1", MODIFY_REFUSAL.formatted("inlined_file_deletes", deletions, "(deleting|modifying) rows"));
        assertQueryFails("UPDATE inlined_file_deletes SET i = 1 WHERE i = 2", MODIFY_REFUSAL.formatted("inlined_file_deletes", deletions, "modifying rows"));
        assertQueryFails("TRUNCATE TABLE inlined_file_deletes", MODIFY_REFUSAL.formatted("inlined_file_deletes", deletions, "truncating it"));

        // nothing was changed
        assertQuery("SELECT count(*) FROM inlined_mixed", "VALUES 103");
        assertQuery("SELECT count(*) FROM inlined_file_deletes", "VALUES 47");
    }

    @Test
    void testInsertIntoTableWithInlinedRows()
            throws SQLException
    {
        // an insert only adds a data file, beside the inlined rows
        assertUpdate("INSERT INTO inlined_insert VALUES (2, 'trino')", 1);
        assertQuery("SELECT id, v FROM inlined_insert", "VALUES (1, 'duckdb'), (2, 'trino')");
        assertThat(catalog.rows("SELECT id::VARCHAR, v FROM inlined_insert ORDER BY id")).containsExactly("1", "duckdb", "2", "trino");
    }

    @Test
    void testDropTableWithInlinedRows()
            throws SQLException
    {
        assertUpdate("DROP TABLE inlined_drop");
        assertThat(catalog.scalar("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'inlined_drop'")).isEqualTo("0");
    }

    /**
     * A scan reads the inlined rows at the snapshot its transaction is pinned to, as it reads the
     * data files, so a row DuckDB inlines afterwards is not part of it.
     */
    @Test
    void testInlinedRowsAreReadAtTheSnapshot()
            throws SQLException
    {
        JdbcDuckLakeMetastore metastore = newMetastore();
        long tableId = tableId("inlined_pinned");
        long snapshotId = metastore.currentSnapshotId();
        DuckLakeInlinedData inlinedData = metastore.inlinedData(snapshotId, tableId);
        catalog.executeInDuckDb("INSERT INTO inlined_pinned VALUES (3)");

        assertThat(readInlinedIds(metastore, tableId, snapshotId, inlinedData)).containsExactlyInAnyOrder(1L, 2L);
        assertThat(readInlinedIds(metastore, tableId, metastore.currentSnapshotId(), inlinedData)).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertQuery("SELECT id FROM inlined_pinned", "VALUES 1, 2, 3");
    }

    private static List<Long> readInlinedIds(JdbcDuckLakeMetastore metastore, long tableId, long snapshotId, DuckLakeInlinedData inlinedData)
            throws SQLException
    {
        ImmutableList.Builder<Long> ids = ImmutableList.builder();
        for (var inlinedTable : inlinedData.tables()) {
            try (JdbcDuckLakeMetastore.InlinedRows rows = metastore.openInlinedRows(
                    tableId,
                    snapshotId,
                    inlinedData.watermarkSnapshotId(),
                    inlinedTable.tableName(),
                    ImmutableList.of(inlinedTable.columns().getFirst().name()),
                    10)) {
                while (rows.next()) {
                    ids.add(rows.row().getLong(1));
                }
            }
        }
        return ids.build();
    }

    @Test
    void testReadsAfterFlush()
            throws SQLException
    {
        assertQuery("SELECT id, v FROM inlined_flushed", "VALUES (1, 'one'), (3, 'three')");
        catalog.executeInDuckDb("CALL ducklake_flush_inlined_data('lake', table_name => 'inlined_flushed')");
        assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s".formatted(tableId("inlined_flushed")))).isPositive();
        // the rows now live in a data file and the deleted one in its delete file
        assertQuery("SELECT id, v FROM inlined_flushed", "VALUES (1, 'one'), (3, 'three')");
        assertQuery("SELECT count(*) FROM inlined_flushed", "VALUES 2");
        assertMatchesDuckDb("SELECT id, v FROM inlined_flushed ORDER BY id");
    }

    /**
     * DuckDB moves inlined rows into data files by deleting them from the catalog database, so a
     * read of rows listed before such a flush would miss them. The read notices and fails rather
     * than return fewer rows.
     */
    @Test
    void testReadOfRowsFlushedAfterListingFails()
            throws SQLException
    {
        JdbcDuckLakeMetastore metastore = newMetastore();
        long tableId = tableId("inlined_race");
        long snapshotId = metastore.currentSnapshotId();
        DuckLakeInlinedData inlinedData = metastore.inlinedData(snapshotId, tableId);
        assertThat(inlinedData.tables()).hasSize(1);
        String inlinedTable = inlinedData.tables().getFirst().tableName();
        List<String> columns = ImmutableList.of(inlinedData.tables().getFirst().columns().getFirst().name());

        assertThat(metastore.inlinedDataFlushedAfter(tableId, inlinedData.watermarkSnapshotId())).isFalse();
        try (JdbcDuckLakeMetastore.InlinedRows rows = metastore.openInlinedRows(tableId, snapshotId, inlinedData.watermarkSnapshotId(), inlinedTable, columns, 10)) {
            int count = 0;
            while (rows.next()) {
                count++;
            }
            assertThat(count).isEqualTo(2);
        }

        // a flush of another table is no reason to fail
        catalog.executeInDuckDb("CALL ducklake_flush_inlined_data('lake', table_name => 'inlined_other')");
        assertThat(metastore.inlinedDataFlushedAfter(tableId, inlinedData.watermarkSnapshotId())).isFalse();

        catalog.executeInDuckDb("CALL ducklake_flush_inlined_data('lake', table_name => 'inlined_race')");
        assertThat(metastore.inlinedDataFlushedAfter(tableId, inlinedData.watermarkSnapshotId())).isTrue();
        assertThatThrownBy(() -> metastore.openInlinedRows(tableId, snapshotId, inlinedData.watermarkSnapshotId(), inlinedTable, columns, 10).close())
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(DUCKLAKE_CONCURRENT_MODIFICATION.toErrorCode()))
                .hasMessageContaining("was flushed to Parquet files while the query was reading it");

        // a listing taken after the flush finds the rows in the data file, and reads them
        assertQuery("SELECT id FROM inlined_race", "VALUES 1, 2");
        assertQuery("SELECT id FROM inlined_other", "VALUES 1");
    }

    /**
     * A flush that drops an inlined data table after its rows were listed leaves them visible in
     * the snapshot of the reading transaction, but the table cannot be read any more. The read
     * fails rather than return no rows, and a count from the catalog is not trusted.
     */
    @Test
    void testInlinedTableDroppedAfterListing()
            throws SQLException
    {
        JdbcDuckLakeMetastore metastore = newMetastore();
        long tableId = tableId("inlined_dropped");
        long snapshotId = metastore.currentSnapshotId();
        DuckLakeInlinedData inlinedData = metastore.inlinedData(snapshotId, tableId);
        assertThat(inlinedData.tables()).hasSize(1);
        String inlinedTable = inlinedData.tables().getFirst().tableName();
        List<String> columns = ImmutableList.of(inlinedData.tables().getFirst().columns().getFirst().name());
        assertThat(metastore.rowCount(snapshotId, tableId).exact()).isTrue();

        // what a drop committed after the read began looks like to the lookup of the table
        executeInMetastore("DROP TABLE %s".formatted(inlinedTable));

        assertThatThrownBy(() -> metastore.openInlinedRows(tableId, snapshotId, inlinedData.watermarkSnapshotId(), inlinedTable, columns, 10).close())
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(DUCKLAKE_CONCURRENT_MODIFICATION.toErrorCode()))
                .hasMessageContaining("was dropped while the query was reading it");
        assertThat(metastore.rowCount(snapshotId, tableId).exact()).isFalse();
    }

    /**
     * DuckDB flushes inlined rows into a data file together with a delete file holding the
     * deletions of those rows, each tagged with the snapshot that made it, and registers the delete
     * file from the oldest of them on. A reader of an older snapshot applies only the deletions it
     * can see, like DuckDB.
     */
    @Test
    void testFlushedDeletionsNewerThanTheSnapshotAreNotApplied()
    {
        inTransaction(session -> {
            // the transaction reads the snapshot current when it first reads the catalog
            assertQuery(session, "SELECT id FROM snapshot_pin", "VALUES 1, 2, 3");
            try {
                catalog.executeInDuckDb(
                        "DELETE FROM inlined_flushed_deletes WHERE id = 4",
                        "CALL ducklake_flush_inlined_data('lake', table_name => 'inlined_flushed_deletes')");
                assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s".formatted(tableId("inlined_flushed_deletes")))).isEqualTo(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            assertQuery(session, "SELECT id FROM inlined_flushed_deletes", "VALUES 1, 3, 4, 5");
            assertQuery(session, "SELECT count(*) FROM inlined_flushed_deletes", "VALUES 4");
        });
        assertQuery("SELECT id FROM inlined_flushed_deletes", "VALUES 1, 3, 5");
        assertQuery("SELECT count(*) FROM inlined_flushed_deletes", "VALUES 3");
    }

    /**
     * A DELETE of a data file that already has a delete file writes one holding both, each
     * deletion tagged with its snapshot and the file registered from the oldest of them on, and
     * removes the old one from the catalog. A reader of a snapshot older than the second DELETE
     * reads the new file and applies only the deletions of the first.
     */
    @Test
    void testMergedDeletionsNewerThanTheSnapshotAreNotApplied()
    {
        inTransaction(session -> {
            assertQuery(session, "SELECT id FROM snapshot_pin", "VALUES 1, 2, 3");
            try {
                catalog.executeInDuckDb("DELETE FROM merged_deletes WHERE i >= 20 AND i < 40");
                assertThat(metastoreLong("SELECT count(*) FROM ducklake_delete_file WHERE table_id = %s".formatted(tableId("merged_deletes")))).isEqualTo(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            assertQuery(session, "SELECT count(*) FROM merged_deletes", "VALUES 80");
            assertQuery(session, "SELECT min(i), max(i), sum(i) FROM merged_deletes", "VALUES (20, 99, 4760)");
        });
        assertQuery("SELECT count(*) FROM merged_deletes", "VALUES 60");
        assertQuery("SELECT min(i), max(i), sum(i) FROM merged_deletes", "VALUES (40, 99, 4170)");
    }

    private JdbcDuckLakeMetastore newMetastore()
    {
        return new JdbcDuckLakeMetastore(
                () -> DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD),
                new DuckLakeConfig());
    }

    private void assertMatchesDuckDb(@Language("SQL") String sql)
            throws SQLException
    {
        MaterializedResult result = computeActual(sql);
        ImmutableList.Builder<String> trinoValues = ImmutableList.builder();
        result.getMaterializedRows().forEach(row -> row.getFields().forEach(value -> trinoValues.add(value == null ? "<null>" : value.toString())));
        assertThat(trinoValues.build()).isEqualTo(catalog.rows(sql));
    }

    private long tableId(String tableName)
            throws SQLException
    {
        return metastoreLong("SELECT table_id FROM ducklake_table WHERE table_name = '%s' AND end_snapshot IS NULL".formatted(tableName));
    }

    private void executeInMetastore(@Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
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
