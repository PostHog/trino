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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

final class TestDuckLakeReads
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
                """
                CREATE TABLE types_scalar (
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
                    c_blob BLOB,
                    c_uuid UUID,
                    c_date DATE,
                    c_time TIME,
                    c_timestamp TIMESTAMP,
                    c_timestamptz TIMESTAMPTZ)
                """,
                """
                INSERT INTO types_scalar VALUES (
                    true, 127, 32767, 2147483647, 9223372036854775807,
                    255, 65535, 4294967295, 18446744073709551615,
                    12345678901234567890123456789012345678,
                    123.45, 1234567890123456789012345.12345,
                    1.5, 2.5, 'hello', '\\xDE\\xAD'::BLOB,
                    '11111111-2222-3333-4444-555555555555',
                    DATE '2024-05-01', TIME '12:34:56.789012',
                    TIMESTAMP '2024-05-01 12:34:56.789012',
                    TIMESTAMPTZ '2024-05-01 12:34:56.789012+02')
                """,
                """
                INSERT INTO types_scalar VALUES (
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                """
                CREATE TABLE types_nested AS SELECT
                    [1, 2, 3] AS c_list,
                    {'a': 1, 'b': 'x'} AS c_struct,
                    MAP {'k1': 10, 'k2': 20} AS c_map
                """,
                "CREATE TABLE snapshots_table (id INTEGER, v VARCHAR)",
                "INSERT INTO snapshots_table VALUES (1, 'one'), (2, 'two')",
                "INSERT INTO snapshots_table VALUES (3, 'three')",
                "CREATE TABLE evolution (a INTEGER)",
                "INSERT INTO evolution VALUES (1), (2)",
                "ALTER TABLE evolution ADD COLUMN b VARCHAR",
                "INSERT INTO evolution VALUES (3, 'three')",
                "CREATE TABLE part_events (ts TIMESTAMP, val INTEGER)",
                "ALTER TABLE part_events SET PARTITIONED BY (year(ts), month(ts))",
                """
                INSERT INTO part_events VALUES
                    (TIMESTAMP '2024-01-15 01:02:03', 1),
                    (TIMESTAMP '2024-02-20 04:05:06', 2),
                    (TIMESTAMP '2025-03-10 07:08:09', 3)
                """,
                "CREATE TABLE deletes_table (id INTEGER, v VARCHAR)",
                "INSERT INTO deletes_table SELECT range, 'v' || range FROM range(0, 10)",
                "INSERT INTO deletes_table SELECT range, 'v' || range FROM range(10, 20)",
                "DELETE FROM deletes_table WHERE id % 3 = 0",
                "DELETE FROM deletes_table WHERE id = 1",
                "CREATE TABLE part_ident (id INTEGER, v VARCHAR)",
                "ALTER TABLE part_ident SET PARTITIONED BY (id)",
                "INSERT INTO part_ident VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "CREATE TABLE stats_prune (id BIGINT, v VARCHAR)",
                "INSERT INTO stats_prune SELECT range, 'v' || range FROM range(0, 100)",
                "INSERT INTO stats_prune SELECT range, 'v' || range FROM range(100, 200)",
                "INSERT INTO stats_prune SELECT range, 'v' || range FROM range(200, 300)",
                "CREATE TABLE part_date (d DATE, v INTEGER)",
                "ALTER TABLE part_date SET PARTITIONED BY (d)",
                "INSERT INTO part_date VALUES (DATE '2024-01-01', 1), (DATE '2024-06-30', 2), (DATE 'infinity', 3)",
                "CREATE TABLE \"Events\" (id INTEGER, name VARCHAR)",
                "INSERT INTO \"Events\" VALUES (1, 'login'), (2, 'logout')",

                // Rows inserted while data inlining is on are written to the catalog database and
                // later flushed into Parquet. A flushed file holds the rows of every snapshot it
                // was flushed from, tagged with an embedded snapshot id, and records the newest of
                // them as partial_max.
                "CALL lake.set_option('data_inlining_row_limit', 100)",
                "CREATE TABLE partial_files (id INTEGER, v VARCHAR)",
                "INSERT INTO partial_files VALUES (1, 'one'), (2, 'two')",
                "INSERT INTO partial_files VALUES (3, 'three')",
                "CALL ducklake_flush_inlined_data('lake')",
                "INSERT INTO partial_files VALUES (4, 'four')",
                "CALL ducklake_flush_inlined_data('lake')",
                "CALL lake.set_option('data_inlining_row_limit', 0)");
        createNameMappingFixtures(catalog);
    }

    /**
     * Creates tables holding Parquet files that were written outside of DuckLake and registered
     * with {@code ducklake_add_data_files}. Such files do not carry DuckLake field ids, so the
     * catalog records a name mapping for them.
     */
    private static void createNameMappingFixtures(TestingDuckLakeCatalog catalog)
            throws SQLException
    {
        String reorderFile = catalog.externalParquetFile("mapped_reorder");
        String renamedFile = catalog.externalParquetFile("mapped_renamed");
        String missingFile = catalog.externalParquetFile("mapped_missing");
        String nestedFile = catalog.externalParquetFile("mapped_nested");
        String hiveFile = catalog.externalParquetFile("mapped_hive/part=5");

        catalog.executeInDuckDb(
                // the Parquet columns are written in a different order than the table columns
                "CREATE TABLE mapped_reorder (id INTEGER, name VARCHAR, amount BIGINT)",
                """
                COPY (SELECT * FROM (VALUES ('x', 100, 1), ('y', 200, 2)) t(name, amount, id))
                TO '%s' (FORMAT parquet)""".formatted(reorderFile),
                "SELECT * FROM ducklake_add_data_files('lake', 'mapped_reorder', '%s', schema => 'main')".formatted(reorderFile),
                // a file written by DuckLake itself, with field ids and no mapping
                "INSERT INTO mapped_reorder VALUES (3, 'z', 300)",

                // renaming a column after the file was registered leaves the mapping pointing at
                // the original Parquet column name, so source_name differs from the column name
                "CREATE TABLE mapped_renamed (a INTEGER, b VARCHAR)",
                """
                COPY (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) t(a, b))
                TO '%s' (FORMAT parquet)""".formatted(renamedFile),
                "SELECT * FROM ducklake_add_data_files('lake', 'mapped_renamed', '%s', schema => 'main')".formatted(renamedFile),
                "ALTER TABLE mapped_renamed RENAME COLUMN a TO a_renamed",
                "INSERT INTO mapped_renamed VALUES (3, 'three')",

                // the file does not contain the 'extra' column, so the mapping has no entry for it
                "CREATE TABLE mapped_missing (id INTEGER, extra VARCHAR)",
                "COPY (SELECT * FROM (VALUES (7), (8)) t(id)) TO '%s' (FORMAT parquet)".formatted(missingFile),
                "SELECT * FROM ducklake_add_data_files('lake', 'mapped_missing', '%s', schema => 'main', allow_missing => true)".formatted(missingFile),

                // the mapping also maps the fields nested inside the struct column
                "CREATE TABLE mapped_nested (id INTEGER, s STRUCT(x INTEGER, y VARCHAR))",
                "COPY (SELECT 1::INTEGER AS id, {'x': 10, 'y': 'a'} AS s) TO '%s' (FORMAT parquet)".formatted(nestedFile),
                "SELECT * FROM ducklake_add_data_files('lake', 'mapped_nested', '%s', schema => 'main')".formatted(nestedFile),

                // the values of the 'part' column are stored in the file path, not in the file
                "CREATE TABLE mapped_hive (part INTEGER, v VARCHAR)",
                "COPY (SELECT 'hello' AS v) TO '%s' (FORMAT parquet)".formatted(hiveFile),
                "SELECT * FROM ducklake_add_data_files('lake', 'mapped_hive', '%s', schema => 'main', hive_partitioning => true)".formatted(hiveFile));
    }

    @Test
    void testShowSchemas()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains("main");
    }

    @Test
    void testShowTables()
    {
        assertThat(computeActual("SHOW TABLES FROM main").getOnlyColumnAsSet())
                .contains("types_scalar", "types_nested", "snapshots_table", "evolution", "part_events");
    }

    @Test
    void testDescribe()
    {
        assertQuery(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'types_scalar' AND column_name IN ('c_uint64', 'c_timestamptz', 'c_uuid', 'c_decimal_short')",
                "VALUES ('c_uint64', 'decimal(20,0)'), ('c_timestamptz', 'timestamp(6) with time zone'), ('c_uuid', 'uuid'), ('c_decimal_short', 'decimal(10,2)')");
    }

    @Test
    void testSelectScalarTypes()
    {
        assertThat(query("SELECT * FROM types_scalar WHERE c_boolean"))
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
                            -- DuckDB stores HUGEINT as DOUBLE in Parquet, so the value is rounded to
                            -- the nearest representable double, exactly as DuckDB reads it back
                            CAST('12345678901234567525491324606797053952' AS decimal(38, 0)),
                            CAST('123.45' AS decimal(10, 2)),
                            CAST('1234567890123456789012345.12345' AS decimal(30, 5)),
                            REAL '1.5',
                            DOUBLE '2.5',
                            VARCHAR 'hello',
                            X'DEAD',
                            UUID '11111111-2222-3333-4444-555555555555',
                            DATE '2024-05-01',
                            TIME '12:34:56.789012',
                            TIMESTAMP '2024-05-01 12:34:56.789012',
                            TIMESTAMP '2024-05-01 10:34:56.789012 UTC')
                        """);
    }

    @Test
    void testSelectNulls()
    {
        assertQuery("SELECT count(*) FROM types_scalar WHERE c_boolean IS NULL", "VALUES 1");
        assertQuery("SELECT count(*) FROM types_scalar WHERE c_uint64 IS NULL", "VALUES 1");
    }

    @Test
    void testCountAndProjections()
    {
        assertQuery("SELECT count(*) FROM types_scalar", "VALUES 2");
        assertQuery("SELECT c_integer FROM types_scalar WHERE c_integer IS NOT NULL", "VALUES 2147483647");
        assertQuery("SELECT c_varchar, c_integer FROM types_scalar WHERE c_varchar IS NOT NULL", "VALUES ('hello', 2147483647)");
    }

    @Test
    void testResultsMatchDuckDb()
            throws SQLException
    {
        assertThat((long) computeScalar("SELECT count(*) FROM types_scalar"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM types_scalar")));
        assertThat((String) computeScalar("SELECT CAST(c_uint64 AS varchar) FROM types_scalar WHERE c_uint64 IS NOT NULL"))
                .isEqualTo(duckDbScalar("SELECT c_uint64::VARCHAR FROM types_scalar WHERE c_uint64 IS NOT NULL"));
        assertThat((String) computeScalar("SELECT CAST(c_hugeint AS varchar) FROM types_scalar WHERE c_hugeint IS NOT NULL"))
                .isEqualTo(duckDbScalar("SELECT c_hugeint::VARCHAR FROM types_scalar WHERE c_hugeint IS NOT NULL"));
        assertThat((long) computeScalar("SELECT sum(val) FROM part_events"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(val)::VARCHAR FROM part_events")));
    }

    @Test
    void testUnsignedBigintRoundTrip()
    {
        // value larger than Long.MAX_VALUE round-trips through the BIGINT reinterpretation
        assertThat(query("SELECT c_uint64 FROM types_scalar WHERE c_uint64 IS NOT NULL"))
                .matches("VALUES CAST('18446744073709551615' AS decimal(20, 0))");
        assertQuery("SELECT count(*) FROM types_scalar WHERE c_uint64 > DECIMAL '9223372036854775807'", "VALUES 1");
    }

    @Test
    void testNestedTypes()
    {
        assertThat(query("SELECT c_list, c_struct, c_map FROM types_nested"))
                .matches(
                        """
                        VALUES (
                            ARRAY[1, 2, 3],
                            CAST(ROW(1, 'x') AS row(a integer, b varchar)),
                            CAST(MAP(ARRAY['k1', 'k2'], ARRAY[10, 20]) AS map(varchar, integer)))
                        """);
        assertQuery("SELECT c_struct.a FROM types_nested", "VALUES 1");
        assertQuery("SELECT c_map['k2'] FROM types_nested", "VALUES 20");
    }

    @Test
    void testMultipleSnapshots()
    {
        assertQuery("SELECT count(*) FROM snapshots_table", "VALUES 3");
        assertQuery("SELECT id, v FROM snapshots_table", "VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    }

    @Test
    void testSchemaEvolution()
    {
        // files written before ALTER TABLE ADD COLUMN do not contain the new column and must read as NULL
        assertQuery("SELECT a, b FROM evolution", "VALUES (1, NULL), (2, NULL), (3, 'three')");
        assertQuery("SELECT a FROM evolution WHERE b IS NULL", "VALUES 1, 2");
    }

    @Test
    void testPartitionedTable()
    {
        assertQuery("SELECT count(*) FROM part_events", "VALUES 3");
        assertQuery("SELECT val FROM part_events", "VALUES 1, 2, 3");
        assertThat(query("SELECT ts FROM part_events WHERE val = 3"))
                .matches("VALUES TIMESTAMP '2025-03-10 07:08:09.000000'");
    }

    @Test
    void testPositionalDeletes()
            throws SQLException
    {
        // ids 0, 3, 6, 9, 12, 15, 18 (multiples of 3) and 1 are deleted across two DELETE statements
        assertQuery("SELECT count(*) FROM deletes_table", "VALUES 12");
        assertQuery("SELECT id FROM deletes_table", "VALUES 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19");
        assertQuery(
                "SELECT id, v FROM deletes_table WHERE id > 10",
                "VALUES (11, 'v11'), (13, 'v13'), (14, 'v14'), (16, 'v16'), (17, 'v17'), (19, 'v19')");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT count(*) FROM deletes_table"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM deletes_table")));
        assertThat((long) computeScalar("SELECT sum(id) FROM deletes_table"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(id)::VARCHAR FROM deletes_table")));
    }

    @Test
    void testPositionalDeletesWithProjections()
    {
        // projection order differs from the table column order
        assertQuery("SELECT v, id FROM deletes_table WHERE id < 5", "VALUES ('v2', 2), ('v4', 4)");
        assertQuery("SELECT v FROM deletes_table WHERE id = 13", "VALUES 'v13'");
        assertQuery("SELECT count(v) FROM deletes_table", "VALUES 12");
    }

    @Test
    void testPositionalDeletesWithPredicatePushdown()
            throws SQLException
    {
        // the delete-position filter and the pushed-down parquet predicate must compose correctly
        assertQuery("SELECT id, v FROM deletes_table WHERE v = 'v13'", "VALUES (13, 'v13')");
        assertQuery("SELECT id FROM deletes_table WHERE id BETWEEN 10 AND 15", "VALUES 10, 11, 13, 14");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT count(*) FROM deletes_table WHERE id BETWEEN 10 AND 15"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM deletes_table WHERE id BETWEEN 10 AND 15")));
        assertThat((long) computeScalar("SELECT sum(id) FROM deletes_table WHERE id > 4"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(id)::VARCHAR FROM deletes_table WHERE id > 4")));
    }

    @Test
    void testInformationSchemaColumns()
    {
        // schema-wide enumeration goes through streamRelationColumns
        MaterializedResult columns = computeActual("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'main'");
        assertThat(columns.getRowCount()).isPositive();

        Map<String, Long> columnCounts = new HashMap<>();
        for (MaterializedRow row : columns.getMaterializedRows()) {
            columnCounts.merge((String) row.getField(0), 1L, Long::sum);
        }
        assertThat(columnCounts.get("types_scalar")).isEqualTo(computeActual("DESCRIBE types_scalar").getRowCount());
        assertThat(columnCounts.get("types_nested")).isEqualTo(computeActual("DESCRIBE types_nested").getRowCount());
        assertThat(columnCounts.get("snapshots_table")).isEqualTo(computeActual("DESCRIBE snapshots_table").getRowCount());

        // mixed-case tables are listed under their lowercase name
        assertThat(columnCounts.get("events")).isEqualTo(2L);
    }

    @Test
    void testMixedCaseTableName()
            throws SQLException
    {
        assertThat(computeActual("SHOW TABLES FROM main").getOnlyColumnAsSet()).contains("events");
        assertQuery("SELECT id, name FROM events", "VALUES (1, 'login'), (2, 'logout')");
        assertThat((long) computeScalar("SELECT count(*) FROM events"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM \"Events\"")));
    }

    @Test
    void testDatePartitionWithSpecialValues()
            throws SQLException
    {
        // DATE 'infinity' partition values cannot be parsed back, so pruning must fail open
        // instead of failing the query
        assertQuery("SELECT count(*) FROM part_date", "VALUES 3");
        assertQuery("SELECT v FROM part_date WHERE d < DATE '2024-02-01'", "VALUES 1");
        assertQuery("SELECT v FROM part_date WHERE d >= DATE '2024-06-01'", "VALUES 2, 3");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT sum(v) FROM part_date WHERE d > DATE '2024-01-01'"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(v)::VARCHAR FROM part_date WHERE d > DATE '2024-01-01'")));
    }

    @Test
    void testPartitionTransformPruning()
    {
        Session session = withFileStatisticsPruning(false);

        // year transform: only the single-row 2025 file is read
        assertQueryStats(
                session,
                "SELECT val FROM part_events WHERE ts >= TIMESTAMP '2025-01-01 00:00:00'",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(1),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(3)));

        // year and month transforms: only the February 2024 file is read
        assertQueryStats(
                session,
                "SELECT val FROM part_events WHERE ts BETWEEN TIMESTAMP '2024-02-01 00:00:00' AND TIMESTAMP '2024-02-29 23:59:59'",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(1),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(2)));

        // pruning does not change results
        assertQuery("SELECT val FROM part_events WHERE ts >= TIMESTAMP '2025-01-01 00:00:00'", "VALUES 3");
        assertQuery("SELECT val FROM part_events WHERE ts < TIMESTAMP '2020-01-01 00:00:00'", "SELECT 1 WHERE false");
    }

    @Test
    void testIdentityPartitionPruning()
    {
        // each partition is a separate file; the identity predicate is enforced by file pruning
        assertQueryStats(
                withFileStatisticsPruning(false),
                "SELECT v FROM part_ident WHERE id = 2",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(1),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of("b")));
        assertQuery("SELECT v FROM part_ident WHERE id = 2", "VALUES 'b'");
        assertQuery("SELECT v FROM part_ident WHERE id IN (1, 3)", "VALUES 'a', 'c'");
        assertQuery("SELECT count(*) FROM part_ident WHERE id = 7", "VALUES 0");
    }

    @Test
    void testFileStatisticsPruning()
    {
        // three files of 100 rows each; the predicate matches only the last file
        assertQueryStats(
                getSession(),
                "SELECT count(v) FROM stats_prune WHERE id >= 250",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(100),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(50L)));

        // with catalog pruning disabled, the predicate pushed down to the parquet reader
        // still prunes row groups, with identical results
        assertQueryStats(
                withFileStatisticsPruning(false),
                "SELECT count(v) FROM stats_prune WHERE id >= 250",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(100),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(50L)));

        // the kill switches disable catalog pruning and parquet statistics and read all files,
        // with identical results
        Session noPruning = Session.builder(withFileStatisticsPruning(false))
                .setCatalogSessionProperty("ducklake", "parquet_ignore_statistics", "true")
                .build();
        assertQueryStats(
                noPruning,
                "SELECT count(v) FROM stats_prune WHERE id >= 250",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(300),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(50L)));

        // a predicate outside the range of all files reads nothing
        assertQueryStats(
                getSession(),
                "SELECT count(v) FROM stats_prune WHERE id > 1000",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(0),
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(Set.of(0L)));
    }

    @Test
    void testShowStats()
    {
        MaterializedResult stats = computeActual("SHOW STATS FOR stats_prune");
        Map<String, MaterializedRow> rowsByColumn = new HashMap<>();
        for (MaterializedRow row : stats.getMaterializedRows()) {
            rowsByColumn.put((String) row.getField(0), row);
        }
        // row count reflects all visible files
        assertThat((Double) rowsByColumn.get(null).getField(4)).isEqualTo(300.0);
        // nulls fraction is known to be zero from the catalog statistics
        assertThat((Double) rowsByColumn.get("id").getField(3)).isEqualTo(0.0);
        // low/high values come from parsed catalog min/max
        assertThat(rowsByColumn.get("id").getField(5)).isEqualTo("0");
        assertThat(rowsByColumn.get("id").getField(6)).isEqualTo("299");

        // row count is net of positional deletes
        MaterializedResult deleteStats = computeActual("SHOW STATS FOR deletes_table");
        MaterializedRow summary = deleteStats.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .collect(onlyElement());
        assertThat((Double) summary.getField(4)).isEqualTo(12.0);
    }

    @Test
    void testNameMappingWithReorderedColumns()
            throws SQLException
    {
        // the registered file stores the columns in a different order than the table
        assertQuery("SELECT id, name, amount FROM mapped_reorder", "VALUES (1, 'x', 100), (2, 'y', 200), (3, 'z', 300)");
        assertQuery("SELECT amount, id FROM mapped_reorder WHERE name = 'y'", "VALUES (200, 2)");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT sum(amount) FROM mapped_reorder"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(amount)::VARCHAR FROM mapped_reorder")));
        assertThat((long) computeScalar("SELECT count(*) FROM mapped_reorder"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM mapped_reorder")));
    }

    @Test
    void testNameMappingWithRenamedColumn()
            throws SQLException
    {
        // the column was renamed after the file was registered, so the mapping resolves the
        // column to the Parquet column 'a', which no longer matches the column name
        assertQuery("SELECT a_renamed, b FROM mapped_renamed", "VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        assertQuery("SELECT b FROM mapped_renamed WHERE a_renamed = 2", "VALUES 'two'");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT sum(a_renamed) FROM mapped_renamed"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(a_renamed)::VARCHAR FROM mapped_renamed")));
        assertThat((String) computeScalar("SELECT b FROM mapped_renamed WHERE a_renamed = 1"))
                .isEqualTo(duckDbScalar("SELECT b FROM mapped_renamed WHERE a_renamed = 1"));
    }

    @Test
    void testPartialFiles()
            throws SQLException
    {
        // every row of the flushed files is at or below the snapshot being read, so the files are
        // read whole
        assertQuery("SELECT id, v FROM partial_files", "VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
        assertQuery("SELECT count(*) FROM partial_files", "VALUES 4");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT sum(id) FROM partial_files"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT sum(id)::VARCHAR FROM partial_files")));
    }

    @Test
    void testNameMappingPredicatePushdown()
            throws SQLException
    {
        // the predicate is pushed to the Parquet reader under the mapped column name; with the
        // catalog pruning disabled the pushed-down predicate is the only file-level filter
        Session session = withFileStatisticsPruning(false);
        assertQuery(session, "SELECT b FROM mapped_renamed WHERE a_renamed >= 2", "VALUES 'two', 'three'");
        assertQuery(session, "SELECT count(*) FROM mapped_renamed WHERE a_renamed > 100", "VALUES 0");
        assertQuery(session, "SELECT id FROM mapped_reorder WHERE amount BETWEEN 150 AND 250", "VALUES 2");

        // results match DuckDB
        assertThat((long) computeScalar(session, "SELECT count(*) FROM mapped_renamed WHERE a_renamed >= 2"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(*) FROM mapped_renamed WHERE a_renamed >= 2")));
    }

    @Test
    void testNameMappingWithMissingColumn()
            throws SQLException
    {
        // the file does not contain the 'extra' column and the mapping has no entry for it, so
        // the column reads as NULL instead of falling back to a Parquet column of the same name
        assertQuery("SELECT id, extra FROM mapped_missing", "VALUES (7, NULL), (8, NULL)");
        assertQuery("SELECT count(*) FROM mapped_missing WHERE extra IS NULL", "VALUES 2");

        // results match DuckDB
        assertThat((long) computeScalar("SELECT count(extra) FROM mapped_missing"))
                .isEqualTo(Long.parseLong(duckDbScalar("SELECT count(extra) FROM mapped_missing")));
    }

    @Test
    void testNameMappingOfNestedFieldsUnsupported()
    {
        // the mapping maps the fields nested inside the struct column, which is not supported
        assertQueryFails("SELECT s FROM mapped_nested", "Column 's' has a name mapping for its nested fields, which is not supported");
        // other columns of the table are readable
        assertQuery("SELECT id FROM mapped_nested", "VALUES 1");
        assertQuery("SELECT count(*) FROM mapped_nested", "VALUES 1");
    }

    @Test
    void testNameMappingOfHivePartitionUnsupported()
    {
        // the values of the partition column are stored in the file path, which is not supported
        assertQueryFails("SELECT part FROM mapped_hive", "Column 'part' is stored as a Hive partition value in the file path, which is not supported");
        // other columns of the table are readable
        assertQuery("SELECT v FROM mapped_hive", "VALUES 'hello'");
        assertQuery("SELECT count(*) FROM mapped_hive", "VALUES 1");
    }

    private Session withFileStatisticsPruning(boolean enabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", "file_statistics_pruning_enabled", Boolean.toString(enabled))
                .build();
    }

    private String duckDbScalar(String sql)
            throws SQLException
    {
        try (Connection connection = catalog.openDuckDbConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next()).isTrue();
            String value = resultSet.getString(1);
            assertThat(resultSet.next()).isFalse();
            return value;
        }
    }
}
