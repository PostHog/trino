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
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Tests that what this connector writes is a DuckLake table like any other: DuckDB reads back
 * exactly what Trino wrote, and Trino keeps reading what it wrote after the catalog has changed
 * underneath it.
 * <p>
 * Every assertion that matters is made twice, once through Trino and once through DuckDB, because
 * the point of writing DuckLake rather than a private format is that both engines agree.
 */
final class TestDuckLakeWrites
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            // the catalog has to exist before Trino can attach to it, and only DuckDB creates one
            catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
        }
        catch (SQLException e) {
            abort("Failed to create a DuckLake catalog with DuckDB (extension download requires network access): " + e);
        }
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    @Test
    void testCreateTableReadableByDuckDb()
    {
        String table = "create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, name VARCHAR) COMMENT 'a table'");
        try {
            assertThat(duckDbRows("SELECT column_name FROM duckdb_columns() WHERE table_name = '" + table + "' ORDER BY column_index"))
                    .isEqualTo(List.of("id", "name"));
            assertThat(duckDbScalar("SELECT comment FROM duckdb_tables() WHERE table_name = '" + table + "'"))
                    .isEqualTo("a table");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertScalarTypesRoundTripThroughDuckDb()
    {
        String table = "scalars_" + randomNameSuffix();
        assertUpdate(
                """
                CREATE TABLE %s (
                    c_boolean BOOLEAN,
                    c_tinyint TINYINT,
                    c_smallint SMALLINT,
                    c_integer INTEGER,
                    c_bigint BIGINT,
                    c_real REAL,
                    c_double DOUBLE,
                    c_decimal_short DECIMAL(18, 4),
                    c_decimal_long DECIMAL(38, 10),
                    c_varchar VARCHAR,
                    c_varbinary VARBINARY,
                    c_uuid UUID,
                    c_date DATE,
                    c_time TIME(6),
                    c_timestamp_seconds TIMESTAMP(0),
                    c_timestamp_millis TIMESTAMP(3),
                    c_timestamp_micros TIMESTAMP(6),
                    c_timestamp_nanos TIMESTAMP(9),
                    c_timestamp_tz TIMESTAMP(6) WITH TIME ZONE)""".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES (
                        true, 1, 2, 3, 4, REAL '1.5', DOUBLE '2.5', DECIMAL '3.1234', DECIMAL '4.1234567890',
                        'hello', X'616263', UUID '00000000-0000-0000-0000-000000000001',
                        DATE '2024-01-02', TIME '12:34:56.123456',
                        TIMESTAMP '2024-01-02 03:04:05',
                        TIMESTAMP '2024-01-02 03:04:05.123',
                        TIMESTAMP '2024-01-02 03:04:05.123456',
                        TIMESTAMP '2024-01-02 03:04:05.123456789',
                        TIMESTAMP '2024-01-02 03:04:05.123456 UTC')""".formatted(table),
                    1);

            assertThat(query("SELECT * FROM " + table))
                    .matches(
                            """
                            VALUES (
                                true, TINYINT '1', SMALLINT '2', 3, BIGINT '4', REAL '1.5', DOUBLE '2.5',
                                CAST(DECIMAL '3.1234' AS DECIMAL(18, 4)), CAST(DECIMAL '4.1234567890' AS DECIMAL(38, 10)),
                                VARCHAR 'hello', X'616263', UUID '00000000-0000-0000-0000-000000000001',
                                DATE '2024-01-02', TIME '12:34:56.123456',
                                TIMESTAMP '2024-01-02 03:04:05',
                                TIMESTAMP '2024-01-02 03:04:05.123',
                                TIMESTAMP '2024-01-02 03:04:05.123456',
                                TIMESTAMP '2024-01-02 03:04:05.123456789',
                                TIMESTAMP '2024-01-02 03:04:05.123456 UTC')""");

            // DuckDB reads the same values out of the same file
            assertThat(duckDbRows("SELECT c_boolean, c_tinyint, c_smallint, c_integer, c_bigint FROM " + table))
                    .isEqualTo(List.of("true", "1", "2", "3", "4"));
            assertThat(duckDbRows("SELECT c_real, c_double, c_decimal_short, c_decimal_long FROM " + table))
                    .isEqualTo(List.of("1.5", "2.5", "3.1234", "4.1234567890"));
            assertThat(duckDbRows("SELECT c_varchar, c_uuid, c_date, c_time FROM " + table))
                    .isEqualTo(List.of("hello", "00000000-0000-0000-0000-000000000001", "2024-01-02", "12:34:56.123456"));
            assertThat(duckDbRows("SELECT c_timestamp_seconds, c_timestamp_millis, c_timestamp_micros, c_timestamp_nanos FROM " + table))
                    .isEqualTo(List.of(
                            "2024-01-02 03:04:05.0",
                            "2024-01-02 03:04:05.123",
                            "2024-01-02 03:04:05.123456",
                            "2024-01-02 03:04:05.123456789"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertNestedTypesRoundTripThroughDuckDb()
    {
        String table = "nested_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_list ARRAY(INTEGER), c_map MAP(VARCHAR, INTEGER), c_row ROW(a INTEGER, b VARCHAR), c_deep ROW(x ARRAY(INTEGER), y ROW(z VARCHAR)))");
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (ARRAY[1, 2, 3], MAP(ARRAY['k'], ARRAY[1]), ROW(7, 'x'), ROW(ARRAY[4, 5], ROW('q'))),
                        (NULL, NULL, NULL, NULL)""".formatted(table),
                    2);

            assertThat(query("SELECT c_list, c_row.b, c_deep.y.z FROM " + table + " WHERE c_list IS NOT NULL"))
                    .matches("VALUES (ARRAY[1, 2, 3], VARCHAR 'x', VARCHAR 'q')");
            assertThat(duckDbRows("SELECT c_list::VARCHAR, c_map::VARCHAR, c_row::VARCHAR, c_deep::VARCHAR FROM " + table + " WHERE c_list IS NOT NULL"))
                    .isEqualTo(List.of("[1, 2, 3]", "{k=1}", "{'a': 7, 'b': x}", "{'x': [4, 5], 'y': {'z': q}}"));
            assertThat(duckDbScalar("SELECT count(*) FROM " + table + " WHERE c_list IS NULL")).isEqualTo("1");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testCreateTableAsSelect()
    {
        String table = "ctas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id, id * 2 AS doubled FROM UNNEST(sequence(1, 100)) AS t(id)", 100);
        try {
            assertQuery("SELECT count(*), sum(doubled) FROM " + table, "VALUES (100, 10100)");
            assertThat(duckDbScalar("SELECT sum(doubled) FROM " + table)).isEqualTo("10100");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertIntoTableCreatedByDuckDb()
            throws SQLException
    {
        String table = "duckdb_made_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, name VARCHAR)".formatted(table),
                "INSERT INTO %s VALUES (1, 'from duckdb')".formatted(table));
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (2, 'from trino')", 1);
            assertQuery("SELECT name FROM " + table + " ORDER BY id", "VALUES ('from duckdb'), ('from trino')");
            assertThat(duckDbRows("SELECT name FROM " + table + " ORDER BY id"))
                    .isEqualTo(List.of("from duckdb", "from trino"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertIntoUnsignedColumnsDuckDbCreated()
            throws SQLException
    {
        String table = "unsigned_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT, h HUGEINT)".formatted(table),
                "INSERT INTO %s VALUES (200, 60000, 4000000000, 18446744073709551615, 123456789)".formatted(table));
        try {
            // Trino reads each unsigned type as the next type wide enough to hold it, and writes
            // the values back into the narrower one the file stores
            assertUpdate(
                    """
                    INSERT INTO %s VALUES (
                        SMALLINT '255', 65535, BIGINT '4294967295',
                        DECIMAL '18446744073709551615', DECIMAL '987654321')""".formatted(table),
                    1);

            assertThat(query("SELECT u8, u16, u32, u64 FROM " + table + " ORDER BY u8"))
                    .matches(
                            """
                            VALUES
                                (SMALLINT '200', 60000, BIGINT '4000000000', DECIMAL '18446744073709551615'),
                                (SMALLINT '255', 65535, BIGINT '4294967295', DECIMAL '18446744073709551615')""");
            assertThat(duckDbRows("SELECT u8::VARCHAR, u16::VARCHAR, u32::VARCHAR, u64::VARCHAR, h::VARCHAR FROM " + table + " ORDER BY u8"))
                    .isEqualTo(List.of(
                            "200",
                            "60000",
                            "4000000000",
                            "18446744073709551615",
                            "123456789",
                            "255",
                            "65535",
                            "4294967295",
                            "18446744073709551615",
                            "987654321"));

            // a value the unsigned column cannot hold is rejected rather than wrapped around
            assertThatThrownBy(() -> assertUpdate(
                    "INSERT INTO %s VALUES (SMALLINT '-1', 1, BIGINT '1', DECIMAL '1', DECIMAL '1')".formatted(table), 1))
                    .hasMessageContaining("Value out of range for a DuckLake uint8 column: -1");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testDeleteLeavesTheRemainingRowsReadableByBothEngines()
    {
        String table = "deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id FROM UNNEST(sequence(1, 10)) AS t(id)", 10);
        try {
            assertUpdate("DELETE FROM " + table + " WHERE id IN (2, 5)", 2);
            assertQuery("SELECT count(*) FROM " + table, "VALUES 8");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("8");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table + " WHERE id IN (2, 5)")).isEqualTo("0");

            // a second delete has to keep the rows the first one removed, since only one delete
            // file may apply to a data file at a time
            assertUpdate("DELETE FROM " + table + " WHERE id = 7", 1);
            assertQuery("SELECT count(*) FROM " + table, "VALUES 7");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table + " WHERE id IN (2, 5, 7)")).isEqualTo("0");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testDeletingEveryRowDropsTheFilesInsteadOfMarkingThem()
    {
        String table = "delete_all_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id FROM UNNEST(sequence(1, 5)) AS t(id)", 5);
        try {
            assertUpdate("DELETE FROM " + table, 5);
            assertQuery("SELECT count(*) FROM " + table, "VALUES 0");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("0");
            assertThat(duckDbScalar("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file d "
                    + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) "
                    + "WHERE t.table_name = '" + table + "' AND d.end_snapshot IS NULL"))
                    .isEqualTo("0");

            // the table still accepts rows afterwards, numbered above the ones that were removed
            assertUpdate("INSERT INTO " + table + " VALUES 42", 1);
            assertQuery("SELECT * FROM " + table, "VALUES 42");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("1");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testUpdateRewritesRowsInPlaceForReaders()
    {
        String table = "updates_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id, 'v' || CAST(id AS VARCHAR) AS name FROM UNNEST(sequence(1, 5)) AS t(id)", 5);
        try {
            assertUpdate("UPDATE " + table + " SET name = 'changed' WHERE id = 3", 1);
            assertQuery("SELECT name FROM " + table + " WHERE id = 3", "VALUES 'changed'");
            assertQuery("SELECT count(*) FROM " + table, "VALUES 5");
            assertThat(duckDbScalar("SELECT name FROM " + table + " WHERE id = 3")).isEqualTo("changed");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("5");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testMergeAppliesEveryClause()
    {
        String table = "merge_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, v VARCHAR)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate(
                    """
                    MERGE INTO %s t USING (VALUES (2, 'B'), (4, 'D')) AS s(id, v) ON t.id = s.id
                        WHEN MATCHED THEN UPDATE SET v = s.v
                        WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)""".formatted(table),
                    2);
            assertQuery("SELECT id, v FROM " + table + " ORDER BY id", "VALUES (1, 'a'), (2, 'B'), (3, 'c'), (4, 'D')");

            assertUpdate("MERGE INTO %s t USING (VALUES 1) AS s(id) ON t.id = s.id WHEN MATCHED THEN DELETE".formatted(table), 1);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 2, 3, 4");
            assertThat(duckDbRows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("2", "3", "4"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testPartitionedWritesUseTheSameLayoutAsDuckDb()
    {
        String table = "partitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, region VARCHAR, ts TIMESTAMP(6)) WITH (partitioning = ARRAY['region', 'year(ts)', 'month(ts)'])");
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, 'us', TIMESTAMP '2024-01-15 00:00:00'),
                        (2, 'eu', TIMESTAMP '2024-02-20 00:00:00'),
                        (3, 'us', TIMESTAMP '2024-01-16 00:00:00'),
                        (4, NULL, NULL)""".formatted(table),
                    4);

            assertQuery("SELECT count(*) FROM " + table, "VALUES 4");
            assertQuery("SELECT id FROM " + table + " WHERE region = 'us' ORDER BY id", "VALUES 1, 3");
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("4");

            // rows of one partition share a file, and the catalog records the value each holds
            assertThat(duckDbScalar("SELECT count(DISTINCT v.data_file_id) FROM __ducklake_metadata_lake.ducklake_file_partition_value v "
                    + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '" + table + "'"))
                    .isEqualTo("3");
            assertThat(duckDbRows(
                    """
                    SELECT partition_value FROM __ducklake_metadata_lake.ducklake_file_partition_value v
                    JOIN __ducklake_metadata_lake.ducklake_data_file f USING (data_file_id)
                    JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = f.table_id
                    WHERE t.table_name = '%s' AND f.path LIKE 'region=us%%'
                    ORDER BY partition_key_index""".formatted(table)))
                    .isEqualTo(List.of("us", "2024", "1"));

            // the property describes the table well enough to recreate it
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table))
                    .contains("partitioning = ARRAY['region','year(ts)','month(ts)']");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testDeletingWholePartitionsDropsTheirFiles()
    {
        String table = "partition_delete_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region'])");
        try {
            assertUpdate("INSERT INTO %s VALUES (1, 'us'), (2, 'eu'), (3, 'us'), (4, 'ap'), (5, NULL)".formatted(table), 5);

            // a predicate the partitioning decides for whole files needs no delete file at all
            assertUpdate("DELETE FROM " + table + " WHERE region = 'us'", 2);
            assertUpdate("DELETE FROM " + table + " WHERE region IS NULL", 1);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 2, 4");
            assertThat(duckDbScalar("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file d "
                    + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '" + table + "'"))
                    .isEqualTo("0");
            assertThat(duckDbRows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("2", "4"));

            // a predicate that reaches beyond the partitioning is answered row by row instead
            assertUpdate("DELETE FROM " + table + " WHERE region = 'eu' AND id > 99", 0);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 2, 4");
            assertUpdate("DELETE FROM " + table + " WHERE region = 'eu' AND id = 2", 1);
            assertQuery("SELECT id FROM " + table, "VALUES 4");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testAddedColumnReadsAsNullInOlderFiles()
    {
        String table = "added_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES 1", 1);
            assertUpdate("ALTER TABLE " + table + " ADD COLUMN b VARCHAR COMMENT 'added later'");
            assertUpdate("INSERT INTO " + table + " VALUES (2, 'two')", 1);

            assertQuery("SELECT a, b FROM " + table + " ORDER BY a", "VALUES (1, NULL), (2, 'two')");
            assertThat(duckDbRows("SELECT coalesce(b, '<null>') FROM " + table + " ORDER BY a")).isEqualTo(List.of("<null>", "two"));
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table)).contains("COMMENT 'added later'");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testWidenedColumnTypeAppliesToOlderFiles()
    {
        String table = "widened_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (i INTEGER, r REAL)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, REAL '1.5')", 1);
            assertUpdate("ALTER TABLE " + table + " ALTER COLUMN i SET DATA TYPE BIGINT");
            assertUpdate("ALTER TABLE " + table + " ALTER COLUMN r SET DATA TYPE DOUBLE");

            assertThat(query("SELECT i, r FROM " + table)).matches("VALUES (BIGINT '1', DOUBLE '1.5')");
            assertThat(duckDbRows("SELECT i::VARCHAR, r::VARCHAR FROM " + table)).isEqualTo(List.of("1", "1.5"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testNarrowingColumnTypeIsRejected()
    {
        String table = "narrowed_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a BIGINT)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES 1", 1);
            // the data file keeps the type it was written with, so the values would not read back
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table + " ALTER COLUMN a SET DATA TYPE INTEGER"))
                    .hasMessageContaining("Cannot change the type of column");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRenamedColumnStillReadsRowsWrittenUnderTheOldName()
            throws SQLException
    {
        String table = "renamed_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER, b VARCHAR)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'before')", 1);
            assertUpdate("ALTER TABLE " + table + " RENAME COLUMN b TO b2");

            // the file still stores the column as 'b', and is read by its identifier
            assertQuery("SELECT b2 FROM " + table, "VALUES 'before'");

            // rows written after the rename sit in a file that stores it as 'b2', so a query
            // spanning both files reads one column out of two files that disagree on its name
            assertUpdate("INSERT INTO " + table + " VALUES (2, 'after')", 1);
            assertQuery("SELECT b2 FROM " + table + " ORDER BY a", "VALUES 'before', 'after'");
            assertQuery("SELECT a FROM " + table + " WHERE b2 = 'before'", "VALUES 1");

            // and DuckDB, which resolves the same way, agrees
            assertThat(duckDbRows("SELECT b2 FROM " + table + " ORDER BY a")).isEqualTo(List.of("before", "after"));

            // renaming twice keeps working, and the original file is now two names behind
            assertUpdate("ALTER TABLE " + table + " RENAME COLUMN b2 TO b3");
            assertQuery("SELECT b3 FROM " + table + " ORDER BY a", "VALUES 'before', 'after'");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testANewColumnMayTakeTheNameARenamedColumnGaveUp()
    {
        String table = "reused_name_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER, x VARCHAR)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'first')", 1);
            // the file written above stores the column now called x_old under the name x, which
            // the column added next takes for itself
            assertUpdate("ALTER TABLE " + table + " RENAME COLUMN x TO x_old");
            assertUpdate("ALTER TABLE " + table + " ADD COLUMN x VARCHAR");
            assertUpdate("INSERT INTO " + table + " VALUES (2, 'second', 'brand new')", 1);

            // the first row predates the new column, so it holds nothing for it
            assertQuery("SELECT a, x_old, x FROM " + table + " ORDER BY a",
                    "VALUES (1, 'first', NULL), (2, 'second', 'brand new')");
            assertQuery("SELECT count(*) FROM " + table + " WHERE x IS NULL", "VALUES 1");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testColumnRenamedByDuckDbIsReadByTrino()
            throws SQLException
    {
        String table = "duckdb_renamed_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER, b VARCHAR)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'x')", 1);
            catalog.executeInDuckDb("ALTER TABLE %s RENAME COLUMN b TO renamed".formatted(table));

            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = '" + table + "' ORDER BY ordinal_position",
                    "VALUES 'a', 'renamed'");
            assertQuery("SELECT renamed FROM " + table, "VALUES 'x'");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testDroppedColumnDisappearsFromBothEngines()
    {
        String table = "dropped_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER, b VARCHAR)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES (1, 'x')", 1);
            assertUpdate("ALTER TABLE " + table + " DROP COLUMN b");
            assertQuery("SELECT * FROM " + table, "VALUES 1");
            assertThat(duckDbRows("SELECT column_name FROM duckdb_columns() WHERE table_name = '" + table + "'"))
                    .isEqualTo(List.of("a"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testViewLifecycle()
    {
        String table = "view_over_" + randomNameSuffix();
        String view = "view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id, id % 2 AS parity FROM UNNEST(sequence(1, 6)) AS t(id)", 6);
        try {
            assertUpdate("CREATE VIEW %s AS SELECT id FROM %s WHERE parity = 0".formatted(view, table));
            assertQuery("SELECT id FROM " + view + " ORDER BY id", "VALUES 2, 4, 6");
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(view);
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view)).contains("CREATE VIEW", table);

            // replacing a view swaps what it selects without changing its name
            assertUpdate("CREATE OR REPLACE VIEW %s AS SELECT id FROM %s WHERE parity = 1".formatted(view, table));
            assertQuery("SELECT id FROM " + view + " ORDER BY id", "VALUES 1, 3, 5");

            assertUpdate("COMMENT ON VIEW %s IS 'odd ids'".formatted(view));
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view)).contains("odd ids");

            String renamed = "view_renamed_" + randomNameSuffix();
            assertUpdate("ALTER VIEW %s RENAME TO %s".formatted(view, renamed));
            assertQuery("SELECT count(*) FROM " + renamed, "VALUES 3");
            assertUpdate("DROP VIEW " + renamed);
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).doesNotContain(view, renamed);
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testViewIsStoredAsADuckLakeViewAndLeavesDuckDbWorking()
            throws SQLException
    {
        String table = "view_source_" + randomNameSuffix();
        String view = "stored_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 AS a, 'x' AS b", 1);
        try {
            assertUpdate("CREATE VIEW %s AS SELECT a AS renamed FROM %s".formatted(view, table));

            // the row is a DuckLake view like any other, naming the dialect its query is written in
            assertThat(duckDbRows("SELECT dialect, column_aliases FROM __ducklake_metadata_lake.ducklake_view "
                    + "WHERE view_name = '" + view + "' AND end_snapshot IS NULL"))
                    .isEqualTo(List.of("trino", "\"renamed\""));

            // DuckDB keeps working alongside it: it reads the tables and can still write to them
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("1");
            catalog.executeInDuckDb("INSERT INTO %s VALUES (2, 'y')".formatted(table));
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("2");
            assertQuery("SELECT renamed FROM " + view + " ORDER BY renamed", "VALUES 1, 2");
        }
        finally {
            assertUpdate("DROP VIEW " + view);
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testAViewAndATableCannotShareAName()
    {
        String name = "clash_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + name + " (a INTEGER)");
        try {
            assertThatThrownBy(() -> assertUpdate("CREATE VIEW %s AS SELECT 1 AS a".formatted(name)))
                    .hasMessageContaining("already exists");
        }
        finally {
            assertUpdate("DROP TABLE " + name);
        }

        String viewName = "clash_view_" + randomNameSuffix();
        assertUpdate("CREATE VIEW %s AS SELECT 1 AS a".formatted(viewName));
        try {
            assertThatThrownBy(() -> assertUpdate("CREATE TABLE %s (a INTEGER)".formatted(viewName)))
                    .hasMessageContaining("already exists");
        }
        finally {
            assertUpdate("DROP VIEW " + viewName);
        }
    }

    @Test
    void testSchemaLifecycle()
    {
        String schema = "schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schema);
        try {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schema);
            assertThat(duckDbRows("SELECT schema_name FROM duckdb_schemas() WHERE schema_name = '" + schema + "'"))
                    .isEqualTo(List.of(schema));

            assertUpdate("CREATE TABLE %s.t (x INTEGER)".formatted(schema));
            assertThatThrownBy(() -> assertUpdate("DROP SCHEMA " + schema))
                    .hasMessageContaining("Cannot drop non-empty schema");
            assertUpdate("DROP TABLE %s.t".formatted(schema));
        }
        finally {
            assertUpdate("DROP SCHEMA " + schema);
        }
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schema);
    }

    @Test
    void testCommentsAreVisibleToBothEngines()
    {
        String table = "comments_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (a INTEGER)");
        try {
            assertUpdate("COMMENT ON TABLE " + table + " IS 'the table'");
            assertUpdate("COMMENT ON COLUMN %s.a IS 'the column'".formatted(table));

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table)).contains("COMMENT 'the table'", "COMMENT 'the column'");
            assertThat(duckDbScalar("SELECT comment FROM duckdb_tables() WHERE table_name = '" + table + "'")).isEqualTo("the table");

            assertUpdate("COMMENT ON TABLE " + table + " IS NULL");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table)).doesNotContain("COMMENT 'the table'");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRenameTableKeepsItsRows()
    {
        String table = "rename_from_" + randomNameSuffix();
        String renamed = "rename_to_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 AS a", 1);
        assertUpdate("ALTER TABLE %s RENAME TO %s".formatted(table, renamed));
        try {
            assertQuery("SELECT a FROM " + renamed, "VALUES 1");
            assertThat(duckDbScalar("SELECT a FROM " + renamed)).isEqualTo("1");
        }
        finally {
            assertUpdate("DROP TABLE " + renamed);
        }
    }

    @Test
    void testStatisticsCoverTheRowsThatWereWritten()
    {
        String table = "stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT id, CAST(NULL AS VARCHAR) AS empty FROM UNNEST(sequence(1, 10)) AS t(id)", 10);
        try {
            // the bounds recorded for the column let a predicate outside them skip the file
            assertQuery("SELECT count(*) FROM " + table + " WHERE id > 100", "VALUES 0");
            assertThat(query("SELECT min(id), max(id) FROM " + table)).matches("VALUES (BIGINT '1', BIGINT '10')");
            assertThat(duckDbScalar("SELECT min_value FROM __ducklake_metadata_lake.ducklake_table_column_stats s "
                    + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '" + table + "' AND s.column_id = 1"))
                    .isEqualTo("1");
            assertThat(duckDbScalar("SELECT max_value FROM __ducklake_metadata_lake.ducklake_table_column_stats s "
                    + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '" + table + "' AND s.column_id = 1"))
                    .isEqualTo("10");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testTargetFileSizeRollsWritesOverIntoSeveralFiles()
    {
        String table = "rolled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, padding VARCHAR)");
        try {
            assertUpdate(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("ducklake", "target_max_file_size", "16kB")
                            .build(),
                    "INSERT INTO " + table + " SELECT id, lpad('', 512, 'x') FROM UNNEST(sequence(1, 2000)) AS t(id)",
                    2000);
            assertQuery("SELECT count(*) FROM " + table, "VALUES 2000");
            assertThat(Long.parseLong(duckDbScalar(
                    "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file f "
                            + "JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) "
                            + "WHERE t.table_name = '" + table + "' AND f.end_snapshot IS NULL")))
                    .isGreaterThan(1);
            assertThat(duckDbScalar("SELECT count(*) FROM " + table)).isEqualTo("2000");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    private String duckDbScalar(@Language("SQL") String sql)
    {
        List<String> row = duckDbRows(sql);
        assertThat(row).hasSize(1);
        return row.getFirst();
    }

    /**
     * The single row the query returns, as strings. Reading everything as text keeps the
     * assertions about what DuckDB sees independent of how JDBC maps each type.
     */
    private List<String> duckDbRows(@Language("SQL") String sql)
    {
        try (Connection connection = catalog.openDuckDbConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            ImmutableList.Builder<String> values = ImmutableList.builder();
            while (resultSet.next()) {
                for (int column = 1; column <= metaData.getColumnCount(); column++) {
                    String value = resultSet.getString(column);
                    values.add(value == null ? "<null>" : value);
                }
            }
            return values.build();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to run in DuckDB: " + sql, e);
        }
    }
}
