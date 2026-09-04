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
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Tests the writes a pipeline makes into tables it does not own: DuckDB creates the table and goes
 * on reading it, Trino writes into it, and both engines have to agree about what is there
 * afterwards.
 * <p>
 * {@link TestDuckLakeWrites} covers the tables Trino itself creates. Here every table is created
 * through DuckDB first, because a table another engine defined carries partitioning, types and
 * views this connector has to accept rather than choose.
 */
final class TestDuckLakeWriteRoundTrips
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
    void testInsertIntoTableDuckDbPartitionedByAColumn()
            throws SQLException
    {
        String table = "part_column_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, region VARCHAR, amount BIGINT)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (region)".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, 'us', 10), (2, 'us', 20),
                        (3, 'eu', 30),
                        (4, 'ap', 40), (5, 'ap', 50)""".formatted(table),
                    5);

            // one file per partition, each recorded under the value its rows hold, rather than
            // one file holding all three
            assertThat(activeDataFileCount(table)).isEqualTo(3);
            assertThat(partitions(table)).isEqualTo(List.of("ap", "eu", "us"));
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 1, 2, 3, 4, 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("1", "2", "3", "4", "5"));

            // a predicate the partitioning decides for whole files ends them without reading a row
            assertQueryStats(
                    getSession(),
                    "DELETE FROM %s WHERE region IN ('us', 'eu')".formatted(table),
                    stats -> assertThat(stats.getProcessedInputPositions()).isEqualTo(0),
                    _ -> {});
            assertThat(activeDataFileCount(table)).isEqualTo(1);
            assertThat(activeDeleteFileCount(table)).isEqualTo(0);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 4, 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("4", "5"));

            // a predicate that reaches beyond the partitioning removes rows one by one, which
            // leaves the file in place and records the positions it dropped in a delete file
            assertUpdate("DELETE FROM %s WHERE region = 'ap' AND id = 4".formatted(table), 1);
            assertThat(activeDataFileCount(table)).isEqualTo(1);
            assertThat(activeDeleteFileCount(table)).isEqualTo(1);
            assertQuery("SELECT id FROM " + table, "VALUES 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table)).isEqualTo(List.of("5"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * The shape a daily pipeline writes: DuckDB files the table by a {@code DATE} column and the
     * pipeline replaces whole days.
     * <p>
     * The days are dropped from the catalog the same way an equivalent {@code VARCHAR} or
     * {@code INTEGER} key is: the partition value of each file decides the delete, so the files
     * end without a row of them being read.
     */
    @Test
    void testDeletingWholeDaysOfATableDuckDbPartitionedByDateEndsTheFiles()
            throws SQLException
    {
        String table = "part_day_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, day DATE, v VARCHAR)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (day)".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, DATE '2026-01-01', 'a'), (2, DATE '2026-01-01', 'b'),
                        (3, DATE '2026-01-02', 'c'),
                        (4, DATE '2026-01-03', 'd'), (5, DATE '2026-01-03', 'e')""".formatted(table),
                    5);

            assertThat(activeDataFileCount(table)).isEqualTo(3);
            assertThat(partitions(table)).isEqualTo(List.of("2026-01-01", "2026-01-02", "2026-01-03"));

            // the two days are ended by the partition values alone, without reading a row
            assertQueryStats(
                    getSession(),
                    "DELETE FROM %s WHERE day BETWEEN DATE '2026-01-01' AND DATE '2026-01-02'".formatted(table),
                    stats -> assertThat(stats.getProcessedInputPositions()).isEqualTo(0),
                    _ -> {});
            assertThat(activeDataFileCount(table)).isEqualTo(1);
            assertThat(activeDeleteFileCount(table)).isEqualTo(0);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 4, 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("4", "5"));

            // a predicate that reaches beyond the partitioning removes rows one by one, which
            // leaves the file in place behind a delete file
            assertUpdate("DELETE FROM %s WHERE day = DATE '2026-01-03' AND id = 4".formatted(table), 1);
            assertThat(activeDataFileCount(table)).isEqualTo(1);
            assertThat(activeDeleteFileCount(table)).isEqualTo(1);
            assertQuery("SELECT id FROM " + table, "VALUES 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table)).isEqualTo(List.of("5"));

            // the day the pipeline reloads is written back as its own file
            assertUpdate("INSERT INTO %s VALUES (6, DATE '2026-01-01', 'a2')".formatted(table), 1);
            assertThat(partitions(table)).isEqualTo(List.of("2026-01-01", "2026-01-03"));
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("5", "6"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * The same daily shape as {@link #testDeletingWholeDaysOfATableDuckDbPartitionedByDateEndsTheFiles},
     * with the day held as a {@code TIMESTAMPTZ} truncated to midnight rather than as a
     * {@code DATE}. That is the column a pipeline materializing
     * {@code DATE_TRUNC('day', ts)::TIMESTAMPTZ AS day} files its tables by.
     * <p>
     * It costs the same: nothing. The value is recorded in UTC and read back as the instant it
     * names, so the delete is decided from the catalog for this type too.
     */
    @Test
    void testDeletingWholeDaysOfATableDuckDbPartitionedByTimestampWithTimeZoneEndsTheFiles()
            throws SQLException
    {
        String table = "part_tstz_day_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, day TIMESTAMPTZ, v VARCHAR)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (day)".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, TIMESTAMP '2026-01-01 00:00:00 UTC', 'a'), (2, TIMESTAMP '2026-01-01 00:00:00 UTC', 'b'),
                        (3, TIMESTAMP '2026-01-02 00:00:00 UTC', 'c'),
                        (4, TIMESTAMP '2026-01-03 00:00:00 UTC', 'd'), (5, TIMESTAMP '2026-01-03 00:00:00 UTC', 'e')""".formatted(table),
                    5);

            // an identity partition key of this type is written, one file per day, and the value
            // is recorded in UTC whatever zone the row carried
            assertThat(activeDataFileCount(table)).isEqualTo(3);
            assertThat(partitions(table)).isEqualTo(List.of(
                    "2026-01-01 00:00:00+00",
                    "2026-01-02 00:00:00+00",
                    "2026-01-03 00:00:00+00"));

            // the partition values answer the delete, so the two days end without a row being read
            assertQueryStats(
                    getSession(),
                    "DELETE FROM %s WHERE day BETWEEN TIMESTAMP '2026-01-01 00:00:00 UTC' AND TIMESTAMP '2026-01-02 00:00:00 UTC'".formatted(table),
                    stats -> assertThat(stats.getProcessedInputPositions()).isEqualTo(0),
                    _ -> {});
            assertThat(activeDataFileCount(table)).isEqualTo(1);
            assertThat(activeDeleteFileCount(table)).isEqualTo(0);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 4, 5");
            assertThat(catalog.rows("SELECT id::VARCHAR, day::VARCHAR FROM " + table + " ORDER BY id"))
                    .isEqualTo(List.of("4", "2026-01-03 00:00:00+00", "5", "2026-01-03 00:00:00+00"));

            // DuckDB files a row of its own under the value Trino recorded for that day
            catalog.executeInDuckDb("INSERT INTO %s VALUES (6, TIMESTAMPTZ '2026-01-03 00:00:00+00', 'f')".formatted(table));
            assertThat(partitions(table)).isEqualTo(List.of("2026-01-03 00:00:00+00", "2026-01-03 00:00:00+00"));
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 4, 5, 6");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * A day partition holding {@code DATE 'infinity'}, which DuckDB records as the string
     * {@code infinity} and no date parser reads back.
     * <p>
     * The value is not guessed at in either direction: the file is never pruned, and it is never
     * counted as matching. A predicate the connector cannot decide for one file is one it cannot
     * enforce for the table, so the delete is answered row by row and the engine keeps filtering.
     * The days it covers still end, because nothing is left in them, and the infinite day is left
     * where it is.
     */
    @Test
    void testDeletingWholeDaysBesideAnInfiniteDateLeavesItAlone()
            throws SQLException
    {
        String table = "part_inf_day_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, day DATE, v VARCHAR)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (day)".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, DATE '2026-01-01', 'a'), (2, DATE '2026-01-01', 'b'),
                        (3, DATE '2026-01-02', 'c'),
                        (4, DATE '2026-01-03', 'd'), (5, DATE '2026-01-03', 'e')""".formatted(table),
                    5);
            catalog.executeInDuckDb("INSERT INTO %s VALUES (9, DATE 'infinity', 'inf')".formatted(table));
            assertThat(partitions(table)).isEqualTo(List.of("2026-01-01", "2026-01-02", "2026-01-03", "infinity"));

            // the three rows of the two days are read to place the delete. The infinite day is
            // read too, and its file gives up no row, because the predicate excludes the value
            // its Parquet statistics report even though its partition value says nothing
            assertQueryStats(
                    getSession(),
                    "DELETE FROM %s WHERE day BETWEEN DATE '2026-01-01' AND DATE '2026-01-02'".formatted(table),
                    stats -> assertThat(stats.getProcessedInputPositions()).isEqualTo(3),
                    _ -> {});

            // exactly the two days are gone, and no delete file was written for either
            assertThat(activeDataFileCount(table)).isEqualTo(2);
            assertThat(activeDeleteFileCount(table)).isEqualTo(0);
            assertThat(partitions(table)).isEqualTo(List.of("2026-01-03", "infinity"));

            // and the infinite day still reads as the row DuckDB wrote, through both engines
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 4, 5, 9");
            assertThat(catalog.rows("SELECT day::VARCHAR FROM %s WHERE id = 9".formatted(table))).isEqualTo(List.of("infinity"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * A read of the same table: the days the connector can read back are pruned by their partition
     * value, and the one it cannot is read and filtered rather than dropped.
     */
    @Test
    void testReadingADayBesideAnInfiniteDatePrunesTheOtherDays()
            throws SQLException
    {
        String table = "read_inf_day_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, day DATE, v VARCHAR)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (day)".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, DATE '2026-01-01', 'a'), (2, DATE '2026-01-01', 'b'),
                        (3, DATE '2026-01-02', 'c'),
                        (4, DATE '2026-01-03', 'd'), (5, DATE '2026-01-03', 'e')""".formatted(table),
                    5);
            catalog.executeInDuckDb("INSERT INTO %s VALUES (9, DATE 'infinity', 'inf')".formatted(table));

            // With the file statistics of the catalog and of the reader both switched off, the
            // partition values are the only thing left to prune by. The two days that cannot
            // match are not opened; the day asked for and the day whose value does not parse are
            // read, and the engine drops the row of the second.
            Session partitionPruningOnly = Session.builder(getSession())
                    .setCatalogSessionProperty("ducklake", "file_statistics_pruning_enabled", "false")
                    .setCatalogSessionProperty("ducklake", "parquet_ignore_statistics", "true")
                    .build();
            assertQueryStats(
                    partitionPruningOnly,
                    "SELECT id FROM %s WHERE day = DATE '2026-01-02'".formatted(table),
                    stats -> assertThat(stats.getProcessedInputPositions()).isEqualTo(2),
                    result -> assertThat(result.getOnlyColumnAsSet()).isEqualTo(Set.of(3)));

            // the row of the infinite day is not lost: a predicate that covers it returns it
            assertQuery("SELECT id FROM %s WHERE day > DATE '2026-01-02' ORDER BY id".formatted(table), "VALUES 4, 5, 9");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertIntoTableDuckDbPartitionedByTemporalTransforms()
            throws SQLException
    {
        String table = "part_transform_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, ts TIMESTAMP)".formatted(table),
                "ALTER TABLE %s SET PARTITIONED BY (year(ts), day(ts))".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, TIMESTAMP '2026-01-01 00:30:00'), (2, TIMESTAMP '2026-01-01 12:30:00'),
                        (3, TIMESTAMP '2026-01-02 00:30:00'),
                        (4, TIMESTAMP '2026-01-03 00:30:00')""".formatted(table),
                    4);

            // the transforms are computed by the connector, not by the value itself, and the
            // components DuckDB uses are recorded: the year, then the day of the month
            assertThat(activeDataFileCount(table)).isEqualTo(3);
            assertThat(partitions(table)).isEqualTo(List.of("2026/1", "2026/2", "2026/3"));
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 1, 2, 3, 4");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("1", "2", "3", "4"));

            // DuckDB files a row it writes itself under the same partition values, so a predicate
            // on the partitioned column reads the rows of both engines together
            catalog.executeInDuckDb("INSERT INTO %s VALUES (5, TIMESTAMP '2026-01-02 06:00:00')".formatted(table));
            assertThat(partitions(table)).isEqualTo(List.of("2026/1", "2026/2", "2026/2", "2026/3"));
            assertQuery("SELECT id FROM " + table + " WHERE ts >= TIMESTAMP '2026-01-02 00:00:00' AND ts < TIMESTAMP '2026-01-03 00:00:00' ORDER BY id", "VALUES 3, 5");

            // a transform is not a value the connector can enforce a predicate on, so a delete
            // covering whole partitions is still answered row by row
            assertUpdate("DELETE FROM %s WHERE ts < TIMESTAMP '2026-01-02 00:00:00'".formatted(table), 2);
            assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 3, 4, 5");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).isEqualTo(List.of("3", "4", "5"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testTrinoWritesEveryTypeADuckDbTableDeclares()
            throws SQLException
    {
        String table = "types_" + randomNameSuffix();
        catalog.executeInDuckDb(
                """
                CREATE TABLE %s (
                    id INTEGER,
                    props STRUCT(type VARCHAR, tier VARCHAR, frequency VARCHAR, provider VARCHAR),
                    context STRUCT("$lib" VARCHAR, "$session_id" VARCHAR),
                    c_hugeint HUGEINT,
                    c_ubigint UBIGINT,
                    c_json JSON,
                    c_timestamptz TIMESTAMPTZ,
                    c_decimal DECIMAL(18,3),
                    c_uuid UUID,
                    c_list VARCHAR[],
                    c_map MAP(VARCHAR, INTEGER))""".formatted(table));
        try {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES (
                        1,
                        CAST(ROW('pageview', 'gold', 'daily', 'stripe') AS ROW(type VARCHAR, tier VARCHAR, frequency VARCHAR, provider VARCHAR)),
                        CAST(ROW('web', 'sess-1') AS ROW("$lib" VARCHAR, "$session_id" VARCHAR)),
                        DECIMAL '123456789',
                        DECIMAL '18446744073709551615',
                        '{"a": 1}',
                        TIMESTAMP '2026-01-01 12:34:56.123456 UTC',
                        DECIMAL '123456789012345.678',
                        UUID '11111111-2222-3333-4444-555555555555',
                        ARRAY['a', 'b'],
                        MAP(ARRAY['k1', 'k2'], ARRAY[10, 20]))""".formatted(table),
                    1);

            // DuckDB reads the file back into the types the table declares, not into whatever the
            // Parquet columns happen to be
            assertThat(catalog.rows(
                    """
                    SELECT typeof(props), typeof(context), typeof(c_hugeint), typeof(c_ubigint), typeof(c_json),
                        typeof(c_timestamptz), typeof(c_decimal), typeof(c_uuid), typeof(c_list), typeof(c_map)
                    FROM %s WHERE id = 1""".formatted(table)))
                    .isEqualTo(List.of(
                            "STRUCT(\"type\" VARCHAR, tier VARCHAR, frequency VARCHAR, provider VARCHAR)",
                            "STRUCT(\"$lib\" VARCHAR, \"$session_id\" VARCHAR)",
                            "HUGEINT",
                            "UBIGINT",
                            "JSON",
                            "TIMESTAMP WITH TIME ZONE",
                            "DECIMAL(18,3)",
                            "UUID",
                            "VARCHAR[]",
                            "MAP(VARCHAR, INTEGER)"));
            assertThat(catalog.rows(
                    """
                    SELECT props::VARCHAR, context::VARCHAR, c_hugeint::VARCHAR, c_ubigint::VARCHAR, c_json::VARCHAR,
                        c_timestamptz::VARCHAR, c_decimal::VARCHAR, c_uuid::VARCHAR, c_list::VARCHAR, c_map::VARCHAR
                    FROM %s WHERE id = 1""".formatted(table)))
                    .isEqualTo(List.of(
                            "{'type': pageview, 'tier': gold, 'frequency': daily, 'provider': stripe}",
                            "{'$lib': web, '$session_id': sess-1}",
                            "123456789",
                            "18446744073709551615",
                            "{\"a\": 1}",
                            "2026-01-01 12:34:56.123456+00",
                            "123456789012345.678",
                            "11111111-2222-3333-4444-555555555555",
                            "[a, b]",
                            "{k1=10, k2=20}"));

            // and the row DuckDB writes reads back through Trino as the same values
            catalog.executeInDuckDb(
                    """
                    INSERT INTO %s VALUES (
                        2,
                        {'type': 'click', 'tier': 'free', 'frequency': 'weekly', 'provider': 'apple'},
                        {'$lib': 'ios', '$session_id': 'sess-2'},
                        987654321::HUGEINT,
                        18446744073709551614::UBIGINT,
                        '{"b": 2}'::JSON,
                        TIMESTAMPTZ '2026-02-02 01:02:03.654321+00',
                        (-123456789012345.678)::DECIMAL(18,3),
                        '22222222-3333-4444-5555-666666666666'::UUID,
                        ['x', 'y'],
                        MAP {'k3': 30})""".formatted(table));

            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position".formatted(table),
                    """
                    VALUES
                        ('id', 'integer'),
                        ('props', 'row("type" varchar, "tier" varchar, "frequency" varchar, "provider" varchar)'),
                        ('context', 'row("$lib" varchar, "$session_id" varchar)'),
                        ('c_hugeint', 'decimal(38,0)'),
                        ('c_ubigint', 'decimal(20,0)'),
                        ('c_json', 'varchar'),
                        ('c_timestamptz', 'timestamp(6) with time zone'),
                        ('c_decimal', 'decimal(18,3)'),
                        ('c_uuid', 'uuid'),
                        ('c_list', 'array(varchar)'),
                        ('c_map', 'map(varchar, integer)')""");

            assertThat(query(
                    """
                    SELECT props.type, props.provider, context."$lib", context."$session_id",
                        c_hugeint, c_ubigint, c_json, c_timestamptz, c_decimal, c_uuid, c_list, c_map['k1']
                    FROM %s WHERE id = 1""".formatted(table)))
                    .matches(
                            """
                            VALUES (
                                VARCHAR 'pageview', VARCHAR 'stripe', VARCHAR 'web', VARCHAR 'sess-1',
                                CAST(DECIMAL '123456789' AS DECIMAL(38, 0)),
                                CAST(DECIMAL '18446744073709551615' AS DECIMAL(20, 0)),
                                VARCHAR '{"a": 1}',
                                TIMESTAMP '2026-01-01 12:34:56.123456 UTC',
                                CAST(DECIMAL '123456789012345.678' AS DECIMAL(18, 3)),
                                UUID '11111111-2222-3333-4444-555555555555',
                                ARRAY[VARCHAR 'a', VARCHAR 'b'],
                                10)""");
            assertThat(query(
                    """
                    SELECT props.tier, context."$lib", c_hugeint, c_ubigint, c_json, c_timestamptz, c_decimal, c_uuid, c_list, c_map['k3']
                    FROM %s WHERE id = 2""".formatted(table)))
                    .matches(
                            """
                            VALUES (
                                VARCHAR 'free', VARCHAR 'ios',
                                CAST(DECIMAL '987654321' AS DECIMAL(38, 0)),
                                CAST(DECIMAL '18446744073709551614' AS DECIMAL(20, 0)),
                                VARCHAR '{"b": 2}',
                                TIMESTAMP '2026-02-02 01:02:03.654321 UTC',
                                CAST(DECIMAL '-123456789012345.678' AS DECIMAL(18, 3)),
                                UUID '22222222-3333-4444-5555-666666666666',
                                ARRAY[VARCHAR 'x', VARCHAR 'y'],
                                30)""");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testTrinoReplacesAViewDuckDbCreated()
            throws SQLException
    {
        String table = "view_source_" + randomNameSuffix();
        String view = "v1_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER, v VARCHAR)".formatted(table),
                "INSERT INTO %s VALUES (1, 'a'), (2, 'b')".formatted(table),
                "CREATE VIEW %s AS SELECT * FROM %s".formatted(view, table));
        try {
            assertThat(liveViewRow(view, "dialect")).isEqualTo(List.of("duckdb"));

            assertUpdate("CREATE OR REPLACE VIEW %s AS SELECT id FROM %s WHERE id = 2".formatted(view, table));

            // the connector does not edit the row DuckDB wrote: it ends it and inserts its own,
            // so the catalog keeps a history of both. The query is stored as Trino formats it,
            // not as it was typed
            assertThat(liveViewRow(view, "dialect, sql, column_aliases"))
                    .isEqualTo(List.of("trino", formattedViewQuery(table), "\"id\""));
            assertThat(catalog.scalar(
                    "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_view WHERE view_name = '%s' AND end_snapshot IS NOT NULL".formatted(view)))
                    .isEqualTo("1");
            assertQuery("SELECT id FROM " + view, "VALUES 2");

            // DuckDB still loads the catalog and reads the view, because the query the connector
            // stored is written the way it was typed and happens to parse in DuckDB too
            assertThat(catalog.rows("SELECT view_name FROM duckdb_views() WHERE view_name = '%s'".formatted(view))).isEqualTo(List.of(view));
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + view)).isEqualTo(List.of("2"));

            assertUpdate("DROP VIEW " + view);
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).doesNotContain(view);
            assertThat(catalog.rows("SELECT view_name FROM duckdb_views() WHERE view_name = '%s'".formatted(view))).isEmpty();
            assertThat(liveViewRow(view, "dialect")).isEmpty();
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * A view whose query names its tables through the Trino catalog, as a tool that generates SQL
     * against a specific catalog writes them.
     * <p>
     * The connector stores the query text as it was written, so the catalog name goes into the
     * DuckLake view along with it. Trino resolves that name and reads the view; DuckDB has no
     * database of that name and fails to bind it, while the rest of the catalog keeps working.
     */
    @Test
    void testAViewQualifiedByTheTrinoCatalogNameDoesNotBindInDuckDb()
            throws SQLException
    {
        String table = "qualified_source_" + randomNameSuffix();
        String view = "qualified_view_" + randomNameSuffix();
        catalog.executeInDuckDb(
                "CREATE TABLE %s (id INTEGER)".formatted(table),
                "INSERT INTO %s VALUES (1), (2)".formatted(table));
        try {
            assertUpdate("CREATE VIEW %s AS SELECT id FROM ducklake.main.%s WHERE id = 2".formatted(view, table));

            assertThat(liveViewRow(view, "sql")).isEqualTo(List.of(formattedViewQuery("ducklake.main." + table)));
            assertQuery("SELECT id FROM " + view, "VALUES 2");

            // DuckDB lists the view and keeps reading the tables beside it
            assertThat(catalog.rows("SELECT view_name FROM duckdb_views() WHERE view_name = '%s'".formatted(view))).isEqualTo(List.of(view));
            assertThat(catalog.scalar("SELECT count(*) FROM " + table)).isEqualTo("2");

            assertThat(duckDbFailure("SELECT * FROM " + view)).contains("Binder Error: Catalog \"ducklake\" does not exist!");
        }
        finally {
            assertUpdate("DROP VIEW " + view);
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertingEveryColumnAfterDuckDbAddsOne()
            throws SQLException
    {
        String table = "added_by_duckdb_" + randomNameSuffix();
        catalog.executeInDuckDb("CREATE TABLE %s (a INTEGER, b VARCHAR)".formatted(table));
        try {
            assertUpdate("INSERT INTO %s VALUES (1, 'one')".formatted(table), 1);
            catalog.executeInDuckDb("ALTER TABLE %s ADD COLUMN c INTEGER".formatted(table));

            // the column the other engine added is there to be written, and the rows written
            // before it hold nothing for it
            assertUpdate("INSERT INTO %s (a, b, c) VALUES (2, 'two', 22)".formatted(table), 1);
            assertQuery("SELECT a, b, c FROM " + table + " ORDER BY a", "VALUES (1, 'one', NULL), (2, 'two', 22)");
            assertThat(catalog.rows("SELECT a::VARCHAR, b, coalesce(c::VARCHAR, '<null>') FROM " + table + " ORDER BY a"))
                    .isEqualTo(List.of("1", "one", "<null>", "2", "two", "22"));
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * {@code SELECT id FROM <source> WHERE id = 2} as the connector stores it: the query text Trino
     * formats out of the parsed statement, rather than the text the statement was written with.
     */
    private static String formattedViewQuery(String source)
    {
        return "SELECT id\nFROM\n  %s\nWHERE (id = 2)\n".formatted(source);
    }

    /**
     * The message DuckDB fails the query with. Fails the test if the query succeeds.
     */
    private String duckDbFailure(@Language("SQL") String sql)
    {
        try {
            catalog.rows(sql);
        }
        catch (RuntimeException e) {
            return e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
        }
        throw new AssertionError("Expected DuckDB to fail: " + sql);
    }

    private long activeDataFileCount(String table)
    {
        return Long.parseLong(catalog.scalar(
                """
                SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file f
                JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL""".formatted(table)));
    }

    private long activeDeleteFileCount(String table)
    {
        return Long.parseLong(catalog.scalar(
                """
                SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file d
                JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)
                WHERE t.table_name = '%s' AND d.end_snapshot IS NULL""".formatted(table)));
    }

    /**
     * The partition each file a table currently has was written under, one entry per file, sorted.
     * A partitioning with several keys reads as its values in key order, separated by {@code /}.
     */
    private List<String> partitions(String table)
    {
        return catalog.rows(
                """
                SELECT string_agg(v.partition_value, '/' ORDER BY v.partition_key_index)
                FROM __ducklake_metadata_lake.ducklake_file_partition_value v
                JOIN __ducklake_metadata_lake.ducklake_data_file f USING (data_file_id)
                JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = f.table_id
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL
                GROUP BY f.data_file_id
                ORDER BY 1""".formatted(table));
    }

    private List<String> liveViewRow(String view, String columns)
    {
        return catalog.rows(
                "SELECT %s FROM __ducklake_metadata_lake.ducklake_view WHERE view_name = '%s' AND end_snapshot IS NULL".formatted(columns, view));
    }
}
