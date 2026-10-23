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
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
final class TestDuckLakeAddFiles
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
        return DuckLakeQueryRunner.builder(catalog)
                .addConnectorProperty("ducklake.add-files-procedure.enabled", "true")
                .build();
    }

    @Test
    void testRegistersExistingFilesWithoutRewritingThemAndReplaysOnce()
            throws Exception
    {
        String table = "import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT 1::INTEGER id, 'original'::VARCHAR payload) TO '%s' (FORMAT PARQUET)".formatted(file));
        byte[] original = Files.readAllBytes(Path.of(file));
        Set<Path> originalFiles;
        try (var paths = Files.walk(catalog.dataPath())) {
            originalFiles = paths.filter(Files::isRegularFile).collect(Collectors.toSet());
        }
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, payload VARCHAR)");
        try {
            String statement = addFiles(table, "first", false, location(file));
            assertUpdate(statement);
            assertQuery("SELECT * FROM " + table, "VALUES (1, 'original')");
            assertThat(catalog.rows("SELECT id::VARCHAR, payload FROM " + table)).isEqualTo(List.of("1", "original"));
            assertThat(Files.readAllBytes(Path.of(file))).isEqualTo(original);
            try (var paths = Files.walk(catalog.dataPath())) {
                assertThat(paths.filter(Files::isRegularFile).collect(Collectors.toSet())).isEqualTo(originalFiles);
            }
            String snapshot = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            assertUpdate(statement);
            assertThat(catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')")).isEqualTo(snapshot);
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(1 AS BIGINT)");
            assertQueryFails(addFiles(table, "second", false, location(file)), ".*already registered.*");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInvalidBatchPublishesNothing()
            throws SQLException
    {
        String table = "invalid_import_" + randomNameSuffix();
        String valid = catalog.externalParquetFile(table + "_valid");
        String invalid = catalog.externalParquetFile(table + "_invalid");
        catalog.executeInDuckDb(
                "COPY (SELECT 1::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(valid),
                "COPY (SELECT 'wrong'::VARCHAR id) TO '%s' (FORMAT PARQUET)".formatted(invalid));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            String snapshot = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            assertQueryFails(addFiles(table, "invalid", false, location(valid), location(invalid)), ".*type.*");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
            assertThat(catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')")).isEqualTo(snapshot);
            assertUpdate(addFiles(table, "valid", false, location(valid)));
            assertQuery("SELECT * FROM " + table, "VALUES 1");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testAtomicReplacementAndImportIdRejectsDifferentFiles()
            throws SQLException
    {
        String table = "replace_import_" + randomNameSuffix();
        String first = catalog.externalParquetFile(table + "_first");
        String second = catalog.externalParquetFile(table + "_second");
        catalog.executeInDuckDb(
                "COPY (SELECT 1::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(first),
                "COPY (SELECT 2::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(second));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            assertUpdate(addFiles(table, "first", false, location(first)));
            assertQueryFails(addFiles(table, "first", false, location(second)), ".*import_id.*different.*");
            assertUpdate(addFiles(table, "replace", true, location(second)));
            assertQuery("SELECT * FROM " + table, "VALUES 2");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table)).isEqualTo(List.of("2"));
            assertUpdate(addFiles(table, "replace", true, location(second)));
            assertQuery("SELECT * FROM " + table, "VALUES 2");
            assertUpdate(addFiles(table, "empty", true));
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testNestedColumnsAndSchemaEvolution()
            throws Exception
    {
        String table = "nested_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT {'x': 7::INTEGER, 'label': 'seven'} info, [1::INTEGER, 2] numbers, map(['k'], [3::INTEGER]) lookup) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (info ROW(x INTEGER, label VARCHAR), numbers ARRAY(INTEGER), lookup MAP(VARCHAR, INTEGER))");
        try {
            assertUpdate(addFiles(table, "nested", false, location(file)));
            assertQuery("SELECT info.x, info.label, numbers[2], lookup['k'] FROM " + table, "VALUES (7, 'seven', 2, 3)");
            assertThat(catalog.rows("SELECT info.x::VARCHAR, info.label, numbers[2]::VARCHAR, lookup['k']::VARCHAR FROM " + table)).containsExactly("7", "seven", "2", "3");
            assertUpdate("ALTER TABLE " + table + " RENAME COLUMN info TO details");
            assertQuery("SELECT details.x, details.label FROM " + table, "VALUES (7, 'seven')");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testMissingExtraAndCaseInsensitiveFields()
            throws Exception
    {
        String table = "schema_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT 8::INTEGER ID, 'ignored' extra) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, missing VARCHAR)");
        try {
            assertQueryFails(addFiles(table, "strict", false, location(file)), ".*Missing required column.*");
            String statement = addFiles(table, "relaxed", false, location(file));
            statement = statement.substring(0, statement.length() - 1) + ", allow_missing => true, ignore_extra_columns => true)";
            assertUpdate(statement);
            assertQuery("SELECT * FROM " + table, "VALUES (8, CAST(NULL AS VARCHAR))");
            assertThat(catalog.rows("SELECT id::VARCHAR, missing FROM " + table)).containsExactly("8", "<null>");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testHivePartitionConstantsAndPhysicalPartitionValidation()
            throws Exception
    {
        String table = "partition_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/region=west");
        catalog.executeInDuckDb("COPY (SELECT 5::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, region VARCHAR) WITH (partitioning = ARRAY['region'])");
        try {
            String statement = addFiles(table, "hive", false, location(file));
            assertUpdate(statement.substring(0, statement.length() - 1) + ", hive_partitioning => true)");
            assertQuery("SELECT * FROM " + table + " WHERE region = 'west'", "VALUES (5, 'west')");
            assertQuery("SELECT * FROM " + table + " WHERE region = 'east'", "SELECT 1, '' WHERE false");
            assertThat(catalog.rows("SELECT id::VARCHAR, region FROM " + table)).containsExactly("5", "west");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
        String physical = "physical_partition_" + randomNameSuffix();
        file = catalog.externalParquetFile(physical + "/region=wrong");
        catalog.executeInDuckDb("COPY (SELECT 1::INTEGER id, 'east' region UNION ALL SELECT 2, 'west') TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + physical + " (id INTEGER, region VARCHAR) WITH (partitioning = ARRAY['region'])");
        try {
            String statement = addFiles(physical, "physical", false, location(file));
            assertUpdate(statement.substring(0, statement.length() - 1) + ", hive_partitioning => true)");
            assertQuery("SELECT id FROM " + physical + " WHERE region = 'east'", "VALUES 1");
            assertQuery("SELECT id FROM " + physical + " WHERE region = 'west'", "VALUES 2");
        }
        finally {
            assertUpdate("DROP TABLE " + physical);
        }
    }

    @Test
    void testRejectsUnsafeInputsAndCorruptFiles()
            throws Exception
    {
        String table = "unsafe_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        Files.writeString(Path.of(file), "not parquet data");
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            assertQueryFails(addFiles(table, "bad", false, location(file)), ".*Parquet.*");
            assertQueryFails(addFiles(table, "outside", false, "s3://other-bucket/secret.parquet"), ".*catalog data path.*");
            assertQueryFails(addFiles(table, "traversal", false, "local:///main/../secret.parquet"), ".*traversal.*");
            assertQueryFails(addFiles(table, "duplicate", false, location(file), location(file)), ".*distinct.*");
            assertQueryFails(addFiles(table, "empty", false), ".*files must contain.*");
            assertQueryFails(addFiles(table, "bad id", false, location(file)), ".*import_id must contain.*");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testConcurrentRetriesPublishOnce()
            throws Exception
    {
        String table = "concurrent_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT 42::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try (var executor = Executors.newFixedThreadPool(4)) {
            String statement = addFiles(table, "same_import", false, location(file));
            var futures = IntStream.range(0, 4).mapToObj(_ -> executor.submit(() -> assertUpdate(statement))).toList();
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            assertQuery("SELECT * FROM " + table, "VALUES 42");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table)).containsExactly("42");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInsertAndReplacementPermissions()
            throws Exception
    {
        String table = "permissions_import_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            String append = addFiles(table, "denied", false, "local:///unread.parquet");
            assertAccessDenied(append, ".*Cannot insert into table.*", privilege(getSession().getUser(), table, INSERT_TABLE));
            String replace = addFiles(table, "denied_replace", true);
            assertAccessDenied(replace, ".*Cannot delete from table.*", privilege(getSession().getUser(), table, DELETE_TABLE));
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testMissingRowGroupStatisticsNeverPruneRows()
            throws Exception
    {
        String table = "statistics_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT i::INTEGER id FROM range(4096) t(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)".formatted(file));
        byte[] bytes = Files.readAllBytes(Path.of(file));
        int footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = bytes.length - 8 - footerLength;
        var metadata = Util.readFileMetaData(new ByteArrayInputStream(bytes, footerStart, footerLength));
        assertThat(metadata.getRow_groups()).hasSize(2);
        metadata.getRow_groups().getLast().getColumns().getFirst().getMeta_data().unsetStatistics();
        var footer = new ByteArrayOutputStream();
        Util.writeFileMetaData(metadata, footer);
        var rewritten = new ByteArrayOutputStream();
        rewritten.write(bytes, 0, footerStart);
        rewritten.write(footer.toByteArray());
        rewritten.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        rewritten.write("PAR1".getBytes(StandardCharsets.UTF_8));
        Files.write(Path.of(file), rewritten.toByteArray());
        assertUpdate("CREATE TABLE " + table + " (id INTEGER) WITH (partitioning = ARRAY['id'])");
        try {
            assertUpdate(addFiles(table, "missing_stats", false, location(file)));
            assertQuery("SELECT id FROM " + table + " WHERE id = 4095", "VALUES 4095");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(4096 AS BIGINT)");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " WHERE id = 4095")).containsExactly("4095");
            assertUpdate(replaceRange(table, "clear_second_group", "id", "2048", "4096"));
            assertQuery("SELECT count(*), min(id), max(id) FROM " + table, "VALUES (CAST(2048 AS BIGINT), 0, 2047)");
            assertThat(catalog.rows("SELECT count(*)::VARCHAR, max(id)::VARCHAR FROM " + table)).containsExactly("2048", "2047");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testPrimitiveTypesAndTemporalPartitions()
            throws Exception
    {
        String table = "typed_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT 9::BIGINT id, true flag, DATE '2024-02-03' event_date, TIMESTAMP '2024-02-03 12:34:56.123456' ts, 123.45::DECIMAL(10,2) amount, 'bytes'::BLOB payload, 2.5::FLOAT score) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, flag BOOLEAN, event_date DATE, ts TIMESTAMP(6), amount DECIMAL(10,2), payload VARBINARY, score REAL) WITH (partitioning = ARRAY['year(ts)', 'month(ts)', 'day(ts)'])");
        try {
            assertUpdate(addFiles(table, "types", false, location(file)));
            assertQuery("SELECT id, flag, event_date, ts, amount, to_hex(payload), score FROM " + table + " WHERE ts >= TIMESTAMP '2024-02-03 12:00:00'", "VALUES (9, true, DATE '2024-02-03', TIMESTAMP '2024-02-03 12:34:56.123456', 123.45, '6279746573', 2.5)");
            assertThat(catalog.rows("SELECT id::VARCHAR, amount::VARCHAR, ts::VARCHAR FROM " + table)).containsExactly("9", "123.45", "2024-02-03 12:34:56.123456");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testMissingNestedFieldsPreserveNullRows()
            throws Exception
    {
        String table = "nested_missing_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table);
        catalog.executeInDuckDb("COPY (SELECT 1::INTEGER id, {'x': 7::INTEGER} info UNION ALL SELECT 2, NULL) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, info ROW(missing VARCHAR))");
        try {
            String statement = addFiles(table, "missing_nested", false, location(file));
            assertQueryFails(statement.substring(0, statement.length() - 1) + ", allow_missing => true, ignore_extra_columns => true)", ".*A row must map at least one physical field.*");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
            assertUpdate("DROP TABLE " + table);
            assertUpdate("CREATE TABLE " + table + " (id INTEGER, info ROW(x INTEGER, missing VARCHAR))");
            statement = addFiles(table, "partial_nested", false, location(file));
            assertUpdate(statement.substring(0, statement.length() - 1) + ", allow_missing => true)");
            assertQuery("SELECT id, info.x, info.missing, info IS NULL FROM " + table, "VALUES (1, 7, CAST(NULL AS VARCHAR), false), (2, CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), true)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testAtomicRangeReplacementPreservesBoundaryAndNullRows()
            throws Exception
    {
        String table = "range_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/incoming");
        catalog.executeInDuckDb("COPY (SELECT 20::INTEGER id UNION ALL SELECT 29) TO '%s' (FORMAT PARQUET)".formatted(file));
        byte[] original = Files.readAllBytes(Path.of(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        assertUpdate("INSERT INTO " + table + " VALUES (9), (10), (15), (19), (20), (29), (30), (NULL)", 8);
        // Existing positional deletes must survive when the range creates a new delete file.
        assertUpdate("DELETE FROM " + table + " WHERE id = 9", 1);
        try {
            String before = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            String statement = replaceRange(table, "range", "id", "20", "30", location(file));
            assertUpdate(statement);
            assertQuery("SELECT id FROM " + table, "VALUES 10, 15, 19, 20, 29, 30, CAST(NULL AS INTEGER)");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id NULLS LAST")).containsExactly("10", "15", "19", "20", "29", "30", "<null>");
            String after = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            assertThat(Long.parseLong(after)).isEqualTo(Long.parseLong(before) + 1);
            assertThat(Files.readAllBytes(Path.of(file))).isEqualTo(original);
            assertUpdate(statement);
            assertThat(catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')")).isEqualTo(after);
            assertQueryFails(replaceRange(table, "range", "id", "19", "30", location(file)), ".*import_id.*different.*");
            assertUpdate(replaceRange(table, "clear", "id", "10", "20"));
            assertQuery("SELECT id FROM " + table, "VALUES 20, 29, 30, CAST(NULL AS INTEGER)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRangeReplacementDropsWholeFilesWithoutDeleteFiles()
            throws Exception
    {
        String table = "range_whole_" + randomNameSuffix();
        String inside = catalog.externalParquetFile(table + "/inside");
        String outside = catalog.externalParquetFile(table + "/outside");
        String replacement = catalog.externalParquetFile(table + "/replacement");
        catalog.executeInDuckDb(
                "COPY (SELECT 11::INTEGER id UNION ALL SELECT 19) TO '%s' (FORMAT PARQUET)".formatted(inside),
                "COPY (SELECT 5::INTEGER id UNION ALL SELECT 30) TO '%s' (FORMAT PARQUET)".formatted(outside),
                "COPY (SELECT 12::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(replacement));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            assertUpdate(addFiles(table, "seed", false, location(inside), location(outside)));
            assertUpdate(replaceRange(table, "replace", "id", "10", "20", location(replacement)));
            assertQuery("SELECT * FROM " + table, "VALUES 5, 12, 30");
            assertThat(catalog.scalar("SELECT count(*)::VARCHAR FROM __ducklake_metadata_lake.ducklake_delete_file d JOIN __ducklake_metadata_lake.ducklake_table t USING(table_id) WHERE t.table_name = '%s'".formatted(table))).isEqualTo("0");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testInvalidRangeBatchPublishesNothing()
            throws Exception
    {
        String table = "range_invalid_" + randomNameSuffix();
        String outside = catalog.externalParquetFile(table + "/outside");
        String nullable = catalog.externalParquetFile(table + "/nullable");
        catalog.executeInDuckDb(
                "COPY (SELECT 10::INTEGER id UNION ALL SELECT 20) TO '%s' (FORMAT PARQUET)".formatted(outside),
                "COPY (SELECT 12::INTEGER id UNION ALL SELECT NULL) TO '%s' (FORMAT PARQUET)".formatted(nullable));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        assertUpdate("INSERT INTO " + table + " VALUES 11, 18, 30", 3);
        try {
            String snapshot = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            assertQueryFails(replaceRange(table, "outside", "id", "10", "20", location(outside)), ".*entirely inside.*");
            assertQueryFails(replaceRange(table, "null", "id", "10", "20", location(nullable)), ".*non-NULL.*");
            assertQueryFails(replaceRange(table, "reverse", "id", "20", "10"), ".*start must be less than end.*");
            assertQueryFails(replaceRange(table, "equal", "id", "10", "10"), ".*start must be less than end.*");
            assertQueryFails(replaceRange(table, "missing", "unknown", "10", "20"), ".*column does not exist.*");
            String incomplete = addFiles(table, "incomplete", false);
            assertQueryFails(incomplete.substring(0, incomplete.length() - 1) + ", replace_range_column => 'id')", ".*must be specified together.*");
            String conflicting = replaceRange(table, "conflicting", "id", "10", "20");
            assertQueryFails(conflicting.replace("replace => false", "replace => true"), ".*mutually exclusive.*");
            assertAccessDenied(replaceRange(table, "permission", "id", "10", "20"), ".*Cannot delete from table.*", privilege(getSession().getUser(), table, DELETE_TABLE));
            assertQuery("SELECT * FROM " + table, "VALUES 11, 18, 30");
            assertThat(catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')")).isEqualTo(snapshot);
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testTimestampRangeAndConcurrentRetries()
            throws Exception
    {
        String table = "range_time_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/incoming");
        catalog.executeInDuckDb("COPY (SELECT TIMESTAMP '2024-02-15 00:00:00.123456' ts) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (ts TIMESTAMP(6))");
        assertUpdate("INSERT INTO " + table + " VALUES TIMESTAMP '2024-01-31 23:59:59.999999', TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00'", 3);
        try (var executor = Executors.newFixedThreadPool(3)) {
            String statement = replaceRange(table, "month", "ts", "2024-02-01 00:00:00", "2024-03-01 00:00:00", location(file));
            var futures = IntStream.range(0, 3).mapToObj(_ -> executor.submit(() -> assertUpdate(statement))).toList();
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            assertQuery("SELECT * FROM " + table, "VALUES TIMESTAMP '2024-01-31 23:59:59.999999', TIMESTAMP '2024-02-15 00:00:00.123456', TIMESTAMP '2024-03-01 00:00:00'");
            assertThat(catalog.rows("SELECT ts::VARCHAR FROM " + table + " ORDER BY ts")).containsExactly("2024-01-31 23:59:59.999999", "2024-02-15 00:00:00.123456", "2024-03-01 00:00:00");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    private static String replaceRange(String table, String importId, String column, String start, String end, String... files)
    {
        String statement = addFiles(table, importId, false, files);
        return statement.substring(0, statement.length() - 1) + ", replace_range_column => '" + column + "', replace_range_start => '" + start + "', replace_range_end => '" + end + "')";
    }

    private String location(String file)
    {
        return "local:///" + catalog.dataPath().relativize(Path.of(file));
    }

    private static String addFiles(String table, String importId, boolean replace, String... files)
    {
        String paths = String.join(", ", Arrays.stream(files).map(path -> "'" + path.replace("'", "''") + "'").toList());
        return "ALTER TABLE %s EXECUTE add_files(files => ARRAY[%s], import_id => '%s', replace => %s)".formatted(table, paths, importId, replace);
    }
}
