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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
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
        rewriteFooter(file, metadata -> {
            assertThat(metadata.getRow_groups()).hasSize(2);
            metadata.getRow_groups().getLast().getColumns().getFirst().getMeta_data().unsetStatistics();
        });
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

    @Test
    void testRecordedFilesCannotBeRegisteredAgain()
            throws Exception
    {
        String table = "reregister_" + randomNameSuffix();
        String other = "reregister_other_" + randomNameSuffix();
        String first = catalog.externalParquetFile(table + "/first");
        String second = catalog.externalParquetFile(table + "/second");
        String shared = catalog.externalParquetFile("imports_" + randomNameSuffix());
        catalog.executeInDuckDb(
                "COPY (SELECT 1::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(first),
                "COPY (SELECT 2::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(second),
                "COPY (SELECT 3::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(shared));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        assertUpdate("CREATE TABLE " + other + " (id INTEGER)");
        try {
            assertUpdate(addFiles(table, "first", false, location(first), location(shared)));
            // a file another table references would be deleted once either reference expires
            assertQueryFails(addFiles(other, "shared", false, location(shared)), ".*already registered.*");
            assertUpdate(addFiles(table, "replace", true, location(second)));
            // the replaced file is still referenced by earlier snapshots
            assertQueryFails(addFiles(table, "again", false, location(first)), ".*already registered.*");
            // and once they expire, it waits to be deleted
            catalog.executeInDuckDb("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY)");
            assertThat(catalog.scalar("SELECT count(*)::VARCHAR FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion WHERE path LIKE '%%%s/first/external.parquet'".formatted(table)))
                    .isEqualTo("1");
            assertQueryFails(addFiles(table, "after_expiry", false, location(first)), ".*already registered.*");
            assertQuery("SELECT * FROM " + table, "VALUES 2");
            assertQuery("SELECT count(*) FROM " + other, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
            assertUpdate("DROP TABLE " + other);
        }
    }

    @Test
    void testDeleteFilesCannotBeRegistered()
    {
        String table = "delete_file_import_" + randomNameSuffix();
        // the columns are those of a delete file, which could otherwise be read as a data file
        assertUpdate("CREATE TABLE " + table + " AS SELECT CAST(id AS VARCHAR) AS file_path, id AS pos FROM UNNEST(sequence(1, 10)) AS t(id)", 10);
        try {
            assertUpdate("DELETE FROM " + table + " WHERE pos = 1", 1);
            String deleteFile = "local:///main/%s/%s".formatted(table, catalog.scalar(
                    "SELECT d.path FROM __ducklake_metadata_lake.ducklake_delete_file d JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '%s' AND t.end_snapshot IS NULL AND d.end_snapshot IS NULL".formatted(table)));
            assertQueryFails(addFiles(table, "delete_file", false, deleteFile), ".*already registered.*");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(9 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testFilesInAnotherTableDirectoryCannotBeRegistered()
            throws Exception
    {
        String source = "source_" + randomNameSuffix();
        String target = "target_" + randomNameSuffix();
        String unregistered = catalog.externalParquetFile(source + "/unregistered");
        catalog.executeInDuckDb("COPY (SELECT 8::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(unregistered));
        assertUpdate("CREATE TABLE " + source + " (id INTEGER)");
        assertUpdate("CREATE TABLE " + target + " (id INTEGER)");
        try {
            assertUpdate("INSERT INTO " + source + " VALUES 7", 1);
            String sourceFile = "local:///main/%s/%s".formatted(source, catalog.scalar(
                    "SELECT f.path FROM __ducklake_metadata_lake.ducklake_data_file f JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id) WHERE t.table_name = '%s' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL".formatted(source)));
            assertQueryFails(addFiles(target, "registered", false, sourceFile), ".*directory of another table.*");
            assertQueryFails(addFiles(target, "unregistered", false, location(unregistered)), ".*directory of another table.*");
            assertQuery("SELECT count(*) FROM " + target, "VALUES CAST(0 AS BIGINT)");
            // the table owning the directory can register it
            assertUpdate(addFiles(source, "own", false, location(unregistered)));
            assertQuery("SELECT * FROM " + source, "VALUES 7, 8");
        }
        finally {
            assertUpdate("DROP TABLE " + source);
            assertUpdate("DROP TABLE " + target);
        }
    }

    @Test
    void testRetryOfAppliedImportReadsNothing()
            throws Exception
    {
        String table = "retry_import_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/batch");
        String other = catalog.externalParquetFile(table + "/other");
        catalog.executeInDuckDb(
                "COPY (SELECT 15::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(file),
                "COPY (SELECT 16::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(other));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            String statement = replaceRange(table, "batch", "id", "10", "20", location(file));
            assertUpdate(statement);
            assertUpdate("ALTER TABLE " + table + " ADD COLUMN extra VARCHAR");
            assertUpdate("ALTER TABLE " + table + " ALTER COLUMN id SET DATA TYPE BIGINT");
            String snapshot = catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
            // the file no longer matches the table, which a retry must not notice
            assertUpdate(statement);
            // nor does the retry read the file again
            Files.delete(Path.of(file));
            assertUpdate(statement);
            // an equivalent spelling of the bounds is the same import
            assertUpdate(replaceRange(table, "batch", "id", "+10", "0020", location(file)));
            assertThat(catalog.scalar("SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')")).isEqualTo(snapshot);
            assertQueryFails(replaceRange(table, "batch", "id", "10", "20", location(other)), ".*import_id.*different.*");
            assertQueryFails(replaceRange(table, "batch", "id", "10", "21", location(file)), ".*import_id.*different.*");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testUnannotatedIntegersAndNarrowDecimals()
            throws Exception
    {
        String table = "encodings_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/file");
        String replacement = catalog.externalParquetFile(table + "/replacement");
        // Spark and Arrow write signed integers without an annotation, and decimals in the
        // narrowest fixed length array that holds their precision
        MessageType schema = Types.buildMessage()
                .addField(Types.primitive(INT32, OPTIONAL).named("small"))
                .addField(Types.primitive(INT64, OPTIONAL).named("big"))
                .addField(Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(5).as(LogicalTypeAnnotation.decimalType(2, 10)).named("amount"))
                .addField(Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).length(9).as(LogicalTypeAnnotation.decimalType(2, 20)).named("wide"))
                .named("spark_schema");
        Map<List<String>, Type> types = ImmutableMap.of(
                ImmutableList.of("small"), INTEGER,
                ImmutableList.of("big"), BIGINT,
                ImmutableList.of("amount"), createDecimalType(10, 2),
                ImmutableList.of("wide"), createDecimalType(20, 2));
        writeParquet(file, schema, types, new Page(
                block(INTEGER, 7L, -8L),
                block(BIGINT, 100L, 200L),
                block(createDecimalType(10, 2), 12345L, -99L),
                block(createDecimalType(20, 2), Int128.valueOf(123456789012345678L), Int128.valueOf(-1L))));
        writeParquet(replacement, schema, types, new Page(
                block(INTEGER, 9L),
                block(BIGINT, 150L),
                block(createDecimalType(10, 2), 1L),
                block(createDecimalType(20, 2), Int128.valueOf(2L))));
        assertUpdate("CREATE TABLE " + table + " (small INTEGER, big BIGINT, amount DECIMAL(10, 2), wide DECIMAL(20, 2))");
        try {
            assertUpdate(addFiles(table, "encodings", false, location(file)));
            assertQuery("SELECT * FROM " + table,
                    "VALUES (7, 100, 123.45, 1234567890123456.78), (-8, 200, -0.99, -0.01)");
            assertThat(catalog.rows("SELECT small::VARCHAR, big::VARCHAR, amount::VARCHAR, wide::VARCHAR FROM " + table + " ORDER BY big"))
                    .containsExactly("7", "100", "123.45", "1234567890123456.78", "-8", "200", "-0.99", "-0.01");
            // the footer statistics of the unannotated integers prove the range of a replacement
            assertUpdate(replaceRange(table, "replace_big", "big", "150", "250", location(replacement)));
            assertQuery("SELECT small, big FROM " + table, "VALUES (7, 100), (9, 150)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRangeReplacementWithoutStatisticsPruningReadsEveryFile()
            throws SQLException
    {
        String table = "range_no_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        try {
            assertUpdate("INSERT INTO " + table + " VALUES 5, 15, 25", 3);
            // statistics an operator distrusts: they claim the file lies inside the range
            catalog.executeInMetastore(
                    "UPDATE ducklake_file_column_stats SET min_value = '10', max_value = '19' WHERE table_id = (SELECT table_id FROM ducklake_table WHERE table_name = '%s' AND end_snapshot IS NULL)".formatted(table));
            Session noPruning = Session.builder(getSession())
                    .setCatalogSessionProperty("ducklake", "file_statistics_pruning_enabled", "false")
                    .build();
            assertUpdate(noPruning, replaceRange(table, "no_pruning", "id", "10", "20"));
            assertQuery("SELECT id FROM " + table, "VALUES 5, 25");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testListElementsCannotBeMissing()
            throws Exception
    {
        String table = "legacy_list_" + randomNameSuffix();
        String file = catalog.externalParquetFile(table + "/file");
        catalog.executeInDuckDb("COPY (SELECT 1::INTEGER id, [1::INTEGER, 2] numbers) TO '%s' (FORMAT PARQUET)".formatted(file));
        // the element of a list named as the legacy encodings name it
        rewriteFooter(file, metadata -> {
            for (SchemaElement element : metadata.getSchema()) {
                if (element.getName().equals("element")) {
                    element.setName("item");
                }
            }
            for (RowGroup rowGroup : metadata.getRow_groups()) {
                for (ColumnChunk chunk : rowGroup.getColumns()) {
                    chunk.getMeta_data().setPath_in_schema(chunk.getMeta_data().getPath_in_schema().stream()
                            .map(name -> name.equals("element") ? "item" : name)
                            .toList());
                }
            }
        });
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, numbers ARRAY(INTEGER))");
        try {
            String statement = addFiles(table, "legacy", false, location(file));
            assertQueryFails(statement.substring(0, statement.length() - 1) + ", allow_missing => true, ignore_extra_columns => true)", ".*standard Parquet list/map encoding for numbers.*");
            assertQueryFails(statement, ".*standard Parquet list/map encoding for numbers.*");
            assertQuery("SELECT count(*) FROM " + table, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testHivePartitionValuesAreReadLikeDuckDb()
            throws Exception
    {
        String table = "hive_paths_" + randomNameSuffix();
        String repeated = catalog.externalParquetFile(table + "/dt=first/x/dt=second");
        String percent = catalog.externalParquetFile(table + "/dt=50%/escaped");
        catalog.executeInDuckDb(
                "COPY (SELECT 1::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(repeated),
                "COPY (SELECT 2::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(percent));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, dt VARCHAR)");
        try {
            // DuckDB registers a path naming a key twice, and one whose value holds a bare '%'
            catalog.executeInDuckDb(
                    "CALL ducklake_add_data_files('lake', '%s', '%s', hive_partitioning => true)".formatted(table, repeated),
                    "CALL ducklake_add_data_files('lake', '%s', '%s', hive_partitioning => true)".formatted(table, percent));
            assertThat(catalog.rows("SELECT id::VARCHAR, dt FROM " + table + " ORDER BY id")).containsExactly("1", "first", "2", "50%");
            assertQuery("SELECT id, dt FROM " + table, "VALUES (1, 'first'), (2, '50%')");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testLocationsWithPercentSigns()
            throws Exception
    {
        String table = "percent_import_" + randomNameSuffix();
        Path file = Path.of(catalog.externalParquetFile(table + "/region=50%off")).resolveSibling("50%off.parquet");
        catalog.executeInDuckDb("COPY (SELECT 4::INTEGER id) TO '%s' (FORMAT PARQUET)".formatted(file));
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, region VARCHAR)");
        try {
            String statement = addFiles(table, "percent", false, location(file.toString()));
            assertUpdate(statement.substring(0, statement.length() - 1) + ", hive_partitioning => true)");
            assertQuery("SELECT * FROM " + table, "VALUES (4, '50%off')");
            assertThat(catalog.rows("SELECT id::VARCHAR, region FROM " + table)).containsExactly("4", "50%off");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRangeColumnMustBeExactlyOrderedScalar()
    {
        String table = "range_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id INTEGER, score DOUBLE, tags ARRAY(INTEGER), info ROW(x INTEGER))");
        try {
            assertQueryFails(replaceRange(table, "score", "score", "1.0", "2.0"), ".*exactly ordered scalar column, but score has type double.*");
            assertQueryFails(replaceRange(table, "tags", "tags", "[1]", "[2]"), ".*exactly ordered scalar column, but tags has type array\\(integer\\).*");
            assertQueryFails(replaceRange(table, "info", "info", "{1}", "{2}"), ".*exactly ordered scalar column, but info has type row.*");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRequiredFieldsProveColumnsHoldNoNulls()
            throws Exception
    {
        String table = "not_null_import_" + randomNameSuffix();
        String required = catalog.externalParquetFile(table + "/required");
        String optional = catalog.externalParquetFile(table + "/optional");
        String empty = catalog.externalParquetFile(table + "/empty");
        Block x = block(INTEGER, 7L);
        writeParquet(
                required,
                Types.buildMessage()
                        .addField(Types.primitive(INT32, REQUIRED).as(LogicalTypeAnnotation.intType(32, true)).named("id"))
                        .addField(Types.requiredGroup().addField(Types.primitive(INT32, OPTIONAL).as(LogicalTypeAnnotation.intType(32, true)).named("x")).named("info"))
                        .named("schema"),
                ImmutableMap.of(ImmutableList.of("id"), INTEGER, ImmutableList.of("info", "x"), INTEGER),
                new Page(block(INTEGER, 1L), RowBlock.fromFieldBlocks(1, new Block[] {x})));
        // REQUIRED fields prove the absence of NULLs without any statistics
        rewriteFooter(required, metadata -> metadata.getRow_groups().forEach(rowGroup -> rowGroup.getColumns().forEach(chunk -> chunk.getMeta_data().unsetStatistics())));
        catalog.executeInDuckDb(
                "COPY (SELECT 2::INTEGER id, {'x': 8::INTEGER} info) TO '%s' (FORMAT PARQUET)".formatted(optional),
                "COPY (SELECT 3::INTEGER id, {'x': 9::INTEGER} info WHERE false) TO '%s' (FORMAT PARQUET)".formatted(empty),
                "CREATE TABLE %s (id INTEGER NOT NULL, info STRUCT(x INTEGER) NOT NULL)".formatted(table));
        try {
            // a footer cannot count the NULLs of a nested column
            assertQueryFails(addFiles(table, "optional", false, location(optional)), ".*Cannot prove that required column contains no NULLs: info.*");
            assertUpdate(addFiles(table, "required", false, location(required), location(empty)));
            assertQuery("SELECT id, info.x FROM " + table, "VALUES (1, 7)");
            assertThat(catalog.rows("SELECT id::VARCHAR, info.x::VARCHAR FROM " + table)).containsExactly("1", "7");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRangeReplacementAcceptsRequiredColumnsWithoutNullCounts()
            throws Exception
    {
        String table = "required_range_" + randomNameSuffix();
        String required = catalog.externalParquetFile(table + "/required");
        String optional = catalog.externalParquetFile(table + "/optional");
        for (String file : List.of(required, optional)) {
            writeParquet(
                    file,
                    Types.buildMessage()
                            .addField(Types.primitive(INT32, file.equals(required) ? REQUIRED : OPTIONAL).as(LogicalTypeAnnotation.intType(32, true)).named("id"))
                            .named("schema"),
                    ImmutableMap.of(ImmutableList.of("id"), INTEGER),
                    new Page(block(INTEGER, 12L, 18L)));
            // like ClickHouse, keep the bounds of the column but leave out its null count
            rewriteFooter(file, metadata -> metadata.getRow_groups().forEach(rowGroup -> rowGroup.getColumns().forEach(chunk -> {
                assertThat(chunk.getMeta_data().getStatistics().isSetMin_value()).isTrue();
                chunk.getMeta_data().getStatistics().unsetNull_count();
            })));
        }
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        assertUpdate("INSERT INTO " + table + " VALUES 5, 10, 15, 20", 4);
        try {
            // an OPTIONAL column without a null count can hold NULLs outside of any range
            assertQueryFails(replaceRange(table, "optional", "id", "10", "20", location(optional)), ".*Replacement file must have non-NULL values entirely inside the replacement range.*");
            assertUpdate(replaceRange(table, "required", "id", "10", "20", location(required)));
            assertQuery("SELECT id FROM " + table, "VALUES 5, 12, 18, 20");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + table + " ORDER BY id")).containsExactly("5", "12", "18", "20");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    @Test
    void testRangeReplacementRejectsDuplicateDeleteFiles()
            throws Exception
    {
        String table = "duplicate_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id INTEGER)");
        String tableId = "(SELECT table_id FROM ducklake_table WHERE table_name = '%s' AND end_snapshot IS NULL)".formatted(table);
        try {
            assertUpdate("INSERT INTO " + table + " VALUES 10, 20, 30", 3);
            assertUpdate("DELETE FROM " + table + " WHERE id = 30", 1);
            // a catalog no engine writes: two delete files applying to the same data file
            catalog.executeInMetastore(
                    """
                    INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative,
                        format, delete_count, file_size_bytes, footer_size, encryption_key)
                    SELECT delete_file_id + 1000000, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative,
                        format, delete_count, file_size_bytes, footer_size, encryption_key
                    FROM ducklake_delete_file WHERE end_snapshot IS NULL AND table_id = %s""".formatted(tableId));
            assertQueryFails(replaceRange(table, "duplicate", "id", "10", "15"), ".*Multiple delete files are visible for data file.*");
        }
        finally {
            catalog.executeInMetastore("DELETE FROM ducklake_delete_file WHERE delete_file_id >= 1000000 AND table_id = " + tableId);
            assertUpdate("DROP TABLE " + table);
        }
    }

    private static Block block(Type type, Object... values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            TypeUtils.writeNativeValue(type, builder, value);
        }
        return builder.build();
    }

    private static void writeParquet(String file, MessageType schema, Map<List<String>, Type> primitiveTypes, Page page)
            throws IOException
    {
        try (OutputStream output = Files.newOutputStream(Path.of(file));
                ParquetWriter writer = new ParquetWriter(output, schema, primitiveTypes, ParquetWriterOptions.builder().build(), CompressionCodec.SNAPPY, "test", Optional.empty(), Optional.empty())) {
            writer.write(page);
        }
    }

    /**
     * Changes the footer of a Parquet file in place, leaving its data pages as they are.
     */
    private static void rewriteFooter(String file, Consumer<FileMetaData> change)
            throws IOException
    {
        byte[] bytes = Files.readAllBytes(Path.of(file));
        int footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int footerStart = bytes.length - 8 - footerLength;
        FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(bytes, footerStart, footerLength));
        change.accept(metadata);
        var footer = new ByteArrayOutputStream();
        Util.writeFileMetaData(metadata, footer);
        var rewritten = new ByteArrayOutputStream();
        rewritten.write(bytes, 0, footerStart);
        rewritten.write(footer.toByteArray());
        rewritten.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array());
        rewritten.write("PAR1".getBytes(StandardCharsets.UTF_8));
        Files.write(Path.of(file), rewritten.toByteArray());
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
