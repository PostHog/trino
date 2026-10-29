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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Pins every catalog row a commit writes when it registers data files: the data file rows, their
 * partition values and column statistics, the name mappings of registered files, the statistics
 * kept for the whole table, and the snapshot. Values that depend on how a Parquet writer laid out
 * a file, such as its size, are read from the file itself.
 */
@Execution(SAME_THREAD)
final class TestDuckLakeDataFileRows
        extends AbstractTestQueryFramework
{
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
        return DuckLakeQueryRunner.builder(catalog)
                .addConnectorProperty("ducklake.add-files-procedure.enabled", "true")
                .build();
    }

    /**
     * Registers files that share a name mapping and a file with a mapping of its own, files inside
     * a single partition, a file spanning two partitions and a file holding only NULL partition
     * values, with statistics that are known, unknown, or absent because the column is missing.
     */
    @Test
    void testRegisteringFilesWritesTheirRows()
            throws Exception
    {
        String table = "registered_" + randomNameSuffix();
        Path first = writeWithDuckDb(table, "a",
                """
                SELECT * FROM (VALUES
                    (1::BIGINT, 'east', 1.5::DOUBLE, 'x', true, DATE '2024-01-01'),
                    (2, 'east', 2.5, NULL, false, DATE '2024-01-02')) v(id, region, score, note, flag, day)""");
        Path second = writeWithDuckDb(table, "b",
                """
                SELECT * FROM (VALUES
                    (3::BIGINT, 'west', 3.5::DOUBLE, 'y', true, DATE '2024-02-01'),
                    (4, 'west', 4.5, 'z', true, DATE '2024-02-02'),
                    (5, 'west', 5.5, 'w', true, DATE '2024-02-03')) v(id, region, score, note, flag, day)""");
        Path spanning = writeWithDuckDb(table, "c",
                """
                SELECT * FROM (VALUES
                    (6::BIGINT, 'east', 6.5::DOUBLE, 'v', false, DATE '2024-03-01'),
                    (7, 'west', 7.5, 'u', false, DATE '2024-03-02')) v(id, region, score, note, flag, day)""");
        // the file lacks the note column, so it is registered with a name mapping of its own
        Path withoutNote = writeWithDuckDb(table, "d",
                """
                SELECT * FROM (VALUES
                    (8::BIGINT, NULL::VARCHAR, 'NaN'::DOUBLE, true, DATE '2024-04-01'),
                    (9, NULL, 9.5, false, DATE '2024-04-02')) v(id, region, score, flag, day)""");
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, region VARCHAR, score DOUBLE, note VARCHAR, flag BOOLEAN, day DATE) WITH (partitioning = ARRAY['region'])");
        try {
            CatalogState before = catalogState(table);
            assertUpdate(
                    "ALTER TABLE %s EXECUTE add_files(files => ARRAY[%s], import_id => 'rows', allow_missing => true)".formatted(
                            table,
                            Stream.of(location(spanning), location(withoutNote), location(first), location(second))
                                    .map(path -> "'" + path + "'")
                                    .collect(joining(", "))));

            long tableId = before.tableId();
            long partitionId = before.partitionId();
            long snapshot = before.snapshotId() + 1;
            // mappings draw from the file identifiers before the files do, in the order of the
            // sorted locations
            long sharedMapping = before.nextFileId();
            long ownMapping = sharedMapping + 1;
            long fileA = sharedMapping + 2;
            long fileB = sharedMapping + 3;
            long fileC = sharedMapping + 4;
            long fileD = sharedMapping + 5;
            ParquetFile a = ParquetFile.read(first);
            ParquetFile b = ParquetFile.read(second);
            ParquetFile c = ParquetFile.read(spanning);
            ParquetFile d = ParquetFile.read(withoutNote);
            long id = before.columnId("id");
            long region = before.columnId("region");
            long score = before.columnId("score");
            long note = before.columnId("note");
            long flag = before.columnId("flag");
            long day = before.columnId("day");

            assertThat(dataFiles(tableId)).isEqualTo(ImmutableList.of(
                    row(fileA, snapshot, null, null, "a/external.parquet", true, "parquet", 2, a.size(), a.footerSize(), 0, partitionId, null, sharedMapping),
                    row(fileB, snapshot, null, null, "b/external.parquet", true, "parquet", 3, b.size(), b.footerSize(), 2, partitionId, null, sharedMapping),
                    // a file spanning partitions is not given the table's partitioning
                    row(fileC, snapshot, null, null, "c/external.parquet", true, "parquet", 2, c.size(), c.footerSize(), 5, null, null, sharedMapping),
                    row(fileD, snapshot, null, null, "d/external.parquet", true, "parquet", 2, d.size(), d.footerSize(), 7, partitionId, null, ownMapping)));
            assertThat(partitionValues(tableId)).isEqualTo(ImmutableList.of(
                    row(fileA, 0, "east"),
                    row(fileB, 0, "west"),
                    row(fileD, 0, null)));
            assertThat(fileColumnStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(fileA, id, a.columnSize("id"), 2, 0, "1", "2", false),
                    row(fileA, region, a.columnSize("region"), 2, 0, "east", "east", false),
                    // footers cannot prove the absence of NaN, so floating point bounds are not recorded
                    row(fileA, score, a.columnSize("score"), 2, 0, null, null, null),
                    row(fileA, note, a.columnSize("note"), 2, 1, "x", "x", false),
                    row(fileA, flag, a.columnSize("flag"), 2, 0, "false", "true", false),
                    row(fileA, day, a.columnSize("day"), 2, 0, "2024-01-01", "2024-01-02", false),
                    row(fileB, id, b.columnSize("id"), 3, 0, "3", "5", false),
                    row(fileB, region, b.columnSize("region"), 3, 0, "west", "west", false),
                    row(fileB, score, b.columnSize("score"), 3, 0, null, null, null),
                    row(fileB, note, b.columnSize("note"), 3, 0, "w", "z", false),
                    row(fileB, flag, b.columnSize("flag"), 3, 0, "true", "true", false),
                    row(fileB, day, b.columnSize("day"), 3, 0, "2024-02-01", "2024-02-03", false),
                    row(fileC, id, c.columnSize("id"), 2, 0, "6", "7", false),
                    row(fileC, region, c.columnSize("region"), 2, 0, "east", "west", false),
                    row(fileC, score, c.columnSize("score"), 2, 0, null, null, null),
                    row(fileC, note, c.columnSize("note"), 2, 0, "u", "v", false),
                    row(fileC, flag, c.columnSize("flag"), 2, 0, "false", "false", false),
                    row(fileC, day, c.columnSize("day"), 2, 0, "2024-03-01", "2024-03-02", false),
                    row(fileD, id, d.columnSize("id"), 2, 0, "8", "9", false),
                    row(fileD, region, d.columnSize("region"), 2, 2, null, null, false),
                    row(fileD, score, d.columnSize("score"), 2, 0, null, null, null),
                    // the missing column reads as NULL in every row
                    row(fileD, note, 0, 2, 2, null, null, false),
                    row(fileD, flag, d.columnSize("flag"), 2, 0, "false", "true", false),
                    row(fileD, day, d.columnSize("day"), 2, 0, "2024-04-01", "2024-04-02", false)));
            assertThat(columnMappings(tableId)).isEqualTo(ImmutableList.of(
                    row(sharedMapping, "map_by_name"),
                    row(ownMapping, "map_by_name")));
            assertThat(nameMappings(tableId)).isEqualTo(ImmutableList.of(
                    row(sharedMapping, id, "id", id, null, false),
                    row(sharedMapping, region, "region", region, null, false),
                    row(sharedMapping, score, "score", score, null, false),
                    row(sharedMapping, note, "note", note, null, false),
                    row(sharedMapping, flag, "flag", flag, null, false),
                    row(sharedMapping, day, "day", day, null, false),
                    row(ownMapping, id, "id", id, null, false),
                    row(ownMapping, region, "region", region, null, false),
                    row(ownMapping, score, "score", score, null, false),
                    row(ownMapping, flag, "flag", flag, null, false),
                    row(ownMapping, day, "day", day, null, false)));
            assertThat(tableStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(9, 9, a.size() + b.size() + c.size() + d.size())));
            // a bound one of the files leaves unknown is unknown for the whole table
            assertThat(tableColumnStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(id, false, false, "1", "9"),
                    row(region, true, false, null, null),
                    row(score, false, null, null, null),
                    row(note, true, false, null, null),
                    row(flag, false, false, "false", "true"),
                    row(day, false, false, "2024-01-01", "2024-04-02")));
            assertThat(snapshot(snapshot)).isEqualTo(ImmutableList.of(
                    row(before.schemaVersion(), before.nextCatalogId(), fileD + 1, "inserted_into_table:" + tableId)));
            assertThat(catalog.metastoreRows("SELECT key, begin_snapshot, end_snapshot FROM ducklake_tag WHERE object_id = " + tableId)).isEqualTo(ImmutableList.of(
                    row("trino_import_rows", snapshot, null)));

            // both engines read the rows, numbered in the order of the files
            assertThat(catalog.rows("SELECT rowid, id, region, score::VARCHAR, note, flag, day FROM " + table + " ORDER BY id")).isEqualTo(ImmutableList.of(
                    "0",
                    "1",
                    "east",
                    "1.5",
                    "x",
                    "true",
                    "2024-01-01",
                    "1",
                    "2",
                    "east",
                    "2.5",
                    "<null>",
                    "false",
                    "2024-01-02",
                    "2",
                    "3",
                    "west",
                    "3.5",
                    "y",
                    "true",
                    "2024-02-01",
                    "3",
                    "4",
                    "west",
                    "4.5",
                    "z",
                    "true",
                    "2024-02-02",
                    "4",
                    "5",
                    "west",
                    "5.5",
                    "w",
                    "true",
                    "2024-02-03",
                    "5",
                    "6",
                    "east",
                    "6.5",
                    "v",
                    "false",
                    "2024-03-01",
                    "6",
                    "7",
                    "west",
                    "7.5",
                    "u",
                    "false",
                    "2024-03-02",
                    "7",
                    "8",
                    "<null>",
                    "nan",
                    "<null>",
                    "true",
                    "2024-04-01",
                    "8",
                    "9",
                    "<null>",
                    "9.5",
                    "<null>",
                    "false",
                    "2024-04-02"));
            assertThat(catalog.rows("SELECT id FROM " + table + " WHERE region = 'east' ORDER BY id")).isEqualTo(ImmutableList.of("1", "2", "6"));
            assertThat(catalog.rows("SELECT id FROM " + table + " WHERE region IS NULL ORDER BY id")).isEqualTo(ImmutableList.of("8", "9"));
            assertQuery(
                    "SELECT id, region, note, flag, day FROM " + table,
                    """
                    VALUES
                        (1, 'east', 'x', true, DATE '2024-01-01'),
                        (2, 'east', NULL, false, DATE '2024-01-02'),
                        (3, 'west', 'y', true, DATE '2024-02-01'),
                        (4, 'west', 'z', true, DATE '2024-02-02'),
                        (5, 'west', 'w', true, DATE '2024-02-03'),
                        (6, 'east', 'v', false, DATE '2024-03-01'),
                        (7, 'west', 'u', false, DATE '2024-03-02'),
                        (8, NULL, NULL, true, DATE '2024-04-01'),
                        (9, NULL, NULL, false, DATE '2024-04-02')""");
            assertQuery("SELECT id FROM " + table + " WHERE region = 'west'", "VALUES 3, 4, 5, 7");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    /**
     * Inserts rows into two partitions, then, after a column was added, inserts again: the first
     * insert creates the statistics of every column of the table, the second widens those that
     * exist and creates them for the new column.
     */
    @Test
    void testInsertWritesTheRowsOfItsFiles()
            throws Exception
    {
        String table = "inserted_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (id BIGINT, region VARCHAR, score DOUBLE, note VARCHAR, day DATE) WITH (partitioning = ARRAY['region'])");
        try {
            CatalogState before = catalogState(table);
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                        (1, 'east', 1.5, 'a', DATE '2024-01-01'),
                        (2, 'east', nan(), NULL, DATE '2024-01-02'),
                        (3, 'west', 3.5, 'c', DATE '2024-01-03'),
                        (4, 'west', 4.5, 'd', DATE '2024-01-04'),
                        (5, 'west', -1.0, 'e', DATE '2024-01-05')""".formatted(table),
                    5);

            long tableId = before.tableId();
            long partitionId = before.partitionId();
            long snapshot = before.snapshotId() + 1;
            long id = before.columnId("id");
            long region = before.columnId("region");
            long score = before.columnId("score");
            long note = before.columnId("note");
            long day = before.columnId("day");

            // The workers write a file per partition, and the order in which the commit numbers
            // them is the order in which their fragments arrive.
            List<String> paths = catalog.metastoreRows("SELECT path FROM ducklake_data_file WHERE table_id = %s ORDER BY data_file_id".formatted(tableId)).stream()
                    .map(List::getFirst)
                    .collect(toImmutableList());
            assertThat(paths).hasSize(2);
            assertThat(paths).allMatch(path -> path.matches("region=(east|west)/ducklake-[0-9a-f-]{36}\\.parquet"));
            boolean eastFirst = paths.getFirst().startsWith("region=east/");
            String eastPath = eastFirst ? paths.get(0) : paths.get(1);
            String westPath = eastFirst ? paths.get(1) : paths.get(0);
            long eastFile = before.nextFileId() + (eastFirst ? 0 : 1);
            long westFile = before.nextFileId() + (eastFirst ? 1 : 0);
            long eastRowIdStart = eastFirst ? 0 : 3;
            long westRowIdStart = eastFirst ? 2 : 0;
            ParquetFile east = ParquetFile.read(tableDirectory(table).resolve(eastPath));
            ParquetFile west = ParquetFile.read(tableDirectory(table).resolve(westPath));

            List<List<String>> eastFileRow = ImmutableList.of(
                    row(eastFile, snapshot, null, null, eastPath, true, "parquet", 2, east.size(), east.footerSize(), eastRowIdStart, partitionId, null, null));
            List<List<String>> westFileRow = ImmutableList.of(
                    row(westFile, snapshot, null, null, westPath, true, "parquet", 3, west.size(), west.footerSize(), westRowIdStart, partitionId, null, null));
            assertThat(dataFiles(tableId)).isEqualTo(ordered(eastFirst, eastFileRow, westFileRow));
            assertThat(partitionValues(tableId)).isEqualTo(ordered(eastFirst, ImmutableList.of(row(eastFile, 0, "east")), ImmutableList.of(row(westFile, 0, "west"))));
            List<List<String>> eastStatistics = ImmutableList.of(
                    row(eastFile, id, east.columnSize("id"), 2, 0, "1", "2", null),
                    row(eastFile, region, east.columnSize("region"), 2, 0, "east", "east", null),
                    // a NaN leaves the bounds of a floating point column unknown
                    row(eastFile, score, east.columnSize("score"), 2, 0, null, null, true),
                    row(eastFile, note, east.columnSize("note"), 2, 1, "a", "a", null),
                    row(eastFile, day, east.columnSize("day"), 2, 0, "2024-01-01", "2024-01-02", null));
            List<List<String>> westStatistics = ImmutableList.of(
                    row(westFile, id, west.columnSize("id"), 3, 0, "3", "5", null),
                    row(westFile, region, west.columnSize("region"), 3, 0, "west", "west", null),
                    row(westFile, score, west.columnSize("score"), 3, 0, "-1.0", "4.5", false),
                    row(westFile, note, west.columnSize("note"), 3, 0, "c", "e", null),
                    row(westFile, day, west.columnSize("day"), 3, 0, "2024-01-03", "2024-01-05", null));
            assertThat(fileColumnStatistics(tableId)).isEqualTo(ordered(eastFirst, eastStatistics, westStatistics));
            assertThat(tableStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(5, 5, east.size() + west.size())));
            assertThat(tableColumnStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(id, false, null, "1", "5"),
                    row(region, false, null, "east", "west"),
                    row(score, false, true, null, null),
                    row(note, true, null, "a", "e"),
                    row(day, false, null, "2024-01-01", "2024-01-05")));
            assertThat(snapshot(snapshot)).isEqualTo(ImmutableList.of(
                    row(before.schemaVersion(), before.nextCatalogId(), before.nextFileId() + 2, "inserted_into_table:" + tableId)));

            assertUpdate("ALTER TABLE " + table + " ADD COLUMN extra BIGINT");
            CatalogState altered = catalogState(table);
            long extra = altered.columnId("extra");
            assertUpdate("INSERT INTO " + table + " VALUES (6, 'west', 6.5, 'f', DATE '2024-01-06', 60)", 1);
            long secondSnapshot = altered.snapshotId() + 1;
            long thirdFile = altered.nextFileId();
            String thirdPath = catalog.metastoreRows("SELECT path FROM ducklake_data_file WHERE data_file_id = " + thirdFile).getFirst().getFirst();
            assertThat(thirdPath).matches("region=west/ducklake-[0-9a-f-]{36}\\.parquet");
            ParquetFile third = ParquetFile.read(tableDirectory(table).resolve(thirdPath));

            assertThat(dataFiles(tableId)).isEqualTo(ImmutableList.<List<String>>builder()
                    .addAll(ordered(eastFirst, eastFileRow, westFileRow))
                    .add(row(thirdFile, secondSnapshot, null, null, thirdPath, true, "parquet", 1, third.size(), third.footerSize(), 5, partitionId, null, null))
                    .build());
            assertThat(fileColumnStatistics(tableId)).isEqualTo(ImmutableList.<List<String>>builder()
                    .addAll(ordered(eastFirst, eastStatistics, westStatistics))
                    .add(row(thirdFile, id, third.columnSize("id"), 1, 0, "6", "6", null))
                    .add(row(thirdFile, region, third.columnSize("region"), 1, 0, "west", "west", null))
                    .add(row(thirdFile, score, third.columnSize("score"), 1, 0, "6.5", "6.5", false))
                    .add(row(thirdFile, note, third.columnSize("note"), 1, 0, "f", "f", null))
                    .add(row(thirdFile, day, third.columnSize("day"), 1, 0, "2024-01-06", "2024-01-06", null))
                    .add(row(thirdFile, extra, third.columnSize("extra"), 1, 0, "60", "60", null))
                    .build());
            assertThat(tableStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(6, 6, east.size() + west.size() + third.size())));
            assertThat(tableColumnStatistics(tableId)).isEqualTo(ImmutableList.of(
                    row(id, false, null, "1", "6"),
                    row(region, false, null, "east", "west"),
                    row(score, false, true, null, null),
                    row(note, true, null, "a", "f"),
                    row(day, false, null, "2024-01-01", "2024-01-06"),
                    row(extra, false, null, "60", "60")));
            assertThat(snapshot(secondSnapshot)).isEqualTo(ImmutableList.of(
                    row(altered.schemaVersion(), altered.nextCatalogId(), thirdFile + 1, "inserted_into_table:" + tableId)));

            // both engines read the rows, and DuckDB numbers them from the row identifiers the files were given
            assertThat(catalog.rows("SELECT id, region, score::VARCHAR, note, day, extra FROM " + table + " ORDER BY id")).isEqualTo(ImmutableList.of(
                    "1",
                    "east",
                    "1.5",
                    "a",
                    "2024-01-01",
                    "<null>",
                    "2",
                    "east",
                    "nan",
                    "<null>",
                    "2024-01-02",
                    "<null>",
                    "3",
                    "west",
                    "3.5",
                    "c",
                    "2024-01-03",
                    "<null>",
                    "4",
                    "west",
                    "4.5",
                    "d",
                    "2024-01-04",
                    "<null>",
                    "5",
                    "west",
                    "-1.0",
                    "e",
                    "2024-01-05",
                    "<null>",
                    "6",
                    "west",
                    "6.5",
                    "f",
                    "2024-01-06",
                    "60"));
            assertThat(catalog.rows("SELECT rowid FROM " + table + " WHERE region = 'east' ORDER BY rowid"))
                    .isEqualTo(rowIds(eastRowIdStart, eastRowIdStart + 1));
            assertThat(catalog.rows("SELECT rowid FROM " + table + " WHERE region = 'west' ORDER BY rowid"))
                    .isEqualTo(rowIds(westRowIdStart, westRowIdStart + 1, westRowIdStart + 2, 5));
            assertQuery(
                    "SELECT id, region, note, day, extra FROM " + table,
                    """
                    VALUES
                        (1, 'east', 'a', DATE '2024-01-01', NULL),
                        (2, 'east', NULL, DATE '2024-01-02', NULL),
                        (3, 'west', 'c', DATE '2024-01-03', NULL),
                        (4, 'west', 'd', DATE '2024-01-04', NULL),
                        (5, 'west', 'e', DATE '2024-01-05', NULL),
                        (6, 'west', 'f', DATE '2024-01-06', 60)""");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }

    private List<List<String>> dataFiles(long tableId)
    {
        return catalog.metastoreRows(
                """
                SELECT data_file_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count,
                    file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id
                FROM ducklake_data_file WHERE table_id = %s ORDER BY data_file_id""".formatted(tableId));
    }

    private List<List<String>> partitionValues(long tableId)
    {
        return catalog.metastoreRows(
                """
                SELECT data_file_id, partition_key_index, partition_value
                FROM ducklake_file_partition_value WHERE table_id = %s ORDER BY data_file_id, partition_key_index""".formatted(tableId));
    }

    private List<List<String>> fileColumnStatistics(long tableId)
    {
        return catalog.metastoreRows(
                """
                SELECT data_file_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan
                FROM ducklake_file_column_stats WHERE table_id = %s ORDER BY data_file_id, column_id""".formatted(tableId));
    }

    private List<List<String>> columnMappings(long tableId)
    {
        return catalog.metastoreRows("SELECT mapping_id, type FROM ducklake_column_mapping WHERE table_id = %s ORDER BY mapping_id".formatted(tableId));
    }

    private List<List<String>> nameMappings(long tableId)
    {
        return catalog.metastoreRows(
                """
                SELECT m.mapping_id, m.column_id, m.source_name, m.target_field_id, m.parent_column, m.is_partition
                FROM ducklake_name_mapping m JOIN ducklake_column_mapping c ON m.mapping_id = c.mapping_id
                WHERE c.table_id = %s ORDER BY m.mapping_id, m.column_id""".formatted(tableId));
    }

    private List<List<String>> tableStatistics(long tableId)
    {
        return catalog.metastoreRows("SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats WHERE table_id = " + tableId);
    }

    private List<List<String>> tableColumnStatistics(long tableId)
    {
        return catalog.metastoreRows(
                """
                SELECT column_id, contains_null, contains_nan, min_value, max_value
                FROM ducklake_table_column_stats WHERE table_id = %s ORDER BY column_id""".formatted(tableId));
    }

    private List<List<String>> snapshot(long snapshotId)
    {
        return catalog.metastoreRows(
                """
                SELECT s.schema_version, s.next_catalog_id, s.next_file_id, c.changes_made
                FROM ducklake_snapshot s JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id
                WHERE s.snapshot_id = %s""".formatted(snapshotId));
    }

    /**
     * The table and the newest snapshot of the catalog, as they are before a statement changes them.
     */
    private CatalogState catalogState(String table)
    {
        List<String> tableRow = getOnlyElement(catalog.metastoreRows(
                """
                SELECT t.table_id, p.partition_id
                FROM ducklake_table t JOIN ducklake_partition_info p ON t.table_id = p.table_id
                WHERE t.table_name = '%s' AND t.end_snapshot IS NULL AND p.end_snapshot IS NULL""".formatted(table)));
        long tableId = Long.parseLong(tableRow.get(0));
        List<String> snapshotRow = getOnlyElement(catalog.metastoreRows(
                "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1"));
        Map<String, Long> columnIds = catalog.metastoreRows("SELECT column_name, column_id FROM ducklake_column WHERE table_id = %s AND end_snapshot IS NULL".formatted(tableId)).stream()
                .collect(toImmutableMap(List::getFirst, column -> Long.parseLong(column.get(1))));
        return new CatalogState(
                tableId,
                Long.parseLong(tableRow.get(1)),
                Long.parseLong(snapshotRow.get(0)),
                Long.parseLong(snapshotRow.get(1)),
                Long.parseLong(snapshotRow.get(2)),
                Long.parseLong(snapshotRow.get(3)),
                columnIds);
    }

    private record CatalogState(long tableId, long partitionId, long snapshotId, long schemaVersion, long nextCatalogId, long nextFileId, Map<String, Long> columnIds)
    {
        public long columnId(String name)
        {
            Long columnId = columnIds.get(name);
            assertThat(columnId).as("column id of %s", name).isNotNull();
            return columnId;
        }
    }

    private Path writeWithDuckDb(String table, String directory, @Language("SQL") String query)
            throws SQLException
    {
        Path file = Path.of(catalog.externalParquetFile(table + "/" + directory));
        catalog.executeInDuckDb("COPY (%s) TO '%s' (FORMAT PARQUET)".formatted(query, file));
        return file;
    }

    private String location(Path file)
    {
        return "local:///" + catalog.dataPath().relativize(file);
    }

    private Path tableDirectory(String table)
    {
        return catalog.dataPath().resolve("main").resolve(table);
    }

    private static List<List<String>> ordered(boolean eastFirst, List<List<String>> east, List<List<String>> west)
    {
        if (eastFirst) {
            return ImmutableList.<List<String>>builder().addAll(east).addAll(west).build();
        }
        return ImmutableList.<List<String>>builder().addAll(west).addAll(east).build();
    }

    private static List<String> rowIds(long... rowIds)
    {
        return Arrays.stream(rowIds).mapToObj(Long::toString).collect(toImmutableList());
    }

    /**
     * The values of a catalog row as {@link TestingDuckLakeCatalog#metastoreRows} reads them.
     */
    private static List<String> row(Object... values)
    {
        return Arrays.stream(values)
                .map(value -> value == null ? "<null>" : value.toString())
                .collect(toImmutableList());
    }

    /**
     * What a Parquet file says about itself: its size, the size of its footer, and the compressed
     * size of every top level column, summed over the row groups.
     */
    private record ParquetFile(long size, long footerSize, Map<String, Long> columnSizes)
    {
        public static ParquetFile read(Path file)
                throws IOException
        {
            byte[] bytes = Files.readAllBytes(file);
            int footerSize = ByteBuffer.wrap(bytes, bytes.length - 8, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
            FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(bytes, bytes.length - 8 - footerSize, footerSize));
            Map<String, Long> columnSizes = new HashMap<>();
            for (RowGroup rowGroup : metadata.getRow_groups()) {
                for (ColumnChunk chunk : rowGroup.getColumns()) {
                    columnSizes.merge(chunk.getMeta_data().getPath_in_schema().getFirst(), chunk.getMeta_data().getTotal_compressed_size(), Long::sum);
                }
            }
            return new ParquetFile(bytes.length, footerSize, ImmutableMap.copyOf(columnSizes));
        }

        public long columnSize(String column)
        {
            Long columnSize = columnSizes.get(column);
            assertThat(columnSize).as("size of column %s", column).isNotNull();
            return columnSize;
        }
    }
}
