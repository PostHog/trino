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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.PASSWORD;
import static io.trino.plugin.ducklake.TestingDuckLakeCatalog.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Covers the footer size the catalog records for a data file: what it counts, and the read the
 * connector saves by passing it to the Parquet reader.
 */
final class TestDuckLakeFooterSize
        extends AbstractTestQueryFramework
{
    /**
     * Size of the Parquet postscript: the four byte length of the footer preceding it, followed by
     * the file magic. The catalog records the length of the footer without them.
     */
    private static final int POST_SCRIPT_SIZE = 8;
    /**
     * How much of the tail the Parquet reader fetches when it has to guess where the footer
     * starts, from {@code ParquetReaderOptions}. A footer longer than this costs a second read.
     */
    private static final long DEFAULT_FOOTER_READ_SIZE = 48 * 1024;
    /**
     * Files at or below this size are read whole into memory rather than by ranges, from
     * {@code ParquetReaderOptions}. The wide fixture has to be larger for a footer read to show up
     * on its own.
     */
    private static final long SMALL_FILE_THRESHOLD = 3 * 1024 * 1024;

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
                // several columns, so that the footer is far larger than the postscript and the
                // two candidate meanings of footer_size differ visibly
                """
                CREATE TABLE footers (
                    c_bigint BIGINT,
                    c_varchar VARCHAR,
                    c_double DOUBLE,
                    c_date DATE,
                    c_boolean BOOLEAN)
                """,
                """
                INSERT INTO footers
                SELECT i, 'value ' || i, i / 3.0, DATE '2020-01-01' + CAST(i AS INTEGER), i % 2 = 0
                FROM range(50000) t(i)
                """,

                // Many columns over many small row groups, so that the footer carries metadata for
                // thousands of column chunks and grows past the length the reader guesses. The
                // values vary so that the file stays larger than the small file threshold.
                "CALL lake.set_option('parquet_row_group_size', 2048)",
                wideTableDefinition("wide"),
                wideTableInsert("wide"),
                wideTableDefinition("wide_without_footer_size"),
                wideTableInsert("wide_without_footer_size"),
                wideTableDefinition("wide_wrong_footer_size"),
                wideTableInsert("wide_wrong_footer_size"));

        // The catalog of an older DuckLake, or one written by something that does not record the
        // footer size, leaves it unset. The reader then finds the footer on its own.
        updateCatalog(
                catalog,
                """
                UPDATE ducklake_data_file f SET footer_size = NULL
                FROM ducklake_table t
                WHERE t.table_id = f.table_id AND t.table_name = 'wide_without_footer_size'""");
    }

    private static String wideTableDefinition(String tableName)
    {
        StringBuilder columns = new StringBuilder();
        for (int column = 0; column < 20; column++) {
            columns.append(", c%s VARCHAR".formatted(column));
        }
        return "CREATE TABLE %s (id BIGINT%s)".formatted(tableName, columns);
    }

    private static String wideTableInsert(String tableName)
    {
        StringBuilder values = new StringBuilder();
        for (int column = 0; column < 20; column++) {
            values.append(", 'column %s value ' || ((i * 7919 + %s) %% 100000)".formatted(column, column));
        }
        return "INSERT INTO %s SELECT i%s FROM range(120000) t(i)".formatted(tableName, values);
    }

    @Test
    void testFooterSizeExcludesThePostScript()
            throws Exception
    {
        List<DataFile> dataFiles = dataFiles("footers");
        assertThat(dataFiles).isNotEmpty();
        for (DataFile dataFile : dataFiles) {
            assertThat(dataFile.footerSize())
                    .describedAs("footer size of %s", dataFile.path())
                    .isEqualTo(metadataLength(dataFile.path()))
                    .isLessThan(dataFile.fileSizeBytes() - POST_SCRIPT_SIZE);
        }
    }

    /**
     * A footer longer than the length the reader guesses is fetched once, at the size the catalog
     * records, rather than fetched short and fetched again.
     */
    @Test
    void testLongFooterIsReadOnce()
            throws Exception
    {
        DataFile dataFile = onlyDataFile("wide");
        assertThat(dataFile.footerSize())
                .describedAs("the fixture only tests anything if its footer outgrows the guess")
                .isGreaterThan(DEFAULT_FOOTER_READ_SIZE);
        assertThat(dataFile.fileSizeBytes())
                .describedAs("a file read whole into memory never reads its footer on its own")
                .isGreaterThan(SMALL_FILE_THRESHOLD);

        assertQueryStats(
                onlyFooterRead(),
                // no row group holds a negative id, so all of them are pruned by the statistics in
                // the footer and the only bytes read are the footer itself
                "SELECT max(id) FROM wide WHERE id < 0",
                queryStats -> assertThat(queryStats.getPhysicalInputDataSize().toBytes())
                        .isEqualTo(dataFile.footerSize() + POST_SCRIPT_SIZE),
                result -> assertThat(result.getOnlyValue()).isNull());
    }

    /**
     * Without a footer size in the catalog the reader falls back to guessing, which costs it the
     * discarded first read. Nothing else changes, which is what makes the size a hint.
     */
    @Test
    void testLongFooterWithoutCatalogSizeIsReadTwice()
            throws Exception
    {
        DataFile dataFile = onlyDataFile("wide_without_footer_size");
        assertThat(dataFile.footerSize()).isEqualTo(0);
        long footerBytes = metadataLength(dataFile.path()) + POST_SCRIPT_SIZE;

        assertQueryStats(
                onlyFooterRead(),
                "SELECT max(id) FROM wide_without_footer_size WHERE id < 0",
                queryStats -> assertThat(queryStats.getPhysicalInputDataSize().toBytes())
                        .isEqualTo(DEFAULT_FOOTER_READ_SIZE + footerBytes),
                result -> assertThat(result.getOnlyValue()).isNull());
    }

    /**
     * A footer size that does not match the file is a hint like any other: too small and the
     * reader reads again, too large for the file and the connector leaves it alone. Either way the
     * reader locates the footer from the file itself and the query is unaffected.
     */
    @Test
    void testWrongFooterSizeStillReads()
            throws Exception
    {
        @Language("SQL") String query = "SELECT id, c0, c19 FROM wide_wrong_footer_size WHERE id = 4242";
        Object expected = computeActual("SELECT id, c0, c19 FROM wide WHERE id = 4242");

        setFooterSize("wide_wrong_footer_size", "1");
        assertThat(computeActual(query)).isEqualTo(expected);

        setFooterSize("wide_wrong_footer_size", "file_size_bytes * 10");
        assertThat(computeActual(query)).isEqualTo(expected);

        setFooterSize("wide_wrong_footer_size", "-1");
        assertThat(computeActual(query)).isEqualTo(expected);
    }

    /**
     * The size is only a hint, so a file with a long footer reads the same either way.
     */
    @Test
    void testLongFooterFilesReadTheSame()
    {
        assertQuery("SELECT count(*), max(id) FROM wide", "VALUES (120000, 119999)");
        assertThat(computeActual("SELECT id, c0, c19 FROM wide ORDER BY id LIMIT 100"))
                .isEqualTo(computeActual("SELECT id, c0, c19 FROM wide_without_footer_size ORDER BY id LIMIT 100"));
        assertThat(computeActual("SELECT id, c0, c19 FROM wide WHERE id = 4242"))
                .isEqualTo(computeActual("SELECT id, c0, c19 FROM wide_without_footer_size WHERE id = 4242"));
    }

    /**
     * Pruning whole files by their catalog statistics is off, so the scan opens the file and reads
     * its footer instead of skipping it, and reads nothing else because every row group is pruned.
     */
    private Session onlyFooterRead()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", "file_statistics_pruning_enabled", "false")
                .build();
    }

    private DataFile onlyDataFile(String tableName)
            throws Exception
    {
        List<DataFile> dataFiles = dataFiles(tableName);
        assertThat(dataFiles).hasSize(1);
        return dataFiles.getFirst();
    }

    private List<DataFile> dataFiles(String tableName)
            throws SQLException, IOException
    {
        @Language("SQL") String sql =
                """
                SELECT f.path, f.file_size_bytes, f.footer_size
                FROM ducklake_data_file f
                JOIN ducklake_table t ON t.table_id = f.table_id
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL""".formatted(tableName);
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<DataFile> dataFiles = new ArrayList<>();
            while (resultSet.next()) {
                dataFiles.add(new DataFile(
                        locate(resultSet.getString("path")),
                        resultSet.getLong("file_size_bytes"),
                        resultSet.getLong("footer_size")));
            }
            return dataFiles;
        }
    }

    private void setFooterSize(String tableName, String footerSizeExpression)
            throws SQLException
    {
        updateCatalog(catalog,
                """
                UPDATE ducklake_data_file f SET footer_size = %s
                FROM ducklake_table t
                WHERE t.table_id = f.table_id AND t.table_name = '%s'""".formatted(footerSizeExpression, tableName));
    }

    private static void updateCatalog(TestingDuckLakeCatalog catalog, @Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    /**
     * Finds the data file under the data directory by its file name. The catalog stores paths
     * relative to the table, which this test has no reason to reassemble.
     */
    private Path locate(String path)
            throws IOException
    {
        String fileName = Path.of(path).getFileName().toString();
        try (Stream<Path> files = Files.walk(catalog.dataPath())) {
            return files.filter(file -> file.getFileName().toString().equals(fileName))
                    .collect(onlyElement());
        }
    }

    /**
     * The length the Parquet postscript of the file reports for the metadata preceding it.
     */
    private static long metadataLength(Path path)
            throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            byte[] postScript = new byte[POST_SCRIPT_SIZE];
            file.seek(file.length() - POST_SCRIPT_SIZE);
            file.readFully(postScript);
            assertThat(new String(postScript, 4, 4, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
            return ByteBuffer.wrap(postScript).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
    }

    private record DataFile(Path path, long fileSizeBytes, long footerSize) {}
}
