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
package io.trino.plugin.ducklake.metastore;

import io.trino.plugin.ducklake.DuckLakeConfig;
import io.trino.plugin.ducklake.TestingDuckLakeCatalog;
import io.trino.spi.TrinoException;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_CONFLICT;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_CHANGE_TYPE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * An ingest pipeline on DuckDB and a transform pipeline on Trino commit to one DuckLake catalog, so
 * a Trino commit regularly finds that another writer claimed the next snapshot first.
 * <p>
 * The race is made deterministic by committing the foreign snapshot from DuckDB inside the
 * connector's own commit action, which is the moment between reading the newest snapshot and
 * writing the new one. The first attempt therefore always loses it.
 */
@TestInstance(PER_CLASS)
final class TestDuckLakeConcurrentCommits
{
    private TestingDuckLakeCatalog catalog;
    private JdbcDuckLakeMetastore metastore;

    @BeforeAll
    void setUp()
    {
        catalog = new TestingDuckLakeCatalog();
        try {
            catalog.executeInDuckDb("CREATE TABLE bootstrap (x INTEGER)", "DROP TABLE bootstrap");
        }
        catch (SQLException e) {
            abort("Failed to create a DuckLake catalog with DuckDB (extension download requires network access): " + e);
        }
        metastore = new JdbcDuckLakeMetastore(
                () -> DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD),
                new DuckLakeConfig());
    }

    @AfterAll
    void tearDown()
    {
        metastore = null;
        catalog.close();
        catalog = null;
    }

    /**
     * The foreign snapshot changes a table this statement did not touch, so the commit is re-based
     * onto it and lands with the data file it already wrote.
     */
    @Test
    void testCommitIsRebasedOntoAForeignSnapshotOnAnotherTable()
            throws Exception
    {
        String table = "rebase_" + randomNameSuffix();
        String other = "unrelated_" + randomNameSuffix();
        createTable(table);
        createTable(other);
        DataFile dataFile = stageDataFile(table, 42);

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger attempts = new AtomicInteger();
        long committed = registerDataFile(readSnapshotId, table, dataFile, attempts, () -> executeInDuckDb("INSERT INTO " + other + " VALUES (7)"));

        assertThat(attempts).hasValue(2);
        // the foreign snapshot took the identifier the first attempt wanted, and this commit took the next one
        assertThat(committed).isEqualTo(readSnapshotId + 2);
        assertThat(committed).isEqualTo(metastore.currentSnapshotId());

        // the file was registered once, and DuckDB reads its row through the re-based commit
        assertThat(dataFileCount(table)).isEqualTo(1);
        assertThat(duckDbRows("SELECT x FROM " + table)).isEqualTo(List.of(42L));
        assertThat(duckDbRows("SELECT x FROM " + other)).isEqualTo(List.of(7L));
    }

    /**
     * The foreign snapshot alters the very table this statement writes to, so the data file it wrote
     * no longer describes the table's columns. Re-basing would register it anyway, so the commit
     * fails and says so.
     */
    @Test
    void testCommitFailsWhenAForeignSnapshotAltersTheTargetTable()
            throws Exception
    {
        String table = "conflict_" + randomNameSuffix();
        createTable(table);
        DataFile dataFile = stageDataFile(table, 42);

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> registerDataFile(readSnapshotId, table, dataFile, attempts, () -> executeInDuckDb("ALTER TABLE " + table + " ADD COLUMN z INTEGER")))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_COMMIT_CONFLICT.toErrorCode()))
                .hasMessageContaining("Conflicting concurrent commit to the DuckLake catalog")
                .hasMessageContaining("but another transaction altered it");

        // every attempt was rolled back, so no row of the statement reached the catalog
        assertThat(attempts).hasValue(2);
        assertThat(dataFileCount(table)).isEqualTo(0);
        assertThat(duckDbRows("SELECT x FROM " + table)).isEmpty();
    }

    /**
     * Two writers adding rows to one table is the ordinary case of an ingest pipeline running beside
     * a transform, and DuckLake lets both land.
     */
    @Test
    void testCommitIsRebasedOntoAForeignInsertIntoTheSameTable()
            throws Exception
    {
        String table = "shared_" + randomNameSuffix();
        createTable(table);
        DataFile dataFile = stageDataFile(table, 42);

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger attempts = new AtomicInteger();
        registerDataFile(readSnapshotId, table, dataFile, attempts, () -> executeInDuckDb("INSERT INTO " + table + " VALUES (7)"));

        assertThat(attempts).hasValue(2);
        assertThat(dataFileCount(table)).isEqualTo(2);
        assertThat(duckDbRows("SELECT x FROM " + table + " ORDER BY x")).isEqualTo(List.of(7L, 42L));
    }

    /**
     * A newer DuckLake records a change type this connector has never seen. It cannot tell whether
     * its own commit may land on top of that snapshot, so it fails saying what it read and what to
     * do about it, rather than committing and risking the other writer's work.
     */
    @Test
    void testCommitFailsOnAChangeTypeTheConnectorDoesNotUnderstand()
            throws Exception
    {
        String table = "unknown_" + randomNameSuffix();
        String other = "elsewhere_" + randomNameSuffix();
        createTable(table);
        createTable(other);
        DataFile dataFile = stageDataFile(table, 42);

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> registerDataFile(readSnapshotId, table, dataFile, attempts, () -> {
            // a real foreign snapshot, rewritten to record a change this connector cannot interpret
            executeInDuckDb("INSERT INTO " + other + " VALUES (7)");
            executeInCatalogDatabase(
                    """
                    UPDATE ducklake_snapshot_changes SET changes_made = 'teleported_table:99'
                    WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot_changes)""");
        }))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_UNSUPPORTED_CHANGE_TYPE.toErrorCode()))
                .hasMessageContaining("recorded the DuckLake change type 'teleported_table'")
                .hasMessageContaining("Upgrade the DuckLake connector");

        // reading the row again reaches the same conclusion, so the failure is not retried
        assertThat(attempts).hasValue(2);
        assertThat(dataFileCount(table)).isEqualTo(0);
    }

    /**
     * Registers an already written data file into a table the way {@code finishInsert} does, while
     * {@code foreignCommit} lands a snapshot from DuckDB during the first attempt. Returns the
     * snapshot the commit ended up on.
     */
    private long registerDataFile(long readSnapshotId, String tableName, DataFile dataFile, AtomicInteger attempts, Runnable foreignCommit)
    {
        return metastore.commit(readSnapshotId, commit -> {
            if (attempts.incrementAndGet() == 1) {
                foreignCommit.run();
            }
            DuckLakeCommit.TableIdentity table = commit.findTable("main", tableName).orElseThrow();
            DuckLakeCommit.TableStatsRow stats = commit.tableStats(table.tableId())
                    .orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
            commit.insertDataFile(table.tableId(), commit.allocateFileId(), new DuckLakeCommit.DataFileRow(
                    dataFile.path(),
                    dataFile.recordCount(),
                    dataFile.fileSizeBytes(),
                    dataFile.footerSize(),
                    stats.nextRowId(),
                    OptionalLong.empty()));
            commit.writeTableStats(table.tableId(), new DuckLakeCommit.TableStatsRow(
                    stats.recordCount() + dataFile.recordCount(),
                    stats.nextRowId() + dataFile.recordCount(),
                    stats.fileSizeBytes() + dataFile.fileSizeBytes()));
            commit.recordInsert(table.tableId());
            return commit.effectiveSnapshotId();
        });
    }

    private void createTable(String tableName)
            throws SQLException
    {
        catalog.executeInDuckDb("CREATE TABLE %s (x INTEGER)".formatted(tableName));
    }

    /**
     * Produces a data file lying in the table's directory but registered nowhere, standing in for
     * the file a Trino worker writes before the commit that registers it. DuckDB writes it into a
     * throwaway table of the same shape, so that it carries the DuckLake field ids a reader needs,
     * and the copy is made under the target table where the registered path resolves to it.
     */
    private DataFile stageDataFile(String tableName, int value)
            throws SQLException
    {
        String sourceTable = tableName + "_source";
        catalog.executeInDuckDb(
                "CREATE TABLE %s (x INTEGER)".formatted(sourceTable),
                "INSERT INTO %s VALUES (%s)".formatted(sourceTable, value));

        DataFile source = onlyDataFile(sourceTable);
        Path staged = tableDirectory(tableName).resolve("staged.parquet");
        try {
            Files.createDirectories(staged.getParent());
            Files.copy(tableDirectory(sourceTable).resolve(source.path()), staged, REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // the source table is dropped so that its own file is not counted with the target's rows
        catalog.executeInDuckDb("DROP TABLE " + sourceTable);
        return new DataFile(staged.getFileName().toString(), source.recordCount(), source.fileSizeBytes(), source.footerSize());
    }

    /**
     * Where the data files of a table live, which is the table's path resolved under its schema's.
     * A registered path is relative to it.
     */
    private Path tableDirectory(String tableName)
    {
        @Language("SQL") String sql =
                """
                SELECT s.path AS schema_path, t.path AS table_path
                FROM ducklake_table t
                JOIN ducklake_schema s ON t.schema_id = s.schema_id
                WHERE t.table_name = '%s' AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL""".formatted(tableName);
        return inCatalogDatabase(sql, resultSet -> {
            verify(resultSet.next(), "no table named %s", tableName);
            return catalog.dataPath().resolve(resultSet.getString("schema_path")).resolve(resultSet.getString("table_path"));
        });
    }

    private DataFile onlyDataFile(String tableName)
    {
        @Language("SQL") String sql =
                """
                SELECT f.path, f.record_count, f.file_size_bytes, f.footer_size
                FROM ducklake_data_file f
                JOIN ducklake_table t ON f.table_id = t.table_id
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL""".formatted(tableName);
        return inCatalogDatabase(sql, resultSet -> {
            verify(resultSet.next(), "no data file for table %s", tableName);
            DataFile file = new DataFile(
                    resultSet.getString("path"),
                    resultSet.getLong("record_count"),
                    resultSet.getLong("file_size_bytes"),
                    resultSet.getLong("footer_size"));
            verify(!resultSet.next(), "more than one data file for table %s", tableName);
            return file;
        });
    }

    private long dataFileCount(String tableName)
    {
        @Language("SQL") String sql =
                """
                SELECT count(*) FROM ducklake_data_file f
                JOIN ducklake_table t ON f.table_id = t.table_id
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL AND t.end_snapshot IS NULL""".formatted(tableName);
        return inCatalogDatabase(sql, resultSet -> {
            verify(resultSet.next(), "no result returned by %s", sql);
            return resultSet.getLong(1);
        });
    }

    private <T> T inCatalogDatabase(@Language("SQL") String sql, ResultSetMapper<T> mapper)
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return mapper.map(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeInCatalogDatabase(@Language("SQL") String sql)
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeInDuckDb(@Language("SQL") String sql)
    {
        try {
            catalog.executeInDuckDb(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Long> duckDbRows(@Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = catalog.openDuckDbConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<Long> rows = new ArrayList<>();
            while (resultSet.next()) {
                rows.add(resultSet.getLong(1));
            }
            return rows;
        }
    }

    private record DataFile(String path, long recordCount, long fileSizeBytes, long footerSize) {}

    @FunctionalInterface
    private interface ResultSetMapper<T>
    {
        T map(ResultSet resultSet)
                throws SQLException;
    }
}
