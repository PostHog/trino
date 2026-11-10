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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_CONFLICT;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_CHANGE_TYPE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * An ingest pipeline on DuckDB and a transform pipeline on Trino commit to one DuckLake catalog, so
 * a Trino commit regularly finds that another writer committed first.
 * <p>
 * A commit claims its snapshot before it runs its action, so a foreign commit started during the
 * action waits for it instead of landing in between. A foreign commit that lands after the
 * statement read its snapshot but before the commit starts is therefore written before the commit
 * is called, and the commit is re-based onto it.
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
        executeInDuckDb("INSERT INTO " + other + " VALUES (7)");
        AtomicInteger attempts = new AtomicInteger();
        long committed = registerDataFile(readSnapshotId, table, dataFile, attempts);

        assertThat(attempts).hasValue(1);
        // the foreign snapshot took the identifier after the one the statement read, and this commit took the next one
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
        executeInDuckDb("ALTER TABLE " + table + " ADD COLUMN z INTEGER");
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> registerDataFile(readSnapshotId, table, dataFile, attempts))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_COMMIT_CONFLICT.toErrorCode()))
                .hasMessageContaining("Conflicting concurrent commit to the DuckLake catalog")
                .hasMessageContaining("but another transaction altered it");

        // the conflict is not retried, and the attempt was rolled back, so no row of the statement reached the catalog
        assertThat(attempts).hasValue(1);
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
        executeInDuckDb("INSERT INTO " + table + " VALUES (7)");
        AtomicInteger attempts = new AtomicInteger();
        registerDataFile(readSnapshotId, table, dataFile, attempts);

        assertThat(attempts).hasValue(1);
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
        // a real foreign snapshot, rewritten to record a change this connector cannot interpret
        executeInDuckDb("INSERT INTO " + other + " VALUES (7)");
        executeInCatalogDatabase(
                """
                UPDATE ducklake_snapshot_changes SET changes_made = 'teleported_table:99'
                WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot_changes)""");
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> registerDataFile(readSnapshotId, table, dataFile, attempts))
                .isInstanceOf(TrinoException.class)
                .matches(failure -> ((TrinoException) failure).getErrorCode().equals(DUCKLAKE_UNSUPPORTED_CHANGE_TYPE.toErrorCode()))
                .hasMessageContaining("recorded the DuckLake change type 'teleported_table'")
                .hasMessageContaining("Upgrade the DuckLake connector");

        // reading the row again would reach the same conclusion, so the failure is not retried
        assertThat(attempts).hasValue(1);
        assertThat(dataFileCount(table)).isEqualTo(0);
    }

    /**
     * The commit holds its snapshot from before its action runs until it ends, so another writer
     * cannot take the identifier in between. The foreign commit waits, loses the claim once the first
     * commit lands, and is replayed onto the snapshot after it. The action of the first commit runs
     * once, which is the saving over a commit that finds out only at the end that it lost.
     */
    @Test
    void testForeignCommitWaitsWhileACommitHoldsItsSnapshot()
            throws Exception
    {
        String table = "claim_" + randomNameSuffix();
        String other = "claim_other_" + randomNameSuffix();
        createTable(table);
        createTable(other);
        long tableId = metastore.findTable(metastore.currentSnapshotId(), "main", table).orElseThrow().tableId();
        long otherId = metastore.findTable(metastore.currentSnapshotId(), "main", other).orElseThrow().tableId();

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger actions = new AtomicInteger();
        AtomicInteger foreignActions = new AtomicInteger();
        ExecutorService executor = newSingleThreadExecutor();
        try {
            AtomicReference<Future<Long>> foreign = new AtomicReference<>();
            long committed = metastore.commit(readSnapshotId, commit -> {
                actions.incrementAndGet();
                commit.recordInsert(tableId);
                Future<Long> started = executor.submit(() -> metastore.commit(readSnapshotId, foreignCommit -> {
                    foreignActions.incrementAndGet();
                    foreignCommit.recordInsert(otherId);
                    return foreignCommit.effectiveSnapshotId();
                }));
                foreign.set(started);
                // the foreign commit cannot finish while this transaction holds the snapshot
                assertThatThrownBy(() -> started.get(500, MILLISECONDS)).isInstanceOf(TimeoutException.class);
                return commit.effectiveSnapshotId();
            });
            assertThat(actions).hasValue(1);
            assertThat(committed).isEqualTo(readSnapshotId + 1);
            // the foreign commit lost the claim, and took the identifier after this commit
            assertThat(foreign.get().get(30, SECONDS)).isEqualTo(readSnapshotId + 2);
            assertThat(foreignActions).hasValue(1);
            assertThat(metastore.currentSnapshotId()).isEqualTo(readSnapshotId + 2);
        }
        finally {
            executor.shutdownNow();
        }
    }

    /**
     * DuckDB is the other writer in production. It waits on the claimed snapshot as well, retries
     * once the first commit lands, and its insert lands after that commit.
     */
    @Test
    void testDuckDbWriteWaitsWhileACommitHoldsItsSnapshot()
            throws Exception
    {
        String table = "claim_duck_" + randomNameSuffix();
        String other = "claim_duck_other_" + randomNameSuffix();
        createTable(table);
        createTable(other);
        long tableId = metastore.findTable(metastore.currentSnapshotId(), "main", table).orElseThrow().tableId();

        long readSnapshotId = metastore.currentSnapshotId();
        AtomicInteger actions = new AtomicInteger();
        ExecutorService executor = newSingleThreadExecutor();
        try {
            // the first DuckDB statement on the thread loads the extension, which must not be what makes the insert slow
            executor.submit(() -> executeInDuckDb("SELECT count(*) FROM " + other)).get(60, SECONDS);
            AtomicReference<Future<?>> foreign = new AtomicReference<>();
            long committed = metastore.commit(readSnapshotId, commit -> {
                actions.incrementAndGet();
                commit.recordInsert(tableId);
                Future<?> started = executor.submit(() -> executeInDuckDb("INSERT INTO " + other + " VALUES (7)"));
                foreign.set(started);
                assertThatThrownBy(() -> started.get(500, MILLISECONDS)).isInstanceOf(TimeoutException.class);
                return commit.effectiveSnapshotId();
            });

            assertThat(actions).hasValue(1);
            assertThat(committed).isEqualTo(readSnapshotId + 1);
            foreign.get().get(60, SECONDS);
            assertThat(metastore.currentSnapshotId()).isEqualTo(readSnapshotId + 2);
            assertThat(duckDbRows("SELECT x FROM " + other)).isEqualTo(List.of(7L));
        }
        finally {
            executor.shutdownNow();
        }
    }

    /**
     * A commit whose action changes nothing writes no snapshot, so the snapshot it claimed is
     * released rather than left behind as an empty one.
     */
    @Test
    void testCommitThatChangesNothingLeavesNoSnapshot()
            throws Exception
    {
        String table = "empty_" + randomNameSuffix();
        createTable(table);
        long snapshotId = metastore.currentSnapshotId();

        long committed = metastore.commit(snapshotId, commit -> {
            assertThat(commit.findTable("main", table)).isPresent();
            return commit.effectiveSnapshotId();
        });

        assertThat(committed).isEqualTo(snapshotId);
        assertThat(metastore.currentSnapshotId()).isEqualTo(snapshotId);
        long laterSnapshots = inCatalogDatabase("SELECT count(*) FROM ducklake_snapshot WHERE snapshot_id > " + snapshotId, resultSet -> {
            verify(resultSet.next());
            return resultSet.getLong(1);
        });
        assertThat(laterSnapshots).isEqualTo(0);
        // the identifier is free for the next commit
        executeInDuckDb("INSERT INTO " + table + " VALUES (1)");
        assertThat(metastore.currentSnapshotId()).isEqualTo(snapshotId + 1);
    }

    /**
     * The check inside the commit sees every file recorded under the names to register: the ones
     * the check read before the commit, and the ones recorded after, including a file that moved to
     * the files waiting to be deleted, which has no snapshot of its own.
     */
    @Test
    void testCommitSeesFilesRecordedBeforeAndAfterTheCheckRead()
            throws Exception
    {
        String table = "recorded_" + randomNameSuffix();
        createTable(table);
        DataFile before = stageDataFile(table, 1);
        DataFile after = stageDataFile(table, 2);
        DataFile scheduled = stageDataFile(table, 3);
        registerDataFile(metastore.currentSnapshotId(), table, before, new AtomicInteger());

        DuckLakeCommit.FileRegistrationState state = metastore.fileRegistrationState(ImmutableSet.of(before.path(), after.path(), scheduled.path()));
        assertThat(state.recordedFiles()).extracting(DuckLakeCommit.RecordedFile::path).containsExactly(before.path());

        registerDataFile(metastore.currentSnapshotId(), table, after, new AtomicInteger());
        // an invented file id, and a directory other than the table's with the file name of a file to register
        String scheduledPath = "elsewhere/" + scheduled.path();
        executeInCatalogDatabase("INSERT INTO ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, schedule_start) VALUES (999999999, '%s', true, now())".formatted(scheduledPath));
        try {
            List<DuckLakeCommit.RecordedFile> recorded = metastore.commit(metastore.currentSnapshotId(), commit -> commit.recordedFiles(state));
            assertThat(recorded).extracting(DuckLakeCommit.RecordedFile::path).containsExactlyInAnyOrder(before.path(), after.path(), scheduledPath);
        }
        finally {
            executeInCatalogDatabase("DELETE FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = 999999999");
        }
    }

    @Test
    void testRangeReplacementRejectsConcurrentInsertsAndDeletes()
            throws Exception
    {
        String tableName = "range_conflict_" + randomNameSuffix();
        createTable(tableName);
        executeInDuckDb("INSERT INTO " + tableName + " VALUES (7), (8)");
        long tableId = metastore.findTable(metastore.currentSnapshotId(), "main", tableName).orElseThrow().tableId();
        long snapshotBeforeInsert = metastore.currentSnapshotId();
        executeInDuckDb("INSERT INTO " + tableName + " VALUES (9)");
        assertThatThrownBy(() -> metastore.commit(snapshotBeforeInsert, commit -> {
            commit.setTableTag(tableId, "range_receipt", Optional.of("should_rollback"));
            commit.recordDelete(tableId);
            commit.recordInsert(tableId);
            return null;
        })).isInstanceOf(TrinoException.class).hasMessageContaining("another transaction inserted into it");

        long snapshotBeforeDelete = metastore.currentSnapshotId();
        executeInDuckDb("DELETE FROM " + tableName + " WHERE x = 7");
        assertThatThrownBy(() -> metastore.commit(snapshotBeforeDelete, commit -> {
            commit.setTableTag(tableId, "range_receipt", Optional.of("should_rollback"));
            commit.recordDelete(tableId);
            commit.recordInsert(tableId);
            return null;
        })).isInstanceOf(TrinoException.class).hasMessageContaining("another transaction deleted from it");
        assertThat(duckDbRows("SELECT x FROM " + tableName + " ORDER BY x")).containsExactly(8L, 9L);
        assertThat(catalog.scalar("SELECT count(*)::VARCHAR FROM __ducklake_metadata_lake.ducklake_tag WHERE object_id = " + tableId + " AND key = 'range_receipt'")).isEqualTo("0");
    }

    /**
     * Registers an already written data file into a table the way {@code finishInsert} does.
     * Returns the snapshot the commit ended up on.
     */
    private long registerDataFile(long readSnapshotId, String tableName, DataFile dataFile, AtomicInteger attempts)
    {
        return metastore.commit(readSnapshotId, commit -> {
            attempts.incrementAndGet();
            DuckLakeCommit.TableIdentity table = commit.findTable("main", tableName).orElseThrow();
            DuckLakeCommit.TableStatsRow stats = commit.tableStats(table.tableId())
                    .orElseGet(() -> new DuckLakeCommit.TableStatsRow(0, 0, 0));
            commit.insertDataFiles(table.tableId(), ImmutableList.of(new DuckLakeCommit.NewDataFile(
                    commit.allocateFileId(),
                    new DuckLakeCommit.DataFileRow(
                            dataFile.path(),
                            dataFile.recordCount(),
                            dataFile.fileSizeBytes(),
                            dataFile.footerSize(),
                            stats.nextRowId(),
                            OptionalLong.empty()),
                    ImmutableList.of(),
                    ImmutableList.of())));
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
        Path staged = tableDirectory(tableName).resolve("staged_" + randomNameSuffix() + ".parquet");
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
