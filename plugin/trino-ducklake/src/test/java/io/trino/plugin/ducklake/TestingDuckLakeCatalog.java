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

import org.intellij.lang.annotations.Language;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.ducklake.metastore.DuckLakeMetastoreConnectionFactory.APPLICATION_NAME;
import static java.lang.String.format;
import static org.testcontainers.postgresql.PostgreSQLContainer.POSTGRESQL_PORT;

/**
 * A DuckLake catalog backed by a PostgreSQL testcontainer, with fixtures written by real DuckDB
 * through the {@code ducklake} extension.
 */
public final class TestingDuckLakeCatalog
        implements Closeable
{
    public static final String USER = "test";
    public static final String PASSWORD = "test";
    private static final String DATABASE = "lakedb";
    private static final String COMMIT_CONFLICT_MESSAGE = "Failed to commit DuckLake transaction";
    private static final int COMMIT_ATTEMPTS = 5;
    private static final Duration COMMIT_RETRY_DELAY = Duration.ofMillis(100);

    private final PostgreSQLContainer dockerContainer;
    private final Path dataPath;

    public TestingDuckLakeCatalog()
    {
        dockerContainer = new PostgreSQLContainer("postgres:16")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        dockerContainer.start();
        try {
            dataPath = Files.createTempDirectory("ducklake-data");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path dataPath()
    {
        return dataPath;
    }

    /**
     * Returns the path of a Parquet file to write into the data directory of the given table of
     * the {@code main} schema, creating the directory. Files written there can be registered into
     * the table with the DuckDB {@code ducklake_add_data_files} function.
     */
    public String externalParquetFile(String tableDirectory)
    {
        Path directory = dataPath.resolve("main").resolve(tableDirectory);
        try {
            Files.createDirectories(directory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return directory.resolve("external.parquet").toString();
    }

    public String jdbcUrl()
    {
        return format(
                "jdbc:postgresql://%s:%s/%s",
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(POSTGRESQL_PORT),
                DATABASE);
    }

    /**
     * Opens a DuckDB connection with this DuckLake catalog attached as {@code lake} and set as
     * the current database. Requires network access to download the DuckDB extensions.
     */
    public Connection openDuckDbConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSTALL ducklake");
            statement.execute("INSTALL postgres");
            statement.execute("LOAD ducklake");
            statement.execute("LOAD postgres");
            // DATA_INLINING_ROW_LIMIT 0 forces DuckDB to write Parquet files instead of inlining
            // small inserts into the catalog database
            statement.execute(format(
                    "ATTACH 'ducklake:postgres:host=%s port=%s user=%s password=%s dbname=%s' AS lake (DATA_PATH '%s/', DATA_INLINING_ROW_LIMIT 0)",
                    dockerContainer.getHost(),
                    dockerContainer.getMappedPort(POSTGRESQL_PORT),
                    USER,
                    PASSWORD,
                    DATABASE,
                    dataPath));
            statement.execute("USE lake");
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    /**
     * Number of connections the DuckLake connector currently holds to the catalog database,
     * counted on the server. Connections opened by the connector are recognized by the
     * {@code application_name} it reports.
     */
    public long connectorConnectionCount()
    {
        String sql = format(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = '%s' AND application_name = '%s'",
                DATABASE,
                APPLICATION_NAME);
        try (Connection connection = DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            verify(resultSet.next(), "no result returned by %s", sql);
            return resultSet.getLong(1);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Runs the statements in DuckDB, in order, running one again when its DuckLake commit lost a
     * race against another writer.
     * <p>
     * Every change to a DuckLake catalog claims the next snapshot, so a commit conflicts with any
     * other commit that started from the same one. The connector retries such a commit; DuckDB
     * reports it and leaves the connection it came from unusable. Tests write to one catalog from
     * both engines at the same time, so a statement that loses the race runs again on a new
     * connection, which is what a writer facing the conflict would do. Options an earlier
     * statement set survive the new connection, because DuckLake keeps them in the catalog rather
     * than on the connection.
     */
    public void executeInDuckDb(@Language("SQL") String... statements)
            throws SQLException
    {
        Connection connection = openDuckDbConnection();
        try {
            for (String sql : statements) {
                for (int attempt = 1; ; attempt++) {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(sql);
                        break;
                    }
                    catch (SQLException e) {
                        if (attempt == COMMIT_ATTEMPTS || !nullToEmpty(e.getMessage()).contains(COMMIT_CONFLICT_MESSAGE)) {
                            throw e;
                        }
                    }
                    connection.close();
                    sleepBeforeRetry(attempt);
                    connection = openDuckDbConnection();
                }
            }
        }
        finally {
            connection.close();
        }
    }

    /**
     * Runs the statements against the catalog database itself, rather than through DuckDB. Used to
     * put the catalog into a state no engine writes, such as the one an older DuckLake version
     * would have left.
     */
    public void executeInMetastore(@Language("SQL") String... statements)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void sleepBeforeRetry(int attempt)
    {
        try {
            Thread.sleep(COMMIT_RETRY_DELAY.toMillis() * attempt);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
