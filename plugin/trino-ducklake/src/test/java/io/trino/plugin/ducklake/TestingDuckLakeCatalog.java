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
import java.sql.SQLException;
import java.sql.Statement;

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

    public void executeInDuckDb(@Language("SQL") String... statements)
            throws SQLException
    {
        try (Connection connection = openDuckDbConnection();
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
