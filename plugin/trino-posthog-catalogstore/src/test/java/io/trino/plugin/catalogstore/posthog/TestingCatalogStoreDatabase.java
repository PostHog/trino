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
package io.trino.plugin.catalogstore.posthog;

import com.google.common.collect.ImmutableMap;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * A PostgreSQL testcontainer standing in for the database a cell shares between its coordinators.
 */
public final class TestingCatalogStoreDatabase
        implements AutoCloseable
{
    private static final String DATABASE = "catalogs";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private final PostgreSQLContainer container;

    public TestingCatalogStoreDatabase()
    {
        container = new PostgreSQLContainer("postgres:16")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        container.start();
    }

    /**
     * Configuration of a catalog store owning the catalogs of the given cell in this database.
     */
    public Map<String, String> storeProperties(String cellId)
    {
        return ImmutableMap.<String, String>builder()
                .put("catalog-store.cell-id", cellId)
                .put("catalog-store.connection-url", container.getJdbcUrl())
                .put("catalog-store.connection-user", USER)
                .put("catalog-store.connection-password", PASSWORD)
                .buildOrThrow();
    }

    /**
     * Runs a statement against the database directly, bypassing the catalog store, so that tests
     * can plant rows no correct store would ever write.
     */
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute: " + sql, e);
        }
    }

    @Override
    public void close()
    {
        container.stop();
    }
}
