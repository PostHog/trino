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

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.testing.TestingNames.randomNameSuffix;

/**
 * An isolated PostgreSQL fixture for the database a cell shares between its coordinators.
 */
public final class TestingCatalogStoreDatabase
        implements AutoCloseable
{
    private static final String DATABASE = "catalogs";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private final PostgreSQLContainer container;
    private final String connectionUrl;
    private final String user;
    private final String password;
    private final String schema;

    public TestingCatalogStoreDatabase()
    {
        String externalUrl = System.getProperty("catalogstore.test.jdbc-url");
        if (externalUrl != null) {
            validateExternalUrl(externalUrl);
            container = null;
            user = System.getProperty("catalogstore.test.jdbc-user", USER);
            password = System.getProperty("catalogstore.test.jdbc-password", PASSWORD);
            schema = "catalogstore_test_" + randomNameSuffix();
            connectionUrl = externalUrl + "?currentSchema=" + schema;
            execute("CREATE SCHEMA " + schema);
            return;
        }
        container = new PostgreSQLContainer("postgres:16")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        container.start();
        connectionUrl = container.getJdbcUrl();
        user = USER;
        password = PASSWORD;
        schema = null;
    }

    static void validateExternalUrl(String externalUrl)
    {
        checkArgument(externalUrl.startsWith("jdbc:postgresql://"), "Test database requires a local PostgreSQL JDBC URL");
        URI address;
        try {
            address = URI.create(externalUrl.substring("jdbc:".length()));
        }
        catch (IllegalArgumentException _) {
            throw new IllegalArgumentException("Invalid local test database URL");
        }
        checkArgument(address.getHost() != null && Set.of("localhost", "127.0.0.1", "[::1]").contains(address.getHost()), "Test database must use a loopback host");
        checkArgument(address.getUserInfo() == null && address.getRawQuery() == null && address.getRawFragment() == null, "Test database URL must not contain credentials or options");
        checkArgument(address.getPath() != null && address.getPath().matches("/[A-Za-z_][A-Za-z0-9_]*"), "Test database URL requires one database name");
    }

    /**
     * Configuration of a catalog store owning the catalogs of the given cell in this database.
     */
    public Map<String, String> storeProperties(String cellId)
    {
        return ImmutableMap.<String, String>builder()
                .put("catalog-store.cell-id", cellId)
                .put("catalog-store.connection-url", connectionUrl)
                .put("catalog-store.connection-user", user)
                .put("catalog-store.connection-password", password)
                .buildOrThrow();
    }

    /**
     * Runs a statement against the database directly, bypassing the catalog store, so that tests
     * can plant rows no correct store would ever write.
     */
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(connectionUrl, user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute catalog store fixture statement", e);
        }
    }

    public Connection openConnection()
            throws SQLException
    {
        return DriverManager.getConnection(connectionUrl, user, password);
    }

    @Override
    public void close()
    {
        if (container != null) {
            container.stop();
        }
        else {
            execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }
}
