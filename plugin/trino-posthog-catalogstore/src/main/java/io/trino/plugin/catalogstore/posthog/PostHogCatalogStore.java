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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.plugin.catalogstore.posthog.CatalogVersions.computeCatalogVersion;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A {@link CatalogStore} that keeps dynamically created catalogs in a PostgreSQL table, so that
 * catalogs survive a coordinator restart and are shared by every coordinator of a cell. The table
 * is owned by this plugin and created on startup if it does not exist:
 *
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS trino_catalogs (
 *     cell_id         varchar     NOT NULL,
 *     catalog_name    varchar     NOT NULL,
 *     connector_name  varchar     NOT NULL,
 *     catalog_version varchar     NOT NULL,
 *     properties      text        NOT NULL,
 *     updated_at      timestamptz NOT NULL DEFAULT now(),
 *     PRIMARY KEY (cell_id, catalog_name)
 * )
 * }</pre>
 *
 * <p>Every statement is scoped by {@code catalog-store.cell-id}: several cells can share one
 * database without ever seeing, replacing or removing each other's catalogs.
 *
 * <p>The {@code properties} column holds the raw property map of the catalog as a JSON object.
 * Values are stored verbatim, including unresolved secret references such as
 * {@code ${ENV:PASSWORD}}, because Trino resolves those per node when the connector is created.
 */
public class PostHogCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(PostHogCatalogStore.class);

    private static final JsonCodec<Map<String, String>> PROPERTIES_CODEC = mapJsonCodec(String.class, String.class);

    private static final String CREATE_TABLE_SQL =
            """
            CREATE TABLE IF NOT EXISTS trino_catalogs (
                cell_id         varchar     NOT NULL,
                catalog_name    varchar     NOT NULL,
                connector_name  varchar     NOT NULL,
                catalog_version varchar     NOT NULL,
                properties      text        NOT NULL,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (cell_id, catalog_name)
            )
            """;

    private static final String SELECT_CATALOGS_SQL =
            """
            SELECT catalog_name, connector_name, catalog_version, properties
            FROM trino_catalogs
            WHERE cell_id = ?
            """;

    private static final String UPSERT_CATALOG_SQL =
            """
            INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties, updated_at)
            VALUES (?, ?, ?, ?, ?, now())
            ON CONFLICT (cell_id, catalog_name) DO UPDATE SET
                connector_name = excluded.connector_name,
                catalog_version = excluded.catalog_version,
                properties = excluded.properties,
                updated_at = now()
            """;

    private static final String DELETE_CATALOG_SQL =
            """
            DELETE FROM trino_catalogs
            WHERE cell_id = ? AND catalog_name = ?
            """;

    private final String cellId;
    private final PostHogCatalogStoreConnectionFactory connectionFactory;

    @Inject
    public PostHogCatalogStore(PostHogCatalogStoreConfig config, PostHogCatalogStoreConnectionFactory connectionFactory)
    {
        requireNonNull(config, "config is null");
        this.cellId = requireNonNull(config.getCellId(), "cellId is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        createTable();
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        ImmutableList.Builder<StoredCatalog> catalogs = ImmutableList.builder();
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(SELECT_CATALOGS_SQL)) {
            statement.setString(1, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String catalogName = resultSet.getString("catalog_name");
                    try {
                        catalogs.add(new DatabaseStoredCatalog(
                                new CatalogName(catalogName),
                                new ConnectorName(resultSet.getString("connector_name")),
                                new CatalogVersion(resultSet.getString("catalog_version")),
                                resultSet.getString("properties")));
                    }
                    catch (RuntimeException e) {
                        // A single unusable row must not keep the healthy catalogs of this cell from loading
                        log.error(e, "Skipping unreadable catalog '%s' of cell '%s'", catalogName, cellId);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to load catalogs of cell '%s'".formatted(cellId), e);
        }
        return catalogs.build();
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        // Intentionally does not write to the store; the engine calls addOrReplaceCatalog once the catalog is created
        return new CatalogProperties(
                catalogName,
                computeCatalogVersion(catalogName, connectorName, properties),
                connectorName,
                ImmutableMap.copyOf(properties));
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        // Properties are stored exactly as given, so that secret references are resolved by each node and never by the store
        String properties = PROPERTIES_CODEC.toJson(catalogProperties.properties());
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(UPSERT_CATALOG_SQL)) {
            statement.setString(1, cellId);
            statement.setString(2, catalogProperties.name().toString());
            statement.setString(3, catalogProperties.connectorName().toString());
            statement.setString(4, catalogProperties.version().toString());
            statement.setString(5, properties);
            statement.executeUpdate();
        }
        catch (SQLException e) {
            log.error(e, "Could not store catalog '%s' of cell '%s'", catalogProperties.name(), cellId);
            // don't expose exception to end user
            throw new TrinoException(CATALOG_STORE_ERROR, "Could not store catalog properties");
        }
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        try (Connection connection = connectionFactory.openConnection();
                PreparedStatement statement = connection.prepareStatement(DELETE_CATALOG_SQL)) {
            statement.setString(1, cellId);
            statement.setString(2, catalogName.toString());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            log.error(e, "Could not remove catalog '%s' of cell '%s'", catalogName, cellId);
            // don't expose exception to end user
            throw new TrinoException(CATALOG_STORE_ERROR, "Could not remove catalog properties");
        }
    }

    private void createTable()
    {
        try (Connection connection = connectionFactory.openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE_SQL);
        }
        catch (SQLException e) {
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to create the catalog store table", e);
        }
    }

    private record DatabaseStoredCatalog(CatalogName name, ConnectorName connectorName, CatalogVersion version, String properties)
            implements StoredCatalog
    {
        private DatabaseStoredCatalog
        {
            requireNonNull(name, "name is null");
            requireNonNull(connectorName, "connectorName is null");
            requireNonNull(version, "version is null");
            requireNonNull(properties, "properties is null");
        }

        @Override
        public CatalogProperties loadProperties()
        {
            Map<String, String> catalogProperties;
            try {
                catalogProperties = PROPERTIES_CODEC.fromJson(properties);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(CATALOG_STORE_ERROR, "Invalid properties stored for catalog '%s'".formatted(name), e);
            }
            return new CatalogProperties(name, version, connectorName, ImmutableMap.copyOf(catalogProperties));
        }
    }
}
