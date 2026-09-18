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
import io.trino.spi.catalog.RevisionedCatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The catalog store of a coordinator whose catalogs are published by an external writer. It never
 * writes: it runs no bootstrap DDL, so it works with a read-only database role, and it rejects
 * every catalog mutation instead of changing definitions other coordinators share.
 *
 * <p>Beside the definitions in {@code trino_catalogs} it reads the state the publisher maintains
 * for the cell:
 *
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS trino_catalog_writer_state (
 *     cell_id         varchar     NOT NULL,
 *     revision        bigint      NOT NULL,
 *     writer_epoch    bigint      NOT NULL,
 *     writer_identity varchar     NOT NULL,
 *     catalog_count   integer     NOT NULL,
 *     updated_at      timestamptz NOT NULL DEFAULT now(),
 *     PRIMARY KEY (cell_id)
 * )
 * }</pre>
 *
 * <p>Those tables belong to the publisher, which creates them, advances {@code revision} in the
 * same transaction as every definition change, and records {@code catalog_count} as the number of
 * rows that revision consists of. This store only reads them; the epoch and the mutation journal
 * are the publisher's concern.
 *
 * <p>Startup still loads catalogs the way the writable store does, including skipping a row it
 * cannot read, so that a coordinator can be started against a cell that no publisher has taken
 * over yet. Everything that follows goes through {@link #fetchSnapshot()}, which refuses an
 * incomplete state instead of reporting it as a smaller set of catalogs.
 */
public class PostHogManagedCatalogStore
        implements CatalogStore, RevisionedCatalogStore
{
    private static final Logger log = Logger.get(PostHogManagedCatalogStore.class);

    private static final JsonCodec<Map<String, String>> PROPERTIES_CODEC = mapJsonCodec(String.class, String.class);

    private static final String SELECT_CATALOGS_SQL =
            """
            SELECT catalog_name, connector_name, catalog_version, properties
            FROM trino_catalogs
            WHERE cell_id = ?
            """;

    private static final String SELECT_REVISION_SQL =
            """
            SELECT revision
            FROM trino_catalog_writer_state
            WHERE cell_id = ?
            """;

    private static final String SELECT_WRITER_STATE_SQL =
            """
            SELECT revision, catalog_count
            FROM trino_catalog_writer_state
            WHERE cell_id = ?
            """;

    private final String cellId;
    private final PostHogCatalogStoreConnectionFactory connectionFactory;
    private final int snapshotTimeoutSeconds;

    @Inject
    public PostHogManagedCatalogStore(PostHogCatalogStoreConfig config, PostHogCatalogStoreConnectionFactory connectionFactory)
    {
        requireNonNull(config, "config is null");
        this.cellId = requireNonNull(config.getCellId(), "cellId is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.snapshotTimeoutSeconds = toIntExact(config.getSnapshotTimeout().roundTo(SECONDS));
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        ImmutableList.Builder<StoredCatalog> catalogs = ImmutableList.builder();
        try (Connection connection = openReaderConnection();
                PreparedStatement statement = prepare(connection, SELECT_CATALOGS_SQL)) {
            statement.setString(1, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String catalogName = resultSet.getString("catalog_name");
                    try {
                        catalogs.add(new DatabaseStoredCatalog(readCatalog(resultSet)));
                    }
                    catch (RuntimeException e) {
                        // A single unusable row must not keep the healthy catalogs of this cell from loading.
                        // What made it unusable is a parse failure that quotes the row, and a catalog
                        // property can hold a credential, so the ordinary line names the failure's types
                        log.error("Skipping unreadable catalog '%s' of cell '%s' (%s)", catalogName, cellId, failureTypes(e));
                        log.debug(e, "Skipping unreadable catalog '%s' of cell '%s'", catalogName, cellId);
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
        throw rejectMutation(catalogName);
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        throw rejectMutation(catalogProperties.name());
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        throw rejectMutation(catalogName);
    }

    /**
     * Revision the publisher of this cell last committed, or empty while no publisher has adopted
     * the cell at all. Empty is not revision zero: an emptied, restored or wrongly addressed store
     * looks exactly like one that was never written, and neither is a desired state. This is polled
     * often, so it reads one small row and nothing else.
     */
    @Override
    public OptionalLong currentRevision()
    {
        try (Connection connection = openReaderConnection();
                PreparedStatement statement = prepare(connection, SELECT_REVISION_SQL)) {
            statement.setString(1, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    // No publisher has ever adopted this cell. Reporting a revision here would make an
                    // emptied, restored or wrongly addressed store look like a published empty state
                    return OptionalLong.empty();
                }
                return OptionalLong.of(resultSet.getLong("revision"));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to read the catalog revision of cell '%s'".formatted(cellId), e);
        }
    }

    /**
     * Reads the revision and every catalog of the cell in one repeatable-read transaction, so that
     * the returned definitions are exactly the ones the revision describes. A row that cannot be
     * read, or a row count that disagrees with the published one, fails the whole snapshot: an
     * incompletely readable state must not be mistaken for a state in which catalogs were deleted.
     */
    @Override
    public CatalogSnapshot fetchSnapshot()
    {
        try (Connection connection = openReaderConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);
            connection.setReadOnly(true);
            try {
                PublishedState publishedState = readPublishedState(connection);
                ImmutableList.Builder<CatalogProperties> catalogs = ImmutableList.builder();
                int rows = 0;
                try (PreparedStatement statement = prepare(connection, SELECT_CATALOGS_SQL)) {
                    statement.setString(1, cellId);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            rows++;
                            catalogs.add(readCatalog(resultSet));
                        }
                    }
                }
                if (publishedState.catalogCount().isEmpty()) {
                    // Nothing was ever published here. Whether the cell is untouched or was emptied
                    // cannot be told apart from this side, so neither is treated as desired state
                    throw new IncompleteSnapshotException(
                            "Cell '%s' has no published writer state, but %s catalogs".formatted(cellId, rows));
                }
                long publishedCount = publishedState.catalogCount().orElseThrow();
                if (publishedCount != rows) {
                    throw new IncompleteSnapshotException("Catalog snapshot of cell '%s' is incomplete: revision %s declares %s catalogs but %s were read"
                            .formatted(cellId, publishedState.revision(), publishedCount, rows));
                }
                return new CatalogSnapshot(publishedState.revision(), catalogs.build());
            }
            finally {
                connection.rollback();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to read the catalog snapshot of cell '%s'".formatted(cellId), e);
        }
    }

    private PublishedState readPublishedState(Connection connection)
            throws SQLException
    {
        try (PreparedStatement statement = prepare(connection, SELECT_WRITER_STATE_SQL)) {
            statement.setString(1, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return new PublishedState(0, OptionalLong.empty());
                }
                return new PublishedState(resultSet.getLong("revision"), OptionalLong.of(resultSet.getLong("catalog_count")));
            }
        }
    }

    private CatalogProperties readCatalog(ResultSet resultSet)
            throws SQLException
    {
        String catalogName = resultSet.getString("catalog_name");
        String connectorName = resultSet.getString("connector_name");
        String catalogVersion = resultSet.getString("catalog_version");
        String properties = resultSet.getString("properties");
        try {
            return new CatalogProperties(
                    new CatalogName(catalogName),
                    new CatalogVersion(catalogVersion),
                    new ConnectorName(connectorName),
                    // Properties are used exactly as stored, so secret references are resolved by this node and never by the store
                    ImmutableMap.copyOf(PROPERTIES_CODEC.fromJson(properties)));
        }
        catch (RuntimeException e) {
            // Never silently drop the row: a catalog that cannot be read is not a catalog that was deleted
            throw new IncompleteSnapshotException("Catalog '%s' of cell '%s' cannot be read".formatted(catalogName, cellId), e);
        }
    }

    /**
     * A failure described by the types it is made of. The messages of the failures this store sees
     * quote the row or the connection they came from, and neither belongs in an ordinary log line;
     * they are logged at debug level instead.
     */
    private static String failureTypes(Throwable failure)
    {
        StringBuilder types = new StringBuilder();
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 5; depth++) {
            if (depth > 0) {
                types.append(" caused by ");
            }
            types.append(current.getClass().getName());
            current = current.getCause() == current ? null : current.getCause();
        }
        return types.toString();
    }

    private static TrinoException rejectMutation(CatalogName catalogName)
    {
        return new TrinoException(NOT_SUPPORTED, "Catalog store is read-only, catalog '%s' cannot be changed by this coordinator".formatted(catalogName));
    }

    /**
     * A connection of this store carries the same budget end to end: connecting, waiting for the
     * socket and executing a statement are all bounded, so a store that stops answering leaves this
     * coordinator with the catalogs it already has instead of a thread that never returns.
     */
    private Connection openReaderConnection()
            throws SQLException
    {
        return connectionFactory.openConnection(OptionalInt.of(snapshotTimeoutSeconds));
    }

    private PreparedStatement prepare(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(snapshotTimeoutSeconds);
        }
        catch (SQLException e) {
            statement.close();
            throw e;
        }
        return statement;
    }

    private record PublishedState(long revision, OptionalLong catalogCount) {}

    private record DatabaseStoredCatalog(CatalogProperties catalogProperties)
            implements StoredCatalog
    {
        private DatabaseStoredCatalog
        {
            requireNonNull(catalogProperties, "catalogProperties is null");
        }

        @Override
        public CatalogName name()
        {
            return catalogProperties.name();
        }

        @Override
        public CatalogProperties loadProperties()
        {
            return catalogProperties;
        }
    }
}
