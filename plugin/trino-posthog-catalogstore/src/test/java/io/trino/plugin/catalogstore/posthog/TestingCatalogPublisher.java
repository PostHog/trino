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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.json.JsonCodec;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.plugin.catalogstore.posthog.CatalogVersions.computeCatalogVersion;
import static java.util.Objects.requireNonNull;

/**
 * The catalog publisher of a cell, as the controller that owns the shared catalog store implements
 * it. Trino coordinators never publish: this is a test double of the external writer, so that the
 * managed-reader side can be tested against the schema and the transaction shape that the real
 * publisher is contracted to use.
 *
 * <p>Every mutation runs in one transaction that locks the writer-state row, verifies the exact
 * epoch and owner, applies the definition change, advances the revision and records the operation.
 * A writer whose epoch or identity no longer matches the stored one is fenced and cannot publish;
 * taking over is a separate, explicit step.
 */
public final class TestingCatalogPublisher
{
    private static final JsonCodec<Map<String, String>> PROPERTIES_CODEC = mapJsonCodec(String.class, String.class);

    private static final String CREATE_CATALOGS_TABLE_SQL =
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

    private static final String CREATE_WRITER_STATE_TABLE_SQL =
            """
            CREATE TABLE IF NOT EXISTS trino_catalog_writer_state (
                cell_id         varchar     NOT NULL,
                revision        bigint      NOT NULL,
                writer_epoch    bigint      NOT NULL,
                writer_identity varchar     NOT NULL,
                catalog_count   integer     NOT NULL,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (cell_id)
            )
            """;

    private static final String CREATE_JOURNAL_TABLE_SQL =
            """
            CREATE TABLE IF NOT EXISTS trino_catalog_journal (
                cell_id         varchar     NOT NULL,
                revision        bigint      NOT NULL,
                operation_id    varchar     NOT NULL,
                operation       varchar     NOT NULL,
                catalog_name    varchar     NOT NULL,
                catalog_version varchar,
                payload_hash    varchar     NOT NULL,
                writer_epoch    bigint      NOT NULL,
                committed_at    timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (cell_id, revision)
            )
            """;

    private static final String CREATE_JOURNAL_OPERATION_INDEX_SQL =
            """
            CREATE UNIQUE INDEX IF NOT EXISTS trino_catalog_journal_operation
            ON trino_catalog_journal (cell_id, operation_id)
            """;

    private final TestingCatalogStoreDatabase database;
    private final String cellId;
    private final long epoch;
    private final String identity;

    public TestingCatalogPublisher(TestingCatalogStoreDatabase database, String cellId, long epoch, String identity)
    {
        this.database = requireNonNull(database, "database is null");
        this.cellId = requireNonNull(cellId, "cellId is null");
        this.epoch = epoch;
        this.identity = requireNonNull(identity, "identity is null");
    }

    /**
     * Creates the tables of the shared store. The managed reader never does this, which is why the
     * publisher has to.
     */
    public void createTables()
    {
        try (Connection connection = database.openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(CREATE_CATALOGS_TABLE_SQL);
            statement.execute(CREATE_WRITER_STATE_TABLE_SQL);
            statement.execute(CREATE_JOURNAL_TABLE_SQL);
            statement.execute(CREATE_JOURNAL_OPERATION_INDEX_SQL);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create the shared catalog store tables", e);
        }
    }

    /**
     * @return the revision this mutation committed
     */
    public long publishCatalog(String operationId, String catalogName, String connectorName, Map<String, String> properties)
    {
        return publish(operationId, "ADD_OR_REPLACE", catalogName, Optional.of(new Definition(connectorName, properties)));
    }

    /**
     * @return the revision this mutation committed
     */
    public long removeCatalog(String operationId, String catalogName)
    {
        return publish(operationId, "REMOVE", catalogName, Optional.empty());
    }

    /**
     * Takes writer ownership over, which is the only way an epoch ever moves. It locks the same
     * row an in-flight mutation holds, so a delayed write of the previous owner either committed
     * before this or fails its epoch check afterwards.
     */
    public void takeOverFrom(long previousEpoch)
    {
        try (Connection connection = database.openConnection()) {
            connection.setAutoCommit(false);
            try {
                WriterState state = lockWriterState(connection);
                if (state.epoch() != previousEpoch) {
                    throw new IllegalStateException("Expected epoch %s but the store is at %s".formatted(previousEpoch, state.epoch()));
                }
                if (epoch <= previousEpoch) {
                    throw new IllegalStateException("Taking over requires a higher epoch than %s".formatted(previousEpoch));
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE trino_catalog_writer_state SET writer_epoch = ?, writer_identity = ?, updated_at = now() WHERE cell_id = ?")) {
                    statement.setLong(1, epoch);
                    statement.setString(2, identity);
                    statement.setString(3, cellId);
                    statement.executeUpdate();
                }
                connection.commit();
            }
            catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to take over the catalog writer of cell " + cellId, e);
        }
    }

    private long publish(String operationId, String operation, String catalogName, Optional<Definition> definition)
    {
        try (Connection connection = database.openConnection()) {
            connection.setAutoCommit(false);
            try {
                insertInitialWriterState(connection);
                WriterState state = lockWriterState(connection);
                if (state.epoch() != epoch || !state.identity().equals(identity)) {
                    throw new FencedWriterException("Writer %s of epoch %s is fenced; the store is owned by %s of epoch %s"
                            .formatted(identity, epoch, state.identity(), state.epoch()));
                }
                String payloadHash = payloadHash(operation, catalogName, definition);
                Optional<Long> replayed = findRecordedOperation(connection, operationId, payloadHash);
                if (replayed.isPresent()) {
                    connection.commit();
                    return replayed.get();
                }
                applyMutation(connection, operation, catalogName, definition);
                long revision = advanceWriterState(connection);
                recordOperation(connection, revision, operationId, operation, catalogName, definition, payloadHash);
                connection.commit();
                return revision;
            }
            catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to publish %s of catalog %s".formatted(operation, catalogName), e);
        }
    }

    private void insertInitialWriterState(Connection connection)
            throws SQLException
    {
        // An existing store is adopted with the catalogs it already has; seeding zero would describe a state that was never published
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO trino_catalog_writer_state (cell_id, revision, writer_epoch, writer_identity, catalog_count)
                SELECT ?, 0, ?, ?, count(*) FROM trino_catalogs WHERE cell_id = ?
                ON CONFLICT (cell_id) DO NOTHING
                """)) {
            statement.setString(1, cellId);
            statement.setLong(2, epoch);
            statement.setString(3, identity);
            statement.setString(4, cellId);
            statement.executeUpdate();
        }
    }

    private WriterState lockWriterState(Connection connection)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT revision, writer_epoch, writer_identity FROM trino_catalog_writer_state WHERE cell_id = ? FOR UPDATE")) {
            statement.setString(1, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("No writer state for cell " + cellId);
                }
                return new WriterState(resultSet.getLong("revision"), resultSet.getLong("writer_epoch"), resultSet.getString("writer_identity"));
            }
        }
    }

    private Optional<Long> findRecordedOperation(Connection connection, String operationId, String payloadHash)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT revision, payload_hash FROM trino_catalog_journal WHERE cell_id = ? AND operation_id = ?")) {
            statement.setString(1, cellId);
            statement.setString(2, operationId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                String recordedHash = resultSet.getString("payload_hash");
                if (!recordedHash.equals(payloadHash)) {
                    throw new IllegalStateException("Operation %s was recorded with a different intent".formatted(operationId));
                }
                return Optional.of(resultSet.getLong("revision"));
            }
        }
    }

    private void applyMutation(Connection connection, String operation, String catalogName, Optional<Definition> definition)
            throws SQLException
    {
        if (definition.isEmpty()) {
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM trino_catalogs WHERE cell_id = ? AND catalog_name = ?")) {
                statement.setString(1, cellId);
                statement.setString(2, catalogName);
                statement.executeUpdate();
            }
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties, updated_at)
                VALUES (?, ?, ?, ?, ?, now())
                ON CONFLICT (cell_id, catalog_name) DO UPDATE SET
                    connector_name = excluded.connector_name,
                    catalog_version = excluded.catalog_version,
                    properties = excluded.properties,
                    updated_at = now()
                """)) {
            statement.setString(1, cellId);
            statement.setString(2, catalogName);
            statement.setString(3, definition.get().connectorName());
            statement.setString(4, catalogVersion(catalogName, definition.get()));
            statement.setString(5, PROPERTIES_CODEC.toJson(definition.get().properties()));
            statement.executeUpdate();
        }
    }

    private long advanceWriterState(Connection connection)
            throws SQLException
    {
        // The count is taken inside the mutating transaction, so it always describes the revision it is stored with
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE trino_catalog_writer_state
                SET revision = revision + 1,
                    catalog_count = (SELECT count(*) FROM trino_catalogs WHERE cell_id = ?),
                    updated_at = now()
                WHERE cell_id = ?
                RETURNING revision
                """)) {
            statement.setString(1, cellId);
            statement.setString(2, cellId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("Writer state of cell " + cellId + " disappeared");
                }
                return resultSet.getLong("revision");
            }
        }
    }

    private void recordOperation(
            Connection connection,
            long revision,
            String operationId,
            String operation,
            String catalogName,
            Optional<Definition> definition,
            String payloadHash)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO trino_catalog_journal (cell_id, revision, operation_id, operation, catalog_name, catalog_version, payload_hash, writer_epoch)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setString(1, cellId);
            statement.setLong(2, revision);
            statement.setString(3, operationId);
            statement.setString(4, operation);
            statement.setString(5, catalogName);
            statement.setString(6, definition.map(value -> catalogVersion(catalogName, value)).orElse(null));
            statement.setString(7, payloadHash);
            statement.setLong(8, epoch);
            statement.executeUpdate();
        }
    }

    private static String catalogVersion(String catalogName, Definition definition)
    {
        return computeCatalogVersion(new CatalogName(catalogName), new ConnectorName(definition.connectorName()), definition.properties()).toString();
    }

    private static String payloadHash(String operation, String catalogName, Optional<Definition> definition)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putUnencodedChars("catalog-mutation-hash");
        hashString(hasher, operation);
        hashString(hasher, catalogName);
        if (definition.isEmpty()) {
            hasher.putInt(-1);
            return hasher.hash().toString();
        }
        hashString(hasher, definition.get().connectorName());
        hasher.putInt(definition.get().properties().size());
        ImmutableSortedMap.copyOf(definition.get().properties()).forEach((key, value) -> {
            hashString(hasher, key);
            hashString(hasher, value);
        });
        return hasher.hash().toString();
    }

    private static void hashString(Hasher hasher, String value)
    {
        hasher.putInt(value.length());
        hasher.putUnencodedChars(value);
    }

    public static class FencedWriterException
            extends RuntimeException
    {
        public FencedWriterException(String message)
        {
            super(message);
        }
    }

    private record Definition(String connectorName, Map<String, String> properties)
    {
        private Definition
        {
            requireNonNull(connectorName, "connectorName is null");
            properties = Map.copyOf(requireNonNull(properties, "properties is null"));
        }
    }

    private record WriterState(long revision, long epoch, String identity) {}
}
