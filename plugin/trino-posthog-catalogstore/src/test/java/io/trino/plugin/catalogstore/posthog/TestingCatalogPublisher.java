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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.google.common.primitives.UnsignedBytes;
import io.airlift.json.JsonCodec;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.plugin.catalogstore.posthog.CatalogVersions.computeCatalogVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
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

    /**
     * The canonical schema both repositories test against, so the reader's expectations and the
     * publisher's DDL cannot drift apart. See {@code shared-catalog-store-schema.sql}.
     */
    public static final String SHARED_SCHEMA_RESOURCE = "shared-catalog-store-schema.sql";

    /**
     * Property keys are ordered by their UTF-8 bytes. Java's natural string order compares UTF-16
     * code units, which puts characters outside the basic plane in a different place than every
     * byte-oriented language does.
     */
    private static final Comparator<String> UTF_8_ORDER =
            comparing(value -> value.getBytes(UTF_8), UnsignedBytes.lexicographicalComparator());

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
            for (String sql : sharedSchemaStatements()) {
                statement.execute(sql);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create the shared catalog store tables", e);
        }
    }

    /**
     * The statements of the canonical schema file, in order. A statement ends at a line that
     * contains nothing but a semicolon, so the file stays readable and copyable into psql.
     */
    public static List<String> sharedSchemaStatements()
    {
        String schema;
        try {
            schema = Resources.toString(Resources.getResource(SHARED_SCHEMA_RESOURCE), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + SHARED_SCHEMA_RESOURCE, e);
        }
        return Splitter.onPattern("(?m)^;$").splitToStream(schema)
                .map(String::strip)
                .filter(statement -> !statement.isEmpty())
                .collect(toImmutableList());
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
    public void takeOver()
    {
        try (Connection connection = database.openConnection()) {
            connection.setAutoCommit(false);
            try {
                insertInitialWriterState(connection);
                WriterState state = lockWriterState(connection);
                if (state.epoch() > epoch) {
                    throw new FencedWriterException("Recorded epoch %s is newer than %s".formatted(state.epoch(), epoch));
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
                if (state.epoch() > epoch) {
                    throw new FencedWriterException("Recorded epoch %s is newer than %s".formatted(state.epoch(), epoch));
                }
                if (state.epoch() != epoch || !state.identity().equals(identity)) {
                    // A mutation never claims ownership as a side effect; whoever wants it takes it over
                    throw new FencedWriterException("The store is owned by '%s' at epoch %s, this writer is '%s' at epoch %s"
                            .formatted(state.identity(), state.epoch(), identity, epoch));
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
        // Seeded with the real row count, never a literal zero: a store that already holds catalogs
        // must not be described as holding none. Seeded without an owner, because becoming the writer
        // is an explicit step and never a side effect of seeding or of a mutation.
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO trino_catalog_writer_state (cell_id, revision, writer_epoch, writer_identity, catalog_count)
                SELECT ?, 0, 0, '', count(*) FROM trino_catalogs WHERE cell_id = ?
                ON CONFLICT (cell_id) DO NOTHING
                """)) {
            statement.setString(1, cellId);
            statement.setString(2, cellId);
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

    /**
     * The payload hash of a mutation, as the publisher computes it: SHA-256 over the operation, the
     * catalog name, the connector name and then the properties in key order, each value written as
     * its length in bytes as a big-endian 32-bit integer followed by its UTF-8 bytes, and the
     * property count written the same way. A removal has an empty connector name and no properties.
     *
     * <p>The encoding is defined in bytes rather than in any language's string type on purpose:
     * the controller that publishes and anything that verifies a replay have to arrive at the same
     * value for the same intent, and they are not written in the same language.
     */
    public static String payloadHash(String operation, String catalogName, String connectorName, Map<String, String> properties)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hashString(hasher, operation);
        hashString(hasher, catalogName);
        hashString(hasher, connectorName);
        hashLength(hasher, properties.size());
        // Ordered by UTF-8 bytes, not by Java's UTF-16 code units: the two disagree for characters
        // outside the basic plane, and the publisher orders them the first way
        ImmutableSortedMap.copyOf(properties, UTF_8_ORDER)
                .forEach((key, value) -> {
                    hashString(hasher, key);
                    hashString(hasher, value);
                });
        return hasher.hash().toString();
    }

    private static String payloadHash(String operation, String catalogName, Optional<Definition> definition)
    {
        return payloadHash(
                operation,
                catalogName,
                definition.map(Definition::connectorName).orElse(""),
                definition.map(Definition::properties).orElse(ImmutableMap.of()));
    }

    private static void hashString(Hasher hasher, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        hashLength(hasher, bytes.length);
        hasher.putBytes(bytes);
    }

    private static void hashLength(Hasher hasher, int length)
    {
        // Big endian, spelled out: the byte order of a length must not depend on the library that writes it
        hasher.putBytes(new byte[] {
                (byte) (length >>> 24),
                (byte) (length >>> 16),
                (byte) (length >>> 8),
                (byte) length,
        });
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
