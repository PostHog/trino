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
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.RevisionedCatalogStore;
import io.trino.spi.catalog.RevisionedCatalogStore.CatalogSnapshot;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.catalogstore.posthog.CatalogVersions.computeCatalogVersion;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * The reader half of the shared store: a coordinator that holds a read-only role, follows what the
 * publisher committed, and refuses a state it cannot read completely.
 */
@TestInstance(PER_CLASS)
final class TestPostHogManagedCatalogStore
{
    private static final String PUBLISHER = "publisher-1";

    private TestingCatalogStoreDatabase database;

    @BeforeAll
    void startDatabase()
    {
        database = new TestingCatalogStoreDatabase();
    }

    @AfterAll
    void stopDatabase()
    {
        database.close();
        database = null;
    }

    @Test
    void testFollowsPublishedRevisions()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        RevisionedCatalogStore store = managedStore(cellId);

        assertThat(store.currentRevision()).isEqualTo(0);
        assertThat(store.fetchSnapshot().catalogs()).isEmpty();

        long first = publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of("tpch.splits-per-node", "2"));
        assertThat(store.currentRevision()).isEqualTo(first);

        CatalogSnapshot snapshot = store.fetchSnapshot();
        assertThat(snapshot.revision()).isEqualTo(first);
        CatalogProperties catalog = getOnlyElement(snapshot.catalogs());
        assertThat(catalog.name()).isEqualTo(new CatalogName("org_17"));
        assertThat(catalog.connectorName()).isEqualTo(new ConnectorName("tpch"));
        assertThat(catalog.properties()).containsExactlyEntriesOf(ImmutableMap.of("tpch.splits-per-node", "2"));

        long second = publisher.removeCatalog("op-2", "org_17");
        assertThat(second).isGreaterThan(first);
        assertThat(store.fetchSnapshot().catalogs()).isEmpty();
        assertThat(store.fetchSnapshot().revision()).isEqualTo(second);
    }

    /**
     * Secret references are the reason properties are stored verbatim: the store must not resolve
     * them, and the version must stay the hash of exactly what was published.
     */
    @Test
    void testPropertiesAndVersionsSurviveUnchanged()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        Map<String, String> properties = ImmutableMap.of(
                "connection-password", "${ENV:WAREHOUSE_PASSWORD}",
                "unicode-property", "wärehöuse-ünïcode-✓",
                "empty-property", "");
        publisher.publishCatalog("op-1", "org_29", "tpch", properties);

        CatalogProperties catalog = getOnlyElement(managedStore(cellId).fetchSnapshot().catalogs());

        assertThat(catalog.properties()).containsExactlyInAnyOrderEntriesOf(properties);
        assertThat(catalog.version()).isEqualTo(computeCatalogVersion(new CatalogName("org_29"), new ConnectorName("tpch"), properties));
    }

    @Test
    void testUnreadableRowFailsTheSnapshotInsteadOfDeletingACatalog()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());
        publisher.publishCatalog("op-2", "org_18", "tpch", ImmutableMap.of());

        database.execute("UPDATE trino_catalogs SET properties = 'not json' WHERE cell_id = '%s' AND catalog_name = 'org_18'".formatted(cellId));

        RevisionedCatalogStore store = managedStore(cellId);
        assertThatThrownBy(store::fetchSnapshot)
                .hasMessageContaining("Catalog 'org_18' of cell '%s' cannot be read".formatted(cellId));
        // The catalog is still published; nothing about this state says it was removed
        assertThat(rowCount(cellId)).isEqualTo(2);
    }

    @Test
    void testIncompleteSnapshotIsRefused()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        // A row that was written without going through the publisher: the count no longer describes the rows
        database.execute(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties)
                VALUES ('%s', 'org_18', 'tpch', 'version', '{}')
                """.formatted(cellId));

        assertThatThrownBy(managedStore(cellId)::fetchSnapshot)
                .hasMessageContaining("is incomplete")
                .hasMessageContaining("declares 1 catalogs but 2 were read");
    }

    @Test
    void testCatalogsWithoutWriterStateAreNotReportedAsEmpty()
    {
        String cellId = newCell();
        publisher(cellId, 1).createTables();
        database.execute(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties)
                VALUES ('%s', 'org_17', 'tpch', 'version', '{}')
                """.formatted(cellId));

        assertThatThrownBy(managedStore(cellId)::fetchSnapshot)
                .hasMessageContaining("has 1 catalogs but no published writer state");
    }

    /**
     * A publisher that adopts a store which already has catalogs must record what is there. Seeding
     * zero would publish a revision claiming the cell is empty.
     */
    @Test
    void testAdoptingAnExistingStoreKeepsItsCatalogs()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        publisher.createTables();
        database.execute(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties)
                VALUES ('%s', 'org_17', 'tpch', '%s', '{}')
                """.formatted(cellId, computeCatalogVersion(new CatalogName("org_17"), new ConnectorName("tpch"), ImmutableMap.of())));

        publisher.publishCatalog("op-1", "org_18", "tpch", ImmutableMap.of());

        CatalogSnapshot snapshot = managedStore(cellId).fetchSnapshot();
        assertThat(snapshot.catalogs().stream().map(catalog -> catalog.name().toString()).toList())
                .containsExactlyInAnyOrder("org_17", "org_18");
    }

    @Test
    void testFencedWriterCannotPublish()
    {
        String cellId = newCell();
        TestingCatalogPublisher first = publisher(cellId, 1);
        first.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        TestingCatalogPublisher second = new TestingCatalogPublisher(database, cellId, 2, "publisher-2");
        second.takeOverFrom(1);
        long afterTakeover = second.publishCatalog("op-2", "org_18", "tpch", ImmutableMap.of());

        assertThatThrownBy(() -> first.publishCatalog("op-3", "org_19", "tpch", ImmutableMap.of()))
                .isInstanceOf(TestingCatalogPublisher.FencedWriterException.class)
                .hasMessageContaining("is fenced");
        assertThat(managedStore(cellId).currentRevision()).isEqualTo(afterTakeover);
    }

    /**
     * An identical retry of a mutation whose response was lost must not publish twice.
     */
    @Test
    void testRepeatedOperationIsRecognizedAsAReplay()
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId, 1);
        long revision = publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        assertThat(publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of())).isEqualTo(revision);
        assertThat(managedStore(cellId).currentRevision()).isEqualTo(revision);

        assertThatThrownBy(() -> publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of("changed", "intent")))
                .hasMessageContaining("was recorded with a different intent");
    }

    /**
     * A managed reader is started against a database in which it may not create anything. It uses
     * its own fixture, because the point is that these tables are missing until a publisher
     * creates them.
     */
    @Test
    void testReadOnlyStoreRejectsMutationsAndRunsNoDdl()
    {
        try (TestingCatalogStoreDatabase untouchedDatabase = new TestingCatalogStoreDatabase()) {
            String cellId = newCell();
            CatalogStore store = getOnlyElement(new PostHogCatalogStorePlugin().getCatalogStoreFactories())
                    .create(ImmutableMap.<String, String>builder()
                            .putAll(untouchedDatabase.storeProperties(cellId))
                            .put("catalog-store.read-only", "true")
                            .buildOrThrow());

            assertThat(tableExists(untouchedDatabase, "trino_catalogs")).isFalse();

            assertThatThrownBy(() -> store.createCatalogProperties(new CatalogName("org_17"), new ConnectorName("tpch"), ImmutableMap.of()))
                    .hasMessageContaining("Catalog store is read-only");
            assertThatThrownBy(() -> store.removeCatalog(new CatalogName("org_17")))
                    .hasMessageContaining("Catalog store is read-only");
        }
    }

    private String newCell()
    {
        return "cell" + randomNameSuffix();
    }

    private TestingCatalogPublisher publisher(String cellId, long epoch)
    {
        TestingCatalogPublisher publisher = new TestingCatalogPublisher(database, cellId, epoch, PUBLISHER);
        publisher.createTables();
        return publisher;
    }

    private RevisionedCatalogStore managedStore(String cellId)
    {
        CatalogStore store = getOnlyElement(new PostHogCatalogStorePlugin().getCatalogStoreFactories())
                .create(ImmutableMap.<String, String>builder()
                        .putAll(database.storeProperties(cellId))
                        .put("catalog-store.read-only", "true")
                        .buildOrThrow());
        return (RevisionedCatalogStore) store;
    }

    private long rowCount(String cellId)
    {
        return scalar(database, "SELECT count(*) FROM trino_catalogs WHERE cell_id = '%s'".formatted(cellId));
    }

    private static boolean tableExists(TestingCatalogStoreDatabase database, String tableName)
    {
        return scalar(database, "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '%s'".formatted(tableName)) > 0;
    }

    private static long scalar(TestingCatalogStoreDatabase database, String sql)
    {
        try (Connection connection = database.openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next()).isTrue();
            return resultSet.getLong(1);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to inspect the catalog store", e);
        }
    }
}
