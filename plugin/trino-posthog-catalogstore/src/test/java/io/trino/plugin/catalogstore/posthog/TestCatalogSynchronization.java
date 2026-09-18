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
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogPruneTask;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.CatalogSyncResource;
import io.trino.server.CatalogSyncResource.CatalogSyncStatus;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Coordinators that follow the catalogs an external publisher commits, while they run. These are
 * real coordinators against a real PostgreSQL store; the publisher is the test double of the
 * controller that owns the store, see {@link TestingCatalogPublisher}.
 */
@TestInstance(PER_CLASS)
final class TestCatalogSynchronization
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
    void testTwoCoordinatorsObserveAdditionsWithoutRestart()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);

        try (DistributedQueryRunner first = managedCoordinator(cellId);
                DistributedQueryRunner second = managedCoordinator(cellId)) {
            assertThat(first.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_17");

            long revision = publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of("tpch.splits-per-node", "2"));

            for (DistributedQueryRunner coordinator : new DistributedQueryRunner[] {first, second}) {
                assertEventually(() -> assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17"));
                assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
                assertEventually(() -> assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(revision));
                assertThat(catalogSyncStatus(coordinator).ready()).isTrue();
            }
        }
    }

    /**
     * A read-only store without synchronization is a valid way to look at a cell, and it is also
     * what a half-configured serving coordinator looks like. It keeps the pre-existing behavior -
     * load once at startup, follow nothing - and says exactly that instead of claiming readiness.
     * A serving deployment has to set both settings.
     */
    @Test
    void testDisabledSynchronizationIgnoresPublishedCatalogs()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);

        try (DistributedQueryRunner coordinator = coordinator(cellId, false)) {
            publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

            Thread.sleep(2_000);
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_17");

            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            assertThat(status.enabled()).isFalse();
            assertThat(status.ready()).isFalse();
            assertThat(status.lastFailure()).isEqualTo("SYNCHRONIZATION_DISABLED");
        }
    }

    @Test
    void testRemovedCatalogIsNoLongerReachable()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);

            long revision = publisher.removeCatalog("op-2", "org_17");

            assertEventually(() -> assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_17"));
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(revision));
        }
    }

    /**
     * A row that cannot be read is corruption, not a deletion. The coordinator keeps every catalog
     * it has and stops claiming readiness until the store is readable again.
     */
    @Test
    void testUnreadableRowNeverRemovesCatalogs()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());

            publisher.publishCatalog("op-2", "org_18", "tpch", ImmutableMap.of());
            database.execute("UPDATE trino_catalogs SET properties = 'not json' WHERE cell_id = '%s' AND catalog_name = 'org_18'".formatted(cellId));

            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isFalse());
            // A bounded category, not the text of the failure: readiness is readable by anyone with
            // management-read access, and store errors can name URLs, properties or credentials
            assertThat(catalogSyncStatus(coordinator).lastFailure()).isEqualTo("SNAPSHOT_INCOMPLETE");
            // The catalog that was fine all along is still there and still works
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17");
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);

            database.execute("UPDATE trino_catalogs SET properties = '{}' WHERE cell_id = '%s' AND catalog_name = 'org_18'".formatted(cellId));
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17", "org_18");
        }
    }

    /**
     * A store that cannot be read at all is the same: last good state stays, readiness goes away.
     */
    @Test
    void testUnreadableStoreMakesReadinessStale()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            Long applied = catalogSyncStatus(coordinator).appliedRevision();

            database.execute("DELETE FROM trino_catalog_writer_state WHERE cell_id = '%s'".formatted(cellId));

            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isFalse());
            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            assertThat(status.appliedRevision()).isEqualTo(applied);
            assertThat(status.lastFailure()).isEqualTo("NOTHING_PUBLISHED");
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
        }
    }

    /**
     * The dangerous version of the same thing: the whole store is gone, so there is nothing left to
     * contradict an empty desired state. A restored-from-scratch or wrongly addressed database must
     * not be able to remove every catalog of a serving coordinator.
     */
    @Test
    void testEmptiedStoreNeverRemovesCatalogs()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            Long applied = catalogSyncStatus(coordinator).appliedRevision();

            database.execute("DELETE FROM trino_catalogs WHERE cell_id = '%s'".formatted(cellId));
            database.execute("DELETE FROM trino_catalog_writer_state WHERE cell_id = '%s'".formatted(cellId));

            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).lastFailure()).isEqualTo("NOTHING_PUBLISHED"));
            Thread.sleep(1_000);

            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            assertThat(status.ready()).isFalse();
            assertThat(status.appliedRevision()).isEqualTo(applied);
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17");
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
        }
    }

    /**
     * A store that answers with an older revision than the one already applied - an old dump, a
     * lagging replica, another cell - is a regression, not a new desired state. The coordinator
     * freezes on what it has.
     */
    @Test
    void testRevisionRegressionFreezesTheAppliedState()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());
        publisher.publishCatalog("op-2", "org_18", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(2L);

            // What restoring an older backup looks like: fewer catalogs, and a revision to match
            database.execute("DELETE FROM trino_catalogs WHERE cell_id = '%s' AND catalog_name = 'org_18'".formatted(cellId));
            database.execute("UPDATE trino_catalog_writer_state SET revision = 1, catalog_count = 1 WHERE cell_id = '%s'".formatted(cellId));

            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).lastFailure()).isEqualTo("REVISION_REGRESSED"));
            Thread.sleep(1_000);

            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            assertThat(status.ready()).isFalse();
            assertThat(status.appliedRevision()).isEqualTo(2L);
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17", "org_18");
            assertThat(coordinator.execute("SELECT count(*) FROM org_18.tiny.nation").getOnlyValue()).isEqualTo(25L);
        }
    }

    /**
     * A catalog whose connector cannot start does not make the revision applied, and it does not
     * cost the coordinator the catalogs that do work.
     */
    @Test
    void testCatalogThatCannotStartIsNotAcknowledged()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());

            long revision = publisher.publishCatalog("op-2", "org_18", "tpch", ImmutableMap.of("unsupported-property", "value"));

            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).lastFailure()).isEqualTo("CATALOGS_NOT_APPLIED"));
            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            assertThat(status.ready()).isFalse();
            assertThat(status.observedRevision()).isEqualTo(revision);
            assertThat(status.appliedRevision()).isNotEqualTo(revision);
            assertThat(status.failedCatalogs()).isEqualTo(1);

            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_18");

            // Repairing the definition is enough; nothing has to be restarted
            long repaired = publisher.publishCatalog("op-3", "org_18", "tpch", ImmutableMap.of());
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(repaired);
            assertThat(coordinator.execute("SELECT count(*) FROM org_18.tiny.nation").getOnlyValue()).isEqualTo(25L);
        }
    }

    @Test
    void testManagedCoordinatorCannotChangeSharedCatalogs()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());

            assertThatThrownBy(() -> coordinator.execute("CREATE CATALOG org_18 USING tpch"))
                    .hasMessageContaining("Catalog store is read-only");
            assertThatThrownBy(() -> coordinator.execute("DROP CATALOG org_17"))
                    .hasMessageContaining("Catalog store is read-only");

            assertThat(coordinator.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17");
            assertThat(catalogSyncStatus(coordinator).ready()).isTrue();
        }
    }

    /**
     * Replacing a catalog gives it a new version and therefore a new handle. A transaction that is
     * already bound to the old one has to keep working on it: it is still registered, its connector
     * is still there, and only once the transaction is over may the old handle be pruned. New work
     * gets the new definition in the meantime.
     */
    @Test
    void testReplacedCatalogKeepsServingAnOpenTransaction()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);
        publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of("tpch.splits-per-node", "2"));

        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());

            TransactionManager transactionManager = coordinator.getTransactionManager();
            TransactionId transactionId = transactionManager.beginTransaction(false);
            CatalogHandle openHandle = transactionManager.getCatalogHandle(transactionId, "org_17").orElseThrow();
            assertThat(transactionManager.getTransactionInfo(transactionId).getRegisteredCatalogs()).contains(openHandle);

            long replaced = publisher.publishCatalog("op-2", "org_17", "tpch", ImmutableMap.of("tpch.splits-per-node", "7"));
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(replaced));

            // The transaction still resolves the catalog it began with, and the connector is still there
            assertThat(transactionManager.getCatalogHandle(transactionId, "org_17")).contains(openHandle);
            assertThat(transactionManager.getCatalogMetadata(transactionId, openHandle)).isNotNull();
            ConnectorServicesProvider connectorServices = coordinator.getCoordinator().getInstance(Key.get(ConnectorServicesProvider.class));
            assertThat(connectorServices.getConnectorServices(openHandle)).isNotNull();

            // While anything that starts now gets the replacement
            CatalogHandle replacedHandle = coordinator.inTransaction(session ->
                    coordinator.getTransactionManager().getCatalogHandle(session.getRequiredTransactionId(), "org_17").orElseThrow());
            assertThat(replacedHandle).isNotEqualTo(openHandle);
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);

            // Pruning cannot take the old connector away while the transaction holds it
            CatalogPruneTask catalogPruneTask = coordinator.getCoordinator().getInstance(Key.get(CatalogPruneTask.class));
            catalogPruneTask.pruneCatalogs();
            assertThat(connectorServices.getConnectorServices(openHandle)).isNotNull();

            getFutureValue(transactionManager.asyncCommit(transactionId));
            catalogPruneTask.pruneCatalogs();
            assertThatThrownBy(() -> connectorServices.getConnectorServices(openHandle))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * Workers receive catalogs through their coordinator, including the ones that were installed
     * from a published revision rather than created here. A distributed query over such a catalog
     * is the only thing that proves it.
     */
    @Test
    void testWorkersRunQueriesOnPublishedCatalogs()
            throws Exception
    {
        String cellId = newCell();
        TestingCatalogPublisher publisher = publisher(cellId);

        try (DistributedQueryRunner coordinator = coordinator(cellId, true, 2)) {
            long revision = publisher.publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).appliedRevision()).isEqualTo(revision));

            assertThat(coordinator.execute("SELECT count(*) FROM org_17.sf1.orders").getOnlyValue()).isEqualTo(1_500_000L);
            assertThat(coordinator.execute(
                    "SELECT count(*) FROM org_17.sf1.orders o JOIN org_17.tiny.customer c ON o.custkey = c.custkey").getOnlyValue())
                    .isEqualTo(14_892L);
        }
    }

    /**
     * A readiness answer describes one process. The identity it is bound to has to change when the
     * process does, and it is not the coordinator id an existing deployment already knows.
     */
    @Test
    void testReadinessIsBoundToTheProcess()
            throws Exception
    {
        String cellId = newCell();
        publisher(cellId).publishCatalog("op-1", "org_17", "tpch", ImmutableMap.of());

        String processId;
        try (DistributedQueryRunner coordinator = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(coordinator).ready()).isTrue());
            CatalogSyncStatus status = catalogSyncStatus(coordinator);
            processId = status.processId();
            assertThat(processId).isNotEmpty();
            assertThat(status.coordinatorId()).isNotEmpty();
            assertThat(status.coordinatorId()).isNotEqualTo(processId);
            assertThat(status.observedRevision()).isEqualTo(status.appliedRevision());
            assertThat(status.failedCatalogs()).isZero();
        }

        try (DistributedQueryRunner restarted = managedCoordinator(cellId)) {
            assertEventually(() -> assertThat(catalogSyncStatus(restarted).ready()).isTrue());
            assertThat(catalogSyncStatus(restarted).processId()).isNotEqualTo(processId);
        }
    }

    private String newCell()
    {
        return "cell" + randomNameSuffix();
    }

    private TestingCatalogPublisher publisher(String cellId)
    {
        TestingCatalogPublisher publisher = new TestingCatalogPublisher(database, cellId, 1, PUBLISHER);
        publisher.createTables();
        publisher.takeOver();
        return publisher;
    }

    private static CatalogSyncStatus catalogSyncStatus(DistributedQueryRunner coordinator)
    {
        return coordinator.getCoordinator().getInstance(Key.get(CatalogSyncResource.class)).catalogSyncStatus();
    }

    private DistributedQueryRunner managedCoordinator(String cellId)
            throws Exception
    {
        return coordinator(cellId, true, 0);
    }

    private DistributedQueryRunner coordinator(String cellId, boolean synchronizing)
            throws Exception
    {
        return coordinator(cellId, synchronizing, 0);
    }

    private DistributedQueryRunner coordinator(String cellId, boolean synchronizing, int workerCount)
            throws Exception
    {
        Map<String, String> storeProperties = ImmutableMap.<String, String>builder()
                .putAll(database.storeProperties(cellId))
                .put("catalog-store.read-only", "true")
                .buildOrThrow();
        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setWorkerCount(workerCount)
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("catalog.store", "posthog")
                        .put("catalog.sync.enabled", String.valueOf(synchronizing))
                        .put("catalog.sync.poll-interval", "200ms")
                        .put("catalog.sync.min-retry-delay", "200ms")
                        .put("catalog.sync.max-retry-delay", "1s")
                        .buildOrThrow())
                .setAdditionalModule(new TestingCatalogStoreModule(storeProperties))
                .setAdditionalSetup(queryRunner -> queryRunner.installPlugin(new TpchPlugin()))
                .build();
    }

    /**
     * A server loads its catalog store before any plugin is installed, so the factory of this
     * plugin is registered with the engine directly instead of through
     * {@code QueryRunner.installPlugin}.
     *
     * <p>The same module instance is installed into the coordinator and into every worker, so it
     * keeps no per-injector state. A worker has no catalog store manager, and then there is nothing
     * to register with.
     */
    private static class TestingCatalogStoreModule
            implements Module
    {
        private final Map<String, String> storeProperties;

        public TestingCatalogStoreModule(Map<String, String> storeProperties)
        {
            this.storeProperties = ImmutableMap.copyOf(requireNonNull(storeProperties, "storeProperties is null"));
        }

        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public PreconfiguredCatalogStoreFactory createCatalogStoreFactory(Optional<CatalogStoreManager> catalogStoreManager)
        {
            PreconfiguredCatalogStoreFactory factory = new PreconfiguredCatalogStoreFactory(storeProperties);
            catalogStoreManager.ifPresent(manager -> manager.addCatalogStoreFactory(factory));
            return factory;
        }
    }

    private static class PreconfiguredCatalogStoreFactory
            implements CatalogStoreFactory
    {
        private final CatalogStoreFactory delegate = getOnlyElement(new PostHogCatalogStorePlugin().getCatalogStoreFactories());
        private final Map<String, String> storeProperties;

        public PreconfiguredCatalogStoreFactory(Map<String, String> storeProperties)
        {
            this.storeProperties = ImmutableMap.copyOf(requireNonNull(storeProperties, "storeProperties is null"));
        }

        @Override
        public String getName()
        {
            return delegate.getName();
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            checkArgument(config.isEmpty(), "expected the server to pass no configuration, got %s", config);
            return delegate.create(storeProperties);
        }
    }
}
