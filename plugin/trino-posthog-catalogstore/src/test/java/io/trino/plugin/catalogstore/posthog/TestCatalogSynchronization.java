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
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.CatalogStoreManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.CatalogSyncResource;
import io.trino.server.CatalogSyncResource.CatalogSyncStatus;
import io.trino.server.ServerConfig;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
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
     * The control case, and the behavior every existing deployment keeps: without
     * {@code catalog.sync.enabled} a running coordinator does not pick up a catalog that appears in
     * the store after it started.
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
            assertThat(status.notReadyReason()).contains("not enabled");
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
            assertThat(catalogSyncStatus(coordinator).lastFailure()).contains("cannot be read");
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
            assertThat(status.lastFailure()).isNotNull();
            assertThat(coordinator.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
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
        return publisher;
    }

    private static CatalogSyncStatus catalogSyncStatus(DistributedQueryRunner coordinator)
    {
        return coordinator.getCoordinator().getInstance(Key.get(CatalogSyncResource.class)).catalogSyncStatus();
    }

    private DistributedQueryRunner managedCoordinator(String cellId)
            throws Exception
    {
        return coordinator(cellId, true);
    }

    private DistributedQueryRunner coordinator(String cellId, boolean synchronizing)
            throws Exception
    {
        Map<String, String> storeProperties = ImmutableMap.<String, String>builder()
                .putAll(database.storeProperties(cellId))
                .put("catalog-store.read-only", "true")
                .buildOrThrow();
        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setWorkerCount(0)
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

    private static class TestingCatalogStoreModule
            extends AbstractConfigurationAwareModule
    {
        private final Map<String, String> storeProperties;

        public TestingCatalogStoreModule(Map<String, String> storeProperties)
        {
            this.storeProperties = ImmutableMap.copyOf(requireNonNull(storeProperties, "storeProperties is null"));
        }

        @Override
        protected void setup(Binder binder)
        {
            if (buildConfigObject(ServerConfig.class).isCoordinator()) {
                install(new RegisterCatalogStoreFactoryModule(storeProperties));
            }
        }
    }

    /**
     * A server loads its catalog store before any plugin is installed, so the factory of this
     * plugin is registered with the engine directly instead of through
     * {@code QueryRunner.installPlugin}.
     */
    private static class RegisterCatalogStoreFactoryModule
            extends AbstractConfigurationAwareModule
    {
        private final Map<String, String> storeProperties;

        public RegisterCatalogStoreFactoryModule(Map<String, String> storeProperties)
        {
            this.storeProperties = ImmutableMap.copyOf(requireNonNull(storeProperties, "storeProperties is null"));
        }

        @Override
        protected void setup(Binder binder) {}

        @Provides
        @Singleton
        public PreconfiguredCatalogStoreFactory createCatalogStoreFactory(CatalogStoreManager catalogStoreManager)
        {
            PreconfiguredCatalogStoreFactory factory = new PreconfiguredCatalogStoreFactory(storeProperties);
            catalogStoreManager.addCatalogStoreFactory(factory);
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
