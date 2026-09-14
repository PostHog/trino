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
import io.trino.execution.QueryManager;
import io.trino.metadata.CatalogManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.ServerConfig;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * The reason this catalog store exists: a catalog created at runtime with {@code CREATE CATALOG}
 * must still be there after the coordinator restarts, without anything replaying the creation.
 */
@TestInstance(PER_CLASS)
final class TestPostHogCatalogStoreDurability
{
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
    void testCatalogSurvivesCoordinatorRestart()
            throws Exception
    {
        Map<String, String> storeProperties = database.storeProperties("cell" + randomNameSuffix());

        try (QueryRunner queryRunner = createQueryRunner(storeProperties)) {
            assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_17");

            queryRunner.execute("CREATE CATALOG org_17 USING tpch WITH (\"tpch.splits-per-node\" = '2')");

            assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17");
            assertThat(queryRunner.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);
        }

        // a restarted coordinator, reading the catalog back from the database instead of having it recreated
        try (QueryRunner queryRunner = createQueryRunner(storeProperties)) {
            assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("org_17");
            assertThat(queryRunner.execute("SELECT count(*) FROM org_17.tiny.nation").getOnlyValue()).isEqualTo(25L);

            queryRunner.execute("DROP CATALOG org_17");
        }

        // and a dropped catalog stays dropped
        try (QueryRunner queryRunner = createQueryRunner(storeProperties)) {
            assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_17");
        }
    }

    @Test
    void testCellsDoNotSeeEachOtherCatalogs()
            throws Exception
    {
        Map<String, String> firstCell = database.storeProperties("cell" + randomNameSuffix());
        Map<String, String> secondCell = database.storeProperties("cell" + randomNameSuffix());

        try (QueryRunner queryRunner = createQueryRunner(firstCell)) {
            queryRunner.execute("CREATE CATALOG org_29 USING tpch");
        }

        try (QueryRunner queryRunner = createQueryRunner(secondCell)) {
            assertThat(queryRunner.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("org_29");
        }
    }

    @Test
    void testStandbyLoadsSharedCatalogWithoutCreationReplay()
            throws Exception
    {
        Map<String, String> properties = database.storeProperties("cell" + randomNameSuffix());
        try (QueryRunner blue = createQueryRunner(properties)) {
            blue.execute("CREATE CATALOG shared_catalog USING tpch");
            try (QueryRunner green = createQueryRunner(properties)) {
                assertThat(green.execute("SELECT count(*) FROM shared_catalog.tiny.nation").getOnlyValue()).isEqualTo(25L);
                assertThat(blue.execute("SELECT count(*) FROM shared_catalog.tiny.nation").getOnlyValue()).isEqualTo(25L);
            }
        }
    }

    @Test
    void testRunningStandbyDoesNotDiscoverLateCreation()
            throws Exception
    {
        Map<String, String> properties = database.storeProperties("cell" + randomNameSuffix());
        try (QueryRunner blue = createQueryRunner(properties)) {
            try (QueryRunner green = createQueryRunner(properties)) {
                blue.execute("CREATE CATALOG late_catalog USING tpch");
                assertThat(green.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("late_catalog");
            }
            try (QueryRunner restartedGreen = createQueryRunner(properties)) {
                assertThat(restartedGreen.execute("SELECT count(*) FROM late_catalog.tiny.nation").getOnlyValue()).isEqualTo(25L);
            }
        }
    }

    @Test
    void testDropDoesNotRemoveAnotherRunningCoordinatorsCatalog()
            throws Exception
    {
        Map<String, String> properties = database.storeProperties("cell" + randomNameSuffix());
        try (QueryRunner blue = createQueryRunner(properties)) {
            blue.execute("CREATE CATALOG removed_catalog USING tpch");
            try (QueryRunner green = createQueryRunner(properties)) {
                blue.execute("DROP CATALOG removed_catalog");
                assertThat(green.execute("SELECT count(*) FROM removed_catalog.tiny.nation").getOnlyValue()).isEqualTo(25L);
            }
            try (QueryRunner restartedGreen = createQueryRunner(properties)) {
                assertThat(restartedGreen.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("removed_catalog");
            }
        }
    }

    @Test
    void testDropOfLocallyUnknownCatalogStillDeletesSharedDefinition()
            throws Exception
    {
        Map<String, String> properties = database.storeProperties("cell" + randomNameSuffix());
        try (QueryRunner blue = createQueryRunner(properties)) {
            try (QueryRunner green = createQueryRunner(properties)) {
                blue.execute("CREATE CATALOG late_catalog USING tpch");
                assertThatThrownBy(() -> green.execute("DROP CATALOG late_catalog"))
                        .hasMessageContaining("Catalog 'late_catalog' not found");
                assertThat(blue.execute("SELECT count(*) FROM late_catalog.tiny.nation").getOnlyValue()).isEqualTo(25L);
            }
            try (QueryRunner restartedGreen = createQueryRunner(properties)) {
                assertThat(restartedGreen.execute("SHOW CATALOGS").getOnlyColumnAsSet()).doesNotContain("late_catalog");
            }
        }
    }

    @Test
    void testCompetingCreationReplacesSharedPropertiesButNotExistingConnectors()
            throws Exception
    {
        Map<String, String> properties = database.storeProperties("cell" + randomNameSuffix());
        try (DistributedQueryRunner blue = createQueryRunner(properties);
                DistributedQueryRunner green = createQueryRunner(properties)) {
            blue.execute("CREATE CATALOG shared_catalog USING tpch WITH (\"tpch.splits-per-node\" = '2')");
            green.execute("CREATE CATALOG shared_catalog USING tpch WITH (\"tpch.splits-per-node\" = '7')");
            assertThat(catalogProperties(blue, "shared_catalog")).containsEntry("tpch.splits-per-node", "2");
            assertThat(catalogProperties(green, "shared_catalog")).containsEntry("tpch.splits-per-node", "7");
        }
        try (DistributedQueryRunner restarted = createQueryRunner(properties)) {
            assertThat(catalogProperties(restarted, "shared_catalog")).containsEntry("tpch.splits-per-node", "7");
        }
    }

    @Test
    void testFailedStartupCatalogRemainsNamedAndCreationDoesNotRepairIt()
            throws Exception
    {
        String cellId = "cell" + randomNameSuffix();
        Map<String, String> properties = database.storeProperties(cellId);
        try (QueryRunner initial = createQueryRunner(properties)) {
            initial.execute("CREATE CATALOG broken_catalog USING tpch");
        }
        database.execute("UPDATE trino_catalogs SET properties = '{\"unsupported-property\":\"value\"}' WHERE cell_id = '%s' AND catalog_name = 'broken_catalog'".formatted(cellId));
        try (QueryRunner restarted = createQueryRunner(properties)) {
            assertThat(restarted.execute("SHOW CATALOGS").getOnlyColumnAsSet()).contains("broken_catalog");
            assertThat(restarted.execute("SELECT state FROM system.metadata.catalogs WHERE catalog_name = 'broken_catalog'").getOnlyValue()).isEqualTo("FAILING");
            assertThatThrownBy(() -> restarted.execute("CREATE CATALOG IF NOT EXISTS broken_catalog USING tpch"))
                    .hasMessageContaining("Catalog 'broken_catalog' failed to initialize and is disabled");
            assertThat(restarted.execute("SELECT state FROM system.metadata.catalogs WHERE catalog_name = 'broken_catalog'").getOnlyValue()).isEqualTo("FAILING");
        }
    }

    @Test
    void testCanceledCreationCanPersistAfterFailedResponse()
            throws Exception
    {
        assertCanceledMutationCanPersist(false);
    }

    @Test
    void testCanceledDropCanPersistAfterFailedResponse()
            throws Exception
    {
        assertCanceledMutationCanPersist(true);
    }

    private static void assertCanceledMutationCanPersist(boolean drop)
            throws Exception
    {
        try (TestingCatalogStoreDatabase isolatedDatabase = new TestingCatalogStoreDatabase();
                DistributedQueryRunner runner = createQueryRunner(isolatedDatabase.storeProperties("cell" + randomNameSuffix()));
                Connection blocker = isolatedDatabase.openConnection()) {
            String query = "CREATE CATALOG canceled_catalog USING tpch";
            int initialCount = 0;
            if (drop) {
                runner.execute(query);
                query = "DROP CATALOG canceled_catalog";
                initialCount = 1;
            }
            blocker.setAutoCommit(false);
            try (Statement statement = blocker.createStatement()) {
                statement.setQueryTimeout(10);
                statement.execute("LOCK TABLE trino_catalogs IN ACCESS EXCLUSIVE MODE");
                try (var executor = newSingleThreadExecutor()) {
                    String mutation = query;
                    var response = executor.submit(() -> runner.execute(mutation));
                    try {
                        assertEventually(() -> assertThat(scalar(
                                statement,
                                "SELECT count(*) FROM pg_locks WHERE relation = 'trino_catalogs'::regclass AND NOT granted")).isPositive());
                        QueryManager manager = runner.getCoordinator().getQueryManager();
                        var queryId = getOnlyElement(manager.getQueries().stream()
                                .filter(info -> info.getQuery().equals(mutation) && !info.getState().isDone())
                                .toList()).getQueryId();
                        manager.cancelQuery(queryId);
                        assertThat(manager.getQueryState(queryId)).isEqualTo(FAILED);
                        assertThat(manager.getFullQueryInfo(queryId).getErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());
                        assertThatThrownBy(() -> response.get(10, SECONDS))
                                .isInstanceOf(ExecutionException.class)
                                .hasMessageContaining("Query was canceled");
                        assertThat(scalar(statement, "SELECT count(*) FROM trino_catalogs WHERE catalog_name = 'canceled_catalog'"))
                                .isEqualTo(initialCount);
                    }
                    finally {
                        blocker.rollback();
                    }
                    int finalCount = 1 - initialCount;
                    assertEventually(() -> assertThat(scalar(
                            statement,
                            "SELECT count(*) FROM trino_catalogs WHERE catalog_name = 'canceled_catalog'")).isEqualTo(finalCount));
                }
            }
            finally {
                blocker.rollback();
            }
        }
    }

    private static long scalar(Statement statement, String sql)
    {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
        catch (java.sql.SQLException exception) {
            throw new RuntimeException("Failed to inspect isolated catalog mutation", exception);
        }
    }

    private static Map<String, String> catalogProperties(DistributedQueryRunner queryRunner, String catalogName)
    {
        CatalogManager manager = queryRunner.getCoordinator().getInstance(Key.get(CatalogManager.class));
        return manager.getCatalogProperties(manager.getCatalog(new CatalogName(catalogName)).orElseThrow().getCatalogHandle())
                .orElseThrow().properties();
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> storeProperties)
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setWorkerCount(0)
                .setCoordinatorProperties(ImmutableMap.of("catalog.store", "posthog"))
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

    /**
     * The real factory of the plugin, configured from the test instead of from
     * {@code etc/catalog-store.properties}.
     */
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
