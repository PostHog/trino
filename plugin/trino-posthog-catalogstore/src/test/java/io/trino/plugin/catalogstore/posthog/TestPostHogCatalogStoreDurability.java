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
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.CatalogStoreManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.ServerConfig;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
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

    private static QueryRunner createQueryRunner(Map<String, String> storeProperties)
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
