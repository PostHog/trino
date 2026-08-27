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
package io.trino.connector;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.cache.CacheManagerConfig;
import io.trino.cache.CacheManagerRegistry;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static org.assertj.core.api.Assertions.assertThat;

class TestCatalogLifecycleListeners
{
    @Test
    void testSuccessfulStaticAndDynamicCatalogLifecycleNotifications(@TempDir Path catalogDirectory)
            throws IOException
    {
        List<CatalogName> loadedCatalogs = new ArrayList<>();
        CatalogLifecycleListeners listeners = new CatalogLifecycleListeners(
                Set.of(loadedCatalogs::add, _ -> { throw new RuntimeException("listener failure"); }));

        Files.writeString(catalogDirectory.resolve("static_catalog.properties"), "connector.name=mock\n");
        StaticCatalogManager staticManager = new StaticCatalogManager(
                new TestingCatalogFactory(Set.of()),
                new StaticCatalogManagerConfig().setCatalogConfigurationDir(catalogDirectory.toFile()),
                MoreExecutors.directExecutor(),
                listeners);
        staticManager.loadInitialCatalogs();

        InMemoryCatalogStore catalogStore = new InMemoryCatalogStore();
        catalogStore.addOrReplaceCatalog(catalogProperties("dynamic_initial"));
        CoordinatorDynamicCatalogManager dynamicManager = new CoordinatorDynamicCatalogManager(
                catalogStore,
                new TestingCatalogFactory(Set.of()),
                cacheManagerRegistry(),
                MoreExecutors.directExecutor(),
                listeners);
        dynamicManager.loadInitialCatalogs();
        dynamicManager.createCatalog(new CatalogName("created_catalog"), new ConnectorName("mock"), Map.of(), false);
        dynamicManager.createCatalog(new CatalogName("created_catalog"), new ConnectorName("mock"), Map.of(), true);
        dynamicManager.dropCatalog(new CatalogName("created_catalog"), false);
        dynamicManager.createCatalog(new CatalogName("created_catalog"), new ConnectorName("mock"), Map.of(), false);

        assertThat(loadedCatalogs).containsExactly(
                new CatalogName("static_catalog"),
                new CatalogName("dynamic_initial"),
                new CatalogName("created_catalog"),
                new CatalogName("created_catalog"));

        dynamicManager.stop();
        staticManager.stop();
    }

    @Test
    void testFailedCatalogLoadDoesNotNotify()
    {
        List<CatalogName> loadedCatalogs = new ArrayList<>();
        CatalogLifecycleListeners listeners = new CatalogLifecycleListeners(Set.of(loadedCatalogs::add));
        InMemoryCatalogStore catalogStore = new InMemoryCatalogStore();
        CatalogProperties broken = catalogProperties("broken_catalog");
        catalogStore.addOrReplaceCatalog(broken);

        CoordinatorDynamicCatalogManager manager = new CoordinatorDynamicCatalogManager(
                catalogStore,
                new TestingCatalogFactory(Set.of(broken.name())),
                cacheManagerRegistry(),
                MoreExecutors.directExecutor(),
                listeners);
        manager.loadInitialCatalogs();

        assertThat(loadedCatalogs).isEmpty();
        manager.stop();
    }

    private static CatalogProperties catalogProperties(String name)
    {
        return new CatalogProperties(new CatalogName(name), new CatalogVersion("1"), new ConnectorName("mock"), ImmutableMap.of());
    }

    private static CacheManagerRegistry cacheManagerRegistry()
    {
        return new CacheManagerRegistry(OpenTelemetry.noop(), noopTracer(), new SecretsResolver(ImmutableMap.of()), new CacheManagerConfig());
    }

    private static class TestingCatalogFactory
            implements CatalogFactory
    {
        private final Set<CatalogName> failures;

        private TestingCatalogFactory(Set<CatalogName> failures)
        {
            this.failures = Set.copyOf(failures);
        }

        @Override
        public void addConnectorFactory(ConnectorFactory connectorFactory) {}

        @Override
        public CatalogConnector createCatalog(CatalogProperties catalogProperties)
        {
            if (failures.contains(catalogProperties.name())) {
                throw new RuntimeException("catalog load failure");
            }
            Connector connector = MockConnectorFactory.create().create(catalogProperties.name().toString(), catalogProperties.properties(), new TestingConnectorContext());
            CatalogHandle catalogHandle = createRootCatalogHandle(catalogProperties.name(), catalogProperties.version());
            ConnectorServices connectorServices = new ConnectorServices(noopTracer(), catalogHandle, connector);
            return new CatalogConnector(
                    catalogHandle,
                    catalogProperties.connectorName(),
                    connectorServices,
                    connectorServices,
                    connectorServices,
                    Optional.of(catalogProperties));
        }

        @Override
        public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
        {
            throw new UnsupportedOperationException();
        }
    }
}
