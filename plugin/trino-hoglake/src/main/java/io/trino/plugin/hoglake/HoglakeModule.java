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
package io.trino.plugin.hoglake;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageSorter;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HoglakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HoglakeConfig.class);
        closingBinder(binder).registerCloseable(HoglakeClient.class);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HoglakeSessionProperties.class).in(Scopes.SINGLETON);

        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(HoglakeParquetFooterCache.class)
                .as(generator -> generator.generatedNameOf(HoglakeParquetFooterCache.class, catalogName.get().toString()));
    }

    @Provides
    @Singleton
    @SuppressWarnings("CloseableProvides") // Registered with ClosingBinder above.
    public static HoglakeClient createClient(HoglakeConfig config)
    {
        return new HoglakeClient(config.getUri(), config.getCatalog(), config.getRequestTimeout());
    }

    @Provides
    @Singleton
    public static HoglakeMetadata createMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory)
    {
        return new HoglakeMetadata(client, fileSystemFactory);
    }

    @Provides
    @Singleton
    public static ConnectorSplitManager createSplitManager(HoglakeClient client, SplitAffinityProvider affinityProvider)
    {
        // The filesystem module binds a key-producing provider only when the
        // catalog caches filesystem data, and a no-op provider otherwise.
        return new HoglakeSplitManager(client, HoglakeSessionProperties::getMaxSplitSize, affinityProvider);
    }

    @Provides
    @Singleton
    public static HoglakeParquetFooterCache createParquetFooterCache(HoglakeConfig config)
    {
        // One cache per catalog on each node, shared by every split the node reads.
        return new HoglakeParquetFooterCache(config.getParquetFooterCacheMaxSize());
    }

    @Provides
    @Singleton
    public static ConnectorPageSourceProvider createPageSourceProvider(TrinoFileSystemFactory fileSystemFactory, HoglakeParquetFooterCache footerCache)
    {
        return new HoglakePageSourceProvider(fileSystemFactory, footerCache);
    }

    @Provides
    @Singleton
    public static ConnectorPageSinkProvider createPageSinkProvider(TrinoFileSystemFactory fileSystemFactory, NodeVersion nodeVersion, PageSorter pageSorter, HoglakeClient client)
    {
        return new HoglakePageSinkProvider(fileSystemFactory, nodeVersion.toString(), pageSorter, client);
    }
}
