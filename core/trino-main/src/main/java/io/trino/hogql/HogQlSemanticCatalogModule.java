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
package io.trino.hogql;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.CatalogLifecycleListener;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotCache;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotJsonDecoder;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class HogQlSemanticCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        HogQlSemanticCatalogConfig config = buildConfigObject(HogQlSemanticCatalogConfig.class);
        httpClientBinder(binder)
                .bindHttpClient("hogql-semantic-catalog", ForHogQlSemanticCatalog.class)
                .withConfigDefaults(httpClientConfig -> httpClientConfig
                        .setRequestTimeout(config.getRequestTimeout())
                        .setMaxResponseContentLength(config.getMaximumResponseSize())
                        .setMaxRequestsQueuedPerDestination(config.getLoaderQueueCapacity()));
        binder.bind(HogQlSemanticCatalogHttpTransport.class).in(Scopes.SINGLETON);
        binder.bind(HogQlSemanticCatalogManager.class).asEagerSingleton();
        newSetBinder(binder, CatalogLifecycleListener.class)
                .addBinding()
                .to(HogQlSemanticCatalogPrewarmListener.class)
                .in(Scopes.SINGLETON);
        OptionalBinder.newOptionalBinder(binder, HogQlSemanticCatalogSnapshotProvider.class)
                .setBinding()
                .toProvider(SnapshotProviderFactory.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static HogQlSemanticCatalogSnapshotJsonDecoder provideDecoder()
    {
        return new HogQlSemanticCatalogSnapshotJsonDecoder();
    }

    @Provides
    @Singleton
    public static HogQlSemanticCatalogSnapshotLoader provideLoader(HogQlSemanticCatalogManager manager)
    {
        return manager.loader();
    }

    @Provides
    @Singleton
    public static HogQlSemanticCatalogSnapshotCache provideCache(HogQlSemanticCatalogManager manager)
    {
        return manager.cache();
    }

    public static final class SnapshotProviderFactory
            implements Provider<HogQlSemanticCatalogSnapshotProvider>
    {
        private final HogQlSemanticCatalogSnapshotCache cache;

        @Inject
        public SnapshotProviderFactory(HogQlSemanticCatalogSnapshotCache cache)
        {
            this.cache = cache;
        }

        @Override
        public HogQlSemanticCatalogSnapshotProvider get()
        {
            return HogQlSemanticCatalogSnapshotProvider.fromCache(cache);
        }
    }
}
