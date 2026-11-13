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
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.filesystem.s3.S3SecurityMappingEnabledConfig;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageSorter;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

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
        configBinder(binder).bindConfig(S3SecurityMappingEnabledConfig.class);
        closingBinder(binder).registerCloseable(HoglakeClient.class);
        closingBinder(binder).registerResource(new Key<Optional<HoglakePosthogSchema>>() {}, schema -> schema.ifPresent(HoglakePosthogSchema::close));
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HoglakeSessionProperties.class).in(Scopes.SINGLETON);

        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(HoglakeParquetFooterCache.class)
                .as(generator -> generator.generatedNameOf(HoglakeParquetFooterCache.class, catalogName.get().toString()));
    }

    @Provides
    @Singleton
    public static HoglakeTokenSource createTokenSource(HoglakeConfig config)
    {
        // One way to get a token, or none: a catalog given both a token endpoint
        // and a fixed token would silently use one of them.
        if (config.configuredAuthSources() > 1) {
            throw new IllegalArgumentException("hoglake.auth.token, hoglake.auth.token-file and the token endpoint settings "
                    + "(hoglake.auth.token-uri, hoglake.auth.client-id, hoglake.auth.client-secret) are mutually exclusive");
        }
        if (config.getAuthToken().isPresent()) {
            return new HoglakeTokenSource.Fixed(config.getAuthToken().get());
        }
        if (config.getAuthTokenFile().isPresent()) {
            return new HoglakeTokenSource.TokenFile(Path.of(config.getAuthTokenFile().get()));
        }
        return config.getAuthTokenUri()
                .<HoglakeTokenSource>map(tokenUri -> new HoglakeTokenSource.ClientCredentials(
                        URI.create(tokenUri),
                        config.getAuthClientId().orElseThrow(),
                        config.getAuthClientSecret().orElseThrow(),
                        config.getAuthAudience(),
                        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()))
                .orElse(HoglakeTokenSource.NONE);
    }

    @Provides
    @Singleton
    @SuppressWarnings("CloseableProvides") // Registered with ClosingBinder above.
    public static HoglakeClient createClient(HoglakeConfig config, HoglakeTokenSource tokenSource)
    {
        return new HoglakeClient(config.getUri(), config.getCatalog(), config.getRequestTimeout(), tokenSource);
    }

    /**
     * The posthog schema's mount, on the same server as the org catalog. It is read
     * with a token of the Trino login that asks, which the catalog's client obtains
     * from the same token endpoint by token exchange. The org catalog's own token
     * source is not given to it.
     *
     * <p>A catalog with the schema enabled and no token endpoint still loads: the
     * control plane sets the two independently, and failing here would take the
     * org's own catalog down. Every read of the schema then fails.
     */
    @Provides
    @Singleton
    public static Optional<HoglakePosthogSchema> createPosthogSchema(HoglakeConfig config)
    {
        if (!config.isPosthogSchemaEnabled()) {
            return Optional.empty();
        }
        String mountCatalog = HoglakePosthogSchema.mountCatalog(config.getCatalog());
        HoglakeLoginTokenSource loginTokens = config.getAuthTokenUri()
                .<HoglakeLoginTokenSource>map(tokenUri -> new HoglakeLoginTokenSource.TokenExchange(
                        URI.create(tokenUri),
                        config.getAuthClientId().orElseThrow(),
                        config.getAuthClientSecret().orElseThrow(),
                        config.getAuthAudience(),
                        HoglakePosthogSchema.LOGIN_SUBJECT_TOKEN_TYPE,
                        Optional.of(HoglakePosthogSchema.mountScope(mountCatalog)),
                        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()))
                .orElse(HoglakeLoginTokenSource.UNCONFIGURED);
        return Optional.of(new HoglakePosthogSchema(
                config.getCatalog(),
                HoglakeClient.forMount(config.getUri(), mountCatalog, config.getRequestTimeout()),
                loginTokens));
    }

    @Provides
    @Singleton
    public static HoglakeMetadata createMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory, Optional<HoglakePosthogSchema> posthogSchema)
    {
        return new HoglakeMetadata(client, fileSystemFactory, posthogSchema);
    }

    @Provides
    @Singleton
    public static ConnectorSplitManager createSplitManager(HoglakeClient client, Optional<HoglakePosthogSchema> posthogSchema, SplitAffinityProvider affinityProvider)
    {
        // The filesystem module binds a key-producing provider only when the
        // catalog caches filesystem data, and a no-op provider otherwise.
        return new HoglakeSplitManager(client, posthogSchema, HoglakeSessionProperties::getMaxSplitSize, affinityProvider);
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
    public static ConnectorPageSourceProvider createPageSourceProvider(TrinoFileSystemFactory fileSystemFactory, HoglakeParquetFooterCache footerCache, HoglakeConfig config, S3SecurityMappingEnabledConfig securityMappingConfig)
    {
        return new HoglakePageSourceProvider(fileSystemFactory, footerCache, Optional.of(CacheKey.of("hoglake-data-v1", config.getUri(), config.getCatalog())), securityMappingConfig.isEnabled());
    }

    @Provides
    @Singleton
    public static ConnectorPageSinkProvider createPageSinkProvider(TrinoFileSystemFactory fileSystemFactory, NodeVersion nodeVersion, PageSorter pageSorter, HoglakeClient client)
    {
        return new HoglakePageSinkProvider(fileSystemFactory, nodeVersion.toString(), pageSorter, client);
    }
}
