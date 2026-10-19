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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.catalog.CatalogStore;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PostHogCatalogStoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PostHogCatalogStoreConfig.class);
        binder.bind(PostHogCatalogStoreConnectionFactory.class).in(Scopes.SINGLETON);
        // The two stores are different implementations, not one store with a mode: a managed reader
        // publishes revisions and writes nothing, and the writable store behaves as it always has
        PostHogCatalogStoreConfig config = buildConfigObject(PostHogCatalogStoreConfig.class);
        if (config.isReadOnly() && config.getDelegateEndpoint().isPresent()) {
            // Mutations are forwarded to the control plane. Only the managed store delegates: the
            // writable store applies them itself, which the configuration refuses to combine
            Duration requestTimeout = config.getDelegateRequestTimeout();
            httpClientBinder(binder)
                    .bindHttpClient("catalog-store.delegate", ForCatalogRequests.class)
                    .withConfigDefaults(client -> client
                            .setConnectTimeout(new Duration(Math.min(requestTimeout.toMillis(), 2_000), MILLISECONDS))
                            .setRequestTimeout(requestTimeout)
                            .setMaxResponseContentLength(DataSize.ofBytes(CatalogRequestClient.MAX_RESPONSE_BYTES)));
            binder.bind(CatalogRequestClient.class).in(Scopes.SINGLETON);
            binder.bind(CatalogStore.class).to(DelegatingManagedCatalogStore.class).in(Scopes.SINGLETON);
        }
        else if (config.isReadOnly()) {
            binder.bind(CatalogStore.class).to(PostHogManagedCatalogStore.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(CatalogStore.class).to(PostHogCatalogStore.class).in(Scopes.SINGLETON);
        }
    }
}
