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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotProvider;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.FunctionBundle;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public final class HogQlExchangeRateModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        HogQlSemanticCatalogConfig config = buildConfigObject(HogQlSemanticCatalogConfig.class);
        httpClientBinder(binder)
                .bindHttpClient("hogql-exchange-rate", ForHogQlExchangeRate.class)
                .withConfigDefaults(httpClientConfig -> httpClientConfig
                        .setRequestTimeout(config.getRequestTimeout())
                        .setMaxResponseContentLength(config.getMaximumResponseSize())
                        .setMaxRequestsQueuedPerDestination(config.getLoaderQueueCapacity()));
        binder.bind(HogQlExchangeRateHttpTransport.class).in(Scopes.SINGLETON);
        binder.bind(HogQlExchangeRateManager.class).in(Scopes.SINGLETON);
        OptionalBinder.newOptionalBinder(binder, HogQlExchangeRateSnapshotProvider.class)
                .setBinding()
                .to(HogQlExchangeRateManager.class);
    }

    @Provides
    @Singleton
    public static HogQlExchangeRateSnapshotJsonDecoder provideDecoder()
    {
        return new HogQlExchangeRateSnapshotJsonDecoder();
    }

    @ProvidesIntoSet
    @Singleton
    public static FunctionBundle provideFunctionBundle(HogQlExchangeRateManager manager)
    {
        return new InternalFunctionBundle(new HogQlExchangeRateFunction(manager));
    }
}
