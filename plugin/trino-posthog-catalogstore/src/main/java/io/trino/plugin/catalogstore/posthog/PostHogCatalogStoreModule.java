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
import io.trino.spi.catalog.CatalogStore;

import static io.airlift.configuration.ConfigBinder.configBinder;

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
        if (buildConfigObject(PostHogCatalogStoreConfig.class).isReadOnly()) {
            binder.bind(CatalogStore.class).to(PostHogManagedCatalogStore.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(CatalogStore.class).to(PostHogCatalogStore.class).in(Scopes.SINGLETON);
        }
    }
}
