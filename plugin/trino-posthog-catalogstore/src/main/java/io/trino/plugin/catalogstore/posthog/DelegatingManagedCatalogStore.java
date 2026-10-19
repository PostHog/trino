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

import com.google.inject.Inject;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.DelegatedCatalogStore;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.Identity;

import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A {@link PostHogManagedCatalogStore} whose coordinator forwards {@code CREATE CATALOG} and
 * {@code DROP CATALOG} to the control plane instead of rejecting them. The store itself still
 * writes nothing: the control plane publishes the accepted change, and it arrives here through
 * catalog synchronization like every other published catalog.
 */
public class DelegatingManagedCatalogStore
        extends PostHogManagedCatalogStore
        implements DelegatedCatalogStore
{
    private final CatalogRequestClient client;
    private final Duration visibilityTimeout;

    @Inject
    public DelegatingManagedCatalogStore(PostHogCatalogStoreConfig config, PostHogCatalogStoreConnectionFactory connectionFactory, CatalogRequestClient client)
    {
        super(config, connectionFactory);
        this.client = requireNonNull(client, "client is null");
        this.visibilityTimeout = config.getDelegateVisibilityTimeout().toJavaTime();
    }

    @Override
    public CatalogRequestResult createCatalog(Identity identity, CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean ifNotExists)
    {
        return client.createCatalog(identity, catalogName, connectorName, properties, ifNotExists);
    }

    @Override
    public CatalogRequestResult dropCatalog(Identity identity, CatalogName catalogName, boolean ifExists)
    {
        return client.dropCatalog(identity, catalogName, ifExists);
    }

    @Override
    public Duration visibilityTimeout()
    {
        return visibilityTimeout;
    }
}
