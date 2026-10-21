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
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.catalog.DelegatedCatalogStore;
import io.trino.spi.catalog.RevisionedCatalogStore;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPostHogCatalogStorePlugin
{
    @Test
    void testFactoryIsRegistered()
    {
        assertThat(factory().getName()).isEqualTo("posthog");
    }

    @Test
    void testCellIdIsRequired()
    {
        assertThatThrownBy(() -> factory().create(ImmutableMap.of("catalog-store.connection-url", "jdbc:postgresql://example.net:5432/catalogs")))
                .hasMessageContaining("catalog-store.cell-id")
                .hasMessageContaining("must not be empty");
    }

    @Test
    void testManagedStoreDelegatesOnlyWithEndpoint(@TempDir Path temporaryDirectory)
            throws IOException
    {
        CatalogStore managed = factory().create(managedProperties());
        assertThat(managed).isNotInstanceOf(DelegatedCatalogStore.class);
        assertThatThrownBy(() -> managed.createCatalogProperties(new CatalogName("ext_a__b"), new ConnectorName("postgresql"), Map.of()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Catalog store is read-only");

        Path tokenFile = Files.writeString(temporaryDirectory.resolve("token"), "synthetic-token\n");
        CatalogStore delegating = factory().create(ImmutableMap.<String, String>builder()
                .putAll(managedProperties())
                .put("catalog-store.delegate.endpoint", "https://control-plane.example.net/trino/catalog-requests/v1")
                .put("catalog-store.delegate.token-file", tokenFile.toString())
                .put("catalog-store.delegate.visibility-timeout", "40s")
                .buildOrThrow());
        assertThat(delegating).isInstanceOf(DelegatedCatalogStore.class);
        assertThat(((RevisionedCatalogStore) delegating).storeIdentity()).contains("cell-1");
        assertThat(((DelegatedCatalogStore) delegating).visibilityTimeout()).isEqualTo(Duration.ofSeconds(40));
        // The store itself still writes nothing
        assertThatThrownBy(() -> delegating.removeCatalog(new CatalogName("ext_a__b")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Catalog store is read-only");
    }

    @Test
    void testWritableStoreRefusesDelegation(@TempDir Path temporaryDirectory)
            throws IOException
    {
        Path tokenFile = Files.writeString(temporaryDirectory.resolve("token"), "synthetic-token\n");
        assertThatThrownBy(() -> factory().create(ImmutableMap.of(
                "catalog-store.cell-id", "cell-1",
                "catalog-store.connection-url", "jdbc:postgresql://example.net:5432/catalogs",
                "catalog-store.delegate.endpoint", "https://control-plane.example.net/trino/catalog-requests/v1",
                "catalog-store.delegate.token-file", tokenFile.toString())))
                .hasMessageContaining("catalog-store.delegate.endpoint requires catalog-store.read-only=true");
    }

    @Test
    void testDelegationRequiresHttps(@TempDir Path temporaryDirectory)
            throws IOException
    {
        Path tokenFile = Files.writeString(temporaryDirectory.resolve("token"), "synthetic-token\n");
        assertThatThrownBy(() -> factory().create(ImmutableMap.<String, String>builder()
                .putAll(managedProperties())
                .put("catalog-store.delegate.endpoint", "http://control-plane.example.net/trino/catalog-requests/v1")
                .put("catalog-store.delegate.token-file", tokenFile.toString())
                .buildOrThrow()))
                .hasMessageContaining("requires HTTPS");
    }

    private static Map<String, String> managedProperties()
    {
        return ImmutableMap.of(
                "catalog-store.cell-id", "cell-1",
                "catalog-store.connection-url", "jdbc:postgresql://example.net:5432/catalogs",
                "catalog-store.read-only", "true");
    }

    private static CatalogStoreFactory factory()
    {
        return getOnlyElement(new PostHogCatalogStorePlugin().getCatalogStoreFactories());
    }
}
