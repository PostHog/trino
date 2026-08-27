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

import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.trino.connector.CatalogLifecycleListener;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotCache;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlSemanticCatalogGuiceWiring
{
    private static final Key<Optional<HogQlSemanticCatalogSnapshotProvider>> OPTIONAL_PROVIDER_KEY =
            Key.get(new TypeLiteral<>() {});
    private static final Key<Set<CatalogLifecycleListener>> CATALOG_LISTENERS_KEY =
            Key.get(new TypeLiteral<>() {});

    @Test
    public void testOptionalProviderFollowsHogQlAndCatalogConfiguration()
            throws IOException
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(Map.of("hogql.enabled", "false"))
                .build()) {
            assertThat(server.getInstance(OPTIONAL_PROVIDER_KEY)).isEmpty();
            assertThat(server.getInstance(CATALOG_LISTENERS_KEY)).isEmpty();
        }

        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(Map.of(
                        "hogql.enabled", "true",
                        "hogql.semantic-catalog.uri", "https://duckgres.example/metadata"))
                .build()) {
            Optional<HogQlSemanticCatalogSnapshotProvider> provider = server.getInstance(OPTIONAL_PROVIDER_KEY);
            HogQlSemanticCatalogManager manager = server.getInstance(Key.get(HogQlSemanticCatalogManager.class));
            HogQlSemanticCatalogSnapshotCache cache = server.getInstance(Key.get(HogQlSemanticCatalogSnapshotCache.class));

            assertThat(provider).isPresent();
            assertThat(server.getInstance(CATALOG_LISTENERS_KEY)).hasOnlyElementsOfType(HogQlSemanticCatalogPrewarmListener.class);
            assertThat(server.getInstance(OPTIONAL_PROVIDER_KEY)).containsSame(provider.orElseThrow());
            assertThat(cache).isSameAs(manager.cache());
        }
    }
}
