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
import io.trino.plugin.catalogstore.posthog.CatalogVersions;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code FileCatalogStore.computeCatalogVersion} is package private in the engine, so this test
 * lives in the engine's package to check that the copy of the scheme in
 * {@link io.trino.plugin.catalogstore.posthog.CatalogVersions} still agrees with it. Catalogs must
 * get the same version regardless of which store a cell is configured with.
 */
final class TestCatalogVersionsMatchFileCatalogStore
{
    @Test
    void testMatchesFileCatalogStore()
    {
        assertMatches(new CatalogName("orders"), new ConnectorName("ducklake"), ImmutableMap.of(
                "ducklake.metadata.connection-url", "jdbc:postgresql://db:5432/lake",
                "ducklake.data-path", "s3://bucket/prefix/"));
        assertMatches(new CatalogName("empty"), new ConnectorName("tpch"), ImmutableMap.of());
        assertMatches(new CatalogName("secrets"), new ConnectorName("ducklake"), ImmutableMap.of(
                "ducklake.metadata.connection-password", "${ENV:CATALOG_PASSWORD}"));
    }

    private static void assertMatches(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        assertThat(CatalogVersions.computeCatalogVersion(catalogName, connectorName, properties))
                .isEqualTo(computeCatalogVersion(catalogName, connectorName, properties));
    }
}
