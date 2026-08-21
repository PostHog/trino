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
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.plugin.catalogstore.posthog.CatalogVersions.computeCatalogVersion;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCatalogVersions
{
    private static final CatalogName CATALOG_NAME = new CatalogName("orders");
    private static final ConnectorName CONNECTOR_NAME = new ConnectorName("ducklake");
    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "ducklake.metadata.connection-url", "jdbc:postgresql://db:5432/lake",
            "ducklake.data-path", "s3://bucket/prefix/");

    /**
     * The version has to be reproducible across coordinators and restarts, so it is pinned here.
     * Changing it invalidates every catalog version already written to a catalog store.
     */
    @Test
    void testKnownVersion()
    {
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, PROPERTIES))
                .isEqualTo(new CatalogVersion("0b48ccd298e6f062b4f8d81466bbeab8ef9cea832c4c08356f7e661cad1d040c"));
    }

    @Test
    void testIsDeterministic()
    {
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, PROPERTIES))
                .isEqualTo(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, PROPERTIES));
    }

    @Test
    void testIgnoresPropertyOrder()
    {
        Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("ducklake.data-path", "s3://bucket/prefix/");
        reversed.put("ducklake.metadata.connection-url", "jdbc:postgresql://db:5432/lake");

        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, reversed))
                .isEqualTo(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, PROPERTIES));
    }

    @Test
    void testEveryInputChangesTheVersion()
    {
        CatalogVersion version = computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, PROPERTIES);

        assertThat(computeCatalogVersion(new CatalogName("invoices"), CONNECTOR_NAME, PROPERTIES)).isNotEqualTo(version);
        assertThat(computeCatalogVersion(CATALOG_NAME, new ConnectorName("iceberg"), PROPERTIES)).isNotEqualTo(version);
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                .putAll(PROPERTIES)
                .put("ducklake.max-split-size", "32MB")
                .buildOrThrow()))
                .isNotEqualTo(version);
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.of(
                "ducklake.metadata.connection-url", "jdbc:postgresql://db:5432/lake",
                "ducklake.data-path", "s3://other-bucket/prefix/")))
                .isNotEqualTo(version);
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.of())).isNotEqualTo(version);
    }

    /**
     * A length prefix keeps concatenations of the same characters apart, so that moving a character
     * from a key to the value it precedes changes the version.
     */
    @Test
    void testLengthPrefixSeparatesAdjacentStrings()
    {
        assertThat(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.of("ab", "cd")))
                .isNotEqualTo(computeCatalogVersion(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.of("a", "bcd")));
    }
}
