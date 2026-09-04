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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestHogQlSemanticCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HogQlSemanticCatalogConfig.class)
                .setUri(null)
                .setAuthenticationTokenFile(null)
                .setMaximumEntries(100)
                .setRefreshAfter(new Duration(1, MINUTES))
                .setExpireAfter(new Duration(5, MINUTES))
                .setFailureBackoff(new Duration(10, SECONDS))
                .setLoaderThreads(4)
                .setLoaderQueueCapacity(64)
                .setRequestTimeout(new Duration(10, SECONDS))
                .setMaximumResponseSize(DataSize.of(8, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "hogql.semantic-catalog.uri", "https://duckgres.example/metadata",
                "hogql.semantic-catalog.authentication-token-file", "/run/secrets/hogql-catalog-token",
                "hogql.semantic-catalog.maximum-entries", "17",
                "hogql.semantic-catalog.refresh-after", "2m",
                "hogql.semantic-catalog.expire-after", "9m",
                "hogql.semantic-catalog.failure-backoff", "20s",
                "hogql.semantic-catalog.loader-threads", "3",
                "hogql.semantic-catalog.loader-queue-capacity", "41",
                "hogql.semantic-catalog.request-timeout", "7s",
                "hogql.semantic-catalog.maximum-response-size", "4MB");

        HogQlSemanticCatalogConfig expected = new HogQlSemanticCatalogConfig()
                .setUri(URI.create("https://duckgres.example/metadata"))
                .setAuthenticationTokenFile("/run/secrets/hogql-catalog-token")
                .setMaximumEntries(17)
                .setRefreshAfter(new Duration(2, MINUTES))
                .setExpireAfter(new Duration(9, MINUTES))
                .setFailureBackoff(new Duration(20, SECONDS))
                .setLoaderThreads(3)
                .setLoaderQueueCapacity(41)
                .setRequestTimeout(new Duration(7, SECONDS))
                .setMaximumResponseSize(DataSize.of(4, MEGABYTE));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testUriRequiresAuthenticationTokenFile()
    {
        assertFailsValidation(
                new HogQlSemanticCatalogConfig().setUri(URI.create("https://duckgres.example/metadata")),
                "authenticationConfigured",
                "hogql.semantic-catalog.authentication-token-file is required when hogql.semantic-catalog.uri is set",
                AssertTrue.class);
    }
}
