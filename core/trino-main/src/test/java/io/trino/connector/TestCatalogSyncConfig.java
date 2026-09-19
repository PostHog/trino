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
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

final class TestCatalogSyncConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CatalogSyncConfig.class)
                .setEnabled(false)
                .setPollInterval(new Duration(5, SECONDS))
                .setMinRetryDelay(new Duration(500, MILLISECONDS))
                .setMaxRetryDelay(new Duration(30, SECONDS)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("catalog.sync.enabled", "true")
                .put("catalog.sync.poll-interval", "7s")
                .put("catalog.sync.min-retry-delay", "1s")
                .put("catalog.sync.max-retry-delay", "1m")
                .buildOrThrow();

        CatalogSyncConfig expected = new CatalogSyncConfig()
                .setEnabled(true)
                .setPollInterval(new Duration(7, SECONDS))
                .setMinRetryDelay(new Duration(1, SECONDS))
                .setMaxRetryDelay(new Duration(1, MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    void testRetryDelayRangeIsValidated()
    {
        assertValidates(new CatalogSyncConfig()
                .setMinRetryDelay(new Duration(1, SECONDS))
                .setMaxRetryDelay(new Duration(1, SECONDS)));

        assertFailsValidation(
                new CatalogSyncConfig()
                        .setMinRetryDelay(new Duration(10, SECONDS))
                        .setMaxRetryDelay(new Duration(1, SECONDS)),
                "retryDelayRangeValid",
                "catalog.sync.max-retry-delay must not be shorter than catalog.sync.min-retry-delay",
                AssertTrue.class);
    }
}
