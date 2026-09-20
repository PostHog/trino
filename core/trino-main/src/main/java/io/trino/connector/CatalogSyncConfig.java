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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CatalogSyncConfig
{
    private boolean enabled;
    private Duration pollInterval = new Duration(5, SECONDS);
    private Duration minRetryDelay = new Duration(500, MILLISECONDS);
    private Duration maxRetryDelay = new Duration(30, SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("catalog.sync.enabled")
    @ConfigDescription("Follow the catalogs published in the catalog store while running, instead of only loading them at startup")
    public CatalogSyncConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getPollInterval()
    {
        return pollInterval;
    }

    @Config("catalog.sync.poll-interval")
    @ConfigDescription("How often the published catalog revision is polled; the actual delay is jittered around this value")
    public CatalogSyncConfig setPollInterval(Duration pollInterval)
    {
        this.pollInterval = pollInterval;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMinRetryDelay()
    {
        return minRetryDelay;
    }

    @Config("catalog.sync.min-retry-delay")
    @ConfigDescription("Shortest delay before retrying after a failed poll or a catalog that could not be applied")
    public CatalogSyncConfig setMinRetryDelay(Duration minRetryDelay)
    {
        this.minRetryDelay = minRetryDelay;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxRetryDelay()
    {
        return maxRetryDelay;
    }

    @Config("catalog.sync.max-retry-delay")
    @ConfigDescription("Longest delay between retries; reached by exponential backoff with full jitter")
    public CatalogSyncConfig setMaxRetryDelay(Duration maxRetryDelay)
    {
        this.maxRetryDelay = maxRetryDelay;
        return this;
    }

    @AssertTrue(message = "catalog.sync.max-retry-delay must not be shorter than catalog.sync.min-retry-delay")
    public boolean isRetryDelayRangeValid()
    {
        return maxRetryDelay.compareTo(minRetryDelay) >= 0;
    }
}
