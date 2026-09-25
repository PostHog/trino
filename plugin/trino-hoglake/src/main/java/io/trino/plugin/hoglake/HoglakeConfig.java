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
package io.trino.plugin.hoglake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HoglakeConfig
{
    // With the footer cache, a split costs a cache hit plus reader setup, so a smaller
    // target buys balance across drivers and work stealing at little cost. Hoglake row
    // groups average about 150 MiB, so a range this size holds about two of them.
    public static final DataSize DEFAULT_MAX_SPLIT_SIZE = DataSize.of(256, MEGABYTE);

    private static final Pattern DURATION = Pattern.compile("\\s*(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h|d)\\s*");

    private String uri;
    private String catalog = "hoglake";
    private Duration requestTimeout = Duration.ofMinutes(2);
    private DataSize maxSplitSize = DEFAULT_MAX_SPLIT_SIZE;
    private DataSize parquetFooterCacheMaxSize = DataSize.of(64, MEGABYTE);

    @NotNull
    public String getUri()
    {
        return uri;
    }

    @Config("hoglake.uri")
    @ConfigDescription("Hoglake REST base URI, without a /v1 suffix")
    public HoglakeConfig setUri(String uri)
    {
        this.uri = uri == null ? null : HoglakeClient.validateBaseUri(uri).toString();
        return this;
    }

    public String getCatalog()
    {
        return catalog;
    }

    @Config("hoglake.catalog")
    @ConfigDescription("Hoglake catalog to expose")
    public HoglakeConfig setCatalog(String catalog)
    {
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("hoglake.catalog must not be empty");
        }
        this.catalog = catalog;
        return this;
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hoglake.client.request-timeout")
    @ConfigDescription("Timeout for a Hoglake REST request")
    public HoglakeConfig setRequestTimeout(String requestTimeout)
    {
        this.requestTimeout = parseDuration(requestTimeout);
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("hoglake.max-split-size")
    @ConfigDescription("Largest byte range of one Parquet file assigned to a single split")
    public HoglakeConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @NotNull
    public DataSize getParquetFooterCacheMaxSize()
    {
        return parquetFooterCacheMaxSize;
    }

    @Config("hoglake.parquet-footer-cache.max-size")
    @ConfigDescription("Maximum serialized size of the parsed Parquet footers each worker caches; 0B disables the cache")
    public HoglakeConfig setParquetFooterCacheMaxSize(DataSize parquetFooterCacheMaxSize)
    {
        this.parquetFooterCacheMaxSize = parquetFooterCacheMaxSize;
        return this;
    }

    /**
     * Airlift-style duration: a positive decimal plus ms/s/m/h/d.
     */
    private static Duration parseDuration(String value)
    {
        Matcher matcher = DURATION.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.client.request-timeout '" + value + "' (expected e.g. 500ms, 30s, 2m, 1h)");
        }
        double amount = Double.parseDouble(matcher.group(1));
        double millisPerUnit = switch (matcher.group(2)) {
            case "ms" -> 1;
            case "s" -> 1_000;
            case "m" -> 60_000;
            case "h" -> 3_600_000;
            case "d" -> 86_400_000;
            default -> throw new IllegalStateException("unreachable");
        };
        long millis = Math.round(amount * millisPerUnit);
        if (millis <= 0) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.client.request-timeout '" + value + "': must be positive");
        }
        return Duration.ofMillis(millis);
    }
}
