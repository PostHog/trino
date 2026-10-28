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
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;
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
    private boolean variantPathAssemblyEnabled = true;
    private boolean variantLazyResidualEnabled = true;
    private String authTokenUri;
    private String authClientId;
    private String authClientSecret;
    private String authAudience = "hoglake";

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

    public boolean isVariantPathAssemblyEnabled()
    {
        return variantPathAssemblyEnabled;
    }

    @Config("hoglake.variant-path-assembly-enabled")
    @ConfigDescription("Build pushed-down VARIANT subscripts from the parts of each value that they read, instead of from the whole value")
    public HoglakeConfig setVariantPathAssemblyEnabled(boolean variantPathAssemblyEnabled)
    {
        this.variantPathAssemblyEnabled = variantPathAssemblyEnabled;
        return this;
    }

    public boolean isVariantLazyResidualEnabled()
    {
        return variantLazyResidualEnabled;
    }

    @Config("hoglake.variant-lazy-residual-enabled")
    @ConfigDescription("Read the metadata and value columns of a shredded VARIANT with pushed-down subscripts only for the batches of rows that need them")
    public HoglakeConfig setVariantLazyResidualEnabled(boolean variantLazyResidualEnabled)
    {
        this.variantLazyResidualEnabled = variantLazyResidualEnabled;
        return this;
    }

    /**
     * Airlift-style duration: a positive decimal plus ms/s/m/h/d.
     */
    public Optional<String> getAuthTokenUri()
    {
        return Optional.ofNullable(authTokenUri);
    }

    @Config("hoglake.auth.token-uri")
    @ConfigDescription("OAuth2 token endpoint that mints Hoglake access tokens (client credentials grant)")
    public HoglakeConfig setAuthTokenUri(String authTokenUri)
    {
        this.authTokenUri = authTokenUri == null || authTokenUri.isBlank() ? null : validateTokenUri(authTokenUri.trim());
        return this;
    }

    public Optional<String> getAuthClientId()
    {
        return Optional.ofNullable(authClientId);
    }

    @Config("hoglake.auth.client-id")
    @ConfigDescription("Client id for the Hoglake token endpoint")
    public HoglakeConfig setAuthClientId(String authClientId)
    {
        this.authClientId = authClientId == null || authClientId.isBlank() ? null : authClientId;
        return this;
    }

    public Optional<String> getAuthClientSecret()
    {
        return Optional.ofNullable(authClientSecret);
    }

    @Config("hoglake.auth.client-secret")
    @ConfigSecuritySensitive
    @ConfigDescription("Client secret for the Hoglake token endpoint")
    public HoglakeConfig setAuthClientSecret(String authClientSecret)
    {
        this.authClientSecret = authClientSecret == null || authClientSecret.isEmpty() ? null : authClientSecret;
        return this;
    }

    @NotNull
    public String getAuthAudience()
    {
        return authAudience;
    }

    @Config("hoglake.auth.audience")
    @ConfigDescription("Audience requested from the token endpoint")
    public HoglakeConfig setAuthAudience(String authAudience)
    {
        this.authAudience = authAudience;
        return this;
    }

    // All three or none: a catalog with a token endpoint and no secret would
    // otherwise register fine and then fail every query.
    @AssertTrue(message = "hoglake.auth.token-uri, hoglake.auth.client-id and hoglake.auth.client-secret must be set together")
    public boolean isAuthConfigComplete()
    {
        int set = (authTokenUri == null ? 0 : 1) + (authClientId == null ? 0 : 1) + (authClientSecret == null ? 0 : 1);
        return set == 0 || set == 3;
    }

    private static String validateTokenUri(String value)
    {
        URI parsed;
        try {
            parsed = new URI(value);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid hoglake.auth.token-uri '" + value + "': " + e.getMessage(), e);
        }
        if (!"http".equals(parsed.getScheme()) && !"https".equals(parsed.getScheme()) || parsed.getHost() == null) {
            throw new IllegalArgumentException("Invalid hoglake.auth.token-uri '" + value + "': must be an http(s) URL with a host");
        }
        return parsed.toString();
    }

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
