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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

import static io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder.MAXIMUM_PAYLOAD_BYTES;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HogQlSemanticCatalogConfig
{
    private URI uri;
    private String authenticationTokenFile;
    private int maximumEntries = 100;
    private Duration refreshAfter = new Duration(1, MINUTES);
    private Duration expireAfter = new Duration(5, MINUTES);
    private Duration failureBackoff = new Duration(10, SECONDS);
    private int loaderThreads = 4;
    private int loaderQueueCapacity = 64;
    private Duration requestTimeout = new Duration(10, SECONDS);
    private DataSize maximumResponseSize = DataSize.ofBytes(MAXIMUM_PAYLOAD_BYTES);

    public URI getUri()
    {
        return uri;
    }

    @Config("hogql.semantic-catalog.uri")
    @ConfigDescription("Duckgres HogQL compatibility metadata base URI")
    public HogQlSemanticCatalogConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    public String getAuthenticationTokenFile()
    {
        return authenticationTokenFile;
    }

    @Config("hogql.semantic-catalog.authentication-token-file")
    @ConfigDescription("File containing the Duckgres HogQL compatibility metadata authentication token")
    public HogQlSemanticCatalogConfig setAuthenticationTokenFile(String authenticationTokenFile)
    {
        this.authenticationTokenFile = authenticationTokenFile;
        return this;
    }

    @AssertTrue(message = "hogql.semantic-catalog.authentication-token-file is required when hogql.semantic-catalog.uri is set")
    public boolean isAuthenticationConfigured()
    {
        return uri == null || (authenticationTokenFile != null && !authenticationTokenFile.isBlank());
    }

    @Min(1)
    public int getMaximumEntries()
    {
        return maximumEntries;
    }

    @Config("hogql.semantic-catalog.maximum-entries")
    public HogQlSemanticCatalogConfig setMaximumEntries(int maximumEntries)
    {
        this.maximumEntries = maximumEntries;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRefreshAfter()
    {
        return refreshAfter;
    }

    @Config("hogql.semantic-catalog.refresh-after")
    public HogQlSemanticCatalogConfig setRefreshAfter(Duration refreshAfter)
    {
        this.refreshAfter = refreshAfter;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getExpireAfter()
    {
        return expireAfter;
    }

    @Config("hogql.semantic-catalog.expire-after")
    public HogQlSemanticCatalogConfig setExpireAfter(Duration expireAfter)
    {
        this.expireAfter = expireAfter;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getFailureBackoff()
    {
        return failureBackoff;
    }

    @Config("hogql.semantic-catalog.failure-backoff")
    public HogQlSemanticCatalogConfig setFailureBackoff(Duration failureBackoff)
    {
        this.failureBackoff = failureBackoff;
        return this;
    }

    @Min(1)
    public int getLoaderThreads()
    {
        return loaderThreads;
    }

    @Config("hogql.semantic-catalog.loader-threads")
    public HogQlSemanticCatalogConfig setLoaderThreads(int loaderThreads)
    {
        this.loaderThreads = loaderThreads;
        return this;
    }

    @Min(1)
    public int getLoaderQueueCapacity()
    {
        return loaderQueueCapacity;
    }

    @Config("hogql.semantic-catalog.loader-queue-capacity")
    public HogQlSemanticCatalogConfig setLoaderQueueCapacity(int loaderQueueCapacity)
    {
        this.loaderQueueCapacity = loaderQueueCapacity;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hogql.semantic-catalog.request-timeout")
    public HogQlSemanticCatalogConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    @MinDataSize("1B")
    @MaxDataSize("32MB")
    public DataSize getMaximumResponseSize()
    {
        return maximumResponseSize;
    }

    @Config("hogql.semantic-catalog.maximum-response-size")
    public HogQlSemanticCatalogConfig setMaximumResponseSize(DataSize maximumResponseSize)
    {
        this.maximumResponseSize = maximumResponseSize;
        return this;
    }
}
