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
package io.trino.server.security;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TrinoCallerTokenConfig
{
    private boolean enabled;
    private URI issuerUri;
    private File cellTokenFile;
    private Duration timeout = new Duration(5, SECONDS);
    private DataSize maxResponseSize = DataSize.of(64, KILOBYTE);
    private boolean allowInsecureHttpForDevelopment;
    private Duration reusePeriod = new Duration(0, SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("http-server.authentication.password.caller-token.enabled")
    @ConfigDescription("Acquire a caller token after managed password authentication")
    public TrinoCallerTokenConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public URI getIssuerUri()
    {
        return issuerUri;
    }

    @Config("http-server.authentication.password.caller-token.issuer-uri")
    @ConfigDescription("Exact URI of the dedicated caller grant endpoint")
    public TrinoCallerTokenConfig setIssuerUri(URI issuerUri)
    {
        this.issuerUri = issuerUri;
        return this;
    }

    public File getCellTokenFile()
    {
        return cellTokenFile;
    }

    @Config("http-server.authentication.password.caller-token.cell-token-file")
    @ConfigDescription("File containing the dedicated cell bearer credential, read for every acquisition")
    public TrinoCallerTokenConfig setCellTokenFile(File cellTokenFile)
    {
        this.cellTokenFile = cellTokenFile;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    @MaxDuration("30s")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("http-server.authentication.password.caller-token.timeout")
    @ConfigDescription("Maximum duration of one caller grant request")
    public TrinoCallerTokenConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    @NotNull
    @MinDataSize("1B")
    @MaxDataSize("1MB")
    public DataSize getMaxResponseSize()
    {
        return maxResponseSize;
    }

    @Config("http-server.authentication.password.caller-token.max-response-size")
    @ConfigDescription("Maximum size of a caller grant response")
    public TrinoCallerTokenConfig setMaxResponseSize(DataSize maxResponseSize)
    {
        this.maxResponseSize = maxResponseSize;
        return this;
    }

    public boolean isAllowInsecureHttpForDevelopment()
    {
        return allowInsecureHttpForDevelopment;
    }

    @Config("http-server.authentication.password.caller-token.allow-insecure-http-for-development")
    @ConfigDescription("Permit HTTP only for explicit development or test use")
    public TrinoCallerTokenConfig setAllowInsecureHttpForDevelopment(boolean allowInsecureHttpForDevelopment)
    {
        this.allowInsecureHttpForDevelopment = allowInsecureHttpForDevelopment;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    @MaxDuration("5m")
    public Duration getReusePeriod()
    {
        return reusePeriod;
    }

    @Config("http-server.authentication.password.caller-token.reuse-period")
    @ConfigDescription("Time a token is reused for the same login and password; zero acquires a token for every request")
    public TrinoCallerTokenConfig setReusePeriod(Duration reusePeriod)
    {
        this.reusePeriod = reusePeriod;
        return this;
    }

    @AssertTrue(message = "Enabled caller tokens require an issuer URI without user information, query, or fragment")
    public boolean isIssuerUriValid()
    {
        return !enabled || (issuerUri != null &&
                issuerUri.isAbsolute() &&
                issuerUri.getHost() != null &&
                issuerUri.getRawUserInfo() == null &&
                issuerUri.getRawQuery() == null &&
                issuerUri.getRawFragment() == null);
    }

    @AssertTrue(message = "Enabled caller tokens require HTTPS unless the development HTTP exception is explicit")
    public boolean isIssuerTransportValid()
    {
        return !enabled || (issuerUri != null &&
                ("https".equalsIgnoreCase(issuerUri.getScheme()) ||
                        (allowInsecureHttpForDevelopment && "http".equalsIgnoreCase(issuerUri.getScheme()))));
    }

    @AssertTrue(message = "Enabled caller tokens require a readable regular cell token file")
    public boolean isCellTokenFileValid()
    {
        return !enabled || (cellTokenFile != null &&
                Files.isRegularFile(cellTokenFile.toPath()) &&
                Files.isReadable(cellTokenFile.toPath()));
    }
}
