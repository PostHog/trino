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
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HoglakeSharedConfig
{
    public static final int MAX_TOKEN_BYTES = 64 * 1024;
    public static final int MAX_HEADER_BYTES = 8 * 1024;
    public static final int MAX_VERIFICATION_FILE_BYTES = 1024 * 1024;
    public static final int MAX_SIGNING_KEYS = 32;
    public static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private URI uri;
    private Duration requestTimeout = new Duration(120, SECONDS);
    private boolean multiTenant;
    private File jwksFile;
    private String issuer;
    private String backendBindings;
    private Duration clockSkew = new Duration(60, SECONDS);
    private Duration scopedMaxLifetime = new Duration(900, SECONDS);

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("hoglake.uri")
    @ConfigDescription("Hoglake REST base URI for shared metadata requests")
    public HoglakeSharedConfig setUri(URI uri)
    {
        if (uri != null && ((!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) ||
                uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null)) {
            throw new IllegalArgumentException("Invalid shared Hoglake URI");
        }
        this.uri = uri;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hoglake.client.request-timeout")
    @ConfigDescription("Timeout for each shared Hoglake metadata request")
    public HoglakeSharedConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public boolean isMultiTenant()
    {
        return multiTenant;
    }

    @Config("hoglake.multi-tenant")
    @ConfigDescription("Enable independently verified session authority for shared metadata requests")
    public HoglakeSharedConfig setMultiTenant(boolean multiTenant)
    {
        this.multiTenant = multiTenant;
        return this;
    }

    @NotNull
    public File getJwksFile()
    {
        return jwksFile;
    }

    @Config("hoglake.shared.jwks-file")
    @ConfigDescription("Mounted public JWKS file for offline session verification")
    public HoglakeSharedConfig setJwksFile(File jwksFile)
    {
        this.jwksFile = jwksFile;
        return this;
    }

    @NotNull
    public String getIssuer()
    {
        return issuer;
    }

    @Config("hoglake.shared.issuer")
    @ConfigDescription("Exact issuer accepted for shared session tokens")
    public HoglakeSharedConfig setIssuer(String issuer)
    {
        this.issuer = issuer;
        return this;
    }

    @NotNull
    public String getBackendBindings()
    {
        return backendBindings;
    }

    @Config("hoglake.shared.backend-bindings")
    @ConfigDescription("JSON object mapping canonical backend UUIDs to postgres or hoglite")
    public HoglakeSharedConfig setBackendBindings(String backendBindings)
    {
        this.backendBindings = backendBindings;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    @MaxDuration("600s")
    public Duration getClockSkew()
    {
        return clockSkew;
    }

    @Config("hoglake.shared.clock-skew")
    @ConfigDescription("Clock tolerance for shared session verification, from 0s through 600s")
    public HoglakeSharedConfig setClockSkew(Duration clockSkew)
    {
        this.clockSkew = clockSkew;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    @MaxDuration("900s")
    public Duration getScopedMaxLifetime()
    {
        return scopedMaxLifetime;
    }

    @Config("hoglake.shared.scoped-max-lifetime")
    @ConfigDescription("Maximum scoped session lifetime, from 1s through 900s")
    public HoglakeSharedConfig setScopedMaxLifetime(Duration scopedMaxLifetime)
    {
        this.scopedMaxLifetime = scopedMaxLifetime;
        return this;
    }
}
