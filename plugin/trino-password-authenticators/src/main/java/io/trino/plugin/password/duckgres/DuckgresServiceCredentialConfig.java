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
package io.trino.plugin.password.duckgres;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

public class DuckgresServiceCredentialConfig
{
    private URI endpoint;
    private File tokenFile;
    private boolean allowInsecureHttp;

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("duckgres-service-credential.endpoint")
    @ConfigDescription("Control plane service-credential authentication endpoint")
    public DuckgresServiceCredentialConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    @FileExists
    public File getTokenFile()
    {
        return tokenFile;
    }

    @Config("duckgres-service-credential.token-file")
    @ConfigDescription("File containing the dedicated control plane authentication token")
    public DuckgresServiceCredentialConfig setTokenFile(File tokenFile)
    {
        this.tokenFile = tokenFile;
        return this;
    }

    public boolean isAllowInsecureHttp()
    {
        return allowInsecureHttp;
    }

    @Config("duckgres-service-credential.allow-insecure-http")
    @ConfigDescription("Allow HTTP only on a trusted private route to the control plane")
    public DuckgresServiceCredentialConfig setAllowInsecureHttp(boolean allowInsecureHttp)
    {
        this.allowInsecureHttp = allowInsecureHttp;
        return this;
    }
}
