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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PasswordAuthenticatorConfig
{
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private List<File> passwordAuthenticatorFiles = ImmutableList.of(new File("etc/password-authenticator.properties"));
    private List<String> hostQualifiedUserDomains = ImmutableList.of();
    private Set<String> hostQualifiedUserExcludedLabels = ImmutableSet.of();

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.password.user-mapping.pattern")
    public PasswordAuthenticatorConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.password.user-mapping.file")
    public PasswordAuthenticatorConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }

    @NotNull
    @NotEmpty(message = "At least one password authenticator config file is required")
    public List<@FileExists File> getPasswordAuthenticatorFiles()
    {
        return passwordAuthenticatorFiles;
    }

    @Config("password-authenticator.config-files")
    @ConfigDescription("Ordered list of password authenticator config files")
    public PasswordAuthenticatorConfig setPasswordAuthenticatorFiles(List<String> passwordAuthenticatorFiles)
    {
        this.passwordAuthenticatorFiles = passwordAuthenticatorFiles.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    @NotNull
    public List<String> getHostQualifiedUserDomains()
    {
        return hostQualifiedUserDomains;
    }

    @Config("http-server.authentication.password.host-qualified-user.domains")
    @ConfigDescription("Domains whose single-label subdomain names the tenant a password login is qualified with")
    public PasswordAuthenticatorConfig setHostQualifiedUserDomains(List<String> hostQualifiedUserDomains)
    {
        this.hostQualifiedUserDomains = ImmutableList.copyOf(hostQualifiedUserDomains);
        return this;
    }

    @NotNull
    public Set<String> getHostQualifiedUserExcludedLabels()
    {
        return hostQualifiedUserExcludedLabels;
    }

    @Config("http-server.authentication.password.host-qualified-user.excluded-labels")
    @ConfigDescription("Subdomain labels of the host-qualified user domains that do not name a tenant")
    public PasswordAuthenticatorConfig setHostQualifiedUserExcludedLabels(Set<String> hostQualifiedUserExcludedLabels)
    {
        this.hostQualifiedUserExcludedLabels = ImmutableSet.copyOf(hostQualifiedUserExcludedLabels);
        return this;
    }

    public HostQualifiedUsers createHostQualifiedUsers()
    {
        return new HostQualifiedUsers(hostQualifiedUserDomains, hostQualifiedUserExcludedLabels);
    }
}
