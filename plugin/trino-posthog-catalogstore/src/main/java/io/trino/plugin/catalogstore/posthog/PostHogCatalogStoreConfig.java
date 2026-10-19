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
package io.trino.plugin.catalogstore.posthog;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PostHogCatalogStoreConfig
{
    private String cellId;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private File connectionPasswordFile;
    private boolean readOnly;
    private Duration snapshotTimeout = new Duration(5, SECONDS);
    private URI delegateEndpoint;
    private File delegateTokenFile;
    private boolean delegateAllowInsecureHttp;
    private Duration delegateRequestTimeout = new Duration(15, SECONDS);
    private Duration delegateVisibilityTimeout = new Duration(3, MINUTES);

    @NotEmpty
    public String getCellId()
    {
        return cellId;
    }

    @Config("catalog-store.cell-id")
    @ConfigDescription("Identifier of the cell this coordinator belongs to, scoping the catalogs it owns")
    public PostHogCatalogStoreConfig setCellId(String cellId)
    {
        this.cellId = cellId;
        return this;
    }

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("catalog-store.connection-url")
    @ConfigDescription("JDBC URL of the PostgreSQL database holding the catalogs, e.g. jdbc:postgresql://host:5432/dbname")
    @ConfigSecuritySensitive
    public PostHogCatalogStoreConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionUser()
    {
        return Optional.ofNullable(connectionUser);
    }

    @Config("catalog-store.connection-user")
    @ConfigDescription("User name for the catalog store database")
    public PostHogCatalogStoreConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionPassword()
    {
        return Optional.ofNullable(connectionPassword);
    }

    @Config("catalog-store.connection-password")
    @ConfigDescription("Password for the catalog store database")
    @ConfigSecuritySensitive
    public PostHogCatalogStoreConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public Optional<@FileExists File> getConnectionPasswordFile()
    {
        return Optional.ofNullable(connectionPasswordFile);
    }

    @Config("catalog-store.connection-password-file")
    @ConfigDescription("File holding the password for the catalog store database, re-read on every connection")
    public PostHogCatalogStoreConfig setConnectionPasswordFile(File connectionPasswordFile)
    {
        this.connectionPasswordFile = connectionPasswordFile;
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Config("catalog-store.read-only")
    @ConfigDescription("Never write to the catalog store: skip the bootstrap DDL and reject catalog mutations, because an external writer publishes the catalogs")
    public PostHogCatalogStoreConfig setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getSnapshotTimeout()
    {
        return snapshotTimeout;
    }

    @Config("catalog-store.snapshot-timeout")
    @ConfigDescription("Budget for a single statement of a read-only store; the writable store keeps waiting as before")
    public PostHogCatalogStoreConfig setSnapshotTimeout(Duration snapshotTimeout)
    {
        this.snapshotTimeout = snapshotTimeout;
        return this;
    }

    public Optional<URI> getDelegateEndpoint()
    {
        return Optional.ofNullable(delegateEndpoint);
    }

    @Config("catalog-store.delegate.endpoint")
    @ConfigDescription("Base URL of the control plane endpoint that CREATE CATALOG and DROP CATALOG are forwarded to; unset rejects them")
    public PostHogCatalogStoreConfig setDelegateEndpoint(URI delegateEndpoint)
    {
        this.delegateEndpoint = delegateEndpoint;
        return this;
    }

    public Optional<@FileExists File> getDelegateTokenFile()
    {
        return Optional.ofNullable(delegateTokenFile);
    }

    @Config("catalog-store.delegate.token-file")
    @ConfigDescription("File holding the bearer token for the catalog request endpoint, re-read on every request")
    public PostHogCatalogStoreConfig setDelegateTokenFile(File delegateTokenFile)
    {
        this.delegateTokenFile = delegateTokenFile;
        return this;
    }

    public boolean isDelegateAllowInsecureHttp()
    {
        return delegateAllowInsecureHttp;
    }

    @Config("catalog-store.delegate.allow-insecure-http")
    @ConfigDescription("Allow HTTP only on a trusted private route to the catalog request endpoint")
    public PostHogCatalogStoreConfig setDelegateAllowInsecureHttp(boolean delegateAllowInsecureHttp)
    {
        this.delegateAllowInsecureHttp = delegateAllowInsecureHttp;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getDelegateRequestTimeout()
    {
        return delegateRequestTimeout;
    }

    @Config("catalog-store.delegate.request-timeout")
    @ConfigDescription("Budget for a single catalog request to the control plane")
    public PostHogCatalogStoreConfig setDelegateRequestTimeout(Duration delegateRequestTimeout)
    {
        this.delegateRequestTimeout = delegateRequestTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getDelegateVisibilityTimeout()
    {
        return delegateVisibilityTimeout;
    }

    @Config("catalog-store.delegate.visibility-timeout")
    @ConfigDescription("How long an accepted catalog request waits for the published change to arrive on this coordinator")
    public PostHogCatalogStoreConfig setDelegateVisibilityTimeout(Duration delegateVisibilityTimeout)
    {
        this.delegateVisibilityTimeout = delegateVisibilityTimeout;
        return this;
    }

    @AssertTrue(message = "catalog-store.delegate.endpoint requires catalog-store.read-only=true")
    public boolean isDelegateOnlyForReadOnlyStore()
    {
        return delegateEndpoint == null || readOnly;
    }

    @AssertTrue(message = "catalog-store.delegate.endpoint requires catalog-store.delegate.token-file")
    public boolean isDelegateTokenFileConfigured()
    {
        return delegateEndpoint == null || delegateTokenFile != null;
    }

    @AssertTrue(message = "catalog-store.connection-password and catalog-store.connection-password-file cannot both be set")
    public boolean isConnectionPasswordConfigurationValid()
    {
        return connectionPassword == null || connectionPasswordFile == null;
    }
}
