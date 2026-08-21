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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class PostHogCatalogStoreConfig
{
    private String cellId;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private File connectionPasswordFile;

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

    @AssertTrue(message = "catalog-store.connection-password and catalog-store.connection-password-file cannot both be set")
    public boolean isConnectionPasswordConfigurationValid()
    {
        return connectionPassword == null || connectionPasswordFile == null;
    }
}
