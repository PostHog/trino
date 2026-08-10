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
package io.trino.plugin.ducklake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class DuckLakeConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String metadataSchema = "public";
    private String dataPath;
    private boolean fileStatisticsPruningEnabled = true;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("ducklake.metadata.connection-url")
    @ConfigDescription("JDBC URL of the DuckLake catalog database, e.g. jdbc:postgresql://host:5432/dbname")
    @ConfigSecuritySensitive
    public DuckLakeConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionUser()
    {
        return Optional.ofNullable(connectionUser);
    }

    @Config("ducklake.metadata.connection-user")
    @ConfigDescription("User name for the DuckLake catalog database")
    public DuckLakeConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionPassword()
    {
        return Optional.ofNullable(connectionPassword);
    }

    @Config("ducklake.metadata.connection-password")
    @ConfigDescription("Password for the DuckLake catalog database")
    @ConfigSecuritySensitive
    public DuckLakeConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @NotEmpty
    public String getMetadataSchema()
    {
        return metadataSchema;
    }

    @Config("ducklake.metadata.schema")
    @ConfigDescription("Schema in the catalog database holding the ducklake_* metadata tables")
    public DuckLakeConfig setMetadataSchema(String metadataSchema)
    {
        this.metadataSchema = metadataSchema;
        return this;
    }

    @NotNull
    public String getDataPath()
    {
        return dataPath;
    }

    @Config("ducklake.data-path")
    @ConfigDescription("Base location of the DuckLake data files, e.g. s3://bucket/prefix/")
    public DuckLakeConfig setDataPath(String dataPath)
    {
        this.dataPath = dataPath;
        return this;
    }

    public boolean isFileStatisticsPruningEnabled()
    {
        return fileStatisticsPruningEnabled;
    }

    @Config("ducklake.file-statistics-pruning.enabled")
    @ConfigDescription("Prune data files using per-file column statistics from the DuckLake catalog")
    public DuckLakeConfig setFileStatisticsPruningEnabled(boolean fileStatisticsPruningEnabled)
    {
        this.fileStatisticsPruningEnabled = fileStatisticsPruningEnabled;
        return this;
    }
}
