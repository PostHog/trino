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
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DuckLakeConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private File connectionPasswordFile;
    private int connectionPoolMaxSize = 10;
    private Duration connectionPoolIdleTimeout = new Duration(1, MINUTES);
    private Duration connectionPoolAcquisitionTimeout = new Duration(30, SECONDS);
    private String metadataSchema = "public";
    private String dataPath;
    private boolean fileStatisticsPruningEnabled = true;
    private DataSize maxSplitSize = DataSize.of(64, MEGABYTE);
    private DataSize targetMaxFileSize = DataSize.of(128, MEGABYTE);
    private int maxOpenPartitions = 100;

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

    public Optional<@FileExists File> getConnectionPasswordFile()
    {
        return Optional.ofNullable(connectionPasswordFile);
    }

    @Config("ducklake.metadata.connection-password-file")
    @ConfigDescription("File holding the password for the DuckLake catalog database, re-read on every connection")
    public DuckLakeConfig setConnectionPasswordFile(File connectionPasswordFile)
    {
        this.connectionPasswordFile = connectionPasswordFile;
        return this;
    }

    @AssertTrue(message = "ducklake.metadata.connection-password and ducklake.metadata.connection-password-file cannot both be set")
    public boolean isConnectionPasswordConfigurationValid()
    {
        return connectionPassword == null || connectionPasswordFile == null;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("ducklake.metadata.connection-pool.max-size")
    @ConfigDescription("Maximum number of connections this catalog keeps open to the DuckLake catalog database")
    public DuckLakeConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getConnectionPoolIdleTimeout()
    {
        return connectionPoolIdleTimeout;
    }

    @Config("ducklake.metadata.connection-pool.idle-timeout")
    @ConfigDescription("Close pooled connections idle for longer than this, so that an unused catalog holds no connections")
    public DuckLakeConfig setConnectionPoolIdleTimeout(Duration connectionPoolIdleTimeout)
    {
        this.connectionPoolIdleTimeout = connectionPoolIdleTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getConnectionPoolAcquisitionTimeout()
    {
        return connectionPoolAcquisitionTimeout;
    }

    @Config("ducklake.metadata.connection-pool.acquisition-timeout")
    @ConfigDescription("Fail a query that waits longer than this for a connection from the pool")
    public DuckLakeConfig setConnectionPoolAcquisitionTimeout(Duration connectionPoolAcquisitionTimeout)
    {
        this.connectionPoolAcquisitionTimeout = connectionPoolAcquisitionTimeout;
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

    @NotNull
    @MinDataSize("1kB")
    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("ducklake.max-split-size")
    @ConfigDescription("Target size of a split; larger data files are read as several byte ranges in parallel")
    public DuckLakeConfig setMaxSplitSize(DataSize maxSplitSize)
    {
        this.maxSplitSize = maxSplitSize;
        return this;
    }

    @NotNull
    @MinDataSize("1kB")
    public DataSize getTargetMaxFileSize()
    {
        return targetMaxFileSize;
    }

    @Config("ducklake.target-max-file-size")
    @ConfigDescription("Roll over to a new data file once the current one reaches this size")
    public DuckLakeConfig setTargetMaxFileSize(DataSize targetMaxFileSize)
    {
        this.targetMaxFileSize = targetMaxFileSize;
        return this;
    }

    @Min(1)
    public int getMaxOpenPartitions()
    {
        return maxOpenPartitions;
    }

    @Config("ducklake.max-open-partitions")
    @ConfigDescription("Maximum number of partitions a single writer keeps open while writing")
    public DuckLakeConfig setMaxOpenPartitions(int maxOpenPartitions)
    {
        this.maxOpenPartitions = maxOpenPartitions;
        return this;
    }
}
