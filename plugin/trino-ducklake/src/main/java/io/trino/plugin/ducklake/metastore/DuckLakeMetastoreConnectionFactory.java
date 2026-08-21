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
package io.trino.plugin.ducklake.metastore;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.ConnectionFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;
import static java.util.Objects.requireNonNull;

/**
 * Opens a new physical connection to the DuckLake catalog database. Pooling is layered on top by
 * {@link PooledDuckLakeConnectionFactory}.
 */
public class DuckLakeMetastoreConnectionFactory
        implements ConnectionFactory
{
    /**
     * Reported to PostgreSQL as {@code application_name}, so that connections opened by this
     * connector are recognizable in {@code pg_stat_activity}.
     */
    public static final String APPLICATION_NAME = "Trino DuckLake";

    private final Driver driver;
    private final String connectionUrl;
    private final Optional<String> connectionUser;
    private final Optional<String> connectionPassword;
    private final Optional<File> connectionPasswordFile;

    public DuckLakeMetastoreConnectionFactory(
            Driver driver,
            String connectionUrl,
            Optional<String> connectionUser,
            Optional<String> connectionPassword,
            Optional<File> connectionPasswordFile)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionUser = requireNonNull(connectionUser, "connectionUser is null");
        this.connectionPassword = requireNonNull(connectionPassword, "connectionPassword is null");
        this.connectionPasswordFile = requireNonNull(connectionPasswordFile, "connectionPasswordFile is null");
        checkArgument(
                connectionPassword.isEmpty() || connectionPasswordFile.isEmpty(),
                "connectionPassword and connectionPasswordFile cannot both be set");
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Properties properties = new Properties();
        connectionUser.ifPresent(user -> properties.setProperty("user", user));
        password().ifPresent(password -> properties.setProperty("password", password));
        properties.setProperty("ApplicationName", APPLICATION_NAME);
        Connection connection = driver.connect(connectionUrl, properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL is valid");
        return connection;
    }

    /**
     * The password file is read for every physical connection, so that a rotated credential is
     * picked up without restarting Trino or recreating the catalog.
     */
    private Optional<String> password()
    {
        if (connectionPasswordFile.isEmpty()) {
            return connectionPassword;
        }
        File file = connectionPasswordFile.get();
        try {
            return Optional.of(trimTrailingNewlines(readString(file.toPath(), UTF_8)));
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to read DuckLake metadata connection password file: " + file, e);
        }
    }

    /**
     * Removes the trailing line terminators that a Kubernetes secret file, {@code echo} or an
     * editor commonly appends. Other characters, including trailing spaces, are part of the
     * password and are preserved.
     */
    @VisibleForTesting
    static String trimTrailingNewlines(String value)
    {
        int end = value.length();
        while (end > 0 && (value.charAt(end - 1) == '\n' || value.charAt(end - 1) == '\r')) {
            end--;
        }
        return value.substring(0, end);
    }
}
