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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.ducklake.metastore.DuckLakeMetastoreConnectionFactory;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import org.jdbi.v3.core.ConnectionFactory;

import java.sql.Driver;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DuckLakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DuckLakeTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(DuckLakeConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(DuckLakeSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(DuckLakeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(DuckLakePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(JdbcDuckLakeMetastore.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeMetadataFactory.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static ConnectionFactory createConnectionFactory(DuckLakeConfig config)
    {
        Driver driver;
        try {
            driver = (Driver) Class.forName("org.postgresql.Driver").getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to load PostgreSQL JDBC driver", e);
        }
        return new DuckLakeMetastoreConnectionFactory(
                driver,
                config.getConnectionUrl(),
                config.getConnectionUser(),
                config.getConnectionPassword());
    }
}
