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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class HoglakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HoglakeConfig.class);
        closingBinder(binder).registerCloseable(HoglakeClient.class);
    }

    @Provides
    @Singleton
    @SuppressWarnings("CloseableProvides") // Registered with ClosingBinder above.
    public static HoglakeClient createClient(HoglakeConfig config)
    {
        return new HoglakeClient(config.getUri(), config.getCatalog(), config.getRequestTimeout());
    }

    @Provides
    @Singleton
    public static HoglakeMetadata createMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory)
    {
        return new HoglakeMetadata(client, fileSystemFactory);
    }

    @Provides
    @Singleton
    public static ConnectorSplitManager createSplitManager(HoglakeClient client)
    {
        return new HoglakeSplitManager(client);
    }

    @Provides
    @Singleton
    public static ConnectorPageSourceProvider createPageSourceProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        return new HoglakePageSourceProvider(fileSystemFactory);
    }

    @Provides
    @Singleton
    public static ConnectorPageSinkProvider createPageSinkProvider(TrinoFileSystemFactory fileSystemFactory, NodeVersion nodeVersion)
    {
        return new HoglakePageSinkProvider(fileSystemFactory, nodeVersion.toString());
    }
}
