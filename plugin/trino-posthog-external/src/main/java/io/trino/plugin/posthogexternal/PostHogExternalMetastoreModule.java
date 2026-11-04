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
package io.trino.plugin.posthogexternal;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreTableOperationsProvider;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;

import static java.util.Objects.requireNonNull;

/**
 * Gives the Delta connector the PostHog metastore in place of a Hive, Glue or file metastore.
 */
public class PostHogExternalMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final PostHogMetastoreClient client;

    public PostHogExternalMetastoreModule(PostHogMetastoreClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .toInstance(new PostHogExternalMetastoreFactory(client));
        // the cross-query cache stays off unless hive.metastore-cache-ttl is set; see PostHogExternalConnectorFactory
        install(new CachingHiveMetastoreModule());
        // the file metastore's operations only call the HiveMetastore interface, and only on writes, which this catalog refuses
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeFileMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(Integer.MAX_VALUE);
    }
}
