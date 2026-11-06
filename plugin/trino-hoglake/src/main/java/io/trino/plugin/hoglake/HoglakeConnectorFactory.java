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

import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Locale.ENGLISH;

public class HoglakeConnectorFactory
        implements ConnectorFactory
{
    static final String HOGTOWER_REFUSAL = "hoglake.s3-credentials.source=hogtower requires plan 2.8, hogtower's credential vend";

    @Override
    public String getName()
    {
        return "hoglake";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        checkCredentialSource(HoglakeFileSystemConfig.normalize(config));
        ClassLoader classLoader = getClass().getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Injector injector = new Bootstrap(
                    "io.trino.bootstrap.catalog." + catalogName,
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new HoglakeModule(),
                    new FileSystemModule(catalogName, context, false),
                    new ConnectorContextModule(catalogName, context))
                    .doNotInitializeLogging()
                    .disableSystemProperties()
                    .setOptionalConfigurationProperties(HoglakeFileSystemConfig.defaults(config))
                    .setRequiredConfigurationProperties(HoglakeFileSystemConfig.normalize(config))
                    .initialize();

            return new HoglakeConnector(
                    injector.getInstance(HoglakeMetadata.class),
                    new ClassLoaderSafeConnectorSplitManager(injector.getInstance(ConnectorSplitManager.class), classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(injector.getInstance(ConnectorPageSourceProvider.class), classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(injector.getInstance(ConnectorPageSinkProvider.class), classLoader),
                    injector.getInstance(LifeCycleManager.class),
                    injector.getInstance(new Key<Set<SessionPropertiesProvider>>() {}));
        }
    }

    /**
     * With a per-team credential source, no catalog-wide credential may exist
     * that a read could fall back to if a split's credential were lost.
     */
    private static void checkCredentialSource(Map<String, String> config)
    {
        String source = config.getOrDefault("hoglake.s3-credentials.source", "none").trim().toLowerCase(ENGLISH);
        if (source.equals("none")) {
            return;
        }
        if (source.equals("hogtower")) {
            throw new IllegalArgumentException(HOGTOWER_REFUSAL);
        }
        for (String property : List.of("s3.iam-role", "s3.aws-access-key")) {
            if (config.containsKey(property)) {
                throw new IllegalArgumentException("%s cannot be set with hoglake.s3-credentials.source".formatted(property));
            }
        }
        if (Boolean.parseBoolean(config.getOrDefault("s3.security-mapping.enabled", "false").trim())) {
            throw new IllegalArgumentException("s3.security-mapping.enabled cannot be set with hoglake.s3-credentials.source");
        }
    }
}
