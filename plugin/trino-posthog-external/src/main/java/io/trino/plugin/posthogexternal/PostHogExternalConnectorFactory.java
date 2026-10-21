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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Scopes;
import io.trino.plugin.deltalake.DeltaLakeConnectorFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

/**
 * Delta Lake over tables in tenant buckets, with schemas and table registrations stored by PostHog.
 * <p>
 * The cross-query metastore cache stays off by default ({@code hive.metastore-cache-ttl=0s}),
 * so a registration is visible to the next query on the same coordinator.
 * Every metastore lookup is one REST call; within one query Delta still caches it per transaction.
 */
public class PostHogExternalConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "posthog_external";
    static final String PREFIX = "posthog-external.";
    static final String METASTORE_URI = PREFIX + "metastore-uri";
    static final String METASTORE_TOKEN_FILE = PREFIX + "metastore-token-file";
    static final String CATALOG_ID = PREFIX + "catalog-id";

    // forced: this catalog brings its own access control, see PostHogExternalAccessControl
    private static final Map<String, String> FORCED = ImmutableMap.of("delta.security", "system");
    private static final Map<String, String> DEFAULTS = ImmutableMap.of(
            "delta.register-table-procedure.enabled", "true",
            "hive.metastore-cache-ttl", "0s");

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");
        Map<String, String> deltaConfig = new HashMap<>(DEFAULTS);
        config.forEach((key, value) -> {
            if (key.startsWith(PREFIX)) {
                return;
            }
            if (FORCED.containsKey(key) && !FORCED.get(key).equalsIgnoreCase(value)) {
                throw new TrinoException(CONFIGURATION_INVALID, "Catalog property %s cannot be set for the %s connector".formatted(key, CONNECTOR_NAME));
            }
            deltaConfig.put(key, value);
        });
        deltaConfig.putAll(FORCED);
        for (String key : config.keySet()) {
            if (key.startsWith(PREFIX) && !key.equals(METASTORE_URI) && !key.equals(METASTORE_TOKEN_FILE) && !key.equals(CATALOG_ID)) {
                throw new TrinoException(CONFIGURATION_INVALID, "Unknown catalog property " + key);
            }
        }

        PostHogMetastoreClient client = new PostHogMetastoreClient(
                URI.create(required(config, METASTORE_URI)),
                Path.of(required(config, METASTORE_TOKEN_FILE)),
                required(config, CATALOG_ID));

        Connector connector = DeltaLakeConnectorFactory.createConnector(
                catalogName,
                ImmutableMap.copyOf(deltaConfig),
                context,
                Optional.of(new PostHogExternalMetastoreModule(client)),
                binder -> binder.bind(ConnectorAccessControl.class).to(PostHogExternalAccessControl.class).in(Scopes.SINGLETON));
        return CreateTableRefusingConnector.wrap(connector);
    }

    private static String required(Map<String, String> config, String key)
    {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new TrinoException(CONFIGURATION_INVALID, "Catalog property %s is required".formatted(key));
        }
        return value;
    }
}
