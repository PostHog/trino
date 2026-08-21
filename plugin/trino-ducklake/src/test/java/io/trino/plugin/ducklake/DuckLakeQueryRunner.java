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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class DuckLakeQueryRunner
{
    private DuckLakeQueryRunner() {}

    public static Builder builder(TestingDuckLakeCatalog catalog)
    {
        return new Builder()
                .addConnectorProperty("ducklake.metadata.connection-url", catalog.jdbcUrl())
                .addConnectorProperty("ducklake.metadata.connection-user", TestingDuckLakeCatalog.USER)
                .addConnectorProperty("ducklake.metadata.connection-password", TestingDuckLakeCatalog.PASSWORD)
                .addConnectorProperty("ducklake.data-path", "local:///")
                .addConnectorProperty("fs.local.enabled", "true")
                .addConnectorProperty("local.location", catalog.dataPath().toString());
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("ducklake")
                    .setSchema("main")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder removeConnectorProperty(String key)
        {
            connectorProperties.remove(requireNonNull(key, "key is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new DuckLakePlugin());
                queryRunner.createCatalog("ducklake", "ducklake", connectorProperties);
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }
}
