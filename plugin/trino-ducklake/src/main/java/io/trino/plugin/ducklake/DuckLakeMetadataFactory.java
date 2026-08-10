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

import com.google.inject.Inject;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class DuckLakeMetadataFactory
{
    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;

    @Inject
    public DuckLakeMetadataFactory(JdbcDuckLakeMetastore metastore, DuckLakeConfig config)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = config.getDataPath();
    }

    public DuckLakeMetadata create(ConnectorIdentity ignoredIdentity)
    {
        return new DuckLakeMetadata(metastore, dataPath);
    }
}
