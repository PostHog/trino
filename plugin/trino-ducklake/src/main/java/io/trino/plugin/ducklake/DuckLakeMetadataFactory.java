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
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class DuckLakeMetadataFactory
{
    private final JdbcDuckLakeMetastore metastore;
    private final String dataPath;
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec;
    private final JsonCodec<ConnectorViewDefinition> viewDefinitionCodec;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DuckLakeWriterFactory writerFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public DuckLakeMetadataFactory(
            JdbcDuckLakeMetastore metastore,
            DuckLakeConfig config,
            JsonCodec<DuckLakeDataFile> dataFileCodec,
            JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec,
            JsonCodec<ConnectorViewDefinition> viewDefinitionCodec,
            TrinoFileSystemFactory fileSystemFactory,
            DuckLakeWriterFactory writerFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.dataPath = config.getDataPath();
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.mergeFragmentCodec = requireNonNull(mergeFragmentCodec, "mergeFragmentCodec is null");
        this.viewDefinitionCodec = requireNonNull(viewDefinitionCodec, "viewDefinitionCodec is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
    }

    public DuckLakeMetadata create(ConnectorIdentity ignoredIdentity)
    {
        return new DuckLakeMetadata(
                metastore,
                dataPath,
                dataFileCodec,
                mergeFragmentCodec,
                viewDefinitionCodec,
                fileSystemFactory,
                writerFactory,
                fileFormatDataSourceStats,
                parquetReaderOptions);
    }
}
