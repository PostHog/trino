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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class DuckLakeSessionProperties
        implements SessionPropertiesProvider
{
    private static final String FILE_STATISTICS_PRUNING_ENABLED = "file_statistics_pruning_enabled";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_MAX_READ_BLOCK_ROW_COUNT = "parquet_max_read_block_row_count";
    private static final String PARQUET_USE_COLUMN_INDEX = "parquet_use_column_index";
    private static final String PARQUET_IGNORE_STATISTICS = "parquet_ignore_statistics";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DuckLakeSessionProperties(DuckLakeConfig duckLakeConfig, ParquetReaderConfig parquetReaderConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        FILE_STATISTICS_PRUNING_ENABLED,
                        "Prune data files using per-file column statistics from the DuckLake catalog",
                        duckLakeConfig.isFileStatisticsPruningEnabled(),
                        false),
                dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        parquetReaderConfig.getMaxReadBlockSize(),
                        false),
                integerProperty(
                        PARQUET_MAX_READ_BLOCK_ROW_COUNT,
                        "Parquet: Maximum number of rows read in a batch",
                        parquetReaderConfig.getMaxReadBlockRowCount(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_INDEX,
                        "Use Parquet column index",
                        parquetReaderConfig.isUseColumnIndex(),
                        false),
                booleanProperty(
                        PARQUET_IGNORE_STATISTICS,
                        "Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics",
                        parquetReaderConfig.isIgnoreStatistics(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isFileStatisticsPruningEnabled(ConnectorSession session)
    {
        return session.getProperty(FILE_STATISTICS_PRUNING_ENABLED, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static int getParquetMaxReadBlockRowCount(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_ROW_COUNT, Integer.class);
    }

    public static boolean isParquetUseColumnIndex(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_INDEX, Boolean.class);
    }

    public static boolean isParquetIgnoreStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_IGNORE_STATISTICS, Boolean.class);
    }
}
