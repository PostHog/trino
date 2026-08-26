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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_WRITER_ERROR;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetWriterBatchSize;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getParquetWriterPageValueCount;
import static io.trino.plugin.ducklake.util.PathResolver.resolve;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;

/**
 * Opens DuckLake data files for writing.
 * <p>
 * Files are named the way DuckDB names them, {@code ducklake-<uuid>.parquet}, and are addressed
 * relative to the table's location so that the catalog stays independent of where the table lives.
 * The identifier is random rather than time-ordered, which spreads the names evenly across the
 * key space of an object store.
 */
public class DuckLakeWriterFactory
{
    private static final String FILE_NAME_PREFIX = "ducklake-";
    private static final String DATA_FILE_SUFFIX = ".parquet";
    private static final String DELETE_FILE_SUFFIX = "-delete.parquet";

    private final NodeVersion nodeVersion;
    private final ParquetWriterConfig parquetWriterConfig;

    @Inject
    public DuckLakeWriterFactory(NodeVersion nodeVersion, ParquetWriterConfig parquetWriterConfig)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.parquetWriterConfig = requireNonNull(parquetWriterConfig, "parquetWriterConfig is null");
    }

    public static String newDataFileName()
    {
        return FILE_NAME_PREFIX + UUID.randomUUID() + DATA_FILE_SUFFIX;
    }

    public static String newDeleteFileName()
    {
        return FILE_NAME_PREFIX + UUID.randomUUID() + DELETE_FILE_SUFFIX;
    }

    /**
     * Opens a data file of the table, at a path relative to the table's location.
     */
    public DuckLakeFileWriter createWriter(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            String tableLocation,
            DuckLakeParquetSchema schema,
            String relativePath,
            List<Optional<String>> partitionValues)
    {
        return new DuckLakeFileWriter(
                createParquetWriter(session, fileSystem, resolve(tableLocation, relativePath, true), schema),
                schema,
                relativePath,
                partitionValues);
    }

    public ParquetFileWriter createParquetWriter(ConnectorSession session, TrinoFileSystem fileSystem, String path, DuckLakeParquetSchema schema)
    {
        Location location = Location.of(path);
        try {
            TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
            RollbackAction rollbackAction = () -> fileSystem.deleteFile(location);
            ParquetWriterOptions options = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxPageValueCount(getParquetWriterPageValueCount(session))
                    .setMaxBlockSize(getParquetWriterBlockSize(session))
                    .setBatchSize(getParquetWriterBatchSize(session))
                    .build();
            return new ParquetFileWriter(
                    outputFile,
                    rollbackAction,
                    schema.fileColumnTypes(),
                    schema.fileColumnNames(),
                    schema.messageType(),
                    schema.primitiveTypes(),
                    options,
                    IntStream.range(0, schema.fileColumnNames().size()).toArray(),
                    // DuckDB writes Snappy by default and every DuckLake reader supports it
                    SNAPPY,
                    nodeVersion.toString(),
                    Optional.empty(),
                    Optional.empty());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(DUCKLAKE_WRITER_ERROR, "Failed to create Parquet file " + path, e);
        }
    }

    public ParquetWriterConfig parquetWriterConfig()
    {
        return parquetWriterConfig;
    }
}
