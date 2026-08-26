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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_TOO_MANY_OPEN_PARTITIONS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Writes rows into the data files of one DuckLake table.
 * <p>
 * A writer is kept open per partition the pages touch, and is rolled over into a new file once it
 * reaches the table's target file size, so that a long-running insert produces files of a size
 * readers can split evenly rather than one very large file.
 */
public class DuckLakePageSink
        implements ConnectorPageSink
{
    private final ConnectorSession session;
    private final TrinoFileSystem fileSystem;
    private final DuckLakeWriterFactory writerFactory;
    private final DuckLakeParquetSchema schema;
    private final String tableLocation;
    private final long targetFileSizeBytes;
    private final int maxOpenWriters;
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final Optional<DuckLakePartitioner> partitioner;
    private final PageIndexer pageIndexer;

    private final List<DuckLakeFileWriter> writers = new ArrayList<>();
    private final List<List<Optional<String>>> writerPartitionValues = new ArrayList<>();
    private final List<String> writerPartitionPaths = new ArrayList<>();
    private final ImmutableList.Builder<Slice> completedFiles = ImmutableList.builder();

    private long writtenBytes;
    private long memoryUsage;

    public DuckLakePageSink(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            DuckLakeWriterFactory writerFactory,
            PageIndexerFactory pageIndexerFactory,
            DuckLakeParquetSchema schema,
            Optional<DuckLakePartitioner> partitioner,
            String tableLocation,
            long targetFileSizeBytes,
            int maxOpenWriters,
            JsonCodec<DuckLakeDataFile> dataFileCodec)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.maxOpenWriters = maxOpenWriters;
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.pageIndexer = pageIndexerFactory.createPageIndexer(partitioner
                .map(DuckLakePartitioner::partitionTypes)
                .orElse(ImmutableList.of()));
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }
        if (partitioner.isEmpty()) {
            appendToWriter(0, page, ImmutableList.of(), "");
            return NOT_BLOCKED;
        }

        DuckLakePartitioner partitioning = partitioner.get();
        Page partitionColumns = partitioning.partitionColumns(page);
        int[] writerIndexes = pageIndexer.indexPage(partitionColumns);
        int writerCount = pageIndexer.getMaxIndex() + 1;
        if (writerCount > maxOpenWriters) {
            throw new TrinoException(DUCKLAKE_TOO_MANY_OPEN_PARTITIONS, "Exceeded limit of %s open writers for partitions".formatted(maxOpenWriters));
        }

        int[] sizes = new int[writerCount];
        for (int writerIndex : writerIndexes) {
            sizes[writerIndex]++;
        }
        int[][] positions = new int[writerCount][];
        for (int writerIndex = 0; writerIndex < writerCount; writerIndex++) {
            if (sizes[writerIndex] > 0) {
                positions[writerIndex] = new int[sizes[writerIndex]];
            }
        }
        int[] offsets = new int[writerCount];
        for (int position = 0; position < writerIndexes.length; position++) {
            int writerIndex = writerIndexes[position];
            positions[writerIndex][offsets[writerIndex]++] = position;
        }

        for (int writerIndex = 0; writerIndex < writerCount; writerIndex++) {
            if (positions[writerIndex] == null) {
                continue;
            }
            int firstPosition = positions[writerIndex][0];
            List<Optional<String>> partitionValues = partitioning.partitionValues(partitionColumns, firstPosition);
            appendToWriter(
                    writerIndex,
                    page.getPositions(positions[writerIndex], 0, positions[writerIndex].length),
                    partitionValues,
                    partitioning.partitionPath(partitionValues));
        }
        return NOT_BLOCKED;
    }

    private void appendToWriter(int writerIndex, Page page, List<Optional<String>> partitionValues, String partitionPath)
    {
        while (writers.size() <= writerIndex) {
            writers.add(null);
            writerPartitionValues.add(ImmutableList.of());
            writerPartitionPaths.add("");
        }
        DuckLakeFileWriter writer = writers.get(writerIndex);
        if (writer == null) {
            writerPartitionValues.set(writerIndex, partitionValues);
            writerPartitionPaths.set(writerIndex, partitionPath);
            writer = openWriter(writerIndex);
        }

        long previousWrittenBytes = writer.writtenBytes();
        long previousMemoryUsage = writer.memoryUsage();
        writer.appendRows(page);
        writtenBytes += writer.writtenBytes() - previousWrittenBytes;
        memoryUsage += writer.memoryUsage() - previousMemoryUsage;

        if (writer.writtenBytes() >= targetFileSizeBytes) {
            memoryUsage -= writer.memoryUsage();
            completedFiles.add(toFragment(writer.close()));
            writers.set(writerIndex, null);
        }
    }

    private DuckLakeFileWriter openWriter(int writerIndex)
    {
        String relativePath = writerPartitionPaths.get(writerIndex) + DuckLakeWriterFactory.newDataFileName();
        DuckLakeFileWriter writer = writerFactory.createWriter(
                session,
                fileSystem,
                tableLocation,
                schema,
                relativePath,
                writerPartitionValues.get(writerIndex));
        writers.set(writerIndex, writer);
        memoryUsage += writer.memoryUsage();
        return writer;
    }

    private Slice toFragment(DuckLakeDataFile dataFile)
    {
        return wrappedBuffer(dataFileCodec.toJsonBytes(dataFile));
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        for (int writerIndex = 0; writerIndex < writers.size(); writerIndex++) {
            DuckLakeFileWriter writer = writers.get(writerIndex);
            if (writer == null) {
                continue;
            }
            writers.set(writerIndex, null);
            if (writer.recordCount() == 0) {
                // a writer that never received a row would produce an empty file with nothing to register
                writer.rollback();
                continue;
            }
            completedFiles.add(toFragment(writer.close()));
        }
        memoryUsage = 0;
        return completedFuture(completedFiles.build());
    }

    @Override
    public void abort()
    {
        RuntimeException failure = null;
        for (DuckLakeFileWriter writer : writers) {
            if (writer == null) {
                continue;
            }
            try {
                writer.rollback();
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        writers.clear();
        memoryUsage = 0;
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Splits the partition columns out of a page and renders their values, both as the strings the
     * catalog records and as the directory layout the files are written into.
     */
    public interface DuckLakePartitioner
    {
        List<Type> partitionTypes();

        Page partitionColumns(Page page);

        List<Optional<String>> partitionValues(Page partitionColumns, int position);

        String partitionPath(List<Optional<String>> partitionValues);
    }
}
