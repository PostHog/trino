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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.ducklake.DuckLakePageSourceProvider.withCatalogFooterSize;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

final class DuckLakeSplitSource
        implements ConnectorSplitSource
{
    private final Iterator<DuckLakeSplit> files;
    private final TrinoFileSystem fileSystem;
    private final ParquetReaderOptions parquetReaderOptions;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final long targetSplitBytes;
    private final Executor executor;
    private final Deque<DuckLakeSplit> pendingSplits = new ArrayDeque<>();

    private CompletableFuture<List<ConnectorSplit>> currentBatch;
    private boolean closed;

    public DuckLakeSplitSource(
            List<DuckLakeSplit> files,
            TrinoFileSystem fileSystem,
            ParquetReaderOptions parquetReaderOptions,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            long targetSplitBytes,
            Executor executor)
    {
        this.files = ImmutableList.copyOf(files).iterator();
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        checkArgument(targetSplitBytes > 0, "targetSplitBytes is not positive: %s", targetSplitBytes);
        this.targetSplitBytes = targetSplitBytes;
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        checkState(!closed, "split source is closed");
        checkState(currentBatch == null || currentBatch.isDone(), "previous batch future is not done");
        checkArgument(maxSize > 0, "maxSize must be positive: %s", maxSize);

        if (!pendingSplits.isEmpty()) {
            return completedFuture(removeBatch(maxSize));
        }
        if (!files.hasNext()) {
            return completedFuture(ImmutableList.of());
        }

        DuckLakeSplit file = files.next();
        currentBatch = supplyAsync(() -> planFile(file), executor)
                .thenApply(splits -> {
                    synchronized (this) {
                        if (closed) {
                            return ImmutableList.of();
                        }
                        pendingSplits.addAll(splits);
                        return removeBatch(maxSize);
                    }
                });
        return currentBatch;
    }

    private List<ConnectorSplit> removeBatch(int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> result = ImmutableList.builderWithExpectedSize(Math.min(maxSize, pendingSplits.size()));
        int size = 0;
        while (size < maxSize && !pendingSplits.isEmpty()) {
            result.add(pendingSplits.removeFirst());
            size++;
        }
        return result.build();
    }

    private List<DuckLakeSplit> planFile(DuckLakeSplit file)
    {
        try {
            return planFile(fileSystem, parquetReaderOptions, fileFormatDataSourceStats, targetSplitBytes, file);
        }
        catch (ParquetCorruptionException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet metadata for " + file.path(), e);
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Failed to plan Parquet row groups for " + file.path(), e);
        }
    }

    static List<DuckLakeSplit> planFile(
            TrinoFileSystem fileSystem,
            ParquetReaderOptions parquetReaderOptions,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            long targetSplitBytes,
            DuckLakeSplit file)
            throws IOException
    {
        ParquetReaderOptions options = withCatalogFooterSize(parquetReaderOptions, file.footerSize(), file.fileSizeBytes());
        ParquetMetadata metadata;
        try (TrinoParquetDataSource source = new TrinoParquetDataSource(
                fileSystem.newInputFile(Location.of(file.path()), file.fileSizeBytes()), options, fileFormatDataSourceStats)) {
            metadata = MetadataReader.readFooter(source, options, Optional.empty(), Optional.empty());
        }

        List<DuckLakeRowGroupPlanner.RowGroupSplit> plannedGroups;
        try {
            plannedGroups = DuckLakeRowGroupPlanner.plan(metadata, targetSplitBytes);
        }
        catch (IOException e) {
            throw new ParquetCorruptionException(e, new ParquetDataSourceId(file.path()), "%s", e.getMessage());
        }

        ImmutableList.Builder<DuckLakeSplit> splits = ImmutableList.builderWithExpectedSize(plannedGroups.size());
        for (DuckLakeRowGroupPlanner.RowGroupSplit group : plannedGroups) {
            splits.add(new DuckLakeSplit(
                    file.dataFileId(),
                    file.path(),
                    0,
                    file.fileSizeBytes(),
                    file.fileSizeBytes(),
                    file.footerSize(),
                    group.recordCount(),
                    file.rowIdStart(),
                    file.deleteFile(),
                    file.partitionValues(),
                    file.nameMapping(),
                    DuckLakeSplitManager.splitWeight(group.compressedBytes(), targetSplitBytes),
                    Optional.of(group.metadata())));
        }
        return splits.build();
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || (!files.hasNext() && pendingSplits.isEmpty() && (currentBatch == null || currentBatch.isDone()));
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        pendingSplits.clear();
        if (currentBatch != null) {
            currentBatch.cancel(true);
        }
    }
}
