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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.DataSize;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.format.RowGroup;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Parsed Parquet footers of recently read data files, per worker and per
 * catalog. A large file is read by many byte-range splits, and each one needs
 * the whole footer; without this cache, every split of a file with hundreds of
 * row groups decodes the same multi-megabyte footer again. Caching the footer
 * bytes alone would save only the read, not the Thrift decode that dominates,
 * so this keeps the decoded {@link ParquetMetadata}.
 *
 * <p>Entries are weighed by their serialized footer size. The decoded objects
 * are larger than that, so the heap they hold is a multiple of the configured
 * bound. A footer weighing more than the whole bound is never kept.
 *
 * <p>The cache's hit and miss statistics are exported over JMX, and each
 * lookup reports whether it was a hit, so a split can report it too.
 */
public final class HoglakeParquetFooterCache
{
    private final Cache<Key, ParsedFooter> cache;

    public HoglakeParquetFooterCache(DataSize maxSize)
    {
        long maxSizeBytes = maxSize.toBytes();
        if (maxSizeBytes == 0) {
            this.cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(0)
                    .shareNothingWhenDisabled()
                    .recordStats()
                    .build();
        }
        else {
            this.cache = EvictableCacheBuilder.newBuilder()
                    .maximumWeight(maxSizeBytes)
                    // Guava divides the weight bound among segments; a single segment makes the
                    // bound apply to the cache as a whole, so any footer up to it can be kept.
                    // Contention is low: a worker loads each file's footer once.
                    .concurrencyLevel(1)
                    .weigher((Key _, ParsedFooter footer) -> footer.weight())
                    .recordStats()
                    .build();
        }
    }

    /**
     * A cache that keeps nothing: every lookup loads.
     */
    public static HoglakeParquetFooterCache disabled()
    {
        return new HoglakeParquetFooterCache(DataSize.ofBytes(0));
    }

    /**
     * The file's parsed footer, loaded on a miss. Concurrent lookups of one
     * file share a single load. A load that fails is not cached; its failure
     * is thrown to the callers waiting on it, and the next lookup loads again.
     *
     * <p>The lookup is a miss exactly when it ran the loader itself: the
     * loader runs on the calling thread, and only for the one lookup that
     * loads. A lookup that waits on another's load decodes nothing, so it is
     * a hit here, although the cache's statistics count it as a miss.
     */
    public Lookup get(Key key, FooterLoader loader)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(loader, "loader is null");
        AtomicBoolean loaded = new AtomicBoolean();
        try {
            ParsedFooter footer = cache.get(key, () -> {
                loaded.set(true);
                return loader.load();
            });
            return new Lookup(footer, !loaded.get());
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            Throwable cause = e.getCause();
            throwIfInstanceOf(cause, IOException.class);
            throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        }
    }

    @VisibleForTesting
    boolean contains(Key key)
    {
        return cache.getIfPresent(key) != null;
    }

    @VisibleForTesting
    long totalWeight()
    {
        return cache.asMap().values().stream()
                .mapToLong(ParsedFooter::weight)
                .sum();
    }

    @VisibleForTesting
    CacheStats stats()
    {
        return cache.stats();
    }

    @Managed
    @Nested
    public CacheStatsMBean getCacheStats()
    {
        return new CacheStatsMBean(cache);
    }

    /**
     * A footer and whether this lookup found it without loading it.
     */
    public record Lookup(ParsedFooter footer, boolean hit)
    {
        public Lookup
        {
            requireNonNull(footer, "footer is null");
        }
    }

    /**
     * A data file's path is immutable per data file, but the size is part of
     * the key too, so a footer is never served for a file of another length.
     */
    public record Key(String path, long fileSizeBytes)
    {
        public Key
        {
            requireNonNull(path, "path is null");
            checkArgument(fileSizeBytes >= 0, "fileSizeBytes is negative: %s", fileSizeBytes);
        }
    }

    /**
     * A decoded footer, the file's physical row count, and the footer's
     * serialized size, which is its weight in the cache.
     *
     * <p>The metadata is shared read-only by every split of the file on this
     * worker. That is safe only because Hoglake reads no encrypted files, so
     * it carries no per-reader decryption context.
     */
    public record ParsedFooter(ParquetMetadata metadata, long fileRowCount, int weight)
    {
        public ParsedFooter
        {
            requireNonNull(metadata, "metadata is null");
            checkArgument(fileRowCount >= 0, "fileRowCount is negative: %s", fileRowCount);
            checkArgument(weight >= 0, "weight is negative: %s", weight);
        }

        /**
         * Counts the file's rows once, so every range of the file can bound
         * its deletion vector without walking the row groups again. The count
         * sums the row groups' Thrift row counts, as {@link ParquetMetadata#getBlocks()}
         * does, without building the metadata of every column chunk.
         */
        public static ParsedFooter of(ParquetMetadata metadata, int footerSize)
        {
            long fileRowCount = 0;
            List<RowGroup> rowGroups = metadata.getParquetMetadata().getRow_groups();
            if (rowGroups != null) {
                for (RowGroup rowGroup : rowGroups) {
                    fileRowCount = Math.addExact(fileRowCount, rowGroup.getNum_rows());
                }
            }
            return new ParsedFooter(metadata, fileRowCount, footerSize);
        }
    }

    @FunctionalInterface
    public interface FooterLoader
    {
        ParsedFooter load()
                throws IOException;
    }
}
