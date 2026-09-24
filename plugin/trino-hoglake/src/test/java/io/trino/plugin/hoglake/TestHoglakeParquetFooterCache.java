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

import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.HoglakeParquetFooterCache.Key;
import io.trino.plugin.hoglake.HoglakeParquetFooterCache.ParsedFooter;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeParquetFooterCache
{
    private static final int ROWS = 25;
    private static final byte[] FILE = ConnectorTestFixtures.writeParquet(
            List.of(new FileColumn(optional(INT64).id(1).named("value"), BIGINT, new ArrayList<>(LongStream.range(0, ROWS).boxed().toList()))),
            ParquetWriterOptions.builder().setMaxRowGroupRowCount(10).build());
    private static final ParquetMetadata METADATA = parse(FILE);

    @Test
    void parsedFooterCountsTheRowsOfEveryRowGroup()
            throws IOException
    {
        assertThat(METADATA.getBlocks()).hasSize(3);
        ParsedFooter footer = ParsedFooter.of(METADATA, 123);
        assertThat(footer.fileRowCount()).isEqualTo(ROWS);
        assertThat(footer.weight()).isEqualTo(123);
    }

    @Test
    void aHitDoesNotLoadAgain()
            throws IOException
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        AtomicInteger loads = new AtomicInteger();
        Key key = new Key("memory:///a.parquet", FILE.length);

        ParsedFooter first = cache.get(key, () -> footer(loads, 100));
        ParsedFooter second = cache.get(key, () -> footer(loads, 100));

        assertThat(loads).hasValue(1);
        assertThat(second).isSameAs(first);
        assertThat(cache.stats().hitCount()).isEqualTo(1);
    }

    @Test
    void theFileSizeIsPartOfTheKey()
            throws IOException
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        AtomicInteger loads = new AtomicInteger();

        cache.get(new Key("memory:///a.parquet", 10), () -> footer(loads, 100));
        cache.get(new Key("memory:///a.parquet", 11), () -> footer(loads, 100));
        cache.get(new Key("memory:///b.parquet", 10), () -> footer(loads, 100));

        assertThat(loads).hasValue(3);
    }

    @Test
    void aDisabledCacheLoadsEveryTimeAndKeepsNothing()
            throws IOException
    {
        HoglakeParquetFooterCache cache = HoglakeParquetFooterCache.disabled();
        AtomicInteger loads = new AtomicInteger();
        Key key = new Key("memory:///a.parquet", FILE.length);

        cache.get(key, () -> footer(loads, 100));
        cache.get(key, () -> footer(loads, 100));

        assertThat(loads).hasValue(2);
        assertThat(cache.contains(key)).isFalse();
        assertThat(cache.totalWeight()).isZero();
    }

    @Test
    void aFailedLoadIsThrownUnwrappedAndNotCached()
            throws IOException
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        Key key = new Key("memory:///a.parquet", FILE.length);
        IOException ioFailure = new IOException("test footer read failure");
        IllegalStateException uncheckedFailure = new IllegalStateException("test footer parse failure");

        assertThatThrownBy(() -> cache.get(key, () -> {
            throw ioFailure;
        })).isSameAs(ioFailure);
        assertThatThrownBy(() -> cache.get(key, () -> {
            throw uncheckedFailure;
        })).isSameAs(uncheckedFailure);
        assertThat(cache.contains(key)).isFalse();

        AtomicInteger loads = new AtomicInteger();
        cache.get(key, () -> footer(loads, 100));
        assertThat(loads).hasValue(1);
        assertThat(cache.contains(key)).isTrue();
    }

    @Test
    void aFooterHeavierThanTheWholeBoundIsNotKept()
            throws IOException
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        AtomicInteger loads = new AtomicInteger();
        Key small = new Key("memory:///small.parquet", 1);
        Key exact = new Key("memory:///exact.parquet", 1);
        Key oversized = new Key("memory:///oversized.parquet", 1);

        cache.get(small, () -> footer(loads, 1));
        ParsedFooter loaded = cache.get(oversized, () -> footer(loads, 1001));

        // The caller still gets the footer it loaded; the cache does not keep it,
        // and keeping out an oversized footer does not evict the others.
        assertThat(loaded.weight()).isEqualTo(1001);
        assertThat(cache.contains(oversized)).isFalse();
        assertThat(cache.contains(small)).isTrue();

        // A footer as heavy as the whole bound fits, even though it evicts the rest.
        cache.get(exact, () -> footer(loads, 1000));
        assertThat(cache.contains(exact)).isTrue();
        assertThat(cache.totalWeight()).isEqualTo(1000);
    }

    @Test
    void theTotalWeightStaysWithinTheBound()
            throws IOException
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        AtomicInteger loads = new AtomicInteger();
        for (int file = 0; file < 20; file++) {
            Key key = new Key("memory:///file-" + file + ".parquet", FILE.length);
            cache.get(key, () -> footer(loads, 300));
            assertThat(cache.totalWeight()).isLessThanOrEqualTo(1000);
            assertThat(cache.contains(key)).isTrue();
        }
        // Three footers of 300 fit in 1000; the least recently used ones were evicted.
        assertThat(cache.totalWeight()).isEqualTo(900);
        assertThat(cache.contains(new Key("memory:///file-0.parquet", FILE.length))).isFalse();
    }

    @Test
    void concurrentLookupsOfOneFileShareOneLoad()
            throws Exception
    {
        HoglakeParquetFooterCache cache = new HoglakeParquetFooterCache(DataSize.ofBytes(1000));
        Key key = new Key("memory:///a.parquet", FILE.length);
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        AtomicReference<ParsedFooter> firstResult = new AtomicReference<>();
        Thread first = new Thread(() -> {
            try {
                firstResult.set(cache.get(key, () -> {
                    loading.countDown();
                    awaitUninterruptibly(release);
                    return footer(loads, 100);
                }));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        first.start();
        loading.await();

        AtomicReference<ParsedFooter> secondResult = new AtomicReference<>();
        Thread second = new Thread(() -> {
            try {
                secondResult.set(cache.get(key, () -> footer(loads, 100)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        second.start();
        // The second lookup waits on the first one's load rather than starting its own.
        long deadline = System.nanoTime() + SECONDS.toNanos(10);
        while (second.getState() != Thread.State.WAITING && second.getState() != Thread.State.TIMED_WAITING) {
            assertThat(System.nanoTime() - deadline)
                    .describedAs("the second lookup did not wait on the first one's load")
                    .isNegative();
            Thread.onSpinWait();
        }
        release.countDown();
        first.join();
        second.join();

        assertThat(loads).hasValue(1);
        assertThat(secondResult.get()).isSameAs(firstResult.get());
    }

    private static ParsedFooter footer(AtomicInteger loads, int weight)
    {
        loads.incrementAndGet();
        return ParsedFooter.of(METADATA, weight);
    }

    private static ParquetMetadata parse(byte[] file)
    {
        try {
            return MetadataReader.parseFooter(new ParquetDataSourceId("test"), file.length, Slices.wrappedBuffer(file), Optional.empty(), Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
