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

import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Byte-range splits read through the real page-source path
 * (HoglakePageSourceProvider -> trino-parquet -> memory filesystem) over a
 * Parquet file with many row groups. Whatever the cuts, the ranges of a file
 * together return every row exactly once.
 */
class TestHoglakeRangeReads
{
    private static final String PATH = "memory:///ranges/data.parquet";
    private static final int ROWS = 100;
    private static final int ROWS_PER_ROW_GROUP = 10;
    private static final HoglakeColumnHandle VALUE = new HoglakeColumnHandle("value", 1, BIGINT, true);

    private static final byte[] FILE = ConnectorTestFixtures.writeParquet(
            List.of(new FileColumn(optional(INT64).id(1).named("value"), BIGINT, new ArrayList<>(allValues()))),
            ParquetWriterOptions.builder().setMaxRowGroupRowCount(ROWS_PER_ROW_GROUP).build());
    private static final List<Long> ROW_GROUP_OFFSETS = ConnectorTestFixtures.rowGroupOffsets(FILE);
    private static final HoglakeSplit WHOLE_FILE = new HoglakeSplit(PATH, FILE.length, ROWS, Optional.empty(), 0);

    @Test
    void fixtureHasOneRowGroupPerTenRows()
    {
        assertThat(ROW_GROUP_OFFSETS).hasSize(ROWS / ROWS_PER_ROW_GROUP);
        assertThat(values(read(WHOLE_FILE, List.of(VALUE), TupleDomain.all()))).containsExactlyElementsOf(allValues());
    }

    @Test
    void adjacentRangesReturnTheWholeFileOnce()
    {
        // Without the footer cache every range parses the footer; with it the
        // second range reuses the first one's parse. The rows must not differ.
        for (HoglakeParquetFooterCache footerCache : List.of(HoglakeParquetFooterCache.disabled(), new HoglakeParquetFooterCache(DataSize.of(1, MEGABYTE)))) {
            TailCountingProvider provider = new TailCountingProvider(footerCache);
            // A cut inside row group 4's bytes, not on a row-group boundary.
            long middle = (ROW_GROUP_OFFSETS.get(4) + ROW_GROUP_OFFSETS.get(5)) / 2;
            List<Long> first = values(read(provider, range(0, middle), List.of(VALUE), TupleDomain.all()));
            List<Long> second = values(read(provider, range(middle, FILE.length - middle), List.of(VALUE), TupleDomain.all()));

            assertThat(first).isNotEmpty();
            assertThat(second).isNotEmpty();
            List<Long> union = new ArrayList<>(first);
            union.addAll(second);
            assertThat(union).containsExactlyElementsOf(allValues());
        }
    }

    /**
     * Every range of a file needs the whole footer. With the footer cache, the
     * ranges one worker reads parse it once between them, rather than once per
     * range; without it, each range fetches the footer again.
     */
    @Test
    void rangesOfOneFileReadTheFooterOnceWithTheCache()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(scan(List.of()), 256);
        assertThat(splits).hasSizeGreaterThan(2);

        HoglakeParquetFooterCache footerCache = new HoglakeParquetFooterCache(DataSize.of(1, MEGABYTE));
        TailCountingProvider cached = new TailCountingProvider(footerCache);
        assertThat(readAllSplits(cached, splits, List.of(VALUE))).containsExactlyElementsOf(allValues());
        assertThat(cached.tailReads).isEqualTo(1);
        assertThat(footerCache.contains(new HoglakeParquetFooterCache.Key(PATH, FILE.length))).isTrue();

        // Reading the file again reads no footer at all.
        assertThat(readAllSplits(cached, splits, List.of(VALUE))).containsExactlyElementsOf(allValues());
        assertThat(cached.tailReads).isEqualTo(1);

        TailCountingProvider uncached = new TailCountingProvider(HoglakeParquetFooterCache.disabled());
        assertThat(readAllSplits(uncached, splits, List.of(VALUE))).containsExactlyElementsOf(allValues());
        assertThat(uncached.tailReads).isEqualTo(splits.size());
    }

    @Test
    void everyPlannedCutReadsEachRowOnce()
    {
        for (long maxSplitSize : new long[] {37, 100, 256, 777, FILE.length - 1}) {
            List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(scan(List.of()), maxSplitSize);
            assertThat(splits).hasSizeGreaterThan(1);
            assertThat(readAllSplits(splits, List.of(VALUE)))
                    .describedAs("even cuts of %s bytes", maxSplitSize)
                    .containsExactlyElementsOf(allValues());
        }
    }

    @Test
    void rowGroupAlignedCutsReadWholeRowGroups()
    {
        long rowGroupSize = ROW_GROUP_OFFSETS.get(1) - ROW_GROUP_OFFSETS.get(0);
        long maxSplitSize = rowGroupSize * 3;
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(scan(ROW_GROUP_OFFSETS), maxSplitSize);

        assertThat(splits).hasSizeGreaterThan(1);
        for (HoglakeSplit split : splits.subList(1, splits.size())) {
            assertThat(ROW_GROUP_OFFSETS).contains(split.start());
        }
        for (HoglakeSplit split : splits) {
            List<Long> values = values(read(split, List.of(VALUE), TupleDomain.all()));
            assertThat(values).isNotEmpty();
            assertThat(values.size() % ROWS_PER_ROW_GROUP).isZero();
            assertThat(values.size()).isLessThanOrEqualTo(3 * ROWS_PER_ROW_GROUP);
        }
        assertThat(readAllSplits(splits, List.of(VALUE))).containsExactlyElementsOf(allValues());
    }

    @Test
    void rangeWithoutARowGroupStartIsEmpty()
    {
        long start = ROW_GROUP_OFFSETS.get(3) + 1;
        long end = ROW_GROUP_OFFSETS.get(4);
        assertThat(read(range(start, end - start), List.of(VALUE), TupleDomain.all())).isEmpty();
        assertThat(read(range(start, 0), List.of(VALUE), TupleDomain.all())).isEmpty();
    }

    @Test
    void predicatePrunesRowGroupsInsideARange()
    {
        // Row groups 2 to 5 (rows 20 to 59) start inside the range; only row
        // group 4 (rows 40 to 49) can hold a value in [45, 47].
        long start = ROW_GROUP_OFFSETS.get(2);
        long end = ROW_GROUP_OFFSETS.get(6);
        TupleDomain<HoglakeColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(
                VALUE, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 45L, true, 47L, true)), false)));

        assertThat(values(read(range(start, end - start), List.of(VALUE), predicate)))
                .containsExactlyElementsOf(LongStream.range(40, 50).boxed().toList());
        // Outside the range, the matching row group is not this split's to read.
        assertThat(read(range(0, start), List.of(VALUE), predicate)).isEmpty();
    }

    /**
     * An unfiltered count needs no column: the file's first range answers it
     * from the catalog's whole-file record count, and its other ranges return
     * no rows, so the ranges sum to the file's count without any range reading
     * the footer or the data.
     */
    @Test
    void countThroughRangesSumsToTheFileRowCount()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(scan(List.of()), 256);
        assertThat(splits).hasSizeGreaterThan(1);
        assertThat(splits).allSatisfy(split -> assertThat(split.recordCount()).isEqualTo(ROWS));

        assertThat(splits.getFirst().start()).isZero();
        assertThat(read(splits.getFirst(), List.of(), TupleDomain.all())).hasSize(ROWS);
        long rows = 0;
        for (HoglakeSplit split : splits) {
            long rangeRows = read(split, List.of(), TupleDomain.all()).size();
            if (split.start() > 0) {
                assertThat(rangeRows).isZero();
            }
            rows += rangeRows;
        }
        assertThat(rows).isEqualTo(ROWS);

        // Not even the footer is read, with or without the footer cache.
        TailCountingProvider uncached = new TailCountingProvider(HoglakeParquetFooterCache.disabled());
        for (HoglakeSplit split : splits) {
            read(uncached, split, List.of(), TupleDomain.all());
        }
        assertThat(uncached.tailReads).isZero();
    }

    @Test
    void catalogFooterSizeSizesTheFirstFooterRead()
    {
        long footerSize = ConnectorTestFixtures.footerSize(FILE);
        ParquetReaderOptions defaults = ParquetReaderOptions.defaultOptions();

        assertThat(HoglakePageSourceProvider.withCatalogFooterSize(defaults, OptionalLong.of(footerSize), FILE.length).getFooterReadSize())
                .isEqualTo(DataSize.ofBytes(footerSize + 8));
        // A footer size the file cannot hold is ignored rather than trusted.
        assertThat(HoglakePageSourceProvider.withCatalogFooterSize(defaults, OptionalLong.of(FILE.length), FILE.length))
                .isSameAs(defaults);
        assertThat(HoglakePageSourceProvider.withCatalogFooterSize(defaults, OptionalLong.of(-1), FILE.length))
                .isSameAs(defaults);
        assertThat(HoglakePageSourceProvider.withCatalogFooterSize(defaults, OptionalLong.empty(), FILE.length))
                .isSameAs(defaults);

        // Reads succeed whether the hint is exact, too small, or unusable.
        for (long hint : new long[] {footerSize, 1, FILE.length}) {
            HoglakeSplit split = new HoglakeSplit(0, PATH, FILE.length, ROWS, Optional.empty(), 0, Optional.empty(), 0, FILE.length, SplitWeight.standard(), OptionalLong.of(hint), Optional.empty());
            assertThat(values(read(split, List.of(VALUE), TupleDomain.all()))).containsExactlyElementsOf(allValues());
        }
    }

    private static HoglakeSplit range(long start, long length)
    {
        return WHOLE_FILE.withRange(start, length, SplitWeight.standard());
    }

    private static List<HoglakeDtos.ScanFile> scan(List<Long> splitOffsets)
    {
        return List.of(new HoglakeDtos.ScanFile(
                new HoglakeDtos.DataFile(1, PATH, "parquet", ROWS, FILE.length, ConnectorTestFixtures.footerSize(FILE), 0, "provided", 1, splitOffsets),
                null));
    }

    private static List<Long> readAllSplits(List<HoglakeSplit> splits, List<HoglakeColumnHandle> columns)
    {
        return readAllSplits(new HoglakePageSourceProvider(ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, FILE))), splits, columns);
    }

    private static List<Long> readAllSplits(HoglakePageSourceProvider provider, List<HoglakeSplit> splits, List<HoglakeColumnHandle> columns)
    {
        List<Long> values = new ArrayList<>();
        for (HoglakeSplit split : splits) {
            values.addAll(values(read(provider, split, columns, TupleDomain.all())));
        }
        return values;
    }

    private static List<List<Object>> read(HoglakeSplit split, List<HoglakeColumnHandle> columns, TupleDomain<HoglakeColumnHandle> predicate)
    {
        return read(new HoglakePageSourceProvider(ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, FILE))), split, columns, predicate);
    }

    private static List<List<Object>> read(HoglakePageSourceProvider provider, HoglakeSplit split, List<HoglakeColumnHandle> columns, TupleDomain<HoglakeColumnHandle> predicate)
    {
        try (ConnectorPageSource source = provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("test", "ranges", 1, "synthetic-table", List.of(VALUE), predicate),
                Optional.empty(),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT)) {
            return ConnectorTestFixtures.readAll(source, columns.stream().map(HoglakeColumnHandle::type).toList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<Long> values(List<List<Object>> rows)
    {
        return rows.stream()
                .map(row -> (Long) row.getFirst())
                .toList();
    }

    private static List<Long> allValues()
    {
        return LongStream.range(0, ROWS).boxed().toList();
    }

    /**
     * Counts the footer fetches of every split it opens: the reader fetches a
     * footer, and only a footer, through {@link ParquetDataSource#readTail}.
     */
    private static final class TailCountingProvider
            extends HoglakePageSourceProvider
    {
        private int tailReads;

        private TailCountingProvider(HoglakeParquetFooterCache footerCache)
        {
            super(ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, FILE)), footerCache);
        }

        @Override
        ParquetDataSource createDataSource(TrinoInputFile inputFile, long fileSize, ParquetReaderOptions options)
                throws IOException
        {
            return new TailCountingDataSource(super.createDataSource(inputFile, fileSize, options));
        }

        private final class TailCountingDataSource
                implements ParquetDataSource
        {
            private final ParquetDataSource delegate;

            private TailCountingDataSource(ParquetDataSource delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public ParquetDataSourceId getId()
            {
                return delegate.getId();
            }

            @Override
            public long getReadBytes()
            {
                return delegate.getReadBytes();
            }

            @Override
            public long getReadTimeNanos()
            {
                return delegate.getReadTimeNanos();
            }

            @Override
            public long getEstimatedSize()
            {
                return delegate.getEstimatedSize();
            }

            @Override
            public Slice readTail(int length)
                    throws IOException
            {
                tailReads++;
                return delegate.readTail(length);
            }

            @Override
            public Slice readFully(long position, int length)
                    throws IOException
            {
                return delegate.readFully(position, length);
            }

            @Override
            public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
            {
                return delegate.planRead(diskRanges, memoryContext);
            }

            @Override
            public Metrics getMetrics()
            {
                return delegate.getMetrics();
            }

            @Override
            public void close()
                    throws IOException
            {
                delegate.close();
            }
        }
    }
}
