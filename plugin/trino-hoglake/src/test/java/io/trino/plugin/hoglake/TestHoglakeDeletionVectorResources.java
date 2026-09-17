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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.memoryFileSystem;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.session;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeDeletionVectorResources
{
    private static final String DATA_PATH = "memory:///resources/data.parquet";
    private static final String VECTOR_PATH = "memory:///resources/data.dv";
    private static final HoglakeColumnHandle VALUE = new HoglakeColumnHandle("value", 1, BIGINT, true);

    @Test
    void inputIsReservedBeforeOpeningTheVector()
            throws IOException
    {
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 1);
        for (long limit : new long[] {0, Long.MAX_VALUE}) {
            RecordingMemory memory = new RecordingMemory(limit);
            AtomicBoolean opened = new AtomicBoolean();
            MemoryFileSystem fileSystem = new MemoryFileSystem()
            {
                @Override
                public TrinoInputFile newInputFile(Location location)
                {
                    return new ObservedInputFile(super.newInputFile(location), vector.length, () -> {
                        assertThat(memory.bytes).isGreaterThanOrEqualTo(sizeOfByteArray(vector.length));
                        opened.set(true);
                    });
                }
            };
            fileSystem.newOutputFile(Location.of(VECTOR_PATH)).createOrOverwrite(vector);
            try (HoglakeSplitResources resources = new HoglakeSplitResources(memory)) {
                HoglakeSplit split = new HoglakeSplit(DATA_PATH, 100, 3, Optional.of(VECTOR_PATH), 1, Optional.of("puffin-dv"));
                if (limit == 0) {
                    assertThatThrownBy(() -> HoglakeDeletionVectorLoader.load(fileSystem, split, 3, resources))
                            .isSameAs(memory.failure);
                    assertThat(opened).isFalse();
                }
                else {
                    assertThat(HoglakeDeletionVectorLoader.load(fileSystem, split, 3, resources).cardinality()).isEqualTo(1);
                    assertThat(opened).isTrue();
                }
            }
            assertThat(memory.bytes).isZero();
        }
    }

    @Test
    void invalidFileLengthIsRejectedBeforeReservingOrReading()
    {
        for (long length : new long[] {-1, HoglakeDeletionVectorLoader.MAX_DELETION_VECTOR_BYTES + 1}) {
            RecordingMemory memory = new RecordingMemory(0);
            MemoryFileSystem fileSystem = new MemoryFileSystem()
            {
                @Override
                public TrinoInputFile newInputFile(Location location)
                {
                    return new ObservedInputFile(super.newInputFile(location), length, () -> {
                        throw new AssertionError("invalid-length vector must not be opened");
                    });
                }
            };
            try (HoglakeSplitResources resources = new HoglakeSplitResources(memory)) {
                HoglakeSplit split = new HoglakeSplit(DATA_PATH, 100, 3, Optional.of(VECTOR_PATH), 1, Optional.of("puffin-dv"));
                assertThatThrownBy(() -> HoglakeDeletionVectorLoader.load(fileSystem, split, 3, resources))
                        .hasMessageContaining("outside the 0-to-");
                assertThat(memory.bytes).isZero();
            }
        }
    }

    @Test
    void readerConstructionFailureReleasesTheLoadedVector()
            throws Exception
    {
        for (boolean failClose : new boolean[] {false, true}) {
            byte[] parquet = parquet();
            byte[] vector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 1);
            long retained = TestingDeletionVectorMemory.retainedBytes(HoglakeDeletionVector.read(vector, VECTOR_PATH));
            RecordingMemory memory = new RecordingMemory(Long.MAX_VALUE);
            TrackingProvider provider = new TrackingProvider(memoryFileSystem(Map.of(DATA_PATH, parquet, VECTOR_PATH, vector)), memory, retained, true, failClose);
            assertThatThrownBy(() -> open(provider, parquet, 3, List.of(VALUE), memory))
                    .isSameAs(provider.failure);
            assertThat(provider.source.planned).isTrue();
            assertThat(provider.source.closeCalls).isEqualTo(1);
            assertThat(memory.bytes).isZero();
            if (failClose) {
                assertThat(provider.failure.getSuppressed()).containsExactly(provider.closeFailure);
            }
            else {
                assertThat(provider.failure.getSuppressed()).isEmpty();
            }
        }
    }

    @Test
    void readsFooterOnceAndReleasesAfterExplicitClose()
            throws Exception
    {
        byte[] parquet = parquet();
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 1);
        long retained = TestingDeletionVectorMemory.retainedBytes(HoglakeDeletionVector.read(vector, VECTOR_PATH));
        RecordingMemory memory = new RecordingMemory(Long.MAX_VALUE);
        TrackingProvider provider = new TrackingProvider(memoryFileSystem(Map.of(DATA_PATH, parquet, VECTOR_PATH, vector)), memory, retained, false, false);
        try (ConnectorPageSource source = open(provider, parquet, 3, List.of(VALUE), memory)) {
            assertThat(provider.source.tailReads).isEqualTo(1);
            assertThat(memory.bytes).isGreaterThanOrEqualTo(retained);
            assertThat(source.getNextSourcePage().getPositionCount()).isPositive();
            assertThat(memory.bytes).isGreaterThanOrEqualTo(source.getMemoryUsage());
        }
        assertThat(provider.source.closeCalls).isEqualTo(1);
        assertThat(memory.bytes).isZero();
    }

    @Test
    void rejectedReservationClosesConstructionResources()
    {
        byte[] parquet = parquet();
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 1);
        RecordingMemory memory = new RecordingMemory(1024 * 1024);
        TrackingProvider provider = new TrackingProvider(memoryFileSystem(Map.of(DATA_PATH, parquet, VECTOR_PATH, vector)), memory, 0, false, false);
        assertThatThrownBy(() -> open(provider, parquet, 3, List.of(VALUE), memory))
                .isSameAs(memory.failure);
        assertThat(provider.source.planned).isFalse();
        assertThat(provider.source.closeCalls).isEqualTo(1);
        assertThat(memory.bytes).isZero();
    }

    @Test
    void metadataCountUsesCatalogBoundWhileScanUsesFooterBound()
            throws IOException
    {
        byte[] parquet = parquet();
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 5);
        RecordingMemory memory = new RecordingMemory(Long.MAX_VALUE);
        TrackingProvider provider = new TrackingProvider(memoryFileSystem(Map.of(DATA_PATH, parquet, VECTOR_PATH, vector)), memory, 0, false, false);
        try (ConnectorPageSource count = open(provider, parquet, 10, List.of(), memory)) {
            assertThat(provider.source).isNull();
            assertThat(count.getNextSourcePage().getPositionCount()).isEqualTo(9);
            assertThat(memory.bytes).isZero();
        }
        assertThatThrownBy(() -> open(provider, parquet, 10, List.of(VALUE), memory))
                .hasMessageContaining("beyond the 3 rows");
        assertThat(provider.source.closeCalls).isEqualTo(1);
        assertThat(memory.bytes).isZero();
    }

    private static ConnectorPageSource open(HoglakePageSourceProvider provider, byte[] parquet, long catalogRows, List<ColumnHandle> columns, MemoryContext memory)
    {
        return provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                session(),
                new HoglakeSplit(DATA_PATH, parquet.length, catalogRows, Optional.of(VECTOR_PATH), 1, Optional.of("puffin-dv")),
                new HoglakeTableHandle("test", "resources", 1, "test-table", List.of(), TupleDomain.all()),
                Optional.empty(),
                columns,
                DynamicFilter.EMPTY,
                memory);
    }

    private static byte[] parquet()
    {
        return ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(INT64).id(1).named("value"), BIGINT, List.of(0L, 1L, 2L))));
    }

    private record ObservedInputFile(TrinoInputFile delegate, long length, Runnable onOpen)
            implements TrinoInputFile
    {
        @Override
        public TrinoInput newInput()
                throws IOException
        {
            onOpen.run();
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return delegate.exists();
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private static final class RecordingMemory
            implements MemoryContext
    {
        private final long limit;
        private final TrinoException failure = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "test memory limit");
        private long bytes;

        private RecordingMemory(long limit)
        {
            this.limit = limit;
        }

        @Override
        public void setBytes(long bytes)
        {
            this.bytes = bytes;
            if (bytes > limit) {
                throw failure;
            }
        }
    }

    private static final class TrackingProvider
            extends HoglakePageSourceProvider
    {
        private final RecordingMemory memory;
        private final long retained;
        private final boolean failPlan;
        private final boolean failClose;
        private final TrinoException failure = new TrinoException(GENERIC_INTERNAL_ERROR, "test reader construction failure");
        private final IOException closeFailure = new IOException("test close failure");
        private TrackingDataSource source;

        private TrackingProvider(TrinoFileSystemFactory fileSystem, RecordingMemory memory, long retained, boolean failPlan, boolean failClose)
        {
            super(fileSystem);
            this.memory = memory;
            this.retained = retained;
            this.failPlan = failPlan;
            this.failClose = failClose;
        }

        @Override
        ParquetDataSource createDataSource(TrinoInputFile file, long length, ParquetReaderOptions options)
                throws IOException
        {
            source = new TrackingDataSource(super.createDataSource(file, length, options));
            return source;
        }

        private final class TrackingDataSource
                implements ParquetDataSource
        {
            private final ParquetDataSource delegate;
            private int tailReads;
            private int closeCalls;
            private boolean planned;

            private TrackingDataSource(ParquetDataSource delegate)
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
            public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> ranges, AggregatedMemoryContext allocation)
            {
                planned = true;
                assertThat(allocation.getBytes()).isGreaterThanOrEqualTo(retained);
                assertThat(memory.bytes).isGreaterThanOrEqualTo(allocation.getBytes());
                if (failPlan) {
                    throw failure;
                }
                return delegate.planRead(ranges, allocation);
            }

            @Override
            public void close()
                    throws IOException
            {
                closeCalls++;
                delegate.close();
                if (failClose) {
                    throw closeFailure;
                }
            }
        }
    }
}
