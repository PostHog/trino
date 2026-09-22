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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeDeletePublisher
{
    private static final HoglakeDeleteHandle HANDLE = new HoglakeDeleteHandle(
            new HoglakeTableHandle("ns", "table", 7, "synthetic-table", List.of()), "memory:///warehouse/", UUID.randomUUID().toString());

    @Test
    void testMultipleWorkersUnionExistingVectorAndHighPositions()
            throws IOException
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        HoglakeDeleteBitmap previous = new HoglakeDeleteBitmap();
        previous.add(1);
        byte[] prior = previous.encode("memory:///data");
        storage.newOutputFile(Location.of("memory:///prior")).createOrOverwrite(prior);
        try (Catalog client = new Catalog()) {
            client.previous = new HoglakeDtos.DeleteFile(2, 1, "memory:///prior", 1, prior.length, 6);
            new HoglakeDeletePublisher(client, storage).publish(HANDLE, List.of(fragment(1, 2, 1L << 32), fragment(1, 2, (1L << 32) + 1)));
            var registration = client.committed.deletes().getFirst().files().getFirst();
            assertThat(registration.deleteCount()).isEqualTo(4);
            byte[] bytes = storage.newInputFile(Location.of(registration.path())).newStream().readAllBytes();
            HoglakeDeletionVector vector = HoglakeDeletionVector.read(bytes, registration.path());
            assertThat(vector.cardinality()).isEqualTo(4);
            for (long position : new long[] {1, 2, 1L << 32, (1L << 32) + 1}) {
                assertThat(vector.isRowDeleted(position)).isTrue();
            }
            assertThat(vector.referencedDataFile()).contains("memory:///data");
            assertThat(storage.newInputFile(Location.of("memory:///prior")).exists()).isTrue();
        }
    }

    @Test
    void testDuplicateMixedFragmentsPublishOnlyOnce()
            throws IOException
    {
        var registration = new HoglakeDtos.FileRegistration("memory:///warehouse/replacement.parquet", 2, 100, 10);
        byte[] encoded = new ObjectMapper().writeValueAsBytes(registration);
        Slice append = Slices.allocate(Long.BYTES + encoded.length);
        append.setLong(0, HoglakeMergeSink.APPEND_FRAGMENT);
        append.setBytes(Long.BYTES, encoded);
        Slice deleted = fragment(1, 2, 7);
        try (Catalog client = new Catalog()) {
            new HoglakeDeletePublisher(client, new MemoryFileSystem()).publish(HANDLE, List.of(append, deleted, append, deleted));
            assertThat(client.committed.appends()).hasSize(1);
            assertThat(client.committed.appends().getFirst().files()).containsExactly(registration);
            assertThat(client.committed.deletes().getFirst().files().getFirst().deleteCount()).isEqualTo(2);
        }
    }

    @Test
    void testCancellationAndUploadFailureCannotPublish()
            throws IOException
    {
        for (boolean interrupt : List.of(false, true)) {
            MemoryFileSystem storage = new MemoryFileSystem()
            {
                @Override
                public TrinoOutputFile newOutputFile(Location location)
                {
                    TrinoOutputFile delegate = super.newOutputFile(location);
                    return new TrinoOutputFile()
                    {
                        @Override
                        public void createOrOverwrite(byte[] data)
                                throws IOException
                        {
                            delegate.createOrOverwrite(data);
                            if (interrupt) {
                                Thread.currentThread().interrupt();
                            }
                            else {
                                throw new IOException("synthetic upload failure");
                            }
                        }

                        @Override
                        public OutputStream create(AggregatedMemoryContext memoryContext)
                                throws IOException
                        {
                            return delegate.create(memoryContext);
                        }

                        @Override
                        public Location location()
                        {
                            return location;
                        }
                    };
                }
            };
            try (Catalog client = new Catalog()) {
                try {
                    assertThatThrownBy(() -> new HoglakeDeletePublisher(client, storage).publish(HANDLE, List.of(fragment(1, 2))))
                            .isInstanceOf(TrinoException.class);
                    assertThat(client.committed).isNull();
                    assertThat(storage.isEmpty()).isTrue();
                    assertThat(Thread.currentThread().isInterrupted()).isEqualTo(interrupt);
                }
                finally {
                    Thread.interrupted();
                }
            }
        }
    }

    @Test
    void testUnknownPublicationRetainsUploads()
            throws IOException
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        try (Catalog client = new Catalog()) {
            client.loseResponse = true;
            assertThatThrownBy(() -> new HoglakeDeletePublisher(client, storage).publish(HANDLE, List.of(fragment(1, 2))))
                    .hasMessageContaining("unknown");
            assertThat(client.committed).isNotNull();
            var registered = client.committed.deletes().getFirst().files().getFirst();
            assertThat(storage.newInputFile(Location.of(registered.path())).exists()).isTrue();
        }
    }

    @Test
    void testWorkingMemoryAccountedAndReleased()
            throws IOException
    {
        AtomicLong current = new AtomicLong();
        AtomicLong peak = new AtomicLong();
        try (Catalog client = new Catalog()) {
            new HoglakeDeletePublisher(client, new MemoryFileSystem()).publish(HANDLE, List.of(fragment(1, 2)), bytes -> {
                current.set(bytes);
                peak.accumulateAndGet(bytes, Math::max);
            });
            assertThat(client.committed).isNotNull();
            assertThat(peak.get()).isGreaterThan(500_000);
            assertThat(current.get()).isZero();
        }
    }

    @Test
    void testWorkingMemoryFailureCannotPublish()
            throws IOException
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        AtomicLong current = new AtomicLong();
        try (Catalog client = new Catalog()) {
            assertThatThrownBy(() -> new HoglakeDeletePublisher(client, storage).publish(HANDLE, List.of(fragment(1, 2)), bytes -> {
                current.set(bytes);
                if (bytes > 1024) {
                    throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "synthetic memory limit");
                }
            })).isInstanceOf(TrinoException.class).hasMessageContaining("synthetic memory limit");
            assertThat(client.committed).isNull();
            assertThat(storage.isEmpty()).isTrue();
            assertThat(current.get()).isZero();
        }
    }

    @Test
    void testEncodingReservationReleasedBeforeLoadingNextVector()
            throws IOException
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        HoglakeDeleteBitmap previous = new HoglakeDeleteBitmap();
        previous.add(0);
        byte[] prior = previous.encode("memory:///data2");
        storage.newOutputFile(Location.of("memory:///prior")).createOrOverwrite(prior);
        AtomicLong current = new AtomicLong();
        try (Catalog client = new Catalog()
        {
            @Override
            public List<HoglakeDtos.ScanFile> scan(String namespace, String table, long snapshot)
            {
                return List.of(
                        super.scan(namespace, table, snapshot).getFirst(),
                        new HoglakeDtos.ScanFile(
                                new HoglakeDtos.DataFile(2, "memory:///data2", "parquet", 10, 100, 10L, 0, "ready", 1),
                                new HoglakeDtos.DeleteFile(3, 2, "memory:///prior", 1, prior.length, 6)));
            }
        }) {
            // Each phase fits within 3 MiB, but retaining the first file's encoding
            // workspace while decoding the second file's old vector exceeds it.
            new HoglakeDeletePublisher(client, storage).publish(
                    HANDLE,
                    List.of(fragment(1, LongStream.range(0, 500_000).map(position -> position * 2).toArray()), fragment(2, 1)),
                    bytes -> {
                        if (bytes > 3L * 1024 * 1024) {
                            throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "synthetic 3 MiB publication limit");
                        }
                        current.set(bytes);
                    });
            assertThat(current.get()).isZero();
            var registrations = client.committed.deletes().getFirst().files();
            assertThat(registrations).extracting(HoglakeDtos.DeleteRegistration::deleteCount).containsExactly(500_000L, 2L);
            for (var registration : registrations) {
                try (var input = storage.newInputFile(Location.of(registration.path())).newStream()) {
                    assertThat(HoglakeDeletionVector.read(input.readAllBytes(), registration.path()).cardinality())
                            .isEqualTo(registration.deleteCount());
                }
            }
            assertThat(storage.newInputFile(Location.of("memory:///prior")).exists()).isTrue();
        }
    }

    private static Slice fragment(long fileId, long... positions)
            throws IOException
    {
        HoglakeDeleteBitmap bitmap = new HoglakeDeleteBitmap();
        for (long position : positions) {
            bitmap.add(position);
        }
        byte[] vector = bitmap.encode("");
        Slice fragment = Slices.allocate(Long.BYTES + vector.length);
        fragment.setLong(0, fileId);
        fragment.setBytes(Long.BYTES, vector);
        return fragment;
    }

    private static class Catalog
            extends HoglakeClient
    {
        private HoglakeDtos.DeleteFile previous;
        private HoglakeDtos.Commit committed;
        private boolean loseResponse;

        public Catalog()
        {
            super("http://localhost:1", "test");
        }

        @Override
        public List<HoglakeDtos.ScanFile> scan(String namespace, String table, long snapshot)
        {
            assertThat(snapshot).isEqualTo(7);
            return List.of(new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(1, "memory:///data", "parquet", 1L << 33, 100, 10L, 0, "ready", 1), previous));
        }

        @Override
        public void commitMutation(HoglakeDtos.Commit request)
        {
            committed = request;
            if (loseResponse) {
                throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "publication outcome is unknown");
            }
        }
    }
}
