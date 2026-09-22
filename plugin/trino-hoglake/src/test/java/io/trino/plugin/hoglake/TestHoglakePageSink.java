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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.operator.PagesIndex.TestingFactory;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakePageSink
{
    private static final HoglakeColumnHandle FIRST = new HoglakeColumnHandle("first", 7, BIGINT, true);
    private static final HoglakeColumnHandle SECOND = new HoglakeColumnHandle("second", 12, BIGINT, false);
    private static final HoglakeWriteHandle HANDLE = new HoglakeWriteHandle("test", "table", UUID.randomUUID().toString(), 3, "memory:///warehouse/", List.of(FIRST, SECOND), List.of(SECOND), Optional.empty());

    @Test
    void testClaimsPrecedeUploadAndAbortNeverDeletesHandedOffFiles()
            throws Exception
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        List<String> abandoned = new ArrayList<>();
        String owner = UUID.randomUUID().toString();
        try (var client = new HoglakeClient("http://localhost:1", "test")
        {
            @Override
            public String claimUpload(String claimedOwner, String prefix, String kind)
            {
                assertThat(claimedOwner).isEqualTo(owner);
                assertThat(kind).isEqualTo("data");
                return prefix + "trino-upload/" + UUID.randomUUID() + ".parquet";
            }

            @Override
            public void renewUploads(String claimedOwner)
            {
                assertThat(claimedOwner).isEqualTo(owner);
            }

            @Override
            public void abandonUploads(String claimedOwner, List<String> paths)
            {
                assertThat(claimedOwner).isEqualTo(owner);
                abandoned.addAll(paths);
            }
        }) {
            HoglakeWriteHandle handle = new HoglakeWriteHandle(
                    "test",
                    "table",
                    HANDLE.tableUuid(),
                    3,
                    "memory:///warehouse/",
                    List.of(FIRST),
                    List.of(FIRST),
                    Optional.empty(),
                    Optional.of(owner),
                    List.of(),
                    List.of(),
                    true);
            HoglakePageSink sink = new HoglakePageSink(storage, handle, "test", null, client);
            sink.appendPage(new Page(block(1L)));
            var fragment = sink.finish().get().iterator().next();
            var file = new ObjectMapper().readValue(fragment.getBytes(), HoglakeDtos.FileRegistration.class);
            assertThat(file.path()).contains("/trino-upload/");
            sink.abort();
            assertThat(abandoned).isEmpty();
            assertThat(storage.newInputFile(Location.of(file.path())).exists()).isTrue();
            HoglakePageSink unfinished = new HoglakePageSink(storage, handle, "test", null, client);
            unfinished.appendPage(new Page(block(2L)));
            unfinished.abort();
            assertThat(abandoned).hasSize(1);
            // The catalog drain owns deletion after fencing; workers do not delete claimed objects.
            assertThat(storage.newInputFile(Location.of(abandoned.getFirst())).exists()).isTrue();
        }
    }

    @Test
    void testSortedFilesIncludeNullOrderingAcrossInputPages()
            throws Exception
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        HoglakeWriteHandle handle = new HoglakeWriteHandle(
                "test",
                "sorted",
                UUID.randomUUID().toString(),
                3,
                "memory:///warehouse/",
                List.of(FIRST),
                List.of(FIRST),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                List.of(new HoglakeDtos.SortField(FIRST.fieldId(), "desc", "nulls_first")));
        var sorter = new PagesIndexPageSorter(new TestingFactory(false));
        HoglakePageSink sink = new HoglakePageSink(storage, handle, "test", sorter);
        sink.appendPage(new Page(block(1L)));
        sink.appendPage(new Page(block(null)));
        sink.appendPage(new Page(block(9L)));
        assertThat(sink.getMemoryUsage()).isPositive();
        var fragments = sink.finish().get();
        assertThat(fragments).hasSize(1);
        var file = new ObjectMapper().readValue(fragments.iterator().next().getBytes(), HoglakeDtos.FileRegistration.class);
        try (var source = new HoglakePageSourceProvider(_ -> storage).createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                new HoglakeSplit(file.path(), file.fileSizeBytes(), file.recordCount(), Optional.empty(), 0),
                new HoglakeTableHandle("test", "sorted", 3, handle.tableUuid(), handle.columns()),
                Optional.empty(),
                List.of(FIRST),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT)) {
            assertThat(ConnectorTestFixtures.readAll(source, List.of(BIGINT)))
                    .containsExactly(Arrays.asList((Object) null), List.of(9L), List.of(1L));
        }
        assertThat(sink.getMemoryUsage()).isZero();
    }

    @Test
    void testPartitionFilesContainOnlyTheirRegisteredValues()
            throws Exception
    {
        MemoryFileSystem storage = new MemoryFileSystem();
        HoglakeWriteHandle handle = new HoglakeWriteHandle(
                "test",
                "partitioned",
                UUID.randomUUID().toString(),
                3,
                "memory:///warehouse/",
                List.of(FIRST),
                List.of(FIRST),
                Optional.empty(),
                Optional.empty(),
                List.of(new HoglakeDtos.PartitionField(FIRST.fieldId(), "identity", null)));
        HoglakePageSink sink = new HoglakePageSink(storage, handle, "test");
        var values = BIGINT.createBlockBuilder(null, 5);
        BIGINT.writeLong(values, 7);
        values.appendNull();
        BIGINT.writeLong(values, 9);
        BIGINT.writeLong(values, 7);
        values.appendNull();
        sink.appendPage(new Page(values.build()));
        var fragments = sink.finish().get();
        assertThat(fragments).hasSize(3);
        long total = 0;
        for (var fragment : fragments) {
            var file = new ObjectMapper().readValue(fragment.getBytes(), HoglakeDtos.FileRegistration.class);
            var split = new HoglakeSplit(file.path(), file.fileSizeBytes(), file.recordCount(), Optional.empty(), 0);
            try (var source = new HoglakePageSourceProvider(_ -> storage).createPageSource(
                    HoglakeTransactionHandle.INSTANCE,
                    ConnectorTestFixtures.session(),
                    split,
                    new HoglakeTableHandle("test", "partitioned", 3, handle.tableUuid(), handle.columns()),
                    Optional.empty(),
                    List.of(FIRST),
                    DynamicFilter.EMPTY,
                    MemoryContext.NO_LIMIT)) {
                var rows = ConnectorTestFixtures.readAll(source, List.of(BIGINT));
                assertThat(rows).hasSize((int) file.recordCount());
                for (var row : rows) {
                    assertThat(row.getFirst() == null ? null : row.getFirst().toString()).isEqualTo(file.partitionValues().getFirst());
                }
                total += rows.size();
            }
        }
        assertThat(total).isEqualTo(5);
    }

    @Test
    void testHandleSerialization()
    {
        JsonCodec<HoglakeWriteHandle> codec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonDeserializers(Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER))).get())
                .jsonCodec(HoglakeWriteHandle.class);
        assertThat(codec.fromJson(codec.toJson(HANDLE))).isEqualTo(HANDLE);
        HoglakeWriteHandle output = new HoglakeWriteHandle(HANDLE.namespace(), HANDLE.table(), HANDLE.tableUuid(), HANDLE.snapshot(), HANDLE.dataPath(), HANDLE.columns(), HANDLE.inputColumns(), Optional.of("target"));
        assertThat(codec.fromJson(codec.toJson(output))).isEqualTo(output);
        HoglakeWriteHandle insert = new HoglakeWriteHandle(HANDLE.namespace(), HANDLE.table(), HANDLE.tableUuid(), HANDLE.snapshot(), HANDLE.dataPath(), HANDLE.columns(), HANDLE.inputColumns(), Optional.empty(), Optional.of(UUID.randomUUID().toString()));
        assertThat(codec.fromJson(codec.toJson(insert))).isEqualTo(insert);
    }

    @Test
    void testFieldIdsOmittedColumnAndFooter()
            throws Exception
    {
        TrinoFileSystem storage = new MemoryFileSystemFactory().create(ConnectorTestFixtures.session());
        HoglakePageSink sink = new HoglakePageSink(storage, HANDLE, "test");
        sink.appendPage(new Page(block(42L)));
        Slice fragment = sink.finish().get().iterator().next();
        HoglakeDtos.FileRegistration file = new ObjectMapper().readValue(fragment.getBytes(), HoglakeDtos.FileRegistration.class);
        assertThat(file.recordCount()).isEqualTo(1);
        assertThat(file.fileSizeBytes()).isEqualTo(storage.newInputFile(Location.of(file.path())).length());
        try (var input = storage.newInputFile(Location.of(file.path())).newInput()) {
            Slice trailer = input.readTail(8);
            assertThat(trailer.getInt(0)).isEqualTo(file.footerSize());
        }
        // Read by catalog ids after renaming the columns. This would return NULL if ids were absent.
        HoglakeSplit split = new HoglakeSplit(file.path(), file.fileSizeBytes(), 1, Optional.empty(), 0);
        var source = new HoglakePageSourceProvider(_ -> storage).createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("test", "table", 3, HANDLE.tableUuid(), HANDLE.columns()),
                Optional.empty(),
                List.of(new HoglakeColumnHandle("renamed_second", 12, BIGINT, false), new HoglakeColumnHandle("renamed_first", 7, BIGINT, true)),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
        try (source) {
            assertThat(ConnectorTestFixtures.readAll(source, List.of(BIGINT, BIGINT))).containsExactly(Arrays.asList(42L, null));
        }
        sink.abort();
        assertThat(storage.newInputFile(Location.of(file.path())).exists()).isTrue();
    }

    @Test
    void testAbortAndRequiredColumn()
            throws Exception
    {
        TrinoFileSystem storage = new MemoryFileSystemFactory().create(ConnectorTestFixtures.session());
        HoglakePageSink sink = new HoglakePageSink(storage, HANDLE, "test");
        sink.appendPage(new Page(block(42L)));
        sink.abort();
        assertThat(storage.listFiles(Location.of("memory:///warehouse/")).hasNext()).isFalse();
        HoglakePageSink invalid = new HoglakePageSink(storage, HANDLE, "test");
        assertThatThrownBy(() -> invalid.appendPage(new Page(block(null))))
                .isInstanceOf(TrinoException.class).hasMessageContaining("required column");
        invalid.abort();
        HoglakePageSink empty = new HoglakePageSink(storage, HANDLE, "test");
        assertThat(empty.finish().get()).isEmpty();
        assertThat(storage.listFiles(Location.of("memory:///warehouse/")).hasNext()).isFalse();
    }

    @Test
    void testCatalogDataPaths()
            throws Exception
    {
        for (String dataPath : List.of("s3://bucket", "s3://bucket/", "s3://bucket/prefix", "s3://bucket/prefix/")) {
            assertCatalogDataPath(dataPath);
        }
    }

    private void assertCatalogDataPath(String dataPath)
            throws Exception
    {
        TrinoFileSystem storage = new MemoryFileSystem()
        {
            @Override
            public TrinoInputFile newInputFile(Location location)
            {
                return super.newInputFile(Location.of("memory:///" + location.path()));
            }

            @Override
            public TrinoOutputFile newOutputFile(Location location)
            {
                return super.newOutputFile(Location.of("memory:///" + location.path()));
            }
        };
        HoglakeWriteHandle handle = new HoglakeWriteHandle(HANDLE.namespace(), HANDLE.table(), HANDLE.tableUuid(), HANDLE.snapshot(), dataPath, HANDLE.columns(), HANDLE.inputColumns(), Optional.empty());
        HoglakePageSink sink = new HoglakePageSink(storage, handle, "test");
        sink.appendPage(new Page(block(42L)));
        assertThat(sink.getMemoryUsage()).isPositive();
        HoglakeDtos.FileRegistration file = new ObjectMapper().readValue(sink.finish().get().iterator().next().getBytes(), HoglakeDtos.FileRegistration.class);
        String prefix = dataPath.endsWith("/") ? dataPath : dataPath + "/";
        assertThat(file.path()).startsWith(prefix + "data/").endsWith(".parquet");
        assertThat(file.recordCount()).isEqualTo(1);
        assertThat(file.fileSizeBytes()).isEqualTo(storage.newInputFile(Location.of(file.path())).length());
        assertThat(sink.getCompletedBytes()).isEqualTo(file.fileSizeBytes());
        assertThat(sink.getMemoryUsage()).isZero();
        sink.abort();
        assertThat(storage.newInputFile(Location.of(file.path())).exists()).isTrue();
    }

    private static Block block(Long value)
    {
        var builder = BIGINT.createBlockBuilder(null, 1);
        if (value == null) {
            builder.appendNull();
        }
        else {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }
}
