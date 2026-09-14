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

import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.blob.cache.memory.MemoryBlobCache;
import io.trino.blob.cache.memory.MemoryBlobCacheConfig;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.CacheRequirements;
import io.trino.spi.cache.ConnectorCacheFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.cache.CacheCapability.CAN_EXCEED_HEAP_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeFileSystemCache
{
    @Test
    void testCachedAndUncachedReads()
            throws IOException
    {
        for (boolean enabled : List.of(false, true)) {
            try (S3ObjectServer server = new S3ObjectServer()) {
                // The test supplies an in-process cache; production capability selection is
                // owned by Trino's cache registry and requires a disk-capable manager.
                BlobCache cache = new MemoryBlobCache(new MemoryBlobCacheConfig());
                AtomicInteger cacheRequests = new AtomicInteger();
                ConnectorContext context = new CacheContext(requirements -> {
                    cacheRequests.incrementAndGet();
                    assertThat(requirements).isEqualTo(new CacheRequirements("filesystem.data", Set.of(CAN_EXCEED_HEAP_SIZE)));
                    return Optional.of(cache);
                });
                Connector connector = new HoglakeConnectorFactory().create("cache_test", Map.of(
                        "hoglake.uri", "http://localhost:8080",
                        "hoglake.s3.endpoint", server.endpoint(),
                        "hoglake.s3.access-key", "test-access",
                        "hoglake.s3.secret-key", "test-secret",
                        "hoglake.s3.path-style", "true",
                        "s3.max-error-retries", "1",
                        "fs.cache.enabled", Boolean.toString(enabled)), context);
                try {
                    assertThat(read(connector, server.data.length)).containsExactly(List.of(11L), List.of(22L));
                    int coldReads = server.dataReads.get();
                    assertThat(coldReads).isPositive();
                    assertThat(read(connector, server.data.length)).containsExactly(List.of(11L), List.of(22L));
                    if (enabled) {
                        assertThat(server.dataReads.get()).isEqualTo(coldReads);
                        assertThat(cacheRequests.get()).isEqualTo(1);
                    }
                    else {
                        assertThat(server.dataReads.get()).isGreaterThan(coldReads);
                        assertThat(cacheRequests.get()).isZero();
                    }

                    // Same location and file size, different modification time and content.
                    int previousReads = server.dataReads.get();
                    int previousLength = server.data.length;
                    server.data = parquet(33L, 44L);
                    server.lastModified = server.lastModified.plusSeconds(1);
                    assertThat(server.data.length).isEqualTo(previousLength);
                    assertThat(read(connector, server.data.length)).containsExactly(List.of(33L), List.of(44L));
                    assertThat(server.dataReads.get()).isGreaterThan(previousReads);
                }
                finally {
                    connector.shutdown();
                }
            }
        }
    }

    @Test
    void testMissingCacheManagerFailsAndDoesNotPoisonNextCreation()
    {
        assertThatThrownBy(() -> new HoglakeConnectorFactory().create("missing_cache", Map.of(
                "hoglake.uri", "http://localhost:8080", "fs.cache.enabled", "true"), new TestingConnectorContext()))
                .hasStackTraceContaining("cache-manager.config-files")
                .hasStackTraceContaining("missing_cache");
        new HoglakeConnectorFactory().create("missing_cache", Map.of(
                "hoglake.uri", "http://localhost:8080"), new TestingConnectorContext()).shutdown();
    }

    @Test
    void testDisabledCacheDoesNotConsultManager()
    {
        ConnectorContext context = new CacheContext(_ -> {
            throw new AssertionError("Disabled caching must not request a cache");
        });
        for (Map<String, String> config : List.of(
                Map.of("hoglake.uri", "http://localhost:8080"),
                Map.of("hoglake.uri", "http://localhost:8080", "fs.cache.enabled", "false"))) {
            Connector connector = new HoglakeConnectorFactory().create("disabled_cache", config, context);
            connector.shutdown();
            connector.shutdown();
        }
    }

    @Test
    void testExplicitFilesystemDisableOverridesDefault()
    {
        for (String property : List.of("fs.s3.enabled", "fs.native-s3.enabled")) {
            Connector connector = new HoglakeConnectorFactory().create("disabled_s3", Map.of(
                    "hoglake.uri", "http://localhost:8080", property, "false"), new TestingConnectorContext());
            try {
                assertThatThrownBy(() -> read(connector, 100))
                        .hasStackTraceContaining("No factory for location");
            }
            finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void testInvalidPropertiesFailAtCatalogLoad()
    {
        for (Map<String, String> property : List.of(
                Map.of("fs.cache.enabled", "invalid"),
                Map.of("hoglake.s3.pathstyle", "true"),
                Map.of("fs.cache.directories", "/tmp/cache"))) {
            Map<String, String> config = new HashMap<>(property);
            config.put("hoglake.uri", "http://localhost:8080");
            assertThatThrownBy(() -> new HoglakeConnectorFactory().create("invalid", config, new TestingConnectorContext()))
                    .hasMessageContaining(property.keySet().iterator().next());
        }
    }

    private static List<List<Object>> read(Connector connector, int fileSize)
            throws IOException
    {
        try (ConnectorPageSource source = connector.getPageSourceProvider().createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                new HoglakeSplit("s3://test-bucket/data.parquet", fileSize, 2, Optional.empty(), 0),
                new HoglakeTableHandle("test", "table", 1, "test-table", List.of()),
                Optional.empty(),
                List.of(new HoglakeColumnHandle("value", 1, BIGINT, true)),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT)) {
            return ConnectorTestFixtures.readAll(source, List.of(BIGINT));
        }
    }

    private static byte[] parquet(long first, long second)
    {
        return ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(INT64).id(1).named("value"), BIGINT, List.of(first, second))));
    }

    private static final class S3ObjectServer
            implements AutoCloseable
    {
        private final HttpServer server;
        private final AtomicInteger dataReads = new AtomicInteger();
        private volatile byte[] data = parquet(11L, 22L);
        private volatile Instant lastModified = Instant.parse("2026-01-01T00:00:00Z");

        private S3ObjectServer()
                throws IOException
        {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
            server.createContext("/test-bucket/data.parquet", exchange -> {
                try (exchange) {
                    byte[] content = data;
                    exchange.getResponseHeaders().set("Last-Modified", DateTimeFormatter.RFC_1123_DATE_TIME.format(lastModified.atZone(ZoneOffset.UTC)));
                    if (exchange.getRequestMethod().equals("HEAD")) {
                        exchange.getResponseHeaders().set("Content-Length", Integer.toString(content.length));
                        exchange.sendResponseHeaders(200, -1);
                        return;
                    }
                    dataReads.incrementAndGet();
                    String range = exchange.getRequestHeaders().getFirst("Range");
                    int start = 0;
                    int end = content.length - 1;
                    int status = 200;
                    if (range != null) {
                        String[] offsets = range.substring("bytes=".length()).split("-", -1);
                        if (offsets[0].isEmpty()) {
                            start = Math.max(0, content.length - Integer.parseInt(offsets[1]));
                        }
                        else {
                            start = Integer.parseInt(offsets[0]);
                            if (!offsets[1].isEmpty()) {
                                end = Math.min(end, Integer.parseInt(offsets[1]));
                            }
                        }
                        status = 206;
                        exchange.getResponseHeaders().set("Content-Range", "bytes %s-%s/%s".formatted(start, end, content.length));
                    }
                    exchange.sendResponseHeaders(status, end - start + 1);
                    exchange.getResponseBody().write(content, start, end - start + 1);
                }
            });
            server.start();
        }

        private String endpoint()
        {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close()
        {
            server.stop(0);
        }
    }

    private record CacheContext(ConnectorCacheFactory cacheFactory)
            implements ConnectorContext
    {
        private static final TestingConnectorContext DELEGATE = new TestingConnectorContext();

        @Override
        public ConnectorCacheFactory getCacheFactory()
        {
            return cacheFactory;
        }

        @Override
        public OpenTelemetry getOpenTelemetry()
        {
            return DELEGATE.getOpenTelemetry();
        }

        @Override
        public Tracer getTracer()
        {
            return DELEGATE.getTracer();
        }

        @Override
        public NodeManager getNodeManager()
        {
            return DELEGATE.getNodeManager();
        }

        @Override
        public VersionEmbedder getVersionEmbedder()
        {
            return DELEGATE.getVersionEmbedder();
        }

        @Override
        public TypeManager getTypeManager()
        {
            return DELEGATE.getTypeManager();
        }

        @Override
        public MetadataProvider getMetadataProvider()
        {
            return DELEGATE.getMetadataProvider();
        }

        @Override
        public PageSorter getPageSorter()
        {
            return DELEGATE.getPageSorter();
        }

        @Override
        public PageIndexerFactory getPageIndexerFactory()
        {
            return DELEGATE.getPageIndexerFactory();
        }

        @Override
        public BlocksHashFactory getBlocksHashFactory()
        {
            return DELEGATE.getBlocksHashFactory();
        }
    }
}
