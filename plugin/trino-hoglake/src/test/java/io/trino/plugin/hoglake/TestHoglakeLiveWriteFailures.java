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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real catalog mutations with a proxy that replaces the response after the server accepts them.
 */
@EnabledIfSystemProperty(named = "hoglake.test.uri", matches = ".+")
final class TestHoglakeLiveWriteFailures
{
    @Test
    void testAmbiguousWritesAndConcurrentDdl()
            throws Exception
    {
        String uri = System.getProperty("hoglake.test.uri");
        String catalog = "trino_fault_" + UUID.randomUUID().toString().replace("-", "");
        String base = uri + "/v1/catalogs/" + catalog;
        try (FaultProxy proxy = new FaultProxy(uri);
                HoglakeClient client = new HoglakeClient(uri, catalog);
                StandaloneQueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("test").build())) {
            proxy.post(uri + "/v1/catalogs", Map.of("name", catalog, "data_path", System.getProperty("hoglake.test.data-path", "s3://trino-write-test/") + catalog + "/"));
            proxy.post(base + "/namespaces", Map.of("name", "test"));
            runner.installPlugin(new HoglakePlugin());
            runner.createCatalog("hoglake", "hoglake", Map.of(
                    "hoglake.uri", proxy.uri(),
                    "hoglake.catalog", catalog,
                    "s3.endpoint", System.getProperty("hoglake.test.s3-endpoint"),
                    "s3.region", "us-east-1",
                    "s3.path-style-access", "true",
                    "s3.aws-access-key", "synthetic-test",
                    "s3.aws-secret-key", "synthetic-test-password"));

            proxy.failAfter("prepare");
            assertThatThrownBy(() -> runner.execute("CREATE TABLE uncertain_prepare AS SELECT BIGINT '1' AS id"))
                    .hasMessageContaining("Malformed Hoglake write response");
            assertThat(client.getTable("test", "uncertain_prepare")).isEmpty();
            assertEventually(() -> assertThat(client.getTableCreation(proxy.lastOperation()).state()).isEqualTo("aborted"));

            // A lost successful publication response is resolved from the durable receipt.
            proxy.failAfter("/commit");
            runner.execute("CREATE TABLE uncertain_empty (id bigint)");
            assertThat(client.getTable("test", "uncertain_empty")).isPresent();
            proxy.failAfter("/commit");
            runner.execute("CREATE TABLE uncertain_commit AS SELECT BIGINT '2' AS id");
            assertThat(runner.execute("SELECT id FROM uncertain_commit").getOnlyValue()).isEqualTo(2L);
            assertThat(client.getTableCreation(proxy.lastOperation()).state()).isEqualTo("committed");

            // INSERT still uses the existing append API: uncertain outcomes preserve data.
            runner.execute("CREATE TABLE append_target (id bigint)");
            proxy.failAfter("/commit");
            assertThatThrownBy(() -> runner.execute("INSERT INTO append_target VALUES 3"));
            assertThat(runner.execute("SELECT id FROM append_target").getOnlyValue()).isEqualTo(3L);

            // Even when receipt lookup fails, rollback must not delete a committed table.
            proxy.failStatus = true;
            proxy.failAfter("/commit");
            assertThatThrownBy(() -> runner.execute("CREATE TABLE unresolved AS SELECT BIGINT '4' AS id"))
                    .hasMessageContaining("outcome is unknown");
            proxy.failStatus = false;
            assertThat(runner.execute("SELECT id FROM unresolved").getOnlyValue()).isEqualTo(4L);
            assertThat(proxy.failures()).isEqualTo(5);

            proxy.beforeCommit(() -> {
                client.createTable("test", "contended", List.of(new HoglakeDtos.ColumnDefinition("original", "long", Map.of(), true)));
                client.dropStagingTable("test", "contended");
                client.createTable("test", "contended", List.of(new HoglakeDtos.ColumnDefinition("other", "long", Map.of(), true)));
            });
            assertThatThrownBy(() -> runner.execute("CREATE TABLE contended AS SELECT BIGINT '5' AS id")).hasMessageContaining("already exists");
            assertThat(client.getTable("test", "contended").orElseThrow().columns()).extracting(HoglakeDtos.Column::name).containsExactly("other");
            assertThat(client.getTableCreation(proxy.lastOperation()).state()).isEqualTo("rejected");

            // Aborting a prepared operation fences publication without deleting any table.
            HoglakeMetadata metadataToAbort = new HoglakeMetadata(client);
            HoglakeWriteHandle aborted = (HoglakeWriteHandle) metadataToAbort.beginCreateTable(
                    ConnectorTestFixtures.session(),
                    new ConnectorTableMetadata(new SchemaTableName("test", "never_published"), List.of(new ColumnMetadata("id", BIGINT))),
                    Optional.empty(),
                    RetryMode.NO_RETRIES,
                    false);
            assertThat(client.getTable("test", "never_published")).isEmpty();
            metadataToAbort.rollback();
            assertThat(client.getTableCreation(aborted.creationOperation().orElseThrow()).state()).isEqualTo("aborted");
            assertThatThrownBy(() -> metadataToAbort.finishCreateTable(ConnectorTestFixtures.session(), aborted, List.of(), List.of()))
                    .hasMessageContaining("aborted");
            assertThat(client.listTables("test")).noneMatch(table -> table.name().startsWith("_trino_ctas_"));

            // The real server must reject stale schema and incarnation pins from INSERT planning.
            runner.execute("CREATE TABLE stale (id bigint)");
            HoglakeMetadata metadata = new HoglakeMetadata(client);
            var session = ConnectorTestFixtures.session();
            var table = metadata.getTableHandle(session, new SchemaTableName("test", "stale"), Optional.empty(), Optional.empty());
            HoglakeWriteHandle handle = (HoglakeWriteHandle) metadata.beginInsert(session, table, List.copyOf(metadata.getColumnHandles(session, table).values()), RetryMode.NO_RETRIES);
            // Reuse a real, already uploaded Parquet file: rejected commits cannot register it.
            HoglakeDtos.DataFile existing = client.scan("test", "append_target", client.getCatalog().headSnapshotId()).getFirst().dataFile();
            HoglakeDtos.FileRegistration file = new HoglakeDtos.FileRegistration(existing.path(), existing.recordCount(), existing.fileSizeBytes(), existing.footerSize());
            var fragments = List.of(io.airlift.slice.Slices.wrappedBuffer(new ObjectMapper().writeValueAsBytes(file)));
            proxy.post(base + "/namespaces/test/tables/stale/alter", Map.of("ops", List.of(Map.of("op", "add_column", "column", Map.of("name", "extra", "type", "long", "nullable", true)))));
            assertThatThrownBy(() -> metadata.finishInsert(session, handle, List.of(), fragments, List.of())).hasMessageContaining("conflict");
            client.dropStagingTable("test", "stale");
            client.createTable("test", "stale", List.of(new HoglakeDtos.ColumnDefinition("id", "long", Map.of(), true)));
            assertThatThrownBy(() -> metadata.finishInsert(session, handle, List.of(), fragments, List.of())).hasMessageContaining("conflict");
            assertThat(client.getTable("test", "stale").orElseThrow().recordCount()).isZero();
            assertThat(runner.execute("SELECT id FROM append_target").getOnlyValue()).isEqualTo(3L);
        }
    }

    private static final class FaultProxy
            implements AutoCloseable
    {
        private final HttpServer server;
        private final HttpClient http = HttpClient.newHttpClient();
        private final String upstream;
        private final AtomicInteger failures = new AtomicInteger();
        private volatile String failedSuffix;
        private volatile String lastOperation;
        private volatile boolean failStatus;
        private volatile Runnable beforeCommit;

        public FaultProxy(String upstream)
                throws IOException
        {
            this.upstream = upstream;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        public String uri()
        {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        public void failAfter(String suffix)
        {
            failedSuffix = suffix;
        }

        public int failures()
        {
            return failures.get();
        }

        public String lastOperation()
        {
            return lastOperation;
        }

        public void beforeCommit(Runnable action)
        {
            beforeCommit = action;
        }

        public void post(String uri, Object body)
        {
            try {
                HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(uri))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new ObjectMapper().writeValueAsBytes(body))).build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).describedAs(response.body()).isIn(200, 201);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void handle(HttpExchange exchange)
                throws IOException
        {
            try (exchange) {
                String path = exchange.getRequestURI().toString();
                boolean prepare = exchange.getRequestMethod().equals("PUT");
                boolean write = prepare || exchange.getRequestMethod().equals("POST");
                if (prepare && path.contains("/table-creations/")) {
                    lastOperation = path.substring(path.lastIndexOf('/') + 1);
                }
                if (write && path.endsWith("/commit") && beforeCommit != null) {
                    Runnable action = beforeCommit;
                    beforeCommit = null;
                    action.run();
                }
                HttpResponse<byte[]> response = http.send(HttpRequest.newBuilder(URI.create(upstream + path))
                        .header("Content-Type", "application/json")
                        .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()))
                        .build(), HttpResponse.BodyHandlers.ofByteArray());
                byte[] body = response.body();
                if (write && failedSuffix != null && (path.endsWith(failedSuffix) || (prepare && failedSuffix.equals("prepare")))) {
                    assertThat(response.statusCode()).isIn(200, 201);
                    failedSuffix = null;
                    failures.incrementAndGet();
                    body = "response lost after successful mutation".getBytes(UTF_8);
                }
                if (failStatus && !write && path.contains("/table-creations/")) {
                    body = "receipt response unavailable".getBytes(UTF_8);
                }
                exchange.sendResponseHeaders(response.statusCode(), body.length);
                exchange.getResponseBody().write(body);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close()
        {
            server.stop(0);
            http.shutdownNow();
        }
    }
}
