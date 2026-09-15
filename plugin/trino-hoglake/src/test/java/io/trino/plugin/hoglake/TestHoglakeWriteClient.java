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
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeWriteClient
{
    private static final HoglakeDtos.Commit COMMIT = new HoglakeDtos.Commit(7, List.of(new HoglakeDtos.Append(
            "test",
            "values",
            "12345678-1234-5678-90ab-1234567890ab",
            List.of(new HoglakeDtos.FileRegistration("s3://test-bucket/data/file.parquet", 3, 1000, 200)))));

    @Test
    void testCommitWireContract()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var received = new java.util.concurrent.atomic.AtomicReference<String>();
        server.createContext("/", exchange -> {
            try (exchange) {
                received.set(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
                byte[] response = "{\"snapshot_id\":8,\"schema_version\":1}".getBytes(UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
            client.commit(COMMIT);
            var body = new ObjectMapper().readTree(received.get());
            assertThat(body.path("read_snapshot").asLong()).isEqualTo(7);
            var append = body.path("appends").get(0);
            assertThat(append.path("expected_table_uuid").asText()).isEqualTo(COMMIT.appends().getFirst().expectedTableUuid());
            var file = append.path("files").get(0);
            assertThat(file.path("record_count").asLong()).isEqualTo(3);
            assertThat(file.path("file_size_bytes").asLong()).isEqualTo(1000);
            assertThat(file.path("footer_size").asLong()).isEqualTo(200);
            assertThat(file.has("column_stats")).isFalse();
            assertThat(file.has("partition_values")).isFalse();
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testWriteFailuresAreCodedAndNotRetried()
            throws Exception
    {
        for (Failure failure : List.of(
                new Failure(400, "{}", INVALID_ARGUMENTS),
                new Failure(404, "{}", HOGLAKE_CATALOG_NOT_FOUND),
                new Failure(409, "{}", TRANSACTION_CONFLICT),
                new Failure(410, "{}", HOGLAKE_SNAPSHOT_EXPIRED),
                new Failure(422, "{}", INVALID_ARGUMENTS),
                new Failure(503, "{}", HOGLAKE_CATALOG_UNAVAILABLE),
                new Failure(202, "{}", HOGLAKE_INVALID_RESPONSE),
                new Failure(200, "not json", HOGLAKE_INVALID_RESPONSE),
                new Failure(200, "null", HOGLAKE_INVALID_RESPONSE),
                new Failure(200, "{}", HOGLAKE_INVALID_RESPONSE))) {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            AtomicInteger requests = new AtomicInteger();
            server.createContext("/", exchange -> {
                try (exchange) {
                    requests.incrementAndGet();
                    exchange.getRequestBody().readAllBytes();
                    byte[] response = failure.body().getBytes(UTF_8);
                    exchange.sendResponseHeaders(failure.status(), response.length);
                    exchange.getResponseBody().write(response);
                }
            });
            server.start();
            try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
                assertThatThrownBy(() -> client.commit(COMMIT)).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(1);
            }
            finally {
                server.stop(0);
            }
        }
    }

    private record Failure(int status, String body, ErrorCodeSupplier code) {}
}
