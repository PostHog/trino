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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeWriteClient
{
    private static final HoglakeDtos.Commit COMMIT = new HoglakeDtos.Commit(7, List.of(new HoglakeDtos.Append(
            "test",
            "values",
            "12345678-1234-5678-90ab-1234567890ab",
            List.of(new HoglakeDtos.FileRegistration("s3://test-bucket/data/file.parquet", 3, 1000, 200)))));
    private static final HoglakeDtos.Commit IDENTIFIED_COMMIT = new HoglakeDtos.Commit(COMMIT.readSnapshot(), COMMIT.appends(), "12345678-1234-5678-90ab-1234567890ac");

    @Test
    void testCommitWireContract()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var received = new AtomicReference<String>();
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
                assertThatThrownBy(() -> client.truncateTable("test", "values", COMMIT.appends().getFirst().expectedTableUuid())).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(2);
                assertThatThrownBy(() -> client.renameTable("test", "values", "renamed", COMMIT.appends().getFirst().expectedTableUuid())).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(3);
                assertThatThrownBy(() -> client.alterColumns("test", "values", COMMIT.appends().getFirst().expectedTableUuid(), 7, Map.of("op", "drop_column", "name", "id"))).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(4);
                assertThatThrownBy(() -> client.dropNamespace("test", 1)).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(5);
                assertThatThrownBy(() -> client.createNamespace("test")).isInstanceOfSatisfying(
                        TrinoException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(failure.code().toErrorCode()));
                assertThat(requests.get()).isEqualTo(6);
            }
            finally {
                server.stop(0);
            }
        }
    }

    @Test
    void testIdentifiedCommitRecovery()
            throws Exception
    {
        String operation = "12345678-1234-5678-90ab-1234567890ac";
        HoglakeDtos.Commit identified = new HoglakeDtos.Commit(COMMIT.readSnapshot(), COMMIT.appends(), operation);
        for (String scenario : List.of("lost", "before", "unavailable", "mismatched", "null", "absent", "rejected")) {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            AtomicInteger writes = new AtomicInteger();
            AtomicInteger lookups = new AtomicInteger();
            AtomicReference<String> payload = new AtomicReference<>();
            server.createContext("/", exchange -> {
                try (exchange) {
                    int status = 200;
                    String response;
                    if (exchange.getRequestMethod().equals("POST")) {
                        assertThat(exchange.getRequestURI().getPath()).endsWith("/commit/prepared");
                        String body = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
                        if (payload.get() == null) {
                            payload.set(body);
                        }
                        assertThat(body).isEqualTo(payload.get());
                        int attempt = writes.incrementAndGet();
                        if (scenario.equals("rejected")) {
                            status = 422;
                            response = "{}";
                        }
                        else if (scenario.equals("before") && attempt > 1) {
                            response = "{\"snapshot_id\":8}";
                        }
                        else {
                            if (!scenario.equals("lost")) {
                                status = 503;
                            }
                            response = "response unavailable";
                        }
                    }
                    else {
                        lookups.incrementAndGet();
                        assertThat(exchange.getRequestURI().getPath()).endsWith("/commit/receipts/" + operation);
                        if (scenario.equals("before") || scenario.equals("absent")) {
                            status = 404;
                            response = "{}";
                        }
                        else if (scenario.equals("unavailable")) {
                            status = 503;
                            response = "{}";
                        }
                        else if (scenario.equals("null")) {
                            response = "null";
                        }
                        else {
                            String receiptOperation = operation;
                            if (scenario.equals("mismatched")) {
                                receiptOperation = "another-operation";
                            }
                            response = "{\"operation_id\":\"%s\",\"snapshot_id\":8}".formatted(receiptOperation);
                        }
                    }
                    byte[] bytes = response.getBytes(UTF_8);
                    exchange.sendResponseHeaders(status, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
            });
            server.start();
            try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
                if (scenario.equals("lost") || scenario.equals("before")) {
                    client.commit(identified);
                    assertThat(lookups.get()).isEqualTo(1);
                }
                else if (scenario.equals("rejected")) {
                    assertThatThrownBy(() -> client.commit(identified)).hasMessageContaining("rejected");
                    assertThat(lookups.get()).isZero();
                }
                else {
                    assertThatThrownBy(() -> client.commit(identified))
                            .hasMessageContaining("outcome is unknown")
                            .hasMessageContaining(operation);
                    assertThat(lookups.get()).isEqualTo(3);
                }
                int expectedWrites = 1;
                if (scenario.equals("before")) {
                    expectedWrites = 2;
                }
                if (scenario.equals("absent")) {
                    expectedWrites = 3;
                }
                assertThat(writes.get()).isEqualTo(expectedWrites);
                assertThat(new ObjectMapper().readTree(payload.get()).path("idempotency_key").asText()).isEqualTo(operation);
            }
            finally {
                server.stop(0);
            }
        }
    }

    @Test
    void testRecoveryBackoffWithInvalidRetryAfter()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        List<Long> requests = new CopyOnWriteArrayList<>();
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.add(System.nanoTime());
                exchange.getRequestBody().readAllBytes();
                exchange.getResponseHeaders().add("Retry-After", "invalid");
                int status = 404;
                if (exchange.getRequestMethod().equals("POST")) {
                    status = 503;
                }
                exchange.sendResponseHeaders(status, -1);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
            assertThatThrownBy(() -> client.commit(IDENTIFIED_COMMIT)).hasMessageContaining("outcome is unknown");
            assertThat(requests).hasSize(6);
            // Each receipt lookup follows a failed POST with exponential backoff.
            for (int attempt = 0; attempt < 3; attempt++) {
                assertThat(Duration.ofNanos(requests.get(2 * attempt + 1) - requests.get(2 * attempt)))
                        .isGreaterThanOrEqualTo(Duration.ofMillis(100L << attempt));
            }
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testRecoveryHonorsRetryAfter()
            throws Exception
    {
        for (String scenario : List.of("write-seconds", "write-date", "receipt-seconds")) {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            AtomicInteger requests = new AtomicInteger();
            AtomicReference<Instant> retryAt = new AtomicReference<>();
            AtomicReference<Instant> recoveredAt = new AtomicReference<>();
            server.createContext("/", exchange -> {
                try (exchange) {
                    int attempt = requests.incrementAndGet();
                    exchange.getRequestBody().readAllBytes();
                    if (scenario.equals("receipt-seconds") && attempt == 1) {
                        exchange.sendResponseHeaders(503, -1);
                    }
                    else if (retryAt.get() == null) {
                        Instant now = Instant.now();
                        String hint = "1";
                        retryAt.set(now.plusSeconds(1));
                        if (scenario.equals("write-date")) {
                            Instant date = now.plusSeconds(2).truncatedTo(ChronoUnit.SECONDS);
                            retryAt.set(date);
                            hint = RFC_1123_DATE_TIME.format(date.atZone(ZoneOffset.UTC));
                        }
                        exchange.getResponseHeaders().add("Retry-After", hint);
                        exchange.sendResponseHeaders(429, -1);
                    }
                    else {
                        recoveredAt.set(Instant.now());
                        byte[] response = "{\"operation_id\":\"%s\",\"snapshot_id\":8}".formatted(IDENTIFIED_COMMIT.operationId()).getBytes(UTF_8);
                        exchange.sendResponseHeaders(200, response.length);
                        exchange.getResponseBody().write(response);
                    }
                }
            });
            server.start();
            try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
                client.commit(IDENTIFIED_COMMIT);
                assertThat(recoveredAt.get()).isAfterOrEqualTo(retryAt.get());
                int expectedRequests = 2;
                if (scenario.equals("receipt-seconds")) {
                    expectedRequests = 3;
                }
                assertThat(requests.get()).isEqualTo(expectedRequests);
            }
            finally {
                server.stop(0);
            }
        }
    }

    @Test
    void testRetryAfterBeyondRecoveryBudget()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                exchange.getResponseHeaders().add("Retry-After", "60");
                exchange.sendResponseHeaders(503, -1);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake", Duration.ofSeconds(2))) {
            assertThatThrownBy(() -> client.commit(IDENTIFIED_COMMIT))
                    .hasMessageContaining("outcome is unknown")
                    .hasMessageContaining(IDENTIFIED_COMMIT.operationId());
            assertThat(requests.get()).isEqualTo(1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testRecoveryRequestsShareDeadline()
            throws Exception
    {
        assertRecoveryRequestsShareDeadline(false);
        assertRecoveryRequestsShareDeadline(true);
    }

    private static void assertRecoveryRequestsShareDeadline(boolean stallBody)
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch releaseRequest = new CountDownLatch(1);
        server.createContext("/", exchange -> {
            try (exchange) {
                int attempt = requests.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                if (attempt == 1) {
                    exchange.sendResponseHeaders(503, -1);
                }
                else if (attempt == 2) {
                    Thread.sleep(Duration.ofSeconds(1));
                    exchange.sendResponseHeaders(404, -1);
                }
                else {
                    if (stallBody) {
                        exchange.sendResponseHeaders(200, 100);
                        exchange.getResponseBody().write('{');
                        exchange.getResponseBody().flush();
                    }
                    releaseRequest.await(10, SECONDS);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake", Duration.ofSeconds(2))) {
            long started = System.nanoTime();
            assertThatThrownBy(() -> client.commit(IDENTIFIED_COMMIT))
                    .hasMessageContaining("outcome is unknown")
                    .hasMessageContaining(IDENTIFIED_COMMIT.operationId());
            // The retry has less than one second left, not a fresh two-second timeout.
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(2750));
            assertThat(requests.get()).isEqualTo(3);
        }
        finally {
            releaseRequest.countDown();
            server.stop(0);
        }
    }

    @Test
    void testInterruptedRecoveryPreservesUnknownOutcome()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                exchange.getResponseHeaders().add("Retry-After", "30");
                exchange.sendResponseHeaders(503, -1);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
            AtomicReference<TrinoException> failure = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread worker = Thread.ofVirtual().start(() -> {
                try {
                    client.commit(IDENTIFIED_COMMIT);
                }
                catch (TrinoException e) {
                    failure.set(e);
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            });
            try {
                assertEventually(() -> {
                    assertThat(requests.get()).isEqualTo(1);
                    assertThat(worker.getState()).isEqualTo(Thread.State.TIMED_WAITING);
                });
                worker.interrupt();
                assertThat(worker.join(Duration.ofSeconds(5))).isTrue();
                assertThat(failure.get()).hasMessageContaining("outcome is unknown").hasMessageContaining(IDENTIFIED_COMMIT.operationId());
                assertThat(interrupted.get()).isTrue();
                assertThat(requests.get()).isEqualTo(1);
            }
            finally {
                worker.interrupt();
                worker.join(Duration.ofSeconds(5));
            }
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testDropRejectsMissingTargetButStagingCleanupToleratesIt()
            throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                exchange.sendResponseHeaders(404, -1);
            }
        });
        server.start();
        try (HoglakeClient client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake")) {
            assertThatThrownBy(() -> client.dropTable("ns", "old_name", "12345678-1234-5678-90ab-1234567890ab"))
                    .isInstanceOfSatisfying(TrinoException.class,
                            exception -> assertThat(exception.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_NOT_FOUND.toErrorCode()));
            client.dropStagingTable("ns", "old_name");
        }
        finally {
            server.stop(0);
        }
    }

    private record Failure(int status, String body, ErrorCodeSupplier code) {}
}
