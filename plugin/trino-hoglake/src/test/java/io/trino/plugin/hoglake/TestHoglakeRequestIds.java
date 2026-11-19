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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.testing.StandaloneQueryRunner;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The request id each request carries, and the retry of busy answers, against a fake
 * Hoglake that records every request with its {@code X-Request-Id} and can answer a
 * path with a number of busy answers before the real one.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Tests mutate the shared server state
final class TestHoglakeRequestIds
{
    private static final String QUERY_ID = "20261119_160000_00001_abcde";
    private static final String CATALOG_PATH = "/v1/catalogs/lake";
    private static final String TABLE_PATH = CATALOG_PATH + "/namespaces/ns1/tables/t";
    private static final String SCAN_PATH = TABLE_PATH + "/scan";
    private static final String UPLOADS_PATH = CATALOG_PATH + "/uploads/";
    private static final String OWNER = "12345678-1234-5678-90ab-1234567890ac";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record Request(String method, String path, String requestId) {}

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger busyAnswers = new AtomicInteger();
    private volatile String busyPath = "";
    private volatile int busyStatus;
    private volatile String retryAfter;
    private HttpServer server;
    private String uri;
    private HoglakeClient client;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
        uri = "http://127.0.0.1:" + server.getAddress().getPort();
        client = new HoglakeClient(uri, "lake", Duration.ofSeconds(10));
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("ns1").build());
        queryRunner.installPlugin(new HoglakePlugin());
        queryRunner.createCatalog("hoglake", "hoglake", ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", "lake")
                // Storage is never reached: every scan here is empty
                .put("s3.endpoint", "http://127.0.0.1:9")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "synthetic-test")
                .put("s3.aws-secret-key", "synthetic-test-password")
                .buildOrThrow());
    }

    @AfterAll
    void tearDown()
    {
        queryRunner.close();
        client.close();
        server.stop(0);
    }

    @BeforeEach
    void reset()
    {
        busy("", 0, 0, null);
        requests.clear();
    }

    @Test
    void testEveryRequestOfAQueryCarriesTheQueryId()
    {
        QueryId first = queryRunner.executeWithPlan(queryRunner.getDefaultSession(), "SELECT * FROM t").queryId();
        assertThat(requests).extracting(Request::path).contains(TABLE_PATH, SCAN_PATH);
        assertThat(requests).allSatisfy(request -> assertThat(request.requestId()).startsWith(first + "."));
        assertThat(requests).extracting(Request::requestId).doesNotHaveDuplicates();

        // A retried scan plan keeps its id, and every other request of the next query has its own
        requests.clear();
        busy(SCAN_PATH, 503, 1, "0");
        QueryId second = queryRunner.executeWithPlan(queryRunner.getDefaultSession(), "SELECT * FROM t WHERE v > 1").queryId();
        assertThat(requests).allSatisfy(request -> assertThat(request.requestId()).startsWith(second + "."));
        List<Request> scans = requests.stream().filter(request -> request.path().equals(SCAN_PATH)).toList();
        assertThat(scans).hasSize(2);
        assertThat(scans.get(1)).isEqualTo(scans.get(0));
        assertThat(requests.stream().map(Request::requestId).distinct()).hasSize(requests.size() - 1);
    }

    @Test
    void testRequestIdShape()
    {
        assertThat(HoglakeClient.requestId(Optional.of(QUERY_ID))).matches(QUERY_ID + "\\.[0-9a-z]{1,13}");
        // No query, or a query id that would not make an id Hoglake accepts: a random id
        for (Optional<String> queryId : List.of(Optional.<String>empty(), Optional.of("q".repeat(127)), Optional.of("query id"))) {
            assertThat(HoglakeClient.requestId(queryId)).matches("\\p{XDigit}{8}(-\\p{XDigit}{4}){3}-\\p{XDigit}{12}");
        }
        client.getCatalog();
        assertThat(requests.getFirst().requestId()).matches("\\p{XDigit}{8}(-\\p{XDigit}{4}){3}-\\p{XDigit}{12}");
    }

    @Test
    void testBusyAnswersAreRetriedWithTheSameId()
    {
        HoglakeClient query = client.forQuery(QUERY_ID);
        assertRetried(TABLE_PATH, 503, () -> query.getTable("ns1", "t", 5));
        assertRetried(SCAN_PATH, 503, () -> query.planningScan("ns1", "t", 5, Set.of()));
        assertRetried(UPLOADS_PATH, 503, () -> query.claimUpload(OWNER, "s3://lake/ns1/t", "data"));
        assertRetried(UPLOADS_PATH + "renew", 429, () -> query.renewUploads(OWNER));
    }

    @Test
    void testRetriesAreBoundedAndOnlyForBusyAnswers()
    {
        HoglakeClient query = client.forQuery(QUERY_ID);
        // Busy every time: the request and three retries, then the busy failure
        busy(TABLE_PATH, 503, Integer.MAX_VALUE, "0");
        long started = System.nanoTime();
        assertUnavailable(() -> query.getTable("ns1", "t", 5));
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        assertThat(requests).hasSize(4);
        assertThat(requests).extracting(Request::requestId).containsOnly(requests.getFirst().requestId());

        // Retry-After is honoured, and a wait that would end past the timeout is not started
        try (HoglakeClient shortTimeout = new HoglakeClient(uri, "lake", Duration.ofMillis(2500))) {
            requests.clear();
            busy(TABLE_PATH, 503, Integer.MAX_VALUE, "1");
            long waited = System.nanoTime();
            assertUnavailable(() -> shortTimeout.forQuery(QUERY_ID).getTable("ns1", "t", 5));
            assertThat(Duration.ofNanos(System.nanoTime() - waited)).isBetween(Duration.ofSeconds(2), Duration.ofMillis(2500));
            assertThat(requests).hasSize(3);
        }

        // Any other failure, and a write that is not safe to repeat, are sent once
        for (int status : List.of(500, 502, 400)) {
            requests.clear();
            busy(TABLE_PATH, status, Integer.MAX_VALUE, "0");
            assertThatThrownBy(() -> query.getTable("ns1", "t", 5)).isInstanceOf(TrinoException.class);
            assertThat(requests).hasSize(1);
        }
        requests.clear();
        busy(CATALOG_PATH + "/namespaces", 503, Integer.MAX_VALUE, "0");
        assertUnavailable(() -> query.createNamespace("ns2"));
        assertThat(requests).hasSize(1);
    }

    @Test
    void testCommitRecoveryResendsWithTheSameId()
    {
        String commitPath = CATALOG_PATH + "/commit/prepared";
        busy(commitPath, 503, 1, "0");
        client.forQuery(QUERY_ID).commit(new HoglakeDtos.Commit(5, List.of(), OWNER));
        assertThat(requests).extracting(Request::path).containsExactly(commitPath, CATALOG_PATH + "/commit/receipts/" + OWNER, commitPath);
        assertThat(requests.get(2)).isEqualTo(requests.get(0));
        assertThat(requests.get(1).requestId()).startsWith(QUERY_ID + ".").isNotEqualTo(requests.get(0).requestId());
    }

    private void assertRetried(String path, int status, Runnable call)
    {
        requests.clear();
        busy(path, status, 1, "0");
        call.run();
        assertThat(requests).hasSize(2);
        assertThat(requests.get(1)).isEqualTo(requests.get(0));
        assertThat(requests.getFirst().requestId()).startsWith(QUERY_ID + ".");
    }

    private static void assertUnavailable(ThrowingCallable call)
    {
        assertThatThrownBy(call).isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
    }

    private void busy(String path, int status, int answers, String retryAfter)
    {
        busyPath = path;
        busyStatus = status;
        this.retryAfter = retryAfter;
        busyAnswers.set(answers);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getRawPath();
            JsonNode body = MAPPER.readTree(exchange.getRequestBody());
            requests.add(new Request(method, path, exchange.getRequestHeaders().getFirst(HoglakeClient.REQUEST_ID_HEADER)));
            String answer = "{\"error\":\"busy\"}";
            int status = busyStatus;
            if (!path.startsWith(busyPath) || busyAnswers.getAndDecrement() <= 0) {
                status = 200;
                answer = respond(method, path, body);
            }
            else if (retryAfter != null) {
                exchange.getResponseHeaders().set("Retry-After", retryAfter);
            }
            if (answer == null) {
                status = 404;
                answer = "{\"error\":\"not_found\"}";
            }
            byte[] payload = answer.getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, payload.length);
            exchange.getResponseBody().write(payload);
        }
    }

    private static String respond(String method, String path, JsonNode body)
    {
        if (path.startsWith(UPLOADS_PATH)) {
            String upload = path.substring(UPLOADS_PATH.length());
            if (method.equals("PUT")) {
                return "{\"upload_id\":\"%s\",\"owner\":\"%s\",\"path\":\"%s/trino-upload/%s.parquet\",\"state\":\"active\"}"
                        .formatted(upload, body.path("owner").asText(), body.path("prefix").asText(), upload);
            }
            return "{\"renewed\":1}";
        }
        return switch (path) {
            case CATALOG_PATH -> "{\"name\":\"lake\",\"data_path\":\"s3://lake/\",\"head_snapshot_id\":5,\"schema_version\":1}";
            case CATALOG_PATH + "/namespaces" -> "[{\"name\":\"ns1\",\"namespace_id\":1}]";
            case CATALOG_PATH + "/namespaces/ns1" -> "{\"name\":\"ns1\",\"namespace_id\":1}";
            case CATALOG_PATH + "/namespaces/ns1/tables" -> "[{\"name\":\"t\",\"table_uuid\":\"uuid-t\"}]";
            case TABLE_PATH -> "{\"name\":\"t\",\"namespace\":\"ns1\",\"table_uuid\":\"uuid-t\",\"columns\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"field_id\":1,\"ordinal\":0}],\"read_snapshot_id\":5,\"properties\":{}}";
            case SCAN_PATH -> "[]";
            case CATALOG_PATH + "/commit/prepared" -> "{\"snapshot_id\":6}";
            default -> null;
        };
    }
}
