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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.units.Duration;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_RESPONSE_BYTES;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSharedHttpClient
{
    private static final String CATALOG = "catalog-a";
    private static final String PRINCIPAL = "warehouse.alice";
    private static final String USER = "warehouse.mapped-alice";
    private static final String TENANT = "routed-tenant-a";
    private static final String BODY_MARKER = "private-response-marker";
    private static final String NAME_MARKER = "private-name-marker?secret=value";
    private static final String TABLE_JSON =
            """
            {"name":"table-a","namespace":"namespace-a","table_uuid":"synthetic-table-a",
             "columns":[{"field_id":1,"ordinal":0,"name":"column-a","type":"string","nullable":true}],
             "record_count":0,"file_count":0,"file_size_bytes":0,"properties":{"key":"value"}}
            """;

    @TempDir
    Path directory;

    @Test
    void testExactRoutesAndBearer()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            fixture.handler = exchange -> {
                assertRequest(exchange, jwt);
                switch (exchange.getRequestURI().getRawPath()) {
                    case "/v1/catalogs/catalog-a/namespaces" -> {
                        assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                        respond(exchange, 200, "[{\"name\":\"namespace-a\",\"namespace_id\":7}]");
                    }
                    case "/v1/catalogs/catalog-a/namespaces/namespace-a/tables" -> {
                        assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                        respond(exchange, 200, "[{\"name\":\"table-a\",\"table_uuid\":\"synthetic-table-a\"}]");
                    }
                    case "/v1/catalogs/catalog-a/namespaces/namespace-a/tables/table-a" -> {
                        assertThat(exchange.getRequestURI().getRawQuery()).isEqualTo("totals=false");
                        respond(exchange, 200, TABLE_JSON);
                    }
                    default -> throw new AssertionError("Unexpected metadata route");
                }
            };
            assertThat(fixture.client.listNamespaces(session)).containsExactly(new HoglakeDtos.Namespace("namespace-a", 7L));
            assertThat(fixture.client.listTables(session, "namespace-a"))
                    .containsExactly(new HoglakeDtos.TableSummary("table-a", "synthetic-table-a"));
            assertThat(fixture.client.table(session, "namespace-a", "table-a")).hasValueSatisfying(table -> {
                assertThat(table.name()).isEqualTo("table-a");
                assertThat(table.columns()).hasSize(1);
                assertThat(table.properties()).containsEntry("key", "value");
            });
            assertThat(fixture.requests).hasValue(3);
        }
    }

    @Test
    void testEncodedSegmentsDoNotChangeTheRoute()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            ConnectorSession session = fixture.session();
            fixture.handler = exchange -> {
                assertThat(exchange.getRequestMethod()).isEqualTo("GET");
                assertThat(exchange.getRequestURI().getRawPath()).isEqualTo(
                        "/v1/catalogs/catalog-a/namespaces/a%2Fb%20%3F%23%25%C3%BC/tables/%2E%2E%2Ft%3Fx%3D1");
                assertThat(exchange.getRequestURI().getRawQuery()).isEqualTo("totals=false");
                respond(exchange, 200, TABLE_JSON);
            };
            assertThat(fixture.client.table(session, "a/b ?#%ü", "../t?x=1")).isPresent();
            fixture.handler = exchange -> {
                assertThat(exchange.getRequestURI().getRawPath()).isEqualTo("/v1/catalogs/catalog-a/namespaces/%2E%2E/tables");
                assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                respond(exchange, 200, "[]");
            };
            assertThat(fixture.client.listTables(session, "..")).isEmpty();
            assertFailure(() -> fixture.client.table(session, NAME_MARKER, ""), INVALID_ARGUMENTS, NAME_MARKER);
            assertFailure(() -> fixture.client.listTables(session, null), INVALID_ARGUMENTS, NAME_MARKER);
            assertThat(fixture.requests).hasValue(2);
        }
    }

    @Test
    void testConcurrentCallersKeepCatalogAndRoutedTenantIsolated()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            String firstToken = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            String secondToken = fixture.jwt("catalog-b", "warehouse.bob", "routed-tenant-b", Instant.now());
            ConnectorSession first = session(USER, PRINCIPAL, firstToken);
            ConnectorSession second = session("warehouse.mapped-bob", "warehouse.bob", secondToken);
            CountDownLatch concurrentRequests = new CountDownLatch(2);
            fixture.handler = exchange -> {
                concurrentRequests.countDown();
                assertThat(concurrentRequests.await(5, SECONDS)).isTrue();
                assertThat(exchange.getRequestMethod()).isEqualTo("GET");
                assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                switch (exchange.getRequestURI().getRawPath()) {
                    case "/v1/catalogs/catalog-a/namespaces" -> {
                        assertRequest(exchange, firstToken);
                        respond(exchange, 200, "[{\"name\":\"visible-a\"}]");
                    }
                    case "/v1/catalogs/catalog-b/namespaces" -> {
                        assertRequest(exchange, secondToken);
                        respond(exchange, 200, "[{\"name\":\"visible-b\"}]");
                    }
                    default -> throw new AssertionError("Unexpected caller catalog");
                }
            };
            List<Future<List<HoglakeDtos.Namespace>>> results = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                ConnectorSession caller = first;
                if (index % 2 != 0) {
                    caller = second;
                }
                ConnectorSession requestSession = caller;
                results.add(callers.submit(() -> fixture.client.listNamespaces(requestSession)));
            }
            for (int index = 0; index < results.size(); index++) {
                String expected = "visible-a";
                if (index % 2 != 0) {
                    expected = "visible-b";
                }
                assertThat(results.get(index).get(10, SECONDS)).extracting(HoglakeDtos.Namespace::name).containsExactly(expected);
            }
            assertThat(fixture.requests).hasValue(32);
        }
    }

    @Test
    void testVerificationPrecedesEveryRequestAndPathValidation()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            fixture.handler = exchange -> respond(exchange, 200, "[]");
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            assertThat(fixture.client.listNamespaces(session(USER, PRINCIPAL, jwt))).isEmpty();
            String expired = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now().minusSeconds(3600));
            ConnectorSession noPrincipal = session(USER, null, jwt);
            ConnectorSession noToken = session(USER, PRINCIPAL, null);
            for (ConnectorSession invalid : List.of(
                    session(USER, PRINCIPAL, expired),
                    session(USER, "warehouse.changed-principal", jwt),
                    noPrincipal,
                    noToken)) {
                assertFailure(() -> fixture.client.listNamespaces(invalid), PERMISSION_DENIED, jwt, expired, PRINCIPAL, USER, CATALOG, TENANT);
                assertFailure(() -> fixture.client.listTables(invalid, null), PERMISSION_DENIED, jwt, expired, NAME_MARKER);
                assertFailure(() -> fixture.client.table(invalid, null, null), PERMISSION_DENIED, jwt, expired, NAME_MARKER);
            }
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testPreviouslyAcceptedTokenExpiresBeforeTheNextRequest()
            throws Exception
    {
        Instant issued = Instant.now();
        MutableClock clock = new MutableClock(new AtomicReference<>(issued), ZoneOffset.UTC);
        try (Fixture fixture = new Fixture(directory, new Duration(10, SECONDS), clock)) {
            fixture.handler = exchange -> respond(exchange, 200, "[]");
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, issued);
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            assertThat(fixture.client.listNamespaces(session)).isEmpty();
            clock.now.set(issued.plusSeconds(1000));
            assertFailure(() -> fixture.client.listNamespaces(session), PERMISSION_DENIED, jwt, CATALOG, PRINCIPAL);
            assertFailure(() -> fixture.client.listTables(session, null), PERMISSION_DENIED, jwt);
            assertFailure(() -> fixture.client.table(session, null, null), PERMISSION_DENIED, jwt);
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testMissingResultsUseSanitizedErrors()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            ConnectorSession session = fixture.session();
            fixture.handler = exchange -> respond(exchange, 404, BODY_MARKER);
            assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_CATALOG_NOT_FOUND, CATALOG, BODY_MARKER);
            assertFailure(() -> fixture.client.listTables(session, NAME_MARKER), NOT_FOUND, NAME_MARKER, BODY_MARKER);
            assertThatThrownBy(() -> fixture.client.listTables(session, NAME_MARKER))
                    .isInstanceOfSatisfying(SchemaNotFoundException.class, failure -> assertThat(failure.getSchemaName()).isEqualTo("<redacted>"));
            assertThat(fixture.client.table(session, NAME_MARKER, NAME_MARKER)).isEmpty();
            assertThat(fixture.requests).hasValue(4);
        }
    }

    @Test
    void testAuthenticationFailuresAndRedirectsNeverRetry()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            for (int status : List.of(401, 403, 301, 302, 307, 308, 500)) {
                int before = fixture.requests.get();
                fixture.handler = exchange -> {
                    assertThat(exchange.getRequestURI().getRawPath()).startsWith("/v1/catalogs/catalog-a/namespaces");
                    assertRequest(exchange, jwt);
                    exchange.getResponseHeaders().set("Location", fixture.endpoint + "/issuer?" + NAME_MARKER);
                    respond(exchange, status, jwt + PRINCIPAL + BODY_MARKER);
                };
                ErrorCodeSupplier code = HOGLAKE_INVALID_RESPONSE;
                if (status == 401 || status == 403) {
                    code = PERMISSION_DENIED;
                }
                if (status == 500) {
                    code = HOGLAKE_CATALOG_UNAVAILABLE;
                }
                assertFailure(() -> fixture.client.listNamespaces(session), code, jwt, PRINCIPAL, CATALOG, TENANT, BODY_MARKER, NAME_MARKER);
                assertFailure(() -> fixture.client.listTables(session, NAME_MARKER), code, jwt, BODY_MARKER, NAME_MARKER);
                assertFailure(() -> fixture.client.table(session, NAME_MARKER, NAME_MARKER), code, jwt, BODY_MARKER, NAME_MARKER);
                assertThat(fixture.requests.get() - before).isEqualTo(3);
            }
        }
    }

    @Test
    void testMalformedResponsesDoNotExposePayloads()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            for (String response : List.of(
                    jwt + BODY_MARKER,
                    "{\"echo\":\"" + jwt + "\"}",
                    "null",
                    "[null]",
                    "[{\"name\":7}]",
                    "[{\"name\":\"valid\",\"namespace_id\":\"7\"}]",
                    "[{\"name\":\"first\",\"name\":\"second\"}]",
                    "[] {}",
                    "[] " + jwt + BODY_MARKER)) {
                fixture.handler = exchange -> respond(exchange, 200, response);
                assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_INVALID_RESPONSE, jwt, BODY_MARKER, PRINCIPAL, CATALOG);
            }
            for (String response : List.of("{}", "[null]", "[{\"name\":7,\"table_uuid\":\"value\"}]", "[{\"name\":\"value\",\"table_uuid\":null}]")) {
                fixture.handler = exchange -> respond(exchange, 200, response);
                assertFailure(() -> fixture.client.listTables(session, NAME_MARKER), HOGLAKE_INVALID_RESPONSE, jwt, NAME_MARKER);
            }
            for (String response : List.of(
                    "{}",
                    TABLE_JSON.replace("\"columns\":[", "\"columns\":{\"invalid\":["),
                    TABLE_JSON.replace("\"field_id\":1", "\"field_id\":\"1\""),
                    TABLE_JSON.replace("\"nullable\":true", "\"nullable\":\"true\""),
                    TABLE_JSON.replace("\"record_count\":0", "\"record_count\":null"),
                    TABLE_JSON.replace("\"value\"", "7"),
                    TABLE_JSON + " []")) {
                fixture.handler = exchange -> respond(exchange, 200, response);
                assertFailure(() -> fixture.client.table(session, NAME_MARKER, NAME_MARKER), HOGLAKE_INVALID_RESPONSE, jwt, NAME_MARKER);
            }
        }
    }

    @Test
    void testUnexpectedContentEncodingIsRejected()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            ConnectorSession session = fixture.session();
            for (String encoding : List.of("gzip", "br", "identity, gzip")) {
                fixture.handler = exchange -> {
                    assertThat(exchange.getRequestHeaders().getFirst("Accept-Encoding")).isEqualTo("identity");
                    exchange.getResponseHeaders().set("Content-Encoding", encoding);
                    respond(exchange, 200, "[]");
                };
                assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_INVALID_RESPONSE, BODY_MARKER);
            }
            fixture.handler = exchange -> {
                exchange.getResponseHeaders().set("Content-Encoding", "identity");
                respond(exchange, 200, "[]");
            };
            assertThat(fixture.client.listNamespaces(session)).isEmpty();
            assertThat(fixture.requests).hasValue(4);
        }
    }

    @Test
    void testOversizedStreamingResponseIsRejected()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            ConnectorSession session = fixture.session();
            fixture.handler = exchange -> {
                exchange.sendResponseHeaders(200, 0);
                byte[] chunk = new byte[8192];
                for (int bytes = 0; bytes <= MAX_RESPONSE_BYTES; bytes += chunk.length) {
                    exchange.getResponseBody().write(chunk);
                }
            };
            assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_INVALID_RESPONSE, BODY_MARKER);
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testOversizedContentLengthIsRejectedBeforeBody()
            throws Exception
    {
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(directory)) {
            ConnectorSession session = fixture.session();
            fixture.handler = exchange -> {
                exchange.sendResponseHeaders(200, MAX_RESPONSE_BYTES + 1L);
                exchange.getResponseBody().flush();
                assertThat(release.await(5, SECONDS)).isTrue();
            };
            try {
                assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_INVALID_RESPONSE, BODY_MARKER);
            }
            finally {
                release.countDown();
            }
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testTimeoutIncludesTheResponseBody()
            throws Exception
    {
        CountDownLatch headersSent = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(directory, new Duration(1000, MILLISECONDS))) {
            ConnectorSession session = fixture.session();
            fixture.handler = exchange -> {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write('[');
                exchange.getResponseBody().flush();
                headersSent.countDown();
                assertThat(release.await(10, SECONDS)).isTrue();
                exchange.getResponseBody().write(']');
            };
            long start = System.nanoTime();
            try {
                assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_CATALOG_UNAVAILABLE, BODY_MARKER);
                assertThat(headersSent.getCount()).isZero();
                assertThat(System.nanoTime() - start).isLessThan(SECONDS.toNanos(5));
            }
            finally {
                release.countDown();
            }
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testTruncatedResponseIsSanitized()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            fixture.handler = exchange -> {
                exchange.sendResponseHeaders(200, 4096);
                exchange.getResponseBody().write((jwt + BODY_MARKER).getBytes(UTF_8));
                exchange.getResponseBody().flush();
            };
            assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_CATALOG_UNAVAILABLE, jwt, BODY_MARKER);
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testDeadlineExpiresDuringBoundedParsing()
            throws Exception
    {
        ExpiringTicker ticker = new ExpiringTicker(SECONDS.toNanos(10));
        try (Fixture fixture = new Fixture(directory, new Duration(10, SECONDS), Clock.systemUTC(), ticker)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            fixture.handler = exchange -> respond(exchange, 200, "[" + " ".repeat(8192) + "{\"name\":\"valid\"}]");
            // Four ticker reads precede parsing. The next pair checks the first bounded read.
            ticker.expireOnCheck(7);
            assertFailure(() -> fixture.client.listNamespaces(session), HOGLAKE_CATALOG_UNAVAILABLE, jwt, BODY_MARKER, CATALOG, PRINCIPAL);
            assertThat(ticker.checks).isGreaterThanOrEqualTo(7);
            assertThat(fixture.requests).hasValue(1);
        }
    }

    @Test
    void testDeadlineExpiresAfterEachDtoConversion()
            throws Exception
    {
        ExpiringTicker ticker = new ExpiringTicker(SECONDS.toNanos(10));
        try (Fixture fixture = new Fixture(directory, new Duration(10, SECONDS), Clock.systemUTC(), ticker)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            fixture.handler = exchange -> {
                assertRequest(exchange, jwt);
                switch (exchange.getRequestURI().getRawPath()) {
                    case "/v1/catalogs/catalog-a/namespaces" -> respond(exchange, 200, "[{\"name\":\"namespace-a\"}]");
                    case "/v1/catalogs/catalog-a/namespaces/namespace-a/tables" -> respond(exchange, 200, "[{\"name\":\"table-a\",\"table_uuid\":\"synthetic-table-a\"}]");
                    case "/v1/catalogs/catalog-a/namespaces/namespace-a/tables/table-a" -> respond(exchange, 200, TABLE_JSON);
                    default -> throw new AssertionError("Unexpected metadata route");
                }
            };
            List<Runnable> calls = List.of(
                    () -> fixture.client.listNamespaces(session),
                    () -> fixture.client.listTables(session, "namespace-a"),
                    () -> fixture.client.table(session, "namespace-a", "table-a"));
            for (Runnable call : calls) {
                ticker.reset();
                call.run();
                // The penultimate check follows conversion. The final check guards the public return.
                int conversionCheck = ticker.checks - 1;
                ticker.expireOnCheck(conversionCheck);
                assertFailure(call, HOGLAKE_CATALOG_UNAVAILABLE, jwt, BODY_MARKER, CATALOG, PRINCIPAL);
                assertThat(ticker.checks).isEqualTo(conversionCheck);
            }
            assertThat(fixture.requests).hasValue(6);
        }
    }

    @Test
    void testDeadlineGuardsEveryEmptyMetadataResult()
            throws Exception
    {
        ExpiringTicker ticker = new ExpiringTicker(SECONDS.toNanos(10));
        try (Fixture fixture = new Fixture(directory, new Duration(10, SECONDS), Clock.systemUTC(), ticker)) {
            String jwt = fixture.jwt(CATALOG, PRINCIPAL, TENANT, Instant.now());
            ConnectorSession session = session(USER, PRINCIPAL, jwt);
            fixture.handler = exchange -> {
                assertRequest(exchange, jwt);
                if (exchange.getRequestURI().getRawQuery() != null) {
                    respond(exchange, 404, BODY_MARKER);
                    return;
                }
                respond(exchange, 200, "[]");
            };
            List<Runnable> calls = List.of(
                    () -> fixture.client.listNamespaces(session),
                    () -> fixture.client.listTables(session, "namespace-a"),
                    () -> fixture.client.table(session, "namespace-a", "table-a"));
            for (Runnable call : calls) {
                ticker.reset();
                call.run();
                int returnCheck = ticker.checks;
                ticker.expireOnCheck(returnCheck);
                assertFailure(call, HOGLAKE_CATALOG_UNAVAILABLE, jwt, BODY_MARKER, CATALOG, PRINCIPAL);
                assertThat(ticker.checks).isEqualTo(returnCheck);
            }
            assertThat(fixture.requests).hasValue(6);
        }
    }

    private static void assertRequest(HttpExchange exchange, String jwt)
    {
        assertThat(exchange.getRequestMethod()).isEqualTo("GET");
        assertThat(exchange.getRequestHeaders().get("Authorization")).containsExactly("Bearer " + jwt);
        assertThat(exchange.getRequestHeaders().getFirst("Accept-Encoding")).isEqualTo("identity");
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException
    {
        byte[] bytes = body.getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static void assertFailure(Runnable action, ErrorCodeSupplier code, String... forbidden)
    {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TrinoException.class, failure -> {
            assertThat(failure.getErrorCode()).isEqualTo(code.toErrorCode());
            assertThat(failure.getCause()).isNull();
            assertThat(failure.getSuppressed()).isEmpty();
            StringWriter trace = new StringWriter();
            failure.printStackTrace(new PrintWriter(trace));
            assertThat(trace.toString()).doesNotContain(forbidden).doesNotContain("Bearer ", "Authorization", "totals=false");
            assertThat(failure.toString()).doesNotContain(forbidden);
        });
    }

    @FunctionalInterface
    private interface Handler
    {
        void handle(HttpExchange exchange)
                throws IOException, InterruptedException;
    }

    private static final class ExpiringTicker
            implements LongSupplier
    {
        private final long timeoutNanos;
        private int checks;
        private int expiryCheck = Integer.MAX_VALUE;

        private ExpiringTicker(long timeoutNanos)
        {
            this.timeoutNanos = timeoutNanos;
        }

        public void expireOnCheck(int check)
        {
            assertThat(check).isPositive();
            checks = 0;
            expiryCheck = check;
        }

        public void reset()
        {
            checks = 0;
            expiryCheck = Integer.MAX_VALUE;
        }

        @Override
        public long getAsLong()
        {
            checks++;
            if (checks >= expiryCheck) {
                return timeoutNanos;
            }
            return 0;
        }
    }

    private static final class MutableClock
            extends Clock
    {
        private final AtomicReference<Instant> now;
        private final ZoneId zone;

        private MutableClock(AtomicReference<Instant> now, ZoneId zone)
        {
            this.now = now;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone()
        {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return new MutableClock(now, zone);
        }

        @Override
        public Instant instant()
        {
            return now.get();
        }
    }

    private static final class Fixture
            implements AutoCloseable
    {
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final HttpServer server;
        private final AtomicReference<AssertionError> handlerFailure = new AtomicReference<>();
        private final AtomicInteger requests = new AtomicInteger();
        private final KeyPair keyPair;
        private final URI endpoint;
        private final HoglakeSharedConfig config;
        private final HoglakeSharedHttpClient client;
        private volatile Handler handler = _ -> {
            throw new AssertionError("Unexpected HTTP request");
        };

        private Fixture(Path directory)
                throws Exception
        {
            this(directory, new Duration(10, SECONDS));
        }

        private Fixture(Path directory, Duration timeout)
                throws Exception
        {
            this(directory, timeout, Clock.systemUTC());
        }

        private Fixture(Path directory, Duration timeout, Clock clock)
                throws Exception
        {
            this(directory, timeout, clock, System::nanoTime);
        }

        private Fixture(Path directory, Duration timeout, Clock clock, LongSupplier ticker)
                throws Exception
        {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            keyPair = generator.generateKeyPair();
            Path jwks = Files.createTempFile(directory, "shared-http-", ".json");
            writeJwks(jwks, keyPair, "synthetic-key");
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            config = config(jwks, endpoint)
                    .setIssuer(endpoint + "/issuer")
                    .setRequestTimeout(timeout);
            client = new HoglakeSharedHttpClient(config, new HoglakeSharedSessionVerifier(config, clock, System::nanoTime), ticker);
            server.createContext("/", exchange -> {
                requests.incrementAndGet();
                try (exchange) {
                    handler.handle(exchange);
                }
                catch (AssertionError failure) {
                    handlerFailure.compareAndSet(null, failure);
                }
                catch (IOException _) {
                    // Cancellation can disconnect a streaming response.
                }
                catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            });
            server.start();
        }

        public String jwt(String catalog, String principal, String tenant, Instant now)
                throws Exception
        {
            Map<String, Object> claims = claims(catalog, principal, tenant, now);
            claims.put("iss", config.getIssuer());
            return token(keyPair, "synthetic-key", claims);
        }

        public ConnectorSession session()
                throws Exception
        {
            return HoglakeSharedSessionTestSupport.session(USER, PRINCIPAL, jwt(CATALOG, PRINCIPAL, TENANT, Instant.now()));
        }

        @Override
        public void close()
                throws Exception
        {
            try (AutoCloseable handlerAssertions = () -> {
                AssertionError failure = handlerFailure.get();
                if (failure != null) {
                    throw failure;
                }
            };
                    AutoCloseable executorCleanup = () -> {
                        executor.shutdownNow();
                        assertThat(executor.awaitTermination(5, SECONDS)).isTrue();
                    };
                    AutoCloseable serverCleanup = () -> server.stop(0)) {
                client.close();
            }
        }
    }
}
