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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.StreamingBodyGenerator;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.server.security.TrinoCallerTokenClient.AcquisitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.ACCEPT_ENCODING;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_ENCODING;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.BAD_REQUEST;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpVersion.HTTP_1;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTrinoCallerTokenClient
{
    private static final URI ISSUER = URI.create("https://issuer.example.test/oauth/token");
    private static final String LOGIN = "example-db.alice+ ü&=name";
    private static final String PASSWORD = "synthetic +&=%:\n密码";
    private static final String SUCCESS = "{\"access_token\":\"synthetic-opaque-token\",\"token_type\":\"Bearer\",\"expires_in\":900}";

    @Test
    void testExactProofAndRotatedCellToken(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-one\n");
        var requests = new AtomicInteger();
        try (var httpClient = new TestingHttpClient(request -> {
            int attempt = requests.incrementAndGet();
            assertThat(request.getUri()).isEqualTo(ISSUER);
            assertThat(request.getMethod()).isEqualTo("POST");
            assertThat(request.getHttpVersion()).contains(HTTP_1);
            assertThat(request.isFollowRedirects()).isFalse();
            assertThat(request.getHeader(ACCEPT_ENCODING)).isEqualTo("identity");
            assertThat(request.getRequestTimeout()).contains(new Duration(5, SECONDS));
            assertThat(request.getIdleTimeout()).contains(new Duration(5, SECONDS));
            assertThat(request.getMaxResponseContentLength()).contains(DataSize.ofBytes(64 * 1024));
            assertThat(request.getHeader(CONTENT_TYPE)).isEqualTo("application/x-www-form-urlencoded; charset=utf-8");
            assertThat(request.getHeaders().get(AUTHORIZATION)).containsExactly("Bearer synthetic-cell-" + (attempt == 1 ? "one" : "two"));
            var body = (StreamingBodyGenerator) request.getBodyGenerator();
            assertThat(decodeForm(new String(body.source().readAllBytes(), UTF_8))).isEqualTo(ImmutableMap.of(
                    "grant_type", "urn:posthog:params:oauth:grant-type:trino-login",
                    "username", LOGIN,
                    "password", PASSWORD));
            return mockResponse(OK, JSON_UTF_8, SUCCESS.replace("synthetic-opaque-token", "synthetic+opaque/token_" + attempt + "=="));
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file));
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic+opaque/token_1==");
            Files.writeString(file, "synthetic-cell-two\n");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic+opaque/token_2==");
            assertThat(requests.get()).isEqualTo(2);
        }
    }

    @Test
    void testDisabledDoesNotReadEvidence()
    {
        try (var httpClient = new TestingHttpClient(_ -> {
            throw new AssertionError("Disabled acquisition must not send a request");
        })) {
            var client = new TrinoCallerTokenClient(httpClient, new TrinoCallerTokenConfig());
            assertSanitizedFailure(client);
        }
    }

    @Test
    void testEncodedRequestBoundAndEmptyPassword(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        int maxRequestBytes = 16 * 1024;
        String prefix = "密码 +&=";
        String fixedForm = "grant_type=" + URLEncoder.encode("urn:posthog:params:oauth:grant-type:trino-login", UTF_8) +
                "&username=" + URLEncoder.encode(LOGIN, UTF_8) +
                "&password=" + URLEncoder.encode(prefix, UTF_8);
        String exactPassword = prefix + "x".repeat(maxRequestBytes - fixedForm.getBytes(UTF_8).length);
        var requests = new AtomicInteger();
        try (var httpClient = new TestingHttpClient(request -> {
            requests.incrementAndGet();
            var body = (StreamingBodyGenerator) request.getBodyGenerator();
            byte[] bytes = body.source().readAllBytes();
            assertThat(bytes).hasSize(maxRequestBytes);
            assertThat(decodeForm(new String(bytes, UTF_8))).containsEntry("password", exactPassword);
            return mockResponse(OK, JSON_UTF_8, SUCCESS);
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file));
            assertThat(client.acquire(LOGIN, exactPassword)).isEqualTo("synthetic-opaque-token");
            assertThat(requests.get()).isEqualTo(1);
            requests.set(0);
            for (String password : ImmutableList.of(exactPassword + "x", "+".repeat(6000), "密".repeat(2000), "")) {
                assertThatThrownBy(() -> client.acquire(LOGIN, password))
                        .isExactlyInstanceOf(AcquisitionException.class)
                        .hasMessage("Caller token acquisition failed")
                        .hasNoCause();
                assertThat(requests.get()).isZero();
            }
        }
    }

    @Test
    void testUnsafeBearerTokenHasNoSecretDiagnostics(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        for (String token : ImmutableList.of("synthetic token", " synthetic-token", "synthetic-token ", "synthetic\\r\\nAuthorization: synthetic-secret", "synthetic\\u0000token")) {
            try (var httpClient = new TestingHttpClient(_ -> mockResponse(OK, JSON_UTF_8, SUCCESS.replace("synthetic-opaque-token", token)))) {
                assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
            }
        }
    }

    @Test
    void testStrictSuccessValidation(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        for (String response : ImmutableList.of(
                "",
                "null",
                "[]",
                "{",
                "{}",
                SUCCESS + "{}",
                SUCCESS + "trailing",
                SUCCESS.replace("\"synthetic-opaque-token\"", "null"),
                SUCCESS.replace("\"synthetic-opaque-token\"", "42"),
                SUCCESS.replace("synthetic-opaque-token", ""),
                SUCCESS.replace("synthetic-opaque-token", "   "),
                SUCCESS.replace("\"Bearer\"", "null"),
                SUCCESS.replace("Bearer", "Basic"),
                SUCCESS.replace("900", "0"),
                SUCCESS.replace("900", "-1"),
                SUCCESS.replace("900", "1.0"),
                SUCCESS.replace("900", "1e3"),
                SUCCESS.replace("900", "\"900\""),
                SUCCESS.replace("900", "true"),
                SUCCESS.replace(",\"token_type\":\"Bearer\"", ""),
                SUCCESS.replace(",\"expires_in\":900", ""),
                SUCCESS.replace("\"access_token\":", "\"access_token\":\"synthetic-duplicate\",\"access_token\":"),
                SUCCESS.replace("\"token_type\":", "\"token_type\":\"Bearer\",\"token_type\":"),
                SUCCESS.replace("\"expires_in\":", "\"expires_in\":1,\"expires_in\":"),
                SUCCESS.replace("}", ",\"refresh_token\":\"synthetic-forbidden\"}"))) {
            try (var httpClient = new TestingHttpClient(_ -> mockResponse(OK, JSON_UTF_8, response))) {
                assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
            }
        }
    }

    @Test
    void testRefusedStatusDoesNotRetry(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        for (HttpStatus status : ImmutableList.of(
                HttpStatus.CREATED,
                HttpStatus.NO_CONTENT,
                HttpStatus.MOVED_PERMANENTLY,
                HttpStatus.FOUND,
                HttpStatus.TEMPORARY_REDIRECT,
                HttpStatus.PERMANENT_REDIRECT,
                HttpStatus.BAD_REQUEST,
                HttpStatus.UNAUTHORIZED,
                HttpStatus.FORBIDDEN,
                HttpStatus.NOT_FOUND,
                HttpStatus.TOO_MANY_REQUESTS,
                HttpStatus.INTERNAL_SERVER_ERROR,
                HttpStatus.SERVICE_UNAVAILABLE)) {
            var attempts = new AtomicInteger();
            try (var httpClient = new TestingHttpClient(_ -> {
                attempts.incrementAndGet();
                return mockResponse(status, JSON_UTF_8, SUCCESS);
            })) {
                assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
                assertThat(attempts.get()).isEqualTo(1);
            }
        }
    }

    @Test
    void testInvalidContentType(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        for (ImmutableListMultimap<HeaderName, String> headers : ImmutableList.of(
                ImmutableListMultimap.<HeaderName, String>of(),
                ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"),
                ImmutableListMultimap.of(CONTENT_TYPE, "application/json; charset=iso-8859-1"),
                ImmutableListMultimap.of(CONTENT_TYPE, "application/json", CONTENT_TYPE, "application/json"),
                ImmutableListMultimap.of(CONTENT_TYPE, "application/json, text/plain"),
                ImmutableListMultimap.of(CONTENT_TYPE, "invalid"))) {
            try (var httpClient = new TestingHttpClient(_ -> new TestingResponse(OK, headers, SUCCESS.getBytes(UTF_8)))) {
                assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
            }
        }
    }

    @Test
    void testResponseSizeBound(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        byte[] response = SUCCESS.getBytes(UTF_8);
        try (var httpClient = new TestingHttpClient(_ -> mockResponse(OK, JSON_UTF_8, SUCCESS))) {
            var exact = new TrinoCallerTokenClient(httpClient, config(file).setMaxResponseSize(DataSize.ofBytes(response.length)));
            assertThat(exact.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic-opaque-token");
            var oversized = new TrinoCallerTokenClient(httpClient, config(file).setMaxResponseSize(DataSize.ofBytes(response.length - 1)));
            assertSanitizedFailure(oversized);
        }
    }

    @Test
    void testUnexpectedEncoding(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        for (String encoding : ImmutableList.of("gzip", "br", "zstd", "identity, gzip", "")) {
            try (var httpClient = new TestingHttpClient(_ -> new TestingResponse(
                    OK,
                    ImmutableListMultimap.of(CONTENT_TYPE, "application/json", CONTENT_ENCODING, encoding),
                    SUCCESS.getBytes(UTF_8)))) {
                assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
            }
        }
    }

    @Test
    void testFileReadFailuresDoNotSendEvidence(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        try (var httpClient = new TestingHttpClient(_ -> {
            throw new AssertionError("Invalid cell credentials must not send a request");
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file));
            for (String credential : ImmutableList.of("", " \n", "synthetic\ninjected", "synthetic\r\nAuthorization: injected", "x".repeat(16 * 1024 + 1))) {
                Files.writeString(file, credential);
                assertSanitizedFailure(client);
            }
            Files.delete(file);
            assertSanitizedFailure(client);
        }
    }

    @Test
    void testTransportExceptionHasNoSecretDiagnostics(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        try (var httpClient = new TestingHttpClient(_ -> {
            throw new IOException(PASSWORD + " synthetic-cell-token synthetic-opaque-token");
        })) {
            assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config(file)));
        }
    }

    @Test
    void testRealIssuerDoesNotFollowRedirectOrRetry(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var redirectedRequests = new AtomicInteger();
        var status = new AtomicInteger(307);
        var assertionFailure = new AtomicReference<AssertionError>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth/token", exchange -> {
            try {
                requests.incrementAndGet();
                assertThat(exchange.getRequestHeaders().getFirst("Accept-Encoding")).isEqualTo("identity");
                assertThat(decodeForm(new String(exchange.getRequestBody().readAllBytes(), UTF_8))).containsEntry("password", PASSWORD);
                exchange.getResponseHeaders().set("Location", "/redirected");
                exchange.getResponseHeaders().set("Retry-After", "0");
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                byte[] response = SUCCESS.getBytes(UTF_8);
                exchange.sendResponseHeaders(status.get(), response.length);
                try (var output = exchange.getResponseBody()) {
                    output.write(response);
                }
            }
            catch (AssertionError failure) {
                assertionFailure.compareAndSet(null, failure);
                exchange.close();
                throw failure;
            }
        });
        server.createContext("/redirected", exchange -> {
            redirectedRequests.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try (var httpClient = new JettyHttpClient(httpConfig())) {
            var client = new TrinoCallerTokenClient(httpClient, localConfig(file, server));
            for (int responseStatus : new int[] {301, 302, 303, 307, 308, 400, 401, 429, 500, 503}) {
                status.set(responseStatus);
                int before = requests.get();
                assertSanitizedFailure(client);
                assertThat(requests.get()).isEqualTo(before + 1);
                assertThat(redirectedRequests.get()).isZero();
            }
            status.set(200);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic-opaque-token");
        }
        finally {
            server.stop(0);
            assertThat(assertionFailure.get()).isNull();
        }
    }

    @Test
    void testRealIssuerTimeoutDoesNotRetry(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var release = new CountDownLatch(1);
        var assertionFailure = new AtomicReference<AssertionError>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth/token", exchange -> {
            requests.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            try {
                if (!release.await(10, SECONDS)) {
                    throw new AssertionError("The test must release the issuer");
                }
            }
            catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
            catch (AssertionError failure) {
                assertionFailure.compareAndSet(null, failure);
                throw failure;
            }
            finally {
                exchange.close();
            }
        });
        server.start();
        try (var httpClient = new JettyHttpClient(httpConfig())) {
            var config = localConfig(file, server).setTimeout(new Duration(300, MILLISECONDS));
            assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config));
            assertThat(requests.get()).isEqualTo(1);
        }
        finally {
            release.countDown();
            server.stop(0);
            assertThat(assertionFailure.get()).isNull();
        }
    }

    @Test
    void testRealIssuerCompressedResponseIsRejected(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var compressed = new ByteArrayOutputStream();
        try (var output = new GZIPOutputStream(compressed)) {
            output.write(SUCCESS.replace("synthetic-opaque-token", "x".repeat(128 * 1024)).getBytes(UTF_8));
        }
        byte[] body = compressed.toByteArray();
        assertThat(body.length).isLessThan(1024);
        var requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth/token", exchange -> {
            requests.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try (var httpClient = new JettyHttpClient(httpConfig())) {
            var config = localConfig(file, server).setMaxResponseSize(DataSize.ofBytes(1024));
            assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, config));
            assertThat(requests.get()).isEqualTo(1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testRealIssuerDisconnectDoesNotReplayProof(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth/token", exchange -> {
            requests.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.close();
        });
        server.start();
        try (var httpClient = new JettyHttpClient(httpConfig())) {
            assertSanitizedFailure(new TrinoCallerTokenClient(httpClient, localConfig(file, server)));
            assertThat(requests.get()).isEqualTo(1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testNoReuseByDefault(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        try (var httpClient = new TestingHttpClient(_ -> numberedToken(requests, 900))) {
            var client = new TrinoCallerTokenClient(httpClient, config(file));
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-2");
            assertThat(client.reusableEntries()).isEmpty();
        }
    }

    @Test
    void testReuseForTheSameEvidenceOnly(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var ticker = new TestingTicker();
        try (var httpClient = new TestingHttpClient(_ -> numberedToken(requests, 900))) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), ticker);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            assertThat(requests.get()).isEqualTo(1);

            // Another password, another login, and a login and password that join to the same text
            assertThat(client.acquire(LOGIN, PASSWORD + "x")).isEqualTo("token-2");
            assertThat(client.acquire(LOGIN + "x", PASSWORD)).isEqualTo("token-3");
            assertThat(client.acquire(LOGIN + PASSWORD.substring(0, 1), PASSWORD.substring(1))).isEqualTo("token-4");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            assertThat(requests.get()).isEqualTo(4);

            ticker.increment(59, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(1, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-5");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-5");
            assertThat(requests.get()).isEqualTo(5);
        }
    }

    @Test
    void testReuseEndsAtHalfTheStatedLifetime(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var ticker = new TestingTicker();
        try (var httpClient = new TestingHttpClient(_ -> numberedToken(requests, 40))) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), ticker);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(19, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(1, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-2");
        }
        // A lifetime of one second is reused for half of it
        requests.set(0);
        try (var httpClient = new TestingHttpClient(_ -> numberedToken(requests, 1))) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), ticker);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(499, MILLISECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(1, MILLISECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-2");
        }
    }

    @Test
    void testFailedGrantIsNotKept(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        try (var httpClient = new TestingHttpClient(_ -> {
            if (requests.incrementAndGet() <= 2) {
                return mockResponse(BAD_REQUEST, JSON_UTF_8, "{\"error\":\"invalid_grant\"}");
            }
            return mockResponse(OK, JSON_UTF_8, SUCCESS);
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), new TestingTicker());
            assertSanitizedFailure(client);
            assertSanitizedFailure(client);
            assertThat(client.reusableEntries()).isEmpty();
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic-opaque-token");
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("synthetic-opaque-token");
            assertThat(requests.get()).isEqualTo(3);
        }
    }

    @Test
    void testConcurrentRequestsShareOneGrant(@TempDir Path directory)
            throws Exception
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var granting = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(8);
        try (var httpClient = new TestingHttpClient(_ -> {
            granting.countDown();
            release.await();
            return numberedToken(requests, 900);
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), new TestingTicker());
            List<Future<String>> tokens = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                tokens.add(executor.submit(() -> client.acquire(LOGIN, PASSWORD)));
            }
            assertThat(granting.await(10, SECONDS)).isTrue();
            // Let the other callers reach the grant that is in flight
            Thread.sleep(200);
            release.countDown();
            for (Future<String> token : tokens) {
                assertThat(token.get(10, SECONDS)).isEqualTo("token-1");
            }
            assertThat(requests.get()).isEqualTo(1);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testConcurrentRequestsShareOneReplacementGrant(@TempDir Path directory)
            throws Exception
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var ticker = new TestingTicker();
        var release = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(8);
        try (var httpClient = new TestingHttpClient(_ -> {
            if (requests.get() >= 1) {
                release.await();
            }
            return numberedToken(requests, 40);
        })) {
            // Reuse ends at half the lifetime, before the entry itself expires
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), ticker);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(20, SECONDS);
            List<Future<String>> tokens = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                tokens.add(executor.submit(() -> client.acquire(LOGIN, PASSWORD)));
                // Each caller sees the ended entry, or the replacement in flight, in turn
                Thread.sleep(50);
            }
            release.countDown();
            for (Future<String> token : tokens) {
                assertThat(token.get(10, SECONDS)).isEqualTo("token-2");
            }
            assertThat(requests.get()).isEqualTo(2);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testReuseIsCountedFromBeforeTheRequest(@TempDir Path directory)
            throws IOException
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        var requests = new AtomicInteger();
        var ticker = new TestingTicker();
        try (var httpClient = new TestingHttpClient(_ -> {
            // The issuer takes four seconds to answer
            ticker.increment(4, SECONDS);
            return numberedToken(requests, 900);
        })) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), ticker);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(55, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-1");
            ticker.increment(1, SECONDS);
            assertThat(client.acquire(LOGIN, PASSWORD)).isEqualTo("token-2");
        }
    }

    @Test
    void testReusableEntryHoldsNoEvidence(@TempDir Path directory)
            throws Exception
    {
        Path file = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        try (var httpClient = new TestingHttpClient(_ -> mockResponse(OK, JSON_UTF_8, SUCCESS))) {
            var client = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), new TestingTicker());
            client.acquire(LOGIN, PASSWORD);
            assertThat(client.reusableEntries()).hasSize(2);
            Set<String> fields = new HashSet<>();
            for (Object entry : client.reusableEntries()) {
                assertThat(entry.getClass().isRecord()).isTrue();
                assertThat(entry.toString()).doesNotContain(PASSWORD, LOGIN, "synthetic-opaque-token");
                for (RecordComponent component : entry.getClass().getRecordComponents()) {
                    fields.add(component.getName());
                    component.getAccessor().setAccessible(true);
                    Object value = component.getAccessor().invoke(entry);
                    assertThat(String.valueOf(value)).doesNotContain(PASSWORD);
                    if (value instanceof HashCode identifier) {
                        assertThat(identifier.bits()).isEqualTo(256);
                    }
                }
            }
            assertThat(fields).containsExactlyInAnyOrder("login", "identifier", "token", "reusableUntilNanos");

            // Another client has another key: the same evidence gives another identifier
            var other = new TrinoCallerTokenClient(httpClient, config(file).setReusePeriod(new Duration(60, SECONDS)), new TestingTicker());
            other.acquire(LOGIN, PASSWORD);
            assertThat(other.reusableEntries()).doesNotContainAnyElementsOf(
                    client.reusableEntries().stream().filter(entry -> entry.getClass().getSimpleName().equals("Evidence")).toList());
        }
    }

    private static Response numberedToken(AtomicInteger requests, int expiresIn)
    {
        return mockResponse(OK, JSON_UTF_8, "{\"access_token\":\"token-" + requests.incrementAndGet() + "\",\"token_type\":\"Bearer\",\"expires_in\":" + expiresIn + "}");
    }

    private static TrinoCallerTokenConfig config(Path file)
    {
        return new TrinoCallerTokenConfig()
                .setEnabled(true)
                .setIssuerUri(ISSUER)
                .setCellTokenFile(file.toFile());
    }

    private static TrinoCallerTokenConfig localConfig(Path file, HttpServer server)
    {
        return config(file)
                .setIssuerUri(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token"))
                .setAllowInsecureHttpForDevelopment(true);
    }

    private static HttpClientConfig httpConfig()
    {
        return new HttpClientConfig()
                .setHttp2Enabled(false)
                .setLogEnabled(false)
                .setGzipCompressionEnabled(false)
                .setBrotliCompressionEnabled(false)
                .setZstdCompressionEnabled(false);
    }

    private static Map<String, String> decodeForm(String form)
    {
        ImmutableMap.Builder<String, String> fields = ImmutableMap.builder();
        for (String field : form.split("&")) {
            String[] parts = field.split("=", 2);
            assertThat(parts).hasSize(2);
            fields.put(URLDecoder.decode(parts[0], UTF_8), URLDecoder.decode(parts[1], UTF_8));
        }
        return fields.buildOrThrow();
    }

    private static void assertSanitizedFailure(TrinoCallerTokenClient client)
    {
        assertThatThrownBy(() -> client.acquire(LOGIN, PASSWORD))
                .isExactlyInstanceOf(AcquisitionException.class)
                .hasMessage("Caller token acquisition failed")
                .hasNoCause()
                .satisfies(failure -> {
                    assertThat(failure.getSuppressed()).isEmpty();
                    assertThat(failure.getStackTrace()).isEmpty();
                });
    }
}
