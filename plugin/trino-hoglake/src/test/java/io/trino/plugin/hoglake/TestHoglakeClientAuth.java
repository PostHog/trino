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
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The connector's side of Hoglake authentication: a token minted by client
 * credentials, cached, attached to every request, and replaced once when
 * Hoglake answers 401.
 */
class TestHoglakeClientAuth
{
    private static final String NAMESPACE = "{\"name\":\"ns\",\"namespace_id\":1}";

    private HttpServer server;
    private ExecutorService executor;
    private final AtomicInteger mints = new AtomicInteger();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> userAgents = new CopyOnWriteArrayList<>();
    private final List<String> tokenRequests = new CopyOnWriteArrayList<>();
    private volatile int tokenStatus = 200;
    private volatile long tokenDelayMillis;
    // Hoglake accepts only the token with this mint number.
    private volatile int acceptedMint = 1;

    @BeforeEach
    void start()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth/token", exchange -> {
            try (exchange) {
                tokenRequests.add(exchange.getRequestHeaders().getFirst("Authorization") + " " + new String(exchange.getRequestBody().readAllBytes(), UTF_8));
                if (tokenDelayMillis > 0) {
                    try {
                        Thread.sleep(tokenDelayMillis);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (tokenStatus != 200) {
                    respond(exchange, tokenStatus, "{\"error\":\"invalid_client\"}");
                    return;
                }
                respond(exchange, 200, "{\"access_token\":\"t" + mints.incrementAndGet() + "\",\"token_type\":\"Bearer\",\"expires_in\":900}");
            }
        });
        server.createContext("/v1/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                authorizations.add(String.valueOf(authorization));
                userAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
                if (!("Bearer t" + acceptedMint).equals(authorization)) {
                    exchange.getResponseHeaders().add("WWW-Authenticate", "Bearer error=\"unknown_key\"");
                    respond(exchange, 401, "{\"error\":\"unknown_key\"}");
                    return;
                }
                respond(exchange, 201, NAMESPACE);
            }
        });
        // One thread per exchange, so a slow token response does not hold up Hoglake's.
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
    }

    @AfterEach
    void stop()
    {
        server.stop(0);
        executor.shutdownNow();
    }

    @Test
    void attachesAMintedTokenAndReusesIt()
    {
        try (HoglakeClient client = client()) {
            client.createNamespace("ns");
            client.createNamespace("ns");
        }
        assertThat(mints.get()).isEqualTo(1);
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1");
        assertThat(userAgents).allMatch(HoglakeClient.USER_AGENT::equals);
        // RFC 6749 §2.3.1 Basic credentials, client_credentials grant, hoglake audience.
        assertThat(tokenRequests.getFirst())
                .startsWith("Basic ")
                .endsWith("grant_type=client_credentials&audience=hoglake");
    }

    @Test
    void aRefusedTokenIsReplacedOnceAndTheRequestRetried()
    {
        try (HoglakeClient client = client()) {
            client.createNamespace("ns");
            // Hoglake stops accepting t1, e.g. after a key rotation it has not loaded.
            acceptedMint = 2;
            client.createNamespace("ns");
        }
        assertThat(mints.get()).isEqualTo(2);
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1", "Bearer t2");
    }

    @Test
    void aSecond401IsNotRetriedAgain()
    {
        acceptedMint = -1;
        try (HoglakeClient client = client()) {
            assertThatThrownBy(() -> client.createNamespace("ns")).isInstanceOf(TrinoException.class);
        }
        assertThat(authorizations).hasSize(2);
    }

    @Test
    void aFailingTokenEndpointIsAnUnavailableCatalog()
    {
        tokenStatus = 401;
        try (HoglakeClient client = client()) {
            assertThatThrownBy(() -> client.createNamespace("ns")).isInstanceOfSatisfying(
                    TrinoException.class,
                    exception -> assertThat(exception.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
        }
        assertThat(authorizations).isEmpty();
    }

    @Test
    void withoutAuthNoAuthorizationHeaderIsSent()
    {
        acceptedMint = -1;
        try (HoglakeClient client = new HoglakeClient(base(), "lake")) {
            assertThatThrownBy(() -> client.createNamespace("ns")).isInstanceOf(TrinoException.class);
        }
        // One attempt: with no token source there is nothing a retry could change.
        assertThat(authorizations).containsExactly("null");
        assertThat(userAgents).containsExactly(HoglakeClient.USER_AGENT);
    }

    @Test
    void mintingAndTheRetryShareTheRequestTimeout()
    {
        // Each mint takes 600ms against a 1s request timeout: the first fits, the
        // replacement after the 401 does not, so the retry must never be sent.
        // Write recovery relies on this: it passes its remaining budget as the
        // request timeout, and a commit retry must not land after it.
        tokenDelayMillis = 600;
        acceptedMint = 2;
        long started = System.nanoTime();
        try (HoglakeClient client = client(Duration.ofSeconds(1))) {
            assertThatThrownBy(() -> client.createNamespace("ns")).isInstanceOf(TrinoException.class);
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1500));
        assertThat(authorizations).containsExactly("Bearer t1");
    }

    @Test
    void aTokenResponseThatStallsMidBodyIsAbandoned()
            throws Exception
    {
        // Headers, then silence: the mint times out, and the exchange has to be
        // cancelled rather than left holding a connection open.
        try (ServerSocket stalling = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Boolean> closedByClient = CompletableFuture.supplyAsync(() -> {
                try (Socket socket = stalling.accept()) {
                    InputStream in = socket.getInputStream();
                    readHeaders(in);
                    socket.getOutputStream().write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{".getBytes(UTF_8));
                    socket.getOutputStream().flush();
                    socket.setSoTimeout(5_000);
                    // Drain the form body; EOF means the client gave up on the exchange.
                    while (in.read() != -1) {
                        // keep reading
                    }
                    return true;
                }
                catch (SocketTimeoutException e) {
                    return false;
                }
                catch (IOException e) {
                    // A reset is the client closing too.
                    return true;
                }
            });
            HoglakeTokenSource tokens = new HoglakeTokenSource.ClientCredentials(
                    URI.create("http://127.0.0.1:" + stalling.getLocalPort() + "/oauth/token"),
                    "hlc_trino",
                    "s3cret",
                    "hoglake",
                    HttpClient.newHttpClient());
            assertThatThrownBy(() -> tokens.token(Duration.ofMillis(500))).isInstanceOfSatisfying(
                    TrinoException.class,
                    exception -> assertThat(exception.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
            assertThat(closedByClient.get(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void readHeaders(InputStream in)
            throws IOException
    {
        int matched = 0;
        byte[] end = "\r\n\r\n".getBytes(UTF_8);
        while (matched < end.length) {
            int b = in.read();
            if (b == -1) {
                throw new IOException("request ended before its headers");
            }
            matched = b == end[matched] ? matched + 1 : (b == end[0] ? 1 : 0);
        }
    }

    private HoglakeClient client()
    {
        return client(Duration.ofSeconds(10));
    }

    private HoglakeClient client(Duration requestTimeout)
    {
        HoglakeTokenSource tokens = new HoglakeTokenSource.ClientCredentials(
                URI.create(base() + "/oauth/token"),
                "hlc_trino",
                "s3cret",
                "hoglake",
                HttpClient.newHttpClient());
        return new HoglakeClient(base(), "lake", requestTimeout, tokens);
    }

    private String base()
    {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException
    {
        byte[] bytes = body.getBytes(UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
