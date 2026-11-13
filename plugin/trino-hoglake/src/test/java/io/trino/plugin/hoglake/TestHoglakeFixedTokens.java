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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two token sources that never talk to a token endpoint: a token given in
 * the catalog properties, and one read from a file that an operator rotates.
 * The server here is only Hoglake; there is no {@code /oauth/token}.
 */
class TestHoglakeFixedTokens
{
    private static final String NAMESPACE = "{\"name\":\"ns\",\"namespace_id\":1}";

    private HttpServer server;
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    // The one token Hoglake accepts; a test rotates it to stage a 401.
    private volatile String accepted = "t1";
    // What Hoglake does while refusing: the operator rotating the file mid-flight.
    private volatile Runnable onRefusal = () -> {};

    @TempDir
    Path dir;

    @BeforeEach
    void start()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                authorizations.add(String.valueOf(authorization));
                if (!("Bearer " + accepted).equals(authorization)) {
                    onRefusal.run();
                    exchange.getResponseHeaders().add("WWW-Authenticate", "Bearer error=\"invalid_token\"");
                    respond(exchange, 401, "{\"error\":\"invalid_token\"}");
                    return;
                }
                respond(exchange, 201, NAMESPACE);
            }
        });
        server.start();
    }

    @AfterEach
    void stop()
    {
        server.stop(0);
    }

    @Test
    void aFixedTokenIsSentUnchangedOnEveryRequest()
    {
        HoglakeTokenSource tokens = new HoglakeTokenSource.Fixed(" t1 ");
        assertThat(tokens.token(Duration.ofSeconds(1))).contains("t1");
        try (HoglakeClient client = client(tokens)) {
            client.createNamespace("ns");
            client.createNamespace("ns");
        }
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1");
    }

    @Test
    void aFixedTokenThatIsRefusedIsRetriedOnceAndThenFails()
    {
        accepted = "t2";
        try (HoglakeClient client = client(new HoglakeTokenSource.Fixed("t1"))) {
            assertThatThrownBy(() -> client.createNamespace("ns")).isInstanceOf(TrinoException.class);
        }
        // Nothing to refresh, so the retry carries the same token; then the 401 stands.
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1");
    }

    @Test
    void anEmptyFixedTokenIsRejected()
    {
        assertThatThrownBy(() -> new HoglakeTokenSource.Fixed("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTokenFileIsReadOnceAndReusedWhileUnchanged()
            throws IOException
    {
        Path file = dir.resolve("token");
        Files.writeString(file, "t1\n", UTF_8);
        HoglakeTokenSource tokens = new HoglakeTokenSource.TokenFile(file);
        try (HoglakeClient client = client(tokens)) {
            client.createNamespace("ns");
            // A rewrite that keeps the same content and time is not a change.
            client.createNamespace("ns");
        }
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1");
    }

    @Test
    void aRewrittenTokenFileIsPickedUpByItsModificationTime()
            throws IOException
    {
        Path file = dir.resolve("token");
        Files.writeString(file, "t1", UTF_8);
        Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_000_000_000L));
        HoglakeTokenSource tokens = new HoglakeTokenSource.TokenFile(file);
        try (HoglakeClient client = client(tokens)) {
            client.createNamespace("ns");
            accepted = "t2";
            Files.writeString(file, "t2", UTF_8);
            Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_000_001_000L));
            client.createNamespace("ns");
        }
        // The second request already carries the rotated token: no 401 was needed.
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t2");
    }

    @Test
    void aRefusedFileTokenIsReReadOnTheOneRetry()
            throws IOException
    {
        Path file = dir.resolve("token");
        Files.writeString(file, "t1", UTF_8);
        FileTime unchanged = FileTime.fromMillis(1_000_000_000_000L);
        Files.setLastModifiedTime(file, unchanged);
        HoglakeTokenSource tokens = new HoglakeTokenSource.TokenFile(file);
        try (HoglakeClient client = client(tokens)) {
            client.createNamespace("ns");
            // A rotation the modification time does not show (a copy that kept it).
            accepted = "t2";
            onRefusal = () -> {
                try {
                    Files.writeString(file, "t2", UTF_8);
                    Files.setLastModifiedTime(file, unchanged);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            client.createNamespace("ns");
        }
        assertThat(authorizations).containsExactly("Bearer t1", "Bearer t1", "Bearer t2");
    }

    @Test
    void aMissingOrEmptyTokenFileIsAnUnavailableCatalog()
            throws IOException
    {
        Path missing = dir.resolve("missing");
        assertThatThrownBy(() -> new HoglakeTokenSource.TokenFile(missing).token(Duration.ofSeconds(1)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()))
                .hasMessageContaining("does not exist");
        Path empty = dir.resolve("empty");
        Files.writeString(empty, " \n", UTF_8);
        assertThatThrownBy(() -> new HoglakeTokenSource.TokenFile(empty).token(Duration.ofSeconds(1)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()))
                .hasMessageContaining("is empty");
    }

    private HoglakeClient client(HoglakeTokenSource tokens)
    {
        return new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake", Duration.ofSeconds(10), tokens);
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
