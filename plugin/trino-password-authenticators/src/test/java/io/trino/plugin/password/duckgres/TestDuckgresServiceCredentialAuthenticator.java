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
package io.trino.plugin.password.duckgres;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.Duration;
import io.trino.plugin.password.PasswordAuthenticatorPlugin;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.LoadedConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDuckgresServiceCredentialAuthenticator
{
    private static final String USER = "example-team.svc_0123456789abcdef01234567";
    private static final String SECRET = "synthetic-service-secret";

    @TempDir
    Path temporaryDirectory;

    private HttpServer server;
    private JettyHttpClient httpClient;
    private DuckgresServiceCredentialConfig config;
    private Path tokenFile;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<DuckgresServiceCredentialAuthenticator.CredentialRequest> sentCredential = new AtomicReference<>();
    private volatile String response;
    private volatile int status = 200;
    private volatile int delayMillis;
    private volatile String acceptedToken = "synthetic-current-token";

    @BeforeEach
    void setUp()
            throws IOException
    {
        tokenFile = Files.writeString(temporaryDirectory.resolve("token"), "synthetic-current-token\nsynthetic-previous-token\n");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/authenticate", this::handle);
        server.start();
        config = new DuckgresServiceCredentialConfig()
                .setEndpoint(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/authenticate"))
                .setAllowInsecureHttp(true)
                .setTokenFile(tokenFile.toFile());
        httpClient = new JettyHttpClient(new HttpClientConfig().setRequestTimeout(new Duration(5, SECONDS)));
        response = response(USER, Instant.now().plusSeconds(900).toString(), List.of("org_example_team", "tier_scale"));
    }

    @AfterEach
    void tearDown()
    {
        httpClient.close();
        server.stop(0);
    }

    @Test
    void testReportsAuthenticatedCellAndReloadsToken()
            throws IOException
    {
        response = "{\"cell_id\":\"registered:cell-a\"}";
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        LoadedConfiguration loadedConfiguration = authenticator;
        String revision = loadedConfiguration.loadedRevision();
        assertThat(revision).startsWith("service-auth-v1:sha256:").hasSize(87);
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-current-token");
        assertThat(sentCredential.get()).isNull();

        Files.writeString(tokenFile, "synthetic-rotated-token\n");
        assertUnavailable(authenticator);
        acceptedToken = "synthetic-rotated-token";
        assertThat(loadedConfiguration.loadedRevision()).isEqualTo(revision);
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-rotated-token");
        response = "{\"cell_id\":\"registered:cell-b\"}";
        assertThat(loadedConfiguration.loadedRevision()).isNotEqualTo(revision);
        assertThat(requests.get()).isEqualTo(4);
    }

    @Test
    void testLoadedRevisionContract()
    {
        config.setEndpoint(URI.create("https://auth.example.com/auth/trino/service-credentials"));
        try (var client = new TestingHttpClient(request -> {
            assertThat(request.getMethod()).isEqualTo("GET");
            assertThat(request.getUri().getPath()).isEqualTo("/auth/trino/service-credentials");
            return mockResponse(OK, JSON_UTF_8, "{\"cell_id\":\"registered:cell-a\"}");
        })) {
            var authenticator = new DuckgresServiceCredentialAuthenticator(config, client);
            assertThat(authenticator.loadedRevision()).isEqualTo("service-auth-v1:sha256:e404f60570334ffe2d55047999bc09477fefa5f13ae77e72b5d3d47d10dce21d");
            config.setEndpoint(URI.create("https://other.example.com/auth/trino/service-credentials"));
            assertThat(new DuckgresServiceCredentialAuthenticator(config, client).loadedRevision()).isNotEqualTo(authenticator.loadedRevision());
        }
    }

    @Test
    void testLoadedRevisionRejectsMalformedAndOversizedResponses()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (String body : List.of("not-json", "{}", "null", "{\"cell_id\":null}", "{\"cell_id\":\"\"}", "{\"cell_id\":\" \\n\"}", "{\"cell_id\":\"" + "x".repeat(65537) + "\"}")) {
            response = body;
            assertUnavailable(authenticator);
        }
    }

    @Test
    void testLoadedRevisionRejectsErrorsAndDoesNotFollowRedirects()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (int code : List.of(301, 302, 307, 401, 403, 500, 503)) {
            status = code;
            response = "sensitive upstream error";
            assertUnavailable(authenticator);
        }
        assertThat(requests.get()).isEqualTo(7);
    }

    @Test
    void testLoadedRevisionTimesOut()
    {
        delayMillis = 400;
        response = "{\"cell_id\":\"registered:cell-a\"}";
        try (var timeoutClient = new JettyHttpClient(new HttpClientConfig().setRequestTimeout(new Duration(100, MILLISECONDS)))) {
            assertUnavailable(new DuckgresServiceCredentialAuthenticator(config, timeoutClient));
        }
    }

    @Test
    void testLoadedRevisionMissingOrInvalidTokenFailsClosed()
            throws IOException
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (String token : List.of("", " \n", "bad token", "x".repeat(8193))) {
            Files.writeString(tokenFile, token);
            assertUnavailable(authenticator);
        }
        Files.delete(tokenFile);
        assertUnavailable(authenticator);
        assertThat(requests.get()).isZero();
    }

    @Test
    void testAuthenticatesIdentityAndGroupsAndReloadsToken()
            throws IOException
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        var identity = authenticator.createAuthenticatedIdentity(USER, SECRET).orElseThrow();
        assertThat(identity.getUser()).isEqualTo(USER);
        assertThat(identity.getPrincipal().orElseThrow().getName()).isEqualTo(USER);
        assertThat(identity.getGroups()).containsExactlyInAnyOrder("org_example_team", "tier_scale");
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-current-token");
        assertThat(sentCredential.get()).isEqualTo(new DuckgresServiceCredentialAuthenticator.CredentialRequest(USER, SECRET));

        Files.writeString(tokenFile, "synthetic-rotated-token\n");
        assertThat(authenticator.createAuthenticatedPrincipal(USER, SECRET).getName()).isEqualTo(USER);
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-rotated-token");
        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    void testDoesNotCacheRevocation()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        authenticator.createAuthenticatedIdentity(USER, SECRET);
        status = 401;
        assertDenied(authenticator);
        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    void testRejectsNonServiceAndUnqualifiedUsersBeforeCallingEndpoint()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (String user : List.of("root", "example-team.root", "svc_0123456789abcdef01234567", "example.svc_short", "another.example.svc_0123456789abcdef01234567")) {
            assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity(user, SECRET)).isInstanceOf(AccessDeniedException.class);
        }
        assertThat(requests.get()).isZero();
    }

    @Test
    void testRejectsMalformedExpiredAndMismatchedResponses()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (String body : List.of(
                "not-json",
                "{}",
                "null",
                response(USER, Instant.now().minusSeconds(1).toString(), List.of("org_example_team", "tier_scale")),
                response(USER, "not-a-date", List.of("org_example_team", "tier_scale")),
                response("other.svc_0123456789abcdef01234567", Instant.now().plusSeconds(900).toString(), List.of("org_other", "tier_scale")),
                response(USER, Instant.now().plusSeconds(900).toString(), List.of()),
                response(USER, Instant.now().plusSeconds(900).toString(), List.of("org_other", "tier_scale")),
                response(USER, Instant.now().plusSeconds(900).toString(), List.of("org_example_team", "tier_unknown")),
                response(USER, Instant.now().plusSeconds(900).toString(), List.of("org_example_team", "tier_scale", "admin")),
                "x".repeat(65537))) {
            response = body;
            assertDenied(authenticator);
        }
    }

    @Test
    void testRejectsErrorsAndDoesNotFollowRedirects()
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (int code : List.of(301, 302, 307, 401, 403, 500, 503)) {
            status = code;
            response = "sensitive upstream error";
            assertDenied(authenticator);
        }
        assertThat(requests.get()).isEqualTo(7);
    }

    @Test
    void testTimesOut()
    {
        delayMillis = 400;
        try (var timeoutClient = new JettyHttpClient(new HttpClientConfig().setRequestTimeout(new Duration(100, MILLISECONDS)))) {
            assertDenied(new DuckgresServiceCredentialAuthenticator(config, timeoutClient));
        }
    }

    @Test
    void testMissingOrInvalidTokenFailsClosed()
            throws IOException
    {
        var authenticator = new DuckgresServiceCredentialAuthenticator(config, httpClient);
        for (String token : List.of("", " \n", "bad token", "x".repeat(8193))) {
            Files.writeString(tokenFile, token);
            assertDenied(authenticator);
        }
        Files.delete(tokenFile);
        assertDenied(authenticator);
        assertThat(requests.get()).isZero();
    }

    @Test
    void testRequiresExplicitInsecureHttpOptIn()
    {
        config.setAllowInsecureHttp(false);
        assertThatThrownBy(() -> new DuckgresServiceCredentialAuthenticator(config, httpClient)).isInstanceOf(IllegalArgumentException.class);
        for (String endpoint : List.of("https://secret@auth.example.com/check", "https://auth.example.com/check?secret=x", "https://auth.example.com/check#fragment", "file:///token")) {
            config.setEndpoint(URI.create(endpoint));
            assertThatThrownBy(() -> new DuckgresServiceCredentialAuthenticator(config, httpClient)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testFactoryAndPluginRegistration()
    {
        assertThat(new PasswordAuthenticatorPlugin().getPasswordAuthenticatorFactories())
                .extracting(factory -> factory.getName()).contains("duckgres-service-credential", "file");
        var authenticator = new DuckgresServiceCredentialAuthenticatorFactory().create(ImmutableMap.of(
                "duckgres-service-credential.endpoint", config.getEndpoint().toString(),
                "duckgres-service-credential.token-file", tokenFile.toString(),
                "duckgres-service-credential.allow-insecure-http", "true"));
        assertThat(authenticator.createAuthenticatedIdentity(USER, SECRET).orElseThrow().getGroups())
                .containsExactlyInAnyOrder("org_example_team", "tier_scale");
    }

    private static String response(String user, String expiresAt, List<String> groups)
    {
        return jsonCodec(DuckgresServiceCredentialAuthenticator.CredentialResponse.class)
                .toJson(new DuckgresServiceCredentialAuthenticator.CredentialResponse(user, groups, expiresAt));
    }

    private static void assertDenied(DuckgresServiceCredentialAuthenticator authenticator)
    {
        assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity(USER, SECRET))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Invalid service credential")
                .hasNoCause();
    }

    private static void assertUnavailable(DuckgresServiceCredentialAuthenticator authenticator)
    {
        assertThatThrownBy(authenticator::loadedRevision)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Service credential configuration is unavailable")
                .hasNoCause();
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        requests.incrementAndGet();
        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        if (exchange.getRequestMethod().equals("GET")) {
            assertThat(exchange.getRequestBody().readAllBytes()).isEmpty();
            if (!("Bearer " + acceptedToken).equals(authorization.get())) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
        }
        else {
            sentCredential.set(jsonCodec(DuckgresServiceCredentialAuthenticator.CredentialRequest.class).fromJson(exchange.getRequestBody().readAllBytes()));
        }
        try {
            Thread.sleep(delayMillis);
        }
        catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
        exchange.getResponseHeaders().set("Location", config.getEndpoint().toString());
        byte[] bytes = response.getBytes(UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (exchange) {
            exchange.getResponseBody().write(bytes);
        }
    }
}
