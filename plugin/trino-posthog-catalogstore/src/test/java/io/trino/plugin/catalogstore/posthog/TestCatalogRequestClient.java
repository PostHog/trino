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
package io.trino.plugin.catalogstore.posthog;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.plugin.catalogstore.posthog.CatalogRequestClient.CreateRequest;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.DelegatedCatalogStore.CatalogRequestResult;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCatalogRequestClient
{
    private static final String SECRET = "synthetic-catalog-password";
    private static final Identity USER = Identity.ofUser("example_db.analyst");
    private static final CatalogName CATALOG = new CatalogName("ext_example_db__warehouse");
    private static final ConnectorName CONNECTOR = new ConnectorName("postgresql");
    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "connection-url", "jdbc:postgresql://db.example.net:6432/postgres?sslmode=require",
            "connection-user", "reader",
            "connection-password", SECRET);
    private static final JsonCodec<Map<String, Object>> BODY_CODEC = JsonCodec.mapJsonCodec(String.class, Object.class);

    @TempDir
    Path temporaryDirectory;

    private HttpServer server;
    private JettyHttpClient httpClient;
    private Path tokenFile;
    private PostHogCatalogStoreConfig config;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<Map<String, Object>> body = new AtomicReference<>();
    private volatile int status = 200;
    private volatile String response = "{}";
    private volatile String location;

    @BeforeEach
    void setUp()
            throws IOException
    {
        tokenFile = Files.writeString(temporaryDirectory.resolve("token"), "\nsynthetic-cell-token\nsynthetic-previous-token\n");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        config = new PostHogCatalogStoreConfig()
                .setCellId("cell-1")
                .setConnectionUrl("jdbc:postgresql://localhost:5432/catalogs")
                .setReadOnly(true)
                .setDelegateEndpoint(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/trino/catalog-requests/v1"))
                .setDelegateAllowInsecureHttp(true)
                .setDelegateTokenFile(tokenFile.toFile());
        httpClient = new JettyHttpClient(new HttpClientConfig().setRequestTimeout(new Duration(5, SECONDS)));
    }

    @AfterEach
    void tearDown()
    {
        httpClient.close();
        server.stop(0);
    }

    @Test
    void testCreateSendsIdentityPropertiesAndToken()
    {
        response = "{\"catalog\":\"ext_example_db__warehouse\",\"created\":true}";
        CatalogRequestResult result = client().createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, true);

        assertThat(result).isEqualTo(new CatalogRequestResult(CATALOG, true));
        assertThat(path.get()).isEqualTo("/trino/catalog-requests/v1/create");
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-cell-token");
        assertThat(body.get()).isEqualTo(Map.of(
                "user", "example_db.analyst",
                "catalog", "ext_example_db__warehouse",
                "connector", "postgresql",
                "properties", PROPERTIES,
                "if_not_exists", true));

        response = "{\"catalog\":\"ext_example_db__warehouse\",\"created\":false}";
        assertThat(client().createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, true).changed()).isFalse();
    }

    @Test
    void testDrop()
    {
        response = "{\"catalog\":\"ext_example_db__warehouse\",\"dropped\":true}";
        assertThat(client().dropCatalog(USER, CATALOG, false)).isEqualTo(new CatalogRequestResult(CATALOG, true));
        assertThat(path.get()).isEqualTo("/trino/catalog-requests/v1/drop");
        assertThat(body.get()).isEqualTo(Map.of(
                "user", "example_db.analyst",
                "catalog", "ext_example_db__warehouse",
                "if_exists", false));

        response = "{\"catalog\":\"ext_example_db__warehouse\",\"dropped\":false}";
        assertThat(client().dropCatalog(USER, CATALOG, true).changed()).isFalse();
    }

    @Test
    void testTokenIsReadPerRequest()
            throws IOException
    {
        response = "{\"catalog\":\"ext_example_db__warehouse\",\"dropped\":true}";
        CatalogRequestClient client = client();
        client.dropCatalog(USER, CATALOG, true);
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-cell-token");

        Files.writeString(tokenFile, "synthetic-rotated-token\n");
        client.dropCatalog(USER, CATALOG, true);
        assertThat(authorization.get()).isEqualTo("Bearer synthetic-rotated-token");

        // An unusable token is never sent
        Files.writeString(tokenFile, "token with spaces\n");
        assertUnavailable(() -> client.dropCatalog(USER, CATALOG, true));
        Files.writeString(tokenFile, "\n\n");
        assertUnavailable(() -> client.dropCatalog(USER, CATALOG, true));
        Files.writeString(tokenFile, "x".repeat(8193));
        assertUnavailable(() -> client.dropCatalog(USER, CATALOG, true));
        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    void testCreateStatusMapping()
    {
        CatalogRequestClient client = client();

        status = 400;
        response = "{\"error\":\"connection-url must use sslmode=require\"}";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()))
                .hasMessage("connection-url must use sslmode=require");

        status = 403;
        response = "{\"error\":\"catalog name must start with ext_example_db__\"}";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: catalog name must start with ext_example_db__");

        status = 409;
        response = "{\"error\":\"Catalog 'ext_example_db__warehouse' already exists\"}";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ALREADY_EXISTS.toErrorCode()))
                .hasMessage("Catalog 'ext_example_db__warehouse' already exists");

        // Not part of the create contract
        status = 404;
        response = "{\"error\":\"not found\"}";
        assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));

        for (int other : new int[] {401, 500, 502, 503}) {
            status = other;
            response = "{\"error\":\"internal detail\"}";
            assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
        }
    }

    @Test
    void testDropStatusMapping()
    {
        CatalogRequestClient client = client();

        status = 404;
        response = "{\"error\":\"Catalog 'ext_example_db__warehouse' does not exist\"}";
        assertThatThrownBy(() -> client.dropCatalog(USER, CATALOG, false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(CATALOG_NOT_FOUND.toErrorCode()))
                .hasMessage("Catalog 'ext_example_db__warehouse' does not exist");

        status = 400;
        response = "{\"error\":\"invalid catalog name\"}";
        assertThatThrownBy(() -> client.dropCatalog(USER, CATALOG, false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()));

        status = 403;
        response = "{\"error\":\"not your catalog\"}";
        assertThatThrownBy(() -> client.dropCatalog(USER, CATALOG, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: not your catalog");

        // Not part of the drop contract
        status = 409;
        assertUnavailable(() -> client.dropCatalog(USER, CATALOG, false));
        status = 500;
        assertUnavailable(() -> client.dropCatalog(USER, CATALOG, false));
    }

    @Test
    void testErrorMessagesAreBoundedAndSanitized()
    {
        CatalogRequestClient client = client();
        status = 400;
        response = "{\"error\":\"" + "a".repeat(600) + "\"}";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .hasMessage("a".repeat(512));

        response = "{\"error\":\"first line\\nsecond line\"}";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .hasMessage("first line second line");

        // A rejection without a readable message still maps to its class
        response = "not json";
        assertThatThrownBy(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()))
                .hasMessage("Invalid catalog request");
    }

    @Test
    void testMalformedSuccessIsUnavailable()
    {
        CatalogRequestClient client = client();
        response = "{\"catalog\":\"some_other_catalog\",\"created\":true}";
        assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
        response = "{\"catalog\":\"ext_example_db__warehouse\"}";
        assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
        response = "not json";
        assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
        response = "{\"catalog\":\"ext_example_db__warehouse\",\"created\":true,\"padding\":\"" + "x".repeat(70 * 1024) + "\"}";
        assertUnavailable(() -> client.createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
    }

    @Test
    void testRedirectsAreNotFollowed()
    {
        status = 307;
        location = "http://127.0.0.1:" + server.getAddress().getPort() + "/elsewhere/create";
        assertUnavailable(() -> client().createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
        assertThat(requests.get()).isEqualTo(1);
        assertThat(path.get()).isEqualTo("/trino/catalog-requests/v1/create");
    }

    @Test
    void testUnreachableEndpointIsUnavailable()
    {
        server.stop(0);
        assertUnavailable(() -> client().createCatalog(USER, CATALOG, CONNECTOR, PROPERTIES, false));
    }

    @Test
    void testEndpointValidation()
    {
        config.setDelegateAllowInsecureHttp(false);
        assertThatThrownBy(this::client)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Catalog request endpoint requires HTTPS unless insecure HTTP is explicitly enabled");

        config.setDelegateEndpoint(URI.create("https://control-plane.example.net/catalog-requests/v1"));
        assertThat(client()).isNotNull();

        for (String endpoint : new String[] {
                "https://user@control-plane.example.net/v1",
                "https://control-plane.example.net/v1?x=1",
                "https://control-plane.example.net/v1#fragment",
                "ftp://control-plane.example.net/v1",
                "/relative/v1",
        }) {
            config.setDelegateEndpoint(URI.create(endpoint));
            assertThatThrownBy(this::client).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testRequestToStringIsRedacted()
    {
        CreateRequest request = new CreateRequest("example_db.analyst", "ext_example_db__warehouse", "postgresql", PROPERTIES, true);
        assertThat(request.toString())
                .doesNotContain(SECRET)
                .doesNotContain("reader")
                .doesNotContain("db.example.net")
                .contains("ext_example_db__warehouse");
    }

    private CatalogRequestClient client()
    {
        return new CatalogRequestClient(config, httpClient);
    }

    private static void assertUnavailable(ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(CATALOG_STORE_ERROR.toErrorCode()))
                .hasMessage("Catalog requests are unavailable")
                .satisfies(e -> assertThat(e.getCause()).isNull())
                .satisfies(e -> assertThat(e.toString()).doesNotContain(SECRET));
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        requests.incrementAndGet();
        path.set(exchange.getRequestURI().getPath());
        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
        body.set(BODY_CODEC.fromJson(exchange.getRequestBody().readAllBytes()));
        if (location != null) {
            exchange.getResponseHeaders().set("Location", location);
        }
        byte[] bytes = response.getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
