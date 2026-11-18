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

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The storage half of the shared read path, on a connector that only ever plays the
 * worker: it is handed a table handle and a split and never looked the table up. A
 * fake S3 records what reaches it and denies every request.
 */
final class TestHoglakeSharedRemoteReadStorage
{
    private static final String KEY_ID = "synthetic-storage-key";
    private static final String REGION = "us-east-1";
    private static final String CATALOG = "catalog-a";
    private static final String PRINCIPAL = "example.alice";
    private static final String USER = "example.mapped_alice";
    private static final String ACCESS_KEY = "SYNTHETICACCESSKEYID";
    private static final String SECRET_KEY = "synthetic-secret-key-never-sent";
    private static final String SESSION_TOKEN = "synthetic-session-token";
    private static final HoglakeColumnHandle COLUMN = new HoglakeColumnHandle("v", 1, BIGINT, true);

    @TempDir
    Path directory;

    @Test
    void testAWorkerReadsOnlyWithTheVerifiedTokensSession()
            throws Exception
    {
        KeyPair key = keyPair();
        Path jwks = directory.resolve("worker.jwks");
        writeJwks(jwks, key, KEY_ID);
        List<S3Request> s3Requests = new CopyOnWriteArrayList<>();
        HttpServer s3 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s3.createContext("/", exchange -> deny(exchange, s3Requests));
        s3.start();
        Connector worker = new HoglakeConnectorFactory().create("synthetic-worker", properties(jwks, s3), new TestingConnectorContext());
        try {
            String jwt = token(key, KEY_ID, profileClaims(Instant.now(), "7"));
            ConnectorSession caller = session(USER, PRINCIPAL, jwt);
            HoglakeTableHandle team7 = table("team_7", true);

            // Refused before any storage request: no token, a token without the
            // session, a team the token does not hold, a table the server did not
            // call remote, another principal with the caller's token, a foreign key
            List<Runnable> refused = List.of(
                    () -> read(worker, session(USER, PRINCIPAL, null), team7),
                    () -> read(worker, session(USER, PRINCIPAL, token(key, KEY_ID, claims(CATALOG, PRINCIPAL, "route-a", Instant.now()))), team7),
                    () -> read(worker, caller, table("team_8", true)),
                    () -> read(worker, caller, table("team_7", false)),
                    () -> read(worker, caller, table("ns1", true)),
                    () -> read(worker, session("example.mapped_bob", "example.bob", jwt), team7),
                    () -> read(worker, session(USER, PRINCIPAL, token(keyPair(), KEY_ID, profileClaims(Instant.now(), "7"))), team7),
                    () -> read(worker, session(USER, PRINCIPAL, token(key, KEY_ID, profileClaims(Instant.now().minusSeconds(3600), "7"))), team7));
            for (int i = 0; i < refused.size(); i++) {
                Runnable attempt = refused.get(i);
                assertThatThrownBy(attempt::run)
                        .as("case %s", i)
                        .isInstanceOfSatisfying(TrinoException.class, failure ->
                                assertThat(failure.getErrorCode()).isIn(PERMISSION_DENIED.toErrorCode(), NOT_SUPPORTED.toErrorCode()));
            }
            assertThat(s3Requests).isEmpty();

            // The caller's read reaches S3 signed with the token's session, and the
            // denial is a permission error that names no token, credential or object
            assertThatThrownBy(() -> read(worker, caller, team7))
                    .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()))
                    .hasMessage("Shared Hoglake storage session does not allow this read")
                    .hasNoCause()
                    .satisfies(failure -> {
                        StringWriter trace = new StringWriter();
                        failure.printStackTrace(new PrintWriter(trace));
                        assertThat(trace.toString()).doesNotContain(jwt, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, "synthetic-bucket", "data.parquet");
                    });
            assertThat(s3Requests).isNotEmpty().allSatisfy(request -> {
                assertThat(request.path()).isEqualTo("/synthetic-bucket/stream/team_id=7/data.parquet");
                assertThat(request.authorization()).contains("Credential=" + ACCESS_KEY + "/").contains("/" + REGION + "/s3/aws4_request");
                assertThat(request.securityToken()).isEqualTo(SESSION_TOKEN);
                assertThat(request.method()).isIn("GET", "HEAD");
            });
        }
        finally {
            worker.shutdown();
            s3.stop(0);
        }
    }

    private static void read(Connector worker, ConnectorSession session, HoglakeTableHandle table)
    {
        ConnectorTransactionHandle transaction = worker.beginTransaction(READ_COMMITTED, true, true);
        HoglakeSplit split = new HoglakeSplit("s3://synthetic-bucket/stream/team_id=7/data.parquet", 4096, 10, Optional.empty(), 0);
        try (ConnectorPageSource source = worker.getPageSourceProvider().createPageSource(
                transaction, session, split, table, Optional.empty(), List.<ColumnHandle>of(COLUMN), DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
            while (!source.isFinished()) {
                source.getNextSourcePage();
            }
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static HoglakeTableHandle table(String schema, boolean remote)
    {
        return new HoglakeTableHandle(schema, "events", 9001, "uuid-events", List.of(COLUMN), TupleDomain.all(), List.of(), List.of(), false, OptionalLong.empty(), remote);
    }

    private static Map<String, String> properties(Path jwks, HttpServer s3)
    {
        HoglakeSharedConfig config = config(jwks, URI.create("http://127.0.0.1:9"));
        return ImmutableMap.<String, String>builder()
                .put("hoglake.multi-tenant", "true")
                .put("hoglake.uri", config.getUri().toString())
                .put("hoglake.shared.jwks-file", config.getJwksFile().getPath())
                .put("hoglake.shared.issuer", config.getIssuer())
                .put("hoglake.shared.backend-bindings", config.getBackendBindings())
                .put("hoglake.shared.remote-read", "true")
                .put("s3.endpoint", "http://127.0.0.1:" + s3.getAddress().getPort())
                .put("s3.path-style-access", "true")
                .put("s3.region", REGION)
                .buildOrThrow();
    }

    private static Map<String, Object> profileClaims(Instant now, String team)
    {
        Map<String, Object> claims = claims(CATALOG, PRINCIPAL, "route-a", now);
        claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of(team, "read")))));
        claims.put("profile", "storage-session");
        claims.put("version", 1);
        claims.put("purpose", "user-session");
        claims.put("primary_catalog", CATALOG);
        claims.put("remote_schemas", Map.of("team", List.of("read")));
        Map<String, Object> aws = new LinkedHashMap<>();
        aws.put("access_key_id", ACCESS_KEY);
        aws.put("secret_access_key", SECRET_KEY);
        aws.put("session_token", SESSION_TOKEN);
        aws.put("region", REGION);
        aws.put("expires_at", ((Number) claims.get("exp")).longValue() + 60);
        claims.put("aws", aws);
        return claims;
    }

    private static void deny(HttpExchange exchange, List<S3Request> requests)
            throws IOException
    {
        try (exchange) {
            requests.add(new S3Request(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    exchange.getRequestHeaders().getFirst("x-amz-security-token")));
            byte[] body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>".getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            if (exchange.getRequestMethod().equals("HEAD")) {
                exchange.sendResponseHeaders(403, -1);
                return;
            }
            exchange.sendResponseHeaders(403, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    private record S3Request(String method, String path, String authorization, String securityToken) {}
}
