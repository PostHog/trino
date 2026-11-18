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
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hoglake.HoglakeSharedSessionContext.StorageSession;
import io.trino.spi.TrinoException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
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
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.plugin.hoglake.HoglakeSharedSessionVerifier.SESSION_TOKEN;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared catalog's read path against a fake Hoglake that relays the remote team
 * schemas of one catalog. Every scan is empty, so storage is never reached: the
 * storage session is checked where the file system is made.
 */
final class TestHoglakeSharedRemoteRead
{
    private static final String KEY_ID = "synthetic-read-key";
    private static final String REGION = "us-east-1";
    private static final String CATALOG = "catalog-a";
    private static final String PRINCIPAL = "example.alice";
    private static final String USER = "example.mapped_alice";
    private static final String READS_REMOTE_ONLY = "Shared Hoglake reads tables of remote team schemas only";
    private static final String DISCOVERY_ONLY = "Shared Hoglake supports metadata discovery only";
    private static final String INVALID_AUTHORITY = "Shared Hoglake session authority is invalid or unavailable";
    private static final String CATALOG_PATH = "/v1/catalogs/" + CATALOG;

    @TempDir
    Path directory;

    @Test
    void testOnlyTheExactProfileIsAccepted()
    {
        KeyPair key = keyPair();
        Path jwks = directory.resolve("verifier.jwks");
        writeJwks(jwks, key, KEY_ID);
        HoglakeSharedConfig configuration = config(jwks, URI.create("https://metadata.example.test")).setRemoteRead(true);
        HoglakeSharedSessionVerifier verifier = new HoglakeSharedSessionVerifier(configuration, Optional.of(REGION), Clock.systemUTC(), System::nanoTime);

        String jwt = token(key, KEY_ID, profileClaims(Instant.now()));
        HoglakeSharedSessionContext context = verifier.verify(session(USER, PRINCIPAL, jwt));
        StorageSession storage = context.storage().orElseThrow();
        assertThat(storage.accessKeyId()).isEqualTo("synthetic-access-key");
        assertThat(storage.secretAccessKey()).isEqualTo("synthetic-secret-key");
        assertThat(storage.sessionToken()).isEqualTo("synthetic-session-token");
        assertThat(storage.region()).isEqualTo(REGION);
        assertThat(storage.toString()).isEqualTo("StorageSession{redacted}");
        assertThat(context.readsTeamSchema("team_7")).isTrue();
        for (String schema : List.of("team_8", "team_07", "team_", "team_7x", "TEAM_7", "ns1", "team_99999999999999999999")) {
            assertThat(context.readsTeamSchema(schema)).as(schema).isFalse();
        }

        // A plain metadata token still verifies, and reads no table
        HoglakeSharedSessionContext plain = verifier.verify(session(USER, PRINCIPAL, token(key, KEY_ID, claims(CATALOG, PRINCIPAL, "route-a", Instant.now()))));
        assertThat(plain.storage()).isEmpty();
        assertThat(plain.readsTeamSchema("team_7")).isFalse();

        List<Consumer<Map<String, Object>>> refused = List.of(
                claims -> claims.remove("profile"),
                claims -> claims.put("profile", "hogtower.aws-session"),
                claims -> claims.remove("version"),
                claims -> claims.put("version", 2),
                claims -> claims.put("version", "1"),
                claims -> claims.remove("purpose"),
                claims -> claims.put("purpose", "operator"),
                claims -> claims.remove("primary_catalog"),
                claims -> claims.put("primary_catalog", "catalog-b"),
                claims -> claims.remove("remote_schemas"),
                claims -> claims.put("remote_schemas", Map.of()),
                claims -> claims.put("remote_schemas", Map.of("team", List.of("read", "write"))),
                claims -> claims.put("remote_schemas", Map.of("team", List.of("write"))),
                claims -> claims.put("remote_schemas", Map.of("team_7", List.of("read"))),
                claims -> claims.put("remote_schemas", Map.of("team", List.of("read"), "posthog", List.of("read"))),
                claims -> claims.put("remote_schemas", Map.of("team", "read")),
                claims -> claims.remove("aws"),
                claims -> aws(claims).remove("access_key_id"),
                claims -> aws(claims).put("access_key_id", ""),
                claims -> aws(claims).remove("secret_access_key"),
                claims -> aws(claims).remove("session_token"),
                claims -> aws(claims).remove("region"),
                claims -> aws(claims).put("region", "eu-central-1"),
                claims -> aws(claims).remove("expires_at"),
                claims -> aws(claims).put("expires_at", "soon"),
                claims -> aws(claims).put("expires_at", ((Number) claims.get("exp")).longValue() - 1),
                claims -> aws(claims).put("role_arn", "arn:aws:iam::000000000000:role/synthetic"),
                claims -> claims.put("sid", "synthetic-session"),
                claims -> claims.put("generation", 1),
                claims -> claims.put("session_deadline", ((Number) claims.get("exp")).longValue() + 600),
                claims -> claims.put("grants", List.of()),
                claims -> claims.put("cat", new LinkedHashMap<>(Map.of(CATALOG, List.of("read", "write")))),
                claims -> claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of("7", "write"))))));
        for (int i = 0; i < refused.size(); i++) {
            Map<String, Object> claims = profileClaims(Instant.now());
            refused.get(i).accept(claims);
            String changed = token(key, KEY_ID, claims);
            assertThatThrownBy(() -> verifier.verify(session(USER, PRINCIPAL, changed)))
                    .as("case %s", i)
                    .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()))
                    .hasMessage(INVALID_AUTHORITY)
                    .hasNoCause();
        }

        // Without the read path every profile token is refused, the valid one included
        HoglakeSharedSessionVerifier discovery = new HoglakeSharedSessionVerifier(config(jwks, URI.create("https://metadata.example.test")));
        assertThatThrownBy(() -> discovery.verify(session(USER, PRINCIPAL, jwt))).hasMessage(INVALID_AUTHORITY);

        // The read path and its region come together or not at all
        assertThatThrownBy(() -> new HoglakeSharedSessionVerifier(configuration, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HoglakeSharedSessionVerifier(config(jwks, URI.create("https://metadata.example.test")), Optional.of(REGION)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFilesAreReadOnlyWithTheVerifiedSession()
    {
        KeyPair key = keyPair();
        Path jwks = directory.resolve("storage.jwks");
        writeJwks(jwks, key, KEY_ID);
        HoglakeSharedConfig configuration = config(jwks, URI.create("https://metadata.example.test")).setRemoteRead(true);
        HoglakeSharedSessionVerifier verifier = new HoglakeSharedSessionVerifier(configuration, Optional.of(REGION));
        List<ConnectorIdentity> storageIdentities = new CopyOnWriteArrayList<>();
        TrinoFileSystemFactory recording = new TrinoFileSystemFactory()
        {
            private final TrinoFileSystemFactory memory = new MemoryFileSystemFactory();

            @Override
            public TrinoFileSystem create(ConnectorIdentity identity)
            {
                storageIdentities.add(identity);
                return memory.create(identity);
            }
        };
        try (HoglakeSharedReadPath readPath = new HoglakeSharedReadPath(configuration, verifier, recording)) {
            TrinoFileSystemFactory fileSystems = readPath.sessionFileSystems();
            String jwt = token(key, KEY_ID, profileClaims(Instant.now()));

            fileSystems.create(session(USER, PRINCIPAL, jwt));
            assertThat(storageIdentities).hasSize(1);
            ConnectorIdentity storage = storageIdentities.getFirst();
            // The storage identity is the session and nothing of the caller
            assertThat(storage.getUser()).isEqualTo("hoglake-session");
            assertThat(storage.getPrincipal()).isEmpty();
            assertThat(storage.getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                    "internal$s3_aws_access_key", "synthetic-access-key",
                    "internal$s3_aws_secret_key", "synthetic-secret-key",
                    "internal$s3_aws_session_token", "synthetic-session-token"));

            // No session, no file system: nothing stands in for a missing or invalid token
            String plain = token(key, KEY_ID, claims(CATALOG, PRINCIPAL, "route-a", Instant.now()));
            String foreign = token(keyPair(), KEY_ID, profileClaims(Instant.now()));
            Map<String, Object> expired = profileClaims(Instant.now().minusSeconds(3600));
            for (String refused : new String[] {plain, foreign, token(key, KEY_ID, expired), null}) {
                assertThatThrownBy(() -> fileSystems.create(session(USER, PRINCIPAL, refused)))
                        .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode()));
            }
            // Another principal cannot read with the caller's token
            assertThatThrownBy(() -> fileSystems.create(session("example.mapped_bob", "example.bob", jwt)))
                    .hasMessage(INVALID_AUTHORITY);
            assertThat(storageIdentities).hasSize(1);
        }
    }

    @Test
    void testQueriesReadRemoteTeamTablesWithTheCallersToken()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(true)) {
            String jwt = fixture.profileToken();
            Session session = querySession(jwt);

            assertThat(runner.execute(session, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("information_schema", "ns1", "team_7");
            fixture.requests.clear();

            assertThat(runner.execute(session, "SELECT * FROM hoglake.team_7.events").getRowCount()).isZero();
            assertThat(runner.execute(session, "SELECT count(*) FROM hoglake.team_7.events WHERE v > 1").getOnlyValue()).isEqualTo(0L);
            assertThat(runner.execute(session, "SELECT a.v FROM hoglake.team_7.events a JOIN hoglake.team_7.events b ON a.v = b.v").getRowCount()).isZero();

            // Every request is the caller's own token on one of the three read routes
            assertThat(fixture.requests).isNotEmpty().allSatisfy(request -> {
                assertThat(request.method()).isEqualTo("GET");
                assertThat(request.authorization()).isEqualTo("Bearer " + jwt);
                assertThat(request.path()).isIn(
                        CATALOG_PATH + "/namespaces/team_7",
                        CATALOG_PATH + "/namespaces/team_7/tables/events",
                        CATALOG_PATH + "/namespaces/team_7/tables/events/scan");
            });
            // The first read of the table in a query names no snapshot; the scan uses its pin
            assertThat(fixture.requests.stream().filter(request -> request.path().endsWith("/scan")).collect(toImmutableList()))
                    .isNotEmpty()
                    .allSatisfy(request -> assertThat(request.target()).contains("snapshot=9001"));
            assertThat(fixture.requests.stream()
                    .filter(request -> request.path().endsWith("/tables/events"))
                    .map(Request::target)
                    .filter(target -> target.contains("snapshot=") && !target.contains("snapshot=9001"))
                    .collect(toImmutableList()))
                    .isEmpty();
        }
    }

    @Test
    void testEverythingButARemoteTeamReadIsRefused()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(true)) {
            String jwt = fixture.profileToken();
            Session session = querySession(jwt);

            // Refused from the token alone, with no request: a local schema, a team the
            // token does not hold, and a name that only looks like a team
            for (String sql : List.of(
                    "SELECT * FROM hoglake.ns1.t",
                    "SELECT count(*) FROM hoglake.ns1.t",
                    "SELECT * FROM hoglake.team_8.events",
                    "SELECT * FROM hoglake.team_07.events",
                    "SELECT * FROM hoglake.team_7 .events".replace(" .", "x."))) {
                int requests = fixture.requests.size();
                assertThatThrownBy(() -> runner.execute(session, sql))
                        .as(sql)
                        .isInstanceOf(QueryFailedException.class)
                        .hasMessageContaining(READS_REMOTE_ONLY);
                assertThat(fixture.requests).as(sql).hasSize(requests);
            }

            // A team schema the server does not call remote: asked once, then refused
            // before this catalog's snapshot or any table of it is requested
            fixture.requests.clear();
            assertThatThrownBy(() -> runner.execute(session, "SELECT * FROM hoglake.team_9.events"))
                    .isInstanceOf(QueryFailedException.class);
            assertThat(fixture.requests.stream().map(Request::target).collect(toImmutableList()))
                    .containsExactly(CATALOG_PATH + "/namespaces/team_9");

            // Writes and DDL on a readable table are refused, and send nothing that changes it
            for (String sql : List.of(
                    "INSERT INTO hoglake.team_7.events VALUES 1",
                    "DELETE FROM hoglake.team_7.events",
                    "UPDATE hoglake.team_7.events SET v = 2",
                    "ALTER TABLE hoglake.team_7.events ADD COLUMN w bigint",
                    "DROP TABLE hoglake.team_7.events",
                    "CREATE TABLE hoglake.team_7.copy AS SELECT 1 v",
                    "CREATE SCHEMA hoglake.mine")) {
                assertThatThrownBy(() -> runner.execute(session, sql))
                        .as(sql)
                        .isInstanceOf(QueryFailedException.class);
            }
            assertThat(fixture.requests).allSatisfy(request -> assertThat(request.method()).isEqualTo("GET"));

            // A token without the profile still discovers, and reads nothing
            String plain = fixture.plainToken();
            Session discovery = querySession(plain);
            assertThat(runner.execute(discovery, "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet()).contains("team_7");
            fixture.requests.clear();
            assertThatThrownBy(() -> runner.execute(discovery, "SELECT * FROM hoglake.team_7.events"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining(READS_REMOTE_ONLY);
            assertThat(fixture.requests).isEmpty();

            // No token at all
            assertThatThrownBy(() -> runner.execute(querySession(null), "SELECT * FROM hoglake.team_7.events"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining(INVALID_AUTHORITY);
        }
    }

    @Test
    void testTwoCallersReadOnlyWithTheirOwnTokens()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(true)) {
            String alice = fixture.profileToken();
            Map<String, Object> claims = profileClaims(Instant.now());
            claims.put("usr", "example.bob");
            claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of("8", "read")))));
            String bob = token(fixture.key, KEY_ID, claims);
            Session aliceSession = querySession(alice);
            Session bobSession = querySession("example.mapped_bob", "example.bob", bob);

            try (ExecutorService queries = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> results = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    results.add(queries.submit(() -> assertThat(runner.execute(aliceSession, "SELECT * FROM hoglake.team_7.events").getRowCount()).isZero()));
                    // Bob holds team 8 only: team_7 is refused from his token alone
                    results.add(queries.submit(() -> assertThatThrownBy(() -> runner.execute(bobSession, "SELECT * FROM hoglake.team_7.events"))
                            .isInstanceOf(QueryFailedException.class)
                            .hasMessageContaining(READS_REMOTE_ONLY)));
                }
                for (Future<?> result : results) {
                    result.get();
                }
            }
            assertThat(fixture.requests).isNotEmpty().allSatisfy(request -> assertThat(request.authorization()).isEqualTo("Bearer " + alice));

            // Bob's token with Alice's principal, and the reverse, read nothing
            assertThatThrownBy(() -> runner.execute(querySession(bob), "SELECT * FROM hoglake.team_8.events")).hasMessageContaining(INVALID_AUTHORITY);
            assertThatThrownBy(() -> runner.execute(querySession("example.mapped_bob", "example.bob", alice), "SELECT * FROM hoglake.team_7.events")).hasMessageContaining(INVALID_AUTHORITY);
        }
    }

    @Test
    void testRefusalsNameNoTokenOrCredential()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(true)) {
            String jwt = fixture.profileToken();
            Session session = querySession(jwt);
            for (String sql : List.of(
                    "SELECT * FROM hoglake.ns1.t",
                    "SELECT * FROM hoglake.team_8.events",
                    "SELECT * FROM hoglake.team_9.events",
                    "SELECT * FROM hoglake.team_7.missing",
                    "INSERT INTO hoglake.team_7.events VALUES 1")) {
                assertThatThrownBy(() -> runner.execute(session, sql))
                        .as(sql)
                        .isInstanceOf(QueryFailedException.class)
                        .satisfies(failure -> {
                            StringWriter trace = new StringWriter();
                            failure.printStackTrace(new PrintWriter(trace));
                            assertThat(trace.toString())
                                    .doesNotContain(jwt, "Bearer ", "synthetic-access-key", "synthetic-secret-key", "synthetic-session-token");
                        });
            }
        }
    }

    @Test
    void testWithoutTheReadPathAProfileTokenIsRefusedAndNothingIsRead()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(false)) {
            assertThatThrownBy(() -> runner.execute(querySession(fixture.profileToken()), "SHOW SCHEMAS FROM hoglake"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining(INVALID_AUTHORITY);
            assertThatThrownBy(() -> runner.execute(querySession(fixture.plainToken()), "SELECT * FROM hoglake.team_7.events"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining(DISCOVERY_ONLY);
            assertThat(fixture.requests).isEmpty();
        }
    }

    @Test
    void testBootRefusesAnyOtherSourceOfStorageAuthority()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            Map<String, String> valid = fixture.properties(true);
            new HoglakeConnectorFactory().create("hoglake", valid, new TestingConnectorContext()).shutdown();

            for (Map.Entry<String, String> extra : ImmutableMap.<String, String>builder()
                    .put("s3.aws-access-key", "synthetic")
                    .put("s3.aws-secret-key", "synthetic")
                    .put("s3.iam-role", "arn:aws:iam::000000000000:role/synthetic")
                    .put("s3.security-mapping.enabled", "true")
                    .put("fs.cache.enabled", "true")
                    .put("fs.hadoop.enabled", "true")
                    .put("fs.native-gcs.enabled", "true")
                    .put("hoglake.s3.access-key", "synthetic")
                    .put("hoglake.parquet-footer-cache.max-size", "64MB")
                    .put("hoglake.catalog", "lake")
                    .put("hoglake.auth.token", "synthetic")
                    .buildOrThrow().entrySet()) {
                Map<String, String> properties = ImmutableMap.<String, String>builder().putAll(valid).put(extra.getKey(), extra.getValue()).buildOrThrow();
                assertThatThrownBy(() -> new HoglakeConnectorFactory().create("hoglake", properties, new TestingConnectorContext()))
                        .as(extra.getKey())
                        .isInstanceOf(RuntimeException.class);
            }

            Map<String, String> noRegion = new LinkedHashMap<>(valid);
            noRegion.remove("s3.region");
            assertThatThrownBy(() -> new HoglakeConnectorFactory().create("hoglake", noRegion, new TestingConnectorContext()))
                    .hasMessageContaining("s3.region is required");

            Map<String, String> loose = new LinkedHashMap<>(valid);
            loose.put("hoglake.shared.remote-read", "TRUE");
            assertThatThrownBy(() -> new HoglakeConnectorFactory().create("hoglake", loose, new TestingConnectorContext()))
                    .hasMessage("hoglake.shared.remote-read must be exactly true or false");

            // The per-catalog connector does not know the property
            Map<String, String> perCatalog = ImmutableMap.of(
                    "hoglake.uri", valid.get("hoglake.uri"),
                    "hoglake.catalog", "lake",
                    "hoglake.shared.remote-read", "true");
            assertThatThrownBy(() -> new HoglakeConnectorFactory().create("hoglake", perCatalog, new TestingConnectorContext()))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static Map<String, Object> profileClaims(Instant now)
    {
        Map<String, Object> claims = claims(CATALOG, PRINCIPAL, "route-a", now);
        claims.put("profile", "storage-session");
        claims.put("version", 1);
        claims.put("purpose", "user-session");
        claims.put("primary_catalog", CATALOG);
        claims.put("remote_schemas", Map.of("team", List.of("read")));
        Map<String, Object> aws = new LinkedHashMap<>();
        aws.put("access_key_id", "synthetic-access-key");
        aws.put("secret_access_key", "synthetic-secret-key");
        aws.put("session_token", "synthetic-session-token");
        aws.put("region", REGION);
        aws.put("expires_at", ((Number) claims.get("exp")).longValue() + 60);
        claims.put("aws", aws);
        return claims;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aws(Map<String, Object> claims)
    {
        return (Map<String, Object>) claims.get("aws");
    }

    private static Session querySession(String jwt)
    {
        return querySession(USER, PRINCIPAL, jwt);
    }

    private static Session querySession(String user, String principal, String jwt)
    {
        Identity.Builder builder = Identity.forUser(user).withPrincipal(new BasicPrincipal(principal));
        if (jwt != null) {
            builder.withExtraCredentials(Map.of(SESSION_TOKEN, jwt));
        }
        Identity identity = builder.build();
        return testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema("team_7")
                .setIdentity(identity)
                .setOriginalIdentity(identity)
                .build();
    }

    private record Request(String method, String target, String authorization)
    {
        public String path()
        {
            int query = target.indexOf('?');
            return query < 0 ? target : target.substring(0, query);
        }
    }

    private static final class Fixture
            implements AutoCloseable
    {
        private final KeyPair key = keyPair();
        private final Path jwks;
        private final HttpServer server;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private final AtomicReference<String> unexpected = new AtomicReference<>();

        public Fixture(Path directory)
                throws IOException
        {
            jwks = directory.resolve("synthetic-read-public.jwks");
            writeJwks(jwks, key, KEY_ID);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        }

        public String profileToken()
        {
            Map<String, Object> claims = profileClaims(Instant.now());
            // The token holds teams 7 and 9; the server calls only team_7 remote
            claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of("7", "read", "9", "read")))));
            return token(key, KEY_ID, claims);
        }

        public String plainToken()
        {
            return token(key, KEY_ID, claims(CATALOG, PRINCIPAL, "route-a", Instant.now()));
        }

        public Map<String, String> properties(boolean remoteRead)
        {
            HoglakeSharedConfig config = config(jwks, URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hoglake.multi-tenant", "true")
                    .put("hoglake.uri", config.getUri().toString())
                    .put("hoglake.shared.jwks-file", config.getJwksFile().getPath())
                    .put("hoglake.shared.issuer", config.getIssuer())
                    .put("hoglake.shared.backend-bindings", config.getBackendBindings());
            if (remoteRead) {
                properties.put("hoglake.shared.remote-read", "true")
                        // Storage is never reached: every scan here is empty
                        .put("s3.endpoint", "http://127.0.0.1:9")
                        .put("s3.region", REGION);
            }
            return properties.buildOrThrow();
        }

        public StandaloneQueryRunner queryRunner(boolean remoteRead)
        {
            StandaloneQueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("team_7").build());
            try {
                runner.installPlugin(new HoglakePlugin());
                runner.createCatalog("hoglake", "hoglake", properties(remoteRead));
                return runner;
            }
            catch (RuntimeException | Error exception) {
                runner.close();
                throw exception;
            }
        }

        private void handle(HttpExchange exchange)
        {
            try (exchange) {
                String target = exchange.getRequestURI().toASCIIString();
                Request request = new Request(exchange.getRequestMethod(), target, exchange.getRequestHeaders().getFirst("Authorization"));
                requests.add(request);
                String body = respond(request);
                if (body == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] bytes = body.getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
            catch (IOException _) {
                // A cancelled query can close its connection before the response.
            }
        }

        private String respond(Request request)
        {
            String namespaces = CATALOG_PATH + "/namespaces";
            String events = namespaces + "/team_7/tables/events";
            if (!request.method().equals("GET")) {
                unexpected.compareAndSet(null, request.method() + " " + request.target());
                return null;
            }
            String path = request.path();
            if (path.equals(namespaces)) {
                return "[{\"name\":\"ns1\",\"namespace_id\":1},{\"name\":\"team_7\",\"remote\":true}]";
            }
            if (path.equals(namespaces + "/team_7")) {
                return "{\"name\":\"team_7\",\"remote\":true}";
            }
            if (path.equals(namespaces + "/team_9")) {
                // A local namespace with a team's name
                return "{\"name\":\"team_9\",\"namespace_id\":9}";
            }
            if (path.equals(events)) {
                return "{\"name\":\"events\",\"namespace\":\"team_7\",\"table_uuid\":\"uuid-events\",\"columns\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"field_id\":1,\"ordinal\":0}],\"read_snapshot_id\":9001,\"properties\":{},\"remote\":true}";
            }
            if (path.equals(events + "/scan")) {
                return "[]";
            }
            if (path.equals(namespaces + "/ns1/tables") || path.equals(namespaces + "/team_7/tables")) {
                return "[]";
            }
            if (path.startsWith(namespaces + "/team_7/tables/") && !path.substring((namespaces + "/team_7/tables/").length()).contains("/")) {
                // A table the team schema does not have
                return null;
            }
            // The catalog's own snapshot, a local table or another team: never asked for
            unexpected.compareAndSet(null, request.method() + " " + request.target());
            return null;
        }

        @Override
        public void close()
        {
            server.stop(0);
            executor.close();
            assertThat(unexpected.get()).as("A request outside the read routes reached the server").isNull();
        }
    }
}
