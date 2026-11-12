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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.session;
import static io.trino.spi.StandardErrorCode.EXCEEDED_SCAN_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The read-only schema posthog against a fake Hoglake server that serves an org
 * catalog (with its own namespace named posthog, which must never be reached), the
 * org's events mount and the token endpoint, and records every request.
 *
 * <p>The token endpoint answers both grants: client credentials for the org
 * catalog, and the token exchange for one login. The mount serves only a token
 * that was issued for a login, and answers with that login's tables and files, so
 * a request that carries another login's token is visible in the result.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakePosthogSchema
{
    private static final String ORG = "0b5f0c2e-1111-4222-8333-444455556666";
    private static final String MOUNT = ORG + "-events";
    private static final String ORG_PATH = "/v1/catalogs/" + ORG;
    private static final String MOUNT_PATH = "/v1/catalogs/" + MOUNT;
    private static final String SHADOW_PATH = ORG_PATH + "/namespaces/posthog";
    // A catalog that refuses every request, for a client that is not the mount's
    private static final String REFUSING_CATALOG = "refusing";
    // Placed in every refusal body: a user-visible message must never carry it
    private static final String BODY_MARKER = "detail-marker-6f1d";

    // Logins, named as the control plane names them: <database>.<user>
    private static final String ALICE = "wh1.alice";
    private static final String BOB = "wh1.bob";
    private static final String CAROL = "wh1.carol";
    // The token endpoint refuses this login with invalid_grant
    private static final String REFUSED_LOGIN = "wh1.refused";
    // What only some logins see in the mount: one more table, and other files
    private static final Map<String, String> OWN_TABLES = ImmutableMap.of(BOB, "bob_only", CAROL, "carol_only");
    private static final Map<String, long[]> OWN_FILES = ImmutableMap.of(BOB, new long[] {10, 20}, CAROL, new long[] {100});

    private static final String ORG_TOKEN = "Bearer token-for-";
    private static final String LOGIN_TOKEN = "Bearer login-token-";
    // A per-login token that names the org catalog, not the mount
    private static final String ORG_SCOPED_LOGIN_TOKEN = "Bearer orgscoped-token-";
    private static final String EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange";

    private static final String LOGIN_REFUSED = "Access Denied: Cannot read the posthog schema with this login";
    private static final String NOT_THE_LOGIN = "Access Denied: The posthog schema can be read only as the login that authenticated";
    private static final String NOT_OFFERED = "The posthog schema is not available: the token service does not issue per-login tokens";
    private static final String BUSY = "The token service for the posthog schema is busy or unavailable; retry later";
    private static final String UNREACHABLE = "The token service for the posthog schema cannot be reached; retry later";

    private enum Mount
    {
        SERVED,
        MISSING,
        TWO_NAMESPACES,
        REFUSING,
    }

    /**
     * What the token endpoint does with a token exchange. Each one belongs to a
     * catalog of its own, named after it, so a refusal that one catalog remembers
     * cannot change another test.
     */
    private enum Exchange
    {
        ISSUING,
        ORG_SCOPED,
        NOT_OFFERED,
        UNAUTHORIZED,
        BAD_CLIENT,
        BAD_REQUEST,
        BUSY,
        DROPPED,
        STALLED,
        NOT_AN_EXCHANGE;

        public String clientId()
        {
            return "client-" + name().toLowerCase(ENGLISH);
        }

        public String catalog()
        {
            return "org_" + name().toLowerCase(ENGLISH);
        }

        public static Exchange of(String clientId)
        {
            for (Exchange exchange : values()) {
                if (exchange.clientId().equals(clientId)) {
                    return exchange;
                }
            }
            return ISSUING;
        }
    }

    private record Request(String method, String path, String query, String authorization) {}

    private record TokenRequest(String clientId, String grantType, String subject, String subjectTokenType, String audience, String scope, Set<String> fields)
    {
        public boolean isExchange()
        {
            return EXCHANGE_GRANT.equals(grantType);
        }
    }

    // Cleared by tests as they go
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final List<TokenRequest> tokenRequests = new CopyOnWriteArrayList<>();
    // Every request of one test, and every token request of the class
    private final List<Request> testRequests = new CopyOnWriteArrayList<>();
    private final List<TokenRequest> allTokenRequests = new CopyOnWriteArrayList<>();
    private final Set<String> rejectedTokens = ConcurrentHashMap.newKeySet();
    private final AtomicInteger issued = new AtomicInteger();
    private volatile Mount mount = Mount.SERVED;
    private volatile int refusalStatus;
    private volatile String refusalCode;
    private volatile long mountHead = 42;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private ExecutorService callers;
    private HttpClient httpClient;
    private String uri;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = bindInPortRange();
        server.createContext("/", this::handle);
        // One thread per exchange, so a stalled token answer does not hold up the server
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.start();
        uri = "http://127.0.0.1:" + server.getAddress().getPort();
        callers = Executors.newCachedThreadPool();
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        queryRunner = new StandaloneQueryRunner(login(ALICE));
        queryRunner.installPlugin(new HoglakePlugin());
        queryRunner.createCatalog("org_on", "hoglake", catalogProperties(Optional.of("client-on"), true));
        queryRunner.createCatalog("org_off", "hoglake", catalogProperties(Optional.of("client-off"), false));
        queryRunner.createCatalog("org_noauth", "hoglake", catalogProperties(Optional.empty(), true));
        for (Exchange exchange : Exchange.values()) {
            if (exchange == Exchange.ISSUING) {
                continue;
            }
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties(Optional.of(exchange.clientId()), true));
            if (exchange == Exchange.STALLED) {
                // The token endpoint never answers this catalog's exchange in time
                properties.put("hoglake.client.request-timeout", "1500ms");
            }
            queryRunner.createCatalog(exchange.catalog(), "hoglake", properties.buildOrThrow());
        }
    }

    /**
     * A session of a login that authenticated as itself: the principal names the user.
     */
    private static Session login(String login)
    {
        return sqlSession(Identity.forUser(login).withPrincipal(new BasicPrincipal(login)).build());
    }

    private static Session sqlSession(Identity identity)
    {
        return testSessionBuilder().setCatalog("org_on").setSchema("analytics").setIdentity(identity).build();
    }

    private static ConnectorSession connectorLogin(String login)
    {
        return session(ConnectorIdentity.forUser(login).withPrincipal(new BasicPrincipal(login)).build());
    }

    private Map<String, String> catalogProperties(Optional<String> clientId, boolean posthogSchema)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", ORG)
                // Storage is never reached: every read here is answered from catalog counts
                .put("s3.endpoint", "http://127.0.0.1:9")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "synthetic-test")
                .put("s3.aws-secret-key", "synthetic-test-password");
        clientId.ifPresent(id -> properties
                .put("hoglake.auth.token-uri", uri + "/oauth/token")
                .put("hoglake.auth.client-id", id)
                .put("hoglake.auth.client-secret", "synthetic-test-secret"));
        if (posthogSchema) {
            properties.put("hoglake.posthog-schema.enabled", "true");
        }
        return properties.buildOrThrow();
    }

    /**
     * The posthog schema as the connector builds it, for tests that call the
     * connector's classes directly.
     */
    private HoglakePosthogSchema posthogSchema()
    {
        return new HoglakePosthogSchema(ORG, HoglakeClient.forMount(uri, MOUNT), new HoglakeLoginTokenSource.TokenExchange(
                URI.create(uri + "/oauth/token"),
                "client-direct",
                "synthetic-test-secret",
                "hoglake",
                HoglakePosthogSchema.LOGIN_SUBJECT_TOKEN_TYPE,
                Optional.of(HoglakePosthogSchema.mountScope(MOUNT)),
                httpClient));
    }

    /**
     * The mount's client with a token the fake mount serves, for tests of the client alone.
     */
    private static HoglakeClient mountClient(String baseUri)
    {
        return HoglakeClient.forMount(baseUri, MOUNT).withLogin(fixedToken(loginToken(ALICE, 0)));
    }

    private static HoglakeTokenSource fixedToken(String token)
    {
        return new HoglakeTokenSource()
        {
            @Override
            public Optional<String> token(Duration timeout)
            {
                return Optional.of(token);
            }

            @Override
            public void invalidate(String token) {}
        };
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        if (httpClient != null) {
            httpClient.shutdownNow();
        }
        if (callers != null) {
            callers.shutdownNow();
        }
        if (server != null) {
            server.stop(0);
        }
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
        }
    }

    @BeforeEach
    void reset()
    {
        mount = Mount.SERVED;
        mountHead = 42;
        refusalCode = null;
        rejectedTokens.clear();
        requests.clear();
        tokenRequests.clear();
        testRequests.clear();
    }

    /**
     * Checked after every test, whatever it did: the mount is asked only with a
     * token issued for a login, the org catalog only with its own token, and the
     * catalog without the posthog schema never asks for a per-login token.
     */
    @AfterEach
    void verifyEveryTokenStayedOnItsRoute()
    {
        assertThat(testRequests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .allSatisfy(request -> assertThat(request.authorization()).satisfiesAnyOf(
                        authorization -> assertThat(authorization).startsWith(LOGIN_TOKEN),
                        authorization -> assertThat(authorization).startsWith(ORG_SCOPED_LOGIN_TOKEN)));
        assertThat(testRequests)
                .filteredOn(request -> request.path().startsWith(ORG_PATH) && !request.path().startsWith(MOUNT_PATH))
                .allSatisfy(request -> assertThat(request.authorization()).satisfiesAnyOf(
                        // A catalog or a client without credentials
                        authorization -> assertThat(authorization).isNull(),
                        authorization -> assertThat(authorization).startsWith(ORG_TOKEN)));
        assertThat(allTokenRequests).noneMatch(request -> request.isExchange() && request.clientId().equals("client-off"));
        // The org catalog's token is minted once for each catalog: nothing here discards it
        Map<String, Integer> mints = new HashMap<>();
        allTokenRequests.stream()
                .filter(request -> !request.isExchange())
                .forEach(request -> mints.merge(request.clientId(), 1, Integer::sum));
        assertThat(mints.values()).allMatch(count -> count == 1);
        // Every exchange names the mount and the login's token type, and nothing more
        assertThat(allTokenRequests).filteredOn(TokenRequest::isExchange).allSatisfy(request -> {
            assertThat(request.subjectTokenType()).isEqualTo("urn:posthog:params:oauth:token-type:trino-login");
            assertThat(request.audience()).isEqualTo("hoglake");
            assertThat(request.scope()).isEqualTo("hoglake:" + MOUNT + ":read");
            assertThat(request.fields()).containsExactlyInAnyOrder("grant_type", "subject_token", "subject_token_type", "audience", "scope");
        });
    }

    @Test
    void testPropertyUnsetChangesNothing()
    {
        // The org's own namespace posthog is an ordinary schema, read from the org catalog
        assertThat(column("SHOW SCHEMAS FROM org_off")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(column("SHOW TABLES FROM org_off.posthog")).containsExactlyInAnyOrder("events", "shadow");
        assertThat(column("DESCRIBE org_off.posthog.events")).containsExactly("org_column");
        assertThat(scalar("SELECT count(*) FROM org_off.posthog.events")).isEqualTo(5L);
        // Writes go where they always went: the org catalog decides (here, by capability)
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("DELETE FROM org_off.posthog.events"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageNotContaining("read-only");
        // The same for a session that could not read the posthog schema: nothing asks who it is
        Session withoutPrincipal = sqlSession(Identity.ofUser("someone"));
        assertThat(column(withoutPrincipal, "SHOW TABLES FROM org_off.posthog")).containsExactlyInAnyOrder("events", "shadow");
        assertThat(scalar(withoutPrincipal, "SELECT count(*) FROM org_off.posthog.events")).isEqualTo(5L);
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH));
        assertThat(requests).anyMatch(request -> request.path().startsWith(SHADOW_PATH));
        // No per-login token is asked for, and every request carries the catalog's own token
        assertThat(tokenRequests).noneMatch(TokenRequest::isExchange);
        // Nothing of the schema is built: there is no object that could ask for one
        HoglakeConfig config = new HoglakeConfig()
                .setUri(uri)
                .setCatalog(ORG)
                .setAuthTokenUri(uri + "/oauth/token")
                .setAuthClientId("client-off")
                .setAuthClientSecret("synthetic-test-secret");
        assertThat(HoglakeModule.createPosthogSchema(config)).isEmpty();
        Optional<HoglakePosthogSchema> enabled = HoglakeModule.createPosthogSchema(config.setPosthogSchemaEnabled(true));
        assertThat(enabled).isPresent();
        enabled.orElseThrow().close();
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith("/v1/"))
                .allMatch(request -> "Bearer token-for-client-off".equals(request.authorization()));
    }

    @Test
    void testSchemaListedAndServedFromTheMountWithTheLoginsToken()
    {
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactly("events");
        assertThat(column("DESCRIBE org_on.posthog.events")).containsExactly("team_id", "event");
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactlyInAnyOrder("analytics.orders", "posthog.events");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog'"))
                .containsExactlyInAnyOrder("team_id", "event");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog' AND table_name = 'events'"))
                .containsExactlyInAnyOrder("team_id", "event");

        assertThat(requests).anyMatch(request -> request.path().equals(MOUNT_PATH + "/namespaces/ingest/tables/events"));
        // The org catalog's requests carry the catalog's token, as before
        Set<String> orgTokens = requests.stream()
                .filter(request -> request.path().startsWith(ORG_PATH) && !request.path().startsWith(MOUNT_PATH))
                .map(Request::authorization)
                .collect(toImmutableSet());
        assertThat(orgTokens).containsExactly("Bearer token-for-client-on");
        // The mount's requests carry one token, issued for the session's login
        Set<String> mountTokens = requests.stream()
                .filter(request -> request.path().startsWith(MOUNT_PATH))
                .map(Request::authorization)
                .collect(toImmutableSet());
        assertThat(mountTokens).singleElement().satisfies(token -> assertThat(loginOf(token)).contains(ALICE));
        assertThat(tokenRequests).filteredOn(TokenRequest::isExchange).hasSizeLessThanOrEqualTo(1).allMatch(request -> request.subject().equals(ALICE));
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testScanIsPlannedThroughTheMountScanRoute()
    {
        assertThat(scalar("SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        assertThat(requests)
                .filteredOn(request -> request.path().endsWith("/scan"))
                .singleElement()
                .satisfies(request -> {
                    assertThat(request.path()).isEqualTo(MOUNT_PATH + "/namespaces/ingest/tables/events/scan");
                    assertThat(request.query()).startsWith("snapshot=42");
                    assertThat(loginOf(request.authorization())).contains(ALICE);
                });
        // The org catalog's own table plans through its own scan route, as before
        requests.clear();
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(requests)
                .filteredOn(request -> request.path().endsWith("/scan"))
                .singleElement()
                .satisfies(request -> assertThat(request.path()).isEqualTo(ORG_PATH + "/namespaces/analytics/tables/orders/scan"));
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH + "/namespaces"));
    }

    @Test
    void testEveryWriteStatementIsRefusedWithoutAWriteRequest()
    {
        List<String> readOnlyRefusals = ImmutableList.of(
                "INSERT INTO org_on.posthog.events VALUES (1, 'x')",
                "CREATE TABLE org_on.posthog.created (x bigint)",
                "CREATE TABLE org_on.posthog.created AS SELECT 1 x",
                "CREATE OR REPLACE TABLE org_on.posthog.events AS SELECT 1 x",
                "DELETE FROM org_on.posthog.events WHERE team_id = 1",
                "DELETE FROM org_on.posthog.events",
                "UPDATE org_on.posthog.events SET event = 'x'",
                "MERGE INTO org_on.posthog.events t USING (VALUES 1) s(x) ON t.team_id = s.x WHEN MATCHED THEN DELETE",
                "MERGE INTO org_on.posthog.events t USING (VALUES 1) s(x) ON t.team_id = s.x WHEN NOT MATCHED THEN INSERT VALUES (s.x, 'x')",
                "DROP TABLE org_on.posthog.events",
                "TRUNCATE TABLE org_on.posthog.events",
                "ALTER TABLE org_on.posthog.events RENAME TO org_on.posthog.renamed",
                "ALTER TABLE org_on.analytics.orders RENAME TO org_on.posthog.orders",
                "ALTER TABLE org_on.posthog.events ADD COLUMN added bigint",
                "ALTER TABLE org_on.posthog.events DROP COLUMN event",
                "ALTER TABLE org_on.posthog.events RENAME COLUMN event TO renamed",
                "ALTER TABLE org_on.posthog.events ALTER COLUMN team_id SET DATA TYPE decimal(38, 0)",
                "COMMENT ON TABLE org_on.posthog.events IS 'x'",
                "COMMENT ON COLUMN org_on.posthog.events.event IS 'x'",
                "ALTER TABLE org_on.posthog.events SET PROPERTIES extra_properties = MAP(ARRAY['a'], ARRAY['b'])");
        for (String statement : readOnlyRefusals) {
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(statement))
                    .as(statement)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("The posthog schema is read-only");
        }
        // Refused by the engine or by connector defaults, for every schema
        List<String> otherRefusals = ImmutableList.of(
                "CREATE SCHEMA org_on.posthog",
                "DROP SCHEMA org_on.posthog",
                "ALTER SCHEMA org_on.posthog RENAME TO renamed",
                "CREATE VIEW org_on.posthog.v AS SELECT 1 x",
                "CREATE MATERIALIZED VIEW org_on.posthog.mv AS SELECT 1 x",
                "ALTER TABLE org_on.posthog.events EXECUTE optimize",
                "ANALYZE org_on.posthog.events",
                "ALTER TABLE org_on.posthog.events SET AUTHORIZATION someone",
                "ALTER TABLE org_on.posthog.events ALTER COLUMN event DROP NOT NULL");
        for (String statement : otherRefusals) {
            assertThatThrownBy(() -> queryRunner.execute(statement)).as(statement).isInstanceOf(RuntimeException.class);
        }
        // With no mount, CREATE SCHEMA reaches the connector, which refuses the name
        mount = Mount.MISSING;
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("CREATE SCHEMA org_on.posthog"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("The posthog schema is read-only");
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testOrgNamespacePosthogIsNeverReachedOrMerged()
    {
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactly("events");
        assertThat(column("DESCRIBE org_on.posthog.events")).containsExactly("team_id", "event");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasMessageContaining("Table 'org_on.posthog.shadow' does not exist");
        assertThat(column("SELECT table_name FROM org_on.information_schema.tables WHERE table_schema = 'posthog'")).containsExactly("events");

        // No mount: the user sees no schema posthog, never the org's
        mount = Mount.MISSING;
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.events"))
                .hasErrorCode(SCHEMA_NOT_FOUND);

        // Mount down: a typed error, never the org's
        refuseMount(503, "mount_host_unavailable");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SHOW TABLES FROM org_on.posthog"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testMissingMountLeavesTheCatalogWorking()
    {
        mount = Mount.MISSING;
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema");
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactly("analytics.orders");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema <> 'information_schema'"))
                .containsExactly("amount");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SHOW TABLES FROM org_on.posthog"))
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testUnavailableMountLeavesTheCatalogWorking()
    {
        refuseMount(503, "mount_host_unavailable");
        // Listed, since whether it exists is unknown, and other schemas answer as before
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactly("analytics.orders");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema <> 'information_schema'"))
                .containsExactly("amount");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE)
                .hasMessage("The events host serving the posthog schema is unavailable; retry later");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog'"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);

        // The mount answers its catalog but fails planning
        mount = Mount.SERVED;
        refusalCodeOnScanOnly("mount_scan_busy");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE)
                .hasMessage("The Hoglake server is busy planning scans of the posthog schema; retry later");
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testMountRefusingALoginsTokenReplacesOnlyThatToken()
    {
        String dana = "wh1.dana";
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        assertThat(scalar(login(dana), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        String aliceToken = lastMountToken(ALICE);
        String danaToken = lastMountToken(dana);
        requests.clear();
        tokenRequests.clear();

        // The mount stops accepting one login's token, as after a revocation
        rejectedTokens.add(danaToken);
        assertThat(scalar(login(dana), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        // One new token for that login, and one retry of the refused request
        assertThat(tokenRequests).singleElement().satisfies(request -> {
            assertThat(request.isExchange()).isTrue();
            assertThat(request.subject()).isEqualTo(dana);
        });
        List<String> danaTokens = requests.stream()
                .filter(request -> request.path().startsWith(MOUNT_PATH))
                .map(Request::authorization)
                .toList();
        String replacement = danaTokens.getLast();
        assertThat(replacement).isNotEqualTo(danaToken);
        assertThat(loginOf(replacement)).contains(dana);
        // The refused token was sent once, then only its replacement
        assertThat(danaTokens.getFirst()).isEqualTo(danaToken);
        assertThat(danaTokens.subList(1, danaTokens.size())).allMatch(replacement::equals);

        // The other login and the org catalog keep their tokens: nothing was minted for them
        assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        assertThat(lastMountToken(ALICE)).isEqualTo(aliceToken);
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(tokenRequests).hasSize(1);
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(ORG_PATH) && !request.path().startsWith(MOUNT_PATH))
                .allMatch(request -> "Bearer token-for-client-on".equals(request.authorization()));

        // A mount that refuses the new token too: reported after one retry, with no third request
        requests.clear();
        tokenRequests.clear();
        refuseMount(401, "invalid_token");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(dana), "SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE)
                .hasMessage("hoglake request failed: a request for the posthog schema -> HTTP 401 (invalid_token)");
        assertThat(requests).filteredOn(request -> request.path().startsWith(MOUNT_PATH)).hasSize(2);
        assertThat(tokenRequests).singleElement().satisfies(request -> assertThat(request.subject()).isEqualTo(dana));
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .allMatch(request -> loginOf(request.authorization()).equals(Optional.of(dana)));
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(tokenRequests).hasSize(1);
    }

    @Test
    void testEachLoginSeesItsOwnTablesAndFiles()
            throws Exception
    {
        // Interleaved: each statement is answered for the login of its own session
        for (int round = 0; round < 2; round++) {
            assertThat(column(login(ALICE), "SHOW TABLES FROM org_on.posthog")).containsExactly("events");
            assertThat(column(login(BOB), "SHOW TABLES FROM org_on.posthog")).containsExactlyInAnyOrder("events", "bob_only");
            assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
            assertThat(scalar(login(BOB), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(30L);
            assertThat(column(login(CAROL), "SHOW TABLES FROM org_on.posthog")).containsExactlyInAnyOrder("events", "carol_only");
            assertThat(scalar(login(CAROL), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(100L);
            assertThat(scalar(login(BOB), "SELECT count(*) FROM org_on.posthog.bob_only")).isEqualTo(30L);
            assertThat(column(login(BOB), "SELECT table_name FROM org_on.information_schema.tables WHERE table_schema = 'posthog'")).containsExactlyInAnyOrder("events", "bob_only");
            assertThat(column(login(ALICE), "SELECT table_name FROM org_on.information_schema.tables WHERE table_schema = 'posthog'")).containsExactly("events");
            // Another login's table does not exist for this one
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(ALICE), "SELECT count(*) FROM org_on.posthog.bob_only"))
                    .hasMessageContaining("Table 'org_on.posthog.bob_only' does not exist");
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(CAROL), "DESCRIBE org_on.posthog.bob_only"))
                    .hasMessageContaining("does not exist");
        }
        // Each login planned its own files
        for (String login : List.of(ALICE, BOB, CAROL)) {
            assertThat(requests)
                    .filteredOn(request -> request.path().endsWith("/scan") && loginOf(request.authorization()).equals(Optional.of(login)))
                    .isNotEmpty();
        }

        // Concurrent: many statements of the three logins at once
        Map<String, Long> counts = ImmutableMap.of(ALICE, 7L, BOB, 30L, CAROL, 100L);
        List<Future<Boolean>> results = new ArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        for (int round = 0; round < 6; round++) {
            for (String login : counts.keySet()) {
                results.add(callers.submit(() -> {
                    start.await();
                    assertThat(scalar(login(login), "SELECT count(*) FROM org_on.posthog.events")).as(login).isEqualTo(counts.get(login));
                    assertThat(column(login(login), "SHOW TABLES FROM org_on.posthog")).as(login).hasSize(OWN_TABLES.containsKey(login) ? 2 : 1);
                    return true;
                }));
            }
        }
        start.countDown();
        for (Future<Boolean> result : results) {
            assertThat(result.get(60, SECONDS)).isTrue();
        }
        // No login but these three was ever the subject, and each has one token in use
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .allMatch(request -> loginOf(request.authorization()).filter(counts::containsKey).isPresent());
        for (String login : counts.keySet()) {
            assertThat(tokenRequests).filteredOn(request -> request.isExchange() && request.subject().equals(login)).hasSizeLessThanOrEqualTo(1);
        }
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testNoRequestReachesTheMountWithoutALoginsToken()
    {
        record Refusal(String catalog, String login, ErrorCodeSupplier errorCode, String message, boolean listed) {}

        List<Refusal> refusals = ImmutableList.of(
                // Steady states: the schema is not listed
                new Refusal("org_on", REFUSED_LOGIN, PERMISSION_DENIED, LOGIN_REFUSED, false),
                new Refusal(Exchange.NOT_OFFERED.catalog(), ALICE, HOGLAKE_LOGIN_TOKEN_REFUSED, NOT_OFFERED, false),
                new Refusal(Exchange.UNAUTHORIZED.catalog(), ALICE, HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: this catalog's credential may not request per-login tokens", false),
                new Refusal(Exchange.BAD_CLIENT.catalog(), ALICE, HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service refused this catalog's credential", false),
                new Refusal(Exchange.BAD_REQUEST.catalog(), ALICE, HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service refused the request", false),
                // Outages: the schema stays listed
                new Refusal(Exchange.BUSY.catalog(), ALICE, HOGLAKE_CATALOG_UNAVAILABLE, BUSY, true),
                new Refusal(Exchange.DROPPED.catalog(), ALICE, HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE, true),
                new Refusal(Exchange.STALLED.catalog(), ALICE, HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE, true),
                // A token that the endpoint did not issue as an exchange: it could be the catalog's own
                new Refusal(Exchange.NOT_AN_EXCHANGE.catalog(), ALICE, HOGLAKE_INVALID_RESPONSE, "The token service for the posthog schema answered outside its contract", true));
        for (Refusal refusal : refusals) {
            String catalog = refusal.catalog();
            Session session = login(refusal.login());
            requests.clear();
            for (String statement : List.of(
                    "SELECT count(*) FROM %s.posthog.events",
                    "SHOW TABLES FROM %s.posthog",
                    "DESCRIBE %s.posthog.events")) {
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, statement.formatted(catalog)))
                        .as(catalog + ": " + statement)
                        .hasErrorCode(refusal.errorCode())
                        .hasMessage(refusal.message());
            }
            // The engine names the listing in front of the connector's message
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, "SELECT column_name FROM %s.information_schema.columns WHERE table_schema = 'posthog'".formatted(catalog)))
                    .as(catalog)
                    .hasErrorCode(refusal.errorCode())
                    .hasMessage("Error listing table columns for catalog %s: %s".formatted(catalog, refusal.message()));
            List<Object> schemas = column(session, "SHOW SCHEMAS FROM " + catalog);
            assertThat(schemas).as(catalog).contains("analytics", "information_schema");
            assertThat(schemas.contains("posthog")).as(catalog).isEqualTo(refusal.listed());
            // The other schemas and the listings of the whole catalog work
            assertThat(scalar(session, "SELECT count(*) FROM %s.analytics.orders".formatted(catalog))).isEqualTo(5L);
            assertThat(column(session, "SELECT table_schema || '.' || table_name FROM %s.information_schema.tables WHERE table_schema <> 'information_schema'".formatted(catalog)))
                    .containsExactly("analytics.orders");
            assertThat(column(session, "SELECT column_name FROM %s.information_schema.columns WHERE table_schema <> 'information_schema'".formatted(catalog)))
                    .containsExactly("amount");
            // Nothing was sent to the mount: not with the catalog's token, and not without a token
            assertThat(requests).as(catalog).noneMatch(request -> request.path().startsWith(MOUNT_PATH));
            assertThat(requests).as(catalog).anyMatch(request -> request.path().startsWith(ORG_PATH));
            assertNoWriteAndNoShadowRequest();
        }
        // The refused login did not change what another login of the same catalog gets
        assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
    }

    @Test
    void testTokenServiceThatDoesNotOfferTheExchange()
    {
        String catalog = Exchange.NOT_OFFERED.catalog();
        for (String login : List.of(ALICE, BOB, CAROL)) {
            Session session = login(login);
            for (int round = 0; round < 3; round++) {
                assertThat(column(session, "SHOW SCHEMAS FROM " + catalog)).containsExactlyInAnyOrder("analytics", "information_schema");
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, "SELECT count(*) FROM %s.posthog.events".formatted(catalog)))
                        .hasErrorCode(HOGLAKE_LOGIN_TOKEN_REFUSED)
                        .hasMessage(NOT_OFFERED);
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, "SHOW TABLES FROM %s.posthog".formatted(catalog)))
                        .hasErrorCode(HOGLAKE_LOGIN_TOKEN_REFUSED)
                        .hasMessage(NOT_OFFERED);
                assertThat(scalar(session, "SELECT count(*) FROM %s.analytics.orders".formatted(catalog))).isEqualTo(5L);
                assertThat(column(session, "SELECT table_schema || '.' || table_name FROM %s.information_schema.tables WHERE table_schema <> 'information_schema'".formatted(catalog)))
                        .containsExactly("analytics.orders");
            }
        }
        // The refusal is remembered for the catalog: at most one request, for all logins and statements
        assertThat(tokenRequests).filteredOn(TokenRequest::isExchange).hasSizeLessThanOrEqualTo(1);
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH));
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testTokenServiceThatIssuesNoMountToken()
    {
        // The exchange works but ignores the scope: the token names the org catalog. The
        // server shows no mount to such a token, so the schema does not exist for it.
        String catalog = Exchange.ORG_SCOPED.catalog();
        String erin = "wh1.erin";
        Session session = login(erin);
        for (int round = 0; round < 3; round++) {
            assertThat(column(session, "SHOW SCHEMAS FROM " + catalog)).containsExactlyInAnyOrder("analytics", "information_schema");
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, "SELECT count(*) FROM %s.posthog.events".formatted(catalog)))
                    .hasErrorCode(SCHEMA_NOT_FOUND)
                    .hasMessageContaining("Schema 'posthog' does not exist");
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, "SHOW TABLES FROM %s.posthog".formatted(catalog)))
                    .hasErrorCode(SCHEMA_NOT_FOUND);
            assertThat(scalar(session, "SELECT count(*) FROM %s.analytics.orders".formatted(catalog))).isEqualTo(5L);
            assertThat(column(session, "SELECT table_schema || '.' || table_name FROM %s.information_schema.tables WHERE table_schema <> 'information_schema'".formatted(catalog)))
                    .containsExactly("analytics.orders");
        }
        // The login's token went only to the mount's routes, and was requested once
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .isNotEmpty()
                .allMatch(request -> request.authorization().startsWith(ORG_SCOPED_LOGIN_TOKEN) && loginOf(request.authorization()).equals(Optional.of(erin)));
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(ORG_PATH) && !request.path().startsWith(MOUNT_PATH))
                .allMatch(request -> ("Bearer token-for-" + Exchange.ORG_SCOPED.clientId()).equals(request.authorization()));
        assertThat(tokenRequests).filteredOn(TokenRequest::isExchange).singleElement().satisfies(request -> assertThat(request.subject()).isEqualTo(erin));
        // A login the exchange refuses is told so
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(REFUSED_LOGIN), "SELECT count(*) FROM %s.posthog.events".formatted(catalog)))
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage(LOGIN_REFUSED);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testOnlyTheAuthenticatedLoginIsTheSubject()
    {
        // Warm: the login has a token, so a refusal below is not for the lack of one
        assertThat(scalar(login(BOB), "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(30L);
        requests.clear();
        tokenRequests.clear();
        List<Identity> refused = ImmutableList.of(
                // No principal: how the engine analyzes the body of a view that runs as its owner
                Identity.ofUser(BOB),
                // Another user than the one that authenticated: impersonation
                Identity.forUser(BOB).withPrincipal(new BasicPrincipal(ALICE)).build(),
                Identity.forUser(ALICE).withPrincipal(new BasicPrincipal(BOB)).build(),
                Identity.forUser(BOB).withPrincipal(new BasicPrincipal("WH1.BOB")).build(),
                Identity.forUser(BOB).withPrincipal(new BasicPrincipal(BOB + " ")).build());
        for (Identity identity : refused) {
            Session session = sqlSession(identity);
            for (String statement : List.of(
                    "SELECT count(*) FROM org_on.posthog.events",
                    "SHOW TABLES FROM org_on.posthog",
                    "DESCRIBE org_on.posthog.events")) {
                assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, statement))
                        .as(statement)
                        .hasErrorCode(PERMISSION_DENIED)
                        .hasMessage(NOT_THE_LOGIN);
            }
            assertThat(column(session, "SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema");
            assertThat(scalar(session, "SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
            assertThat(column(session, "SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                    .containsExactly("analytics.orders");
        }
        // Refused before anything was asked: no token request, and nothing sent to the mount
        assertThat(tokenRequests).isEmpty();
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH));

        // Nothing else of a session names the subject
        String frank = "wh1.frank";
        Session decorated = testSessionBuilder()
                .setCatalog("org_on")
                .setSchema("analytics")
                .setIdentity(Identity.forUser(frank)
                        .withPrincipal(new BasicPrincipal(frank))
                        .withExtraCredentials(ImmutableMap.of("subject_token", BOB, "user", BOB, "login", BOB, "hoglake.login", BOB))
                        .build())
                .setSource(BOB)
                .setClientInfo(BOB)
                .setClientTags(ImmutableSet.of(BOB))
                .setTraceToken(Optional.of(BOB))
                .build();
        assertThat(scalar(decorated, "SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        assertThat(column(decorated, "SHOW TABLES FROM org_on.posthog")).containsExactly("events");
        assertThat(tokenRequests).singleElement().satisfies(request -> assertThat(request.subject()).isEqualTo(frank));
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .isNotEmpty()
                .allMatch(request -> loginOf(request.authorization()).equals(Optional.of(frank)));
    }

    @Test
    void testSchemaWithoutATokenService()
    {
        // The catalog loaded, and its own schemas work without a token, as before
        Session session = login(ALICE);
        assertThat(scalar(session, "SELECT count(*) FROM org_noauth.analytics.orders")).isEqualTo(5L);
        assertThat(column(session, "SELECT table_schema || '.' || table_name FROM org_noauth.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactly("analytics.orders");
        // The name stays the mount's, which cannot be read: not listed, and never the org's namespace
        assertThat(column(session, "SHOW SCHEMAS FROM org_noauth")).containsExactlyInAnyOrder("analytics", "information_schema");
        for (String statement : List.of(
                "SELECT count(*) FROM org_noauth.posthog.events",
                "SELECT * FROM org_noauth.posthog.shadow",
                "SHOW TABLES FROM org_noauth.posthog",
                "DESCRIBE org_noauth.posthog.events")) {
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(session, statement))
                    .as(statement)
                    .hasErrorCode(HOGLAKE_LOGIN_TOKEN_REFUSED)
                    .hasMessage("The posthog schema is not available: this catalog has no token service configured");
        }
        for (String statement : List.of(
                "INSERT INTO org_noauth.posthog.events VALUES (1, 'x')",
                "CREATE TABLE org_noauth.posthog.created (x bigint)",
                "CREATE SCHEMA org_noauth.posthog",
                "DROP TABLE org_noauth.posthog.shadow")) {
            assertThatThrownBy(() -> queryRunner.execute(session, statement)).as(statement).isInstanceOf(RuntimeException.class);
        }
        // No request without a token reached the mount, and no token was asked for
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH));
        assertThat(requests).filteredOn(request -> request.path().startsWith(ORG_PATH)).isNotEmpty().allMatch(request -> request.authorization() == null);
        assertThat(tokenRequests).isEmpty();
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testMountClientSendsNothingWithoutALogin()
    {
        // The mount's client as the connector holds it: every request fails before it is sent
        HoglakeClient unbound = HoglakeClient.forMount(uri, MOUNT);
        for (Executable call : List.<Executable>of(
                unbound::findCatalog,
                unbound::getCatalog,
                unbound::listNamespaces,
                () -> unbound.listTables("ingest"),
                () -> unbound.getTable("ingest", "events"),
                () -> unbound.getTable("ingest", "events", 42),
                () -> unbound.planningScan("ingest", "events", 42, Set.of(1L)),
                () -> unbound.scan("ingest", "events", 42))) {
            assertThatThrownBy(call::execute)
                    .isInstanceOfSatisfying(TrinoException.class, failure -> {
                        assertThat(failure.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
                        assertThat(failure.getMessage()).isEqualTo("posthog schema request without a login");
                    });
        }
        // A source with no token is not a login either: the mount is never asked without a token
        HoglakeClient tokenless = unbound.withLogin(HoglakeTokenSource.NONE);
        assertThatThrownBy(tokenless::findCatalog)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("posthog schema request without a token");
                });
        assertThat(requests).isEmpty();

        // Bound to a login it works, each view with its own token, and the client it came from still sends nothing
        HoglakeClient alice = unbound.withLogin(fixedToken(loginToken(ALICE, 0)));
        HoglakeClient bob = unbound.withLogin(fixedToken(loginToken(BOB, 0)));
        assertThat(alice.listTables("ingest")).extracting(HoglakeDtos.TableSummary::name).containsExactly("events");
        assertThat(bob.listTables("ingest")).extracting(HoglakeDtos.TableSummary::name).containsExactlyInAnyOrder("events", "bob_only");
        assertThat(alice.listTables("ingest")).extracting(HoglakeDtos.TableSummary::name).containsExactly("events");
        assertThat(alice.catalogName()).isEqualTo(MOUNT);
        assertThatThrownBy(unbound::findCatalog).isInstanceOf(TrinoException.class);
        // A view is not closed: closing one leaves the others and their connections working
        alice.close();
        assertThat(bob.findCatalog()).isPresent();
        assertThat(alice.findCatalog()).isPresent();

        // Only the mount's client is bound to a login, and only once
        assertThatThrownBy(() -> new HoglakeClient(uri, ORG).withLogin(fixedToken(loginToken(ALICE, 0)))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> alice.withLogin(fixedToken(loginToken(BOB, 0)))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testPlanningUsesTheCallingSessionsLogin()
            throws Exception
    {
        HoglakeClient orgClient = new HoglakeClient(uri, ORG);
        HoglakePosthogSchema posthogSchema = posthogSchema();
        HoglakeMetadata metadata = new HoglakeMetadata(orgClient, null, Optional.of(posthogSchema));
        SchemaTableName events = new SchemaTableName("posthog", "events");
        // The handle is resolved by one login
        HoglakeTableHandle handle = (HoglakeTableHandle) metadata.newTransaction(true).getTableHandle(connectorLogin(ALICE), events, Optional.empty(), Optional.empty());
        assertThat(handle.mount()).contains(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        // One split manager for every session, as the connector has
        HoglakeSplitManager splitManager = new HoglakeSplitManager(orgClient, Optional.of(posthogSchema), _ -> DataSize.of(256, DataSize.Unit.MEGABYTE), new NoopSplitAffinityProvider());
        for (String login : List.of(BOB, ALICE, CAROL, BOB)) {
            requests.clear();
            ConnectorSplitSource source = splitManager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(login), handle, Set.of(), Constraint.alwaysTrue());
            List<String> paths = source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get().stream()
                    .map(split -> ((HoglakeSplit) split).path())
                    .toList();
            // Planned with the token of the session that asked for the splits, whoever resolved the handle
            assertThat(requests)
                    .filteredOn(request -> request.path().endsWith("/scan"))
                    .singleElement()
                    .satisfies(request -> assertThat(loginOf(request.authorization())).contains(login));
            assertThat(paths).as(login).hasSize(OWN_FILES.getOrDefault(login, new long[] {3, 4}).length).allMatch(path -> path.contains("/" + login + "/"));
        }
        // The same for the table's metadata
        requests.clear();
        metadata.newTransaction(true).getTableMetadata(connectorLogin(BOB), handle);
        assertThat(requests).filteredOn(request -> request.path().startsWith(MOUNT_PATH)).isNotEmpty().allMatch(request -> loginOf(request.authorization()).equals(Optional.of(BOB)));
        // A session that is not a login plans nothing
        requests.clear();
        assertTrinoExceptionThrownBy(() -> splitManager.getSplits(HoglakeTransactionHandle.INSTANCE, session(), handle, Set.of(), Constraint.alwaysTrue()))
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage(NOT_THE_LOGIN);
        assertThat(requests).isEmpty();
    }

    @Test
    void testMountServingSeveralNamespaces()
    {
        mount = Mount.TWO_NAMESPACES;
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactlyInAnyOrder("events", "sessions");
        assertThat(scalar("SELECT count(*) FROM org_on.posthog.sessions")).isEqualTo(7L);
        assertThat(requests).anyMatch(request -> request.path().equals(MOUNT_PATH + "/namespaces/other/tables/sessions/scan"));
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Table 'events' is ambiguous in the posthog schema");
    }

    @Test
    void testErrorMapping()
    {
        record Expected(int status, String code, ErrorCodeSupplier errorCode) {}

        List<Expected> cases = ImmutableList.of(
                new Expected(403, "read_only_catalog", NOT_SUPPORTED),
                new Expected(501, "mount_operation_unavailable", NOT_SUPPORTED),
                new Expected(422, "mount_scan_too_many_files", EXCEEDED_SCAN_LIMIT),
                new Expected(503, "mount_scan_timeout", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_scan_busy", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_host_unavailable", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_proxy_busy", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(502, "mount_host_invalid_response", HOGLAKE_CATALOG_UNAVAILABLE),
                // Not typed: the status decides, as for any answer, still without the body or URL
                new Expected(500, "internal", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(403, "forbidden", HOGLAKE_INVALID_RESPONSE));
        HoglakeClient client = mountClient(uri);
        for (Expected expected : cases) {
            refuseMount(expected.status(), expected.code());
            for (Executable call : List.<Executable>of(
                    client::findCatalog,
                    client::listNamespaces,
                    () -> client.listTables("ingest"),
                    () -> client.getTable("ingest", "events", 42),
                    () -> client.planningScan("ingest", "events", 42, Set.of(1L)))) {
                assertThatThrownBy(call::execute)
                        .as(expected.code())
                        .isInstanceOfSatisfying(TrinoException.class, failure -> {
                            assertThat(failure.getErrorCode()).isEqualTo(expected.errorCode().toErrorCode());
                            assertThat(failure.getMessage())
                                    .doesNotContain(BODY_MARKER)
                                    .doesNotContain("127.0.0.1")
                                    .doesNotContain(MOUNT)
                                    .doesNotContain("http");
                        });
            }
        }
        // The server has no token exchange of its own: such a code is not a typed refusal
        refuseMount(503, "mount_exchange_unavailable");
        assertThatThrownBy(client::findCatalog)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("hoglake catalog service error: a request for the posthog schema -> HTTP 503 (mount_exchange_unavailable)");
                });

        // The org catalog's client is unchanged: no typed mapping, the body in the message as before
        refuseMount(403, "read_only_catalog");
        HoglakeClient orgShapedClient = new HoglakeClient(uri, REFUSING_CATALOG);
        assertTrinoExceptionThrownBy(() -> orgShapedClient.getTable("ingest", "events", 42))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE)
                .hasMessageContaining(BODY_MARKER);

        // A server that cannot be reached: no URL in the message, and no cause that could carry one
        HoglakeClient unreachable = mountClient("http://127.0.0.1:9");
        assertThatThrownBy(unreachable::findCatalog)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("hoglake request failed: a request for the posthog schema");
                    assertThat(failure.getCause()).isNull();
                });
    }

    @Test
    void testWriteEntryPointsSendNothing()
    {
        HoglakeClient orgClient = new HoglakeClient(uri, ORG);
        HoglakePosthogSchema posthogSchema = posthogSchema();
        HoglakeMetadata metadata = new HoglakeMetadata(orgClient, null, Optional.of(posthogSchema));
        ConnectorSession session = connectorLogin(ALICE);
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle mountHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        // A handle claiming the schema without the mount, as no planning would build it
        HoglakeTableHandle schemaHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column));
        HoglakeTableHandle orgHandle = new HoglakeTableHandle("analytics", "orders", 7, "org-table", List.of(column));
        ConnectorTableMetadata created = new ConnectorTableMetadata(new SchemaTableName("posthog", "created"), List.of(new ColumnMetadata("x", BIGINT)));
        HoglakeWriteHandle writeHandle = new HoglakeWriteHandle("posthog", "events", "mount-table", 42, "s3://bucket/path/", List.of(column), List.of(column), Optional.of("operation"), Optional.empty(), List.of(), List.of(), true);
        TrinoPrincipal owner = new TrinoPrincipal(PrincipalType.USER, session.getUser());

        requests.clear();
        for (ConnectorTableHandle handle : List.<ConnectorTableHandle>of(mountHandle, schemaHandle)) {
            List<Executable> writes = ImmutableList.of(
                    () -> metadata.dropTable(session, handle),
                    () -> metadata.truncateTable(session, handle),
                    () -> metadata.renameTable(session, handle, new SchemaTableName("posthog", "renamed")),
                    () -> metadata.addColumn(session, handle, new ColumnMetadata("added", BIGINT), new ColumnPosition.Last()),
                    () -> metadata.dropColumn(session, handle, column),
                    () -> metadata.renameColumn(session, handle, column, "renamed"),
                    () -> metadata.setColumnType(session, handle, column, BIGINT),
                    () -> metadata.setTableComment(session, handle, Optional.of("x")),
                    () -> metadata.setColumnComment(session, handle, column, Optional.of("x")),
                    () -> metadata.setTableProperties(session, handle, Map.of("extra_properties", Optional.of(Map.of()))),
                    () -> metadata.beginInsert(session, handle, List.<ColumnHandle>of(column), RetryMode.NO_RETRIES),
                    () -> metadata.beginMerge(session, handle, Map.of(), RetryMode.NO_RETRIES),
                    () -> metadata.finishMerge(session, new HoglakeDeleteHandle((HoglakeTableHandle) handle, "s3://bucket/path/", "operation"), List.of(), List.of(), List.of()));
            for (Executable write : writes) {
                assertReadOnly(write);
            }
        }
        List<Executable> writes = ImmutableList.of(
                () -> metadata.createSchema(session, "posthog", Map.of(), owner),
                () -> metadata.dropSchema(session, "posthog", false),
                () -> metadata.createTable(session, created, SaveMode.FAIL),
                () -> metadata.createTable(session, created, SaveMode.IGNORE),
                () -> metadata.beginCreateTable(session, created, Optional.empty(), RetryMode.NO_RETRIES, false),
                () -> metadata.beginCreateTable(session, created, Optional.empty(), RetryMode.NO_RETRIES, true),
                () -> metadata.renameTable(session, orgHandle, new SchemaTableName("posthog", "orders")),
                () -> metadata.finishInsert(session, writeHandle, List.of(), List.of(), List.of()),
                () -> metadata.finishCreateTable(session, writeHandle, List.of(), List.of()));
        for (Executable write : writes) {
            assertReadOnly(write);
        }
        // Out of the schema, too: refused as read-only, not only as a move between schemas
        assertReadOnly(() -> metadata.renameTable(session, mountHandle, new SchemaTableName("analytics", "moved")));
        // A mount handle is refused whatever schema name it carries
        HoglakeTableHandle relabeled = new HoglakeTableHandle("analytics", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        assertReadOnly(() -> metadata.beginInsert(session, relabeled, List.<ColumnHandle>of(column), RetryMode.NO_RETRIES));
        assertReadOnly(() -> metadata.dropTable(session, relabeled));
        assertThat(requests).isEmpty();

        // A configured name is refused in any spelling hoglake could hold
        assertReadOnly(() -> metadata.createSchema(session, "PostHog", Map.of(), owner));
        assertThat(requests).isEmpty();
        assertThat(tokenRequests).isEmpty();
    }

    @Test
    void testPlanningRoutesByTheHandle()
    {
        HoglakeClient orgClient = new HoglakeClient(uri, ORG);
        HoglakePosthogSchema posthogSchema = posthogSchema();
        ConnectorSession session = connectorLogin(ALICE);
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle otherMount = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable("1b5f0c2e-1111-4222-8333-444455556666-events", "ingest"));
        HoglakeTableHandle thisMount = otherMount.withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        assertTrinoExceptionThrownBy(() -> HoglakeMetadata.clientFor(orgClient, Optional.of(posthogSchema), session, otherMount))
                .hasErrorCode(NOT_SUPPORTED);
        assertTrinoExceptionThrownBy(() -> HoglakeMetadata.clientFor(orgClient, Optional.empty(), session, thisMount))
                .hasErrorCode(NOT_SUPPORTED);
        // This catalog's mount, for a login: the mount's client. For any other session: nothing
        assertThat(HoglakeMetadata.clientFor(orgClient, Optional.of(posthogSchema), session, thisMount).catalogName()).isEqualTo(MOUNT);
        assertTrinoExceptionThrownBy(() -> HoglakeMetadata.clientFor(orgClient, Optional.of(posthogSchema), session(), thisMount))
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage(NOT_THE_LOGIN);
        // An org table is served by the org catalog's client, whatever the session
        HoglakeTableHandle orgHandle = new HoglakeTableHandle("analytics", "orders", 7, "org-table", List.of(column));
        assertThat(HoglakeMetadata.clientFor(orgClient, Optional.of(posthogSchema), session(), orgHandle)).isSameAs(orgClient);
        assertThat(requests).isEmpty();
        assertThat(tokenRequests).isEmpty();
    }

    @Test
    void testMountSnapshotIsPinnedPerTransaction()
    {
        HoglakeMetadata metadata = new HoglakeMetadata(new HoglakeClient(uri, ORG), null, Optional.of(posthogSchema()));
        HoglakeMetadata transaction = metadata.newTransaction(false);
        SchemaTableName events = new SchemaTableName("posthog", "events");
        ConnectorSession session = connectorLogin(ALICE);
        HoglakeTableHandle first = (HoglakeTableHandle) transaction.getTableHandle(session, events, Optional.empty(), Optional.empty());
        mountHead = 43;
        HoglakeTableHandle second = (HoglakeTableHandle) transaction.getTableHandle(session, events, Optional.empty(), Optional.empty());
        assertThat(first.snapshotId()).isEqualTo(42);
        assertThat(second.snapshotId()).isEqualTo(42);
        assertThat(second.mount()).contains(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        assertThat(second.stagedFiles()).isEmpty();
        HoglakeTableHandle autocommit = (HoglakeTableHandle) metadata.newTransaction(true).getTableHandle(session, events, Optional.empty(), Optional.empty());
        assertThat(autocommit.snapshotId()).isEqualTo(43);
        // System-table names are not asked for
        requests.clear();
        assertThat(metadata.newTransaction(true).getTableHandle(session, new SchemaTableName("posthog", "events$files"), Optional.empty(), Optional.empty())).isNull();
        assertThat(requests).isEmpty();
    }

    @Test
    void testTableHandleJson()
    {
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(new BlockEncodingSimdSupport(new FeaturesConfig())), TESTING_TYPE_MANAGER);
        JsonCodec<HoglakeTableHandle> codec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)))
                .withJsonDeserializers(Map.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde))).get())
                .jsonCodec(HoglakeTableHandle.class);
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle orgHandle = new HoglakeTableHandle("analytics", "orders", 7, "org-table", List.of(column));
        HoglakeTableHandle mountHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"))
                .withCountOnly();
        assertThat(codec.fromJson(codec.toJson(orgHandle))).isEqualTo(orgHandle);
        assertThat(codec.fromJson(codec.toJson(mountHandle))).isEqualTo(mountHandle);
        assertThat(codec.fromJson(codec.toJson(mountHandle)).namespace()).isEqualTo("ingest");
        assertThat(mountHandle.toString()).isEqualTo("posthog.events@42 mount countOnly");
        // A handle from a build without the posthog schema has no mount
        String withoutMount = codec.toJson(orgHandle).replaceAll(",\\s*\"mount\"\\s*:\\s*null", "");
        assertThat(withoutMount).doesNotContain("\"mount\"");
        assertThat(codec.fromJson(withoutMount)).isEqualTo(orgHandle);
        assertThatThrownBy(() -> mountHandle.withStaged(List.of(), List.of(new HoglakeDtos.DeleteFile(1, 1, "s3://bucket/dv", 1, 1, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertReadOnly(Executable write)
    {
        assertThatThrownBy(write::execute)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("The posthog schema is read-only");
                });
    }

    private void assertNoWriteAndNoShadowRequest()
    {
        Predicate<Request> write = request -> !request.method().equals("GET") && !request.path().equals("/oauth/token");
        assertThat(requests).noneMatch(write);
        assertThat(requests).noneMatch(request -> request.path().startsWith(SHADOW_PATH));
    }

    private void refuseMount(int status, String code)
    {
        mount = Mount.REFUSING;
        refusalStatus = status;
        refusalCode = code;
    }

    private void refusalCodeOnScanOnly(String code)
    {
        refusalStatus = 503;
        refusalCode = code;
    }

    private List<Object> column(String sql)
    {
        return column(queryRunner.getDefaultSession(), sql);
    }

    private List<Object> column(Session session, String sql)
    {
        MaterializedResult result = queryRunner.execute(session, sql);
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .toList();
    }

    private Object scalar(String sql)
    {
        return scalar(queryRunner.getDefaultSession(), sql);
    }

    private Object scalar(Session session, String sql)
    {
        List<MaterializedRow> rows = queryRunner.execute(session, sql).getMaterializedRows();
        assertThat(rows).hasSize(1);
        return rows.getFirst().getField(0);
    }

    /**
     * The token of the last request that this login sent to the mount.
     */
    private String lastMountToken(String login)
    {
        return testRequests.stream()
                .filter(request -> request.path().startsWith(MOUNT_PATH))
                .map(Request::authorization)
                .filter(authorization -> loginOf(authorization).equals(Optional.of(login)))
                .reduce((_, last) -> last)
                .orElseThrow();
    }

    /**
     * A token of the fake token endpoint for a login. It carries the login it was
     * issued for, so the fake mount can answer for that login and a test can see
     * whose token a request carried.
     */
    private static String loginToken(String login, int number)
    {
        return "login-token-" + Base64.getUrlEncoder().withoutPadding().encodeToString(login.getBytes(UTF_8)) + "." + number;
    }

    private static Optional<String> loginOf(String authorization)
    {
        if (authorization == null) {
            return Optional.empty();
        }
        for (String prefix : List.of(LOGIN_TOKEN, ORG_SCOPED_LOGIN_TOKEN)) {
            if (authorization.startsWith(prefix)) {
                String token = authorization.substring(prefix.length());
                return Optional.of(new String(Base64.getUrlDecoder().decode(token.substring(0, token.lastIndexOf('.'))), UTF_8));
            }
        }
        return Optional.empty();
    }

    // ---- the fake server ---------------------------------------------------

    private void handle(HttpExchange exchange)
            throws IOException
    {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = Optional.ofNullable(exchange.getRequestURI().getRawQuery()).orElse("");
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        Request request = new Request(method, path, query, authorization);
        requests.add(request);
        testRequests.add(request);
        String body = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
        if (path.equals("/oauth/token")) {
            tokenResponse(exchange, authorization, body);
            return;
        }
        if (path.equals(MOUNT_PATH) || path.startsWith(MOUNT_PATH + "/")) {
            mountResponse(exchange, method, path.substring(MOUNT_PATH.length()), authorization);
            return;
        }
        if (path.equals(ORG_PATH) || path.startsWith(ORG_PATH + "/")) {
            orgResponse(exchange, method, path.substring(ORG_PATH.length()));
            return;
        }
        if (path.startsWith("/v1/catalogs/" + REFUSING_CATALOG)) {
            refuse(exchange);
            return;
        }
        notFound(exchange);
    }

    /**
     * Both grants of the token endpoint. The client decides which token exchange
     * it gets; its client-credentials grant always works.
     */
    private void tokenResponse(HttpExchange exchange, String authorization, String body)
            throws IOException
    {
        String credentials = new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())), UTF_8);
        String clientId = credentials.substring(0, credentials.indexOf(':'));
        Map<String, String> form = new HashMap<>();
        for (String pair : Splitter.on('&').split(body)) {
            int separator = pair.indexOf('=');
            form.put(URLDecoder.decode(pair.substring(0, separator), UTF_8), URLDecoder.decode(pair.substring(separator + 1), UTF_8));
        }
        TokenRequest request = new TokenRequest(
                clientId,
                form.get("grant_type"),
                form.get("subject_token"),
                form.get("subject_token_type"),
                form.get("audience"),
                form.get("scope"),
                ImmutableSet.copyOf(form.keySet()));
        tokenRequests.add(request);
        allTokenRequests.add(request);
        // One token per client: which catalog's token a request carries is visible
        String clientCredentialsAnswer = "{\"access_token\":\"token-for-%s\", \"token_type\":\"Bearer\", \"expires_in\":3600}".formatted(clientId);
        if (!request.isExchange()) {
            respond(exchange, 200, clientCredentialsAnswer);
            return;
        }
        String subject = request.subject();
        switch (Exchange.of(clientId)) {
            case ISSUING, ORG_SCOPED -> {
                if (subject.equals(REFUSED_LOGIN)) {
                    respond(exchange, 400, tokenRefusal("invalid_grant"));
                    return;
                }
                String token = loginToken(subject, issued.incrementAndGet());
                if (Exchange.of(clientId) == Exchange.ORG_SCOPED) {
                    token = token.replace("login-token-", "orgscoped-token-");
                }
                respond(exchange, 200, "{\"access_token\":\"%s\", \"token_type\":\"Bearer\", \"expires_in\":3600, \"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\"}".formatted(token));
            }
            case NOT_OFFERED -> respond(exchange, 400, tokenRefusal("unsupported_grant_type"));
            case UNAUTHORIZED -> respond(exchange, 400, tokenRefusal("unauthorized_client"));
            case BAD_CLIENT -> respond(exchange, 401, tokenRefusal("invalid_client"));
            case BAD_REQUEST -> respond(exchange, 400, tokenRefusal("invalid_request"));
            case BUSY -> respond(exchange, 503, tokenRefusal("temporarily_unavailable"));
            case DROPPED -> exchange.close();
            case STALLED -> {
                try {
                    // Longer than the catalog's request timeout, and bounded
                    Thread.sleep(4_000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exchange.close();
            }
            // As an endpoint that does not know the grant and falls back to the client's own token
            case NOT_AN_EXCHANGE -> respond(exchange, 200, clientCredentialsAnswer);
        }
    }

    private static String tokenRefusal(String error)
    {
        return "{\"error\":\"%s\", \"error_description\":\"%s\"}".formatted(error, BODY_MARKER);
    }

    private void mountResponse(HttpExchange exchange, String method, String route, String authorization)
            throws IOException
    {
        Mount state = mount;
        if (state == Mount.MISSING) {
            notFound(exchange);
            return;
        }
        if (state == Mount.REFUSING || (refusalCode != null && route.endsWith("/scan"))) {
            refuse(exchange);
            return;
        }
        if (authorization == null || !authorization.startsWith(LOGIN_TOKEN)) {
            // As the server answers a token that does not name the mount: no such catalog
            notFound(exchange);
            return;
        }
        if (rejectedTokens.contains(authorization)) {
            respond(exchange, 401, "{\"error\":\"invalid_token\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
            return;
        }
        if (!method.equals("GET")) {
            respond(exchange, 403, "{\"error\":\"read_only_catalog\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
            return;
        }
        // Everything below is answered for the login the token was issued for
        String login = loginOf(authorization).orElseThrow();
        Optional<String> ownTable = Optional.ofNullable(OWN_TABLES.get(login));
        String files = files("s3://mount-bucket/data/ingest/%s/".formatted(login), OWN_FILES.getOrDefault(login, new long[] {3, 4}));
        boolean two = state == Mount.TWO_NAMESPACES;
        if (ownTable.isPresent() && route.equals("/namespaces/ingest/tables/" + ownTable.get())) {
            respond(exchange, 200, eventsTable(ownTable.get()));
            return;
        }
        if (ownTable.isPresent() && route.equals("/namespaces/ingest/tables/" + ownTable.get() + "/scan")) {
            respond(exchange, 200, files);
            return;
        }
        switch (route) {
            case "" -> respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://mount-bucket/\", \"head_snapshot_id\":%d, \"schema_version\":1, \"read_only\":true}".formatted(MOUNT, mountHead));
            case "/namespaces" -> respond(exchange, 200, two
                    ? "[{\"name\":\"ingest\", \"namespace_id\":1}, {\"name\":\"other\", \"namespace_id\":2}]"
                    : "[{\"name\":\"ingest\", \"namespace_id\":1}]");
            case "/namespaces/ingest/tables" -> respond(exchange, 200, ownTable
                    .map("[{\"name\":\"events\"}, {\"name\":\"%s\"}]"::formatted)
                    .orElse("[{\"name\":\"events\"}]"));
            case "/namespaces/other/tables" -> respond(exchange, two ? 200 : 404, two ? "[{\"name\":\"events\"}, {\"name\":\"sessions\"}]" : "{\"error\":\"not_found\"}");
            case "/namespaces/ingest/tables/events" -> respond(exchange, 200, eventsTable("events"));
            case "/namespaces/other/tables/events", "/namespaces/other/tables/sessions" -> {
                if (two) {
                    respond(exchange, 200, eventsTable(route.substring(route.lastIndexOf('/') + 1)));
                }
                else {
                    notFound(exchange);
                }
            }
            case "/namespaces/ingest/tables/events/scan", "/namespaces/other/tables/sessions/scan", "/namespaces/other/tables/events/scan" -> respond(exchange, 200, files);
            default -> notFound(exchange);
        }
    }

    private void orgResponse(HttpExchange exchange, String method, String route)
            throws IOException
    {
        if (!method.equals("GET")) {
            respond(exchange, 409, "{\"error\":\"conflict\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
            return;
        }
        switch (route) {
            case "" -> respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://org-bucket/\", \"head_snapshot_id\":7, \"schema_version\":1, \"capabilities\":[]}".formatted(ORG));
            case "/namespaces" -> respond(exchange, 200, "[{\"name\":\"analytics\", \"namespace_id\":1}, {\"name\":\"posthog\", \"namespace_id\":2}]");
            case "/namespaces/analytics/tables" -> respond(exchange, 200, "[{\"name\":\"orders\"}]");
            case "/namespaces/posthog/tables" -> respond(exchange, 200, "[{\"name\":\"shadow\"}, {\"name\":\"events\"}]");
            case "/namespaces/analytics/tables/orders" -> respond(exchange, 200, orgTable("orders", "amount"));
            case "/namespaces/posthog/tables/shadow", "/namespaces/posthog/tables/events" -> respond(exchange, 200, orgTable(route.substring(route.lastIndexOf('/') + 1), "org_column"));
            case "/namespaces/analytics/tables/orders/scan", "/namespaces/posthog/tables/events/scan" -> respond(exchange, 200, files("s3://org-bucket/data/analytics/orders/", 5));
            default -> notFound(exchange);
        }
    }

    private static String eventsTable(String name)
    {
        return """
               {"name":"%s", "namespace":"ingest", "table_uuid":"mount-%s",
                "columns":[{"field_id":1, "ordinal":0, "name":"team_id", "type":"long", "nullable":true},
                           {"field_id":2, "ordinal":1, "name":"event", "type":"string", "nullable":true}],
                "record_count":0, "file_count":0, "file_size_bytes":0}
               """.formatted(name, name);
    }

    private static String orgTable(String name, String column)
    {
        return """
               {"name":"%s", "namespace":"org", "table_uuid":"org-%s",
                "columns":[{"field_id":1, "ordinal":0, "name":"%s", "type":"long", "nullable":true}],
                "record_count":0, "file_count":0, "file_size_bytes":0}
               """.formatted(name, name, column);
    }

    private static String files(String prefix, long... recordCounts)
    {
        StringBuilder body = new StringBuilder("[");
        for (int index = 0; index < recordCounts.length; index++) {
            if (index > 0) {
                body.append(',');
            }
            body.append(
                    """
                    {"data_file":{"data_file_id":%d, "path":"%s%d.parquet", "record_count":%d, "file_size_bytes":1000,
                     "file_format":"parquet", "begin_snapshot":1}}
                    """.formatted(index + 1, prefix, index, recordCounts[index]));
        }
        return body.append(']').toString();
    }

    private void refuse(HttpExchange exchange)
            throws IOException
    {
        respond(exchange, refusalStatus, "{\"error\":\"%s\", \"detail\":\"%s http://10.0.0.1:8080/v1/catalogs/%s\"}".formatted(refusalCode, BODY_MARKER, MOUNT));
    }

    private static void notFound(HttpExchange exchange)
            throws IOException
    {
        respond(exchange, 404, "{\"error\":\"not_found\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException
    {
        byte[] bytes = body.getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    /**
     * Local test servers use ports 50000-50999 only.
     */
    private static HttpServer bindInPortRange()
            throws IOException
    {
        int start = ThreadLocalRandom.current().nextInt(1000);
        for (int attempt = 0; attempt < 1000; attempt++) {
            int port = 50000 + (start + attempt) % 1000;
            try {
                return HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
            }
            catch (BindException e) {
                // Taken; try the next one
            }
        }
        throw new IOException("No free port in 50000-50999");
    }
}
