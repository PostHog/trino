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
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.plugin.hoglake.TestHoglakeStorageSessions.OldSplit;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.FakeCredentialService;
import io.trino.plugin.hoglake.testing.FakeCredentialService.SessionRequest;
import io.trino.plugin.hoglake.testing.FakeCredentialService.TokenRequest;
import io.trino.plugin.hoglake.testing.LogCapture;
import io.trino.plugin.hoglake.testing.ManualClock;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_REFUSED;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.session;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.BODY_MARKER;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.CLIENT_SECRET;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.MOUNT_TOKEN;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.SESSION_TOKEN;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.loginOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The posthog schema read end to end with per-team S3 sessions: the real
 * plugin in a query runner, a Hoglake server whose mount names the team of
 * every file, a credential service that issues a session only for a token of
 * its own audience, and an S3 that serves a team's object only to a key that
 * the credential service issued for that team, and an org object only to the
 * catalog's own key. All three record every request.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakePosthogSchemaSessions
{
    private static final String ORG = "0b5f0c2e-1111-4222-8333-444455556666";
    private static final String MOUNT = ORG + "-events";
    private static final String ORG_PATH = "/v1/catalogs/" + ORG;
    private static final String MOUNT_PATH = "/v1/catalogs/" + MOUNT;
    private static final String ROOT = "s3://events-bucket/data/ingest/events/";
    private static final String ORG_DATA = "s3://org-bucket/data/analytics/orders/o.parquet";
    private static final String CATALOG_KEY = "catalog-access-key";
    private static final String ALICE = "wh1.alice";
    // A login that may read one team only
    private static final String BOB = "wh1.bob";
    private static final String CAROL = "wh1.carol";
    // A catalog keeps a login's sessions, so each test that counts requests has a login of its own
    private static final String ERIN = "wh1.erin";
    private static final String FRANK = "wh1.frank";
    private static final String GRACE = "wh1.grace";
    private static final String HEIDI = "wh1.heidi";
    private static final String NO_SESSION = "A read of the posthog schema has no storage session";
    private static final String OUTSIDE = "A file of the posthog schema is outside its team's storage prefix";
    private static final Pattern TEAM = Pattern.compile("/team_id=(\\d+)/");
    private static final Pattern CREDENTIAL = Pattern.compile("Credential=([^/]+)/");
    private static final JsonCodec<HoglakeSplit> SPLIT_CODEC = JsonCodec.jsonCodec(HoglakeSplit.class);
    private static final HoglakeColumnHandle VALUE = new HoglakeColumnHandle("value", 2, BIGINT, true);

    private record Request(String method, String path, String authorization) {}

    private record File(long id, long team, String path, long records, long size, Optional<String> deletionVector, boolean named) {}

    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final Map<String, List<File>> tables = new ConcurrentHashMap<>();
    // Method, object path and access key of every S3 request, and whether S3 served it
    private final List<String> s3Requests = new CopyOnWriteArrayList<>();
    private final List<Request> hoglakeRequests = new CopyOnWriteArrayList<>();
    private FakeCredentialService service;
    private HttpServer hoglake;
    private HttpServer s3;
    private ExecutorService serverExecutor;
    private ExecutorService callers;
    private HttpClient httpClient;
    private LogCapture logs;
    private String uri;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        service = new FakeCredentialService(MOUNT, System::currentTimeMillis);
        serverExecutor = Executors.newCachedThreadPool();
        callers = Executors.newCachedThreadPool();
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        // Four files of three teams; the first has a deletion vector that removes one row
        String a = ROOT + "team_id=4/a.parquet";
        objects.put(a, parquet(4, 1, 2, 3, 4));
        objects.put(ROOT + "team_id=4/a.puffin", PuffinDeletionVectorFixtures.deletionVector(a, 1));
        objects.put(ROOT + "team_id=4/b.parquet", parquet(4, 5, 6));
        objects.put(ROOT + "team_id=7/c.parquet", parquet(7, 70, 71));
        objects.put(ROOT + "team_id=42/d.parquet", parquet(42, 420));
        objects.put(ORG_DATA, parquet(0, 8, 9));
        tables.put("events", List.of(
                file(1, 42, "team_id=42/d.parquet", 1, Optional.empty()),
                file(2, 4, "team_id=4/a.parquet", 4, Optional.of(ROOT + "team_id=4/a.puffin")),
                file(3, 7, "team_id=7/c.parquet", 2, Optional.empty()),
                file(4, 4, "team_id=4/b.parquet", 2, Optional.empty())));
        // No deletion vector anywhere: a count of it reads nothing
        tables.put("plain", List.of(
                file(1, 7, "team_id=7/c.parquet", 2, Optional.empty()),
                file(2, 4, "team_id=4/b.parquet", 2, Optional.empty())));
        // The scan says team 4 for a file that is under team 42's prefix
        tables.put("misplaced", List.of(
                file(1, 4, "team_id=4/b.parquet", 2, Optional.empty()),
                new File(2, 4, ROOT + "team_id=42/d.parquet", 1, objects.get(ROOT + "team_id=42/d.parquet").length, Optional.empty(), true)));
        // A file of a server that does not name its team
        tables.put("teamless", List.of(
                file(1, 4, "team_id=4/b.parquet", 2, Optional.empty()),
                new File(2, 7, ROOT + "team_id=7/c.parquet", 2, objects.get(ROOT + "team_id=7/c.parquet").length, Optional.empty(), false)));
        // A file whose team is not a team
        tables.put("teamzero", List.of(new File(1, 0, ROOT + "team_id=0/z.parquet", 2, 1000, Optional.empty(), true)));
        // A file the catalog has no row count for: even a count must read it
        objects.put(ROOT + "team_id=7/u.parquet", parquet(7, 72, 73));
        tables.put("uncounted", List.of(new File(1, 7, ROOT + "team_id=7/u.parquet", -1, objects.get(ROOT + "team_id=7/u.parquet").length, Optional.empty(), true)));
        // 300 small files of four teams, in the order a scan returns them: the teams mixed
        ImmutableList.Builder<File> wide = ImmutableList.builder();
        for (int index = 0; index < 300; index++) {
            long team = index % 4 + 1L;
            wide.add(new File(index + 1, team, "%steam_id=%d/w%d.parquet".formatted(ROOT, team, index), 3, 1000, Optional.empty(), true));
        }
        tables.put("wide", wide.build());
        // One file of 200 MB, never read
        tables.put("large", List.of(new File(1, 4, ROOT + "team_id=4/large.parquet", 1_000_000, 200L << 20, Optional.empty(), true)));

        s3 = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        s3.createContext("/", this::s3);
        s3.setExecutor(serverExecutor);
        s3.start();
        hoglake = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        hoglake.createContext("/", this::hoglake);
        hoglake.setExecutor(serverExecutor);
        hoglake.start();
        uri = "http://127.0.0.1:" + hoglake.getAddress().getPort();

        queryRunner = new StandaloneQueryRunner(login(ALICE));
        queryRunner.installPlugin(new HoglakePlugin());
        // The catalog that names the credential service, and the one that does not
        queryRunner.createCatalog("org_sessions", "hoglake", catalogProperties("client-sessions", true));
        queryRunner.createCatalog("org_plain", "hoglake", catalogProperties("client-plain", false));
        // A catalog of its own for a refusal that a catalog remembers
        queryRunner.createCatalog("org_notoffered", "hoglake", catalogProperties("client-notoffered", true));
        logs = new LogCapture();
    }

    @AfterAll
    void tearDown()
    {
        if (logs != null) {
            logs.close();
        }
        if (queryRunner != null) {
            queryRunner.close();
        }
        httpClient.shutdownNow();
        callers.shutdownNow();
        hoglake.stop(0);
        s3.stop(0);
        service.close();
        serverExecutor.shutdownNow();
    }

    @BeforeEach
    void reset()
    {
        s3Requests.clear();
        hoglakeRequests.clear();
        service.reset();
        service.setNotOfferedTo("client-notoffered");
    }

    /**
     * Checked after every test, whatever it did: each token went only where it
     * belongs, no storage request was signed with a key of another team or
     * login's session, and nothing secret was logged.
     */
    @AfterEach
    void verifyEveryCredentialStayedOnItsRoute()
    {
        // The mount is asked with the login's mount token and never with its token for the credential service
        assertThat(hoglakeRequests)
                .filteredOn(request -> request.path().startsWith(MOUNT_PATH))
                .allSatisfy(request -> assertThat(request.authorization()).startsWith("Bearer " + MOUNT_TOKEN));
        assertThat(hoglakeRequests).noneMatch(request -> String.valueOf(request.authorization()).contains(SESSION_TOKEN));
        // The credential service is asked with the login's token for it and never with a mount token
        assertThat(service.sessionRequests()).allSatisfy(request -> assertThat(request.headers().get("authorization")).startsWith("Bearer " + SESSION_TOKEN));
        // Each exchange is one of the two, and the one for the credential service has no scope and no values
        assertThat(service.tokenRequests()).filteredOn(TokenRequest::isExchange).allSatisfy(request -> {
            if (request.audience().equals("hogtower-vend")) {
                assertThat(request.fields()).containsExactlyInAnyOrder("grant_type", "subject_token", "subject_token_type", "audience");
            }
            else {
                assertThat(request.audience()).isEqualTo("hoglake");
                assertThat(request.scope()).isEqualTo("hoglake:" + MOUNT + ":read");
            }
        });
        // The catalog without the property never asks for a token of the credential service
        assertThat(service.tokenRequests()).noneMatch(request -> request.clientId().equals("client-plain") && "hogtower-vend".equals(request.audience()));
        // S3 served a team's object only to a session of that team, and nothing of a team to the catalog's key
        assertThat(s3Requests).filteredOn(request -> request.contains("/team_id=")).noneMatch(request -> request.contains(" " + CATALOG_KEY + " served"));
        assertThat(logs.logged()).allSatisfy(text -> assertThat(text).doesNotContain(secrets()));
    }

    @Test
    void testEachTeamsFilesAreReadWithThatTeamsSession()
    {
        List<MaterializedRow> rows = queryRunner.execute(login(ERIN), "SELECT team_id, value FROM org_sessions.posthog.events").getMaterializedRows();
        assertThat(rows.stream().map(row -> row.getField(0) + ":" + row.getField(1)))
                .containsExactlyInAnyOrder("4:1", "4:3", "4:4", "4:5", "4:6", "7:70", "7:71", "42:420");

        // One request for the three teams of the one batch, with the login's token
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> {
            assertThat(request.status()).isEqualTo(200);
            assertThat(request.teams()).containsExactly(4L, 7L, 42L);
            assertThat(request.login()).contains(ERIN);
            assertThat(request.body()).isEqualTo("{\"catalog\":\"%s\",\"access\":\"read\",\"values\":[4,7,42]}".formatted(MOUNT));
        });
        assertThat(service.mints()).isEqualTo(3);
        // Every storage request is for a team's object and was signed with the session issued to the login for that team
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(" served"));
        assertSignedForLogin(s3Requests, ERIN);
        assertThat(s3Requests.stream().map(TestHoglakePosthogSchemaSessions::teamOf).collect(toImmutableSet())).containsExactlyInAnyOrder(4L, 7L, 42L);
        assertThat(s3Requests).anyMatch(request -> request.startsWith("GET " + ROOT + "team_id=4/a.puffin "));
        assertThat(s3Requests).noneMatch(request -> request.contains(CATALOG_KEY));

        // A second query of the login reads with the same sessions: nothing is asked again
        s3Requests.clear();
        assertThat(scalar(login(ERIN), "SELECT sum(value) FROM org_sessions.posthog.events WHERE team_id = 7")).isEqualTo(141L);
        assertThat(service.sessionRequests()).hasSize(1);
        assertThat(service.tokenRequests()).filteredOn(request -> "hogtower-vend".equals(request.audience())).hasSize(1);
        assertSignedForLogin(s3Requests, ERIN);

        // The org's own table is read with the catalog's key, as before, and costs no request
        s3Requests.clear();
        assertThat(scalar(login(ERIN), "SELECT sum(value) FROM org_sessions.analytics.orders")).isEqualTo(17L);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(" " + ORG_DATA + " " + CATALOG_KEY + " served"));
        assertThat(service.sessionRequests()).hasSize(1);
    }

    @Test
    void testTwoLoginsReadWithTheirOwnSessions()
            throws Exception
    {
        // The mount lists one team for the second login: it gets a session for that team alone
        assertThat(column(login(BOB), "SELECT value FROM org_sessions.posthog.events")).containsExactlyInAnyOrder(70L, 71L);
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> {
            assertThat(request.login()).contains(BOB);
            assertThat(request.teams()).containsExactly(7L);
        });
        assertSignedForLogin(s3Requests, BOB);

        // Both logins at once, several times each
        s3Requests.clear();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<List<Object>>> alice = new ArrayList<>();
        List<Future<List<Object>>> bob = new ArrayList<>();
        for (int query = 0; query < 4; query++) {
            alice.add(callers.submit(() -> {
                start.await();
                return column(login(HEIDI), "SELECT value FROM org_sessions.posthog.events WHERE team_id IN (7, 42)");
            }));
            bob.add(callers.submit(() -> {
                start.await();
                return column(login(BOB), "SELECT value FROM org_sessions.posthog.events");
            }));
        }
        start.countDown();
        for (Future<List<Object>> result : alice) {
            assertThat(result.get(120, SECONDS)).containsExactlyInAnyOrder(70L, 71L, 420L);
        }
        for (Future<List<Object>> result : bob) {
            assertThat(result.get(120, SECONDS)).containsExactlyInAnyOrder(70L, 71L);
        }
        // Each request to the credential service names the teams of the login whose token it carries
        assertThat(service.sessionRequests()).allSatisfy(request -> {
            assertThat(request.status()).isEqualTo(200);
            if (request.login().equals(Optional.of(BOB))) {
                assertThat(request.teams()).containsExactly(7L);
            }
            else {
                assertThat(request.login()).contains(HEIDI);
            }
        });
        // One session for each login and team, however many queries ran
        assertThat(service.mints()).isEqualTo(4);
        // A key issued to the second login never reads another team's object
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> {
            assertThat(request).endsWith(" served");
            FakeCredentialService.Issued issued = service.issuedFor(accessKeyOf(request)).orElseThrow();
            assertThat(issued.team()).isEqualTo(teamOf(request));
            if (issued.login().equals(BOB)) {
                assertThat(issued.team()).isEqualTo(7L);
            }
        });
        assertThat(s3Requests.stream().map(request -> service.issuedFor(accessKeyOf(request)).orElseThrow().login()).collect(toImmutableSet()))
                .containsExactlyInAnyOrder(HEIDI, BOB);
    }

    /**
     * A split that is answered from catalog counts and has no deletion vector
     * reads no storage: it carries no session, and none is asked for.
     */
    @Test
    void testACountFromTheCatalogNeedsNoSession()
    {
        assertThat(scalar(login(FRANK), "SELECT count(*) FROM org_sessions.posthog.plain")).isEqualTo(4L);
        assertThat(service.sessionRequests()).isEmpty();
        assertThat(service.tokenRequests()).noneMatch(request -> "hogtower-vend".equals(request.audience()));
        assertThat(s3Requests).isEmpty();

        // A file without a row count in the catalog is read to be counted, with its team's session
        assertThat(scalar(login(FRANK), "SELECT count(*) FROM org_sessions.posthog.uncounted")).isEqualTo(2L);
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> assertThat(request.teams()).containsExactly(7L));
        assertSignedForLogin(s3Requests, FRANK);
        s3Requests.clear();
        service.reset();
        service.setNotOfferedTo("client-notoffered");

        // Only the file with a deletion vector needs its team's session, to read the vector
        assertThat(scalar(login(FRANK), "SELECT count(*) FROM org_sessions.posthog.events")).isEqualTo(8L);
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> assertThat(request.teams()).containsExactly(4L));
        assertThat(service.mints()).isEqualTo(1);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(" " + ROOT + "team_id=4/a.puffin ").endsWith(" served"));
        assertSignedForLogin(s3Requests, FRANK);
    }

    @Test
    void testAFileOutsideItsTeamsPrefixFailsBeforeAnySplitIsHandedOut()
    {
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(GRACE), "SELECT value FROM org_sessions.posthog.misplaced"))
                .hasErrorCode(HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX)
                .hasMessage(OUTSIDE);
        // The session was issued, and no split reached a worker: not the misplaced file's, and not the good file's
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> assertThat(request.teams()).containsExactly(4L));
        assertThat(s3Requests).isEmpty();
        // A count that reads nothing has nothing to check
        assertThat(scalar(login(GRACE), "SELECT count(*) FROM org_sessions.posthog.misplaced")).isEqualTo(3L);
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testAFileWithoutATeamFailsAtPlanning()
    {
        for (String sql : List.of(
                "SELECT value FROM org_sessions.posthog.teamless",
                "SELECT count(*) FROM org_sessions.posthog.teamless",
                "SELECT value FROM org_sessions.posthog.teamzero")) {
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(ALICE), sql))
                    .hasErrorCode(HOGLAKE_INVALID_RESPONSE)
                    .hasMessage("hoglake scan returned a file of the posthog schema without its team");
        }
        assertThat(service.sessionRequests()).isEmpty();
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testARefusedTeamFailsTheQueryAndNothingElse()
    {
        // The credential service refuses one of the login's teams: no file is skipped, the query fails
        service.refuseTeam(CAROL, 42);
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(CAROL), "SELECT value FROM org_sessions.posthog.events"))
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot read every team's events with this login");
        assertThat(s3Requests).isEmpty();
        // The same login reads the teams it may, another login reads all, and the org's own tables are not touched by any of it
        assertThat(column(login(CAROL), "SELECT value FROM org_sessions.posthog.plain")).containsExactlyInAnyOrder(70L, 71L, 5L, 6L);
        assertThat(scalar(login(ALICE), "SELECT sum(value) FROM org_sessions.posthog.events")).isEqualTo(580L);
        assertThat(scalar(login(CAROL), "SELECT sum(value) FROM org_sessions.analytics.orders")).isEqualTo(17L);
        assertThat(column(login(CAROL), "SHOW TABLES FROM org_sessions.posthog")).contains("events");

        // A credential service that does not issue sessions for the mount
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(ALICE), "SELECT value FROM org_notoffered.posthog.events"))
                .hasErrorCode(HOGLAKE_STORAGE_SESSION_REFUSED)
                .hasMessage("The posthog schema cannot be read: the credential service does not issue storage sessions for it");
        assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_notoffered.posthog.plain")).isEqualTo(4L);
        assertThat(scalar(login(ALICE), "SELECT sum(value) FROM org_notoffered.analytics.orders")).isEqualTo(17L);
    }

    /**
     * Without the property everything is as before this change: no client
     * for the credential service exists, nothing is asked, splits carry no
     * session and are planned as they were, and a read fails as it did.
     */
    @Test
    void testWithoutThePropertyNothingChanges()
            throws Exception
    {
        assertTrinoExceptionThrownBy(() -> queryRunner.execute(login(ALICE), "SELECT value FROM org_plain.posthog.events"))
                .hasErrorCode(HOGLAKE_STORAGE_SESSION_REFUSED)
                .hasMessage(NO_SESSION);
        assertThat(scalar(login(ALICE), "SELECT count(*) FROM org_plain.posthog.plain")).isEqualTo(4L);
        assertThat(scalar(login(ALICE), "SELECT sum(value) FROM org_plain.analytics.orders")).isEqualTo(17L);
        assertThat(service.sessionRequests()).isEmpty();
        assertThat(service.sessionRequestsStarted()).isZero();
        assertThat(service.tokenRequests()).noneMatch(request -> "hogtower-vend".equals(request.audience()));
        assertThat(s3Requests).filteredOn(request -> request.contains("/team_id=")).isEmpty();

        // Nothing that could ask for a session is built
        HoglakeConfig config = config("client-plain");
        HoglakeStorageSessionStats stats = new HoglakeStorageSessionStats();
        assertThat(HoglakeModule.createStorageSessions(config, node(true), stats)).isEmpty();
        // The property alone is not enough: the schema must be on
        assertThat(HoglakeModule.createStorageSessions(config("client-plain").setPosthogSchemaCredentialsUri(service.credentialsUri().toString()).setPosthogSchemaEnabled(false), node(true), stats)).isEmpty();

        // The splits of a scan are those of the code before this change, in the same order, byte for byte
        try (HoglakeClient orgClient = new HoglakeClient(uri, ORG); HoglakePosthogSchema schema = posthogSchema()) {
            HoglakeTableHandle handle = handle(schema, orgClient, "wide");
            HoglakeSplitManager plain = new HoglakeSplitManager(orgClient, Optional.of(schema), Optional.empty(), _ -> DataSize.of(256, DataSize.Unit.MEGABYTE), new NoopSplitAffinityProvider());
            for (HoglakeTableHandle planned : List.of(handle, handle.withCountOnly(), handle(schema, orgClient, "large"), handle(schema, orgClient, "events"))) {
                ConnectorSplitSource source = plain.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), planned, Set.of(), Constraint.alwaysTrue());
                assertThat(source).isInstanceOf(FixedSplitSource.class);
                List<HoglakeSplit> expected;
                List<HoglakeDtos.ScanFile> scan = HoglakeSplitManager.prunedScan(schema.client(connectorLogin(ALICE)), planned);
                if (planned.countOnly()) {
                    expected = HoglakeSplitManager.wholeFileSplits(scan, new NoopSplitAffinityProvider());
                }
                else {
                    expected = HoglakeSplitManager.toSplits(scan, DataSize.of(256, DataSize.Unit.MEGABYTE).toBytes(), new NoopSplitAffinityProvider());
                }
                List<String> planning = source.getNextBatch(100_000, DynamicFilterSnapshot.EMPTY).get().stream()
                        .map(split -> SPLIT_CODEC.toJson((HoglakeSplit) split))
                        .collect(toImmutableList());
                assertThat(planning).containsExactlyElementsOf(expected.stream().map(SPLIT_CODEC::toJson).collect(toImmutableList()));
                assertThat(planning).isNotEmpty().noneMatch(json -> json.contains("session"));
            }
        }
        assertThat(service.sessionRequestsStarted()).isZero();
    }

    /**
     * Only a coordinator has a client for the credential service. A worker
     * with the same catalog properties builds none and sends nothing.
     */
    @Test
    void testAWorkerBuildsNoClientForTheCredentialService()
    {
        HoglakeConfig config = config("client-sessions").setPosthogSchemaCredentialsUri(service.credentialsUri().toString());
        HoglakeStorageSessionStats stats = new HoglakeStorageSessionStats();
        assertThat(HoglakeModule.createStorageSessions(config, node(false), stats)).isEmpty();
        Optional<HoglakeStorageSessions> coordinator = HoglakeModule.createStorageSessions(config, node(true), stats);
        assertThat(coordinator).isPresent();
        try (HoglakeStorageSessions sessions = coordinator.orElseThrow()) {
            // Built as the catalog uses it: it asks with the login's token for the service's audience
            assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)).keySet()).containsExactly(4L);
        }
        assertThat(service.sessionRequests()).singleElement().satisfies(request -> assertThat(request.login()).contains(ALICE));
        assertThat(stats.getRequests()).isEqualTo(1);

        // Set without a token endpoint the catalog's sessions are built, and every ask fails, typed, without a request
        HoglakeConfig withoutTokens = new HoglakeConfig().setUri(uri).setCatalog(ORG).setPosthogSchemaEnabled(true).setPosthogSchemaCredentialsUri(service.credentialsUri().toString());
        try (HoglakeStorageSessions sessions = HoglakeModule.createStorageSessions(withoutTokens, node(true), stats).orElseThrow()) {
            assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: this catalog has no token service configured");
        }
        assertThat(service.sessionRequests()).hasSize(1);
    }

    /**
     * The split source through the split manager, with a clock the test moves:
     * at most 100 splits a call, in team order, each batch with the sessions of
     * its own teams only and asked for only when the batch is.
     */
    @Test
    void testSplitsAreHandedOutInBatchesWithTheSessionsOfTheirTeams()
            throws Exception
    {
        ManualClock clock = new ManualClock(Instant.now());
        try (FakeCredentialService paced = new FakeCredentialService(MOUNT, clock::millis);
                HoglakeClient orgClient = new HoglakeClient(uri, ORG);
                HoglakePosthogSchema schema = posthogSchema();
                HoglakeStorageSessions sessions = sessions(paced, clock)) {
            HoglakeTableHandle wide = handle(schema, orgClient, "wide");
            List<HoglakeStorageSessionSplitSource.TeamSplit> planned = tables.get("wide").stream()
                    .map(file -> new HoglakeStorageSessionSplitSource.TeamSplit(file.team(), new HoglakeSplit(file.id(), file.path(), file.size(), file.records(), Optional.empty(), 0, Optional.empty()), true))
                    .collect(toImmutableList());
            HoglakeStorageSessionSplitSource source = new HoglakeStorageSessionSplitSource(planned, sessions.forLogin(ALICE), sessions.executor(), clock);
            // Planning asked for nothing
            assertThat(paced.sessionRequestsStarted()).isZero();
            assertThat(source.isFinished()).isFalse();

            List<HoglakeSplit> handedOut = new ArrayList<>();
            List<List<Long>> askedByBatch = new ArrayList<>();
            while (!source.isFinished()) {
                int before = paced.sessionRequests().size();
                List<ConnectorSplit> batch = source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS);
                // The least the engine takes, whatever it asks for
                assertThat(batch).hasSize(100).hasSizeLessThanOrEqualTo(HoglakePosthogSchema.SPLITS_PER_BATCH);
                List<SessionRequest> asked = paced.sessionRequests().subList(before, paced.sessionRequests().size());
                askedByBatch.add(asked.stream().flatMap(request -> request.teams().stream()).collect(toImmutableList()));
                for (ConnectorSplit split : batch) {
                    HoglakeSplit hoglakeSplit = (HoglakeSplit) split;
                    HoglakeStorageSession session = hoglakeSplit.session().orElseThrow();
                    // The session of the file's own team, issued to the login, with ten minutes left as it is handed out
                    assertThat(hoglakeSplit.path()).startsWith(session.prefix());
                    assertThat(paced.issuedFor(session.accessKeyId())).hasValueSatisfying(issued -> {
                        assertThat(issued.login()).isEqualTo(ALICE);
                        assertThat(issued.team()).isEqualTo(session.teamId());
                    });
                    assertThat(session.expirationEpochMillis() - clock.millis()).isGreaterThanOrEqualTo(Duration.ofMinutes(10).toMillis());
                    handedOut.add(hoglakeSplit);
                }
                // The scan goes on for longer than a session lives
                clock.advance(Duration.ofMinutes(7));
                paced.refillRequestBuckets();
            }
            assertThat(handedOut).hasSize(300);
            // In team order, and stable within a team
            assertThat(handedOut.stream().map(split -> split.session().orElseThrow().teamId())).isSorted();
            assertThat(handedOut.stream().filter(split -> split.session().orElseThrow().teamId() == 3).map(HoglakeSplit::dataFileId)).isSorted();
            assertThat(handedOut.stream().map(HoglakeSplit::dataFileId).collect(toImmutableSet())).hasSize(300);
            // Each batch asked for its own teams and for no team ahead of its splits: 75 splits a team, 100 a batch.
            // Teams 2 and 3 span two batches seven minutes apart, so their sessions are asked for again and replaced.
            assertThat(askedByBatch).containsExactly(List.of(1L, 2L), List.of(2L, 3L), List.of(3L, 4L));
            assertThat(paced.mints()).isEqualTo(6);
            assertThat(source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).isEmpty();

            Metrics metrics = source.getMetrics();
            assertThat(metrics.getMetrics().get(HoglakeStorageSessionSplitSource.SESSION_SPLITS)).isEqualTo(new LongCount(300));
            assertThat(metrics.getMetrics().get(HoglakeStorageSessionSplitSource.SESSION_TEAMS)).isEqualTo(new LongCount(6));
            assertThat(((TDigestHistogram) metrics.getMetrics().get(HoglakeStorageSessionSplitSource.SESSION_SECONDS_LEFT_AT_HAND_OUT)).getMin()).isBetween(600.0, 900.0);

            // Through the split manager: the same, planned from the mount's scan
            HoglakeSplitManager manager = new HoglakeSplitManager(orgClient, Optional.of(schema), Optional.of(sessions), _ -> DataSize.of(256, DataSize.Unit.MEGABYTE), new NoopSplitAffinityProvider());
            ConnectorSplitSource managed = manager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), wide, Set.of(), Constraint.alwaysTrue());
            assertThat(managed).isInstanceOf(HoglakeStorageSessionSplitSource.class);
            int requests = paced.sessionRequests().size();
            List<ConnectorSplit> first = managed.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS);
            assertThat(first).hasSize(100).allSatisfy(split -> assertThat(((HoglakeSplit) split).session().orElseThrow().teamId()).isIn(1L, 2L));
            assertThat(paced.sessionRequests().subList(requests, paced.sessionRequests().size())).singleElement()
                    .satisfies(request -> assertThat(request.teams()).containsExactly(1L, 2L));
            // The engine may ask for fewer
            assertThat(managed.getNextBatch(7, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).hasSize(7);
            managed.close();
            assertThat(managed.isFinished()).isTrue();
            assertThat(managed.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).isEmpty();

            // A count from the catalog: no session on any split, and nothing asked
            requests = paced.sessionRequestsStarted();
            ConnectorSplitSource counted = manager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), wide.withCountOnly(), Set.of(), Constraint.alwaysTrue());
            List<ConnectorSplit> counts = counted.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS);
            assertThat(counts).hasSize(100).allSatisfy(split -> assertThat(((HoglakeSplit) split).session()).isEmpty());
            assertThat(paced.sessionRequestsStarted()).isEqualTo(requests);
            // but the one with a deletion vector gets its team's
            ConnectorSplitSource countedEvents = manager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), handle(schema, orgClient, "events").withCountOnly(), Set.of(), Constraint.alwaysTrue());
            List<ConnectorSplit> eventCounts = countedEvents.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS);
            assertThat(eventCounts).hasSize(4);
            assertThat(eventCounts).filteredOn(split -> ((HoglakeSplit) split).session().isPresent()).singleElement()
                    .satisfies(split -> assertThat(((HoglakeSplit) split).deleteFilePath()).isPresent());

            // The splits of a posthog table are cut to 64 MB at most; a smaller session setting is kept
            ConnectorSplitSource large = manager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), handle(schema, orgClient, "large"), Set.of(), Constraint.alwaysTrue());
            assertThat(large.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).extracting(split -> ((HoglakeSplit) split).length())
                    .containsExactly(64L << 20, 64L << 20, 64L << 20, 8L << 20);
            HoglakeSplitManager smaller = new HoglakeSplitManager(orgClient, Optional.of(schema), Optional.of(sessions), _ -> DataSize.of(32, DataSize.Unit.MEGABYTE), new NoopSplitAffinityProvider());
            assertThat(smaller.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), handle(schema, orgClient, "large"), Set.of(), Constraint.alwaysTrue())
                    .getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).hasSize(7);

            // A session that is not the login's cannot be reached: the source asks as the session that planned
            assertTrinoExceptionThrownBy(() -> manager.getSplits(HoglakeTransactionHandle.INSTANCE, session(), wide, Set.of(), Constraint.alwaysTrue()))
                    .hasErrorCode(PERMISSION_DENIED);
        }
    }

    @Test
    void testABatchFailsWholeAndASourceCanBeClosedWhileItWaits()
            throws Exception
    {
        ManualClock clock = new ManualClock(Instant.now());
        try (FakeCredentialService paced = new FakeCredentialService(MOUNT, clock::millis);
                HoglakeStorageSessions sessions = sessions(paced, clock)) {
            HoglakeSplit good = new HoglakeSplit(1, ROOT + "team_id=4/b.parquet", 100, 2, Optional.empty(), 0, Optional.empty());
            HoglakeSplit other = new HoglakeSplit(2, ROOT + "team_id=7/c.parquet", 100, 2, Optional.empty(), 0, Optional.empty());
            // A data file, and a deletion vector, that the scan puts in a team whose prefix does not hold them
            for (HoglakeSplit misplaced : List.of(
                    new HoglakeSplit(3, ROOT + "team_id=42/d.parquet", 100, 1, Optional.empty(), 0, Optional.empty()),
                    new HoglakeSplit(3, ROOT + "team_id=7/../team_id=42/d.parquet", 100, 1, Optional.empty(), 0, Optional.empty()),
                    new HoglakeSplit(3, ROOT + "team_id=7/d.parquet", 100, 1, Optional.of(ROOT + "team_id=42/d.puffin"), 1, Optional.of("puffin-dv")))) {
                HoglakeStorageSessionSplitSource source = new HoglakeStorageSessionSplitSource(
                        List.of(
                                new HoglakeStorageSessionSplitSource.TeamSplit(4, good, true),
                                new HoglakeStorageSessionSplitSource.TeamSplit(7, other, true),
                                new HoglakeStorageSessionSplitSource.TeamSplit(7, misplaced, true)),
                        sessions.forLogin(ALICE),
                        sessions.executor(),
                        clock);
                CompletableFuture<List<ConnectorSplit>> batch = source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY);
                assertFails(() -> batch.get(60, SECONDS), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
                // Nothing of the batch was handed out, and it is not skipped
                assertThat(source.isFinished()).isFalse();
                assertFails(() -> source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
            }

            // One refused team fails the batch of all its teams
            paced.refuseTeam(ALICE, 9);
            HoglakeStorageSessionSplitSource refused = new HoglakeStorageSessionSplitSource(
                    List.of(
                            new HoglakeStorageSessionSplitSource.TeamSplit(4, good, true),
                            new HoglakeStorageSessionSplitSource.TeamSplit(9, new HoglakeSplit(4, ROOT + "team_id=9/e.parquet", 100, 1, Optional.empty(), 0, Optional.empty()), true)),
                    sessions.forLogin(ALICE),
                    sessions.executor(),
                    clock);
            assertFails(() -> refused.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS), PERMISSION_DENIED, "Access Denied: Cannot read every team's events with this login");
            assertThat(refused.isFinished()).isFalse();

            // Closed while the credential service has not answered: the call ends, and the source is finished
            CountDownLatch hold = new CountDownLatch(1);
            paced.holdSessions(hold);
            HoglakeStorageSessionSplitSource waiting = new HoglakeStorageSessionSplitSource(
                    List.of(new HoglakeStorageSessionSplitSource.TeamSplit(11, new HoglakeSplit(5, ROOT + "team_id=11/f.parquet", 100, 1, Optional.empty(), 0, Optional.empty()), true)),
                    sessions.forLogin(ALICE),
                    sessions.executor(),
                    clock);
            CompletableFuture<List<ConnectorSplit>> pending = waiting.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY);
            // The caller's thread did not wait for the answer
            assertThat(pending).isNotDone();
            waiting.close();
            assertThat(waiting.isFinished()).isTrue();
            hold.countDown();
            pending.get(60, SECONDS);
            assertThat(waiting.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS)).isEmpty();
        }
    }

    /**
     * A node that runs the code before storage sessions does not know the
     * split's session. It ignores the field and reads with the catalog's own
     * credentials, which S3 refuses for a team's file: it fails closed.
     */
    @Test
    void testANodeWithoutSessionsIgnoresTheSessionAndIsRefusedByStorage()
            throws Exception
    {
        ManualClock clock = new ManualClock(Instant.now());
        try (HoglakeClient orgClient = new HoglakeClient(uri, ORG);
                HoglakePosthogSchema schema = posthogSchema();
                HoglakeStorageSessions sessions = sessions(service, clock)) {
            HoglakeSplitManager manager = new HoglakeSplitManager(orgClient, Optional.of(schema), Optional.of(sessions), _ -> DataSize.of(256, DataSize.Unit.MEGABYTE), new NoopSplitAffinityProvider());
            HoglakeTableHandle events = handle(schema, orgClient, "events");
            List<ConnectorSplit> splits = manager.getSplits(HoglakeTransactionHandle.INSTANCE, connectorLogin(ALICE), events, Set.of(), Constraint.alwaysTrue())
                    .getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get(60, SECONDS);
            HoglakeSplit withSession = (HoglakeSplit) splits.stream().filter(split -> ((HoglakeSplit) split).path().endsWith("c.parquet")).findFirst().orElseThrow();
            String wire = SPLIT_CODEC.toJson(withSession);
            assertThat(wire).contains("\"session\"");

            S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                    .setEndpoint("http://127.0.0.1:" + s3.getAddress().getPort())
                    .setRegion("us-east-1")
                    .setPathStyleAccess(true)
                    .setAwsAccessKey(CATALOG_KEY)
                    .setAwsSecretKey("catalog-secret-value")
                    .setMaxErrorRetries(1), new S3FileSystemStats());
            try {
                HoglakePageSourceProvider pageSources = new HoglakePageSourceProvider(fileSystemFactory);
                // This version, after the trip through the engine's codec, reads the file with the session
                assertThat(read(pageSources, events, SPLIT_CODEC.fromJson(wire))).containsExactly(List.of(70L), List.of(71L));
                assertSignedForLogin(s3Requests, ALICE);

                // The older node decodes the same bytes to a split without a session
                OldSplit old = JsonCodec.jsonCodec(OldSplit.class).fromJson(wire);
                HoglakeSplit ignored = new HoglakeSplit(old.dataFileId(), old.path(), old.fileSizeBytes(), old.recordCount(), old.deleteFilePath(), old.deleteCount(), old.deleteFileFormat(), old.start(), old.length(), old.splitWeight(), old.footerSize(), old.affinityKey());
                assertThat(ignored.session()).isEmpty();
                // Its read is the one of a table that has no storage sessions: the catalog's own credentials
                s3Requests.clear();
                HoglakeTableHandle asBefore = new HoglakeTableHandle(events.schemaName(), events.tableName(), events.snapshotId(), events.tableUuid(), events.columns());
                assertFails(() -> read(pageSources, asBefore, ignored), GENERIC_INTERNAL_ERROR, "Failed to open parquet file: " + ROOT + "team_id=7/c.parquet");
                assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(" " + CATALOG_KEY + " denied"));
                // A node with the worker side and no coordinator side refuses the same split before storage
                s3Requests.clear();
                assertFails(() -> read(pageSources, events, ignored), HOGLAKE_STORAGE_SESSION_REFUSED, NO_SESSION);
                assertThat(s3Requests).isEmpty();
            }
            finally {
                fileSystemFactory.destroy();
            }
        }
    }

    // ---- helpers -----------------------------------------------------------

    private HoglakeStorageSessions sessions(FakeCredentialService credentials, ManualClock clock)
    {
        HoglakeLoginTokenSource tokens = HoglakePosthogSchema.storageSessionTokens(credentials.tokenUri(), "client-direct", CLIENT_SECRET, httpClient);
        HoglakeStorageSessionClient client = new HoglakeStorageSessionClient(credentials.credentialsUri(), MOUNT, httpClient, clock);
        return new HoglakeStorageSessions(MOUNT, client, tokens, new HoglakeStorageSessionStats(), clock, clock::advanceTo, HoglakeStorageSessions.MERGE_WINDOW);
    }

    private HoglakePosthogSchema posthogSchema()
    {
        return new HoglakePosthogSchema(ORG, HoglakeClient.forMount(uri, MOUNT), new HoglakeLoginTokenSource.TokenExchange(
                service.tokenUri(),
                "client-direct",
                CLIENT_SECRET,
                "hoglake",
                HoglakePosthogSchema.LOGIN_SUBJECT_TOKEN_TYPE,
                Optional.of(HoglakePosthogSchema.mountScope(MOUNT)),
                httpClient));
    }

    private static HoglakeTableHandle handle(HoglakePosthogSchema schema, HoglakeClient orgClient, String table)
    {
        HoglakeMetadata metadata = new HoglakeMetadata(orgClient, null, Optional.of(schema));
        return (HoglakeTableHandle) metadata.newTransaction(true).getTableHandle(connectorLogin(ALICE), new SchemaTableName("posthog", table), Optional.empty(), Optional.empty());
    }

    private HoglakeConfig config(String clientId)
    {
        return new HoglakeConfig()
                .setUri(uri)
                .setCatalog(ORG)
                .setAuthTokenUri(service.tokenUri().toString())
                .setAuthClientId(clientId)
                .setAuthClientSecret(CLIENT_SECRET)
                .setPosthogSchemaEnabled(true);
    }

    private Map<String, String> catalogProperties(String clientId, boolean credentialService)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", ORG)
                .put("hoglake.auth.token-uri", service.tokenUri().toString())
                .put("hoglake.auth.client-id", clientId)
                .put("hoglake.auth.client-secret", CLIENT_SECRET)
                .put("hoglake.posthog-schema.enabled", "true")
                .put("s3.endpoint", "http://127.0.0.1:" + s3.getAddress().getPort())
                .put("s3.region", "us-east-1")
                .put("s3.path-style-access", "true")
                .put("s3.max-error-retries", "1")
                .put("s3.aws-access-key", CATALOG_KEY)
                .put("s3.aws-secret-key", "catalog-secret-value");
        if (credentialService) {
            properties.put("hoglake.posthog-schema.credentials-uri", service.credentialsUri().toString());
        }
        return properties.buildOrThrow();
    }

    private static Session login(String login)
    {
        return testSessionBuilder()
                .setCatalog("org_sessions")
                .setSchema("posthog")
                .setIdentity(Identity.forUser(login).withPrincipal(new BasicPrincipal(login)).build())
                .build();
    }

    private static ConnectorSession connectorLogin(String login)
    {
        return session(ConnectorIdentity.forUser(login).withPrincipal(new BasicPrincipal(login)).build());
    }

    private static Node node(boolean coordinator)
    {
        return new Node()
        {
            @Override
            public String getHost()
            {
                return "127.0.0.1";
            }

            @Override
            public HostAddress getHostAndPort()
            {
                return HostAddress.fromParts("127.0.0.1", 1);
            }

            @Override
            public String getNodeIdentifier()
            {
                return "node";
            }

            @Override
            public String getVersion()
            {
                return "test";
            }

            @Override
            public boolean isCoordinator()
            {
                return coordinator;
            }
        };
    }

    private List<Object> column(Session session, String sql)
    {
        return queryRunner.execute(session, sql).getMaterializedRows().stream().map(row -> row.getField(0)).collect(toImmutableList());
    }

    private Object scalar(Session session, String sql)
    {
        List<MaterializedRow> rows = queryRunner.execute(session, sql).getMaterializedRows();
        assertThat(rows).hasSize(1);
        return rows.getFirst().getField(0);
    }

    private static List<List<Object>> read(HoglakePageSourceProvider pageSources, HoglakeTableHandle table, HoglakeSplit split)
            throws IOException
    {
        try (ConnectorPageSource source = pageSources.createPageSource(HoglakeTransactionHandle.INSTANCE, ConnectorTestFixtures.session(), split, table, Optional.empty(), List.<ColumnHandle>of(VALUE), DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
            return ConnectorTestFixtures.readAll(source, List.of(BIGINT));
        }
    }

    /**
     * Every request is for a team's object and was signed with a session that the
     * credential service issued to the login for that object's team.
     */
    private void assertSignedForLogin(List<String> requests, String login)
    {
        assertThat(requests).isNotEmpty().allSatisfy(request -> {
            FakeCredentialService.Issued issued = service.issuedFor(accessKeyOf(request)).orElseThrow();
            assertThat(issued.login()).isEqualTo(login);
            assertThat(issued.team()).isEqualTo(teamOf(request));
        });
    }

    private static String accessKeyOf(String request)
    {
        return Splitter.on(' ').splitToList(request).get(2);
    }

    private static long teamOf(String request)
    {
        Matcher team = TEAM.matcher(request);
        assertThat(team.find()).as(request).isTrue();
        return Long.parseLong(team.group(1));
    }

    private static TrinoException assertFails(ThrowingCallable call, ErrorCodeSupplier errorCode, String message)
    {
        Throwable thrown = catchThrowableOfType(Throwable.class, call);
        assertThat(thrown).as("no failure").isNotNull();
        if (thrown instanceof ExecutionException) {
            thrown = thrown.getCause();
        }
        assertThat(thrown).isInstanceOf(TrinoException.class);
        TrinoException failure = (TrinoException) thrown;
        assertThat(failure.getErrorCode()).isEqualTo(errorCode.toErrorCode());
        assertThat(failure).hasMessage(message);
        return failure;
    }

    // Everything that must appear in no log line
    private String[] secrets()
    {
        List<String> secrets = new ArrayList<>(List.of(BODY_MARKER, CLIENT_SECRET, "catalog-secret-value"));
        for (String clientId : List.of("client-sessions", "client-plain", "client-notoffered", "client-direct")) {
            secrets.add(Base64.getEncoder().encodeToString((clientId + ":" + CLIENT_SECRET).getBytes(UTF_8)));
        }
        secrets.addAll(service.allIssuedTokens());
        secrets.addAll(service.sessionValues());
        return secrets.toArray(String[]::new);
    }

    private File file(long id, long team, String name, long records, Optional<String> deletionVector)
    {
        return new File(id, team, ROOT + name, records, objects.get(ROOT + name).length, deletionVector, true);
    }

    private static byte[] parquet(long team, long... values)
    {
        List<Object> teams = new ArrayList<>();
        List<Object> column = new ArrayList<>();
        for (long value : values) {
            teams.add(team);
            column.add(value);
        }
        return ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(optional(INT64).id(1).named("team_id"), BIGINT, teams),
                new FileColumn(optional(INT64).id(2).named("value"), BIGINT, column)));
    }

    // ---- the fake S3 -------------------------------------------------------

    private void s3(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            String path = "s3:/" + exchange.getRequestURI().getPath();
            Matcher key = CREDENTIAL.matcher(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            String accessKey = "none";
            if (key.find()) {
                accessKey = key.group(1);
            }
            Matcher team = TEAM.matcher(path);
            boolean allowed;
            if (team.find()) {
                // A team's object: only a session of that team that has not expired
                long teamId = Long.parseLong(team.group(1));
                allowed = service.issuedFor(accessKey)
                        .map(issued -> issued.team() == teamId && issued.expirationMillis() > System.currentTimeMillis())
                        .orElse(false);
            }
            else {
                allowed = accessKey.equals(CATALOG_KEY);
            }
            String served = "denied";
            if (allowed) {
                served = "served";
            }
            s3Requests.add("%s %s %s %s".formatted(exchange.getRequestMethod(), path, accessKey, served));
            byte[] content = objects.get(path);
            if (!allowed || content == null) {
                int status = 403;
                if (allowed) {
                    status = 404;
                }
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            exchange.getResponseHeaders().set("Last-Modified", DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.parse("2026-01-01T00:00:00Z").atZone(ZoneOffset.UTC)));
            if (exchange.getRequestMethod().equals("HEAD")) {
                exchange.getResponseHeaders().set("Content-Length", Integer.toString(content.length));
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            int start = 0;
            int end = content.length - 1;
            String range = exchange.getRequestHeaders().getFirst("Range");
            int status = 200;
            if (range != null) {
                status = 206;
                String[] offsets = range.substring("bytes=".length()).split("-", -1);
                if (offsets[0].isEmpty()) {
                    start = Math.max(0, content.length - Integer.parseInt(offsets[1]));
                }
                else {
                    start = Integer.parseInt(offsets[0]);
                    if (!offsets[1].isEmpty()) {
                        end = Math.min(end, Integer.parseInt(offsets[1]));
                    }
                }
                exchange.getResponseHeaders().set("Content-Range", "bytes %s-%s/%s".formatted(start, end, content.length));
            }
            exchange.sendResponseHeaders(status, end - start + 1);
            exchange.getResponseBody().write(content, start, end - start + 1);
        }
    }

    // ---- the fake Hoglake server -------------------------------------------

    private void hoglake(HttpExchange exchange)
            throws IOException
    {
        String path = exchange.getRequestURI().getPath();
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        hoglakeRequests.add(new Request(exchange.getRequestMethod(), path, authorization));
        exchange.getRequestBody().readAllBytes();
        if (path.equals(MOUNT_PATH) || path.startsWith(MOUNT_PATH + "/")) {
            // The mount serves a token that was issued for a login and for the mount, and nothing else
            Optional<String> login = Optional.empty();
            if (authorization != null && authorization.startsWith("Bearer ")) {
                login = loginOf(MOUNT_TOKEN, authorization.substring("Bearer ".length()));
            }
            if (login.isEmpty() || !exchange.getRequestMethod().equals("GET")) {
                respond(exchange, 404, "{\"error\":\"not_found\"}");
                return;
            }
            mount(exchange, path.substring(MOUNT_PATH.length()), login.get());
            return;
        }
        if (path.equals(ORG_PATH) || path.startsWith(ORG_PATH + "/")) {
            switch (path.substring(ORG_PATH.length())) {
                case "" -> respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://org-bucket/\", \"head_snapshot_id\":7, \"schema_version\":1, \"capabilities\":[]}".formatted(ORG));
                case "/namespaces" -> respond(exchange, 200, "[{\"name\":\"analytics\", \"namespace_id\":1}]");
                case "/namespaces/analytics/tables" -> respond(exchange, 200, "[{\"name\":\"orders\"}]");
                case "/namespaces/analytics/tables/orders" -> respond(exchange, 200, table("orders", "analytics"));
                case "/namespaces/analytics/tables/orders/scan" -> respond(exchange, 200, scan(List.of(new File(1, 0, ORG_DATA, 2, objects.get(ORG_DATA).length, Optional.empty(), false))));
                default -> respond(exchange, 404, "{\"error\":\"not_found\"}");
            }
            return;
        }
        respond(exchange, 404, "{\"error\":\"not_found\"}");
    }

    private void mount(HttpExchange exchange, String route, String login)
            throws IOException
    {
        String tablesRoute = "/namespaces/ingest/tables";
        if (route.isEmpty()) {
            respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://events-bucket/\", \"head_snapshot_id\":42, \"schema_version\":1, \"read_only\":true}".formatted(MOUNT));
            return;
        }
        if (route.equals("/namespaces")) {
            respond(exchange, 200, "[{\"name\":\"ingest\", \"namespace_id\":1}]");
            return;
        }
        if (route.equals(tablesRoute)) {
            respond(exchange, 200, tables.keySet().stream().sorted().map("{\"name\":\"%s\"}"::formatted).collect(Collectors.joining(",", "[", "]")));
            return;
        }
        if (!route.startsWith(tablesRoute + "/")) {
            respond(exchange, 404, "{\"error\":\"not_found\"}");
            return;
        }
        String name = route.substring(tablesRoute.length() + 1);
        boolean scan = name.endsWith("/scan");
        if (scan) {
            name = name.substring(0, name.length() - "/scan".length());
        }
        List<File> files = tables.get(name);
        if (files == null) {
            respond(exchange, 404, "{\"error\":\"not_found\"}");
            return;
        }
        if (!scan) {
            respond(exchange, 200, table(name, "ingest"));
            return;
        }
        if (login.equals(BOB)) {
            // What the mount lists for a login is that login's teams
            files = files.stream().filter(file -> file.team() == 7).collect(toImmutableList());
        }
        respond(exchange, 200, scan(files));
    }

    private static String table(String name, String namespace)
    {
        return """
               {"name":"%s", "namespace":"%s", "table_uuid":"uuid-%s",
                "columns":[{"field_id":1, "ordinal":0, "name":"team_id", "type":"long", "nullable":true},
                           {"field_id":2, "ordinal":1, "name":"value", "type":"long", "nullable":true}],
                "record_count":0, "file_count":0, "file_size_bytes":0}
               """.formatted(name, namespace, name);
    }

    private static String scan(List<File> files)
    {
        List<String> entries = new ArrayList<>();
        for (File file : files) {
            StringBuilder entry = new StringBuilder("{\"data_file\":{\"data_file_id\":%d, \"path\":\"%s\", \"record_count\":%d, \"file_size_bytes\":%d, \"file_format\":\"parquet\", \"begin_snapshot\":1}"
                    .formatted(file.id(), file.path(), file.records(), file.size()));
            file.deletionVector().ifPresent(path -> entry.append(", \"delete_file\":{\"delete_file_id\":%d, \"data_file_id\":%d, \"path\":\"%s\", \"file_format\":\"puffin-dv\", \"delete_count\":1, \"file_size_bytes\":100, \"begin_snapshot\":1}"
                    .formatted(file.id(), file.id(), path)));
            if (file.named()) {
                entry.append(", \"team_id\":").append(file.team());
            }
            entries.add(entry.append('}').toString());
        }
        return "[" + String.join(",", entries) + "]";
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
}
