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
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Answer;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Issued;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Refusal;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Refused;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.plugin.hoglake.testing.FakeCredentialService;
import io.trino.plugin.hoglake.testing.FakeCredentialService.SessionRequest;
import io.trino.plugin.hoglake.testing.FakeCredentialService.TokenRequest;
import io.trino.plugin.hoglake.testing.LogCapture;
import io.trino.plugin.hoglake.testing.ManualClock;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_REFUSED;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.BODY_MARKER;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.BUCKET;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.CLIENT_SECRET;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.MOUNT_TOKEN;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.SESSION_TOKEN;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.answerJson;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.sessionJson;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.teamPrefix;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The client for the posthog schema's S3 sessions, against a credential service
 * that accepts only a token exchanged for its own audience and records every
 * request: the request it sends, the token it sends it with, what it makes of
 * every refusal, and that it uses an answer only when it is what was asked.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeStorageSessionClient
{
    private static final String ORG = "0b5f0c2e-1111-4222-8333-444455556666";
    private static final String MOUNT = ORG + "-events";
    private static final String ALICE = "wh1.alice";
    private static final String CLIENT_ID = "client-sessions";
    private static final Instant NOW = Instant.parse("2026-11-13T00:00:00Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String ACCESS_KEY = "ASIACANNEDACCESSKEY";
    private static final String SECRET_KEY = "canned-secret-access-key";
    private static final String SESSION_VALUE = "canned-session-token";
    private static final String IN_TIME = "2026-11-13T00:15:00Z";

    private final ManualClock clock = new ManualClock(NOW);
    private FakeCredentialService service;
    private HttpClient httpClient;
    private HoglakeLoginTokenSource loginTokens;
    private HoglakeStorageSessionClient client;
    private LogCapture logs;

    @BeforeAll
    void setUp()
            throws IOException
    {
        service = new FakeCredentialService(MOUNT, clock::millis);
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        client = new HoglakeStorageSessionClient(service.credentialsUri(), MOUNT, httpClient, clock);
        logs = new LogCapture();
    }

    @AfterAll
    void tearDown()
    {
        logs.close();
        httpClient.shutdownNow();
        service.close();
    }

    @BeforeEach
    void reset()
    {
        clock.set(NOW.toEpochMilli());
        service.reset();
        // Nothing is kept from another test: every test starts with no token
        loginTokens = HoglakePosthogSchema.storageSessionTokens(service.tokenUri(), CLIENT_ID, CLIENT_SECRET, httpClient);
    }

    /**
     * Checked after every test: nothing logged names a token, a session value,
     * the catalog's secret, a body or the login.
     */
    @AfterEach
    void verifyNothingSecretWasLogged()
    {
        assertThat(logs.logged()).allSatisfy(text -> assertThat(text).doesNotContain(secrets()));
    }

    @Test
    void testRequestIsExactlyWhatTheServiceAccepts()
    {
        Answer answer = client.request(loginTokens.forLogin(ALICE), ImmutableSet.of(42L, 4L, 7L), TIMEOUT);

        assertThat(answer).isInstanceOf(Issued.class);
        Map<Long, HoglakeStorageSession> sessions = ((Issued) answer).sessions();
        assertThat(sessions.keySet()).containsExactlyInAnyOrder(4L, 7L, 42L);
        sessions.forEach((team, session) -> {
            assertThat(session.teamId()).isEqualTo(team);
            assertThat(session.prefix()).isEqualTo("s3://events-bucket/data/ingest/events/team_id=%d/".formatted(team));
            assertThat(session.expirationEpochMillis()).isEqualTo(NOW.plus(Duration.ofMinutes(15)).toEpochMilli());
            assertThat(service.issuedFor(session.accessKeyId())).hasValueSatisfying(issued -> {
                assertThat(issued.login()).isEqualTo(ALICE);
                assertThat(issued.team()).isEqualTo(team);
            });
        });

        SessionRequest request = service.sessionRequests().stream().collect(onlyElement());
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.headers()).containsEntry("content-type", "application/json").containsEntry("accept", "application/json");
        // Exactly three fields, the teams ascending
        assertThat(request.body()).isEqualTo("{\"catalog\":\"%s\",\"access\":\"read\",\"values\":[4,7,42]}".formatted(MOUNT));
        // The login's token for the service's audience, and no other
        String token = service.issuedTokens().stream().collect(onlyElement());
        assertThat(token).startsWith(SESSION_TOKEN);
        assertThat(request.headers()).containsEntry("authorization", "Bearer " + token);
        assertThat(request.login()).contains(ALICE);

        TokenRequest tokenRequest = service.tokenRequests().stream().collect(onlyElement());
        assertThat(tokenRequest.isExchange()).isTrue();
        assertThat(tokenRequest.clientId()).isEqualTo(CLIENT_ID);
        assertThat(tokenRequest.subject()).isEqualTo(ALICE);
        assertThat(tokenRequest.subjectTokenType()).isEqualTo("urn:posthog:params:oauth:token-type:trino-login");
        assertThat(tokenRequest.audience()).isEqualTo("hogtower-vend");
        // No scope and no values: the service decides what the token may ask for
        assertThat(tokenRequest.scope()).isNull();
        assertThat(tokenRequest.fields()).containsExactlyInAnyOrder("grant_type", "subject_token", "subject_token_type", "audience");

        // The token is kept for the login: a second request mints none
        assertThat(client.request(loginTokens.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT)).isInstanceOf(Issued.class);
        assertThat(service.tokenRequests()).hasSize(1);
        assertThat(String.valueOf(answer) + client).doesNotContain(secrets());
    }

    @Test
    void testARequestNamesOneToTenTeams()
    {
        HoglakeTokenSource tokens = loginTokens.forLogin(ALICE);
        Set<Long> ten = LongStream.rangeClosed(1, 10).boxed().collect(toImmutableSet());
        assertThat(client.request(tokens, ten, TIMEOUT)).isInstanceOfSatisfying(Issued.class, issued -> assertThat(issued.sessions()).hasSize(10));
        assertThat(service.sessionRequests()).hasSize(1);

        Set<Long> eleven = LongStream.rangeClosed(1, 11).boxed().collect(toImmutableSet());
        for (Set<Long> teams : List.<Set<Long>>of(eleven, ImmutableSet.of(), ImmutableSet.of(0L, 4L), ImmutableSet.of(-4L))) {
            assertThatThrownBy(() -> client.request(tokens, teams, TIMEOUT)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(service.sessionRequests()).hasSize(1);
    }

    @Test
    void testEveryRefusalIsOneFixedError()
    {
        record Expected(int status, Map<String, String> headers, String body, Refusal refusal, Duration retryAfter, ErrorCodeSupplier errorCode, String message) {}

        String busy = "The credential service for the posthog schema is busy or unavailable; retry later";
        String refusedRequest = "The posthog schema cannot be read: the credential service refused the request";
        String notOffered = "The posthog schema cannot be read: the credential service does not issue storage sessions for it";
        String invalid = "The credential service for the posthog schema answered outside its contract";
        List<Expected> cases = List.of(
                new Expected(403, ImmutableMap.of(), error("insufficient_scope"), Refusal.TEAM_REFUSED, Duration.ZERO, PERMISSION_DENIED, "Access Denied: Cannot read every team's events with this login"),
                new Expected(404, ImmutableMap.of(), error("not_found"), Refusal.NOT_OFFERED, Duration.ZERO, HOGLAKE_STORAGE_SESSION_REFUSED, notOffered),
                // A listener that does not serve the route at all
                new Expected(404, ImmutableMap.of(), "404 page not found " + BODY_MARKER, Refusal.NOT_OFFERED, Duration.ZERO, HOGLAKE_STORAGE_SESSION_REFUSED, notOffered),
                new Expected(400, ImmutableMap.of(), error("invalid_request"), Refusal.REQUEST_REFUSED, Duration.ZERO, HOGLAKE_STORAGE_SESSION_REFUSED, refusedRequest),
                new Expected(405, ImmutableMap.of(), "", Refusal.REQUEST_REFUSED, Duration.ZERO, HOGLAKE_STORAGE_SESSION_REFUSED, refusedRequest),
                new Expected(429, ImmutableMap.of("Retry-After", "7"), error("slow_down"), Refusal.BUSY, Duration.ofSeconds(7), HOGLAKE_CATALOG_UNAVAILABLE, busy),
                new Expected(429, ImmutableMap.of("Retry-After", "500"), error("slow_down"), Refusal.BUSY, Duration.ofSeconds(30), HOGLAKE_CATALOG_UNAVAILABLE, busy),
                new Expected(429, ImmutableMap.of("Retry-After", "soon"), error("slow_down"), Refusal.BUSY, Duration.ofSeconds(5), HOGLAKE_CATALOG_UNAVAILABLE, busy),
                new Expected(503, ImmutableMap.of("Retry-After", "5"), error("temporarily_unavailable"), Refusal.BUSY, Duration.ofSeconds(5), HOGLAKE_CATALOG_UNAVAILABLE, busy),
                new Expected(500, ImmutableMap.of(), "", Refusal.BUSY, Duration.ofSeconds(5), HOGLAKE_CATALOG_UNAVAILABLE, busy),
                new Expected(302, ImmutableMap.of("Location", "http://127.0.0.1:9/elsewhere"), "", Refusal.INVALID_RESPONSE, Duration.ZERO, HOGLAKE_INVALID_RESPONSE, invalid),
                new Expected(204, ImmutableMap.of(), "", Refusal.INVALID_RESPONSE, Duration.ZERO, HOGLAKE_INVALID_RESPONSE, invalid));
        for (Expected expected : cases) {
            service.answerNext(new FakeCredentialService.Answer(expected.status(), expected.headers(), expected.body()));
            Answer answer = client.request(loginTokens.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT);
            assertThat(answer).as("HTTP %s", expected.status()).isEqualTo(new Refused(expected.refusal(), expected.retryAfter()));
            TrinoException failure = expected.refusal().failure();
            assertThat(failure.getErrorCode()).isEqualTo(expected.errorCode().toErrorCode());
            assertThat(failure).hasMessage(expected.message()).hasNoCause();
            assertThat(stackTrace(failure) + answer).doesNotContain(secrets());
        }
        assertThat(service.sessionRequests()).hasSize(cases.size());
        // The two that are told apart by nothing but their text
        assertThat(Refusal.UNREACHABLE.failure()).hasMessage("The credential service for the posthog schema cannot be reached; retry later");
        assertThat(Refusal.TOKEN_REFUSED.failure()).hasMessage("The posthog schema cannot be read: the credential service refused this catalog's token");
    }

    @Test
    void testRefusedTokenIsReplacedOnceAndNeverTwice()
    {
        HoglakeTokenSource tokens = loginTokens.forLogin(ALICE);
        assertThat(client.request(tokens, ImmutableSet.of(4L), TIMEOUT)).isInstanceOf(Issued.class);
        String first = service.issuedTokens().getLast();

        // The service stops accepting the token: it is dropped, and the request goes out again with a new one
        service.rejectToken(first);
        assertThat(client.request(tokens, ImmutableSet.of(4L), TIMEOUT)).isInstanceOf(Issued.class);
        assertThat(service.issuedTokens()).hasSize(2);
        String second = service.issuedTokens().getLast();
        assertThat(service.sessionRequests()).extracting(SessionRequest::status).containsExactly(200, 401, 200);
        assertThat(service.sessionRequests()).extracting(request -> request.headers().get("authorization"))
                .containsExactly("Bearer " + first, "Bearer " + first, "Bearer " + second);

        // A new token that is refused too: no third request
        service.rejectToken(second);
        service.answerNext(new FakeCredentialService.Answer(401, ImmutableMap.of(), error("invalid_token")));
        assertThat(client.request(tokens, ImmutableSet.of(4L), TIMEOUT)).isEqualTo(new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO));
        assertThat(service.issuedTokens()).hasSize(3);
        assertThat(service.sessionRequests()).extracting(SessionRequest::status).containsExactly(200, 401, 200, 401, 401);
    }

    /**
     * The service accepts only a token exchanged for its own audience, so the
     * login's token for the mount gets nothing from it; and a token source
     * that has no token sends nothing.
     */
    @Test
    void testOnlyATokenForTheServicesAudienceGetsSessions()
    {
        HoglakeLoginTokenSource mountTokens = new HoglakeLoginTokenSource.TokenExchange(
                service.tokenUri(),
                CLIENT_ID,
                CLIENT_SECRET,
                "hoglake",
                HoglakePosthogSchema.LOGIN_SUBJECT_TOKEN_TYPE,
                Optional.of(HoglakePosthogSchema.mountScope(MOUNT)),
                httpClient);
        assertThat(client.request(mountTokens.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT)).isEqualTo(new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO));
        assertThat(service.issuedTokens()).hasSize(2).allMatch(token -> token.startsWith(MOUNT_TOKEN));
        assertThat(service.mints()).isZero();

        service.reset();
        assertThatThrownBy(() -> client.request(HoglakeLoginTokenSource.UNCONFIGURED.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED.toErrorCode()))
                .hasMessage("The posthog schema is not available: this catalog has no token service configured");
        // A token that could not be sent as a header value is not sent
        assertThat(client.request(fixedToken("not a\r\ntoken"), ImmutableSet.of(4L), TIMEOUT)).isEqualTo(new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO));
        assertThat(service.sessionRequests()).isEmpty();

        // The token service does not offer the exchange: the token source's own error
        service.setExchangeOffered(false);
        HoglakeLoginTokenSource fresh = HoglakePosthogSchema.storageSessionTokens(service.tokenUri(), CLIENT_ID, CLIENT_SECRET, httpClient);
        assertThatThrownBy(() -> client.request(fresh.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED.toErrorCode()));
        assertThat(service.sessionRequests()).isEmpty();
    }

    @Test
    void testAnAnswerIsUsedOnlyWhenItIsWhatWasAsked()
    {
        String team4 = session(4, teamPrefix(4), IN_TIME);
        String team7 = session(7, teamPrefix(7), IN_TIME);
        // What is accepted
        assertIssued(ImmutableSet.of(4L, 7L), answerJson(BUCKET, team7, team4));
        // Exactly nine minutes left on this node's clock, and fields this version does not know
        assertIssued(ImmutableSet.of(4L), answerJson(BUCKET, session(4, teamPrefix(4), "2026-11-13T00:09:00Z").replace("{\"team_id\"", "{\"later_field\":[1],\"team_id\"")));
        // An offset instead of Z
        assertIssued(ImmutableSet.of(4L), answerJson(BUCKET, session(4, teamPrefix(4), "2026-11-13T02:15:00.123456789+02:00")));

        List<String> refused = new ArrayList<>(List.of(
                // A team that was not asked for, a missing team, a team twice
                answerJson(BUCKET, team4, session(8, teamPrefix(8), IN_TIME)),
                answerJson(BUCKET, team4),
                answerJson(BUCKET, team4, team4),
                answerJson(BUCKET, team4, team7, session(8, teamPrefix(8), IN_TIME)),
                answerJson(BUCKET),
                // A prefix that does not end in the team's own team_id=<id>/
                answerJson(BUCKET, team7, session(4, teamPrefix(7), IN_TIME)),
                answerJson(BUCKET, team7, session(4, teamPrefix(42), IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ingest/events/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ingest/events/team_id=4", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ingest/events/team_id=4/more/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ingest/events/xteam_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ingest/events/team_id=04/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "team_id=4/", IN_TIME).replace("\"prefix\":\"team_id=4/\"", "\"prefix\":4")),
                // A prefix or a bucket outside the shape the service builds
                answerJson(BUCKET, team7, session(4, "data/../events/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data//events/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "/data/events/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/ev ents/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/*/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/%2e%2e/team_id=4/", IN_TIME)),
                // refused by the rule for a team's prefix alone, and by the rule for the location alone
                answerJson(BUCKET, team7, session(4, "data/./events/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/a..b/team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "team_id=4/", IN_TIME)),
                answerJson(BUCKET, team7, session(4, "data/team_id=7/team_id=4/", IN_TIME)),
                answerJson("Events-Bucket", team7, team4),
                answerJson("events-bucket/other", team7, team4),
                answerJson("events*", team7, team4),
                answerJson("", team7, team4),
                answerJson(BUCKET, team7, team4).replace("\"bucket\":\"events-bucket\",", ""),
                // An expiry that has passed, that leaves less than nine minutes, or that is not a time
                answerJson(BUCKET, team7, session(4, teamPrefix(4), "2026-11-12T23:59:59Z")),
                answerJson(BUCKET, team7, session(4, teamPrefix(4), "2026-11-13T00:00:00Z")),
                answerJson(BUCKET, team7, session(4, teamPrefix(4), "2026-11-13T00:08:59.999Z")),
                answerJson(BUCKET, team7, session(4, teamPrefix(4), "in fifteen minutes")),
                answerJson(BUCKET, team7, session(4, teamPrefix(4), IN_TIME).replace("\"expiration\":\"" + IN_TIME + "\"}", "\"expiration\":1794528900}")),
                // A session value that is missing, empty or not printable ASCII
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), "", SECRET_KEY, SESSION_VALUE, IN_TIME)),
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), ACCESS_KEY, "", SESSION_VALUE, IN_TIME)),
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), ACCESS_KEY, SECRET_KEY, "", IN_TIME)),
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), ACCESS_KEY, SECRET_KEY + " x", SESSION_VALUE, IN_TIME)),
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), ACCESS_KEY, SECRET_KEY, SESSION_VALUE + "\\n", IN_TIME)),
                answerJson(BUCKET, team7, sessionJson(4, teamPrefix(4), ACCESS_KEY + "\\u00e9", SECRET_KEY, SESSION_VALUE, IN_TIME)),
                answerJson(BUCKET, team7, team4.replace("\"session_token\":\"" + SESSION_VALUE + "\",", "")),
                // Not the answer's shape at all
                answerJson(BUCKET, team7, team4).replace("\"credentials\":[", "\"credentials\":[[],"),
                answerJson(BUCKET, team7, team4).replace("\"team_id\":4", "\"team_id\":\"4\""),
                answerJson(BUCKET, team7, team4).replace("\"team_id\":4", "\"team_id\":4.5"),
                "{\"region\":\"us-east-1\",\"bucket\":\"events-bucket\",\"credentials\":{}}",
                "[" + answerJson(BUCKET, team7, team4) + "]",
                answerJson(BUCKET, team7, team4).substring(0, 200),
                "null",
                "",
                SECRET_KEY));
        // A body larger than any answer, valid but for its size
        refused.add(answerJson(BUCKET, team7, team4).replace("{\"region\"", "{\"padding\":\"" + "x".repeat(300 * 1024) + "\",\"region\""));

        Set<Long> asked = ImmutableSet.of(4L, 7L);
        for (String body : refused) {
            service.refillRequestBuckets();
            service.answerNext(new FakeCredentialService.Answer(200, ImmutableMap.of(), body));
            Answer answer = client.request(loginTokens.forLogin(ALICE), asked, TIMEOUT);
            assertThat(answer).as(body.substring(0, Math.min(body.length(), 300))).isEqualTo(new Refused(Refusal.INVALID_RESPONSE, Duration.ZERO));
        }
        TrinoException failure = Refusal.INVALID_RESPONSE.failure();
        assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_INVALID_RESPONSE.toErrorCode());
        assertThat(failure).hasMessage("The credential service for the posthog schema answered outside its contract").hasNoCause();
        assertThat(stackTrace(failure)).doesNotContain(secrets());
    }

    @Test
    void testAServiceThatDoesNotAnswer()
            throws IOException
    {
        // Nothing listens
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        HoglakeStorageSessionClient unreachable = new HoglakeStorageSessionClient(URI.create("http://127.0.0.1:" + closedPort + "/oauth/credentials"), MOUNT, httpClient, clock);
        assertThat(unreachable.request(loginTokens.forLogin(ALICE), ImmutableSet.of(4L), TIMEOUT)).isEqualTo(new Refused(Refusal.UNREACHABLE, Duration.ZERO));

        // No time left: nothing is sent
        assertThat(client.request(fixedToken("vend-token-x.y.1"), ImmutableSet.of(4L), Duration.ZERO)).isEqualTo(new Refused(Refusal.UNREACHABLE, Duration.ZERO));
        assertThat(service.sessionRequests()).isEmpty();

        // It accepts the request and does not answer within the caller's time
        CountDownLatch hold = new CountDownLatch(1);
        service.holdSessions(hold);
        try {
            long start = System.nanoTime();
            assertThat(client.request(loginTokens.forLogin(ALICE), ImmutableSet.of(4L), Duration.ofMillis(300))).isEqualTo(new Refused(Refusal.UNREACHABLE, Duration.ZERO));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
        }
        finally {
            hold.countDown();
            service.holdSessions(null);
        }
    }

    private void assertIssued(Set<Long> teams, String body)
    {
        service.answerNext(new FakeCredentialService.Answer(200, ImmutableMap.of(), body));
        assertThat(client.request(loginTokens.forLogin(ALICE), teams, TIMEOUT)).isInstanceOfSatisfying(Issued.class, issued -> {
            assertThat(issued.sessions().keySet()).isEqualTo(teams);
            assertThat(issued.sessions().values()).allSatisfy(session -> {
                assertThat(session.accessKeyId()).isEqualTo(ACCESS_KEY);
                assertThat(session.secretAccessKey()).isEqualTo(SECRET_KEY);
                assertThat(session.sessionToken()).isEqualTo(SESSION_VALUE);
            });
        });
    }

    private static String session(long team, String prefix, String expiration)
    {
        return sessionJson(team, prefix, ACCESS_KEY, SECRET_KEY, SESSION_VALUE, expiration);
    }

    private static String error(String word)
    {
        return "{\"error\":\"%s\", \"error_description\":\"%s\"}".formatted(word, BODY_MARKER);
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

    private static String stackTrace(Throwable throwable)
    {
        StringWriter text = new StringWriter();
        throwable.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    // Everything that must appear in no log line and no message
    private String[] secrets()
    {
        List<String> secrets = new ArrayList<>(List.of(
                ACCESS_KEY,
                SECRET_KEY,
                SESSION_VALUE,
                BODY_MARKER,
                CLIENT_SECRET,
                ALICE,
                service.uri(),
                Base64.getEncoder().encodeToString((CLIENT_ID + ":" + CLIENT_SECRET).getBytes(UTF_8))));
        secrets.addAll(service.allIssuedTokens());
        secrets.addAll(service.sessionValues());
        return secrets.toArray(String[]::new);
    }
}
