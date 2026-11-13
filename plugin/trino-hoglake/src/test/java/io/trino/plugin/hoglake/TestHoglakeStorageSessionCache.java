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
import io.trino.plugin.hoglake.testing.FakeCredentialService;
import io.trino.plugin.hoglake.testing.FakeCredentialService.SessionRequest;
import io.trino.plugin.hoglake.testing.LogCapture;
import io.trino.plugin.hoglake.testing.ManualClock;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_REFUSED;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.BODY_MARKER;
import static io.trino.plugin.hoglake.testing.FakeCredentialService.CLIENT_SECRET;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The coordinator's cache of S3 sessions for the posthog schema, with the real
 * client against a credential service that charges a request bucket of 10 a
 * second with a burst of 20 and answers 429 with {@code Retry-After}. The
 * service and the cache share a clock that moves only when the cache waits.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeStorageSessionCache
{
    private static final String ORG = "0b5f0c2e-1111-4222-8333-444455556666";
    private static final String MOUNT = ORG + "-events";
    private static final String ALICE = "wh1.alice";
    private static final String BOB = "wh1.bob";
    private static final String CLIENT_ID = "client-cache";
    private static final Instant START = Instant.parse("2026-11-13T00:00:00Z");
    private static final String TEAM_REFUSED = "Access Denied: Cannot read every team's events with this login";
    private static final String BUSY = "The credential service for the posthog schema is busy or unavailable; retry later";
    private static final String UNREACHABLE = "The credential service for the posthog schema cannot be reached; retry later";
    private static final String NOT_OFFERED = "The posthog schema cannot be read: the credential service does not issue storage sessions for it";
    private static final String INVALID = "The credential service for the posthog schema answered outside its contract";

    private final ManualClock clock = new ManualClock(START);
    private FakeCredentialService service;
    private HttpClient httpClient;
    private ExecutorService callers;
    private LogCapture logs;
    private HoglakeStorageSessionClient client;
    private HoglakeStorageSessionStats stats;
    private HoglakeStorageSessions sessions;

    @BeforeAll
    void setUp()
            throws IOException
    {
        service = new FakeCredentialService(MOUNT, clock::millis);
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        client = new HoglakeStorageSessionClient(service.credentialsUri(), MOUNT, httpClient, clock);
        callers = Executors.newCachedThreadPool();
        logs = new LogCapture();
    }

    @AfterAll
    void tearDown()
    {
        logs.close();
        callers.shutdownNow();
        httpClient.shutdownNow();
        service.close();
    }

    /**
     * Every test starts with an empty cache, no token and a full request bucket.
     */
    @BeforeEach
    void reset()
    {
        clock.set(START.toEpochMilli());
        service.reset();
        stats = new HoglakeStorageSessionStats();
        sessions = sessions(client, clock::advanceTo, HoglakeStorageSessions.MERGE_WINDOW);
    }

    @AfterEach
    void verifyNothingSecretWasLogged()
    {
        sessions.close();
        assertThat(logs.logged()).allSatisfy(text -> assertThat(text).doesNotContain(secrets()));
    }

    /**
     * A cache whose waits pass by {@code waiter}; the default moves the clock and returns at once.
     */
    private HoglakeStorageSessions sessions(HoglakeStorageSessionClient client, HoglakeStorageSessions.Waiter waiter, Duration mergeWindow)
    {
        HoglakeLoginTokenSource tokens = HoglakePosthogSchema.storageSessionTokens(service.tokenUri(), CLIENT_ID, CLIENT_SECRET, httpClient);
        return new HoglakeStorageSessions(MOUNT, client, tokens, stats, clock, waiter, mergeWindow);
    }

    @Test
    void testQueriesOfOneLoginShareASession()
    {
        Map<Long, HoglakeStorageSession> first = sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 7L));
        assertThat(first.keySet()).containsExactlyInAnyOrder(4L, 7L);
        // Another query of the login, through another view of it
        Map<Long, HoglakeStorageSession> second = sessions.forLogin(ALICE).sessions(ImmutableSet.of(7L));
        assertThat(second).containsExactly(Map.entry(7L, first.get(7L)));
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 7L))).isEqualTo(first);
        assertThat(service.sessionRequests()).hasSize(1);
        assertThat(stats.getRequests()).isEqualTo(1);
        assertThat(stats.getSessionsIssued()).isEqualTo(2);
        assertThat(stats.getSessionsReused()).isEqualTo(3);
        assertThat(sessions.cachedEntries()).isEqualTo(2);
        // A team the login does not hold yet is asked alone
        sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 9L));
        assertThat(service.sessionRequests()).hasSize(2).last().satisfies(request -> assertThat(request.teams()).containsExactly(9L));
        assertThat(String.valueOf(first) + sessions + sessions.forLogin(ALICE)).doesNotContain(secrets());
    }

    @Test
    void testTwoLoginsNeverShareASession()
            throws Exception
    {
        // Logins that differ only in case or in whitespace are different logins
        List<String> logins = List.of(ALICE, BOB, "wh1.Alice", "WH1.ALICE", " wh1.alice", "wh1.alice ", "wh1. alice", "wh1.alice\t");
        for (String login : logins) {
            HoglakeStorageSession session = sessions.forLogin(login).sessions(ImmutableSet.of(4L)).get(4L);
            assertThat(service.issuedFor(session.accessKeyId())).as(login).hasValueSatisfying(issued -> assertThat(issued.login()).isEqualTo(login));
        }
        assertThat(service.sessionRequests()).hasSize(logins.size());
        assertThat(service.sessionRequests()).extracting(request -> request.login().orElseThrow()).containsExactlyElementsOf(logins);
        assertThat(service.mints()).isEqualTo(logins.size());
        assertThat(sessions.cachedEntries()).isEqualTo(logins.size());

        // Interleaved and concurrent, on held and on new teams: every session was issued for the login that asked
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> results = new ArrayList<>();
        for (int caller = 0; caller < 32; caller++) {
            String login = logins.get(caller % logins.size());
            Set<Long> teams = ImmutableSet.of(4L, 100L + caller % 3);
            results.add(callers.submit(() -> {
                start.await();
                sessions.forLogin(login).sessions(teams).forEach((team, session) ->
                        assertThat(service.issuedFor(session.accessKeyId())).hasValueSatisfying(issued -> {
                            assertThat(issued.login()).isEqualTo(login);
                            assertThat(issued.team()).isEqualTo(team);
                        }));
                return null;
            }));
        }
        start.countDown();
        for (Future<Void> result : results) {
            result.get(60, SECONDS);
        }
        // Every request carried the token of the login whose teams it asked for
        assertThat(service.sessionRequests()).allSatisfy(request -> assertThat(request.status()).isEqualTo(200));
        assertThat(service.mints()).isEqualTo(logins.size() * 4);
    }

    @Test
    void testOneRequestAtATimeForALoginAndTeam()
            throws Exception
    {
        CountDownLatch hold = new CountDownLatch(1);
        service.holdSessions(hold);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<HoglakeStorageSession>> results = new ArrayList<>();
        for (int caller = 0; caller < 16; caller++) {
            results.add(callers.submit(() -> {
                start.await();
                return sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)).get(4L);
            }));
        }
        start.countDown();
        // The one request is at the service, and every caller waits for it
        long deadline = System.nanoTime() + SECONDS.toNanos(30);
        while (service.sessionRequestsStarted() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(200);
        assertThat(service.sessionRequestsStarted()).isEqualTo(1);
        assertThat(results).noneMatch(Future::isDone);
        hold.countDown();
        Set<HoglakeStorageSession> issued = ImmutableSet.of(results.getFirst().get(60, SECONDS));
        for (Future<HoglakeStorageSession> result : results) {
            assertThat(result.get(60, SECONDS)).isIn(issued);
        }
        assertThat(service.sessionRequests()).hasSize(1);
        assertThat(service.mints()).isEqualTo(1);
    }

    @Test
    void testAtMostThreeRequestsAreInFlight()
            throws Exception
    {
        CountDownLatch hold = new CountDownLatch(1);
        service.holdSessions(hold);
        // Sixty teams: six requests
        Set<Long> sixty = LongStream.rangeClosed(1, 60).boxed().collect(toImmutableSet());
        Future<Map<Long, HoglakeStorageSession>> result = callers.submit(() -> sessions.forLogin(ALICE).sessions(sixty));
        long deadline = System.nanoTime() + SECONDS.toNanos(30);
        while (service.sessionRequestsStarted() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(300);
        // Three are at the service; the others wait for one of them to be answered
        assertThat(service.sessionRequestsStarted()).isEqualTo(3);
        assertThat(result).isNotDone();
        hold.countDown();
        assertThat(result.get(60, SECONDS).keySet()).isEqualTo(sixty);
        assertThat(service.sessionRequests()).hasSize(6).allSatisfy(request -> assertThat(request.teams()).hasSize(10));
    }

    @Test
    void testARefusedTeamIsRememberedForAMinute()
    {
        Map<Long, HoglakeStorageSession> held = sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L));
        service.refuseTeam(ALICE, 5);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(5L)), PERMISSION_DENIED, TEAM_REFUSED);
        assertThat(service.sessionRequests()).extracting(SessionRequest::status).containsExactly(200, 403);

        // The login keeps its other teams, and another login the team
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L))).isEqualTo(held);
        assertThat(sessions.forLogin(BOB).sessions(ImmutableSet.of(5L)).keySet()).containsExactly(5L);
        assertThat(service.sessionRequests()).hasSize(3);

        // No request while the refusal is remembered, alone or with other teams, even when the service would now agree
        service.allowTeam(ALICE, 5);
        clock.advance(Duration.ofSeconds(59));
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(5L)), PERMISSION_DENIED, TEAM_REFUSED);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 5L, 6L)), PERMISSION_DENIED, TEAM_REFUSED);
        assertThat(service.sessionRequests()).hasSize(3);
        assertThat(stats.getRefusals()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(5L)).keySet()).containsExactly(5L);
        assertThat(service.sessionRequests()).hasSize(4);
    }

    /**
     * The service refuses a whole request for one team and does not say which.
     * The query that asked fails; the teams the service does issue are found by
     * asking each alone, so another query of the login is not failed by this one.
     */
    @Test
    void testARefusedRequestOfSeveralTeamsFailsOnlyTheRefusedTeams()
    {
        service.refuseTeam(ALICE, 5);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 5L, 6L)), PERMISSION_DENIED, TEAM_REFUSED);
        // The files of the refused team are never skipped: the same ask fails again
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 5L, 6L)), PERMISSION_DENIED, TEAM_REFUSED);
        // The query failed as soon as its refused team was known; the other teams were still asked, each once
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L, 6L)).keySet()).containsExactlyInAnyOrder(4L, 6L);
        assertThat(service.sessionRequests()).extracting(SessionRequest::teams)
                .containsExactly(List.of(4L, 5L, 6L), List.of(4L), List.of(5L), List.of(6L));
        assertThat(service.sessionRequests()).extracting(SessionRequest::status).containsExactly(403, 200, 403, 200);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(5L)), PERMISSION_DENIED, TEAM_REFUSED);
        assertThat(service.sessionRequests()).hasSize(4);
    }

    @Test
    void testARefusalOfTheServiceIsRememberedForEveryLogin()
    {
        // The service does not issue sessions for the mount
        service.setOffered(false);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_STORAGE_SESSION_REFUSED, NOT_OFFERED);
        service.setOffered(true);
        clock.advance(Duration.ofSeconds(59));
        assertFails(() -> sessions.forLogin(BOB).sessions(ImmutableSet.of(7L)), HOGLAKE_STORAGE_SESSION_REFUSED, NOT_OFFERED);
        assertThat(service.sessionRequests()).hasSize(1);
        clock.advance(Duration.ofSeconds(1));
        assertThat(sessions.forLogin(BOB).sessions(ImmutableSet.of(7L)).keySet()).containsExactly(7L);

        // It refuses the request itself
        service.answerNext(new FakeCredentialService.Answer(400, ImmutableMap.of(), "{\"error\":\"invalid_request\",\"error_description\":\"" + BODY_MARKER + "\"}"));
        String refusedRequest = "The posthog schema cannot be read: the credential service refused the request";
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_STORAGE_SESSION_REFUSED, refusedRequest);
        assertFails(() -> sessions.forLogin(BOB).sessions(ImmutableSet.of(8L)), HOGLAKE_STORAGE_SESSION_REFUSED, refusedRequest);
        // What a login holds is still handed out
        assertThat(sessions.forLogin(BOB).sessions(ImmutableSet.of(7L)).keySet()).containsExactly(7L);
        assertThat(service.sessionRequests()).hasSize(3);

        // It refuses the login's token twice: remembered for that login and team only
        clock.advance(Duration.ofSeconds(60));
        service.refillRequestBuckets();
        FakeCredentialService.Answer unauthorized = new FakeCredentialService.Answer(401, ImmutableMap.of(), "{\"error\":\"invalid_token\"}");
        service.answerNext(unauthorized, unauthorized);
        String refusedToken = "The posthog schema cannot be read: the credential service refused this catalog's token";
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_STORAGE_SESSION_REFUSED, refusedToken);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_STORAGE_SESSION_REFUSED, refusedToken);
        assertThat(service.sessionRequests()).hasSize(5);
        assertThat(sessions.forLogin(BOB).sessions(ImmutableSet.of(4L)).keySet()).containsExactly(4L);
    }

    @Test
    void testASessionIsHandedOutOnlyWithTenMinutesLeft()
    {
        HoglakeStorageSessions.LoginSessions alice = sessions.forLogin(ALICE);
        HoglakeStorageSession first = alice.sessions(ImmutableSet.of(4L)).get(4L);
        assertThat(first.expirationEpochMillis() - clock.millis()).isEqualTo(Duration.ofMinutes(15).toMillis());

        // Exactly ten minutes left: handed out again, without a request
        clock.set(first.expirationEpochMillis() - Duration.ofMinutes(10).toMillis());
        assertThat(alice.sessions(ImmutableSet.of(4L)).get(4L)).isEqualTo(first);
        assertThat(service.sessionRequests()).hasSize(1);

        // One millisecond less: a session that dropped below the floor is replaced
        clock.advance(Duration.ofMillis(1));
        HoglakeStorageSession second = alice.sessions(ImmutableSet.of(4L)).get(4L);
        assertThat(service.sessionRequests()).hasSize(2);
        assertThat(second.accessKeyId()).isNotEqualTo(first.accessKeyId());
        assertThat(second.expirationEpochMillis() - clock.millis()).isGreaterThanOrEqualTo(Duration.ofMinutes(10).toMillis());
        assertThat(alice.sessions(ImmutableSet.of(4L)).get(4L)).isEqualTo(second);
        assertThat(service.sessionRequests()).hasSize(2);

        // A scan longer than a session: no hand-out ever has less than ten minutes
        for (int minute = 0; minute < 60; minute++) {
            clock.advance(Duration.ofMinutes(1));
            service.refillRequestBuckets();
            HoglakeStorageSession current = alice.sessions(ImmutableSet.of(4L)).get(4L);
            assertThat(current.expirationEpochMillis() - clock.millis()).isGreaterThanOrEqualTo(Duration.ofMinutes(10).toMillis());
        }
        // About one request each five minutes
        assertThat(service.sessionRequests().size()).isBetween(12, 15);
        // A session is forgotten when it has expired
        clock.advance(Duration.ofMinutes(15));
        assertThat(sessions.cachedEntries()).isZero();
    }

    /**
     * This node's clock runs ahead of the service's. The service then returns
     * a session that this node counts less than ten minutes for. It is
     * accepted with nine, and the service is not asked again for five seconds.
     */
    @Test
    void testAnAnswerIsAcceptedWithNineMinutesAndNotAskedAgainAtOnce()
    {
        service.setClockBehind(Duration.ofSeconds(30).toMillis());
        HoglakeStorageSessions.LoginSessions alice = sessions.forLogin(ALICE);
        HoglakeStorageSession first = alice.sessions(ImmutableSet.of(4L)).get(4L);
        assertThat(first.expirationEpochMillis() - clock.millis()).isEqualTo(Duration.ofSeconds(14 * 60 + 30).toMillis());

        // 9 minutes 45 seconds left here, 10 minutes 15 seconds at the service: asked, and the same session comes back
        clock.set(first.expirationEpochMillis() - Duration.ofSeconds(9 * 60 + 45).toMillis());
        assertThat(alice.sessions(ImmutableSet.of(4L)).get(4L)).isEqualTo(first);
        int asked = service.sessionRequests().size();
        assertThat(asked).isEqualTo(2);
        // Within five seconds of that answer the session is handed out again without a request
        clock.advance(Duration.ofMillis(4_900));
        assertThat(alice.sessions(ImmutableSet.of(4L)).get(4L)).isEqualTo(first);
        assertThat(service.sessionRequests()).hasSize(asked);
        // and then the service is asked again
        clock.advance(Duration.ofMillis(200));
        assertThat(alice.sessions(ImmutableSet.of(4L)).get(4L)).isEqualTo(first);
        assertThat(service.sessionRequests()).hasSize(asked + 1);
        assertThat(service.mints()).isEqualTo(1);

        // A clock two minutes ahead: the service still returns a session that has less than nine minutes here
        HoglakeStorageSessions.LoginSessions bob = sessions.forLogin(BOB);
        service.setClockBehind(Duration.ofMinutes(2).toMillis());
        service.refillRequestBuckets();
        clock.set(START.plus(Duration.ofHours(1)).toEpochMilli());
        HoglakeStorageSession thirteenMinutes = bob.sessions(ImmutableSet.of(4L)).get(4L);
        assertThat(thirteenMinutes.expirationEpochMillis() - clock.millis()).isEqualTo(Duration.ofMinutes(13).toMillis());
        // 8 minutes 30 seconds left here, 10 minutes 30 seconds at the service
        clock.set(thirteenMinutes.expirationEpochMillis() - Duration.ofSeconds(8 * 60 + 30).toMillis());
        int before = service.sessionRequests().size();
        assertFails(() -> bob.sessions(ImmutableSet.of(4L)), HOGLAKE_INVALID_RESPONSE, INVALID);
        assertThat(service.sessionRequests()).hasSize(before + 1);
        // Not asked again at once either
        assertFails(() -> bob.sessions(ImmutableSet.of(4L)), HOGLAKE_INVALID_RESPONSE, INVALID);
        assertThat(service.sessionRequests()).hasSize(before + 1);
        clock.advance(Duration.ofSeconds(5));
        assertFails(() -> bob.sessions(ImmutableSet.of(4L)), HOGLAKE_INVALID_RESPONSE, INVALID);
        assertThat(service.sessionRequests()).hasSize(before + 2);
    }

    @Test
    void testAsksOfOneLoginGoOutTogether()
            throws Exception
    {
        // One ask for thirty teams, in the order 1, 11, 21, 2, 12, 22 and so on: three requests of ten, the teams ascending
        Set<Long> thirty = LongStream.range(0, 30).map(index -> index % 3 * 10 + index / 3 + 1).boxed().collect(toImmutableSet());
        assertThat(thirty).hasSize(30).startsWith(1L, 11L, 21L, 2L);
        assertThat(sessions.forLogin(ALICE).sessions(thirty).keySet()).isEqualTo(thirty);
        assertThat(service.sessionRequests()).extracting(SessionRequest::teams).containsExactlyInAnyOrder(
                LongStream.rangeClosed(1, 10).boxed().toList(),
                LongStream.rangeClosed(11, 20).boxed().toList(),
                LongStream.rangeClosed(21, 30).boxed().toList());

        // Asks of several queries that arrive together: one request for each login, never one for two logins
        service.reset();
        try (HoglakeStorageSessions merging = sessions(client, _ -> Thread.sleep(750), Duration.ofMillis(750))) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Map<Long, HoglakeStorageSession>>> results = new ArrayList<>();
            for (Map.Entry<Long, String> ask : ImmutableMap.of(101L, ALICE, 102L, BOB, 103L, ALICE, 104L, BOB, 105L, ALICE).entrySet()) {
                results.add(callers.submit(() -> {
                    start.await();
                    return merging.forLogin(ask.getValue()).sessions(ImmutableSet.of(ask.getKey()));
                }));
            }
            start.countDown();
            for (Future<Map<Long, HoglakeStorageSession>> result : results) {
                assertThat(result.get(60, SECONDS)).hasSize(1);
            }
            assertThat(service.sessionRequests()).hasSize(2);
            assertThat(service.sessionRequests()).filteredOn(request -> request.login().equals(Optional.of(ALICE)))
                    .singleElement().satisfies(request -> assertThat(request.teams()).containsExactly(101L, 103L, 105L));
            assertThat(service.sessionRequests()).filteredOn(request -> request.login().equals(Optional.of(BOB)))
                    .singleElement().satisfies(request -> assertThat(request.teams()).containsExactly(102L, 104L));
        }
    }

    @Test
    void testRequestsArePacedBelowTheServicesBudget()
    {
        // Forty requests, one after the other: the first sixteen at once, the rest at eight a second
        for (long team = 1; team <= 40; team++) {
            sessions.forLogin(ALICE).sessions(ImmutableSet.of(team));
        }
        List<SessionRequest> requests = service.sessionRequests();
        assertThat(requests).hasSize(40).allSatisfy(request -> assertThat(request.status()).isEqualTo(200));
        assertThat(requests.getLast().atMillis() - requests.getFirst().atMillis()).isGreaterThanOrEqualTo(2_900);
        for (int index = 0; index + 17 < requests.size(); index++) {
            // Never more than the burst and a second's worth within one second
            assertThat(requests.get(index + 17).atMillis() - requests.get(index).atMillis()).isGreaterThanOrEqualTo(125);
        }
        // The service never had to ask this node to wait
        assertThat(stats.getWaits()).isZero();
        assertThat(stats.getRequests()).isEqualTo(40);
    }

    @Test
    void testTheServicesRequestToWaitIsHonoured()
    {
        HoglakeLoginTokenSource tokens = HoglakePosthogSchema.storageSessionTokens(service.tokenUri(), CLIENT_ID, CLIENT_SECRET, httpClient);
        // Other coordinators of the same catalog have used up the catalog's request bucket
        for (long team = 1; team <= 20; team++) {
            client.request(tokens.forLogin(BOB), ImmutableSet.of(1000 + team), Duration.ofSeconds(30));
        }
        long before = clock.millis();
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)).keySet()).containsExactly(4L);
        assertThat(service.sessionRequests()).extracting(SessionRequest::status).endsWith(429, 200);
        assertThat(clock.millis() - before).isBetween(1_000L, 2_000L);
        assertThat(stats.getWaits()).isEqualTo(1);

        // A wide scan while the bucket is empty again: every team gets its session
        for (long team = 21; team <= 60; team++) {
            client.request(tokens.forLogin(BOB), ImmutableSet.of(1000 + team), Duration.ofSeconds(30));
        }
        Set<Long> wide = LongStream.rangeClosed(1, 250).boxed().collect(toImmutableSet());
        Map<Long, HoglakeStorageSession> issued = sessions.forLogin(ALICE).sessions(wide);
        assertThat(issued.keySet()).isEqualTo(wide);
        issued.forEach((team, session) -> assertThat(service.issuedFor(session.accessKeyId())).hasValueSatisfying(owner -> {
            assertThat(owner.login()).isEqualTo(ALICE);
            assertThat(owner.team()).isEqualTo(team);
        }));
        assertThat(stats.getWaits()).isGreaterThan(1);
        assertThat(service.sessionRequests()).filteredOn(request -> request.login().equals(Optional.of(ALICE)) && request.status() == 200)
                .hasSize(1 + 25)
                .allSatisfy(request -> assertThat(request.teams().size()).isLessThanOrEqualTo(10));
    }

    @Test
    void testAQueryFailsAfterAMinuteOfWaiting()
            throws IOException
    {
        // The service asks to wait, every time
        service.setUnavailable(true);
        long before = clock.millis();
        TrinoException failure = assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_CATALOG_UNAVAILABLE, BUSY);
        assertThat(failure).hasNoCause();
        assertThat(clock.millis() - before).isBetween(60_000L, 66_000L);
        // Asked again each time the service's wait had passed, for a minute
        assertThat(service.sessionRequests()).hasSizeBetween(11, 13).allSatisfy(request -> assertThat(request.status()).isEqualTo(503));
        // Nothing is remembered: the next query asks, and succeeds when the service does
        service.setUnavailable(false);
        clock.advance(Duration.ofSeconds(10));
        assertThat(sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)).keySet()).containsExactly(4L);

        // Nothing listens
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        HoglakeStorageSessionClient nowhere = new HoglakeStorageSessionClient(URI.create("http://127.0.0.1:" + closedPort + "/oauth/credentials"), MOUNT, httpClient, clock);
        try (HoglakeStorageSessions unreachable = sessions(nowhere, clock::advanceTo, HoglakeStorageSessions.MERGE_WINDOW)) {
            long start = clock.millis();
            assertFails(() -> unreachable.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
            assertThat(clock.millis() - start).isBetween(60_000L, 66_000L);
        }
    }

    @Test
    void testTheCacheIsBounded()
    {
        // 10,500 sessions of one login
        for (long first = 1; first <= 10_500; first += 10) {
            sessions.forLogin(ALICE).sessions(LongStream.range(first, first + 10).boxed().collect(toImmutableSet()));
        }
        assertThat(service.mints()).isEqualTo(10_500);
        assertThat(sessions.cachedEntries()).isBetween(8_000L, 10_000L);
        // A session that was dropped costs one request, which the service answers with the session it still holds
        int requests = service.sessionRequests().size();
        sessions.forLogin(ALICE).sessions(LongStream.rangeClosed(1, 600).boxed().collect(toImmutableSet()));
        assertThat(service.sessionRequests().size()).isGreaterThan(requests);
        assertThat(service.mints()).isEqualTo(10_500);
        assertThat(sessions.cachedEntries()).isLessThanOrEqualTo(10_000L);
    }

    @Test
    void testALoginWithoutATokenAsksNothing()
    {
        try (HoglakeStorageSessions unconfigured = new HoglakeStorageSessions(MOUNT, client, HoglakeLoginTokenSource.UNCONFIGURED, stats, clock, clock::advanceTo, HoglakeStorageSessions.MERGE_WINDOW)) {
            assertFails(() -> unconfigured.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: this catalog has no token service configured");
        }
        // The token service refuses the login
        service.setExchangeOffered(false);
        assertFails(() -> sessions.forLogin(ALICE).sessions(ImmutableSet.of(4L)), HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service does not issue per-login tokens");
        // A login the token service could not be asked about
        assertFails(() -> sessions.forLogin("").sessions(ImmutableSet.of(4L)), PERMISSION_DENIED, "Access Denied: Cannot read the posthog schema with this login");
        assertThat(service.sessionRequests()).isEmpty();

        // A closed catalog asks nothing and does not hang
        sessions.close();
        assertFails(() -> sessions.forLogin(BOB).sessions(ImmutableSet.of(4L)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
        assertThat(service.sessionRequests()).isEmpty();
    }

    private static TrinoException assertFails(ThrowingCallable call, ErrorCodeSupplier errorCode, String message)
    {
        TrinoException failure = catchThrowableOfType(TrinoException.class, call);
        assertThat(failure).as("no failure").isNotNull();
        assertThat(failure.getErrorCode()).isEqualTo(errorCode.toErrorCode());
        assertThat(failure).hasMessage(message).hasNoCause();
        StringWriter text = new StringWriter();
        failure.printStackTrace(new PrintWriter(text));
        assertThat(text.toString()).doesNotContain(BODY_MARKER, ALICE, BOB, CLIENT_SECRET);
        return failure;
    }

    // Everything that must appear in no log line
    private String[] secrets()
    {
        List<String> secrets = new ArrayList<>(List.of(
                BODY_MARKER,
                CLIENT_SECRET,
                ALICE,
                BOB,
                service.uri(),
                Base64.getEncoder().encodeToString((CLIENT_ID + ":" + CLIENT_SECRET).getBytes(UTF_8))));
        secrets.addAll(service.allIssuedTokens());
        secrets.addAll(service.sessionValues());
        return secrets.toArray(String[]::new);
    }
}
