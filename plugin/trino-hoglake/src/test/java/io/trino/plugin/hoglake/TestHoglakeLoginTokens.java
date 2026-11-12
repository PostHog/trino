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
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The per-login token exchange against a fake token endpoint that records every
 * request. Each issued token names the subject it was requested for, so a token
 * that reaches another login is visible.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeLoginTokens
{
    private static final String CLIENT_ID = "hlc_trino";
    // Has characters that the form encoding changes
    private static final String CLIENT_SECRET = "synthetic+test/secret";
    private static final String BASIC = "Basic " + Base64.getEncoder().encodeToString("hlc_trino:synthetic%2Btest%2Fsecret".getBytes(UTF_8));
    private static final String SUBJECT_TOKEN_TYPE = "urn:posthog:params:oauth:token-type:trino-login";
    private static final String SCOPE = "hoglake:0b5f0c2e-1111-4222-8333-444455556666-events:read";
    private static final String ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
    private static final String TOKEN_PREFIX = "login-token-";
    // Every subject starts with this, so a subject in a message or a log line is visible
    private static final String ALICE = "wh1.alice";
    private static final String BOB = "wh1.bob";
    // Placed in every refusal body: no message and no log line may carry it
    private static final String BODY_MARKER = "detail-marker-6f1d";

    private static final String LOGIN_REFUSED = "Access Denied: Cannot read the posthog schema with this login";
    private static final String NOT_OFFERED = "The posthog schema is not available: the token service does not issue per-login tokens";
    private static final String CLIENT_NOT_ALLOWED = "The posthog schema is not available: this catalog's credential may not request per-login tokens";
    private static final String CLIENT_REFUSED = "The posthog schema is not available: the token service refused this catalog's credential";
    private static final String REQUEST_REFUSED = "The posthog schema is not available: the token service refused the request";
    private static final String UNCONFIGURED = "The posthog schema is not available: this catalog has no token service configured";
    private static final String BUSY = "The token service for the posthog schema is busy or unavailable; retry later";
    private static final String UNREACHABLE = "The token service for the posthog schema cannot be reached; retry later";
    private static final String INVALID_RESPONSE = "The token service for the posthog schema answered outside its contract";

    private record Field2(String name, String value) {}

    private record TokenRequest(String method, String query, String authorization, String contentType, String accept, String userAgent, List<Field2> form)
    {
        public String field(String name)
        {
            return form.stream().filter(field -> field.name().equals(name)).map(Field2::value).findFirst().orElse(null);
        }
    }

    /**
     * What the endpoint does with one request: answer, wait for the gate first, or
     * drop the connection.
     */
    private record Answer(int status, String body, Optional<String> retryAfter, Optional<String> location, Optional<CountDownLatch> gate, boolean drop)
    {
        public static Answer of(int status, String body)
        {
            return new Answer(status, body, Optional.empty(), Optional.empty(), Optional.empty(), false);
        }

        public static Answer refusal(int status, String error)
        {
            return of(status, "{\"error\":\"%s\", \"error_description\":\"%s http://10.0.0.1:8080/oauth/token %s\"}".formatted(error, BODY_MARKER, ALICE));
        }

        public Answer withRetryAfter(String value)
        {
            return new Answer(status, body, Optional.of(value), location, gate, drop);
        }

        public Answer withLocation(String value)
        {
            return new Answer(status, body, retryAfter, Optional.of(value), gate, drop);
        }

        public Answer after(CountDownLatch latch)
        {
            return new Answer(status, body, retryAfter, location, Optional.of(latch), drop);
        }

        public static Answer dropped()
        {
            return new Answer(0, "", Optional.empty(), Optional.empty(), Optional.empty(), true);
        }
    }

    private static final class ManualTicker
            extends Ticker
    {
        private final AtomicLong nanos = new AtomicLong(1_000_000);

        @Override
        public long read()
        {
            return nanos.get();
        }

        public void advance(Duration duration)
        {
            nanos.addAndGet(duration.toNanos());
        }
    }

    private final List<TokenRequest> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger issued = new AtomicInteger();
    private final List<String> logged = new CopyOnWriteArrayList<>();
    private final List<CountDownLatch> gates = new CopyOnWriteArrayList<>();
    private volatile Function<TokenRequest, Answer> endpoint;
    private ManualTicker ticker;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private ExecutorService callers;
    private HttpClient httpClient;
    private String tokenUri;
    // Held strongly: java.util.logging keeps loggers weakly
    private Logger hoglakeLogger;
    private Level previousLevel;
    private final Handler logHandler = new Handler()
    {
        @Override
        public void publish(LogRecord record)
        {
            StringWriter text = new StringWriter();
            text.append(String.valueOf(record.getMessage()));
            if (record.getParameters() != null) {
                for (Object parameter : record.getParameters()) {
                    text.append(' ').append(String.valueOf(parameter));
                }
            }
            if (record.getThrown() != null) {
                record.getThrown().printStackTrace(new PrintWriter(text, true));
            }
            logged.add(text.toString());
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    };

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = bindInPortRange();
        server.createContext("/", this::handle);
        // One thread per exchange, so a stalled answer does not hold up the next request
        serverExecutor = Executors.newCachedThreadPool();
        server.setExecutor(serverExecutor);
        server.start();
        tokenUri = "http://127.0.0.1:" + server.getAddress().getPort() + "/oauth/token";
        callers = Executors.newCachedThreadPool();
        httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        hoglakeLogger = Logger.getLogger("io.trino.plugin.hoglake");
        previousLevel = hoglakeLogger.getLevel();
        logHandler.setLevel(Level.ALL);
        hoglakeLogger.setLevel(Level.ALL);
        hoglakeLogger.addHandler(logHandler);
    }

    @AfterAll
    void tearDown()
    {
        hoglakeLogger.removeHandler(logHandler);
        hoglakeLogger.setLevel(previousLevel);
        httpClient.shutdownNow();
        callers.shutdownNow();
        server.stop(0);
        serverExecutor.shutdownNow();
    }

    @BeforeEach
    void reset()
    {
        endpoint = this::issue;
        ticker = new ManualTicker();
        requests.clear();
        logged.clear();
        issued.set(0);
    }

    @AfterEach
    void verifyNothingSecretWasLogged()
    {
        gates.forEach(CountDownLatch::countDown);
        gates.clear();
        // Every scenario is checked: no token, credential, subject or answer body in a log line
        for (String line : logged) {
            assertHoldsNoSecret(line);
        }
    }

    @Test
    void testRequestShape()
    {
        assertThat(exchange().forLogin(ALICE).token(Duration.ofSeconds(10))).contains(tokenFor(ALICE, 1));
        assertThat(requests).singleElement().satisfies(request -> {
            assertThat(request.method()).isEqualTo("POST");
            assertThat(request.query()).isNull();
            assertThat(request.authorization()).isEqualTo(BASIC);
            assertThat(request.contentType()).isEqualTo("application/x-www-form-urlencoded");
            assertThat(request.accept()).isEqualTo("application/json");
            assertThat(request.userAgent()).isEqualTo(HoglakeClient.USER_AGENT);
            // Exactly these fields, each once: no values, actor_token, client_id or client_secret
            assertThat(request.form()).containsExactlyInAnyOrder(
                    new Field2("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
                    new Field2("subject_token", ALICE),
                    new Field2("subject_token_type", SUBJECT_TOKEN_TYPE),
                    new Field2("audience", "hoglake"),
                    new Field2("scope", SCOPE));
        });

        // A subject cannot add or change a field
        requests.clear();
        String hostile = "wh1.a&scope=other&subject_token=wh1.bob&values=1 +%26";
        assertThat(exchange().forLogin(hostile).token(Duration.ofSeconds(10))).isPresent();
        assertThat(requests).singleElement().satisfies(request -> {
            assertThat(request.form()).hasSize(5);
            assertThat(request.field("subject_token")).isEqualTo(hostile);
            assertThat(request.field("scope")).isEqualTo(SCOPE);
        });

        // Without a scope the field is not sent
        requests.clear();
        HoglakeLoginTokenSource unscoped = new HoglakeLoginTokenSource.TokenExchange(URI.create(tokenUri), CLIENT_ID, CLIENT_SECRET, "other-audience", SUBJECT_TOKEN_TYPE, Optional.empty(), httpClient, ticker);
        assertThat(unscoped.forLogin(ALICE).token(Duration.ofSeconds(10))).isPresent();
        assertThat(requests).singleElement().satisfies(request -> assertThat(request.form()).containsExactlyInAnyOrder(
                new Field2("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
                new Field2("subject_token", ALICE),
                new Field2("subject_token_type", SUBJECT_TOKEN_TYPE),
                new Field2("audience", "other-audience")));
    }

    @Test
    void testEachLoginReceivesOnlyItsOwnToken()
            throws Exception
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        // Interleaved: A, B, A, B, each through a source made for that call
        List<String> tokens = new ArrayList<>();
        for (String subject : List.of(ALICE, BOB, ALICE, BOB)) {
            tokens.add(exchange.forLogin(subject).token(Duration.ofSeconds(10)).orElseThrow());
        }
        assertThat(tokens.stream().map(TestHoglakeLoginTokens::subjectOf)).containsExactly(ALICE, BOB, ALICE, BOB);
        assertThat(tokens.get(2)).isEqualTo(tokens.get(0));
        assertThat(tokens.get(3)).isEqualTo(tokens.get(1));
        assertThat(requestsFor(ALICE)).isEqualTo(1);
        assertThat(requestsFor(BOB)).isEqualTo(1);
        // A similar name, a prefix and another case are other logins
        for (String subject : List.of(ALICE + "x", "wh1.alic", "wh1.Alice", " " + ALICE, ALICE + " ")) {
            assertThat(subjectOf(exchange.forLogin(subject).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(subject);
        }

        // Concurrent: a third login's mint is stalled while the first two ask
        String carol = "wh1.carol";
        String dave = "wh1.dave";
        CountDownLatch gate = gate();
        endpoint = request -> request.field("subject_token").equals(carol) ? issue(request).after(gate) : issue(request);
        Future<String> stalled = callers.submit(() -> exchange.forLogin(carol).token(Duration.ofSeconds(30)).orElseThrow());
        awaitRequestsFor(carol, 1);
        // Another login does not wait for it, and gets its own token
        assertThat(subjectOf(exchange.forLogin(dave).token(Duration.ofSeconds(5)).orElseThrow())).isEqualTo(dave);
        assertThat(subjectOf(exchange.forLogin(ALICE).token(Duration.ofSeconds(5)).orElseThrow())).isEqualTo(ALICE);
        assertThat(stalled).isNotDone();
        gate.countDown();
        assertThat(subjectOf(stalled.get(10, SECONDS))).isEqualTo(carol);
        assertThat(requestsFor(carol)).isEqualTo(1);
        assertThat(requestsFor(dave)).isEqualTo(1);
        assertThat(requestsFor(ALICE)).isEqualTo(1);

        // Many logins at once, each asked from several threads
        List<String> subjects = new ArrayList<>();
        for (int index = 0; index < 24; index++) {
            subjects.add("wh1.user" + index);
        }
        List<Future<String>> results = new ArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        for (int round = 0; round < 4; round++) {
            for (String subject : subjects) {
                results.add(callers.submit(() -> {
                    start.await();
                    return subject + "=" + exchange.forLogin(subject).token(Duration.ofSeconds(20)).orElseThrow();
                }));
            }
        }
        start.countDown();
        for (Future<String> result : results) {
            String[] pair = result.get(30, SECONDS).split("=", 2);
            assertThat(subjectOf(pair[1])).isEqualTo(pair[0]);
        }
        for (String subject : subjects) {
            assertThat(requestsFor(subject)).as(subject).isEqualTo(1);
        }
    }

    @Test
    void testOneRequestForConcurrentCallersOfOneLogin()
            throws Exception
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        CountDownLatch gate = gate();
        endpoint = request -> request.field("subject_token").equals(ALICE) ? issue(request).after(gate) : issue(request);
        List<Future<String>> results = new ArrayList<>();
        for (int caller = 0; caller < 16; caller++) {
            results.add(callers.submit(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(30)).orElseThrow()));
        }
        awaitRequestsFor(ALICE, 1);
        // The other login is served while the first one's mint is stalled
        assertThat(subjectOf(exchange.forLogin(BOB).token(Duration.ofSeconds(5)).orElseThrow())).isEqualTo(BOB);
        Thread.sleep(300);
        assertThat(requestsFor(ALICE)).isEqualTo(1);
        gate.countDown();
        for (Future<String> result : results) {
            assertThat(result.get(10, SECONDS)).isEqualTo(tokenFor(ALICE, 1));
        }
        assertThat(requestsFor(ALICE)).isEqualTo(1);
    }

    @Test
    void testTokenIsRefreshedBeforeItExpires()
    {
        HoglakeTokenSource alice = exchange().forLogin(ALICE);
        // 900 seconds: refreshed 60 seconds early
        String first = alice.token(Duration.ofSeconds(10)).orElseThrow();
        ticker.advance(Duration.ofSeconds(839));
        assertThat(alice.token(Duration.ofSeconds(10))).contains(first);
        assertThat(requests).hasSize(1);
        ticker.advance(Duration.ofSeconds(2));
        String second = alice.token(Duration.ofSeconds(10)).orElseThrow();
        assertThat(second).isNotEqualTo(first);
        assertThat(subjectOf(second)).isEqualTo(ALICE);
        assertThat(requests).hasSize(2);

        // 100 seconds: refreshed a fifth of the lifetime early, not 60 seconds
        endpoint = request -> issue(request, 100);
        HoglakeTokenSource bob = exchange().forLogin(BOB);
        String shortLived = bob.token(Duration.ofSeconds(10)).orElseThrow();
        ticker.advance(Duration.ofSeconds(79));
        assertThat(bob.token(Duration.ofSeconds(10))).contains(shortLived);
        ticker.advance(Duration.ofSeconds(2));
        assertThat(bob.token(Duration.ofSeconds(10)).orElseThrow()).isNotEqualTo(shortLived);
        assertThat(requestsFor(BOB)).isEqualTo(2);

        // A lifetime beyond any clock arithmetic is still a usable token
        endpoint = request -> issue(request, Long.MAX_VALUE);
        assertThat(exchange().forLogin("wh1.carol").token(Duration.ofSeconds(10))).isPresent();
    }

    @Test
    void testInvalidateDropsOnlyTheSameLoginsSameToken()
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        String aliceToken = exchange.forLogin(ALICE).token(Duration.ofSeconds(10)).orElseThrow();
        String bobToken = exchange.forLogin(BOB).token(Duration.ofSeconds(10)).orElseThrow();

        // Another login's token, an unknown token and an unknown login change nothing
        exchange.forLogin(ALICE).invalidate(bobToken);
        exchange.forLogin(BOB).invalidate(aliceToken);
        exchange.forLogin(ALICE).invalidate("some-other-token");
        exchange.forLogin("wh1.carol").invalidate(aliceToken);
        assertThat(exchange.forLogin(ALICE).token(Duration.ofSeconds(10))).contains(aliceToken);
        assertThat(exchange.forLogin(BOB).token(Duration.ofSeconds(10))).contains(bobToken);
        assertThat(requests).hasSize(2);

        // Its own token: that login asks again, the other keeps its token
        exchange.forLogin(ALICE).invalidate(aliceToken);
        String replacement = exchange.forLogin(ALICE).token(Duration.ofSeconds(10)).orElseThrow();
        assertThat(replacement).isNotEqualTo(aliceToken);
        assertThat(subjectOf(replacement)).isEqualTo(ALICE);
        assertThat(exchange.forLogin(BOB).token(Duration.ofSeconds(10))).contains(bobToken);
        assertThat(requestsFor(ALICE)).isEqualTo(2);
        assertThat(requestsFor(BOB)).isEqualTo(1);

        // The token that was replaced is no longer the cached one: nothing to drop
        exchange.forLogin(ALICE).invalidate(aliceToken);
        assertThat(exchange.forLogin(ALICE).token(Duration.ofSeconds(10))).contains(replacement);
        assertThat(requestsFor(ALICE)).isEqualTo(2);
    }

    @Test
    void testDeadlines()
            throws Exception
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        CountDownLatch gate = gate();
        endpoint = request -> issue(request).after(gate);

        // A caller waiting for another caller's mint gives up at its own timeout
        Future<String> minting = callers.submit(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(30)).orElseThrow());
        awaitRequestsFor(ALICE, 1);
        long started = System.nanoTime();
        assertRefused(() -> exchange.forLogin(ALICE).token(Duration.ofMillis(300)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isBetween(Duration.ofMillis(250), Duration.ofSeconds(5));
        assertThat(requestsFor(ALICE)).isEqualTo(1);

        // A mint is cut at the caller's deadline when that is the shorter one
        started = System.nanoTime();
        assertRefused(() -> exchange.forLogin(BOB).token(Duration.ofMillis(400)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isBetween(Duration.ofMillis(350), Duration.ofSeconds(5));
        // The caller's short deadline says nothing about the endpoint: nothing is remembered
        endpoint = this::issue;
        assertThat(subjectOf(exchange.forLogin(BOB).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(BOB);
        // The first caller was never disturbed, and gets its token when the endpoint answers
        gate.countDown();
        assertThat(subjectOf(minting.get(10, SECONDS))).isEqualTo(ALICE);
        assertThat(requestsFor(ALICE)).isEqualTo(1);

        // A mint is cut at 10 seconds whatever the caller allows
        String carol = "wh1.carol";
        CountDownLatch never = gate();
        endpoint = request -> issue(request).after(never);
        started = System.nanoTime();
        assertRefused(() -> exchange.forLogin(carol).token(Duration.ofMinutes(2)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isBetween(Duration.ofMillis(9_500), Duration.ofSeconds(20));
        // An endpoint that does not answer is not asked again for 5 seconds, by any login
        endpoint = this::issue;
        int before = requests.size();
        assertRefused(() -> exchange.forLogin("wh1.dave").token(Duration.ofSeconds(10)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
        assertThat(requests).hasSize(before);
        ticker.advance(Duration.ofSeconds(6));
        assertThat(subjectOf(exchange.forLogin("wh1.dave").token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo("wh1.dave");
    }

    @Test
    void testEveryRefusalHasAFixedMessage()
            throws Exception
    {
        record Row(Answer answer, ErrorCodeSupplier errorCode, String message) {}

        List<Row> rows = ImmutableList.of(
                new Row(Answer.refusal(400, "invalid_grant"), PERMISSION_DENIED, LOGIN_REFUSED),
                new Row(Answer.refusal(400, "unsupported_grant_type"), HOGLAKE_LOGIN_TOKEN_REFUSED, NOT_OFFERED),
                new Row(Answer.refusal(400, "unauthorized_client"), HOGLAKE_LOGIN_TOKEN_REFUSED, CLIENT_NOT_ALLOWED),
                new Row(Answer.refusal(401, "invalid_client"), HOGLAKE_LOGIN_TOKEN_REFUSED, CLIENT_REFUSED),
                new Row(Answer.refusal(400, "invalid_request"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.refusal(400, "invalid_target"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.refusal(400, "invalid_scope"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.refusal(403, "access_denied"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.of(404, ""), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.of(400, "<html>" + BODY_MARKER + "</html>"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                // Not an error word: it is not read as one, whatever it resembles
                new Row(Answer.refusal(400, "Invalid_Grant"), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.of(400, "{\"error\":{\"invalid_grant\":\"%s\"}}".formatted(BODY_MARKER)), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                // Text in the place of the error word: it is not a word, and it is not logged
                new Row(Answer.of(400, "{\"error\":\"invalid_grant %s %s\"}".formatted(BODY_MARKER, ALICE)), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED),
                new Row(Answer.refusal(429, "slow_down"), HOGLAKE_CATALOG_UNAVAILABLE, BUSY),
                new Row(Answer.refusal(503, "temporarily_unavailable"), HOGLAKE_CATALOG_UNAVAILABLE, BUSY),
                new Row(Answer.refusal(500, "server_error"), HOGLAKE_CATALOG_UNAVAILABLE, BUSY),
                // A 5xx is unavailability whatever word it carries
                new Row(Answer.refusal(502, "invalid_grant"), HOGLAKE_CATALOG_UNAVAILABLE, BUSY),
                new Row(Answer.dropped(), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE),
                new Row(Answer.of(200, "{\"detail\":\"%s\"".formatted(BODY_MARKER)), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE),
                new Row(Answer.of(302, "").withLocation(tokenUri + "?elsewhere"), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE),
                new Row(Answer.of(204, ""), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE),
                // Only a 200 carries a token, whatever the body of another status looks like
                new Row(Answer.of(201, answer(tokenFor(ALICE, 9), "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE))), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE),
                new Row(Answer.of(203, answer(tokenFor(ALICE, 9), "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE))), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE));
        for (Row row : rows) {
            requests.clear();
            endpoint = _ -> row.answer();
            assertRefused(() -> exchange().forLogin(ALICE).token(Duration.ofSeconds(10)), row.errorCode(), row.message());
            // Sent once: no retry, and a redirect is not followed
            assertThat(requests).as(row.answer().toString()).hasSize(1);
        }
        assertThat(logged).anyMatch(line -> line.contains("HTTP 400 (invalid_grant)"));
        assertThat(logged).anyMatch(line -> line.contains("HTTP 404") && !line.contains("("));

        // A subject the issuer would refuse is refused here, with no request
        requests.clear();
        endpoint = this::issue;
        assertRefused(() -> exchange().forLogin(""), PERMISSION_DENIED, LOGIN_REFUSED);
        assertRefused(() -> exchange().forLogin("wh1." + "a".repeat(124)), PERMISSION_DENIED, LOGIN_REFUSED);
        assertThat(requests).isEmpty();
        String longest = "wh1." + "a".repeat(123);
        assertThat(subjectOf(exchange().forLogin(longest).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(longest);

        // An endpoint that cannot be reached
        HoglakeLoginTokenSource unreachable = new HoglakeLoginTokenSource.TokenExchange(URI.create("http://127.0.0.1:" + closedPort() + "/oauth/token"), CLIENT_ID, CLIENT_SECRET, "hoglake", SUBJECT_TOKEN_TYPE, Optional.of(SCOPE), httpClient, ticker);
        assertRefused(() -> unreachable.forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);

        // An endpoint the HTTP client refuses to ask: its error would name the endpoint
        HoglakeLoginTokenSource unusable = new HoglakeLoginTokenSource.TokenExchange(URI.create("ftp://127.0.0.1/oauth/token"), CLIENT_ID, CLIENT_SECRET, "hoglake", SUBJECT_TOKEN_TYPE, Optional.of(SCOPE), httpClient, ticker);
        assertRefused(() -> unusable.forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);

        // An interrupted caller: the same message, the interrupt kept, nothing sent
        requests.clear();
        Future<Boolean> interrupted = callers.submit(() -> {
            Thread.currentThread().interrupt();
            assertRefused(() -> exchange().forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_CATALOG_UNAVAILABLE, UNREACHABLE);
            return Thread.interrupted();
        });
        assertThat(interrupted.get(10, SECONDS)).isTrue();
        assertThat(requests).isEmpty();

        // A catalog without a token service
        HoglakeTokenSource unconfigured = HoglakeLoginTokenSource.UNCONFIGURED.forLogin(ALICE);
        assertRefused(() -> unconfigured.token(Duration.ofSeconds(10)), HOGLAKE_LOGIN_TOKEN_REFUSED, UNCONFIGURED);
        unconfigured.invalidate("anything");
        assertThat(requests).isEmpty();
    }

    @Test
    void testRefusalsAreRemembered()
    {
        // A refused login is not asked about again for a minute; another login is served
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        endpoint = request -> request.field("subject_token").equals(ALICE) ? Answer.refusal(400, "invalid_grant") : issue(request);
        for (int attempt = 0; attempt < 5; attempt++) {
            assertRefused(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(10)), PERMISSION_DENIED, LOGIN_REFUSED);
        }
        assertThat(requestsFor(ALICE)).isEqualTo(1);
        assertThat(subjectOf(exchange.forLogin(BOB).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(BOB);
        ticker.advance(Duration.ofSeconds(59));
        assertRefused(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(10)), PERMISSION_DENIED, LOGIN_REFUSED);
        assertThat(requestsFor(ALICE)).isEqualTo(1);
        ticker.advance(Duration.ofSeconds(2));
        endpoint = this::issue;
        assertThat(subjectOf(exchange.forLogin(ALICE).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(ALICE);
        assertThat(requestsFor(ALICE)).isEqualTo(2);

        // A request the issuer refuses is remembered for that login only
        HoglakeLoginTokenSource.TokenExchange refusingRequests = exchange();
        requests.clear();
        endpoint = request -> request.field("subject_token").equals(ALICE) ? Answer.refusal(400, "invalid_request") : issue(request);
        assertRefused(() -> refusingRequests.forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED);
        assertRefused(() -> refusingRequests.forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_LOGIN_TOKEN_REFUSED, REQUEST_REFUSED);
        assertThat(requestsFor(ALICE)).isEqualTo(1);
        assertThat(refusingRequests.forLogin(BOB).token(Duration.ofSeconds(10))).isPresent();

        // A refusal that does not depend on the login is remembered for every login
        for (String error : List.of("unsupported_grant_type", "unauthorized_client", "invalid_client")) {
            HoglakeLoginTokenSource.TokenExchange refused = exchange();
            requests.clear();
            endpoint = _ -> Answer.refusal(400, error);
            assertThatThrownBy(() -> refused.forLogin(ALICE).token(Duration.ofSeconds(10))).isInstanceOf(TrinoException.class);
            for (int attempt = 0; attempt < 5; attempt++) {
                assertThatThrownBy(() -> refused.forLogin(BOB).token(Duration.ofSeconds(10)))
                        .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_LOGIN_TOKEN_REFUSED.toErrorCode()));
            }
            assertThat(requests).as(error).hasSize(1);
            ticker.advance(Duration.ofSeconds(59));
            assertThatThrownBy(() -> refused.forLogin(BOB).token(Duration.ofSeconds(10))).isInstanceOf(TrinoException.class);
            assertThat(requests).as(error).hasSize(1);
            ticker.advance(Duration.ofSeconds(2));
            endpoint = this::issue;
            assertThat(subjectOf(refused.forLogin(BOB).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(BOB);
            assertThat(requests).as(error).hasSize(2);
        }

        // Unavailability is remembered for Retry-After, at most 30 seconds, and 5 seconds without it
        assertUnavailableFor(Answer.refusal(429, "slow_down").withRetryAfter("7"), Duration.ofSeconds(7));
        assertUnavailableFor(Answer.refusal(503, "temporarily_unavailable").withRetryAfter("300"), Duration.ofSeconds(30));
        assertUnavailableFor(Answer.refusal(503, "temporarily_unavailable"), Duration.ofSeconds(5));
        assertUnavailableFor(Answer.refusal(500, "server_error").withRetryAfter("soon"), Duration.ofSeconds(5));
        assertUnavailableFor(Answer.dropped(), Duration.ofSeconds(5));

        // A login with a token keeps using it while the endpoint refuses the others
        HoglakeLoginTokenSource.TokenExchange partly = exchange();
        endpoint = this::issue;
        String aliceToken = partly.forLogin(ALICE).token(Duration.ofSeconds(10)).orElseThrow();
        endpoint = _ -> Answer.refusal(503, "temporarily_unavailable");
        assertRefused(() -> partly.forLogin(BOB).token(Duration.ofSeconds(10)), HOGLAKE_CATALOG_UNAVAILABLE, BUSY);
        assertThat(partly.forLogin(ALICE).token(Duration.ofSeconds(10))).contains(aliceToken);

        // An answer outside the contract is not a token, and the next answer is read afresh
        HoglakeLoginTokenSource.TokenExchange recovering = exchange();
        endpoint = _ -> Answer.of(200, "{}");
        assertRefused(() -> recovering.forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE);
        endpoint = this::issue;
        assertThat(subjectOf(recovering.forLogin(ALICE).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(ALICE);
    }

    private void assertUnavailableFor(Answer answer, Duration retention)
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        requests.clear();
        endpoint = _ -> answer;
        assertThatThrownBy(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(10))).isInstanceOf(TrinoException.class);
        endpoint = this::issue;
        // No login asks while it is remembered
        ticker.advance(retention.minusMillis(500));
        assertThatThrownBy(() -> exchange.forLogin(ALICE).token(Duration.ofSeconds(10)))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
        assertThatThrownBy(() -> exchange.forLogin(BOB).token(Duration.ofSeconds(10)))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
        assertThat(requests).as(answer.toString()).hasSize(1);
        ticker.advance(Duration.ofSeconds(1));
        assertThat(subjectOf(exchange.forLogin(BOB).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(BOB);
        assertThat(requests).as(answer.toString()).hasSize(2);
    }

    @Test
    void testNumberOfCachedLoginsIsBounded()
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        int logins = 1_200;
        for (int index = 0; index < logins; index++) {
            String subject = "wh1.user" + index;
            assertThat(subjectOf(exchange.forLogin(subject).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(subject);
        }
        assertThat(requests).hasSize(logins);
        assertThat(exchange.cachedLogins()).isBetween(1L, 1_000L);
        // A login that was evicted asks again, and still gets its own token
        for (int index = 0; index < logins; index++) {
            String subject = "wh1.user" + index;
            assertThat(subjectOf(exchange.forLogin(subject).token(Duration.ofSeconds(10)).orElseThrow())).isEqualTo(subject);
        }
        assertThat(requests).hasSizeBetween(logins + 200, 2 * logins);
        assertThat(exchange.cachedLogins()).isBetween(1L, 1_000L);

        // A quiet login's slot goes after 20 minutes, with whatever it held
        HoglakeLoginTokenSource.TokenExchange quiet = exchange();
        endpoint = request -> issue(request, 7_200);
        quiet.forLogin(ALICE).token(Duration.ofSeconds(10));
        assertThat(quiet.cachedLogins()).isEqualTo(1);
        ticker.advance(Duration.ofMinutes(21));
        assertThat(quiet.cachedLogins()).isEqualTo(0);
    }

    @Test
    void testAnswerOutsideTheContractIsNotAToken()
    {
        String good = tokenFor(ALICE, 9);
        List<String> bodies = ImmutableList.of(
                // issued_token_type absent, as on a client-credentials answer, or another type
                "{\"access_token\":\"%s\", \"token_type\":\"Bearer\", \"expires_in\":900}".formatted(good),
                answer(good, "Bearer", "900", "\"urn:ietf:params:oauth:token-type:jwt\""),
                answer(good, "Bearer", "900", "null"),
                answer(good, "Bearer", "900", "[\"%s\"]".formatted(ACCESS_TOKEN_TYPE)),
                // token_type
                answer(good, "mac", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good, "", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                "{\"access_token\":\"%s\", \"expires_in\":900, \"issued_token_type\":\"%s\"}".formatted(good, ACCESS_TOKEN_TYPE),
                // expires_in
                answer(good, "Bearer", "0", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good, "Bearer", "-1", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good, "Bearer", "\"900\"", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good, "Bearer", "900.5", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good, "Bearer", "99999999999999999999999", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                "{\"access_token\":\"%s\", \"token_type\":\"Bearer\", \"issued_token_type\":\"%s\"}".formatted(good, ACCESS_TOKEN_TYPE),
                // access_token
                answer("", "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                "{\"token_type\":\"Bearer\", \"expires_in\":900, \"issued_token_type\":\"%s\"}".formatted(ACCESS_TOKEN_TYPE),
                "{\"access_token\":12345, \"token_type\":\"Bearer\", \"expires_in\":900, \"issued_token_type\":\"%s\"}".formatted(ACCESS_TOKEN_TYPE),
                // Not a value an Authorization header can carry
                answer(good + " extra", "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                answer(good + "\\r\\nX-Injected: 1", "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)),
                // Not an object
                "[%s]".formatted(answer(good, "Bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE))),
                "\"%s\"".formatted(good),
                "",
                "not json " + BODY_MARKER);
        for (String body : bodies) {
            requests.clear();
            endpoint = _ -> Answer.of(200, body);
            assertRefused(() -> exchange().forLogin(ALICE).token(Duration.ofSeconds(10)), HOGLAKE_INVALID_RESPONSE, INVALID_RESPONSE);
            assertThat(requests).as(body).hasSize(1);
        }
        // The same answer inside the contract, with the token type in another case
        endpoint = _ -> Answer.of(200, answer(good, "bearer", "900", "\"%s\"".formatted(ACCESS_TOKEN_TYPE)));
        assertThat(exchange().forLogin(ALICE).token(Duration.ofSeconds(10))).contains(good);
    }

    @Test
    void testNothingSecretIsPrinted()
            throws Exception
    {
        HoglakeLoginTokenSource.TokenExchange exchange = exchange();
        HoglakeTokenSource alice = exchange.forLogin(ALICE);
        String token = alice.token(Duration.ofSeconds(10)).orElseThrow();
        endpoint = _ -> Answer.refusal(400, "invalid_grant");
        assertRefused(() -> exchange.forLogin(BOB).token(Duration.ofSeconds(10)), PERMISSION_DENIED, LOGIN_REFUSED);

        assertHoldsNoSecret(exchange.toString());
        assertHoldsNoSecret(alice.toString());
        assertHoldsNoSecret(HoglakeLoginTokenSource.UNCONFIGURED.toString());
        assertHoldsNoSecret(HoglakeLoginTokenSource.UNCONFIGURED.forLogin(ALICE).toString());
        // What the cache holds: the keys carry subjects, the slots carry tokens
        Field slots = HoglakeLoginTokenSource.TokenExchange.class.getDeclaredField("slots");
        slots.setAccessible(true);
        Cache<?, ?> cache = (Cache<?, ?>) slots.get(exchange);
        assertThat(cache.asMap()).hasSize(2);
        for (Object key : cache.asMap().keySet()) {
            assertHoldsNoSecret(key.toString());
        }
        boolean tokenSeen = false;
        for (Object slot : cache.asMap().values()) {
            Field state = slot.getClass().getDeclaredField("state");
            state.setAccessible(true);
            Object held = ((AtomicReference<?>) state.get(slot)).get();
            assertHoldsNoSecret(String.valueOf(held));
            Optional<Field> value = List.of(held.getClass().getDeclaredFields()).stream().filter(field -> field.getName().equals("value")).findFirst();
            if (value.isPresent()) {
                value.get().setAccessible(true);
                tokenSeen |= token.equals(value.get().get(held));
            }
        }
        // The check above looked at the object that holds the token
        assertThat(tokenSeen).isTrue();

        // The log lines of this scenario exist, and say only the status and the error word
        assertThat(logged).anyMatch(line -> line.contains("HTTP 400 (invalid_grant)"));
    }

    private static void assertHoldsNoSecret(String text)
    {
        assertThat(text)
                .doesNotContain(TOKEN_PREFIX)
                .doesNotContain(CLIENT_SECRET)
                .doesNotContain("synthetic%2Btest%2Fsecret")
                .doesNotContain(BASIC.substring("Basic ".length()))
                .doesNotContain("wh1.")
                .doesNotContain(BODY_MARKER)
                .doesNotContain("127.0.0.1")
                .doesNotContain("10.0.0.1")
                .doesNotContain("/oauth/token");
    }

    private static void assertRefused(Executable call, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(call::execute)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(errorCode.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo(message);
                    // Nothing rides along: no cause and no suppressed failure
                    assertThat(failure.getCause()).isNull();
                    assertThat(failure.getSuppressed()).isEmpty();
                    assertHoldsNoSecret(failure.toString());
                });
    }

    private HoglakeLoginTokenSource.TokenExchange exchange()
    {
        return new HoglakeLoginTokenSource.TokenExchange(URI.create(tokenUri), CLIENT_ID, CLIENT_SECRET, "hoglake", SUBJECT_TOKEN_TYPE, Optional.of(SCOPE), httpClient, ticker);
    }

    private long requestsFor(String subject)
    {
        return requests.stream().filter(request -> subject.equals(request.field("subject_token"))).count();
    }

    private void awaitRequestsFor(String subject, int count)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (requestsFor(subject) < count) {
            assertThat(System.nanoTime()).as("request for " + subject).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    private CountDownLatch gate()
    {
        CountDownLatch gate = new CountDownLatch(1);
        gates.add(gate);
        return gate;
    }

    /**
     * A token of the fake endpoint: it carries the subject it was requested for,
     * encoded so that any subject fits in a token.
     */
    private static String tokenFor(String subject, int number)
    {
        return TOKEN_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(subject.getBytes(UTF_8)) + "." + number;
    }

    private static String subjectOf(String token)
    {
        assertThat(token).startsWith(TOKEN_PREFIX);
        return new String(Base64.getUrlDecoder().decode(token.substring(TOKEN_PREFIX.length(), token.lastIndexOf('.'))), UTF_8);
    }

    // ---- the fake token endpoint -------------------------------------------

    private Answer issue(TokenRequest request)
    {
        return issue(request, 900);
    }

    private Answer issue(TokenRequest request, long expiresIn)
    {
        // The token names the subject of the request that it answers
        return Answer.of(200, answer(tokenFor(request.field("subject_token"), issued.incrementAndGet()), "Bearer", String.valueOf(expiresIn), "\"%s\"".formatted(ACCESS_TOKEN_TYPE)));
    }

    private static String answer(String accessToken, String tokenType, String expiresIn, String issuedTokenType)
    {
        return "{\"access_token\":\"%s\", \"token_type\":\"%s\", \"expires_in\":%s, \"issued_token_type\":%s, \"detail\":\"%s\"}".formatted(accessToken, tokenType, expiresIn, issuedTokenType, BODY_MARKER);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        List<Field2> form = new ArrayList<>();
        String body = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
        for (String pair : Splitter.on('&').split(body)) {
            int separator = pair.indexOf('=');
            form.add(new Field2(URLDecoder.decode(pair.substring(0, separator), UTF_8), URLDecoder.decode(pair.substring(separator + 1), UTF_8)));
        }
        TokenRequest request = new TokenRequest(
                exchange.getRequestMethod(),
                exchange.getRequestURI().getRawQuery(),
                exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestHeaders().getFirst("Content-Type"),
                exchange.getRequestHeaders().getFirst("Accept"),
                exchange.getRequestHeaders().getFirst("User-Agent"),
                ImmutableList.copyOf(form));
        Answer answer = endpoint.apply(request);
        requests.add(request);
        if (answer.gate().isPresent()) {
            try {
                // Bounded, so a failed test cannot leave the server thread waiting
                answer.gate().get().await(60, SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (answer.drop()) {
            exchange.close();
            return;
        }
        byte[] bytes = answer.body().getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        answer.retryAfter().ifPresent(value -> exchange.getResponseHeaders().set("Retry-After", value));
        answer.location().ifPresent(value -> exchange.getResponseHeaders().set("Location", value));
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(answer.status(), -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(answer.status(), bytes.length);
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

    /**
     * A port in the same range with nothing listening on it.
     */
    private static int closedPort()
            throws IOException
    {
        HttpServer unused = bindInPortRange();
        int port = unused.getAddress().getPort();
        unused.stop(0);
        return port;
    }
}
