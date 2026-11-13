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
package io.trino.plugin.hoglake.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A credential service that speaks the two routes the posthog schema uses, as
 * the real one does: the token endpoint ({@value #TOKEN_PATH}) with the client
 * credentials grant and the per-login token exchange, and the S3 session
 * endpoint ({@value #CREDENTIALS_PATH}).
 *
 * <p>The session endpoint accepts only a token that was exchanged for its own
 * audience, charges each client's request bucket (10 a second, burst 20) for
 * every request with such a token, refuses a body with an unknown field or with
 * other than 1 to 10 distinct positive teams, refuses a whole request when one
 * team is refused, and returns a team's session again until it has less than
 * ten minutes left. Every request is recorded.
 */
public final class FakeCredentialService
        implements Closeable
{
    public static final String TOKEN_PATH = "/oauth/token";
    public static final String CREDENTIALS_PATH = "/oauth/credentials";
    public static final String MOUNT_AUDIENCE = "hoglake";
    public static final String SESSION_AUDIENCE = "hogtower-vend";
    public static final String MOUNT_TOKEN = "mount-token-";
    public static final String SESSION_TOKEN = "vend-token-";
    public static final String CLIENT_SECRET = "synthetic-client-secret-5b1e";
    // In the description of every refusal, so a test can see that no body is repeated
    public static final String BODY_MARKER = "detail-marker-9c2f";
    public static final String BUCKET = "events-bucket";
    public static final String LOCATION = "data/ingest/events/";
    public static final long SESSION_MILLIS = 15 * 60 * 1000;
    public static final long REUSE_FLOOR_MILLIS = 10 * 60 * 1000;
    public static final int MAX_TEAMS = 10;

    private static final String EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange";
    private static final double REQUESTS_PER_SECOND = 10;
    private static final double REQUEST_BURST = 20;

    public record TokenRequest(String clientId, String grantType, String subject, String subjectTokenType, String audience, String scope, Set<String> fields)
    {
        public boolean isExchange()
        {
            return EXCHANGE_GRANT.equals(grantType);
        }
    }

    /**
     * @param login the login the request's token was exchanged for, when it is a token of this endpoint
     * @param atMillis when the request arrived, by the service's clock
     */
    public record SessionRequest(String method, Map<String, String> headers, String body, Optional<String> login, List<Long> teams, int status, long atMillis) {}

    public record Answer(int status, Map<String, String> headers, String body) {}

    /**
     * What an issued access key was issued for.
     */
    public record Issued(String login, long team, long expirationMillis) {}

    private record SessionKey(String login, long team) {}

    private record Session(String accessKeyId, String secretAccessKey, String sessionToken, long expirationMillis) {}

    private final String mountCatalog;
    private final LongSupplier clock;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final List<TokenRequest> tokenRequests = new CopyOnWriteArrayList<>();
    private final List<SessionRequest> sessionRequests = new CopyOnWriteArrayList<>();
    private final Map<SessionKey, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, Issued> issued = new ConcurrentHashMap<>();
    private final Set<String> rejectedTokens = ConcurrentHashMap.newKeySet();
    private final Set<SessionKey> refusedTeams = ConcurrentHashMap.newKeySet();
    private final Set<String> notOfferedTo = ConcurrentHashMap.newKeySet();
    private final Queue<Answer> nextAnswers = new ConcurrentLinkedQueue<>();
    private final Map<String, double[]> buckets = new HashMap<>();
    private final List<String> issuedTokens = new CopyOnWriteArrayList<>();
    private final List<String> allIssuedTokens = new CopyOnWriteArrayList<>();
    private final AtomicInteger mints = new AtomicInteger();
    private final AtomicInteger started = new AtomicInteger();
    private final AtomicInteger inProgress = new AtomicInteger();
    private volatile boolean offered = true;
    private volatile boolean unavailable;
    private volatile boolean exchangeOffered = true;
    private volatile CountDownLatch hold;
    private volatile long clockOffsetMillis;

    public FakeCredentialService(String mountCatalog, LongSupplier clock)
            throws IOException
    {
        this.mountCatalog = requireNonNull(mountCatalog, "mountCatalog is null");
        this.clock = requireNonNull(clock, "clock is null");
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        // One thread per exchange, so a held answer does not hold up the others
        server.setExecutor(executor);
        server.start();
    }

    public String uri()
    {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public URI tokenUri()
    {
        return URI.create(uri() + TOKEN_PATH);
    }

    public URI credentialsUri()
    {
        return URI.create(uri() + CREDENTIALS_PATH);
    }

    public List<TokenRequest> tokenRequests()
    {
        return tokenRequests;
    }

    public List<SessionRequest> sessionRequests()
    {
        return sessionRequests;
    }

    /**
     * The session requests that have arrived since the last reset, answered or not.
     */
    public int sessionRequestsStarted()
    {
        return started.get();
    }

    /**
     * The session requests that were answered with sessions.
     */
    public List<SessionRequest> served()
    {
        return sessionRequests.stream().filter(request -> request.status() == 200).collect(toImmutableList());
    }

    /**
     * The per-login tokens issued since the last reset, of either audience.
     */
    public List<String> issuedTokens()
    {
        return issuedTokens;
    }

    public List<String> allIssuedTokens()
    {
        return allIssuedTokens;
    }

    public int mints()
    {
        return mints.get();
    }

    public Optional<Issued> issuedFor(String accessKeyId)
    {
        return Optional.ofNullable(issued.get(accessKeyId));
    }

    /**
     * Every value of every session issued so far.
     */
    public Set<String> sessionValues()
    {
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        for (Session session : sessions.values()) {
            values.add(session.accessKeyId(), session.secretAccessKey(), session.sessionToken());
        }
        values.addAll(issued.keySet());
        return values.build();
    }

    public void refuseTeam(String login, long team)
    {
        refusedTeams.add(new SessionKey(login, team));
    }

    public void allowTeam(String login, long team)
    {
        refusedTeams.remove(new SessionKey(login, team));
    }

    /**
     * The session endpoint answers 401 to this token from now on.
     */
    public void rejectToken(String token)
    {
        rejectedTokens.add(token);
    }

    /**
     * The next session requests with a valid token are answered with these, in order, whatever they ask.
     */
    public void answerNext(Answer... answers)
    {
        nextAnswers.addAll(List.of(answers));
    }

    public void setOffered(boolean offered)
    {
        this.offered = offered;
    }

    /**
     * The session endpoint answers this client as it answers when it does not issue sessions for the mount.
     */
    public void setNotOfferedTo(String clientId)
    {
        notOfferedTo.add(clientId);
    }

    public void setUnavailable(boolean unavailable)
    {
        this.unavailable = unavailable;
    }

    public void setExchangeOffered(boolean exchangeOffered)
    {
        this.exchangeOffered = exchangeOffered;
    }

    /**
     * Session requests with a valid token wait for {@code hold} before they are answered.
     */
    public void holdSessions(CountDownLatch hold)
    {
        this.hold = hold;
    }

    /**
     * The service's clock runs this far behind the clock it was given.
     */
    public void setClockBehind(long millis)
    {
        this.clockOffsetMillis = -millis;
    }

    /**
     * Fills every client's request bucket, as time does.
     */
    public void refillRequestBuckets()
    {
        synchronized (buckets) {
            buckets.clear();
        }
    }

    /**
     * Forgets everything but the keys and tokens issued so far, once the
     * requests that are still being answered have been answered.
     */
    public void reset()
    {
        long deadline = System.nanoTime() + SECONDS.toNanos(30);
        while (inProgress.get() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        tokenRequests.clear();
        issuedTokens.clear();
        sessionRequests.clear();
        sessions.clear();
        rejectedTokens.clear();
        refusedTeams.clear();
        notOfferedTo.clear();
        nextAnswers.clear();
        synchronized (buckets) {
            buckets.clear();
        }
        mints.set(0);
        started.set(0);
        offered = true;
        unavailable = false;
        exchangeOffered = true;
        hold = null;
        clockOffsetMillis = 0;
    }

    @Override
    public void close()
    {
        server.stop(0);
        executor.shutdownNow();
    }

    public static String sessionJson(long team, String prefix, String accessKeyId, String secretAccessKey, String sessionToken, String expiration)
    {
        return "{\"team_id\":%d,\"prefix\":\"%s\",\"access_key_id\":\"%s\",\"secret_access_key\":\"%s\",\"session_token\":\"%s\",\"expiration\":\"%s\"}"
                .formatted(team, prefix, accessKeyId, secretAccessKey, sessionToken, expiration);
    }

    public static String answerJson(String bucket, String... sessions)
    {
        return "{\"region\":\"us-east-1\",\"bucket\":\"%s\",\"expiration\":\"2026-01-01T00:00:00Z\",\"credentials\":[%s]}".formatted(bucket, String.join(",", sessions));
    }

    public static String teamPrefix(long team)
    {
        return LOCATION + "team_id=" + team + "/";
    }

    /**
     * The login a token of this service was issued for.
     */
    public static Optional<String> loginOf(String kind, String token)
    {
        if (token == null || !token.startsWith(kind)) {
            return Optional.empty();
        }
        String encoded = token.substring(kind.length(), token.indexOf('.', kind.length()));
        return Optional.of(new String(Base64.getUrlDecoder().decode(encoded), UTF_8));
    }

    private long now()
    {
        return clock.getAsLong() + clockOffsetMillis;
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        inProgress.incrementAndGet();
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
            if (path.equals(TOKEN_PATH)) {
                token(exchange, body);
            }
            else if (path.equals(CREDENTIALS_PATH)) {
                Answer answer = credentials(exchange, body);
                respond(exchange, answer);
            }
            else {
                respond(exchange, refusal(404, "not_found"));
            }
        }
        finally {
            inProgress.decrementAndGet();
        }
    }

    private void token(HttpExchange exchange, String body)
            throws IOException
    {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
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
        if (!credentials.substring(credentials.indexOf(':') + 1).equals(CLIENT_SECRET)) {
            respond(exchange, refusal(401, "invalid_client"));
            return;
        }
        if (!request.isExchange()) {
            respond(exchange, new Answer(200, ImmutableMap.of(), "{\"access_token\":\"token-for-%s\", \"token_type\":\"Bearer\", \"expires_in\":3600}".formatted(clientId)));
            return;
        }
        if (!exchangeOffered) {
            respond(exchange, refusal(400, "unsupported_grant_type"));
            return;
        }
        String kind;
        if (MOUNT_AUDIENCE.equals(request.audience()) && ("hoglake:" + mountCatalog + ":read").equals(request.scope())) {
            kind = MOUNT_TOKEN;
        }
        else if (SESSION_AUDIENCE.equals(request.audience()) && request.scope() == null && !form.containsKey("values")) {
            kind = SESSION_TOKEN;
        }
        else {
            // A mount scope with the session audience, or the reverse, is refused at the mint
            respond(exchange, refusal(400, "invalid_scope"));
            return;
        }
        String token = "%s%s.%s.%d".formatted(
                kind,
                Base64.getUrlEncoder().withoutPadding().encodeToString(request.subject().getBytes(UTF_8)),
                clientId,
                allIssuedTokens.size() + 1);
        issuedTokens.add(token);
        allIssuedTokens.add(token);
        respond(exchange, new Answer(200, ImmutableMap.of(), "{\"access_token\":\"%s\", \"token_type\":\"Bearer\", \"expires_in\":900, \"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\"}".formatted(token)));
    }

    private Answer credentials(HttpExchange exchange, String body)
    {
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(Locale.ROOT), values.getFirst()));
        String authorization = headers.get("authorization");
        String token = null;
        if (authorization != null && authorization.startsWith("Bearer ")) {
            token = authorization.substring("Bearer ".length());
        }
        Optional<String> login = loginOf(SESSION_TOKEN, token);
        List<Long> teams = new ArrayList<>();
        long arrived = now();
        started.incrementAndGet();
        Answer answer = answer(exchange.getRequestMethod(), token, login, body, teams);
        sessionRequests.add(new SessionRequest(exchange.getRequestMethod(), ImmutableMap.copyOf(headers), body, login, ImmutableList.copyOf(teams), answer.status(), arrived));
        return answer;
    }

    private Answer answer(String method, String token, Optional<String> login, String body, List<Long> teams)
    {
        if (!method.equals("POST")) {
            return refusal(405, "method_not_allowed");
        }
        // Only a token exchanged for this endpoint's audience is one of its tokens
        if (login.isEmpty() || rejectedTokens.contains(token)) {
            return refusal(401, "invalid_token");
        }
        // The client's request bucket, charged before anything else is looked at
        String clientId = Splitter.on('.').splitToList(token).get(1);
        Optional<Long> wait = charge(clientId);
        if (wait.isPresent()) {
            return new Answer(429, ImmutableMap.of("Retry-After", wait.get().toString()), refusalBody("slow_down"));
        }
        CountDownLatch held = hold;
        if (held != null) {
            try {
                held.await(30, SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        JsonNode request;
        try {
            request = mapper.readTree(body);
        }
        catch (IOException e) {
            return refusal(400, "invalid_request");
        }
        if (request == null || !request.isObject()) {
            return refusal(400, "invalid_request");
        }
        for (Iterator<String> names = request.fieldNames(); names.hasNext(); ) {
            if (!Set.of("catalog", "access", "values").contains(names.next())) {
                return refusal(400, "invalid_request");
            }
        }
        Set<Long> distinct = new HashSet<>();
        for (JsonNode value : request.path("values")) {
            if (!value.isIntegralNumber() || value.asLong() <= 0 || !distinct.add(value.asLong())) {
                return refusal(400, "invalid_request");
            }
            teams.add(value.asLong());
        }
        if (teams.isEmpty() || teams.size() > MAX_TEAMS) {
            return refusal(400, "invalid_request");
        }
        Answer canned = nextAnswers.poll();
        if (canned != null) {
            return canned;
        }
        if (!offered || notOfferedTo.contains(clientId) || !mountCatalog.equals(request.path("catalog").asText())) {
            return refusal(404, "not_found");
        }
        if (!"read".equals(request.path("access").asText())) {
            return refusal(403, "insufficient_scope");
        }
        // One refused team refuses the whole request, and the answer does not say which
        for (long team : teams) {
            if (refusedTeams.contains(new SessionKey(login.get(), team))) {
                return refusal(403, "insufficient_scope");
            }
        }
        if (unavailable) {
            return new Answer(503, ImmutableMap.of("Retry-After", "5"), refusalBody("temporarily_unavailable"));
        }
        long now = now();
        List<String> credentials = new ArrayList<>();
        long earliest = Long.MAX_VALUE;
        for (long team : teams) {
            // A session is returned again until it has less than the floor left
            Session session = sessions.compute(new SessionKey(login.get(), team), (_, current) -> {
                if (current != null && current.expirationMillis() - now >= REUSE_FLOOR_MILLIS) {
                    return current;
                }
                int number = mints.incrementAndGet();
                Session minted = new Session(
                        "ASIAFAKE%dN%d".formatted(team, number),
                        "secret-value-%d-%d".formatted(team, number),
                        "session-token-value-%d-%d".formatted(team, number),
                        now + SESSION_MILLIS);
                issued.put(minted.accessKeyId(), new Issued(login.get(), team, minted.expirationMillis()));
                return minted;
            });
            earliest = Math.min(earliest, session.expirationMillis());
            credentials.add(sessionJson(team, teamPrefix(team), session.accessKeyId(), session.secretAccessKey(), session.sessionToken(), Instant.ofEpochMilli(session.expirationMillis()).toString()));
        }
        return new Answer(200, ImmutableMap.of(), "{\"region\":\"us-east-1\",\"bucket\":\"%s\",\"expiration\":\"%s\",\"credentials\":[%s]}"
                .formatted(BUCKET, Instant.ofEpochMilli(earliest), String.join(",", credentials)));
    }

    /**
     * Takes one request from the client's bucket, or says how many seconds to wait.
     */
    private Optional<Long> charge(String clientId)
    {
        long now = now();
        synchronized (buckets) {
            // The bucket's level and the time it was last filled
            double[] bucket = buckets.computeIfAbsent(clientId, _ -> new double[] {REQUEST_BURST, now});
            bucket[0] = Math.min(REQUEST_BURST, bucket[0] + (now - bucket[1]) * REQUESTS_PER_SECOND / 1000);
            bucket[1] = now;
            if (bucket[0] >= 1) {
                bucket[0] -= 1;
                return Optional.empty();
            }
            return Optional.of(Math.max(1, (long) Math.ceil((1 - bucket[0]) / REQUESTS_PER_SECOND)));
        }
    }

    private static Answer refusal(int status, String error)
    {
        return new Answer(status, ImmutableMap.of(), refusalBody(error));
    }

    private static String refusalBody(String error)
    {
        return "{\"error\":\"%s\", \"error_description\":\"%s\"}".formatted(error, BODY_MARKER);
    }

    private static void respond(HttpExchange exchange, Answer answer)
            throws IOException
    {
        byte[] bytes = answer.body().getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        answer.headers().forEach(exchange.getResponseHeaders()::set);
        exchange.sendResponseHeaders(answer.status(), bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }
    }
}
