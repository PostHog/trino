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
package io.trino.plugin.hoglake.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hoglake.HoglakeStorageSession;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscribers;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_REFUSED;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.STORAGE_SESSION_ACCEPT_FLOOR;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.STORAGE_SESSION_REQUEST_TIMEOUT;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.STORAGE_SESSION_TEAMS_PER_REQUEST;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Asks the credential service for the S3 sessions that the files of the posthog
 * schema are read with: one session for each team asked, each good for the
 * objects under that team's prefix and nothing else.
 *
 * <p>A request names the events mount and up to
 * {@value io.trino.plugin.hoglake.HoglakePosthogSchema#STORAGE_SESSION_TEAMS_PER_REQUEST}
 * teams, and carries the token of one Trino login for the service's audience. That
 * token is not the login's token for the mount, and it is sent nowhere else.
 *
 * <p>An answer is used only when it is exactly what was asked: a session for
 * each team asked and for no other, each under a prefix that ends in that team's
 * {@code team_id=<id>/}, with enough lifetime left. Anything else is a refusal.
 * No refusal carries anything of the request or of the answer, and no session
 * value, token, URL or body is logged.
 */
public final class HoglakeStorageSessionClient
{
    private static final Logger log = Logger.get(HoglakeStorageSessionClient.class);

    // Ten sessions are about 15 KiB. An answer beyond this is not one.
    private static final int MAX_ANSWER_BYTES = 256 * 1024;
    private static final Duration DEFAULT_RETRY_AFTER = Duration.ofSeconds(5);
    private static final Duration MAX_RETRY_AFTER = Duration.ofSeconds(30);
    private static final Pattern ERROR_WORD = Pattern.compile("[a-z_]{1,64}");
    // RFC 6750 section 2.1. A value the HTTP client refuses would be quoted in its error.
    private static final Pattern BEARER_TOKEN = Pattern.compile("[A-Za-z0-9._~+/-]+=*");
    // The service's own patterns for the bucket and the location it builds a session policy for
    private static final Pattern BUCKET = Pattern.compile("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]");
    private static final Pattern LOCATION = Pattern.compile("[A-Za-z0-9][A-Za-z0-9/_.-]*/");
    private static final Pattern SESSION_VALUE = Pattern.compile("[\\x21-\\x7e]+");

    private final URI credentialsUri;
    private final String mountCatalog;
    private final HttpClient httpClient;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    public HoglakeStorageSessionClient(URI credentialsUri, String mountCatalog, HttpClient httpClient, Clock clock)
    {
        this.credentialsUri = requireNonNull(credentialsUri, "credentialsUri is null");
        this.mountCatalog = requireNonNull(mountCatalog, "mountCatalog is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.clock = requireNonNull(clock, "clock is null");
    }

    /**
     * One request for the sessions of {@code teams}, with a token of the login
     * that {@code loginTokens} belongs to. When the service refuses the token, it
     * is dropped and the request is sent once more with a new one.
     *
     * @throws TrinoException when the login has no token; the token source decides which
     */
    public Answer request(HoglakeTokenSource loginTokens, Set<Long> teams, Duration timeout)
    {
        requireNonNull(loginTokens, "loginTokens is null");
        List<Long> values = teams.stream().sorted().toList();
        checkArgument(!values.isEmpty() && values.size() <= STORAGE_SESSION_TEAMS_PER_REQUEST, "A request names 1 to %s teams", STORAGE_SESSION_TEAMS_PER_REQUEST);
        checkArgument(values.getFirst() > 0, "A team id is positive");
        long deadline = System.nanoTime() + timeout.toNanos();

        Optional<String> token = loginTokens.token(timeout);
        if (token.isEmpty()) {
            return new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO);
        }
        Answer answer = send(token.get(), values, timeout);
        if (!(answer instanceof Refused(Refusal refusal, Duration _)) || refusal != Refusal.TOKEN_REFUSED) {
            return answer;
        }
        // The service refused the token: it may have been revoked or replaced.
        // Once with a new one, and never a third request.
        loginTokens.invalidate(token.get());
        Duration remaining = Duration.ofNanos(deadline - System.nanoTime());
        if (remaining.isNegative() || remaining.isZero()) {
            return new Refused(Refusal.UNREACHABLE, Duration.ZERO);
        }
        Optional<String> replacement = loginTokens.token(remaining);
        if (replacement.isEmpty()) {
            return answer;
        }
        return send(replacement.get(), values, Duration.ofNanos(deadline - System.nanoTime()));
    }

    private Answer send(String token, List<Long> values, Duration remaining)
    {
        if (!BEARER_TOKEN.matcher(token).matches()) {
            return new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO);
        }
        Duration timeout = remaining.compareTo(STORAGE_SESSION_REQUEST_TIMEOUT) < 0 ? remaining : STORAGE_SESSION_REQUEST_TIMEOUT;
        if (timeout.isNegative() || timeout.isZero()) {
            return new Refused(Refusal.UNREACHABLE, Duration.ZERO);
        }
        // Exactly these three fields: the service refuses a body with any other
        String body = "{\"catalog\":\"%s\",\"access\":\"read\",\"values\":[%s]}".formatted(
                mountCatalog,
                values.stream().map(String::valueOf).collect(Collectors.joining(",")));
        HttpResponse<BoundedBody> response;
        CompletableFuture<HttpResponse<BoundedBody>> future = null;
        try {
            HttpRequest request = HttpRequest.newBuilder(credentialsUri)
                    .timeout(timeout)
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("User-Agent", HoglakeClient.USER_AGENT)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            // HttpRequest.timeout bounds receiving the headers, not the body. The get()
            // bounds both, and the cancel aborts a request that stalls in its body.
            future = httpClient.sendAsync(request, _ -> BoundedBody.subscriber());
            response = future.get(timeout.toNanos(), NANOSECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Refused(Refusal.UNREACHABLE, Duration.ZERO);
        }
        catch (ExecutionException | TimeoutException | RuntimeException e) {
            // No cause is kept: an error of the HTTP client can name the endpoint
            return new Refused(Refusal.UNREACHABLE, Duration.ZERO);
        }
        finally {
            if (future != null) {
                future.cancel(true);
            }
        }
        int status = response.statusCode();
        if (status == 200) {
            return issued(response.body(), values);
        }
        if (status < 400) {
            // Redirects are not followed, and no other answer holds sessions
            return new Refused(Refusal.INVALID_RESPONSE, Duration.ZERO);
        }
        // Only the error word is read. The rest of the body reaches nothing.
        Optional<String> error = errorWord(response.body());
        log.debug("Storage sessions of %s teams of %s were refused: HTTP %s%s", values.size(), mountCatalog, status, error.map(word -> " (" + word + ")").orElse(""));
        if (status == 429 || status >= 500) {
            return new Refused(Refusal.BUSY, retryAfter(response));
        }
        return switch (status) {
            case 401 -> new Refused(Refusal.TOKEN_REFUSED, Duration.ZERO);
            // The service does not say which team it refused, or why
            case 403 -> new Refused(Refusal.TEAM_REFUSED, Duration.ZERO);
            case 404 -> new Refused(Refusal.NOT_OFFERED, Duration.ZERO);
            default -> new Refused(Refusal.REQUEST_REFUSED, Duration.ZERO);
        };
    }

    /**
     * The sessions of a 200, or the refusal of an answer that is not exactly
     * what was asked. The answer's region and its own earliest expiry are not
     * used: each session carries its expiry, and the catalog has its region.
     */
    private Answer issued(BoundedBody body, List<Long> asked)
    {
        Refused invalid = new Refused(Refusal.INVALID_RESPONSE, Duration.ZERO);
        if (body.oversized()) {
            return invalid;
        }
        JsonNode answer;
        try {
            answer = mapper.readTree(body.bytes());
        }
        catch (IOException | RuntimeException e) {
            // No cause is kept: a parser error can quote the body
            return invalid;
        }
        if (answer == null || !answer.isObject()) {
            return invalid;
        }
        JsonNode bucket = answer.path("bucket");
        JsonNode credentials = answer.path("credentials");
        if (!bucket.isTextual() || !BUCKET.matcher(bucket.asText()).matches()) {
            return invalid;
        }
        if (!credentials.isArray() || credentials.size() != asked.size()) {
            return invalid;
        }
        long nowMillis = clock.millis();
        Map<Long, HoglakeStorageSession> sessions = new HashMap<>();
        for (JsonNode credential : credentials) {
            JsonNode teamId = credential.path("team_id");
            JsonNode keyPrefix = credential.path("prefix");
            JsonNode accessKeyId = credential.path("access_key_id");
            JsonNode secretAccessKey = credential.path("secret_access_key");
            JsonNode sessionToken = credential.path("session_token");
            JsonNode expiration = credential.path("expiration");
            // Exactly the teams asked, each once
            if (!teamId.isIntegralNumber() || !teamId.canConvertToLong() || !asked.contains(teamId.asLong()) || sessions.containsKey(teamId.asLong())) {
                return invalid;
            }
            long team = teamId.asLong();
            if (!keyPrefix.isTextual() || !isTeamKeyPrefix(keyPrefix.asText(), team)) {
                return invalid;
            }
            // The team's own prefix: the worker and S3 both hold the session to it
            String prefix = "s3://" + bucket.asText() + "/" + keyPrefix.asText();
            if (!HoglakeStorageSession.isTeamPrefix(prefix, team)) {
                return invalid;
            }
            if (!isSessionValue(accessKeyId) || !isSessionValue(secretAccessKey) || !isSessionValue(sessionToken)) {
                return invalid;
            }
            Optional<Instant> expires = instant(expiration);
            if (expires.isEmpty() || expires.get().toEpochMilli() - nowMillis < STORAGE_SESSION_ACCEPT_FLOOR.toMillis()) {
                return invalid;
            }
            sessions.put(team, new HoglakeStorageSession(team, prefix, accessKeyId.asText(), secretAccessKey.asText(), sessionToken.asText(), expires.get().toEpochMilli()));
        }
        return new Issued(ImmutableMap.copyOf(sessions));
    }

    /**
     * Whether a key prefix is what the service builds for a team: the location
     * of the events table's files, which has the shape the service puts into a
     * session policy, and then {@code team_id=<id>/}.
     */
    private static boolean isTeamKeyPrefix(String prefix, long team)
    {
        String teamDirectory = "team_id=" + team + "/";
        if (!prefix.endsWith(teamDirectory)) {
            return false;
        }
        String location = prefix.substring(0, prefix.length() - teamDirectory.length());
        return LOCATION.matcher(location).matches() && !location.contains("//") && !location.contains("..");
    }

    private static boolean isSessionValue(JsonNode value)
    {
        return value.isTextual() && SESSION_VALUE.matcher(value.asText()).matches();
    }

    private static Optional<Instant> instant(JsonNode value)
    {
        if (!value.isTextual()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(value.asText()));
        }
        catch (DateTimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> errorWord(BoundedBody body)
    {
        if (body.oversized()) {
            return Optional.empty();
        }
        try {
            JsonNode answer = mapper.readTree(body.bytes());
            if (answer == null || !answer.path("error").isTextual()) {
                return Optional.empty();
            }
            String word = answer.path("error").asText();
            if (!ERROR_WORD.matcher(word).matches()) {
                return Optional.empty();
            }
            return Optional.of(word);
        }
        catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Duration retryAfter(HttpResponse<?> response)
    {
        Optional<String> header = response.headers().firstValue("Retry-After");
        if (header.isEmpty()) {
            return DEFAULT_RETRY_AFTER;
        }
        long seconds;
        try {
            seconds = Long.parseLong(header.get().trim());
        }
        catch (NumberFormatException e) {
            return DEFAULT_RETRY_AFTER;
        }
        return Duration.ofSeconds(Math.clamp(seconds, 0, MAX_RETRY_AFTER.toSeconds()));
    }

    @Override
    public String toString()
    {
        return "HoglakeStorageSessionClient[redacted]";
    }

    /**
     * What the service answered: the sessions of every team asked, or one
     * refusal for the whole request.
     */
    public sealed interface Answer
            permits Issued, Refused {}

    /**
     * A session for each team asked, by team. A session prints no value of its own.
     */
    public record Issued(Map<Long, HoglakeStorageSession> sessions)
            implements Answer
    {
        public Issued
        {
            sessions = ImmutableMap.copyOf(sessions);
        }
    }

    /**
     * @param retryAfter for {@link Refusal#BUSY}, how long the service asked to wait
     */
    public record Refused(Refusal refusal, Duration retryAfter)
            implements Answer
    {
        public Refused
        {
            requireNonNull(refusal, "refusal is null");
            requireNonNull(retryAfter, "retryAfter is null");
        }
    }

    /**
     * Every way a request can fail to return sessions, with the only text a user
     * sees for it. No message carries anything of the request or of the answer.
     */
    public enum Refusal
    {
        TEAM_REFUSED(PERMISSION_DENIED, "Access Denied: Cannot read every team's events with this login"),
        NOT_OFFERED(HOGLAKE_STORAGE_SESSION_REFUSED, "The posthog schema cannot be read: the credential service does not issue storage sessions for it"),
        REQUEST_REFUSED(HOGLAKE_STORAGE_SESSION_REFUSED, "The posthog schema cannot be read: the credential service refused the request"),
        TOKEN_REFUSED(HOGLAKE_STORAGE_SESSION_REFUSED, "The posthog schema cannot be read: the credential service refused this catalog's token"),
        BUSY(HOGLAKE_CATALOG_UNAVAILABLE, "The credential service for the posthog schema is busy or unavailable; retry later"),
        UNREACHABLE(HOGLAKE_CATALOG_UNAVAILABLE, "The credential service for the posthog schema cannot be reached; retry later"),
        INVALID_RESPONSE(HOGLAKE_INVALID_RESPONSE, "The credential service for the posthog schema answered outside its contract");

        private final ErrorCodeSupplier errorCode;
        private final String message;

        Refusal(ErrorCodeSupplier errorCode, String message)
        {
            this.errorCode = errorCode;
            this.message = message;
        }

        public TrinoException failure()
        {
            return new TrinoException(errorCode, message);
        }
    }

    /**
     * At most {@link #MAX_ANSWER_BYTES} of a response body. What comes after is
     * read and dropped, so a body of any size costs a bounded amount of memory.
     */
    private static final class BoundedBody
    {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private boolean oversized;

        public static HttpResponse.BodySubscriber<BoundedBody> subscriber()
        {
            BoundedBody body = new BoundedBody();
            return BodySubscribers.mapping(BodySubscribers.ofByteArrayConsumer(body::accept), _ -> body);
        }

        private void accept(Optional<byte[]> part)
        {
            if (part.isEmpty() || oversized) {
                return;
            }
            if (bytes.size() + part.get().length > MAX_ANSWER_BYTES) {
                oversized = true;
                bytes.reset();
                return;
            }
            bytes.writeBytes(part.get());
        }

        public boolean oversized()
        {
            return oversized;
        }

        public byte[] bytes()
        {
            return bytes.toByteArray();
        }
    }
}
