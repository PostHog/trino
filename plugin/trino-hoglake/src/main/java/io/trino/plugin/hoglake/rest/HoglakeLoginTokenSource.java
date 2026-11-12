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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Where the bearer tokens for the posthog schema come from: one token source for
 * each Trino login. A request for the schema carries the token of the login that
 * asked for it, never the catalog's own token.
 *
 * <p>This is not a {@link HoglakeTokenSource}, so it cannot be given to a client
 * that expects one token for the whole catalog.
 */
public interface HoglakeLoginTokenSource
{
    /**
     * What a catalog without a token service gets: every login is refused, and no
     * request is sent anywhere.
     */
    HoglakeLoginTokenSource UNCONFIGURED = _ -> new HoglakeTokenSource()
    {
        @Override
        public Optional<String> token(Duration timeout)
        {
            throw new TrinoException(HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: this catalog has no token service configured");
        }

        @Override
        public void invalidate(String token) {}
    };

    /**
     * The token source of one login. Every token it returns was issued for
     * {@code subject}, and it never returns a token issued for another subject.
     */
    HoglakeTokenSource forLogin(String subject);

    /**
     * RFC 8693 token exchange: the catalog's client, authenticated with HTTP Basic,
     * names a login as the subject and receives a token for that login.
     *
     * <p>Tokens are cached for each login until shortly before {@code expires_in}.
     * No field holds "the current token": a token is kept only in the slot of the
     * login it was requested for, and a slot is found only by that login's key.
     *
     * <p>Refusals are cached too, so a login that keeps asking while it is refused
     * does not send a request to the token endpoint each time. The catalog's own
     * tokens come from the same endpoint, and must not have to compete with them.
     */
    final class TokenExchange
            implements HoglakeLoginTokenSource
    {
        private static final Logger log = Logger.get(TokenExchange.class);

        private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
        private static final String ISSUED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
        private static final Pattern ERROR_WORD = Pattern.compile("[a-z_]{1,64}");
        // RFC 6750 section 2.1. Nothing else can be sent in an Authorization header,
        // and a value the HTTP client refuses would be quoted in its error.
        private static final Pattern BEARER_TOKEN = Pattern.compile("[A-Za-z0-9._~+/-]+=*");

        private static final int MAX_SUBJECT_LENGTH = 127;
        private static final int MAX_LOGINS = 1_000;
        // Longer than any token is used, so a slot goes only after its login went quiet.
        private static final Duration SLOT_LIFETIME = Duration.ofMinutes(20);
        // Refresh this far before expiry, and at most a fifth of the lifetime early.
        private static final Duration REFRESH_MARGIN = Duration.ofSeconds(60);
        private static final Duration MAX_LIFETIME = Duration.ofDays(1);
        private static final Duration MINT_TIMEOUT = Duration.ofSeconds(10);
        private static final Duration REFUSAL_RETENTION = Duration.ofSeconds(60);
        private static final Duration UNAVAILABLE_RETENTION = Duration.ofSeconds(5);
        private static final Duration MAX_UNAVAILABLE_RETENTION = Duration.ofSeconds(30);

        private final URI tokenUri;
        private final String basicAuth;
        private final String audience;
        private final String subjectTokenType;
        private final Optional<String> scope;
        private final HttpClient httpClient;
        private final Ticker ticker;
        private final ObjectMapper mapper = new ObjectMapper();

        private final Cache<Key, Slot> slots;
        // A refusal that does not depend on the login. It never holds a token.
        private final AtomicReference<Refused> refusedForAll = new AtomicReference<>();

        public TokenExchange(URI tokenUri, String clientId, String clientSecret, String audience, String subjectTokenType, Optional<String> scope, HttpClient httpClient)
        {
            this(tokenUri, clientId, clientSecret, audience, subjectTokenType, scope, httpClient, Ticker.systemTicker());
        }

        @VisibleForTesting
        public TokenExchange(URI tokenUri, String clientId, String clientSecret, String audience, String subjectTokenType, Optional<String> scope, HttpClient httpClient, Ticker ticker)
        {
            this.tokenUri = requireNonNull(tokenUri, "tokenUri is null");
            // RFC 6749 section 2.3.1: both halves are form-encoded before they are joined.
            this.basicAuth = "Basic " + Base64.getEncoder().encodeToString(
                    (form(requireNonNull(clientId, "clientId is null")) + ":" + form(requireNonNull(clientSecret, "clientSecret is null"))).getBytes(UTF_8));
            this.audience = requireNonNull(audience, "audience is null");
            this.subjectTokenType = requireNonNull(subjectTokenType, "subjectTokenType is null");
            this.scope = requireNonNull(scope, "scope is null");
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.ticker = requireNonNull(ticker, "ticker is null");
            this.slots = EvictableCacheBuilder.newBuilder()
                    .maximumSize(MAX_LOGINS)
                    .expireAfterWrite(SLOT_LIFETIME)
                    .ticker(ticker)
                    .build();
        }

        @Override
        public HoglakeTokenSource forLogin(String subject)
        {
            requireNonNull(subject, "subject is null");
            if (subject.isEmpty() || subject.length() > MAX_SUBJECT_LENGTH) {
                // The issuer refuses these. Refuse them here, without a request.
                throw Refusal.LOGIN_REFUSED.failure();
            }
            return new LoginTokens(subject);
        }

        @VisibleForTesting
        public long cachedLogins()
        {
            slots.cleanUp();
            return slots.size();
        }

        @Override
        public String toString()
        {
            return "TokenExchange[redacted]";
        }

        /**
         * The tokens of one login. The subject is fixed here and is the only input
         * to both the cache lookup and the request.
         */
        private final class LoginTokens
                implements HoglakeTokenSource
        {
            private final String subject;

            private LoginTokens(String subject)
            {
                this.subject = requireNonNull(subject, "subject is null");
            }

            @Override
            public Optional<String> token(Duration timeout)
            {
                Slot slot = uncheckedCacheGet(slots, new Key(subject, scope), Slot::new);
                State state = slot.state.get();
                if (state instanceof Token token && fresh(token)) {
                    return Optional.of(token.value());
                }
                long deadline = System.nanoTime() + timeout.toNanos();
                rethrow(refusedForAll.get());
                rethrow(state);
                // Held while minting, so callers that all find this login's token stale
                // send one request. Another login has another slot and does not wait.
                try {
                    if (!slot.lock.tryLock(timeout.toNanos(), NANOSECONDS)) {
                        throw Refusal.UNREACHABLE.failure();
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Refusal.UNREACHABLE.failure();
                }
                try {
                    // Another caller may have finished while this one waited.
                    state = slot.state.get();
                    if (state instanceof Token token && fresh(token)) {
                        return Optional.of(token.value());
                    }
                    rethrow(refusedForAll.get());
                    rethrow(state);
                    return switch (exchange(subject, Duration.ofNanos(deadline - System.nanoTime()))) {
                        case Token token -> {
                            slot.state.set(token);
                            yield Optional.of(token.value());
                        }
                        case Failure failure -> {
                            Refused refused = new Refused(failure.refusal(), ticker.read() + failure.retention().toNanos());
                            switch (failure.reach()) {
                                case LOGIN -> slot.state.set(refused);
                                case ALL -> refusedForAll.set(refused);
                                case NOT_KEPT -> {}
                            }
                            throw failure.refusal().failure();
                        }
                    };
                }
                finally {
                    slot.lock.unlock();
                }
            }

            @Override
            public void invalidate(String token)
            {
                // Only this login's slot, and only while it still holds this token.
                // Lock-free, so dropping a refused token never waits on a mint.
                Slot slot = slots.getIfPresent(new Key(subject, scope));
                if (slot == null) {
                    return;
                }
                State state = slot.state.get();
                if (state instanceof Token current && current.value().equals(token)) {
                    slot.state.compareAndSet(state, null);
                }
            }

            @Override
            public String toString()
            {
                return "LoginTokens[redacted]";
            }
        }

        private boolean fresh(Token token)
        {
            return ticker.read() - token.refreshAtNanos() < 0;
        }

        private void rethrow(State state)
        {
            if (state instanceof Refused refused && ticker.read() - refused.untilNanos() < 0) {
                throw refused.refusal().failure();
            }
        }

        /**
         * One request for one subject. The answer is a token for that subject or a
         * refusal; nothing of the answer says which login it belongs to.
         */
        private Attempt exchange(String subject, Duration remaining)
        {
            boolean cutShort = remaining.compareTo(MINT_TIMEOUT) < 0;
            Duration timeout = cutShort ? remaining : MINT_TIMEOUT;
            if (timeout.isNegative() || timeout.isZero()) {
                return new Failure(Refusal.UNREACHABLE, Reach.NOT_KEPT, Duration.ZERO);
            }
            StringBuilder form = new StringBuilder()
                    .append("grant_type=").append(form(GRANT_TYPE))
                    .append("&subject_token=").append(form(subject))
                    .append("&subject_token_type=").append(form(subjectTokenType))
                    .append("&audience=").append(form(audience));
            scope.ifPresent(value -> form.append("&scope=").append(form(value)));
            HttpResponse<String> response;
            CompletableFuture<HttpResponse<String>> future = null;
            try {
                HttpRequest request = HttpRequest.newBuilder(tokenUri)
                        .timeout(timeout)
                        .header("Authorization", basicAuth)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("Accept", "application/json")
                        .header("User-Agent", HoglakeClient.USER_AGENT)
                        .POST(HttpRequest.BodyPublishers.ofString(form.toString()))
                        .build();
                // HttpRequest.timeout bounds receiving the headers, not the body. The get()
                // bounds both, and the cancel aborts an exchange that stalls in its body.
                future = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
                response = future.get(timeout.toNanos(), NANOSECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Failure(Refusal.UNREACHABLE, Reach.NOT_KEPT, Duration.ZERO);
            }
            catch (ExecutionException | TimeoutException | RuntimeException e) {
                // No cause is kept: an error of the HTTP client can name the endpoint
                boolean timedOut = e instanceof TimeoutException || e.getCause() instanceof HttpTimeoutException;
                if (timedOut && cutShort) {
                    // A caller with little time left says nothing about the endpoint
                    return new Failure(Refusal.UNREACHABLE, Reach.NOT_KEPT, Duration.ZERO);
                }
                return new Failure(Refusal.UNREACHABLE, Reach.ALL, UNAVAILABLE_RETENTION);
            }
            finally {
                if (future != null) {
                    future.cancel(true);
                }
            }
            int status = response.statusCode();
            if (status == 200) {
                return issued(response.body());
            }
            if (status < 400) {
                // Redirects are not followed, and no other answer is a token
                return new Failure(Refusal.INVALID_RESPONSE, Reach.NOT_KEPT, Duration.ZERO);
            }
            // Only the error word is read. The rest of the body reaches nothing.
            Optional<String> error = errorWord(response.body());
            log.debug("Token exchange for the posthog schema was refused: HTTP %s%s", status, error.map(word -> " (" + word + ")").orElse(""));
            if (status == 429 || status >= 500) {
                return new Failure(Refusal.BUSY, Reach.ALL, retryAfter(response));
            }
            return switch (error.orElse("")) {
                case "invalid_grant" -> new Failure(Refusal.LOGIN_REFUSED, Reach.LOGIN, REFUSAL_RETENTION);
                case "unsupported_grant_type" -> new Failure(Refusal.NOT_OFFERED, Reach.ALL, REFUSAL_RETENTION);
                case "unauthorized_client" -> new Failure(Refusal.CLIENT_NOT_ALLOWED, Reach.ALL, REFUSAL_RETENTION);
                case "invalid_client" -> new Failure(Refusal.CLIENT_REFUSED, Reach.ALL, REFUSAL_RETENTION);
                default -> new Failure(Refusal.REQUEST_REFUSED, Reach.LOGIN, REFUSAL_RETENTION);
            };
        }

        private Attempt issued(String body)
        {
            Failure invalid = new Failure(Refusal.INVALID_RESPONSE, Reach.NOT_KEPT, Duration.ZERO);
            JsonNode answer;
            try {
                answer = mapper.readTree(body);
            }
            catch (IOException | RuntimeException e) {
                // No cause is kept: a parser error can quote the body
                return invalid;
            }
            if (answer == null) {
                return invalid;
            }
            JsonNode accessToken = answer.path("access_token");
            JsonNode expiresIn = answer.path("expires_in");
            JsonNode issuedTokenType = answer.path("issued_token_type");
            JsonNode tokenType = answer.path("token_type");
            if (!accessToken.isTextual() || !BEARER_TOKEN.matcher(accessToken.asText()).matches()) {
                return invalid;
            }
            if (!expiresIn.isIntegralNumber() || !expiresIn.canConvertToLong() || expiresIn.asLong() <= 0) {
                return invalid;
            }
            // The issuer sets this on an exchange and not on a client-credentials grant.
            // Without it the endpoint did not treat the request as an exchange, and the
            // token could be the catalog's own.
            if (!issuedTokenType.isTextual() || !issuedTokenType.asText().equals(ISSUED_TOKEN_TYPE)) {
                return invalid;
            }
            if (!tokenType.isTextual() || !tokenType.asText().equalsIgnoreCase("Bearer")) {
                return invalid;
            }
            Duration lifetime = Duration.ofSeconds(Math.min(expiresIn.asLong(), MAX_LIFETIME.toSeconds()));
            Duration margin = lifetime.dividedBy(5);
            if (REFRESH_MARGIN.compareTo(margin) < 0) {
                margin = REFRESH_MARGIN;
            }
            return new Token(accessToken.asText(), ticker.read() + lifetime.minus(margin).toNanos());
        }

        private Optional<String> errorWord(String body)
        {
            try {
                JsonNode answer = mapper.readTree(body);
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
                return UNAVAILABLE_RETENTION;
            }
            long seconds;
            try {
                seconds = Long.parseLong(header.get().trim());
            }
            catch (NumberFormatException e) {
                return UNAVAILABLE_RETENTION;
            }
            return Duration.ofSeconds(Math.clamp(seconds, 0, MAX_UNAVAILABLE_RETENTION.toSeconds()));
        }

        private static String form(String value)
        {
            return URLEncoder.encode(value, UTF_8);
        }

        /**
         * What a request is cached under. The scope is the same for every request of
         * one instance today; it is part of the key so that a second scope could
         * never share a slot with the first.
         */
        private record Key(String subject, Optional<String> scope)
        {
            private Key
            {
                requireNonNull(subject, "subject is null");
                requireNonNull(scope, "scope is null");
            }

            @Override
            public String toString()
            {
                return "Key[redacted]";
            }
        }

        private static final class Slot
        {
            private final ReentrantLock lock = new ReentrantLock();
            private final AtomicReference<State> state = new AtomicReference<>();
        }

        private sealed interface State
                permits Token, Refused {}

        private sealed interface Attempt
                permits Token, Failure {}

        private record Token(String value, long refreshAtNanos)
                implements State, Attempt
        {
            @Override
            public String toString()
            {
                return "Token[redacted]";
            }
        }

        private record Refused(Refusal refusal, long untilNanos)
                implements State {}

        private record Failure(Refusal refusal, Reach reach, Duration retention)
                implements Attempt {}

        /**
         * Who a refusal is remembered for.
         */
        private enum Reach
        {
            LOGIN,
            ALL,
            NOT_KEPT,
        }

        /**
         * Every way a login can fail to get a token, with the only text a user sees
         * for it. No message carries anything of the request or of the answer.
         */
        private enum Refusal
        {
            LOGIN_REFUSED(PERMISSION_DENIED, "Access Denied: Cannot read the posthog schema with this login"),
            NOT_OFFERED(HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service does not issue per-login tokens"),
            CLIENT_NOT_ALLOWED(HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: this catalog's credential may not request per-login tokens"),
            CLIENT_REFUSED(HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service refused this catalog's credential"),
            REQUEST_REFUSED(HOGLAKE_LOGIN_TOKEN_REFUSED, "The posthog schema is not available: the token service refused the request"),
            BUSY(HOGLAKE_CATALOG_UNAVAILABLE, "The token service for the posthog schema is busy or unavailable; retry later"),
            UNREACHABLE(HOGLAKE_CATALOG_UNAVAILABLE, "The token service for the posthog schema cannot be reached; retry later"),
            INVALID_RESPONSE(HOGLAKE_INVALID_RESPONSE, "The token service for the posthog schema answered outside its contract");

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
    }
}
