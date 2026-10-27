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
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Where the connector's bearer token for the Hoglake REST API comes from.
 *
 * Hoglake verifies tokens; it does not mint them. The issuer is the
 * managed-warehouse control plane (hogtower's {@code POST /oauth/token}),
 * which is also what writes this catalog's properties, so the client
 * credentials arrive with the catalog and nothing here is long-lived.
 */
public interface HoglakeTokenSource
{
    /**
     * No credential: the pre-auth behaviour, and what an unconfigured catalog gets.
     */
    HoglakeTokenSource NONE = new HoglakeTokenSource()
    {
        @Override
        public Optional<String> token()
        {
            return Optional.empty();
        }

        @Override
        public void invalidate(String token) {}
    };

    /**
     * The current access token, minting one if none is cached or it is near expiry.
     */
    Optional<String> token();

    /**
     * Drop [token] if it is still the cached one, so the next {@link #token()}
     * mints. Called after Hoglake answered 401 to it: the token was revoked,
     * signed by a key this Hoglake instance has not loaded yet, or expired
     * early by clock skew, and in every case a fresh one is the fix.
     */
    void invalidate(String token);

    /**
     * RFC 6749 §4.4 client credentials, authenticated with HTTP Basic
     * (§2.3.1), cached until shortly before {@code expires_in}.
     */
    final class ClientCredentials
            implements HoglakeTokenSource
    {
        // Refresh this far before expiry, capped at a fifth of the lifetime so a
        // short-lived test token is not refreshed on every call.
        private static final Duration REFRESH_MARGIN = Duration.ofSeconds(60);
        private static final Duration MINT_TIMEOUT = Duration.ofSeconds(10);

        private final URI tokenUri;
        private final String basicAuth;
        private final String audience;
        private final HttpClient httpClient;
        private final Ticker ticker;
        private final ObjectMapper mapper = new ObjectMapper();

        private String cached;
        private long refreshAtNanos;

        public ClientCredentials(URI tokenUri, String clientId, String clientSecret, String audience, HttpClient httpClient)
        {
            this(tokenUri, clientId, clientSecret, audience, httpClient, Ticker.systemTicker());
        }

        @VisibleForTesting
        ClientCredentials(URI tokenUri, String clientId, String clientSecret, String audience, HttpClient httpClient, Ticker ticker)
        {
            this.tokenUri = requireNonNull(tokenUri, "tokenUri is null");
            // §2.3.1: both halves are form-urlencoded before being joined.
            this.basicAuth = "Basic " + Base64.getEncoder().encodeToString(
                    (form(requireNonNull(clientId, "clientId is null")) + ":" + form(requireNonNull(clientSecret, "clientSecret is null"))).getBytes(UTF_8));
            this.audience = requireNonNull(audience, "audience is null");
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        @Override
        public synchronized Optional<String> token()
        {
            // Synchronized: concurrent splits that all find the token stale must
            // produce one mint, not one each.
            if (cached == null || ticker.read() - refreshAtNanos >= 0) {
                mint();
            }
            return Optional.of(cached);
        }

        @Override
        public synchronized void invalidate(String token)
        {
            if (token.equals(cached)) {
                cached = null;
            }
        }

        private void mint()
        {
            HttpRequest request = HttpRequest.newBuilder(tokenUri)
                    .timeout(MINT_TIMEOUT)
                    .header("Authorization", basicAuth)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Accept", "application/json")
                    .header("User-Agent", HoglakeClient.USER_AGENT)
                    .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&audience=" + form(audience)))
                    .build();
            HttpResponse<String> response;
            try {
                response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                        .get(MINT_TIMEOUT.toNanos(), NANOSECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "interrupted minting a Hoglake access token", e);
            }
            catch (Exception e) {
                throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "could not reach the Hoglake token endpoint " + tokenUri, e);
            }
            if (response.statusCode() != 200) {
                // The body is an RFC 6749 §5.2 error object with no secrets in it;
                // its `error` code is what tells invalid_client from a 503.
                throw new TrinoException(
                        HOGLAKE_CATALOG_UNAVAILABLE,
                        "Hoglake token endpoint " + tokenUri + " answered HTTP " + response.statusCode() + ": " + response.body());
            }
            try {
                JsonNode body = mapper.readTree(response.body());
                String token = body.path("access_token").asText("");
                long expiresIn = body.path("expires_in").asLong(0);
                if (token.isEmpty() || expiresIn <= 0) {
                    throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Hoglake token endpoint returned no usable token");
                }
                Duration lifetime = Duration.ofSeconds(expiresIn);
                Duration margin = REFRESH_MARGIN.compareTo(lifetime.dividedBy(5)) < 0 ? REFRESH_MARGIN : lifetime.dividedBy(5);
                cached = token;
                refreshAtNanos = ticker.read() + lifetime.minus(margin).toNanos();
            }
            catch (IOException e) {
                throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "malformed response from the Hoglake token endpoint", e);
            }
        }

        private static String form(String value)
        {
            return URLEncoder.encode(value, UTF_8);
        }
    }
}
