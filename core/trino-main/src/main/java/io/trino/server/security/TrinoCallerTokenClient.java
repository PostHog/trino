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
package io.trino.server.security;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.net.MediaType;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.hash.Hashing.hmacSha256;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.ACCEPT_ENCODING;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_ENCODING;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpVersion.HTTP_1;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StreamingBodyGenerator.streamingBodyGenerator;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TrinoCallerTokenClient
{
    private static final String GRANT_TYPE = "urn:posthog:params:oauth:grant-type:trino-login";
    private static final int MAX_CELL_TOKEN_FILE_SIZE = 16 * 1024;
    private static final int MAX_GRANT_REQUEST_SIZE = 16 * 1024;
    private static final int MAX_REUSED_TOKENS = 10_000;
    private static final BigInteger MAX_EXPIRES_IN = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final Pattern BEARER_CREDENTIAL = Pattern.compile("[A-Za-z0-9._~+/-]+=*");
    private static final MediaType FORM = MediaType.create("application", "x-www-form-urlencoded").withCharset(UTF_8);
    private static final MediaType JSON = MediaType.create("application", "json");
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .disable(CANONICALIZE_FIELD_NAMES)
            .build();

    private final HttpClient httpClient;
    private final boolean enabled;
    private final URI issuerUri;
    private final Path cellTokenFile;
    private final Duration timeout;
    private final DataSize maxResponseSize;
    private final long reusePeriodNanos;
    private final Ticker ticker;
    // Present only with a reuse period. A key holds no password, see evidence().
    private final Optional<Cache<Evidence, ReusableToken>> reusableTokens;
    private final HashFunction evidenceHasher;

    @Inject
    public TrinoCallerTokenClient(@ForTrinoCallerTokens HttpClient httpClient, TrinoCallerTokenConfig config)
    {
        this(httpClient, config, Ticker.systemTicker());
    }

    @VisibleForTesting
    TrinoCallerTokenClient(HttpClient httpClient, TrinoCallerTokenConfig config, Ticker ticker)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        requireNonNull(config, "config is null");
        if (!config.isIssuerUriValid() || !config.isIssuerTransportValid() || !config.isCellTokenFileValid()) {
            throw new IllegalArgumentException("Invalid caller token configuration");
        }
        enabled = config.isEnabled();
        issuerUri = config.getIssuerUri();
        cellTokenFile = config.getCellTokenFile() == null ? null : config.getCellTokenFile().toPath();
        timeout = requireNonNull(config.getTimeout(), "timeout is null");
        maxResponseSize = requireNonNull(config.getMaxResponseSize(), "maxResponseSize is null");
        if (timeout.getValue(MILLISECONDS) < 1 || timeout.getValue(MILLISECONDS) > 30_000 ||
                maxResponseSize.toBytes() < 1 || maxResponseSize.toBytes() > 1024 * 1024) {
            throw new IllegalArgumentException("Invalid caller token bounds");
        }
        Duration reusePeriod = requireNonNull(config.getReusePeriod(), "reusePeriod is null");
        if (reusePeriod.getValue(MILLISECONDS) < 0 || reusePeriod.getValue(SECONDS) > 300) {
            throw new IllegalArgumentException("Invalid caller token reuse period");
        }
        this.ticker = requireNonNull(ticker, "ticker is null");
        reusePeriodNanos = reusePeriod.roundTo(NANOSECONDS);
        evidenceHasher = createEvidenceHasher();
        if (reusePeriodNanos == 0) {
            reusableTokens = Optional.empty();
        }
        else {
            reusableTokens = Optional.of(EvictableCacheBuilder.newBuilder()
                    .maximumSize(MAX_REUSED_TOKENS)
                    .expireAfterWrite(reusePeriodNanos, NANOSECONDS)
                    .ticker(ticker)
                    .build());
        }
    }

    /**
     * A token for this login and password. The caller has verified both for the
     * current request. With a reuse period, a token granted for the same login and
     * password is returned while it is reusable, and concurrent callers share one
     * grant request. A failed grant is never kept.
     */
    public String acquire(String verifiedQualifiedLogin, String requestPassword)
    {
        if (!enabled || verifiedQualifiedLogin == null || verifiedQualifiedLogin.isEmpty() || requestPassword == null || requestPassword.isEmpty()) {
            throw new AcquisitionException();
        }
        if (reusableTokens.isEmpty()) {
            return request(verifiedQualifiedLogin, requestPassword).token();
        }
        Cache<Evidence, ReusableToken> tokens = reusableTokens.get();
        try {
            Evidence evidence = evidence(verifiedQualifiedLogin, requestPassword);
            ReusableToken held = tokens.getIfPresent(evidence);
            if (held != null) {
                if (ticker.read() - held.reusableUntilNanos() < 0) {
                    return held.token();
                }
                tokens.invalidate(evidence);
            }
            // The loader holds the password only for the grant request. A token that
            // this call or a concurrent one was just granted is returned as it is.
            return uncheckedCacheGet(tokens, evidence, () -> request(verifiedQualifiedLogin, requestPassword)).token();
        }
        catch (RuntimeException _) {
            // Do not retain the loader's failure.
            throw new AcquisitionException();
        }
    }

    @VisibleForTesting
    Set<Object> reusableEntries()
    {
        return reusableTokens.map(tokens -> ImmutableSet.<Object>builder()
                        .addAll(tokens.asMap().keySet())
                        .addAll(tokens.asMap().values())
                        .build())
                .orElse(ImmutableSet.of());
    }

    private Evidence evidence(String login, String password)
    {
        // Preserve exact UTF-16 code units, including unpaired surrogates.
        // Length prefixes separate the login from the password.
        return new Evidence(login, evidenceHasher.newHasher()
                .putInt(login.length())
                .putUnencodedChars(login)
                .putInt(password.length())
                .putUnencodedChars(password)
                .hash());
    }

    private static HashFunction createEvidenceHasher()
    {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        try {
            return hmacSha256(key);
        }
        finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private ReusableToken request(String verifiedQualifiedLogin, String requestPassword)
    {
        try {
            String form = "grant_type=" + encode(GRANT_TYPE) +
                    "&username=" + encode(verifiedQualifiedLogin) +
                    "&password=" + encode(requestPassword);
            byte[] formBytes = form.getBytes(UTF_8);
            if (formBytes.length > MAX_GRANT_REQUEST_SIZE) {
                throw new AcquisitionException();
            }
            String cellToken = readCellToken();
            // Streaming content cannot rewind. The transport must not replay password evidence.
            try (var body = new ByteArrayInputStream(formBytes)) {
                Request request = preparePost()
                        .setUri(issuerUri)
                        .setHeader(AUTHORIZATION, "Bearer " + cellToken)
                        .setHeader(CONTENT_TYPE, FORM.toString())
                        .setHeader(ACCEPT, JSON.toString())
                        .setHeader(ACCEPT_ENCODING, "identity")
                        .setVersion(HTTP_1)
                        .setFollowRedirects(false)
                        .setRequestTimeout(timeout)
                        .setIdleTimeout(timeout)
                        .setMaxResponseContentLength(maxResponseSize)
                        .setBodyGenerator(streamingBodyGenerator(FORM, body))
                        .build();
                Grant grant = httpClient.execute(request, new GrantResponseHandler(toIntExact(maxResponseSize.toBytes())));
                // Never past half of the lifetime the issuer stated
                long reusableNanos = Math.min(reusePeriodNanos, SECONDS.toNanos(grant.expiresInSeconds()) / 2);
                return new ReusableToken(grant.token(), ticker.read() + reusableNanos);
            }
        }
        catch (IOException | RuntimeException _) {
            // Do not retain transport exceptions or issuer response contents.
            throw new AcquisitionException();
        }
    }

    private String readCellToken()
            throws IOException
    {
        if (!Files.isRegularFile(cellTokenFile) || !Files.isReadable(cellTokenFile)) {
            throw new AcquisitionException();
        }
        try (var input = Files.newInputStream(cellTokenFile)) {
            byte[] bytes = input.readNBytes(MAX_CELL_TOKEN_FILE_SIZE + 1);
            if (bytes.length > MAX_CELL_TOKEN_FILE_SIZE) {
                throw new AcquisitionException();
            }
            String token = new String(bytes, UTF_8).strip();
            if (!BEARER_CREDENTIAL.matcher(token).matches()) {
                throw new AcquisitionException();
            }
            return token;
        }
    }

    private static String encode(String value)
    {
        return URLEncoder.encode(value, UTF_8);
    }

    private static final class GrantResponseHandler
            implements ResponseHandler<Grant, RuntimeException>
    {
        private final int maxResponseBytes;

        public GrantResponseHandler(int maxResponseBytes)
        {
            this.maxResponseBytes = maxResponseBytes;
        }

        @Override
        public Grant handleException(Request request, Exception exception)
        {
            throw new AcquisitionException();
        }

        @Override
        public Grant handle(Request request, Response response)
        {
            if (response.getStatusCode() != 200) {
                throw new AcquisitionException();
            }
            List<String> encodings = response.getHeaders(CONTENT_ENCODING);
            if (!encodings.isEmpty() && (encodings.size() != 1 || !"identity".equalsIgnoreCase(encodings.getFirst()))) {
                throw new AcquisitionException();
            }
            List<String> contentTypes = response.getHeaders(CONTENT_TYPE);
            if (contentTypes.size() != 1) {
                throw new AcquisitionException();
            }
            MediaType contentType = MediaType.parse(contentTypes.getFirst());
            if (!contentType.is(JSON) || !contentType.charset().toJavaUtil().orElse(UTF_8).equals(UTF_8)) {
                throw new AcquisitionException();
            }
            try (var input = response.getInputStream()) {
                byte[] bytes = input.readNBytes(maxResponseBytes + 1);
                if (bytes.length > maxResponseBytes) {
                    throw new AcquisitionException();
                }
                return parseToken(bytes);
            }
            catch (IOException _) {
                throw new AcquisitionException();
            }
        }
    }

    private static Grant parseToken(byte[] bytes)
            throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new AcquisitionException();
            }
            String accessToken = null;
            boolean bearer = false;
            int expiresInSeconds = 0;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) {
                    throw new AcquisitionException();
                }
                String field = parser.currentName();
                JsonToken value = parser.nextToken();
                switch (field) {
                    case "access_token" -> {
                        if (value != JsonToken.VALUE_STRING) {
                            throw new AcquisitionException();
                        }
                        accessToken = parser.getText();
                    }
                    case "token_type" -> {
                        if (value != JsonToken.VALUE_STRING || !"Bearer".equalsIgnoreCase(parser.getText())) {
                            throw new AcquisitionException();
                        }
                        bearer = true;
                    }
                    case "expires_in" -> {
                        if (value != JsonToken.VALUE_NUMBER_INT || parser.getBigIntegerValue().signum() <= 0) {
                            throw new AcquisitionException();
                        }
                        expiresInSeconds = parser.getBigIntegerValue().min(MAX_EXPIRES_IN).intValueExact();
                    }
                    default -> throw new AcquisitionException();
                }
            }
            if (accessToken == null || !BEARER_CREDENTIAL.matcher(accessToken).matches() || !bearer || expiresInSeconds <= 0 || parser.nextToken() != null) {
                throw new AcquisitionException();
            }
            return new Grant(accessToken, expiresInSeconds);
        }
    }

    private record Grant(String token, int expiresInSeconds)
    {
        @Override
        public String toString()
        {
            return "Grant{redacted}";
        }
    }

    /**
     * What a reused token is found by: the login and a keyed MAC of the login and
     * password. The key is random, made at startup and kept by this client only.
     */
    private record Evidence(String login, HashCode identifier)
    {
        @Override
        public String toString()
        {
            return "Evidence{redacted}";
        }
    }

    private record ReusableToken(String token, long reusableUntilNanos)
    {
        @Override
        public String toString()
        {
            return "ReusableToken{redacted}";
        }
    }

    public static final class AcquisitionException
            extends RuntimeException
    {
        public AcquisitionException()
        {
            super("Caller token acquisition failed", null, false, false);
        }
    }
}
