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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.hoglake.HoglakeSharedSessionContext.Placement;
import io.trino.plugin.hoglake.HoglakeSharedSessionContext.Predicate;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.ECFieldFp;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.time.Clock;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_HEADER_BYTES;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_SIGNING_KEYS;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_TOKEN_BYTES;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

final class HoglakeSharedSessionVerifier
{
    static final String SESSION_TOKEN = "internal$hoglake_session_jwt";
    private static final long RELOAD_NANOS = SECONDS.toNanos(30);
    private static final Pattern BASE64URL = Pattern.compile("[A-Za-z0-9_-]+");
    private static final Pattern CATALOG = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");
    private static final Pattern TENANT = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Pattern PREDICATE_ID = Pattern.compile("[1-9][0-9]{0,18}");
    private static final Set<String> PRIVATE_KEY_FIELDS = ImmutableSet.of("d", "p", "q", "dp", "dq", "qi", "oth", "k");
    private static final Set<String> PROFILE_FIELDS = ImmutableSet.of("profile", "aws", "primary_catalog", "remote_schemas", "version", "purpose", "sid", "generation", "session_deadline", "grants");
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final String issuer;
    private final Path jwksFile;
    private final Map<UUID, String> backends;
    private final long skewSeconds;
    private final long maxLifetimeSeconds;
    private final Clock clock;
    private final LongSupplier ticker;
    private volatile Map<String, PublicKey> keys;
    private long lastReload;

    @Inject
    public HoglakeSharedSessionVerifier(HoglakeSharedConfig config)
    {
        this(config, Clock.systemUTC(), System::nanoTime);
    }

    HoglakeSharedSessionVerifier(HoglakeSharedConfig config, Clock clock, LongSupplier ticker)
    {
        try {
            require(config.isMultiTenant());
            issuer = config.getIssuer();
            require(issuer != null && !issuer.isBlank());
            jwksFile = config.getJwksFile().toPath();
            require(Files.isRegularFile(jwksFile));
            skewSeconds = seconds(config.getClockSkew());
            maxLifetimeSeconds = seconds(config.getScopedMaxLifetime());
            require(skewSeconds >= 0 && skewSeconds <= 600);
            require(maxLifetimeSeconds > 0 && maxLifetimeSeconds <= 900);
            backends = parseBackends(config.getBackendBindings());
            this.clock = clock;
            this.ticker = ticker;
            keys = readKeys();
            lastReload = ticker.getAsLong();
        }
        catch (Exception _) {
            // Do not retain parser exceptions or file paths in the failure graph.
            throw new IllegalArgumentException("Invalid shared Hoglake verification configuration");
        }
    }

    public HoglakeSharedSessionContext verify(ConnectorSession session)
    {
        try {
            ConnectorIdentity identity = session.getIdentity();
            String token = identity.getExtraCredentials().get(SESSION_TOKEN);
            require(token != null && token.length() <= MAX_TOKEN_BYTES);
            String[] segments = token.split("\\.", -1);
            require(segments.length == 3);
            // The alphabet check also establishes one byte per character.
            for (String segment : segments) {
                require(BASE64URL.matcher(segment).matches());
            }
            JsonNode header = object(decode(segments[0], MAX_HEADER_BYTES));
            require(text(header, "alg").equals("ES256"));
            String keyId = text(header, "kid");
            if (header.has("crit")) {
                require(header.get("crit").isArray() && header.get("crit").isEmpty());
            }
            // These headers cannot supply keys or change the compact payload encoding.
            require(!header.has("jku") && !header.has("jwk") && !header.has("x5u") && !header.has("b64"));
            reloadIfDue();
            PublicKey key = keys.get(keyId);
            require(key != null);
            byte[] signature = decode(segments[2], 64);
            require(signature.length == 64);
            Signature verifier = Signature.getInstance("SHA256withECDSAinP1363Format");
            verifier.initVerify(key);
            verifier.update((segments[0] + "." + segments[1]).getBytes(US_ASCII));
            require(verifier.verify(signature));

            // No claim is parsed or trusted before signature verification.
            JsonNode claims = object(decode(segments[1], MAX_TOKEN_BYTES));
            for (String field : PROFILE_FIELDS) {
                require(!claims.has(field));
            }
            require(text(claims, "iss").equals(issuer));
            text(claims, "sub");
            text(claims, "jti");
            text(claims, "org");
            require(text(claims, "ten").equals("scoped"));
            validateTimes(claims);
            if (claims.has("dlg")) {
                require(claims.get("dlg").isBoolean());
            }
            if (claims.has("cli")) {
                text(claims, "cli");
            }
            String principal = text(claims, "usr");
            require(!principal.isBlank() && principal.length() <= 200);
            require(identity.getPrincipal().isPresent());
            require(principal.equals(identity.getPrincipal().orElseThrow().getName()));
            String user = identity.getUser();
            require(sameDatabase(principal, user));

            JsonNode catalogs = claims.get("cat");
            require(catalogs != null && catalogs.isObject() && catalogs.size() == 1);
            String catalog = catalogs.fieldNames().next();
            require(CATALOG.matcher(catalog).matches());
            Set<String> scopes = scopes(catalogs.get(catalog));
            validateScopeClaim(claims, catalog, scopes);
            Placement placement = placement(claims);
            validateAudience(claims.get("aud"), placement.kind().equals("hoglite"));
            Map<String, Predicate> predicates = predicates(claims.get("prd"));
            return new HoglakeSharedSessionContext(catalog, token, principal, user, scopes, predicates, placement);
        }
        catch (Exception _) {
            // A refusal carries no token, claim value, parser cause, or suppressed exception.
            throw new TrinoException(PERMISSION_DENIED, "Shared Hoglake session authority is invalid or unavailable");
        }
    }

    private static long seconds(Duration duration)
    {
        require(duration != null);
        double value = duration.getValue(SECONDS);
        require(Double.isFinite(value) && value == Math.rint(value));
        require(value >= 0 && value <= 900);
        return (long) value;
    }

    private void validateTimes(JsonNode claims)
    {
        long expiry = integer(claims, "exp");
        long issued = integer(claims, "iat");
        long now = clock.instant().getEpochSecond();
        require(now <= Math.addExact(expiry, skewSeconds));
        require(issued <= Math.addExact(now, skewSeconds));
        long lifetime = Math.subtractExact(expiry, issued);
        require(lifetime > 0 && lifetime <= maxLifetimeSeconds);
        if (claims.has("nbf")) {
            require(integer(claims, "nbf") <= Math.addExact(now, skewSeconds));
        }
    }

    private static boolean sameDatabase(String principal, String user)
    {
        int principalSeparator = principal.indexOf('.');
        int userSeparator = user.indexOf('.');
        return principalSeparator > 0 && principalSeparator < principal.length() - 1 &&
                userSeparator == principalSeparator && userSeparator < user.length() - 1 &&
                principal.substring(0, principalSeparator).equals(user.substring(0, userSeparator));
    }

    private static Set<String> scopes(JsonNode value)
    {
        require(value != null && value.isArray() && !value.isEmpty());
        ImmutableSet.Builder<String> scopes = ImmutableSet.builder();
        for (JsonNode scope : value) {
            require(scope.isTextual());
            String word = scope.textValue();
            require(word.equals("read") || word.equals("write"));
            scopes.add(word);
        }
        // Both accepted grants include READ under the generic scope ordering.
        return scopes.build();
    }

    private static void validateScopeClaim(JsonNode claims, String catalog, Set<String> expected)
    {
        if (!claims.has("scope")) {
            return;
        }
        JsonNode scope = claims.get("scope");
        require(scope.isTextual());
        Map<String, ImmutableSet.Builder<String>> grants = new LinkedHashMap<>();
        for (String value : scope.textValue().split(" ")) {
            if (!value.startsWith("hoglake:")) {
                continue;
            }
            String[] parts = value.split(":", -1);
            require(parts.length == 3 && CATALOG.matcher(parts[1]).matches());
            require(parts[2].equals("read") || parts[2].equals("write"));
            grants.computeIfAbsent(parts[1], _ -> ImmutableSet.builder()).add(parts[2]);
        }
        // Foreign-only scopes supply no alternate Hoglake grants.
        if (!grants.isEmpty()) {
            require(grants.size() == 1 && grants.containsKey(catalog));
            require(grants.get(catalog).build().equals(expected));
        }
    }

    private Placement placement(JsonNode claims)
    {
        JsonNode routing = claims.get("rtg");
        require(routing != null && routing.isObject());
        String kind = text(routing, "b");
        require(kind.equals("postgres") || kind.equals("hoglite"));
        UUID backendId = canonicalUuid(text(routing, "be"));
        require(kind.equals(backends.get(backendId)));
        if (kind.equals("postgres")) {
            require(routing.size() == 2);
            return new Placement(backendId, kind, Optional.empty());
        }
        require(routing.size() == 3);
        String tenant = text(routing, "t");
        require(TENANT.matcher(tenant).matches() && !tenant.startsWith(".") && !tenant.contains(".."));
        return new Placement(backendId, kind, Optional.of(tenant));
    }

    private static void validateAudience(JsonNode audience, boolean hoglite)
    {
        require(audience != null);
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        int count;
        if (audience.isTextual()) {
            values.add(audience.textValue());
            count = 1;
        }
        else {
            require(audience.isArray());
            count = audience.size();
            for (JsonNode value : audience) {
                require(value.isTextual());
                values.add(value.textValue());
            }
        }
        Set<String> actual = values.build();
        require(actual.size() == count);
        Set<String> expected = ImmutableSet.of("hoglake");
        if (hoglite) {
            expected = ImmutableSet.of("hoglake", "hoglite");
        }
        require(actual.equals(expected));
    }

    private static Map<String, Predicate> predicates(JsonNode predicates)
    {
        require(predicates != null && predicates.isObject() && predicates.size() == 1 && predicates.has("team_id"));
        JsonNode values = predicates.get("team_id");
        require(values.isObject() && values.size() >= 1 && values.size() <= 200);
        ImmutableSet.Builder<String> read = ImmutableSet.builder();
        ImmutableSet.Builder<String> write = ImmutableSet.builder();
        for (Map.Entry<String, JsonNode> entry : values.properties()) {
            String value = entry.getKey();
            if (value.equals("*")) {
                require(values.size() == 1);
            }
            else {
                require(PREDICATE_ID.matcher(value).matches());
                require(Long.parseLong(value) > 0);
            }
            JsonNode permission = entry.getValue();
            require(permission.isTextual());
            String word = permission.textValue();
            require(word.equals("read") || word.equals("write"));
            read.add(value);
            if (word.equals("write")) {
                write.add(value);
            }
        }
        return ImmutableMap.of("team_id", new Predicate(read.build(), write.build()));
    }

    private static Map<UUID, String> parseBackends(String bindings)
            throws IOException
    {
        require(bindings != null && bindings.length() <= MAX_VERIFICATION_FILE_BYTES);
        byte[] bytes = bindings.getBytes(UTF_8);
        require(bytes.length <= MAX_VERIFICATION_FILE_BYTES);
        JsonNode root = object(bytes);
        require(!root.isEmpty());
        ImmutableMap.Builder<UUID, String> result = ImmutableMap.builder();
        for (Map.Entry<String, JsonNode> entry : root.properties()) {
            require(entry.getValue().isTextual());
            String kind = entry.getValue().textValue();
            require(kind.equals("postgres") || kind.equals("hoglite"));
            result.put(canonicalUuid(entry.getKey()), kind);
        }
        return result.buildOrThrow();
    }

    private synchronized void reloadIfDue()
    {
        long now = ticker.getAsLong();
        if (now - lastReload < RELOAD_NANOS) {
            return;
        }
        lastReload = now;
        try {
            keys = readKeys();
        }
        catch (Exception _) {
            // A failed mounted-file reload retains the previous public keys.
        }
    }

    private Map<String, PublicKey> readKeys()
            throws IOException, GeneralSecurityException
    {
        require(Files.isRegularFile(jwksFile));
        require(Files.size(jwksFile) <= MAX_VERIFICATION_FILE_BYTES);
        byte[] bytes;
        try (var input = Files.newInputStream(jwksFile)) {
            bytes = input.readNBytes(MAX_VERIFICATION_FILE_BYTES + 1);
        }
        require(bytes.length <= MAX_VERIFICATION_FILE_BYTES);
        JsonNode root = object(bytes);
        JsonNode entries = root.get("keys");
        require(entries != null && entries.isArray());
        ImmutableMap.Builder<String, PublicKey> result = ImmutableMap.builder();
        int count = 0;
        for (JsonNode entry : entries) {
            require(entry.isObject());
            for (String field : PRIVATE_KEY_FIELDS) {
                require(!entry.has(field));
            }
            if (entry.has("use")) {
                if (!text(entry, "use").equals("sig")) {
                    continue;
                }
            }
            if (entry.has("alg")) {
                if (!text(entry, "alg").equals("ES256")) {
                    continue;
                }
            }
            if (entry.has("key_ops")) {
                JsonNode operations = entry.get("key_ops");
                require(operations.isArray());
                require(operations.size() == 1 && operations.get(0).isTextual() && operations.get(0).textValue().equals("verify"));
            }
            require(text(entry, "kty").equals("EC") && text(entry, "crv").equals("P-256"));
            require(++count <= MAX_SIGNING_KEYS);
            result.put(text(entry, "kid"), publicKey(entry));
        }
        require(count > 0);
        return result.buildOrThrow();
    }

    private static PublicKey publicKey(JsonNode entry)
            throws GeneralSecurityException
    {
        byte[] x = decode(text(entry, "x"), 32);
        byte[] y = decode(text(entry, "y"), 32);
        require(x.length == 32 && y.length == 32);
        AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
        parameters.init(new ECGenParameterSpec("secp256r1"));
        ECParameterSpec curve = parameters.getParameterSpec(ECParameterSpec.class);
        BigInteger pointX = new BigInteger(1, x);
        BigInteger pointY = new BigInteger(1, y);
        BigInteger prime = ((ECFieldFp) curve.getCurve().getField()).getP();
        require(pointX.compareTo(prime) < 0 && pointY.compareTo(prime) < 0);
        // Validate the public point before passing it to the standard JCA verifier.
        require(pointY.modPow(BigInteger.TWO, prime).equals(pointX.modPow(BigInteger.valueOf(3), prime)
                .add(curve.getCurve().getA().multiply(pointX)).add(curve.getCurve().getB()).mod(prime)));
        return KeyFactory.getInstance("EC").generatePublic(new ECPublicKeySpec(new ECPoint(pointX, pointY), curve));
    }

    private static UUID canonicalUuid(String value)
    {
        UUID id = UUID.fromString(value);
        require(id.toString().equals(value));
        return id;
    }

    private static byte[] decode(String segment, int maxBytes)
    {
        require(segment.length() <= (maxBytes * 4L + 2) / 3);
        require(BASE64URL.matcher(segment).matches());
        byte[] bytes = Base64.getUrlDecoder().decode(segment);
        require(bytes.length <= maxBytes);
        require(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(segment));
        return bytes;
    }

    private static JsonNode object(byte[] bytes)
            throws IOException
    {
        JsonNode root = JSON.readTree(bytes);
        require(root != null && root.isObject());
        return root;
    }

    private static String text(JsonNode object, String field)
    {
        JsonNode value = object.get(field);
        require(value != null && value.isTextual() && !value.textValue().isEmpty());
        return value.textValue();
    }

    private static long integer(JsonNode object, String field)
    {
        JsonNode value = object.get(field);
        require(value != null && value.isIntegralNumber() && value.canConvertToLong());
        return value.longValue();
    }

    private static void require(boolean condition)
    {
        if (!condition) {
            throw new IllegalArgumentException("Invalid shared Hoglake authority");
        }
    }
}
