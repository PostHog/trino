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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.BACKEND_ID;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.jwk;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.sign;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSharedSessionVerifier
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-11-17T12:00:00Z");
    private static final String PRINCIPAL = "synthetic-db.alice";
    private static final String CATALOG = "synthetic_catalog";
    private static final String KID = "synthetic-signing-key";

    @TempDir
    Path directory;

    private Path file;
    private KeyPair key;
    private HoglakeSharedConfig configuration;
    private AtomicLong ticker;
    private HoglakeSharedSessionVerifier verifier;

    @BeforeEach
    void setUp()
    {
        file = directory.resolve("public-jwks.json");
        key = keyPair();
        writeJwks(file, key, KID);
        configuration = config(file, URI.create("https://metadata.example.test"));
        ticker = new AtomicLong();
        verifier = verifier();
    }

    @Test
    void testVerifiedMappingAndExactToken()
    {
        String jwt = token(key, KID, validClaims());
        HoglakeSharedSessionContext context = verifier.verify(session("synthetic-db.mapped", PRINCIPAL, jwt));
        assertThat(context.catalog()).isEqualTo(CATALOG);
        assertThat(context.token()).isEqualTo(jwt);
        assertThat(context.principal()).isEqualTo(PRINCIPAL);
        assertThat(context.user()).isEqualTo("synthetic-db.mapped");
        assertThat(context.scopes()).containsExactly("read");
        assertThat(context.predicates().get("team_id").read()).containsExactly("7");
        assertThat(context.predicates().get("team_id").write()).isEmpty();
        assertThat(context.placement().tenant()).contains("opaque-tenant");
        assertThat(context.placement().backendId().toString()).isEqualTo(BACKEND_ID);
        assertThat(context.toString()).isEqualTo("HoglakeSharedSessionContext{redacted}");
        assertThat(context.placement().toString()).isEqualTo("Placement{redacted}");
        assertThat(context.predicates().get("team_id").toString()).isEqualTo("Predicate{redacted}");
        assertThatThrownBy(() -> context.scopes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> context.predicates().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> context.predicates().get("team_id").read().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPrincipalAndWarehouseBinding()
    {
        String jwt = token(key, KID, validClaims());
        for (String user : List.of("other-db.alice", "alice", ".alice", "synthetic-db.")) {
            assertDenied(() -> verifier.verify(session(user, PRINCIPAL, jwt)));
        }
        assertDenied(() -> verifier.verify(session(PRINCIPAL, null, jwt)));
        assertDenied(() -> verifier.verify(session(PRINCIPAL, "synthetic-db.other", jwt)));
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, null)));
        denyMutation(value -> value.remove("usr"));
        denyMutation(value -> value.put("usr", ""));
        denyMutation(value -> value.put("usr", " "));
        denyMutation(value -> value.put("usr", "x".repeat(201)));
    }

    @Test
    void testUserLimitUsesUtf16Units()
    {
        String principal = "synthetic-db." + "x".repeat(187);
        assertThat(principal).hasSize(200);
        String jwt = token(key, KID, claims(CATALOG, principal, "opaque-tenant", NOW));
        assertThat(verifier.verify(session(principal, principal, jwt)).principal()).isEqualTo(principal);
        String supplementary = "synthetic-db." + "\uD83D\uDE00".repeat(94);
        assertThat(supplementary).hasSize(201);
        String oversized = token(key, KID, claims(CATALOG, supplementary, "opaque-tenant", NOW));
        assertDenied(() -> verifier.verify(session(supplementary, supplementary, oversized)));
    }

    @Test
    void testMalformedRequiredClaims()
    {
        for (String field : List.of("iss", "sub", "jti", "org", "ten", "usr", "cat", "prd", "rtg", "aud", "iat", "exp")) {
            denyMutation(value -> value.remove(field));
            denyMutation(value -> value.put(field, null));
        }
        for (String field : List.of("sub", "jti", "org", "usr")) {
            denyMutation(value -> value.put(field, ""));
            denyMutation(value -> value.put(field, List.of("invalid")));
        }
        denyMutation(value -> value.put("iss", "https://other.example.test"));
        denyMutation(value -> value.put("ten", "tenant"));
        denyMutation(value -> value.put("ten", "operator"));
        denyMutation(value -> value.put("dlg", "false"));
        denyMutation(value -> value.put("cli", List.of("invalid")));
        for (String field : List.of("profile", "aws", "primary_catalog", "remote_schemas", "sid", "generation", "purpose", "version", "grants", "session_deadline")) {
            denyMutation(value -> value.put(field, "synthetic-experimental-value"));
        }
    }

    @Test
    void testTimesAndOverflow()
    {
        denyMutation(value -> value.put("exp", NOW.minusSeconds(61).getEpochSecond()));
        denyMutation(value -> value.put("iat", NOW.plusSeconds(61).getEpochSecond()));
        denyMutation(value -> value.put("nbf", NOW.plusSeconds(61).getEpochSecond()));
        denyMutation(value -> value.put("exp", NOW.plusSeconds(901).getEpochSecond()));
        denyMutation(value -> value.put("iat", Long.MIN_VALUE));
        denyMutation(value -> value.put("exp", Long.MAX_VALUE));
        denyMutation(value -> value.put("exp", new BigInteger("9223372036854775808")));
        denyMutation(value -> value.put("exp", NOW.minusSeconds(1).getEpochSecond()));
        for (String field : List.of("exp", "iat", "nbf")) {
            denyMutation(value -> value.put(field, 1.5));
            denyMutation(value -> value.put(field, "123"));
            denyMutation(value -> value.put(field, null));
        }
        Map<String, Object> boundary = validClaims();
        boundary.put("iat", NOW.minusSeconds(360).getEpochSecond());
        boundary.put("exp", NOW.minusSeconds(60).getEpochSecond());
        boundary.put("nbf", NOW.plusSeconds(60).getEpochSecond());
        assertThat(verify(boundary).catalog()).isEqualTo(CATALOG);
        Map<String, Object> maximum = validClaims();
        maximum.put("exp", NOW.plusSeconds(900).getEpochSecond());
        assertThat(verify(maximum).catalog()).isEqualTo(CATALOG);
    }

    @Test
    void testCatalogAndScopeAuthority()
    {
        for (Object catalog : List.of(
                Map.of(),
                List.of(CATALOG),
                Map.of("*", List.of("read")),
                Map.of("", List.of("read")),
                Map.of("Uppercase", List.of("read")),
                Map.of("a/b", List.of("read")),
                Map.of("a".repeat(64), List.of("read")),
                Map.of(CATALOG, List.of()),
                Map.of(CATALOG, "read"),
                Map.of(CATALOG, List.of("admin")),
                Map.of(CATALOG, List.of("read", "unknown")),
                Map.of(CATALOG, List.of(1)),
                Map.of(CATALOG, List.of("read"), "other", List.of("read")))) {
            denyMutation(value -> value.put("cat", catalog));
        }
        denyMutation(value -> value.put("scope", "hoglake:other:read"));
        denyMutation(value -> value.put("scope", "hoglake:" + CATALOG + ":write"));
        denyMutation(value -> value.put("scope", "hoglake:" + CATALOG + ":admin"));
        denyMutation(value -> value.put("scope", "hoglake:" + CATALOG + ":read:extra"));
        denyMutation(value -> value.put("scope", List.of("read")));
        Map<String, Object> agreed = validClaims();
        agreed.put("scope", "openid hoglake:" + CATALOG + ":read");
        assertThat(verify(agreed).catalog()).isEqualTo(CATALOG);
        agreed.put("cat", Map.of(CATALOG, List.of("write")));
        agreed.put("scope", "hoglake:" + CATALOG + ":write");
        assertThat(verify(agreed).scopes()).containsExactly("write");
    }

    @Test
    void testPredicateAuthority()
    {
        for (Object predicate : List.of(
                Map.of(),
                List.of("team_id"),
                Map.of("Team_id", Map.of("7", "read")),
                Map.of("team_id", Map.of(), "other", Map.of("8", "read")),
                Map.of("team_id", Map.of()),
                Map.of("team_id", List.of("7")))) {
            denyMutation(value -> value.put("prd", predicate));
        }
        for (String id : List.of("0", "-1", "+1", "01", "1.0", "9223372036854775808", " 7", "7 ", "")) {
            denyMutation(value -> value.put("prd", Map.of("team_id", Map.of(id, "read"))));
        }
        denyMutation(value -> value.put("prd", Map.of("team_id", Map.of("*", "read", "7", "read"))));
        denyMutation(value -> value.put("prd", Map.of("team_id", Map.of("7", "admin"))));
        denyMutation(value -> value.put("prd", Map.of("team_id", Map.of("7", 1))));
        Map<String, String> many = new LinkedHashMap<>();
        for (int index = 1; index <= 200; index++) {
            many.put(Integer.toString(index), "write");
        }
        Map<String, Object> maximum = validClaims();
        maximum.put("prd", Map.of("team_id", many));
        assertThat(verify(maximum).predicates().get("team_id").write()).hasSize(200);
        many.put("201", "read");
        denyMutation(value -> value.put("prd", Map.of("team_id", many)));
        Map<String, Object> wildcard = validClaims();
        wildcard.put("prd", Map.of("team_id", Map.of("*", "write")));
        assertThat(verify(wildcard).predicates().get("team_id").read()).containsExactly("*");
        assertThat(verify(wildcard).predicates().get("team_id").write()).containsExactly("*");
    }

    @Test
    void testRoutingAndExactAudiences()
    {
        for (Object audience : List.of(
                "hoglake",
                "hoglite",
                List.of("hoglake", "hoglite", "trino"),
                List.of("hoglake", "hoglake", "hoglite"),
                List.of("hoglake", 1),
                List.of(),
                Map.of("aud", "hoglake"))) {
            denyMutation(value -> value.put("aud", audience));
        }
        Map<String, Object> reordered = validClaims();
        reordered.put("aud", List.of("hoglite", "hoglake"));
        assertThat(verify(reordered).catalog()).isEqualTo(CATALOG);
        for (Object routing : List.of(
                Map.of(),
                List.of("hoglite"),
                Map.of("b", "unknown", "be", BACKEND_ID),
                Map.of("b", "postgres", "be", BACKEND_ID),
                Map.of("b", "hoglite", "be", BACKEND_ID),
                Map.of("b", "hoglite", "be", BACKEND_ID, "t", "opaque", "extra", "invalid"),
                Map.of("b", "hoglite", "be", "AAAAAAAA-2222-4333-8444-555555555555", "t", "opaque"),
                Map.of("b", "hoglite", "be", "aaaaaaaa-2222-4333-8444-555555555555", "t", "opaque"))) {
            denyMutation(value -> value.put("rtg", routing));
        }
        for (String tenant : List.of("", ".hidden", "has..dots", "slash/name", "x".repeat(129))) {
            denyMutation(value -> value.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", tenant)));
        }
        Map<String, Object> opaque = validClaims();
        opaque.put("rtg", Map.of("b", "hoglite", "be", BACKEND_ID, "t", "opaque.with-dots_1"));
        assertThat(verify(opaque).placement().tenant()).contains("opaque.with-dots_1");
        configuration.setBackendBindings("{\"%s\":\"postgres\"}".formatted(BACKEND_ID));
        verifier = verifier();
        Map<String, Object> postgres = validClaims();
        postgres.put("rtg", Map.of("b", "postgres", "be", BACKEND_ID));
        postgres.put("aud", "hoglake");
        assertThat(verify(postgres).placement().tenant()).isEmpty();
        postgres.put("aud", List.of("hoglake"));
        assertThat(verify(postgres).catalog()).isEqualTo(CATALOG);
        postgres.put("aud", List.of("hoglake", "hoglite"));
        assertDenied(() -> verify(postgres));
    }

    @Test
    void testCompactEncodingHeadersAndSignatures()
            throws Exception
    {
        String jwt = token(key, KID, validClaims());
        for (String invalid : List.of("", " " + jwt, jwt + " ", jwt + ".extra", jwt + "=", "a.b", "a..b", "a.b.c", "x".repeat(65_537))) {
            assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, invalid)));
        }
        String claims = JSON.writeValueAsString(validClaims());
        for (String header : List.of(
                "[]",
                "{}",
                "{\"alg\":\"none\",\"kid\":\"" + KID + "\"}",
                "{\"alg\":\"HS256\",\"kid\":\"" + KID + "\"}",
                "{\"alg\":\"ES256\",\"kid\":\"unknown\"}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"crit\":[\"unknown\"]}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"crit\":false}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"b64\":false}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"jku\":\"https://unused.example.test\"}",
                "{\"alg\":\"ES256\",\"kid\":\"" + KID + "\",\"padding\":\"" + "x".repeat(8192) + "\"}")) {
            String invalid = sign(key, header, claims);
            assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, invalid)));
        }
        String wrongSignature = token(keyPair(), KID, validClaims());
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, wrongSignature)));
        Map<String, Object> changed = validClaims();
        changed.put("org", "changed");
        String changedPayload = token(key, KID, changed).split("\\.")[1];
        String[] parts = jwt.split("\\.");
        String tampered = parts[0] + "." + changedPayload + "." + parts[2];
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, tampered)));
        String zeroSignature = parts[0] + "." + parts[1] + "." + "A".repeat(86);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, zeroSignature)));
        String paddedHeader = parts[0] + "=." + parts[1] + "." + parts[2];
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, paddedHeader)));
        String nonUrlAlphabet = "+" + jwt.substring(1);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, nonUrlAlphabet)));
    }

    @Test
    void testHeaderAndFileCeilings()
            throws Exception
    {
        String header = "{\"alg\":\"ES256\",\"kid\":\"%s\"}".formatted(KID);
        String boundedHeader = header + " ".repeat(HoglakeSharedConfig.MAX_HEADER_BYTES - header.length());
        String claims = JSON.writeValueAsString(validClaims());
        String accepted = sign(key, boundedHeader, claims);
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, accepted)).token()).isEqualTo(accepted);
        String refused = sign(key, boundedHeader + " ", claims);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, refused)));

        String document = Files.readString(file, UTF_8);
        Files.writeString(file, document + " ".repeat(HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES - document.length()), UTF_8);
        assertThat(verifier()).isNotNull();
        Files.writeString(file, Files.readString(file, UTF_8) + " ", UTF_8);
        assertBadConfiguration();
    }

    @Test
    void testDuplicateJsonKeys()
            throws Exception
    {
        String header = "{\"alg\":\"ES256\",\"kid\":\"%s\"}".formatted(KID);
        String original = JSON.writeValueAsString(validClaims());
        String duplicateClaim = original.substring(0, original.length() - 1) + ",\"usr\":\"synthetic-db.other\"}";
        String duplicateHeader = "{\"alg\":\"ES256\",\"alg\":\"ES256\",\"kid\":\"%s\"}".formatted(KID);
        String duplicatePredicate = original.replace("\"7\":\"read\"", "\"7\":\"read\",\"7\":\"write\"");
        for (String invalid : List.of(sign(key, header, duplicateClaim), sign(key, duplicateHeader, original), sign(key, header, duplicatePredicate), sign(key, header, original + " {}"))) {
            assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, invalid)));
        }
    }

    @Test
    void testRotationReadsLiveFileWithBoundedReloads()
            throws Exception
    {
        String old = token(key, KID, validClaims());
        KeyPair rotated = keyPair();
        String current = token(rotated, "rotated", validClaims());
        writeJwks(file, rotated, "rotated");
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, current)));
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, old)).token()).isEqualTo(old);
        ticker.set(SECONDS.toNanos(30));
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, current)).token()).isEqualTo(current);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, old)));
        Files.writeString(file, "synthetic-invalid-file", UTF_8);
        ticker.set(SECONDS.toNanos(60));
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, current)).token()).isEqualTo(current);
        writeJwks(file, key, KID);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, old)));
        ticker.set(SECONDS.toNanos(90));
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, old)).token()).isEqualTo(old);
    }

    @Test
    void testMountedKeysAreStrictAndBounded()
            throws Exception
    {
        Map<String, Object> validKey = jwk(key, KID);
        List<Map<String, Object>> badKeys = new ArrayList<>();
        for (Map.Entry<String, Object> mutation : Map.<String, Object>of(
                "d", "synthetic-private-key-material", "kty", "RSA", "crv", "P-384", "x", "invalid", "y", "invalid", "key_ops", List.of("sign")).entrySet()) {
            Map<String, Object> invalid = new LinkedHashMap<>(validKey);
            invalid.put(mutation.getKey(), mutation.getValue());
            badKeys.add(invalid);
        }
        Map<String, Object> noId = new LinkedHashMap<>(validKey);
        noId.remove("kid");
        badKeys.add(noId);
        Map<String, Object> offCurve = new LinkedHashMap<>(validKey);
        offCurve.put("x", "A".repeat(43));
        offCurve.put("y", "A".repeat(43));
        badKeys.add(offCurve);
        for (Map<String, Object> invalid : badKeys) {
            Files.writeString(file, JSON.writeValueAsString(Map.of("keys", List.of(invalid))), UTF_8);
            assertBadConfiguration();
        }
        Files.writeString(file, JSON.writeValueAsString(Map.of("keys", List.of(validKey, validKey))), UTF_8);
        assertBadConfiguration();
        Files.writeString(file, "{\"keys\":[],\"keys\":[]}", UTF_8);
        assertBadConfiguration();
        Files.writeString(file, "{\"keys\":[]}", UTF_8);
        assertBadConfiguration();
        List<Map<String, Object>> entries = new ArrayList<>();
        for (int index = 0; index < 32; index++) {
            entries.add(jwk(key, "key-" + index));
        }
        Files.writeString(file, JSON.writeValueAsString(Map.of("keys", entries)), UTF_8);
        assertThat(verifier()).isNotNull();
        entries.add(jwk(key, "key-32"));
        Files.writeString(file, JSON.writeValueAsString(Map.of("keys", entries)), UTF_8);
        assertBadConfiguration();
        Files.writeString(file, "x".repeat(HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES + 1), UTF_8);
        assertBadConfiguration();
    }

    @Test
    void testCompatibleKeyPurpose()
            throws Exception
    {
        Map<String, Object> encryption = Map.of("use", "enc");
        Map<String, Object> otherAlgorithm = Map.of("alg", "RS256");
        Map<String, Object> publicKey = jwk(key, KID);
        publicKey.remove("alg");
        publicKey.remove("use");
        publicKey.put("key_ops", List.of("verify"));
        Files.writeString(file, JSON.writeValueAsString(Map.of("keys", List.of(encryption, otherAlgorithm, publicKey))), UTF_8);
        verifier = verifier();
        assertThat(verify(validClaims()).catalog()).isEqualTo(CATALOG);
    }

    @Test
    void testConfigurationAuthority()
    {
        for (String bindings : List.of(
                "",
                "{}",
                "[]",
                "{\"invalid\":\"hoglite\"}",
                "{\"%s\":\"unknown\"}".formatted(BACKEND_ID),
                "{\"%s\":\"hoglite\",\"%s\":\"postgres\"}".formatted(BACKEND_ID, BACKEND_ID),
                "{\"%s\":1}".formatted(BACKEND_ID),
                "x".repeat(HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES + 1))) {
            configuration.setBackendBindings(bindings);
            assertBadConfiguration();
        }
        configuration = config(file, URI.create("https://metadata.example.test"));
        configuration.setMultiTenant(false);
        assertBadConfiguration();
        configuration.setMultiTenant(true).setClockSkew(new Duration(601, SECONDS));
        assertBadConfiguration();
        configuration.setClockSkew(new Duration(60, SECONDS)).setScopedMaxLifetime(new Duration(901, SECONDS));
        assertBadConfiguration();
        configuration.setScopedMaxLifetime(new Duration(0, SECONDS));
        assertBadConfiguration();
    }

    @Test
    void testConcurrentCallerIsolation()
            throws Exception
    {
        String alice = token(key, KID, validClaims());
        Map<String, Object> bobClaims = claims("other_catalog", "other-db.bob", "other-tenant", NOW);
        bobClaims.put("prd", Map.of("team_id", Map.of("9", "read")));
        String bob = token(key, KID, bobClaims);
        List<Callable<HoglakeSharedSessionContext>> requests = new ArrayList<>();
        for (int index = 0; index < 32; index++) {
            requests.add(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, alice)));
            requests.add(() -> verifier.verify(session("other-db.mapped", "other-db.bob", bob)));
        }
        try (var executor = Executors.newFixedThreadPool(8)) {
            var results = executor.invokeAll(requests);
            for (int index = 0; index < results.size(); index += 2) {
                HoglakeSharedSessionContext aliceContext = results.get(index).get();
                HoglakeSharedSessionContext bobContext = results.get(index + 1).get();
                assertThat(aliceContext.token()).isEqualTo(alice);
                assertThat(aliceContext.catalog()).isEqualTo(CATALOG);
                assertThat(aliceContext.predicates().get("team_id").read()).containsExactly("7");
                assertThat(bobContext.token()).isEqualTo(bob);
                assertThat(bobContext.catalog()).isEqualTo("other_catalog");
                assertThat(bobContext.principal()).isEqualTo("other-db.bob");
                assertThat(bobContext.predicates().get("team_id").read()).containsExactly("9");
            }
        }
    }

    @Test
    void testRepeatedVerificationDoesNotCacheAuthority()
    {
        AtomicLong currentSeconds = new AtomicLong(NOW.getEpochSecond());
        verifier = new HoglakeSharedSessionVerifier(configuration, new AdjustableClock(currentSeconds, ZoneOffset.UTC), ticker::get);
        String jwt = token(key, KID, validClaims());
        assertThat(verifier.verify(session(PRINCIPAL, PRINCIPAL, jwt)).token()).isEqualTo(jwt);
        currentSeconds.addAndGet(361);
        assertDenied(() -> verifier.verify(session(PRINCIPAL, PRINCIPAL, jwt)));
    }

    private Map<String, Object> validClaims()
    {
        return claims(CATALOG, PRINCIPAL, "opaque-tenant", NOW);
    }

    private HoglakeSharedSessionContext verify(Map<String, Object> claims)
    {
        return verifier.verify(session(PRINCIPAL, PRINCIPAL, token(key, KID, claims)));
    }

    private HoglakeSharedSessionVerifier verifier()
    {
        return new HoglakeSharedSessionVerifier(configuration, Clock.fixed(NOW, ZoneOffset.UTC), ticker::get);
    }

    private void denyMutation(Consumer<Map<String, Object>> mutation)
    {
        Map<String, Object> changed = validClaims();
        mutation.accept(changed);
        assertDenied(() -> verify(changed));
    }

    private void assertBadConfiguration()
    {
        assertThatThrownBy(this::verifier)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid shared Hoglake verification configuration")
                .hasNoCause()
                .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
    }

    private static void assertDenied(Runnable request)
    {
        assertThatThrownBy(request::run)
                .isInstanceOf(TrinoException.class)
                .hasMessage("Shared Hoglake session authority is invalid or unavailable")
                .hasNoCause()
                .satisfies(failure -> {
                    assertThat(((TrinoException) failure).getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode());
                    assertThat(failure.getSuppressed()).isEmpty();
                });
    }

    private static final class AdjustableClock
            extends Clock
    {
        private final AtomicLong seconds;
        private final ZoneId zone;

        private AdjustableClock(AtomicLong seconds, ZoneId zone)
        {
            this.seconds = seconds;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone()
        {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return new AdjustableClock(seconds, zone);
        }

        @Override
        public Instant instant()
        {
            return Instant.ofEpochSecond(seconds.get());
        }
    }
}
