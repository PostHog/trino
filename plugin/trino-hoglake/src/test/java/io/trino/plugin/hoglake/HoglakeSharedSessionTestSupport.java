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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;

import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HoglakeSharedSessionTestSupport
{
    public static final String BACKEND_ID = "11111111-2222-4333-8444-555555555555";
    public static final String ISSUER = "https://issuer.example.test";
    private static final ObjectMapper JSON = new ObjectMapper();

    private HoglakeSharedSessionTestSupport() {}

    public static KeyPair keyPair()
    {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            return generator.generateKeyPair();
        }
        catch (Exception e) {
            throw new AssertionError("Cannot create a synthetic signing key", e);
        }
    }

    public static void writeJwks(Path file, KeyPair key, String kid)
    {
        try {
            Files.writeString(file, JSON.writeValueAsString(Map.of("keys", List.of(jwk(key, kid)))), UTF_8);
        }
        catch (Exception e) {
            throw new AssertionError("Cannot write a synthetic public JWKS", e);
        }
    }

    public static Map<String, Object> claims(String catalog, String principal, String tenant, Instant now)
    {
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", List.of("hoglake", "hoglite"));
        claims.put("sub", "synthetic-pool-actor");
        claims.put("jti", "synthetic-token-id");
        claims.put("org", "synthetic-audit-tenant");
        claims.put("ten", "scoped");
        claims.put("usr", principal);
        claims.put("iat", now.getEpochSecond());
        claims.put("exp", now.plusSeconds(300).getEpochSecond());
        claims.put("cat", new LinkedHashMap<>(Map.of(catalog, List.of("read"))));
        claims.put("prd", new LinkedHashMap<>(Map.of("team_id", new LinkedHashMap<>(Map.of("7", "read")))));
        claims.put("rtg", new LinkedHashMap<>(Map.of("b", "hoglite", "be", BACKEND_ID, "t", tenant)));
        return claims;
    }

    public static String token(KeyPair key, String kid, Map<String, Object> claims)
    {
        try {
            return sign(key, JSON.writeValueAsString(Map.of("alg", "ES256", "kid", kid)), JSON.writeValueAsString(claims));
        }
        catch (Exception e) {
            throw new AssertionError("Cannot sign synthetic claims", e);
        }
    }

    public static HoglakeSharedConfig config(Path jwks, URI endpoint)
    {
        return new HoglakeSharedConfig()
                .setMultiTenant(true)
                .setUri(endpoint)
                .setJwksFile(jwks.toFile())
                .setIssuer(ISSUER)
                .setBackendBindings("{\"%s\":\"hoglite\"}".formatted(BACKEND_ID));
    }

    public static ConnectorSession session(String mappedUser, String principal, String jwt)
    {
        ConnectorIdentity.Builder identity = ConnectorIdentity.forUser(mappedUser);
        if (principal != null) {
            identity.withPrincipal(new BasicPrincipal(principal));
        }
        if (jwt != null) {
            identity.withExtraCredentials(Map.of(HoglakeSharedSessionVerifier.SESSION_TOKEN, jwt));
        }
        return TestingConnectorSession.builder().setIdentity(identity.build()).build();
    }

    static Map<String, Object> jwk(KeyPair key, String kid)
    {
        ECPublicKey publicKey = (ECPublicKey) key.getPublic();
        return new LinkedHashMap<>(Map.of(
                "kty", "EC",
                "crv", "P-256",
                "alg", "ES256",
                "use", "sig",
                "kid", kid,
                "x", coordinate(publicKey.getW().getAffineX()),
                "y", coordinate(publicKey.getW().getAffineY())));
    }

    static String sign(KeyPair key, String header, String claims)
    {
        try {
            String input = encode(header.getBytes(UTF_8)) + "." + encode(claims.getBytes(UTF_8));
            Signature signer = Signature.getInstance("SHA256withECDSAinP1363Format");
            signer.initSign(key.getPrivate());
            signer.update(input.getBytes(US_ASCII));
            return input + "." + encode(signer.sign());
        }
        catch (Exception e) {
            throw new AssertionError("Cannot sign a synthetic JWT", e);
        }
    }

    private static String coordinate(BigInteger value)
    {
        byte[] bytes = value.toByteArray();
        byte[] coordinate = new byte[32];
        int length = Math.min(bytes.length, coordinate.length);
        System.arraycopy(bytes, bytes.length - length, coordinate, coordinate.length - length, length);
        return encode(coordinate);
    }

    private static String encode(byte[] bytes)
    {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
