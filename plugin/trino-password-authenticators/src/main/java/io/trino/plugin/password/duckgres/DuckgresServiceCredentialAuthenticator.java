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
package io.trino.plugin.password.duckgres;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PasswordAuthenticator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.ofBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DuckgresServiceCredentialAuthenticator
        implements PasswordAuthenticator
{
    private static final Pattern SERVICE_USERNAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}\\.svc_[a-f0-9]{24}");
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;
    private static final int MAX_TOKEN_BYTES = 8192;
    private static final JsonCodec<CredentialRequest> REQUEST_CODEC = jsonCodec(CredentialRequest.class);
    private static final JsonCodec<CredentialResponse> RESPONSE_CODEC = jsonCodec(CredentialResponse.class);

    private final URI endpoint;
    private final Path tokenFile;
    private final HttpClient httpClient;

    @Inject
    public DuckgresServiceCredentialAuthenticator(DuckgresServiceCredentialConfig config, @ForDuckgresServiceCredential HttpClient httpClient)
    {
        endpoint = config.getEndpoint();
        checkArgument(endpoint != null && endpoint.getHost() != null && endpoint.getUserInfo() == null && endpoint.getFragment() == null && endpoint.getQuery() == null,
                "Service credential endpoint must be an absolute URL without user information, query, or fragment");
        checkArgument("https".equals(endpoint.getScheme()) || (config.isAllowInsecureHttp() && "http".equals(endpoint.getScheme())),
                "Service credential endpoint requires HTTPS unless insecure HTTP is explicitly enabled");
        tokenFile = config.getTokenFile().toPath();
        this.httpClient = httpClient;
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        return createAuthenticatedIdentity(user, password).orElseThrow().getPrincipal().orElseThrow();
    }

    @Override
    public Optional<Identity> createAuthenticatedIdentity(String user, String password)
    {
        if (!SERVICE_USERNAME.matcher(user).matches() || password.isEmpty() || password.length() > 4096) {
            throw denied();
        }
        try {
            Request request = preparePost()
                    .setUri(endpoint)
                    .setFollowRedirects(false)
                    .setMaxResponseContentLength(ofBytes(MAX_RESPONSE_BYTES))
                    .setHeader(AUTHORIZATION, "Bearer " + readToken())
                    .setHeader(CONTENT_TYPE, "application/json")
                    .setBodyGenerator(createStaticBodyGenerator(REQUEST_CODEC.toJsonBytes(new CredentialRequest(user, password))))
                    .build();
            CredentialResponse response = httpClient.execute(request, new CredentialResponseHandler());
            if (!user.equals(response.user()) || response.expiresAt() == null || !Instant.parse(response.expiresAt()).isAfter(Instant.now()) ||
                    response.groups() == null || response.groups().size() != 2 ||
                    !response.groups().contains("org_" + user.substring(0, user.indexOf('.')).replace('-', '_')) ||
                    response.groups().stream().noneMatch(Set.of("tier_free", "tier_growth", "tier_scale")::contains)) {
                throw denied();
            }
            return Optional.of(Identity.forUser(user)
                    .withPrincipal(new BasicPrincipal(user))
                    .withGroups(ImmutableSet.copyOf(response.groups()))
                    .build());
        }
        catch (IOException | RuntimeException _) {
            // Upstream errors can contain credentials or response bodies.
            throw denied();
        }
    }

    private String readToken()
            throws IOException
    {
        try (InputStream input = Files.newInputStream(tokenFile)) {
            byte[] bytes = input.readNBytes(MAX_TOKEN_BYTES + 1);
            String token = new String(bytes, UTF_8).lines().filter(line -> !line.isBlank()).findFirst().orElse("").strip();
            if (bytes.length > MAX_TOKEN_BYTES || token.isEmpty() || token.chars().anyMatch(character -> character <= 32 || character >= 127)) {
                throw denied();
            }
            return token;
        }
    }

    private static AccessDeniedException denied()
    {
        return new AccessDeniedException("Invalid service credential");
    }

    private static class CredentialResponseHandler
            implements ResponseHandler<CredentialResponse, RuntimeException>
    {
        @Override
        public CredentialResponse handleException(Request request, Exception exception)
        {
            throw denied();
        }

        @Override
        public CredentialResponse handle(Request request, Response response)
        {
            if (response.getStatusCode() != 200) {
                throw denied();
            }
            try {
                byte[] bytes = response.getInputStream().readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    throw denied();
                }
                return RESPONSE_CODEC.fromJson(bytes);
            }
            catch (IOException | RuntimeException _) {
                throw denied();
            }
        }
    }

    public record CredentialRequest(String username, String password)
    {
        @Override
        public String toString()
        {
            return "CredentialRequest{redacted}";
        }
    }

    public record CredentialResponse(String user, List<String> groups, @JsonProperty("expires_at") String expiresAt) {}
}
