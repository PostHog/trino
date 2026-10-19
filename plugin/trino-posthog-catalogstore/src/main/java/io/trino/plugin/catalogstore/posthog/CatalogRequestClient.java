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
package io.trino.plugin.catalogstore.posthog;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.DelegatedCatalogStore.CatalogRequestResult;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.ofBytes;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Forwards catalog mutations to the control plane, which validates them and publishes the result
 * through the catalog store. Requests carry catalog properties, so nothing here ever puts a request
 * body, or a failure that could quote one, into an exception or a log line.
 *
 * <ul>
 *   <li>{@code POST {endpoint}/create} with {@code {user, catalog, connector, properties, if_not_exists}}
 *   <li>{@code POST {endpoint}/drop} with {@code {user, catalog, if_exists}}
 * </ul>
 *
 * <p>A 200 answers {@code {catalog, created|dropped}}. A rejection answers {@code {error}}, which the
 * control plane guarantees never echoes a property value, and which is shown to the user: 400 is an
 * invalid request, 403 a denied one, 409 an existing catalog on create, 404 a missing catalog on
 * drop. Anything else means catalog requests are unavailable.
 */
public class CatalogRequestClient
{
    private static final Logger log = Logger.get(CatalogRequestClient.class);

    static final int MAX_RESPONSE_BYTES = 64 * 1024;
    static final int MAX_ERROR_LENGTH = 512;
    private static final int MAX_TOKEN_BYTES = 8192;

    private static final JsonCodec<CreateRequest> CREATE_REQUEST_CODEC = jsonCodec(CreateRequest.class);
    private static final JsonCodec<DropRequest> DROP_REQUEST_CODEC = jsonCodec(DropRequest.class);
    private static final JsonCodec<CreateResponse> CREATE_RESPONSE_CODEC = jsonCodec(CreateResponse.class);
    private static final JsonCodec<DropResponse> DROP_RESPONSE_CODEC = jsonCodec(DropResponse.class);
    private static final JsonCodec<ErrorResponse> ERROR_RESPONSE_CODEC = jsonCodec(ErrorResponse.class);

    private final URI createUri;
    private final URI dropUri;
    private final Path tokenFile;
    private final HttpClient httpClient;

    @Inject
    public CatalogRequestClient(PostHogCatalogStoreConfig config, @ForCatalogRequests HttpClient httpClient)
    {
        URI endpoint = config.getDelegateEndpoint().orElseThrow(() -> new IllegalArgumentException("catalog-store.delegate.endpoint is not set"));
        checkArgument(endpoint.isAbsolute() && endpoint.getHost() != null && endpoint.getUserInfo() == null && endpoint.getFragment() == null && endpoint.getQuery() == null,
                "Catalog request endpoint must be an absolute URL without user information, query, or fragment");
        checkArgument("https".equals(endpoint.getScheme()) || (config.isDelegateAllowInsecureHttp() && "http".equals(endpoint.getScheme())),
                "Catalog request endpoint requires HTTPS unless insecure HTTP is explicitly enabled");
        this.createUri = uriBuilderFrom(endpoint).appendPath("create").build();
        this.dropUri = uriBuilderFrom(endpoint).appendPath("drop").build();
        this.tokenFile = config.getDelegateTokenFile()
                .orElseThrow(() -> new IllegalArgumentException("catalog-store.delegate.token-file is not set"))
                .toPath();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public CatalogRequestResult createCatalog(Identity identity, CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean ifNotExists)
    {
        CreateRequest request = new CreateRequest(identity.getUser(), catalogName.toString(), connectorName.toString(), properties, ifNotExists);
        CreateResponse response = execute(createUri, CREATE_REQUEST_CODEC.toJsonBytes(request), CREATE_RESPONSE_CODEC, Operation.CREATE);
        if (response.created() == null || !catalogName.toString().equals(response.catalog())) {
            throw unavailable();
        }
        return new CatalogRequestResult(catalogName, response.created());
    }

    public CatalogRequestResult dropCatalog(Identity identity, CatalogName catalogName, boolean ifExists)
    {
        DropRequest request = new DropRequest(identity.getUser(), catalogName.toString(), ifExists);
        DropResponse response = execute(dropUri, DROP_REQUEST_CODEC.toJsonBytes(request), DROP_RESPONSE_CODEC, Operation.DROP);
        if (response.dropped() == null || !catalogName.toString().equals(response.catalog())) {
            throw unavailable();
        }
        return new CatalogRequestResult(catalogName, response.dropped());
    }

    private <T> T execute(URI uri, byte[] body, JsonCodec<T> responseCodec, Operation operation)
    {
        Request request;
        try {
            request = preparePost()
                    .setUri(uri)
                    .setFollowRedirects(false)
                    .setMaxResponseContentLength(ofBytes(MAX_RESPONSE_BYTES))
                    .setHeader(AUTHORIZATION, "Bearer " + readToken())
                    .setHeader(CONTENT_TYPE, "application/json")
                    .setBodyGenerator(createStaticBodyGenerator(body))
                    .build();
        }
        catch (IOException | RuntimeException e) {
            log.warn("Cannot prepare a catalog request (%s)", e.getClass().getName());
            throw unavailable();
        }
        try {
            return httpClient.execute(request, new CatalogResponseHandler<>(responseCodec, operation));
        }
        catch (TrinoException e) {
            // Raised by the handler, with a message that is safe to show
            throw e;
        }
        catch (RuntimeException e) {
            // The client can fail outside the handler, for example on a malformed authentication
            // challenge, with an exception that describes the request; only its type is logged
            log.warn("Catalog request failed (%s)", e.getClass().getName());
            throw unavailable();
        }
    }

    private String readToken()
            throws IOException
    {
        try (InputStream input = Files.newInputStream(tokenFile)) {
            byte[] bytes = input.readNBytes(MAX_TOKEN_BYTES + 1);
            String token = new String(bytes, UTF_8).lines().filter(line -> !line.isBlank()).findFirst().orElse("").strip();
            if (bytes.length > MAX_TOKEN_BYTES || token.isEmpty() || token.chars().anyMatch(character -> character <= 32 || character >= 127)) {
                throw new IOException("Catalog request token is not usable");
            }
            return token;
        }
    }

    static TrinoException unavailable()
    {
        return new TrinoException(CATALOG_STORE_ERROR, "Catalog requests are unavailable");
    }

    private enum Operation
    {
        CREATE, DROP
    }

    private static class CatalogResponseHandler<T>
            implements ResponseHandler<T, RuntimeException>
    {
        private final JsonCodec<T> codec;
        private final Operation operation;

        private CatalogResponseHandler(JsonCodec<T> codec, Operation operation)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.operation = requireNonNull(operation, "operation is null");
        }

        @Override
        public T handleException(Request request, Exception exception)
        {
            // The exception can describe the request; only its type is logged
            log.warn("Catalog request failed (%s)", exception.getClass().getName());
            throw unavailable();
        }

        @Override
        public T handle(Request request, Response response)
        {
            int status = response.getStatusCode();
            byte[] body = readBody(response);
            if (status == 200) {
                try {
                    return codec.fromJson(body);
                }
                catch (RuntimeException e) {
                    log.warn("Catalog request returned an unreadable response (%s)", e.getClass().getName());
                    throw unavailable();
                }
            }
            Optional<String> error = errorMessage(body);
            switch (status) {
                case 400 -> throw new TrinoException(INVALID_CATALOG_PROPERTY, error.orElse("Invalid catalog request"));
                case 403 -> throw new AccessDeniedException(error.orElse("Catalog request is not permitted"));
                case 404 -> {
                    if (operation == Operation.DROP) {
                        throw new TrinoException(CATALOG_NOT_FOUND, error.orElse("Catalog does not exist"));
                    }
                }
                case 409 -> {
                    if (operation == Operation.CREATE) {
                        throw new TrinoException(ALREADY_EXISTS, error.orElse("Catalog already exists"));
                    }
                }
                default -> {}
            }
            log.warn("Catalog request was answered with status %s", status);
            throw unavailable();
        }

        private static byte[] readBody(Response response)
        {
            try (InputStream input = response.getInputStream()) {
                byte[] bytes = input.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    log.warn("Catalog request returned a response larger than %s bytes", MAX_RESPONSE_BYTES);
                    throw unavailable();
                }
                return bytes;
            }
            catch (IOException e) {
                log.warn("Cannot read a catalog response (%s)", e.getClass().getName());
                throw unavailable();
            }
        }

        private static Optional<String> errorMessage(byte[] body)
        {
            String error;
            try {
                error = ERROR_RESPONSE_CODEC.fromJson(body).error();
            }
            catch (RuntimeException _) {
                return Optional.empty();
            }
            if (error == null) {
                return Optional.empty();
            }
            // Shown to the user as one line of bounded length
            String sanitized = error.codePoints()
                    .map(codePoint -> Character.isISOControl(codePoint) ? ' ' : codePoint)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString()
                    .strip();
            if (sanitized.isEmpty()) {
                return Optional.empty();
            }
            if (sanitized.length() > MAX_ERROR_LENGTH) {
                int end = MAX_ERROR_LENGTH;
                if (Character.isHighSurrogate(sanitized.charAt(end - 1))) {
                    end--;
                }
                sanitized = sanitized.substring(0, end);
            }
            return Optional.of(sanitized);
        }
    }

    public record CreateRequest(
            String user,
            String catalog,
            String connector,
            Map<String, String> properties,
            @JsonProperty("if_not_exists") boolean ifNotExists)
    {
        public CreateRequest
        {
            requireNonNull(user, "user is null");
            requireNonNull(catalog, "catalog is null");
            requireNonNull(connector, "connector is null");
            properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        }

        @Override
        public String toString()
        {
            return "CreateRequest{user=%s, catalog=%s, connector=%s, properties=<redacted>, ifNotExists=%s}".formatted(user, catalog, connector, ifNotExists);
        }
    }

    public record DropRequest(String user, String catalog, @JsonProperty("if_exists") boolean ifExists)
    {
        public DropRequest
        {
            requireNonNull(user, "user is null");
            requireNonNull(catalog, "catalog is null");
        }
    }

    public record CreateResponse(String catalog, Boolean created) {}

    public record DropResponse(String catalog, Boolean dropped) {}

    public record ErrorResponse(String error) {}
}
