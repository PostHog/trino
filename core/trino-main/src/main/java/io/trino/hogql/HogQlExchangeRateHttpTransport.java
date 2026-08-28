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
package io.trino.hogql;

import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.JsonTransport;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class HogQlExchangeRateHttpTransport
        implements JsonTransport
{
    private static final String METADATA_PATH = "v1/hogql/compatibility/exchange-rates";
    private static final String AUTHENTICATION_HEADER = "X-Duckgres-Internal-Secret";
    private static final MediaType JSON = JSON_UTF_8.withoutParameters();

    private final URI baseUri;
    private final HttpClient httpClient;
    private final int maximumResponseBytes;
    private final Supplier<String> authenticationTokenSupplier;

    HogQlExchangeRateHttpTransport(URI baseUri, HttpClient httpClient, Supplier<String> authenticationTokenSupplier)
    {
        this(baseUri, httpClient, HogQlExchangeRateSnapshotJsonDecoder.MAXIMUM_PAYLOAD_BYTES, authenticationTokenSupplier);
    }

    HogQlExchangeRateHttpTransport(URI baseUri, HttpClient httpClient, int maximumResponseBytes, Supplier<String> authenticationTokenSupplier)
    {
        this.baseUri = validateBaseUri(baseUri);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        if (maximumResponseBytes <= 0 || maximumResponseBytes > HogQlExchangeRateSnapshotJsonDecoder.MAXIMUM_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid HogQL exchange-rate response size limit");
        }
        this.maximumResponseBytes = maximumResponseBytes;
        this.authenticationTokenSupplier = requireNonNull(authenticationTokenSupplier, "authenticationTokenSupplier is null");
    }

    @Override
    public CompletionStage<byte[]> load(LoadRequest request)
    {
        requireNonNull(request, "request is null");
        String authenticationToken;
        try {
            authenticationToken = validateAuthenticationToken(authenticationTokenSupplier.get());
        }
        catch (RuntimeException _) {
            return CompletableFuture.failedFuture(unavailable());
        }
        Request httpRequest = prepareGet()
                .setUri(buildUri(baseUri, request))
                .setHeader(ACCEPT, JSON.toString())
                .setHeader(AUTHENTICATION_HEADER, authenticationToken)
                .build();
        return toCompletableFuture(httpClient.executeAsync(httpRequest, new MetadataResponseHandler(request, maximumResponseBytes)));
    }

    static URI buildUri(URI baseUri, LoadRequest request)
    {
        var uriBuilder = uriBuilderFrom(validateBaseUri(baseUri))
                .appendPath(METADATA_PATH)
                .addParameter("protocolVersion", Integer.toString(1));
        request.expectedGeneration().ifPresent(generation -> uriBuilder.addParameter("generation", Long.toString(generation)));
        return uriBuilder.build();
    }

    private static String validateAuthenticationToken(String token)
    {
        requireNonNull(token, "authentication token is null");
        if (token.isBlank() || token.indexOf('\r') >= 0 || token.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("invalid HogQL exchange-rate authentication token");
        }
        return token;
    }

    private static URI validateBaseUri(URI baseUri)
    {
        requireNonNull(baseUri, "HogQL exchange-rate URI is null");
        String scheme = baseUri.getScheme();
        if (scheme == null || !(scheme.toLowerCase(ENGLISH).equals("http") || scheme.toLowerCase(ENGLISH).equals("https")) ||
                baseUri.getHost() == null || baseUri.getUserInfo() != null || baseUri.getQuery() != null || baseUri.getFragment() != null) {
            throw new IllegalArgumentException("invalid HogQL exchange-rate URI");
        }
        return baseUri;
    }

    private static HogQlExchangeRateException unavailable()
    {
        return new HogQlExchangeRateException(Failure.UNAVAILABLE, "HogQL exchange-rate metadata is unavailable");
    }

    private static final class MetadataResponseHandler
            implements ResponseHandler<byte[], RuntimeException>
    {
        private final LoadRequest loadRequest;
        private final int maximumResponseBytes;

        private MetadataResponseHandler(LoadRequest loadRequest, int maximumResponseBytes)
        {
            this.loadRequest = loadRequest;
            this.maximumResponseBytes = maximumResponseBytes;
        }

        @Override
        public byte[] handleException(Request request, Exception exception)
        {
            throw unavailable();
        }

        @Override
        public byte[] handle(Request request, Response response)
        {
            int statusCode = response.getStatusCode();
            if (statusCode < 200 || statusCode >= 300) {
                throw statusFailure(statusCode, loadRequest);
            }
            if (!isJson(response)) {
                throw unavailable();
            }
            try (InputStream input = response.getInputStream()) {
                byte[] payload = input.readNBytes(maximumResponseBytes + 1);
                if (payload.length > maximumResponseBytes) {
                    throw unavailable();
                }
                return payload;
            }
            catch (IOException e) {
                throw unavailable();
            }
        }

        private static boolean isJson(Response response)
        {
            return response.getHeader(CONTENT_TYPE)
                    .map(value -> {
                        try {
                            return MediaType.parse(value).withoutParameters().equals(JSON);
                        }
                        catch (IllegalArgumentException _) {
                            return false;
                        }
                    })
                    .orElse(false);
        }

        private static HogQlExchangeRateException statusFailure(int statusCode, LoadRequest loadRequest)
        {
            if (statusCode == 409 || (statusCode == 404 && loadRequest.expectedGeneration().isPresent())) {
                return new HogQlExchangeRateException(Failure.GENERATION_MISMATCH, "HogQL exchange-rate generation is unavailable");
            }
            return unavailable();
        }
    }
}
