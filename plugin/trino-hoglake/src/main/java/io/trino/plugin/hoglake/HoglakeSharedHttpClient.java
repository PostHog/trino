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

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeSharedConfig.MAX_RESPONSE_BYTES;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The shared client exposes only the approved metadata routes.
 */
public final class HoglakeSharedHttpClient
        implements AutoCloseable
{
    private static final int TRANSFER_BUFFER_SIZE = 8192;
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final HoglakeSharedSessionVerifier verifier;
    private final HttpClient httpClient;
    private final String baseUri;
    private final Duration requestTimeout;
    private final boolean namespacePointLookupEnabled;
    private final LongSupplier ticker;

    @Inject
    public HoglakeSharedHttpClient(HoglakeSharedConfig config, HoglakeSharedSessionVerifier verifier)
    {
        this(config, verifier, System::nanoTime);
    }

    HoglakeSharedHttpClient(HoglakeSharedConfig config, HoglakeSharedSessionVerifier verifier, LongSupplier ticker)
    {
        this.verifier = requireNonNull(verifier, "verifier is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        baseUri = endpoint(config.getUri());
        requestTimeout = Duration.ofNanos(config.getRequestTimeout().roundTo(NANOSECONDS));
        namespacePointLookupEnabled = config.isNamespacePointLookupEnabled();
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid shared Hoglake request timeout");
        }
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    private static String endpoint(URI uri)
    {
        // Configuration errors must not disclose URI credentials or query values.
        if (uri == null || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null ||
                !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("Invalid shared Hoglake endpoint");
        }
        String endpoint = uri.toASCIIString();
        while (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        return endpoint;
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!namespacePointLookupEnabled) {
            return listNamespaces(session).stream().anyMatch(candidate -> candidate.name().equals(namespace));
        }
        return namespaceFound(session, namespace);
    }

    /**
     * A team schema is checked with the namespace GET whatever the lookup setting,
     * because the server's listing names no team schema for a {@code *} grant.
     */
    public boolean teamSchemaExists(ConnectorSession session, String namespace)
    {
        return namespaceFound(session, namespace);
    }

    private boolean namespaceFound(ConnectorSession session, String namespace)
    {
        HoglakeSharedSessionContext context = verify(session);
        RequestBudget budget = new RequestBudget(ticker, requestTimeout.toNanos());
        Optional<JsonNode> response = request(session, context, catalogPath(context) + "/namespaces/" + segment(namespace), budget);
        if (response.isEmpty()) {
            budget.check();
            return false;
        }
        JsonNode value = response.orElseThrow();
        requireObject(value);
        requireText(value, "name");
        optionalInteger(value, "namespace_id");
        if (!namespace.equals(value.path("name").textValue())) {
            throw invalidResponse();
        }
        budget.check();
        return true;
    }

    public List<HoglakeDtos.Namespace> listNamespaces(ConnectorSession session)
    {
        HoglakeSharedSessionContext context = verify(session);
        RequestBudget budget = new RequestBudget(ticker, requestTimeout.toNanos());
        Optional<JsonNode> response = request(session, context, catalogPath(context) + "/namespaces", budget);
        if (response.isEmpty()) {
            throw new TrinoException(HOGLAKE_CATALOG_NOT_FOUND, "Shared Hoglake catalog not found");
        }
        JsonNode array = response.orElseThrow();
        requireArray(array);
        ImmutableList.Builder<HoglakeDtos.Namespace> namespaces = ImmutableList.builder();
        for (JsonNode namespace : array) {
            budget.check();
            requireObject(namespace);
            requireText(namespace, "name");
            optionalInteger(namespace, "namespace_id");
            namespaces.add(convert(namespace, HoglakeDtos.Namespace.class, budget));
        }
        List<HoglakeDtos.Namespace> result = namespaces.build();
        budget.check();
        return result;
    }

    public List<HoglakeDtos.TableSummary> listTables(ConnectorSession session, String namespace)
    {
        HoglakeSharedSessionContext context = verify(session);
        RequestBudget budget = new RequestBudget(ticker, requestTimeout.toNanos());
        Optional<JsonNode> response = request(session, context, catalogPath(context) + "/namespaces/" + segment(namespace) + "/tables", budget);
        if (response.isEmpty()) {
            throw new SchemaNotFoundException("<redacted>", "Shared Hoglake namespace not found");
        }
        JsonNode array = response.orElseThrow();
        requireArray(array);
        ImmutableList.Builder<HoglakeDtos.TableSummary> tables = ImmutableList.builder();
        for (JsonNode table : array) {
            budget.check();
            requireObject(table);
            requireText(table, "name");
            requireText(table, "table_uuid");
            tables.add(convert(table, HoglakeDtos.TableSummary.class, budget));
        }
        List<HoglakeDtos.TableSummary> result = tables.build();
        budget.check();
        return result;
    }

    public Optional<HoglakeDtos.Table> table(ConnectorSession session, String namespace, String table)
    {
        HoglakeSharedSessionContext context = verify(session);
        RequestBudget budget = new RequestBudget(ticker, requestTimeout.toNanos());
        Optional<JsonNode> response = request(session, context, catalogPath(context) + "/namespaces/" + segment(namespace) + "/tables/" + segment(table) + "?totals=false", budget);
        if (response.isEmpty()) {
            budget.check();
            return Optional.empty();
        }
        budget.check();
        JsonNode value = response.orElseThrow();
        requireObject(value);
        requireText(value, "name");
        requireText(value, "namespace");
        requireText(value, "table_uuid");
        requireArray(value.path("columns"));
        for (JsonNode column : value.path("columns")) {
            validateColumn(column, budget);
        }
        for (String field : List.of("record_count", "file_count", "file_size_bytes")) {
            budget.check();
            if (value.has(field) && value.path(field).isNull()) {
                throw invalidResponse();
            }
            optionalInteger(value, field);
        }
        optionalObject(value, "partition_spec");
        optionalObject(value, "sort_spec");
        optionalObject(value, "properties");
        if (value.hasNonNull("properties")) {
            for (JsonNode property : value.path("properties")) {
                budget.check();
                if (!property.isTextual()) {
                    throw invalidResponse();
                }
            }
        }
        optionalText(value, "comment");
        Optional<HoglakeDtos.Table> result = Optional.of(convert(value, HoglakeDtos.Table.class, budget));
        budget.check();
        return result;
    }

    private HoglakeSharedSessionContext verify(ConnectorSession session)
    {
        try {
            return verifier.verify(session);
        }
        catch (RuntimeException _) {
            throw new TrinoException(PERMISSION_DENIED, "Shared Hoglake session verification failed");
        }
    }

    private static String catalogPath(HoglakeSharedSessionContext context)
    {
        return "/v1/catalogs/" + segment(context.catalog());
    }

    private Optional<JsonNode> request(ConnectorSession session, HoglakeSharedSessionContext context, String path, RequestBudget budget)
    {
        AtomicReference<BoundedBodySubscriber> subscriber = new AtomicReference<>();
        CompletableFuture<HttpResponse<byte[]>> exchange = null;
        try {
            budget.check();
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUri + path))
                    .timeout(requestTimeout)
                    .header("Authorization", "Bearer " + context.token())
                    .header("Accept", "application/json")
                    .header("Accept-Encoding", "identity")
                    .header("User-Agent", "trino-hoglake/1")
                    .header(HoglakeClient.REQUEST_ID_HEADER, HoglakeClient.requestId(Optional.of(session.getQueryId())))
                    .GET()
                    .build();
            exchange = httpClient.sendAsync(request, information -> {
                BoundedBodySubscriber body = new BoundedBodySubscriber(information.statusCode() != 200);
                subscriber.set(body);
                if (information.statusCode() == 200) {
                    List<String> encodings = information.headers().allValues("Content-Encoding");
                    if (encodings.stream().anyMatch(encoding -> !encoding.equalsIgnoreCase("identity"))) {
                        body.abort(invalidResponse());
                    }
                    try {
                        if (information.headers().firstValueAsLong("Content-Length").orElse(0) > MAX_RESPONSE_BYTES) {
                            body.abort(invalidResponse());
                        }
                    }
                    catch (RuntimeException _) {
                        body.abort(invalidResponse());
                    }
                }
                return body;
            });
            HttpResponse<byte[]> response = exchange.get(budget.remainingNanos(), NANOSECONDS);
            budget.check();
            int status = response.statusCode();
            if (status == 404) {
                return Optional.empty();
            }
            if (status == 401 || status == 403) {
                throw new TrinoException(PERMISSION_DENIED, "Shared Hoglake request denied");
            }
            if (status >= 500) {
                throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Shared Hoglake service unavailable");
            }
            if (status != 200) {
                throw invalidResponse();
            }
            JsonNode value;
            try (InputStream input = new BudgetInputStream(response.body(), budget)) {
                value = MAPPER.readTree(input);
            }
            budget.check();
            if (value == null) {
                throw invalidResponse();
            }
            return Optional.of(value);
        }
        catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Shared Hoglake request interrupted");
        }
        catch (TimeoutException _) {
            throw requestTimedOut();
        }
        catch (ExecutionException _) {
            BoundedBodySubscriber body = subscriber.get();
            if (body != null && body.invalid()) {
                throw invalidResponse();
            }
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Shared Hoglake request failed");
        }
        catch (TrinoException exception) {
            throw exception;
        }
        catch (IOException | RuntimeException _) {
            budget.check();
            throw invalidResponse();
        }
        finally {
            if (exchange != null) {
                exchange.cancel(true);
            }
            BoundedBodySubscriber body = subscriber.get();
            if (body != null) {
                body.cancel();
            }
        }
    }

    private static String segment(String value)
    {
        if (value == null || value.isEmpty() || value.codePoints().anyMatch(character -> character >= 0xD800 && character <= 0xDFFF)) {
            throw new TrinoException(INVALID_ARGUMENTS, "Invalid shared Hoglake metadata name");
        }
        StringBuilder encoded = new StringBuilder();
        for (byte character : value.getBytes(UTF_8)) {
            int unsigned = Byte.toUnsignedInt(character);
            if ((unsigned >= 'a' && unsigned <= 'z') || (unsigned >= 'A' && unsigned <= 'Z') ||
                    (unsigned >= '0' && unsigned <= '9') || unsigned == '-' || unsigned == '_' || unsigned == '~') {
                encoded.append((char) unsigned);
            }
            else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned & 15, 16)));
            }
        }
        return encoded.toString();
    }

    private static void validateColumn(JsonNode column, RequestBudget budget)
    {
        budget.check();
        requireObject(column);
        if (!column.path("field_id").isIntegralNumber() || !column.path("field_id").canConvertToLong() ||
                !column.path("ordinal").isIntegralNumber() || !column.path("ordinal").canConvertToInt()) {
            throw invalidResponse();
        }
        requireText(column, "name");
        requireText(column, "type");
        optionalObject(column, "type_params");
        optionalText(column, "comment");
        if (column.hasNonNull("nullable") && !column.path("nullable").isBoolean()) {
            throw invalidResponse();
        }
        if (column.hasNonNull("children")) {
            requireArray(column.path("children"));
            for (JsonNode child : column.path("children")) {
                validateColumn(child, budget);
            }
        }
        budget.check();
    }

    private static void requireArray(JsonNode value)
    {
        if (!value.isArray()) {
            throw invalidResponse();
        }
    }

    private static void requireObject(JsonNode value)
    {
        if (!value.isObject()) {
            throw invalidResponse();
        }
    }

    private static void requireText(JsonNode value, String field)
    {
        if (!value.path(field).isTextual() || value.path(field).textValue().isBlank()) {
            throw invalidResponse();
        }
    }

    private static void optionalText(JsonNode value, String field)
    {
        if (value.hasNonNull(field) && !value.path(field).isTextual()) {
            throw invalidResponse();
        }
    }

    private static void optionalInteger(JsonNode value, String field)
    {
        if (value.hasNonNull(field) && (!value.path(field).isIntegralNumber() || !value.path(field).canConvertToLong())) {
            throw invalidResponse();
        }
    }

    private static void optionalObject(JsonNode value, String field)
    {
        if (value.hasNonNull(field)) {
            requireObject(value.path(field));
        }
    }

    private static <T> T convert(JsonNode value, Class<T> type, RequestBudget budget)
    {
        budget.check();
        T result;
        try {
            result = MAPPER.treeToValue(value, type);
        }
        catch (IOException | RuntimeException _) {
            budget.check();
            throw invalidResponse();
        }
        budget.check();
        return result;
    }

    private static TrinoException invalidResponse()
    {
        return new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid shared Hoglake metadata response");
    }

    private static TrinoException requestTimedOut()
    {
        return new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Shared Hoglake request timed out");
    }

    private static final class RequestBudget
    {
        private final LongSupplier ticker;
        private final long started;
        private final long timeoutNanos;

        private RequestBudget(LongSupplier ticker, long timeoutNanos)
        {
            this.ticker = ticker;
            this.started = ticker.getAsLong();
            this.timeoutNanos = timeoutNanos;
        }

        public long remainingNanos()
        {
            long remaining = timeoutNanos - (ticker.getAsLong() - started);
            if (remaining <= 0) {
                throw requestTimedOut();
            }
            return remaining;
        }

        public void check()
        {
            remainingNanos();
        }
    }

    /**
     * Each bounded parser read checks the same request deadline.
     */
    private static final class BudgetInputStream
            extends InputStream
    {
        private final ByteArrayInputStream input;
        private final RequestBudget budget;

        private BudgetInputStream(byte[] bytes, RequestBudget budget)
        {
            input = new ByteArrayInputStream(bytes);
            this.budget = budget;
        }

        @Override
        public int read()
        {
            budget.check();
            int result = input.read();
            budget.check();
            return result;
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
        {
            budget.check();
            int result = input.read(bytes, offset, Math.min(length, TRANSFER_BUFFER_SIZE));
            budget.check();
            return result;
        }
    }

    /**
     * The subscription never buffers more than the approved response ceiling.
     */
    private static final class BoundedBodySubscriber
            implements HttpResponse.BodySubscriber<byte[]>
    {
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final byte[] transfer = new byte[TRANSFER_BUFFER_SIZE];
        private final boolean discard;
        private Flow.Subscription subscription;
        private boolean invalid;
        private boolean complete;

        private BoundedBodySubscriber(boolean discard)
        {
            this.discard = discard;
        }

        @Override
        public CompletionStage<byte[]> getBody()
        {
            return result;
        }

        @Override
        public synchronized void onSubscribe(Flow.Subscription subscription)
        {
            this.subscription = subscription;
            if (discard) {
                result.complete(new byte[0]);
            }
            if (result.isDone()) {
                subscription.cancel();
                return;
            }
            subscription.request(1);
        }

        @Override
        public synchronized void onNext(List<ByteBuffer> buffers)
        {
            if (result.isDone()) {
                return;
            }
            long incoming = 0;
            for (ByteBuffer buffer : buffers) {
                incoming += buffer.remaining();
            }
            if (incoming > MAX_RESPONSE_BYTES - bytes.size()) {
                abort(invalidResponse());
                return;
            }
            for (ByteBuffer buffer : buffers) {
                while (buffer.hasRemaining()) {
                    int count = Math.min(buffer.remaining(), transfer.length);
                    buffer.get(transfer, 0, count);
                    bytes.write(transfer, 0, count);
                }
            }
            subscription.request(1);
        }

        @Override
        public synchronized void onError(Throwable throwable)
        {
            // Transport failures can include the request or a response fragment.
            result.completeExceptionally(new IOException("Shared Hoglake response interrupted"));
        }

        @Override
        public synchronized void onComplete()
        {
            complete = true;
            result.complete(bytes.toByteArray());
        }

        public synchronized void abort(TrinoException failure)
        {
            invalid = true;
            result.completeExceptionally(failure);
            cancel();
        }

        public synchronized boolean invalid()
        {
            return invalid;
        }

        public synchronized void cancel()
        {
            if (subscription != null && !complete) {
                subscription.cancel();
            }
        }
    }
}
