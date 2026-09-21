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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Minimal REST client for the hoglake control plane (/v1). Metadata
 * only: the connector reads parquet itself; the catalog service never
 * serves data.
 *
 * <p>Error taxonomy (see {@link io.trino.plugin.hoglake.HoglakeErrorCode}):
 * network/5xx failures are EXTERNAL, 410 is the typed snapshot-expired
 * signal, 404s become the engine's typed not-found exceptions at the
 * call sites, and malformed bodies are coded — nothing escapes without
 * a Trino error code.
 */
public class HoglakeClient
        implements Closeable
{
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(2);

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String baseUri;
    private final String catalog;
    private final Duration requestTimeout;

    public HoglakeClient(String baseUri, String catalog)
    {
        this(baseUri, catalog, DEFAULT_REQUEST_TIMEOUT);
    }

    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout)
    {
        // Fail at construction (catalog registration), not per query.
        this.baseUri = validateBaseUri(requireNonNull(baseUri, "baseUri is null")).toString();
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Validates a control-plane base URI: parseable, http(s) scheme, a
     * real host (a malformed port like "http://h:notaport" parses to a
     * null host and is rejected here instead of exploding at
     * request-build time on the first query). Trailing slashes are
     * collapsed.
     */
    public static URI validateBaseUri(String uri)
    {
        String trimmed = uri.replaceAll("/+$", "");
        URI parsed;
        try {
            parsed = new URI(trimmed);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': " + e.getMessage(), e);
        }
        String scheme = parsed.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': scheme must be http or https");
        }
        if (parsed.getHost() == null) {
            throw new IllegalArgumentException(
                    "Invalid hoglake.uri '" + uri + "': missing or unparseable host/port");
        }
        return parsed;
    }

    /**
     * Catalog identity + head snapshot: the query's snapshot pin source.
     */
    public HoglakeDtos.Catalog getCatalog()
    {
        return get(catalogPath(""), new TypeReference<HoglakeDtos.Catalog>() {})
                .orElseThrow(this::catalogNotFound);
    }

    public List<HoglakeDtos.Namespace> listNamespaces()
    {
        return get(catalogPath("/namespaces"), new TypeReference<List<HoglakeDtos.Namespace>>() {})
                .orElseThrow(this::catalogNotFound);
    }

    public void createNamespace(String name)
    {
        HoglakeDtos.Namespace result = post(catalogPath("/namespaces"), Map.of("name", name), new TypeReference<HoglakeDtos.Namespace>() {});
        if (!name.equals(result.name()) || result.namespaceId() == null) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake namespace response; outcome may be unknown");
        }
    }

    public HoglakeDtos.Namespace getNamespace(String name)
    {
        return get(namespacePath(name, ""), new TypeReference<HoglakeDtos.Namespace>() {})
                .orElseThrow(() -> new SchemaNotFoundException(name));
    }

    public void dropNamespace(String name, long expectedNamespaceId)
    {
        validateLifecycleResult(write("DELETE", namespacePath(name, "?expected_namespace_id=" + expectedNamespaceId), Map.of(), new TypeReference<HoglakeDtos.CommitResult>() {}));
    }

    public void alterColumns(String namespace, String table, String expectedTableUuid, long readSnapshot, Map<String, Object> operation)
    {
        HoglakeDtos.Table result = post(
                tablePath(namespace, table, "/alter?expected_table_uuid=" + encode(expectedTableUuid) + "&read_snapshot=" + readSnapshot),
                Map.of("ops", List.of(operation)),
                new TypeReference<HoglakeDtos.Table>() {});
        if (!expectedTableUuid.equals(result.tableUuid())) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake alteration response; outcome may be unknown");
        }
    }

    public List<HoglakeDtos.TableSummary> listTables(String namespace)
    {
        return get(namespacePath(namespace, "/tables"), new TypeReference<List<HoglakeDtos.TableSummary>>() {})
                .orElseThrow(() -> new SchemaNotFoundException(namespace));
    }

    /**
     * The table at head (listing surfaces; queries pin via the snapshot overload).
     */
    public Optional<HoglakeDtos.Table> getTable(String namespace, String table)
    {
        return get(tablePath(namespace, table, ""), new TypeReference<HoglakeDtos.Table>() {});
    }

    /**
     * The table at a pinned snapshot.
     */
    public Optional<HoglakeDtos.Table> getTable(String namespace, String table, long snapshot)
    {
        return get(tablePath(namespace, table, "?snapshot=" + snapshot),
                new TypeReference<HoglakeDtos.Table>() {});
    }

    /**
     * The split source: data files paired with live DVs at the pinned snapshot.
     */
    public List<HoglakeDtos.ScanFile> scan(String namespace, String table, long snapshot)
    {
        return get(tablePath(namespace, table, "/scan?snapshot=" + snapshot),
                new TypeReference<List<HoglakeDtos.ScanFile>>() {})
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(namespace, table)));
    }

    public HoglakeDtos.Table createTable(String namespace, String table, List<HoglakeDtos.ColumnDefinition> columns)
    {
        return post(namespacePath(namespace, "/tables"),
                new HoglakeDtos.CreateTable(table, columns),
                new TypeReference<HoglakeDtos.Table>() {});
    }

    public HoglakeDtos.TableCreation prepareTableCreation(String operationId, String namespace, String table, List<HoglakeDtos.ColumnDefinition> columns)
    {
        return prepareTableCreation(operationId, namespace, table, columns, null);
    }

    public HoglakeDtos.TableCreation prepareTableCreation(String operationId, String namespace, String table, List<HoglakeDtos.ColumnDefinition> columns, HoglakeDtos.ReplacementTarget replacement)
    {
        Map<String, Object> definition = new HashMap<>();
        definition.put("namespace", namespace);
        definition.put("name", table);
        definition.put("columns", columns);
        if (replacement != null) {
            definition.put("replacement", replacement);
        }
        return write(
                "PUT",
                catalogPath("/table-creations/" + encode(operationId)),
                definition,
                new TypeReference<HoglakeDtos.TableCreation>() {});
    }

    public HoglakeDtos.TableCreation publishTableCreation(String operationId, List<HoglakeDtos.FileRegistration> files)
    {
        return validateCreationReceipt(operationId, post(catalogPath("/table-creations/" + encode(operationId) + "/commit"), Map.of("files", files), new TypeReference<HoglakeDtos.TableCreation>() {}));
    }

    public HoglakeDtos.TableCreation getTableCreation(String operationId)
    {
        return validateCreationReceipt(operationId, get(catalogPath("/table-creations/" + encode(operationId)), new TypeReference<HoglakeDtos.TableCreation>() {})
                .orElseThrow(() -> new TrinoException(HOGLAKE_CATALOG_NOT_FOUND, "Hoglake creation operation not found: " + operationId)));
    }

    private static HoglakeDtos.TableCreation validateCreationReceipt(String operationId, HoglakeDtos.TableCreation receipt)
    {
        if (!operationId.equals(receipt.operationId()) || receipt.tableUuid() == null ||
                (receipt.state() == null || !List.of("prepared", "committed", "rejected", "aborted").contains(receipt.state())) ||
                ("committed".equals(receipt.state()) && (receipt.snapshotId() == null || receipt.snapshotId() <= 0))) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake creation receipt; inspect operation " + operationId);
        }
        return receipt;
    }

    public void abortTableCreation(String operationId)
    {
        post(catalogPath("/table-creations/" + encode(operationId) + "/abort"), Map.of(), new TypeReference<HoglakeDtos.TableCreation>() {});
    }

    public void renameTable(String namespace, String table, String newName)
    {
        post(tablePath(namespace, table, "/alter"), Map.of("ops", List.of(Map.of("op", "rename_table", "new_name", newName))), new TypeReference<HoglakeDtos.Table>() {});
    }

    public void renameTable(String namespace, String table, String newName, String expectedTableUuid)
    {
        HoglakeDtos.Table result = post(
                tablePath(namespace, table, "/alter?expected_table_uuid=" + encode(expectedTableUuid)),
                Map.of("ops", List.of(Map.of("op", "rename_table", "new_name", newName))),
                new TypeReference<HoglakeDtos.Table>() {});
        if (!expectedTableUuid.equals(result.tableUuid()) || !newName.equals(result.name())) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake rename response; outcome may be unknown");
        }
    }

    public void dropTable(String namespace, String table, String expectedTableUuid)
    {
        validateLifecycleResult(write("DELETE", tablePath(namespace, table, "?expected_table_uuid=" + encode(expectedTableUuid)), Map.of(), new TypeReference<HoglakeDtos.CommitResult>() {}));
    }

    public void truncateTable(String namespace, String table, String expectedTableUuid)
    {
        validateLifecycleResult(post(tablePath(namespace, table, "/truncate?expected_table_uuid=" + encode(expectedTableUuid)), Map.of(), new TypeReference<HoglakeDtos.CommitResult>() {}));
    }

    private static void validateLifecycleResult(HoglakeDtos.CommitResult result)
    {
        if (result.snapshotId() <= 0) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake lifecycle response; outcome may be unknown");
        }
    }

    public void dropStagingTable(String namespace, String table)
    {
        write("DELETE", tablePath(namespace, table, ""), Map.of(), new TypeReference<Object>() {}, true);
    }

    public void commit(HoglakeDtos.Commit request)
    {
        try {
            commitOnce(request, requestTimeout);
            return;
        }
        catch (TrinoException failure) {
            if (request.operationId() == null || isDefiniteCommitRejection(failure)) {
                throw failure;
            }
            // Absence does not fence the original request. Every retry uses the
            // same operation and payload, so the server serializes publication.
            // Recovery gets one request-timeout budget, including waits and IO.
            long recoveryStarted = System.nanoTime();
            RuntimeException lastFailure = failure;
            for (int attempt = 0; attempt < 3; attempt++) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                try {
                    long backoffMillis = 100L << attempt;
                    Duration delay = Duration.ofMillis(backoffMillis + ThreadLocalRandom.current().nextLong(backoffMillis));
                    if (lastFailure instanceof RetryAfterException retryAfter && retryAfter.delay.compareTo(delay) > 0) {
                        delay = retryAfter.delay;
                    }
                    if (delay.compareTo(remainingRecoveryTime(recoveryStarted)) >= 0) {
                        break;
                    }
                    Thread.sleep(delay);
                    if (commitReceipt(request.operationId(), remainingRecoveryTime(recoveryStarted)).isPresent()) {
                        return;
                    }
                    if (attempt < 2) {
                        commitOnce(request, remainingRecoveryTime(recoveryStarted));
                        return;
                    }
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(interrupted);
                    break;
                }
                catch (RuntimeException recoveryFailure) {
                    failure.addSuppressed(recoveryFailure);
                    lastFailure = recoveryFailure;
                }
            }
            throw new TrinoException(
                    HOGLAKE_CATALOG_UNAVAILABLE,
                    "Hoglake write outcome is unknown; preserve files and inspect operation " + request.operationId(),
                    failure);
        }
    }

    public Optional<HoglakeDtos.CommitReceipt> commitReceipt(String operationId)
    {
        return commitReceipt(operationId, requestTimeout);
    }

    private Optional<HoglakeDtos.CommitReceipt> commitReceipt(String operationId, Duration timeout)
    {
        Optional<HoglakeDtos.CommitReceipt> receipt = get(catalogPath("/commit/receipts/" + encode(operationId)), new TypeReference<HoglakeDtos.CommitReceipt>() {}, timeout);
        receipt.ifPresent(result -> {
            if (!operationId.equals(result.operationId()) || result.snapshotId() <= 0) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake write receipt for operation " + operationId);
            }
        });
        return receipt;
    }

    private Duration remainingRecoveryTime(long started)
    {
        Duration remaining = requestTimeout.minusNanos(System.nanoTime() - started);
        if (remaining.isNegative() || remaining.isZero()) {
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Hoglake write recovery deadline exceeded");
        }
        return remaining;
    }

    private static boolean isDefiniteCommitRejection(TrinoException failure)
    {
        return failure.getErrorCode().equals(StandardErrorCode.TRANSACTION_CONFLICT.toErrorCode()) ||
                failure.getErrorCode().equals(StandardErrorCode.INVALID_ARGUMENTS.toErrorCode()) ||
                failure.getErrorCode().equals(HOGLAKE_SNAPSHOT_EXPIRED.toErrorCode()) ||
                failure.getErrorCode().equals(HOGLAKE_CATALOG_NOT_FOUND.toErrorCode());
    }

    private void commitOnce(HoglakeDtos.Commit request, Duration timeout)
    {
        // The required-key endpoint also protects against an older replica
        // silently ignoring the ID after capability negotiation.
        String path = "/commit";
        if (request.operationId() != null) {
            path = request.deletes().isEmpty() ? "/commit/prepared" : "/commit/deletes/prepared";
        }
        HoglakeDtos.CommitResult result = write("POST", catalogPath(path), request, new TypeReference<HoglakeDtos.CommitResult>() {}, timeout);
        if (result.snapshotId() <= 0) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Hoglake commit response is missing a valid snapshot; commit outcome may be unknown");
        }
    }

    private <T> T post(String path, Object body, TypeReference<T> type)
    {
        return write("POST", path, body, type);
    }

    private <T> T write(String method, String path, Object body, TypeReference<T> type)
    {
        return write(method, path, body, type, requestTimeout);
    }

    private <T> T write(String method, String path, Object body, TypeReference<T> type, boolean ignoreMissing)
    {
        return write(method, path, body, type, requestTimeout, ignoreMissing);
    }

    private <T> T write(String method, String path, Object body, TypeReference<T> type, Duration timeout)
    {
        return write(method, path, body, type, timeout, false);
    }

    private <T> T write(String method, String path, Object body, TypeReference<T> type, Duration timeout, boolean ignoreMissing)
    {
        URI uri = URI.create(baseUri + path);
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(body)))
                    .build();
            // Send once at the transport layer. Recovery belongs to the caller
            // and requires a durable operation ID.
            HttpResponse<String> response = send(request);
            int status = response.statusCode();
            if (status == 409) {
                if (method.equals("POST") && path.equals(catalogPath("/namespaces")) && isAlreadyExists(response.body())) {
                    throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "Hoglake namespace already exists");
                }
                throw new TrinoException(StandardErrorCode.TRANSACTION_CONFLICT, "Hoglake write conflict: " + response.body());
            }
            if (status == 404 && ignoreMissing) {
                return null;
            }
            if (status == 410) {
                throw new TrinoException(HOGLAKE_SNAPSHOT_EXPIRED, "Hoglake write snapshot expired");
            }
            if (status == 404) {
                throw new TrinoException(HOGLAKE_CATALOG_NOT_FOUND, "Hoglake write target not found");
            }
            if (status == 400 || status == 422) {
                throw new TrinoException(StandardErrorCode.INVALID_ARGUMENTS, "Hoglake rejected write: " + response.body());
            }
            if (status == 429 || status >= 500) {
                throw new RetryAfterException("Hoglake write failed with HTTP " + status + "; commit outcome may be unknown", response);
            }
            if (status != 200 && status != 201) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Unexpected Hoglake write status: " + status);
            }
            try {
                T result = mapper.readValue(response.body(), type);
                if (result == null) {
                    throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Null Hoglake write response; write may have succeeded");
                }
                return result;
            }
            catch (IOException e) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Malformed Hoglake write response; write may have succeeded", e);
            }
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Hoglake write request failed; write outcome may be unknown", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Hoglake write interrupted; write outcome may be unknown", e);
        }
    }

    private boolean isAlreadyExists(String body)
    {
        try {
            JsonNode error = mapper.readTree(body);
            return error != null && "already_exists".equals(error.path("error").asText());
        }
        catch (IOException ignored) {
            // An unrecognized conflict must not make IF NOT EXISTS report success.
            return false;
        }
    }

    @Override
    public void close()
    {
        // Release the selector thread and executor without waiting on
        // in-flight requests (connector shutdown must not hang on a
        // stuck control-plane call).
        httpClient.shutdownNow();
    }

    // ---- transport ---------------------------------------------------------

    /**
     * GET + parse; empty on 404, coded TrinoException on every other failure.
     */
    private <T> Optional<T> get(String path, TypeReference<T> type)
    {
        return get(path, type, requestTimeout);
    }

    private <T> Optional<T> get(String path, TypeReference<T> type, Duration timeout)
    {
        URI uri = URI.create(baseUri + path);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = send(request);
        }
        catch (IOException e) {
            // Connection refused, DNS, timeouts: the control plane (or the
            // network to it) failed — an EXTERNAL fault, not an engine bug.
            throw new TrinoException(
                    HOGLAKE_CATALOG_UNAVAILABLE,
                    "hoglake request failed: GET " + uri,
                    e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "hoglake request interrupted: GET " + uri, e);
        }
        int status = response.statusCode();
        if (status == 404) {
            return Optional.empty();
        }
        if (status == 410) {
            // The catalog's typed expiry signal: the requested snapshot fell
            // below the expiry floor. With pinned-snapshot planning this
            // means "your read snapshot expired during the query".
            throw new TrinoException(HOGLAKE_SNAPSHOT_EXPIRED,
                    "hoglake snapshot expired during query (history below the expiry floor): GET "
                            + uri + " -> HTTP 410: " + response.body());
        }
        if (status == 429 || status >= 500) {
            throw new RetryAfterException(
                    "hoglake catalog service error: GET " + uri + " -> HTTP " + status + ": " + response.body(), response);
        }
        if (status != 200) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake request failed: GET " + uri + " -> HTTP " + status + ": " + response.body());
        }
        try {
            T result = mapper.readValue(response.body(), type);
            if (result == null) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Null Hoglake response from GET " + uri);
            }
            return Optional.of(result);
        }
        catch (IOException e) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "Malformed hoglake response from GET " + uri,
                    e);
        }
    }

    private HttpResponse<String> send(HttpRequest request)
            throws IOException, InterruptedException
    {
        // HttpRequest.timeout bounds receiving headers, not consuming the body.
        // Bound the whole response so a stalled body cannot overrun recovery.
        var response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            return response.get(request.timeout().orElseThrow().toNanos(), NANOSECONDS);
        }
        catch (TimeoutException e) {
            throw new HttpTimeoutException("Hoglake response timed out");
        }
        catch (ExecutionException e) {
            throw new IOException("Hoglake request failed", e.getCause());
        }
        finally {
            response.cancel(true);
        }
    }

    private static class RetryAfterException
            extends TrinoException
    {
        private final Duration delay;

        public RetryAfterException(String message, HttpResponse<?> response)
        {
            super(HOGLAKE_CATALOG_UNAVAILABLE, message);
            delay = response.headers().firstValue("Retry-After")
                    .map(RetryAfterException::parseDelay)
                    .orElse(Duration.ZERO);
        }

        private static Duration parseDelay(String value)
        {
            try {
                return Duration.ofSeconds(Math.max(0, Long.parseLong(value.trim())));
            }
            catch (NumberFormatException ignored) {
                try {
                    Duration delay = Duration.between(Instant.now(), ZonedDateTime.parse(value.trim(), RFC_1123_DATE_TIME).toInstant());
                    if (!delay.isNegative()) {
                        return delay;
                    }
                }
                catch (DateTimeParseException ignoredDate) {
                    // Invalid hints fall back to the normal recovery backoff.
                }
                return Duration.ZERO;
            }
        }
    }

    private TrinoException catalogNotFound()
    {
        return new TrinoException(
                HOGLAKE_CATALOG_NOT_FOUND,
                "hoglake catalog '" + catalog + "' not found (check the hoglake.catalog property)");
    }

    private String catalogPath(String suffix)
    {
        return "/v1/catalogs/" + encode(catalog) + suffix;
    }

    private String namespacePath(String namespace, String suffix)
    {
        return catalogPath("/namespaces/" + encode(namespace) + suffix);
    }

    private String tablePath(String namespace, String table, String suffix)
    {
        return namespacePath(namespace, "/tables/" + encode(table) + suffix);
    }

    private static String encode(String segment)
    {
        return URLEncoder.encode(segment, UTF_8).replace("+", "%20");
    }
}
