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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.hoglake.HoglakePosthogSchema;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED;
import static io.trino.spi.StandardErrorCode.EXCEEDED_SCAN_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

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
    private static final Logger log = Logger.get(HoglakeClient.class);

    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(2);

    /**
     * How long a catalog's refusal of a scan {@code include} value is
     * trusted before planning asks for it again, so an upgraded catalog is
     * picked up without a restart.
     */
    @VisibleForTesting
    public static final Duration REJECTED_INCLUDE_RETENTION = Duration.ofMinutes(10);

    private static final String COLUMN_STATS = "column_stats";
    private static final String SPLIT_OFFSETS = "split_offsets";

    // The detail of the catalog's 422 for an include value it does not
    // know (Routes.parseScanIncludes), e.g.
    // "include: unknown value(s) 'split_offsets'; supported: column_stats".
    private static final String UNKNOWN_INCLUDE_PREFIX = "include: unknown value(s) ";
    private static final String UNKNOWN_INCLUDE_SUFFIX = "; supported:";
    private static final Pattern QUOTED_VALUE = Pattern.compile("'([^']*)'");
    private static final Pattern ERROR_CODE = Pattern.compile("[a-z_]{1,64}");

    /**
     * Sent on every request. Hoglake counts auth outcomes per client class read
     * from this header, and its enforce flip is gated on this class reaching
     * zero refusals, so it must stay recognisable: the class is the prefix.
     */
    public static final String USER_AGENT = "trino-hoglake/1";

    /**
     * The token source of a mount client that no login was bound to. It fails, so
     * a request that did not go through {@link #withLogin} is never sent.
     */
    private static final HoglakeTokenSource WITHOUT_LOGIN = new HoglakeTokenSource()
    {
        @Override
        public Optional<String> token(Duration timeout)
        {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "posthog schema request without a login");
        }

        @Override
        public void invalidate(String token) {}
    };

    private final HttpClient httpClient;
    private final HoglakeTokenSource tokenSource;
    private final ObjectMapper mapper;
    private final String baseUri;
    private final String catalog;
    private final Duration requestTimeout;
    private final Ticker ticker;
    private final ScanIncludeSupport columnStatsSupport;
    private final ScanIncludeSupport splitOffsetsSupport;
    // The client of the posthog schema's mount catalog: its messages name the schema,
    // never a URL or a response body.
    private final boolean mount;
    // A mount client bound to one login. It shares everything else with the client
    // it was made from, which stays the owner of the HTTP client.
    private final boolean loginView;

    public HoglakeClient(String baseUri, String catalog)
    {
        this(baseUri, catalog, DEFAULT_REQUEST_TIMEOUT);
    }

    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout)
    {
        this(baseUri, catalog, requestTimeout, Ticker.systemTicker());
    }

    @VisibleForTesting
    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout, Ticker ticker)
    {
        this(baseUri, catalog, requestTimeout, ticker, HoglakeTokenSource.NONE);
    }

    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout, HoglakeTokenSource tokenSource)
    {
        this(baseUri, catalog, requestTimeout, Ticker.systemTicker(), tokenSource);
    }

    @VisibleForTesting
    public HoglakeClient(String baseUri, String catalog, Duration requestTimeout, Ticker ticker, HoglakeTokenSource tokenSource)
    {
        this(baseUri, catalog, requestTimeout, ticker, tokenSource, false);
    }

    /**
     * The client of the posthog schema: the mount catalog {@code mountCatalog} on the
     * same server. Its 404 for the catalog means the schema does not exist, and its
     * errors carry no URL or response body.
     *
     * <p>This client has no token of its own and sends nothing: every request fails
     * until {@link #withLogin} binds it to the token source of one login. The mount
     * is never asked with the org catalog's token, or with no token.
     */
    public static HoglakeClient forMount(String baseUri, String mountCatalog, Duration requestTimeout)
    {
        return new HoglakeClient(baseUri, mountCatalog, requestTimeout, Ticker.systemTicker(), WITHOUT_LOGIN, true);
    }

    @VisibleForTesting
    public static HoglakeClient forMount(String baseUri, String mountCatalog)
    {
        return forMount(baseUri, mountCatalog, DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * This mount client for one login: the same server, catalog and connections,
     * with {@code login} as the only source of its tokens. It is cheap, is made for
     * each call from the session that calls, and is not closed.
     */
    public HoglakeClient withLogin(HoglakeTokenSource login)
    {
        checkState(mount && !loginView, "Only the posthog schema's client is bound to a login");
        return new HoglakeClient(this, login);
    }

    private HoglakeClient(HoglakeClient mountClient, HoglakeTokenSource login)
    {
        this.mount = true;
        this.loginView = true;
        this.baseUri = mountClient.baseUri;
        this.catalog = mountClient.catalog;
        this.requestTimeout = mountClient.requestTimeout;
        this.ticker = mountClient.ticker;
        this.tokenSource = requireNonNull(login, "login is null");
        this.httpClient = mountClient.httpClient;
        this.mapper = mountClient.mapper;
        // What the server supports does not depend on the login
        this.columnStatsSupport = mountClient.columnStatsSupport;
        this.splitOffsetsSupport = mountClient.splitOffsetsSupport;
    }

    private HoglakeClient(String baseUri, String catalog, Duration requestTimeout, Ticker ticker, HoglakeTokenSource tokenSource, boolean mount)
    {
        this.mount = mount;
        this.loginView = false;
        this.columnStatsSupport = new ScanIncludeSupport(COLUMN_STATS);
        this.splitOffsetsSupport = new ScanIncludeSupport(SPLIT_OFFSETS);
        // Fail at construction (catalog registration), not per query.
        this.baseUri = validateBaseUri(requireNonNull(baseUri, "baseUri is null")).toString();
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.tokenSource = requireNonNull(tokenSource, "tokenSource is null");
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

    /**
     * The catalog, or empty when the server has none of this name for this caller.
     */
    public Optional<HoglakeDtos.Catalog> findCatalog()
    {
        return get(catalogPath(""), new TypeReference<HoglakeDtos.Catalog>() {});
    }

    public String catalogName()
    {
        return catalog;
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

    public void dropNamespace(String name, long expectedNamespaceId, HoglakeDtos.OperationMetadata operationMetadata)
    {
        validateLifecycleResult(write("DELETE", namespacePath(name, "?expected_namespace_id=" + expectedNamespaceId + "&" + operationMetadataQuery(operationMetadata)), Map.of(), new TypeReference<HoglakeDtos.CommitResult>() {}));
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
     * Every data file paired with its live DV at the pinned snapshot, with no
     * optional parts ({@code snapshot=N} only): the writer's view, which
     * needs neither statistics nor row-group offsets.
     */
    public List<HoglakeDtos.ScanFile> scan(String namespace, String table, long snapshot)
    {
        return fetchScan(namespace, table, "/scan?snapshot=" + snapshot);
    }

    /**
     * The split source for a read: {@link #scan(String, String, long)} plus
     * each file's row-group start offsets ({@code include=split_offsets}),
     * which split planning cuts on when present. A non-empty
     * {@code statsFields} also requests each provided file's statistics for
     * those field ids, so planning can prune files
     * ({@code include=column_stats,split_offsets&stats_fields=...}, ids
     * ascending).
     *
     * <p>Both parts are optional for planning, and a catalog that predates
     * one refuses it with a 422 naming the unknown value. That refusal
     * retries the scan without the named values, and they are left out of
     * this client's planning scans for {@link #REJECTED_INCLUDE_RETENTION},
     * after which they are requested again. Every other failure, including
     * any other 422, is reported as for any request.
     */
    public List<HoglakeDtos.ScanFile> planningScan(String namespace, String table, long snapshot, Set<Long> statsFields)
    {
        boolean columnStats = !statsFields.isEmpty() && columnStatsSupport.shouldRequest();
        boolean splitOffsets = splitOffsetsSupport.shouldRequest();
        while (true) {
            URI uri = URI.create(baseUri + tablePath(namespace, table, planningQuery(snapshot, columnStats ? statsFields : Set.of(), splitOffsets)));
            HttpResponse<String> response = fetch(uri, requestTimeout);
            Set<String> unknown = unknownIncludes(response);
            boolean retry = false;
            if (columnStats && unknown.contains(COLUMN_STATS)) {
                columnStatsSupport.markRejected();
                columnStats = false;
                retry = true;
            }
            if (splitOffsets && unknown.contains(SPLIT_OFFSETS)) {
                splitOffsetsSupport.markRejected();
                splitOffsets = false;
                retry = true;
            }
            if (!retry) {
                Optional<List<HoglakeDtos.ScanFile>> files = interpret(uri, response, new TypeReference<List<HoglakeDtos.ScanFile>>() {});
                if (columnStats) {
                    columnStatsSupport.markAccepted();
                }
                if (splitOffsets) {
                    splitOffsetsSupport.markAccepted();
                }
                return files.orElseThrow(() -> new TableNotFoundException(new SchemaTableName(namespace, table)));
            }
        }
    }

    private static String planningQuery(long snapshot, Set<Long> statsFields, boolean splitOffsets)
    {
        List<String> includes = new ArrayList<>();
        if (!statsFields.isEmpty()) {
            includes.add(COLUMN_STATS);
        }
        if (splitOffsets) {
            includes.add(SPLIT_OFFSETS);
        }
        String query = "/scan?snapshot=" + snapshot;
        if (!includes.isEmpty()) {
            query += "&include=" + String.join(",", includes);
        }
        if (!statsFields.isEmpty()) {
            query += "&stats_fields=" + statsFields.stream()
                    .sorted()
                    .map(String::valueOf)
                    .collect(joining(","));
        }
        return query;
    }

    /**
     * The include values a scan response refuses as unknown: those named by
     * a 422 {@code {"error": "validation", "detail": "include: unknown
     * value(s) 'a', 'b'; supported: ..."}}, and none for any other response.
     */
    private Set<String> unknownIncludes(HttpResponse<String> response)
    {
        if (response.statusCode() != 422) {
            return ImmutableSet.of();
        }
        JsonNode error;
        try {
            error = mapper.readTree(response.body());
        }
        catch (IOException ignored) {
            // Not the catalog's error body, so not the refusal this handles.
            return ImmutableSet.of();
        }
        if (error == null || !"validation".equals(error.path("error").asText())) {
            return ImmutableSet.of();
        }
        String detail = error.path("detail").asText();
        int end = detail.indexOf(UNKNOWN_INCLUDE_SUFFIX);
        if (!detail.startsWith(UNKNOWN_INCLUDE_PREFIX) || end < 0) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        Matcher matcher = QUOTED_VALUE.matcher(detail.substring(UNKNOWN_INCLUDE_PREFIX.length(), end));
        while (matcher.find()) {
            values.add(matcher.group(1));
        }
        return values.build();
    }

    private List<HoglakeDtos.ScanFile> fetchScan(String namespace, String table, String query)
    {
        return get(tablePath(namespace, table, query),
                new TypeReference<List<HoglakeDtos.ScanFile>>() {})
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(namespace, table)));
    }

    public HoglakeDtos.Table createTable(String namespace, String table, List<HoglakeDtos.ColumnDefinition> columns)
    {
        return post(namespacePath(namespace, "/tables"),
                new HoglakeDtos.CreateTable(table, columns),
                new TypeReference<HoglakeDtos.Table>() {});
    }

    /**
     * @param replacement the table that the creation replaces, or null to create a table
     * @param operationMetadata who replaces the table and why, which the server requires to
     *         replace an existing table, or null otherwise
     */
    public HoglakeDtos.TableCreation prepareTableCreation(String operationId, String namespace, String table, List<HoglakeDtos.ColumnDefinition> columns, HoglakeDtos.ReplacementTarget replacement, List<HoglakeDtos.PartitionField> partitionFields, List<HoglakeDtos.SortField> sortFields, String comment, Map<String, String> properties, HoglakeDtos.OperationMetadata operationMetadata)
    {
        checkArgument(operationMetadata != null || replacement == null || replacement.expectedTableUuid() == null, "Replacing an existing table requires operation metadata");
        Map<String, Object> definition = new HashMap<>();
        if (operationMetadata != null) {
            definition.put("author", operationMetadata.author());
            definition.put("message", operationMetadata.message());
        }
        if (comment != null) {
            definition.put("comment", comment);
        }
        if (!properties.isEmpty()) {
            definition.put("properties", properties);
        }
        boolean metadata = comment != null || !properties.isEmpty() || columns.stream().anyMatch(HoglakeDtos.ColumnDefinition::hasComments);
        if (!partitionFields.isEmpty()) {
            definition.put("partition_fields", partitionFields);
        }
        if (!sortFields.isEmpty()) {
            definition.put("sort_fields", sortFields);
        }
        definition.put("namespace", namespace);
        definition.put("name", table);
        definition.put("columns", columns);
        if (replacement != null) {
            definition.put("replacement", replacement);
        }
        return write(
                "PUT",
                catalogPath("/table-creations/" + encode(operationId) + (metadata ? "/metadata" : !sortFields.isEmpty() ? "/sorted" : partitionFields.isEmpty() ? "" : "/partitioned")),
                definition,
                new TypeReference<HoglakeDtos.TableCreation>() {});
    }

    public String claimUpload(String owner, String prefix, String kind)
    {
        String id = UUID.randomUUID().toString();
        HoglakeDtos.UploadClaim claim = write("PUT", catalogPath("/uploads/" + id), Map.of("owner", owner, "prefix", prefix, "file_kind", kind), new TypeReference<HoglakeDtos.UploadClaim>() {});
        String normalized = prefix.endsWith("/") ? prefix : prefix + "/";
        if (!id.equals(claim.uploadId()) || !owner.equals(claim.owner()) || !"active".equals(claim.state()) || claim.path() == null || !claim.path().startsWith(normalized + "trino-upload/")) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid upload claim response");
        }
        return claim.path();
    }

    public void renewUploads(String owner)
    {
        post(catalogPath("/uploads/renew"), Map.of("owner", owner), new TypeReference<Map<String, Integer>>() {});
    }

    public void abandonUploads(String owner, List<String> paths)
    {
        post(catalogPath("/uploads/abandon"), Map.of("owner", owner, "paths", paths), new TypeReference<Map<String, Integer>>() {});
    }

    public HoglakeDtos.TableCreation publishTableCreation(String operationId, List<HoglakeDtos.FileRegistration> files)
    {
        return validateCreationReceipt(operationId, post(catalogPath("/table-creations/" + encode(operationId) + "/commit"), Map.of("files", files), new TypeReference<HoglakeDtos.TableCreation>() {}));
    }

    public HoglakeDtos.TableCreation publishTableCreation(String operationId, List<HoglakeDtos.FileRegistration> files, boolean claimedUploads)
    {
        if (!claimedUploads) {
            return publishTableCreation(operationId, files);
        }
        return validateCreationReceipt(operationId, post(catalogPath("/table-creations/" + encode(operationId) + "/commit/uploads"), Map.of("files", files), new TypeReference<HoglakeDtos.TableCreation>() {}));
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

    public void dropTable(String namespace, String table, String expectedTableUuid, HoglakeDtos.OperationMetadata operationMetadata)
    {
        validateLifecycleResult(write("DELETE", tablePath(namespace, table, "?expected_table_uuid=" + encode(expectedTableUuid) + "&" + operationMetadataQuery(operationMetadata)), Map.of(), new TypeReference<HoglakeDtos.CommitResult>() {}));
    }

    public void truncateTable(String namespace, String table, String expectedTableUuid, HoglakeDtos.OperationMetadata operationMetadata)
    {
        validateLifecycleResult(post(tablePath(namespace, table, "/truncate?expected_table_uuid=" + encode(expectedTableUuid)), operationMetadata, new TypeReference<HoglakeDtos.CommitResult>() {}));
    }

    // The server reads the metadata of a DELETE from its query
    private static String operationMetadataQuery(HoglakeDtos.OperationMetadata operationMetadata)
    {
        return "author=" + encode(operationMetadata.author()) + "&message=" + encode(operationMetadata.message());
    }

    private static void validateLifecycleResult(HoglakeDtos.CommitResult result)
    {
        if (result.snapshotId() <= 0) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake lifecycle response; outcome may be unknown");
        }
    }

    public void dropStagingTable(String namespace, String table, HoglakeDtos.OperationMetadata operationMetadata)
    {
        write("DELETE", tablePath(namespace, table, "?" + operationMetadataQuery(operationMetadata)), Map.of(), new TypeReference<Object>() {}, true);
    }

    public void commit(HoglakeDtos.Commit request)
    {
        commit(request, false, false, false);
    }

    public void commitMutation(HoglakeDtos.Commit request)
    {
        commit(request, true, false, false);
    }

    public void commitClaimed(HoglakeDtos.Commit request, boolean mutation)
    {
        commit(request, mutation, true, false);
    }

    public void commitTransaction(HoglakeDtos.Commit request)
    {
        commit(request, true, true, true);
    }

    private void commit(HoglakeDtos.Commit request, boolean mutation, boolean claimedUploads, boolean transaction)
    {
        commitWithRecovery(request.operationId(), timeout -> commitOnce(request, timeout, mutation, claimedUploads, transaction));
    }

    /**
     * Ends files and appends files in one snapshot. The result is the number of data files the
     * commit ended, or empty when only a receipt confirms the commit and so the count is unknown.
     */
    public OptionalLong commitReplacement(HoglakeDtos.ReplacementCommit request)
    {
        AtomicReference<OptionalLong> ended = new AtomicReference<>(OptionalLong.empty());
        commitWithRecovery(request.operationId(), timeout -> {
            HoglakeDtos.CommitResult result = write("POST", catalogPath("/commit/replacements/prepared"), request, new TypeReference<HoglakeDtos.CommitResult>() {}, timeout);
            if (result.snapshotId() <= 0 || (result.endedFiles() != null && result.endedFiles() < 0)) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Hoglake replacement response is missing a valid snapshot; commit outcome may be unknown");
            }
            // A replay answered from the receipt has no count
            if (result.endedFiles() == null) {
                return;
            }
            ended.set(OptionalLong.of(result.endedFiles()));
        });
        return ended.get();
    }

    @FunctionalInterface
    private interface CommitAttempt
    {
        void send(Duration timeout);
    }

    private void commitWithRecovery(String operationId, CommitAttempt attempt)
    {
        try {
            attempt.send(requestTimeout);
            return;
        }
        catch (TrinoException failure) {
            if (operationId == null || isDefiniteCommitRejection(failure)) {
                throw failure;
            }
            // Absence does not fence the original request. Every retry uses the
            // same operation and payload, so the server serializes publication.
            // Recovery gets one request-timeout budget, including waits and IO.
            long recoveryStarted = System.nanoTime();
            RuntimeException lastFailure = failure;
            for (int attemptNumber = 0; attemptNumber < 3; attemptNumber++) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                try {
                    long backoffMillis = 100L << attemptNumber;
                    Duration delay = Duration.ofMillis(backoffMillis + ThreadLocalRandom.current().nextLong(backoffMillis));
                    if (lastFailure instanceof RetryAfterException retryAfter && retryAfter.delay.compareTo(delay) > 0) {
                        delay = retryAfter.delay;
                    }
                    if (delay.compareTo(remainingRecoveryTime(recoveryStarted)) >= 0) {
                        break;
                    }
                    Thread.sleep(delay);
                    if (commitReceipt(operationId, remainingRecoveryTime(recoveryStarted)).isPresent()) {
                        return;
                    }
                    if (attemptNumber < 2) {
                        attempt.send(remainingRecoveryTime(recoveryStarted));
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
                    "Hoglake write outcome is unknown; preserve files and inspect operation " + operationId,
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

    private void commitOnce(HoglakeDtos.Commit request, Duration timeout, boolean mutation, boolean claimedUploads, boolean transaction)
    {
        // The required-key endpoint also protects against an older replica
        // silently ignoring the ID after capability negotiation.
        String path = "/commit";
        if (request.operationId() != null) {
            path = request.deletes().isEmpty() ? "/commit/prepared" : "/commit/deletes/prepared";
        }
        if (mutation) {
            path = "/commit/mutations/prepared";
        }
        if (claimedUploads) {
            path = "/commit/uploads";
        }
        if (transaction) {
            path = "/commit/transaction";
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

    /**
     * The request as user-visible text: the method and URL for the org catalog, as
     * before, and only the schema for the posthog schema's mount.
     */
    private String describe(URI uri)
    {
        if (mount) {
            return "a request for the posthog schema";
        }
        return "GET " + uri;
    }

    /**
     * A failed answer as user-visible text: with the body for the org catalog, as
     * before, and with only the status and the server's error word for the mount.
     */
    private String describe(URI uri, HttpResponse<String> response)
    {
        if (mount) {
            return describe(uri) + " -> HTTP " + response.statusCode() + errorCode(response.body()).map(code -> " (" + code + ")").orElse("");
        }
        return describe(uri) + " -> HTTP " + response.statusCode() + ": " + response.body();
    }

    /**
     * The typed refusals of a mount and of the mount Client that proxies it, for the
     * posthog schema's client only
     * (hoglake auth-tenancy.md 6.5a and 6.5b), as Trino errors with fixed messages:
     * never the response body or a URL. Empty for every other answer.
     */
    private Optional<TrinoException> typedRefusal(HttpResponse<String> response)
    {
        Optional<String> code = errorCode(response.body());
        if (code.isEmpty()) {
            return Optional.empty();
        }
        String subject = "the posthog schema";
        TrinoException refusal = switch (code.get()) {
            case "read_only_catalog" -> new TrinoException(NOT_SUPPORTED, "Writes are not supported: " + subject + " is read-only");
            case "mount_operation_unavailable" -> new TrinoException(NOT_SUPPORTED, "This operation is not supported on " + subject);
            case "mount_scan_too_many_files" -> new TrinoException(EXCEEDED_SCAN_LIMIT, "The scan of " + subject + " plans too many files; add a filter that selects fewer");
            case "mount_scan_timeout" -> new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "Planning the scan of " + subject + " timed out on the Hoglake server; retry later");
            case "mount_scan_busy" -> new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "The Hoglake server is busy planning scans of " + subject + "; retry later");
            case "mount_host_unavailable" -> new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "The events host serving " + subject + " is unavailable; retry later");
            case "mount_proxy_busy" -> new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "The Hoglake server is busy forwarding requests for " + subject + "; retry later");
            case "mount_host_invalid_response" -> new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "The events host serving " + subject + " answered outside its contract");
            default -> null;
        };
        return Optional.ofNullable(refusal);
    }

    /**
     * The {@code error} word of a Hoglake error body, if it is one: a short
     * lowercase token, so nothing else of the body can reach a message.
     */
    private Optional<String> errorCode(String body)
    {
        if (body == null || body.isEmpty()) {
            return Optional.empty();
        }
        try {
            JsonNode error = mapper.readTree(body);
            if (error == null || !error.path("error").isTextual()) {
                return Optional.empty();
            }
            String code = error.path("error").asText();
            if (!ERROR_CODE.matcher(code).matches()) {
                return Optional.empty();
            }
            return Optional.of(code);
        }
        catch (IOException ignored) {
            return Optional.empty();
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
        if (loginView) {
            // The HTTP client belongs to the client this view was made from
            return;
        }
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
        return interpret(uri, fetch(uri, timeout), type);
    }

    /**
     * GET; coded TrinoException on transport failure, the response as
     * received otherwise.
     */
    private HttpResponse<String> fetch(URI uri, Duration timeout)
    {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        try {
            return send(request);
        }
        catch (IOException e) {
            // Connection refused, DNS, timeouts: the control plane (or the
            // network to it) failed — an EXTERNAL fault, not an engine bug.
            throw new TrinoException(
                    HOGLAKE_CATALOG_UNAVAILABLE,
                    "hoglake request failed: " + describe(uri),
                    cause(e));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "hoglake request interrupted: " + describe(uri), e);
        }
    }

    /**
     * A GET response parsed; empty on 404, coded TrinoException on every
     * other failure.
     */
    private <T> Optional<T> interpret(URI uri, HttpResponse<String> response, TypeReference<T> type)
    {
        int status = response.statusCode();
        if (status == 404) {
            return Optional.empty();
        }
        if (status != 200 && mount) {
            Optional<TrinoException> refusal = typedRefusal(response);
            if (refusal.isPresent()) {
                throw refusal.get();
            }
        }
        if (status == 410) {
            // The catalog's typed expiry signal: the requested snapshot fell
            // below the expiry floor. With pinned-snapshot planning this
            // means "your read snapshot expired during the query".
            throw new TrinoException(HOGLAKE_SNAPSHOT_EXPIRED,
                    "hoglake snapshot expired during query (history below the expiry floor): "
                            + describe(uri, response));
        }
        if (status == 429 || status >= 500) {
            throw new RetryAfterException(
                    "hoglake catalog service error: " + describe(uri, response), response);
        }
        if (status != 200) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake request failed: " + describe(uri, response));
        }
        try {
            T result = mapper.readValue(response.body(), type);
            if (result == null) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Null Hoglake response from " + describe(uri));
            }
            return Optional.of(result);
        }
        catch (IOException e) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "Malformed hoglake response from " + describe(uri),
                    cause(e));
        }
    }

    /**
     * Every request goes through here, so this is where the credential and the
     * User-Agent are attached.
     *
     * A 401 is retried ONCE with a freshly minted token, and that is safe for
     * writes too, despite "send once" above: Hoglake decides authentication
     * before any handler runs, so a 401 means the request did nothing. The
     * cases it covers are the ones a fresh token fixes — revocation, a signing
     * key this Hoglake instance has not loaded yet, expiry by clock skew.
     *
     * The posthog schema's client follows the same rule with the token of the
     * login it is bound to.
     *
     * The request's timeout is one deadline for all of it: getting a token
     * (waiting for another caller's mint included) and both attempts each get
     * only what is left. Write recovery passes its remaining budget in here,
     * and a retry must not reach the server after that budget has run out.
     */
    private HttpResponse<String> send(HttpRequest request)
            throws IOException, InterruptedException
    {
        long deadline = System.nanoTime() + request.timeout().orElseThrow().toNanos();
        Optional<String> token = token(remaining(deadline));
        HttpResponse<String> response = sendOnce(authorized(request, token, remaining(deadline)));
        if (response.statusCode() != 401 || token.isEmpty()) {
            return response;
        }
        // For the mount this is the login's own source: only that login's token is
        // dropped, and the org catalog's token is not involved.
        tokenSource.invalidate(token.get());
        Optional<String> replacement = token(remaining(deadline));
        return sendOnce(authorized(request, replacement, remaining(deadline)));
    }

    private Optional<String> token(Duration timeout)
    {
        Optional<String> token = tokenSource.token(timeout);
        if (mount && token.isEmpty()) {
            // A login's source has a token or fails. The mount is never asked without one.
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "posthog schema request without a token");
        }
        return token;
    }

    /**
     * The cause attached to a user-visible failure: none for the mount, whose
     * transport and parser errors can name the server or quote its answer.
     */
    private Throwable cause(Exception e)
    {
        if (!mount) {
            return e;
        }
        log.debug(e, "Hoglake request for the posthog schema of catalog '%s' failed", catalog);
        return null;
    }

    private static Duration remaining(long deadline)
            throws HttpTimeoutException
    {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new HttpTimeoutException("Hoglake request deadline exceeded");
        }
        return Duration.ofNanos(remaining);
    }

    private static HttpRequest authorized(HttpRequest request, Optional<String> token, Duration timeout)
    {
        HttpRequest.Builder builder = HttpRequest.newBuilder(request, (name, _) -> !name.equalsIgnoreCase("Authorization"))
                .timeout(timeout)
                .setHeader("User-Agent", USER_AGENT);
        token.ifPresent(value -> builder.setHeader("Authorization", "Bearer " + value));
        return builder.build();
    }

    private HttpResponse<String> sendOnce(HttpRequest request)
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

    /**
     * Whether this client's catalog accepts one optional scan include
     * value. A refusal is trusted for {@link #REJECTED_INCLUDE_RETENTION};
     * each change of belief is logged once.
     */
    private final class ScanIncludeSupport
    {
        private final String value;
        // Written under this object's lock; rejectedAt is written before
        // rejected, so a reader seeing rejected also sees its time.
        private volatile boolean rejected;
        private volatile long rejectedAt;

        private ScanIncludeSupport(String value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        public boolean shouldRequest()
        {
            return !rejected || ticker.read() - rejectedAt >= REJECTED_INCLUDE_RETENTION.toNanos();
        }

        public synchronized void markRejected()
        {
            rejectedAt = ticker.read();
            if (!rejected) {
                rejected = true;
                log.info("Hoglake catalog '%s' does not support scan include=%s; planning without it, and asking again after %s minutes", catalog, value, REJECTED_INCLUDE_RETENTION.toMinutes());
            }
        }

        public void markAccepted()
        {
            if (!rejected) {
                return;
            }
            synchronized (this) {
                if (rejected) {
                    rejected = false;
                    log.info("Hoglake catalog '%s' now supports scan include=%s; planning with it again", catalog, value);
                }
            }
        }
    }

    private TrinoException catalogNotFound()
    {
        if (mount) {
            // The org has no events mount, or the server does not show it to this token.
            return new SchemaNotFoundException(HoglakePosthogSchema.SCHEMA_NAME);
        }
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
