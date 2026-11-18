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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.FeaturesConfig;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.session;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * A remote schema against a fake Hoglake that plays the catalog's server and relays
 * the remote schema {@code team_42} with the answers a relaying server gives. The two
 * have their own snapshot spaces: this catalog's head is 5, and the remote tables
 * read at 9001 ({@code events}) and 7001 ({@code orders}). Every request is recorded,
 * and a remote read at a snapshot outside the remote catalog's range is refused as
 * the server refuses it, so this catalog's snapshot fails loudly if it is ever sent.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Tests mutate the shared server state
final class TestHoglakeRemoteTablePin
{
    private static final String CATALOG_PATH = "/v1/catalogs/lake";
    private static final String NAMESPACES_PATH = CATALOG_PATH + "/namespaces";
    private static final String LOCAL_NAMESPACE_PATH = NAMESPACES_PATH + "/ns1";
    private static final String LOCAL_PATH = LOCAL_NAMESPACE_PATH + "/tables/t";
    private static final String REMOTE_PATH = NAMESPACES_PATH + "/team_42";
    private static final String EVENTS_PATH = REMOTE_PATH + "/tables/events";
    private static final String ORDERS_PATH = REMOTE_PATH + "/tables/orders";
    private static final SchemaTableName LOCAL = new SchemaTableName("ns1", "t");
    private static final SchemaTableName EVENTS = new SchemaTableName("team_42", "events");
    private static final SchemaTableName ORDERS = new SchemaTableName("team_42", "orders");
    private static final String READ_ONLY = "Hoglake remote schema 'team_42' is read-only";
    // The oldest snapshot the remote catalog still serves
    private static final long REMOTE_FLOOR = 7000;
    private static final Pattern SNAPSHOT_PARAMETER = Pattern.compile("[?&]snapshot=([^&]*)");

    private record Request(String method, String target)
    {
        public String path()
        {
            int query = target.indexOf('?');
            return query < 0 ? target : target.substring(0, query);
        }

        public Optional<String> snapshot()
        {
            Matcher matcher = SNAPSHOT_PARAMETER.matcher(target);
            if (!matcher.find()) {
                return Optional.empty();
            }
            return Optional.of(matcher.group(1));
        }

        @Override
        public String toString()
        {
            return method + " " + target;
        }
    }

    private record Response(int status, String body) {}

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    // Mode A lists the remote schema, as for a caller allowed that team; mode B does not
    private volatile boolean listsRemoteSchema;
    private volatile long eventsHead;
    private volatile long ordersHead;
    private volatile int remoteNamespaceStatus;
    private volatile String eventsHeadTotals;
    private volatile String localTableExtra;
    private volatile boolean remoteReadSnapshot;
    private volatile Optional<Long> eventsAnswerSnapshot;
    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
        String uri = "http://127.0.0.1:" + server.getAddress().getPort();
        client = new HoglakeClient(uri, "lake");
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("team_42").build());
        queryRunner.installPlugin(new HoglakePlugin());
        queryRunner.createCatalog("hoglake", "hoglake", ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", "lake")
                // Storage is never reached: every scan here is empty
                .put("s3.endpoint", "http://127.0.0.1:9")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "synthetic-test")
                .put("s3.aws-secret-key", "synthetic-test-password")
                .buildOrThrow());
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @BeforeEach
    void reset()
    {
        listsRemoteSchema = true;
        eventsHead = 9001;
        ordersHead = 7001;
        remoteNamespaceStatus = 200;
        eventsHeadTotals = "";
        localTableExtra = "";
        remoteReadSnapshot = true;
        eventsAnswerSnapshot = Optional.empty();
        requests.clear();
    }

    @Test
    void testFirstReadIsUnpinnedAndEveryLaterReadUsesThePin()
    {
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        HoglakeTableHandle events = handle(metadata, EVENTS);
        assertThat(events.snapshotId()).isEqualTo(9001);
        assertThat(events.remote()).isTrue();
        assertThat(events.stagedFiles()).isEmpty();
        // No request for this catalog's head: a remote table never needs it
        assertThat(requests).containsExactly(get(REMOTE_PATH), get(EVENTS_PATH));

        // The table's definition and both scans are read at the pin
        requests.clear();
        metadata.getTableMetadata(session(), events);
        HoglakeSplitManager.prunedScan(client, events);
        HoglakeSplitManager.scan(client, events);
        assertThat(requests).containsExactly(
                get(EVENTS_PATH + "?snapshot=9001"),
                get(EVENTS_PATH + "/scan?snapshot=9001&include=split_offsets"),
                get(EVENTS_PATH + "/scan?snapshot=9001"));

        // The remote catalog moves on: the table is read again at its pin, never unpinned
        eventsHead = 9002;
        requests.clear();
        assertThat(handle(metadata, EVENTS).snapshotId()).isEqualTo(9001);
        assertThat(requests).containsExactly(get(EVENTS_PATH + "?snapshot=9001"));

        // Another table has a pin of its own
        requests.clear();
        assertThat(handle(metadata, ORDERS).snapshotId()).isEqualTo(7001);
        assertThat(requests).containsExactly(get(ORDERS_PATH));
    }

    @Test
    void testNewTransactionTakesANewPin()
    {
        HoglakeMetadata connector = new HoglakeMetadata(client);
        HoglakeTableHandle first = handle(connector.newTransaction(true), EVENTS);
        eventsHead = 9005;
        HoglakeTableHandle second = handle(connector.newTransaction(false), EVENTS);
        assertThat(first.snapshotId()).isEqualTo(9001);
        assertThat(second.snapshotId()).isEqualTo(9005);
        assertThat(requests).containsExactly(get(REMOTE_PATH), get(EVENTS_PATH), get(REMOTE_PATH), get(EVENTS_PATH));
    }

    /**
     * Whatever touches a remote schema, through the engine or in an explicit
     * transaction that also reads a local table, never carries this catalog's
     * snapshot; the local table still does.
     */
    @Test
    void testCatalogSnapshotNeverReachesTheRemoteSchema()
    {
        for (String query : ImmutableList.of(
                "SELECT count(*) FROM events",
                "SELECT * FROM events a JOIN events b ON a.v = b.v JOIN orders c ON a.v = c.v",
                "DESCRIBE events",
                "SHOW STATS FOR events",
                "SELECT * FROM information_schema.columns WHERE table_schema = 'team_42'")) {
            queryRunner.execute(query);
        }
        assertThat(requests).isNotEmpty();
        assertThat(requests).noneMatch(request -> request.path().equals(CATALOG_PATH));
        assertNoCatalogSnapshotSentToTheRemoteSchema();
        // A self-join reads the table at the pin the first reference took
        assertThat(requests.stream().filter(request -> request.path().equals(EVENTS_PATH)).map(Request::snapshot))
                .contains(Optional.empty(), Optional.of("9001"))
                .allMatch(snapshot -> snapshot.isEmpty() || snapshot.get().equals("9001"));

        requests.clear();
        HoglakeMetadata transaction = new HoglakeMetadata(client).newTransaction(false);
        for (SchemaTableName name : List.of(LOCAL, EVENTS, ORDERS, LOCAL, EVENTS)) {
            HoglakeTableHandle handle = handle(transaction, name);
            transaction.getTableMetadata(session(), handle);
            HoglakeSplitManager.prunedScan(client, handle);
        }
        assertNoCatalogSnapshotSentToTheRemoteSchema();
        assertThat(requests.stream().filter(request -> request.path().startsWith(LOCAL_PATH)).map(Request::snapshot))
                .hasSize(6)
                .containsOnly(Optional.of("5"));
        assertThat(requests).filteredOn(request -> request.path().equals(CATALOG_PATH)).hasSize(1);
    }

    @Test
    void testNamespaceIsAskedOncePerSchemaPerTransaction()
    {
        listsRemoteSchema = false;
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(false);
        handle(metadata, EVENTS);
        handle(metadata, ORDERS);
        handle(metadata, EVENTS);
        assertThat(metadata.schemaExists(session(), "team_42")).isTrue();
        handle(metadata, LOCAL);
        handle(metadata, LOCAL);
        assertThat(requests).filteredOn(request -> request.path().equals(REMOTE_PATH)).containsExactly(get(REMOTE_PATH));
        assertThat(requests).filteredOn(request -> request.path().equals(LOCAL_NAMESPACE_PATH)).containsExactly(get(LOCAL_NAMESPACE_PATH));
    }

    @Test
    void testLocalSelectSendsOneMoreRequestOnly()
    {
        queryRunner.execute("SELECT * FROM ns1.t");
        assertThat(requests).containsExactly(
                // The one new request: is the schema remote?
                get(LOCAL_NAMESPACE_PATH),
                // The rest are what a local SELECT sent before: the handle, the engine's
                // reads of the table's metadata, and the planning scan
                get(CATALOG_PATH),
                get(LOCAL_PATH + "?snapshot=5"),
                get(LOCAL_PATH + "?snapshot=5"),
                get(LOCAL_PATH + "?snapshot=5"),
                get(LOCAL_PATH + "?snapshot=5"),
                get(LOCAL_PATH + "/scan?snapshot=5&include=split_offsets"));
    }

    @Test
    void testSchemaTheListingLeavesOutIsOpenedByName()
    {
        // Mode A: the listing holds it, and nothing else is asked
        assertThat(new HoglakeMetadata(client).newTransaction(true).schemaExists(session(), "team_42")).isTrue();
        assertThat(requests).containsExactly(get(NAMESPACES_PATH));

        listsRemoteSchema = false;
        requests.clear();
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        assertThat(metadata.schemaExists(session(), "team_42")).isTrue();
        assertThat(metadata.schemaExists(session(), "unknown")).isFalse();
        assertThat(requests).containsExactly(get(NAMESPACES_PATH), get(REMOTE_PATH), get(NAMESPACES_PATH), get(NAMESPACES_PATH + "/unknown"));
        assertThat(handle(metadata, EVENTS).snapshotId()).isEqualTo(9001);

        assertThat(queryRunner.execute("SHOW TABLES FROM hoglake.team_42").getOnlyColumnAsSet()).containsOnly("events", "orders");
        queryRunner.execute("USE hoglake.team_42");
        assertThat(queryRunner.execute("SELECT count(*) FROM hoglake.team_42.events").getOnlyValue()).isEqualTo(0L);
        assertThatThrownBy(() -> queryRunner.execute("SHOW TABLES FROM hoglake.unknown"))
                .hasMessageContaining("Schema 'unknown' does not exist");
        assertNoCatalogSnapshotSentToTheRemoteSchema();
    }

    @Test
    void testNamespaceLookupFailures()
    {
        // 404: not remote, so the local path runs and finds nothing, as before
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        assertThat(metadata.getTableHandle(session(), new SchemaTableName("gone", "x"), Optional.empty(), Optional.empty())).isNull();
        assertThat(requests).containsExactly(get(NAMESPACES_PATH + "/gone"), get(CATALOG_PATH), get(NAMESPACES_PATH + "/gone/tables/x?snapshot=5"));

        // Any other failure is reported, never taken as local, and no table is read
        remoteNamespaceStatus = 503;
        requests.clear();
        assertUnavailable(() -> new HoglakeMetadata(client).newTransaction(true).getTableHandle(session(), EVENTS, Optional.empty(), Optional.empty()));
        listsRemoteSchema = false;
        assertUnavailable(() -> new HoglakeMetadata(client).newTransaction(true).schemaExists(session(), "team_42"));
        assertThat(requests).noneMatch(request -> request.path().contains("/tables"));
        assertThat(requests).noneMatch(request -> request.path().equals(CATALOG_PATH));
    }

    @Test
    void testEveryWriteToARemoteTableIsRefusedBeforeAnythingIsSent()
    {
        List<String> statements = ImmutableList.of(
                "INSERT INTO events VALUES 1",
                "DELETE FROM events WHERE v = 1",
                "DELETE FROM events",
                "UPDATE events SET v = 2",
                "MERGE INTO events t USING (VALUES 1) s(x) ON t.v = s.x WHEN MATCHED THEN DELETE",
                "MERGE INTO events t USING (VALUES 1) s(x) ON t.v = s.x WHEN NOT MATCHED THEN INSERT VALUES (s.x)",
                "DROP TABLE events",
                "TRUNCATE TABLE events",
                "ALTER TABLE events RENAME TO renamed",
                "ALTER TABLE events ADD COLUMN added bigint",
                "ALTER TABLE events RENAME COLUMN v TO renamed",
                "ALTER TABLE events ALTER COLUMN v SET DATA TYPE decimal(38, 0)",
                "COMMENT ON TABLE events IS 'x'",
                "COMMENT ON COLUMN events.v IS 'x'",
                "ALTER TABLE events SET PROPERTIES extra_properties = MAP(ARRAY['a'], ARRAY['b'])",
                "ALTER TABLE events EXECUTE add_files(location => 's3://lake/data/team_42/events/', import_id => 'run1')",
                "CREATE TABLE created (x bigint)",
                "CREATE TABLE created AS SELECT 1 x",
                "CREATE OR REPLACE TABLE events AS SELECT BIGINT '1' v");
        for (String statement : statements) {
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(statement))
                    .as(statement)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage(READ_ONLY);
        }

        // The handle alone is refused, in a transaction that has not asked about its schema
        ConnectorSession session = session();
        HoglakeTableHandle events = handle(new HoglakeMetadata(client).newTransaction(true), EVENTS);
        ColumnHandle column = events.columns().getFirst();
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        List<Executable> writes = ImmutableList.of(
                () -> metadata.dropColumn(session, events, column),
                () -> metadata.setColumnType(session, events, column, BIGINT),
                () -> metadata.addColumn(session, events, new ColumnMetadata("added", BIGINT), new ColumnPosition.Last()),
                () -> metadata.beginInsert(session, events, List.of(column), RetryMode.NO_RETRIES),
                () -> metadata.beginMerge(session, events, Map.of(), RetryMode.NO_RETRIES),
                () -> metadata.finishMerge(session, new HoglakeDeleteHandle(events, "s3://lake/data/team_42/events/", "operation"), List.of(), List.of(), List.of()),
                () -> metadata.dropTable(session, events),
                () -> metadata.truncateTable(session, events),
                // Refused from the namespace answer it asks for
                () -> metadata.dropSchema(session, "team_42", false));
        for (Executable write : writes) {
            assertTrinoExceptionThrownBy(write::execute)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage(READ_ONLY);
        }
        assertThat(requests).allMatch(request -> request.method().equals("GET"));
        // Only dropSchema reads this catalog, to check its capabilities, before it asks for the namespace
        assertThat(requests).filteredOn(request -> request.path().equals(CATALOG_PATH)).hasSize(1);
        assertNoCatalogSnapshotSentToTheRemoteSchema();
    }

    @Test
    void testTimeTravelIsStillRefused()
    {
        assertTrinoExceptionThrownBy(() -> new HoglakeMetadata(client).newTransaction(true).getTableHandle(
                session(), EVENTS, Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 9000L)), Optional.empty()))
                .hasErrorCode(NOT_SUPPORTED);
        assertThat(requests).isEmpty();
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM events FOR TIMESTAMP AS OF TIMESTAMP '2026-01-01 00:00:00 UTC'"))
                .hasMessageContaining("Table versioning (time travel) not yet supported");
        assertThat(requests).noneMatch(request -> request.path().startsWith(REMOTE_PATH + "/tables"));
    }

    @Test
    void testServerAnswersThatContradictTheClassAreRefused()
    {
        // A local table the server calls remote: this catalog's snapshot reached a remote table
        localTableExtra = ", \"remote\":true";
        assertTrinoExceptionThrownBy(() -> handle(new HoglakeMetadata(client).newTransaction(true), LOCAL))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE);

        // A remote table without the snapshot it was read at cannot be pinned
        remoteReadSnapshot = false;
        assertTrinoExceptionThrownBy(() -> handle(new HoglakeMetadata(client).newTransaction(true), EVENTS))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE);

        // A pinned read answered at another snapshot is not the pinned table
        remoteReadSnapshot = true;
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        handle(metadata, EVENTS);
        eventsAnswerSnapshot = Optional.of(9002L);
        assertTrinoExceptionThrownBy(() -> handle(metadata, EVENTS))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE);
    }

    /**
     * A remote table's row count is the exact total at its own pin, from a read at that
     * pin. A head read's total is a sample, and this catalog's snapshot is never asked.
     */
    @Test
    void testRowCountComesOnlyFromAReadAtThePin()
    {
        eventsHeadTotals = ", \"record_count\":900, \"file_count\":2, \"file_size_bytes\":2048, \"totals_snapshot_id\":8990";
        HoglakeMetadata metadata = new HoglakeMetadata(client).newTransaction(true);
        HoglakeTableHandle first = handle(metadata, EVENTS);
        assertThat(metadata.getTableStatistics(session(), first)).isEqualTo(TableStatistics.empty());

        HoglakeTableHandle pinned = handle(metadata, EVENTS);
        assertThat(pinned.recordCount()).hasValue(1200);
        assertThat(metadata.getTableStatistics(session(), pinned).getRowCount()).isEqualTo(Estimate.of(1200));
        assertThat(metadata.getTableStatistics(session(), pinned.withCountOnly()).getRowCount()).isEqualTo(Estimate.of(1200));
        assertNoCatalogSnapshotSentToTheRemoteSchema();
    }

    @Test
    void testTableHandleJson()
    {
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(new BlockEncodingSimdSupport(new FeaturesConfig())), TESTING_TYPE_MANAGER);
        JsonCodec<HoglakeTableHandle> codec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)))
                .withJsonDeserializers(Map.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde))).get())
                .jsonCodec(HoglakeTableHandle.class);
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "v", "long", null, true));
        HoglakeTableHandle local = new HoglakeTableHandle("ns1", "t", 5, "uuid-t", List.of(column));
        HoglakeTableHandle remote = new HoglakeTableHandle("team_42", "events", 9001, "uuid-events", List.of(column))
                .withRecordCount(OptionalLong.of(1200))
                .withRemote()
                .withCountOnly();
        assertThat(codec.fromJson(codec.toJson(remote))).isEqualTo(remote);
        assertThat(remote.toString()).isEqualTo("team_42.events@9001 remote countOnly");
        // A handle from a build without remote schemas reads as a local table
        String withoutRemote = codec.toJson(local).replaceAll(",\\s*\"remote\"\\s*:\\s*false", "");
        assertThat(withoutRemote).doesNotContain("\"remote\"");
        assertThat(codec.fromJson(withoutRemote).remote()).isFalse();
        assertThat(codec.fromJson(withoutRemote)).isEqualTo(local);
        // A remote table is never written, and is never a posthog schema table
        assertThatThrownBy(() -> remote.withStaged(List.of(), List.of(new HoglakeDtos.DeleteFile(1, 1, "s3://lake/dv", 1, 1, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> remote.withMount(new HoglakeTableHandle.MountTable("lake-events", "ingest")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertUnavailable(Executable call)
    {
        assertThatThrownBy(call::execute)
                .isInstanceOfSatisfying(TrinoException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode()));
    }

    private void assertNoCatalogSnapshotSentToTheRemoteSchema()
    {
        // A snapshot sent under the remote schema is the remote catalog's, never 5
        assertThat(requests)
                .filteredOn(request -> request.path().startsWith(REMOTE_PATH))
                .allSatisfy(request -> request.snapshot().ifPresent(snapshot ->
                        assertThat(Long.parseLong(snapshot)).as(request.toString()).isGreaterThanOrEqualTo(REMOTE_FLOOR)));
    }

    private static HoglakeTableHandle handle(HoglakeMetadata metadata, SchemaTableName name)
    {
        return (HoglakeTableHandle) metadata.getTableHandle(session(), name, Optional.empty(), Optional.empty());
    }

    private static Request get(String target)
    {
        return new Request("GET", target);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        URI uri = exchange.getRequestURI();
        String query = uri.getRawQuery();
        Request request = new Request(exchange.getRequestMethod(), uri.getRawPath() + (query == null ? "" : "?" + query));
        requests.add(request);
        Response response = respond(request);
        byte[] payload = response.body().getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(response.status(), payload.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }

    private Response respond(Request request)
    {
        if (!request.method().equals("GET")) {
            return new Response(500, "{\"error\":\"internal\"}");
        }
        Optional<String> snapshot = request.snapshot();
        return switch (request.path()) {
            case CATALOG_PATH -> ok(
                    """
                    {"name":"lake","data_path":"s3://lake/","head_snapshot_id":5,"schema_version":1,
                     "capabilities":["guarded-schema-evolution-v1","guarded-table-lifecycle-v1","versioned-table-metadata-v1",
                                     "idempotent-append-v1","idempotent-mutation-v1","claimed-uploads-v1","atomic-dml-transactions-v1",
                                     "atomic-table-creation-v1","atomic-table-replacement-v1"]}
                    """);
            case NAMESPACES_PATH -> listing();
            case LOCAL_NAMESPACE_PATH -> ok("{\"name\":\"ns1\",\"namespace_id\":1}");
            case REMOTE_PATH -> remoteNamespace();
            case LOCAL_NAMESPACE_PATH + "/tables" -> ok("[{\"name\":\"t\",\"table_uuid\":\"uuid-t\"}]");
            case REMOTE_PATH + "/tables" -> ok("[{\"name\":\"events\",\"table_uuid\":\"uuid-events\"},{\"name\":\"orders\",\"table_uuid\":\"uuid-orders\"}]");
            case LOCAL_PATH -> localTable(snapshot);
            case LOCAL_PATH + "/scan" -> localScan(snapshot);
            case EVENTS_PATH -> remoteTable("events", eventsHead, snapshot, eventsHeadTotals, eventsAnswerSnapshot);
            case EVENTS_PATH + "/scan" -> remoteScan(eventsHead, snapshot);
            case ORDERS_PATH -> remoteTable("orders", ordersHead, snapshot, "", Optional.empty());
            case ORDERS_PATH + "/scan" -> remoteScan(ordersHead, snapshot);
            default -> new Response(404, "{\"error\":\"not_found\"}");
        };
    }

    private Response listing()
    {
        if (listsRemoteSchema) {
            return ok("[{\"name\":\"ns1\",\"namespace_id\":1},{\"name\":\"team_42\",\"remote\":true}]");
        }
        return ok("[{\"name\":\"ns1\",\"namespace_id\":1}]");
    }

    private Response remoteNamespace()
    {
        if (remoteNamespaceStatus != 200) {
            return new Response(remoteNamespaceStatus, "{\"error\":\"unavailable\"}");
        }
        return ok("{\"name\":\"team_42\",\"remote\":true}");
    }

    /**
     * The local table, at this catalog's head 5 whether or not it is named.
     */
    private Response localTable(Optional<String> snapshot)
    {
        if (snapshot.isPresent() && !snapshot.get().equals("5")) {
            return outOfRange();
        }
        return ok("{\"name\":\"t\",\"namespace\":\"ns1\",\"table_uuid\":\"uuid-t\",\"columns\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"field_id\":1,\"ordinal\":0}],\"read_snapshot_id\":5,\"properties\":{}%s}"
                .formatted(localTableExtra));
    }

    private static Response localScan(Optional<String> snapshot)
    {
        if (!snapshot.equals(Optional.of("5"))) {
            return outOfRange();
        }
        return ok("[]");
    }

    /**
     * A remote table as the relaying server answers it: at the remote catalog's head
     * when no snapshot is named, and exactly at a named snapshot in the remote
     * catalog's range, with that snapshot's exact totals.
     */
    private Response remoteTable(String name, long head, Optional<String> snapshot, String headTotals, Optional<Long> answerSnapshot)
    {
        if (snapshot.isPresent() && !inRemoteRange(snapshot.get(), head)) {
            return outOfRange();
        }
        long read = answerSnapshot.orElse(snapshot.map(Long::parseLong).orElse(head));
        String totals = headTotals;
        if (snapshot.isPresent()) {
            totals = ",\"record_count\":1200,\"file_count\":3,\"file_size_bytes\":4096";
        }
        String readSnapshot = "";
        if (remoteReadSnapshot) {
            readSnapshot = ",\"read_snapshot_id\":" + read;
        }
        return ok("{\"name\":\"%s\",\"namespace\":\"team_42\",\"table_uuid\":\"uuid-%s\",\"columns\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"field_id\":1,\"ordinal\":0}]%s%s,\"properties\":{},\"remote\":true}"
                .formatted(name, name, readSnapshot, totals));
    }

    private static Response remoteScan(long head, Optional<String> snapshot)
    {
        if (snapshot.isEmpty() || !inRemoteRange(snapshot.get(), head)) {
            return outOfRange();
        }
        return ok("[]");
    }

    private static boolean inRemoteRange(String snapshot, long head)
    {
        long value = Long.parseLong(snapshot);
        return value >= REMOTE_FLOOR && value <= head;
    }

    private static Response outOfRange()
    {
        return new Response(422, "{\"error\":\"validation\",\"detail\":\"the requested snapshot is out of range for catalog 'lake'\"}");
    }

    private static Response ok(String body)
    {
        return new Response(200, body);
    }
}
