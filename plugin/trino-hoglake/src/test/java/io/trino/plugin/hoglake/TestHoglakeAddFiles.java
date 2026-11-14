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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * {@code ALTER TABLE ... EXECUTE add_files} against a synthetic REST catalog and real Parquet in a
 * memory filesystem. The fake server records every commit request, so the tests check what the
 * procedure sends, and what it does not.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeAddFiles
{
    private static final String DATA_PATH = "memory:///warehouse/";
    private static final String IN = DATA_PATH + "data/test/_ph_backfill/events/";
    private static final long NOV_9 = LocalDate.of(2026, 11, 9).toEpochDay();
    private static final long NOV_10 = NOV_9 + 1;
    private static final long NOV_11 = NOV_9 + 2;
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    private record Request(String path, JsonNode body) {}

    private final ObjectMapper mapper = new ObjectMapper();
    private final MemoryFileSystemFactory storage = new MemoryFileSystemFactory();
    private final Map<String, HoglakeDtos.Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Long> receipts = new ConcurrentHashMap<>();
    private final List<Request> commits = new CopyOnWriteArrayList<>();
    private final List<String> capabilities = new CopyOnWriteArrayList<>();
    private volatile int commitMode;
    private volatile long endedFiles;
    private long snapshot = 10;
    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner runner;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void setUp()
            throws IOException
    {
        // events: one day(timestamp) partition field. plain: unpartitioned, with a column that is not nullable.
        tables.put("events", new HoglakeDtos.Table(
                "events",
                "test",
                UUID.randomUUID().toString(),
                List.of(
                        new HoglakeDtos.Column(1, 0, "id", "long", null, true),
                        new HoglakeDtos.Column(2, 1, "timestamp", "timestamp", null, true),
                        new HoglakeDtos.Column(3, 2, "name", "string", null, true)),
                0,
                0,
                0,
                Map.of("fields", List.of(Map.of("source_field_id", 2, "transform", "day")))));
        tables.put("plain", new HoglakeDtos.Table(
                "plain",
                "test",
                UUID.randomUUID().toString(),
                List.of(
                        new HoglakeDtos.Column(1, 0, "id", "long", null, false),
                        new HoglakeDtos.Column(2, 1, "name", "string", null, true)),
                0,
                0,
                0));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake");
        fileSystem = storage.create(ConnectorIdentity.ofUser("test"));
        runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("test").build());
        runner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return List.of(new ConnectorFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "hoglake_add_files_test";
                    }

                    @Override
                    public Connector create(String name, Map<String, String> config, ConnectorContext context)
                    {
                        return new HoglakeConnector(
                                new HoglakeMetadata(client, storage),
                                new HoglakeSplitManager(client, _ -> HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE),
                                new HoglakePageSourceProvider(storage),
                                new HoglakePageSinkProvider(storage, "test"),
                                new Bootstrap().quiet().initialize().getInstance(LifeCycleManager.class),
                                Set.of(new HoglakeSessionProperties(new HoglakeConfig())));
                    }
                });
            }
        });
        runner.createCatalog("hoglake", "hoglake_add_files_test", Map.of());
    }

    @BeforeEach
    void reset()
            throws IOException
    {
        commits.clear();
        receipts.clear();
        commitMode = 0;
        endedFiles = 0;
        capabilities.clear();
        capabilities.addAll(List.of("idempotent-append-v1", "atomic-partition-replacement-v1"));
        fileSystem.deleteDirectory(Location.of(DATA_PATH));
        runner.getAccessControl().reset();
    }

    @AfterAll
    void tearDown()
    {
        runner.close();
        client.close();
        server.stop(0);
    }

    private synchronized void handle(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            String tables = "/v1/catalogs/lake/namespaces/test/tables";
            if (path.equals("/v1/catalogs/lake")) {
                respond(exchange, 200, new HoglakeDtos.Catalog("lake", DATA_PATH, snapshot, 1, List.copyOf(capabilities)));
            }
            else if (path.equals("/v1/catalogs/lake/namespaces")) {
                respond(exchange, 200, List.of(new HoglakeDtos.Namespace("test", 1L)));
            }
            else if (path.startsWith(tables + "/") && this.tables.containsKey(path.substring(tables.length() + 1))) {
                respond(exchange, 200, this.tables.get(path.substring(tables.length() + 1)));
            }
            else if (path.startsWith("/v1/catalogs/lake/commit/receipts/")) {
                String key = path.substring(path.lastIndexOf('/') + 1);
                Long receipt = receipts.get(key);
                respond(exchange, receipt == null ? 404 : 200, receipt == null ? Map.of() : Map.of("operation_id", key, "snapshot_id", receipt));
            }
            else if (path.equals("/v1/catalogs/lake/commit/prepared") || path.equals("/v1/catalogs/lake/commit/replacements/prepared")) {
                JsonNode body = mapper.readTree(exchange.getRequestBody());
                commits.add(new Request(path, body));
                if (commitMode == 409) {
                    respond(exchange, 409, Map.of("error", "transaction_conflict"));
                    return;
                }
                snapshot++;
                receipts.put(body.path("idempotency_key").asText(), snapshot);
                // The commit applied, but the answer is lost. Mode 200 without a count is a replay answered from the receipt.
                boolean count = path.endsWith("/replacements/prepared") && commitMode != 201;
                respond(exchange, commitMode == 503 ? 503 : 200, count ? Map.of("snapshot_id", snapshot, "ended_files", endedFiles) : Map.of("snapshot_id", snapshot));
            }
            else {
                respond(exchange, 404, Map.of());
            }
        }
    }

    private void respond(HttpExchange exchange, int status, Object body)
            throws IOException
    {
        byte[] bytes = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    // ---- listing, footers and partition values -----------------------------

    @Test
    void testListsRecursivelyWithPrefixAndRegistersFooterCounts()
            throws IOException
    {
        byte[] first = eventsFile(NOV_9, 3);
        write(IN + "day=2026-11-09/run1_a.parquet", first);
        write(IN + "day=2026-11-09/run1_b.parquet", eventsFile(NOV_9, 1));
        write(IN + "day=2026-11-09/sub/run1_c.parquet", eventsFile(NOV_9, 2));
        write(IN + "day=2026-11-09/other_d.parquet", eventsFile(NOV_9, 5));
        write(IN + "day=2026-11-09/run1_notes.txt", "not parquet".getBytes(UTF_8));

        MaterializedResult result = addFiles("events", "location => '" + IN + "day=2026-11-09/', file_prefix => 'run1_', import_id => 'run1'");

        assertThat(metrics(result)).containsEntry("added_data_files", 3L).containsEntry("ended_data_files", 0L);
        Request commit = onlyCommit("/v1/catalogs/lake/commit/prepared");
        JsonNode append = commit.body().path("appends").get(0);
        assertThat(append.path("expected_table_uuid").asText()).isEqualTo(tables.get("events").tableUuid());
        assertThat(commit.body().path("read_snapshot").asLong()).isEqualTo(snapshot - 1);
        assertThat(commit.body().path("idempotency_key").asText())
                .isEqualTo(UUID.nameUUIDFromBytes(("hoglake-add-files:" + tables.get("events").tableUuid() + ":run1").getBytes(UTF_8)).toString());
        assertThat(commit.body().has("file_ends")).isFalse();
        List<JsonNode> files = new ArrayList<>();
        append.path("files").forEach(files::add);
        assertThat(files).extracting(file -> file.path("path").asText())
                .containsExactly(IN + "day=2026-11-09/run1_a.parquet", IN + "day=2026-11-09/run1_b.parquet", IN + "day=2026-11-09/sub/run1_c.parquet");
        assertThat(files).extracting(file -> file.path("record_count").asLong()).containsExactly(3L, 1L, 2L);
        assertThat(files.getFirst().path("file_size_bytes").asLong()).isEqualTo(first.length);
        assertThat(files.getFirst().path("footer_size").asLong()).isEqualTo(ConnectorTestFixtures.footerSize(first));
        assertThat(files).allSatisfy(file -> assertThat(file.path("partition_values")).hasSize(1).first().satisfies(value -> assertThat(value.asText()).isEqualTo(Long.toString(NOV_9))));
    }

    @Test
    void testPartitionValuesAreDerivedPerFile()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 2));
        write(IN + "b.parquet", eventsFile(NOV_11, 2));

        addFiles("events", "location => '" + IN + "', import_id => 'days'");

        List<String> values = new ArrayList<>();
        onlyCommit("/v1/catalogs/lake/commit/prepared").body().path("appends").get(0).path("files").forEach(file -> values.add(file.path("partition_values").get(0).asText()));
        assertThat(values).containsExactly(Long.toString(NOV_9), Long.toString(NOV_11));
    }

    @Test
    void testFileSpanningTwoDaysIsRefused()
            throws IOException
    {
        write(IN + "spans.parquet", eventsFile(List.of(NOV_9 * MICROS_PER_DAY + 5, NOV_10 * MICROS_PER_DAY + 5)));

        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'spans'"))
                .hasMessageContaining("spans more than one day");
        assertThat(commits).isEmpty();
    }

    @Test
    void testFileOutsideTheRangeIsRefused()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 1));
        write(IN + "b.parquet", eventsFile(NOV_10, 1));

        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'range', " + range(NOV_9, NOV_10)))
                .hasMessageContaining("outside the replaced range");
        assertThat(commits).isEmpty();
    }

    @Test
    void testRangeMustBeUtcDayAligned()
    {
        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'unaligned', replace_range_column => 'timestamp', replace_range_start => '2026-11-09 12:00:00.000000 UTC', replace_range_end => '2026-11-10 00:00:00.000000 UTC'"))
                .hasMessageContaining("UTC day boundary");
        assertThat(commits).isEmpty();
    }

    @Test
    void testRangeOnAnUnpartitionedTableIsRefused()
    {
        assertThatThrownBy(() -> addFiles("plain", "location => '" + IN + "', import_id => 'plain', replace_range_column => 'id', replace_range_start => '2026-11-09', replace_range_end => '2026-11-10'"))
                .hasMessageContaining("partitioned by day");
    }

    // ---- columns and types -------------------------------------------------

    @Test
    void testTypeMismatchIsRefused()
            throws IOException
    {
        // id is a bigint in the table: a string column must not be registered as one
        write(IN + "wrong.parquet", ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(1).named("id"), VARCHAR, List.of("a")),
                new FileColumn(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(2).named("name"), VARCHAR, List.of("a")))));

        assertThatThrownBy(() -> addFiles("plain", "location => '" + IN + "', import_id => 'types'"))
                .hasMessageContaining("cannot be read as column id");
        assertThat(commits).isEmpty();
    }

    @Test
    void testMissingColumnNeedsAllowMissingAndNullability()
            throws IOException
    {
        write(IN + "no_name.parquet", plainFile(true, false));
        write(DATA_PATH + "other/no_id.parquet", plainFile(false, true));

        assertThatThrownBy(() -> addFiles("plain", "location => '" + IN + "', import_id => 'missing'"))
                .hasMessageContaining("has no column name");
        assertThat(commits).isEmpty();

        addFiles("plain", "location => '" + IN + "', import_id => 'allowed', allow_missing => true");
        assertThat(commits).hasSize(1);

        // id is not nullable, so a file that lacks it cannot be read even when missing columns are allowed
        assertThatThrownBy(() -> addFiles("plain", "location => '" + DATA_PATH + "other/', import_id => 'required', allow_missing => true"))
                .hasMessageContaining("has no column id, which is not nullable");
        assertThat(commits).hasSize(1);
    }

    @Test
    void testFileColumnAbsentFromTheTableIsRefused()
            throws IOException
    {
        write(IN + "extra.parquet", ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(Types.required(PrimitiveTypeName.INT64).id(1).named("id"), BIGINT, List.of(1L)),
                new FileColumn(Types.optional(PrimitiveTypeName.INT64).id(9).named("surprise"), BIGINT, Arrays.asList(1L)))));

        assertThatThrownBy(() -> addFiles("plain", "location => '" + IN + "', import_id => 'extra', allow_missing => true"))
                .hasMessageContaining("has column surprise, which is not a column of the table");
    }

    // ---- replacement -------------------------------------------------------

    @Test
    void testReplaceEndsEveryFile()
            throws IOException
    {
        endedFiles = 4;
        write(IN + "full/a.parquet", eventsFile(NOV_9, 2));

        MaterializedResult result = addFiles("events", "location => '" + IN + "full/', import_id => 'full', replace => true");

        assertThat(metrics(result)).containsEntry("added_data_files", 1L).containsEntry("ended_data_files", 4L);
        JsonNode body = onlyCommit("/v1/catalogs/lake/commit/replacements/prepared").body();
        assertThat(body.path("file_ends")).hasSize(1);
        JsonNode end = body.path("file_ends").get(0);
        assertThat(end.path("all_files").asBoolean()).isTrue();
        assertThat(end.has("partitions")).isFalse();
        assertThat(end.path("expected_table_uuid").asText()).isEqualTo(tables.get("events").tableUuid());
        assertThat(end.path("namespace").asText()).isEqualTo("test");
        assertThat(end.path("table").asText()).isEqualTo("events");
        assertThat(body.path("appends").get(0).path("files")).hasSize(1);
        assertThat(body.path("deletes")).isEmpty();
        assertThat(body.path("author").asText()).isEqualTo("user");
        assertThat(body.path("message").asText()).startsWith("Trino add_files, query ");
        assertThat(body.path("read_snapshot").asLong()).isEqualTo(snapshot - 1);
    }

    @Test
    void testRangeReplacementEndsTheDaysOfTheRange()
            throws IOException
    {
        endedFiles = 3;
        write(IN + "a.parquet", eventsFile(NOV_9, 2));
        write(IN + "b.parquet", eventsFile(NOV_10, 2));

        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'two_days', " + range(NOV_9, NOV_11));

        assertThat(metrics(result)).containsEntry("added_data_files", 2L).containsEntry("ended_data_files", 3L);
        JsonNode end = onlyCommit("/v1/catalogs/lake/commit/replacements/prepared").body().path("file_ends").get(0);
        assertThat(end.path("all_files").asBoolean()).isFalse();
        assertThat(end.path("partitions")).hasSize(2);
        assertThat(end.path("partitions").get(0)).hasSize(1);
        assertThat(end.path("partitions").get(0).get(0).asText()).isEqualTo(Long.toString(NOV_9));
        assertThat(end.path("partitions").get(1).get(0).asText()).isEqualTo(Long.toString(NOV_10));
    }

    @Test
    void testEmptyLocationWithAReplacementOnlyEndsTheDay()
    {
        endedFiles = 2;

        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'empty_day', " + range(NOV_9, NOV_10));

        assertThat(metrics(result)).containsEntry("added_data_files", 0L).containsEntry("ended_data_files", 2L);
        JsonNode body = onlyCommit("/v1/catalogs/lake/commit/replacements/prepared").body();
        assertThat(body.path("appends")).isEmpty();
        assertThat(body.path("file_ends").get(0).path("partitions").get(0).get(0).asText()).isEqualTo(Long.toString(NOV_9));
    }

    @Test
    void testEmptyLocationWithoutAReplacementCommitsNothing()
    {
        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'nothing'");

        assertThat(metrics(result)).containsEntry("added_data_files", 0L).containsEntry("ended_data_files", 0L);
        assertThat(commits).isEmpty();
    }

    @Test
    void testReplacementNeedsTheServerCapability()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 1));
        capabilities.remove("atomic-partition-replacement-v1");

        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'old_server', replace => true"))
                .hasMessageContaining("atomic-partition-replacement-v1");
        assertThat(commits).isEmpty();
        // An append does not need it
        addFiles("events", "location => '" + IN + "', import_id => 'old_server_append'");
        assertThat(commits).hasSize(1);
    }

    // ---- retries and failures ----------------------------------------------

    @Test
    void testAppliedImportIsNotCommittedAgain()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 1));
        addFiles("events", "location => '" + IN + "', import_id => 'again', replace => true");
        assertThat(commits).hasSize(1);

        // The files are gone, and the table could have changed: the receipt alone decides
        fileSystem.deleteDirectory(Location.of(IN));
        MaterializedResult repeat = addFiles("events", "location => '" + IN + "', import_id => 'again', replace => true");

        assertThat(metrics(repeat)).containsEntry("added_data_files", 0L).containsEntry("ended_data_files", 0L);
        assertThat(commits).hasSize(1);
    }

    @Test
    void testLostReplacementAnswerIsRecoveredFromTheReceipt()
            throws IOException
    {
        commitMode = 503;
        write(IN + "a.parquet", eventsFile(NOV_9, 1));

        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'lost', delete_on_failure => true, replace => true");

        // The commit applied once, the files stay, and the count of ended files is unknown
        assertThat(commits).hasSize(1);
        assertThat(metrics(result)).containsEntry("added_data_files", 1L).containsEntry("ended_data_files", -1L);
        assertThat(fileSystem.newInputFile(Location.of(IN + "a.parquet")).exists()).isTrue();
    }

    @Test
    void testDeleteOnFailureDeletesRefusedFilesButNeverAfterASentCommit()
            throws IOException
    {
        String refused = IN + "refused/";
        write(refused + "run_a.parquet", eventsFile(NOV_9, 1));
        write(refused + "run_b.parquet", eventsFile(List.of(NOV_9 * MICROS_PER_DAY, NOV_10 * MICROS_PER_DAY)));
        write(refused + "other.parquet", eventsFile(NOV_9, 1));

        // Without the flag nothing is deleted
        assertThatThrownBy(() -> addFiles("events", "location => '" + refused + "', file_prefix => 'run_', import_id => 'keep'"))
                .hasMessageContaining("spans more than one day");
        assertThat(fileSystem.newInputFile(Location.of(refused + "run_a.parquet")).exists()).isTrue();

        assertThatThrownBy(() -> addFiles("events", "location => '" + refused + "', file_prefix => 'run_', import_id => 'delete', delete_on_failure => true"))
                .hasMessageContaining("spans more than one day");
        assertThat(fileSystem.newInputFile(Location.of(refused + "run_a.parquet")).exists()).isFalse();
        assertThat(fileSystem.newInputFile(Location.of(refused + "run_b.parquet")).exists()).isFalse();
        // Files that did not match the prefix are not the procedure's
        assertThat(fileSystem.newInputFile(Location.of(refused + "other.parquet")).exists()).isTrue();
        assertThat(commits).isEmpty();

        // A conflict is the server's answer to a sent commit: the files are kept for the retry
        String conflicted = IN + "conflicted/";
        write(conflicted + "a.parquet", eventsFile(NOV_9, 1));
        commitMode = 409;
        assertThatThrownBy(() -> addFiles("events", "location => '" + conflicted + "', import_id => 'conflict', delete_on_failure => true, " + range(NOV_9, NOV_10)))
                .hasMessageContaining("conflict");
        assertThat(commits).hasSize(1);
        assertThat(fileSystem.newInputFile(Location.of(conflicted + "a.parquet")).exists()).isTrue();
    }

    @Test
    void testReplacementAnsweredWithoutACountIsASuccessWithAnUnknownCount()
            throws IOException
    {
        commitMode = 201;
        write(IN + "a.parquet", eventsFile(NOV_9, 1));

        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'replayed', " + range(NOV_9, NOV_10));

        assertThat(metrics(result)).containsEntry("added_data_files", 1L).containsEntry("ended_data_files", -1L);
    }

    @Test
    void testIdempotencyKeyBelongsToTheTableIncarnation()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 1));
        addFiles("events", "location => '" + IN + "', import_id => 'incarnation'");
        HoglakeDtos.Table table = tables.get("events");
        try {
            // CREATE OR REPLACE TABLE gives the name a new UUID: the old receipt must not apply
            tables.put("events", new HoglakeDtos.Table(table.name(), table.namespace(), UUID.randomUUID().toString(), table.columns(), 0, 0, 0, table.partitionSpec()));
            addFiles("events", "location => '" + IN + "', import_id => 'incarnation'");
            assertThat(commits).hasSize(2);
        }
        finally {
            tables.put("events", table);
        }
    }

    @Test
    void testExpectedRowsMustMatchTheFiles()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 3));
        write(IN + "b.parquet", eventsFile(NOV_9, 2));
        // A file with no rows counts as zero
        write(IN + "empty.parquet", eventsFile(List.of()));

        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'rows', expected_rows => 4, delete_on_failure => true, " + range(NOV_9, NOV_10)))
                .hasMessageContaining("hold 5 rows, but expected_rows is 4");
        assertThat(commits).isEmpty();
        // A mismatch is a refusal of the files, so delete_on_failure applies
        assertThat(fileSystem.newInputFile(Location.of(IN + "a.parquet")).exists()).isFalse();

        write(IN + "a.parquet", eventsFile(NOV_9, 3));
        write(IN + "b.parquet", eventsFile(NOV_9, 2));
        addFiles("events", "location => '" + IN + "', import_id => 'rows', expected_rows => 5, " + range(NOV_9, NOV_10));
        assertThat(commits).hasSize(1);
    }

    @Test
    void testZeroExpectedRowsWithNoFilesEndsTheDay()
    {
        endedFiles = 1;

        // The first attempt may already have deleted the files: a nonzero expectation must fail loudly instead
        assertThatThrownBy(() -> addFiles("events", "location => '" + IN + "', import_id => 'gone', expected_rows => 100, " + range(NOV_9, NOV_10)))
                .hasMessageContaining("hold 0 rows, but expected_rows is 100");
        assertThat(commits).isEmpty();

        MaterializedResult result = addFiles("events", "location => '" + IN + "', import_id => 'quiet_day', expected_rows => 0, " + range(NOV_9, NOV_10));
        assertThat(metrics(result)).containsEntry("added_data_files", 0L).containsEntry("ended_data_files", 1L);
        assertThat(commits).hasSize(1);
    }

    /**
     * Statistics-based partition derivation on a file a real ClickHouse wrote. Generated with
     * <pre>
     * clickhouse local --query "SELECT toInt64(number) AS id, toDateTime64('2026-11-09 00:00:00', 6, 'UTC') + toIntervalSecond(number * 600) AS timestamp, concat('event ', toString(number)) AS name FROM numbers(100) INTO OUTFILE 'src/test/resources/clickhouse_events_2026_11_09.parquet' FORMAT Parquet SETTINGS output_format_parquet_string_as_string=1"
     * </pre>
     */
    @Test
    void testRegistersAFileWrittenByClickHouse()
            throws IOException
    {
        try (var resource = TestHoglakeAddFiles.class.getResourceAsStream("/clickhouse_events_2026_11_09.parquet")) {
            write(IN + "run_ch_1.parquet", resource.readAllBytes());
        }

        MaterializedResult result = addFiles("events", "location => '" + IN + "', file_prefix => 'run_', import_id => 'clickhouse', expected_rows => 100, allow_missing => true, " + range(NOV_9, NOV_10));

        assertThat(metrics(result)).containsEntry("added_data_files", 1L);
        JsonNode body = onlyCommit("/v1/catalogs/lake/commit/replacements/prepared").body();
        JsonNode file = body.path("appends").get(0).path("files").get(0);
        assertThat(file.path("record_count").asLong()).isEqualTo(100);
        assertThat(file.path("partition_values").get(0).asText()).isEqualTo(Long.toString(NOV_9));
    }

    @Test
    void testLocationOutsideTheDataPathIsRefused()
    {
        assertThatThrownBy(() -> addFiles("events", "location => 'memory:///elsewhere/events/', import_id => 'outside'"))
                .hasMessageContaining("outside the catalog data path");
        assertThatThrownBy(() -> addFiles("events", "location => '" + DATA_PATH + "../elsewhere/', import_id => 'dots'"))
                .hasMessageContaining("outside the catalog data path");
        assertThat(commits).isEmpty();
    }

    // ---- access control ----------------------------------------------------

    @Test
    void testPermissions()
            throws IOException
    {
        write(IN + "a.parquet", eventsFile(NOV_9, 1));
        String append = "location => '" + IN + "', import_id => 'permissions'";
        String replacement = append + ", replace => true";
        String ranged = append + ", " + range(NOV_9, NOV_10);

        runner.getAccessControl().deny(privilege("user", "events", INSERT_TABLE));
        assertThatThrownBy(() -> addFiles("events", append)).hasMessageContaining("Cannot insert into table");
        runner.getAccessControl().reset();

        // Replacing data deletes it, so it needs the permission to delete as well
        runner.getAccessControl().deny(privilege("user", "events", DELETE_TABLE));
        assertThatThrownBy(() -> addFiles("events", replacement)).hasMessageContaining("Cannot delete from table");
        assertThatThrownBy(() -> addFiles("events", ranged)).hasMessageContaining("Cannot delete from table");
        assertThat(commits).isEmpty();
        // An append deletes nothing
        addFiles("events", append);
        assertThat(commits).hasSize(1);
    }

    // ---- helpers -----------------------------------------------------------

    private MaterializedResult addFiles(String table, String properties)
    {
        return runner.execute("ALTER TABLE hoglake.test." + table + " EXECUTE add_files(" + properties + ")");
    }

    private static Map<String, Long> metrics(MaterializedResult result)
    {
        Map<String, Long> metrics = new HashMap<>();
        result.getMaterializedRows().forEach(row -> metrics.put((String) row.getField(0), (Long) row.getField(1)));
        return metrics;
    }

    private Request onlyCommit(String path)
    {
        assertThat(commits).hasSize(1);
        assertThat(commits.getFirst().path()).isEqualTo(path);
        return commits.getFirst();
    }

    private static String range(long startDay, long endDay)
    {
        return "replace_range_column => 'timestamp', replace_range_start => '%s 00:00:00.000000 UTC', replace_range_end => '%s 00:00:00.000000 UTC'"
                .formatted(LocalDate.ofEpochDay(startDay), LocalDate.ofEpochDay(endDay));
    }

    private void write(String location, byte[] bytes)
            throws IOException
    {
        fileSystem.newOutputFile(Location.of(location)).createOrOverwrite(bytes);
    }

    /**
     * An events file whose rows all fall in the day.
     */
    private static byte[] eventsFile(long day, int rows)
    {
        List<Long> timestamps = new ArrayList<>();
        for (int row = 0; row < rows; row++) {
            timestamps.add(day * MICROS_PER_DAY + row * 1_000_000L);
        }
        return eventsFile(timestamps);
    }

    private static byte[] eventsFile(List<Long> timestampMicros)
    {
        List<Object> ids = new ArrayList<>();
        List<Object> names = new ArrayList<>();
        for (int row = 0; row < timestampMicros.size(); row++) {
            ids.add((long) row);
            names.add("event " + row);
        }
        return ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(Types.optional(PrimitiveTypeName.INT64).id(1).named("id"), BIGINT, ids),
                new FileColumn(Types.optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).id(2).named("timestamp"), TIMESTAMP_MICROS, new ArrayList<Object>(timestampMicros)),
                new FileColumn(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(3).named("name"), VARCHAR, names)));
    }

    private static byte[] plainFile(boolean withId, boolean withName)
    {
        List<FileColumn> columns = new ArrayList<>();
        if (withId) {
            columns.add(new FileColumn(Types.required(PrimitiveTypeName.INT64).id(1).named("id"), BIGINT, List.of(1L, 2L)));
        }
        if (withName) {
            columns.add(new FileColumn(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(2).named("name"), VARCHAR, List.of("a", "b")));
        }
        return ConnectorTestFixtures.writeParquet(columns);
    }
}
