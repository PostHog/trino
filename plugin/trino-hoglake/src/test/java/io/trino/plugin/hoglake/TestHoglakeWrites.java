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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.Page;
import io.trino.spi.Plugin;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * SQL execution against a synthetic REST catalog and real Parquet in a memory filesystem.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeWrites
{
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, HoglakeDtos.Table> tables = new ConcurrentHashMap<>();
    private final Map<String, HoglakeDtos.TableCreation> creations = new ConcurrentHashMap<>();
    private final Map<String, List<HoglakeDtos.ScanFile>> files = new ConcurrentHashMap<>();
    private final TrinoFileSystemFactory storage = new MemoryFileSystemFactory();
    private long snapshot = 1;
    private long nextFileId = 1;
    private volatile int commitStatus = 200;
    private volatile int commits;
    private volatile boolean atomicCreation = true;
    private volatile boolean idempotentAppend;
    private volatile boolean idempotentDelete;
    private final Map<String, Long> deleteReceipts = new ConcurrentHashMap<>();
    private volatile boolean corruptCommitResponse;
    private volatile boolean lifecycleSupport = true;
    private volatile boolean schemaCreationRace;
    private int namespaceCreates;
    private volatile boolean replacementSupport = true;
    private final Map<String, HoglakeDtos.ReplacementTarget> replacementTargets = new ConcurrentHashMap<>();
    private volatile boolean corruptLifecycleResponse;
    private int lifecycleRequests;
    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner runner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake");
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
                        return "hoglake_write_test";
                    }

                    @Override
                    public Connector create(String name, Map<String, String> config, ConnectorContext context)
                    {
                        return new HoglakeConnector(
                                new HoglakeMetadata(client, storage),
                                new HoglakeSplitManager(client),
                                new HoglakePageSourceProvider(storage),
                                new HoglakePageSinkProvider(storage, "test"),
                                new Bootstrap().quiet().initialize().getInstance(LifeCycleManager.class));
                    }
                });
            }
        });
        runner.createCatalog("hoglake", "hoglake_write_test", Map.of());
    }

    private synchronized void handle(HttpExchange exchange)
            throws IOException
    {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            String prefix = "/v1/catalogs/lake/namespaces/test/tables";
            if (path.contains("/table-creations/")) {
                handleCreation(exchange, path.substring(path.indexOf("/table-creations/") + "/table-creations/".length()));
            }
            else if (path.equals("/v1/catalogs/lake")) {
                List<String> capabilities = new ArrayList<>();
                if (atomicCreation) {
                    capabilities.add("atomic-table-creation-v1");
                }
                if (replacementSupport) {
                    capabilities.add("atomic-table-replacement-v1");
                }
                if (lifecycleSupport) {
                    capabilities.add("guarded-table-lifecycle-v1");
                }
                if (schemaCreationRace) {
                    capabilities.add("guarded-schema-evolution-v1");
                }
                if (idempotentDelete) {
                    capabilities.add("idempotent-delete-v1");
                }
                if (idempotentAppend) {
                    capabilities.add("idempotent-append-v1");
                }
                respond(exchange, 200, new HoglakeDtos.Catalog("lake", "memory:///warehouse/", snapshot, 1, capabilities));
            }
            else if (path.equals("/v1/catalogs/lake/namespaces")) {
                if (exchange.getRequestMethod().equals("POST")) {
                    namespaceCreates++;
                    respond(exchange, 409, Map.of("error", "already_exists", "detail", "namespace already exists"));
                }
                else {
                    respond(exchange, 200, List.of(new HoglakeDtos.Namespace("test", 1L)));
                }
            }
            else if (path.equals(prefix) && exchange.getRequestMethod().equals("POST")) {
                HoglakeDtos.CreateTable request = mapper.readValue(exchange.getRequestBody(), HoglakeDtos.CreateTable.class);
                if (tables.containsKey(request.name())) {
                    respond(exchange, 409, Map.of("error", "already_exists"));
                    return;
                }
                List<HoglakeDtos.Column> columns = new ArrayList<>();
                for (HoglakeDtos.ColumnDefinition column : request.columns()) {
                    columns.add(new HoglakeDtos.Column(columns.size() + 1, columns.size(), column.name(), column.type(), column.typeParams(), column.nullable()));
                }
                HoglakeDtos.Table table = new HoglakeDtos.Table(request.name(), "test", UUID.randomUUID().toString(), columns, 0, 0, 0);
                tables.put(request.name(), table);
                files.put(request.name(), new ArrayList<>());
                snapshot++;
                respond(exchange, 201, table);
            }
            else if (path.equals(prefix)) {
                respond(exchange, 200, tables.values().stream().map(table -> new HoglakeDtos.TableSummary(table.name(), table.tableUuid())).toList());
            }
            else if (path.startsWith(prefix + "/")) {
                String name = path.substring(prefix.length() + 1).split("/")[0];
                if (!tables.containsKey(name)) {
                    respond(exchange, 404, Map.of());
                    return;
                }
                if (!exchange.getRequestMethod().equals("GET")) {
                    lifecycleRequests++;
                    if (!("expected_table_uuid=" + tables.get(name).tableUuid()).equals(exchange.getRequestURI().getQuery())) {
                        respond(exchange, 409, Map.of("error", "incarnation_changed"));
                        return;
                    }
                }
                if (path.endsWith("/truncate")) {
                    HoglakeDtos.Table table = tables.get(name);
                    tables.put(name, new HoglakeDtos.Table(name, table.namespace(), table.tableUuid(), table.columns(), 0, 0, 0));
                    files.put(name, new ArrayList<>());
                    snapshot++;
                    respond(exchange, corruptLifecycleResponse ? 503 : 200, Map.of("snapshot_id", snapshot));
                }
                else if (path.endsWith("/alter")) {
                    String newName = mapper.readTree(exchange.getRequestBody()).path("ops").get(0).path("new_name").asText();
                    if (tables.containsKey(newName)) {
                        respond(exchange, 409, Map.of("error", "already_exists"));
                        return;
                    }
                    HoglakeDtos.Table table = tables.remove(name);
                    HoglakeDtos.Table renamed = new HoglakeDtos.Table(newName, table.namespace(), table.tableUuid(), table.columns(), table.recordCount(), table.fileCount(), table.fileSizeBytes());
                    tables.put(newName, renamed);
                    files.put(newName, files.remove(name));
                    snapshot++;
                    respond(exchange, 200, renamed);
                }
                else if (exchange.getRequestMethod().equals("DELETE")) {
                    tables.remove(name);
                    files.remove(name);
                    snapshot++;
                    respond(exchange, 200, Map.of("snapshot_id", snapshot));
                }
                else if (path.endsWith("/scan")) {
                    respond(exchange, 200, files.get(name));
                }
                else {
                    respond(exchange, 200, tables.get(name));
                }
            }
            else if (path.contains("/commit/receipts/")) {
                String operation = path.substring(path.lastIndexOf('/') + 1);
                Long receipt = deleteReceipts.get(operation);
                respond(exchange, receipt == null ? 404 : 200, receipt == null ? Map.of() : Map.of("operation_id", operation, "snapshot_id", receipt));
            }
            else if (path.equals("/v1/catalogs/lake/commit/deletes/prepared")) {
                HoglakeDtos.Commit request = mapper.readValue(exchange.getRequestBody(), HoglakeDtos.Commit.class);
                if (commitStatus != 200) {
                    respond(exchange, commitStatus, Map.of("error", "synthetic_failure"));
                    return;
                }
                commits++;
                for (HoglakeDtos.Deletes deletes : request.deletes()) {
                    List<HoglakeDtos.ScanFile> current = files.get(deletes.table());
                    for (HoglakeDtos.DeleteRegistration registration : deletes.files()) {
                        for (int i = 0; i < current.size(); i++) {
                            HoglakeDtos.ScanFile file = current.get(i);
                            if (file.dataFile().dataFileId() == registration.dataFileId()) {
                                current.set(i, new HoglakeDtos.ScanFile(file.dataFile(), new HoglakeDtos.DeleteFile(
                                        nextFileId++,
                                        registration.dataFileId(),
                                        registration.path(),
                                        registration.deleteCount(),
                                        registration.fileSizeBytes(),
                                        snapshot + 1)));
                            }
                        }
                    }
                }
                snapshot++;
                deleteReceipts.put(request.operationId(), snapshot);
                respond(exchange, corruptCommitResponse ? 503 : 200, Map.of("snapshot_id", snapshot));
            }
            else if (path.equals("/v1/catalogs/lake/commit")) {
                commits++;
                HoglakeDtos.Commit request = mapper.readValue(exchange.getRequestBody(), HoglakeDtos.Commit.class);
                if (commitStatus != 200) {
                    respond(exchange, commitStatus, Map.of("error", "synthetic_failure"));
                    return;
                }
                for (HoglakeDtos.Append append : request.appends()) {
                    HoglakeDtos.Table table = tables.get(append.table());
                    if (!table.tableUuid().equals(append.expectedTableUuid())) {
                        respond(exchange, 409, Map.of("error", "incarnation_changed"));
                        return;
                    }
                    long count = table.recordCount();
                    long bytes = table.fileSizeBytes();
                    for (HoglakeDtos.FileRegistration file : append.files()) {
                        assertThat(file.recordCount()).isPositive();
                        assertThat(file.footerSize()).isPositive();
                        files.get(append.table()).add(new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(nextFileId++, file.path(), "parquet", file.recordCount(), file.fileSizeBytes(), file.footerSize(), count, "pending", snapshot + 1), null));
                        count += file.recordCount();
                        bytes += file.fileSizeBytes();
                    }
                    tables.put(append.table(), new HoglakeDtos.Table(table.name(), table.namespace(), table.tableUuid(), table.columns(), count, files.get(append.table()).size(), bytes));
                }
                snapshot++;
                respond(exchange, 200, corruptCommitResponse ? Map.of() : Map.of("snapshot_id", snapshot, "schema_version", 1));
            }
            else {
                respond(exchange, 404, Map.of());
            }
        }
    }

    private void handleCreation(HttpExchange exchange, String suffix)
            throws IOException
    {
        String operation = suffix.split("/")[0];
        HoglakeDtos.TableCreation creation = creations.get(operation);
        if (exchange.getRequestMethod().equals("PUT")) {
            var request = mapper.readTree(exchange.getRequestBody());
            if (request.has("replacement")) {
                replacementTargets.put(operation, mapper.treeToValue(request.get("replacement"), HoglakeDtos.ReplacementTarget.class));
            }
            List<HoglakeDtos.Column> columns = new ArrayList<>();
            for (var column : request.path("columns")) {
                HoglakeDtos.ColumnDefinition definition = mapper.treeToValue(column, HoglakeDtos.ColumnDefinition.class);
                columns.add(new HoglakeDtos.Column(columns.size() + 1, columns.size(), definition.name(), definition.type(), definition.typeParams(), definition.nullable()));
            }
            creation = new HoglakeDtos.TableCreation(operation, UUID.randomUUID().toString(), "test", request.path("name").asText(), columns, "memory:///warehouse/" + operation + "/", "prepared", null, null);
        }
        else if (creation == null) {
            respond(exchange, 404, Map.of());
            return;
        }
        else if (suffix.endsWith("/abort") && creation.state().equals("prepared")) {
            creation = new HoglakeDtos.TableCreation(operation, creation.tableUuid(), "test", creation.name(), creation.columns(), creation.writePath(), "aborted", null, "client_abort");
        }
        else if (suffix.endsWith("/commit") && creation.state().equals("prepared")) {
            commits++;
            if (commitStatus != 200) {
                respond(exchange, commitStatus, Map.of("error", "synthetic_failure"));
                return;
            }
            HoglakeDtos.ReplacementTarget replacement = replacementTargets.get(operation);
            String currentUuid = Optional.ofNullable(tables.get(creation.name())).map(HoglakeDtos.Table::tableUuid).orElse(null);
            if (replacement != null && !Objects.equals(replacement.expectedTableUuid(), currentUuid)) {
                creation = new HoglakeDtos.TableCreation(operation, creation.tableUuid(), "test", creation.name(), creation.columns(), creation.writePath(), "rejected", null, "target_changed");
            }
            else if (replacement == null && tables.containsKey(creation.name())) {
                creation = new HoglakeDtos.TableCreation(operation, creation.tableUuid(), "test", creation.name(), creation.columns(), creation.writePath(), "rejected", null, "target_exists");
            }
            else {
                var request = mapper.readTree(exchange.getRequestBody());
                List<HoglakeDtos.ScanFile> registered = new ArrayList<>();
                long count = 0;
                long bytes = 0;
                snapshot++;
                for (var node : request.path("files")) {
                    HoglakeDtos.FileRegistration file = mapper.treeToValue(node, HoglakeDtos.FileRegistration.class);
                    registered.add(new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(nextFileId++, file.path(), "parquet", file.recordCount(), file.fileSizeBytes(), file.footerSize(), count, "pending", snapshot), null));
                    count += file.recordCount();
                    bytes += file.fileSizeBytes();
                }
                tables.put(creation.name(), new HoglakeDtos.Table(creation.name(), "test", creation.tableUuid(), creation.columns(), count, registered.size(), bytes));
                files.put(creation.name(), registered);
                creation = new HoglakeDtos.TableCreation(operation, creation.tableUuid(), "test", creation.name(), creation.columns(), creation.writePath(), "committed", snapshot, null);
            }
        }
        creations.put(operation, creation);
        if (corruptCommitResponse && suffix.endsWith("/commit")) {
            byte[] invalid = new byte[] {'!'};
            exchange.sendResponseHeaders(200, invalid.length);
            exchange.getResponseBody().write(invalid);
            return;
        }
        respond(exchange, 200, creation);
    }

    private void respond(HttpExchange exchange, int status, Object body)
            throws IOException
    {
        byte[] bytes = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @AfterAll
    void tearDown()
    {
        if (runner != null) {
            runner.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testConcurrentSchemaCreation()
    {
        schemaCreationRace = true;
        int before = namespaceCreates;
        try {
            // Listing reports absence, but another creator wins before POST.
            runner.execute("CREATE SCHEMA IF NOT EXISTS raced_schema");
            assertThatThrownBy(() -> runner.execute("CREATE SCHEMA raced_schema"))
                    .hasMessageContaining("already exists");
            assertThat(namespaceCreates).isEqualTo(before + 2);
        }
        finally {
            schemaCreationRace = false;
        }
    }

    @Test
    void testSchemaEvolutionRefusals()
    {
        runner.execute("CREATE TABLE evolution_refusals (id bigint, spare bigint)");
        assertThatThrownBy(() -> new HoglakeMetadata(client).dropSchema(ConnectorTestFixtures.session(), "test", false))
                .hasMessageContaining("guarded-schema-evolution-v1");
        for (String statement : List.of(
                "CREATE SCHEMA new_schema",
                "ALTER TABLE evolution_refusals ADD COLUMN extra bigint",
                "ALTER TABLE evolution_refusals RENAME COLUMN spare TO renamed",
                "ALTER TABLE evolution_refusals DROP COLUMN spare")) {
            assertThatThrownBy(() -> runner.execute(statement)).hasMessageContaining("guarded-schema-evolution-v1");
        }
        assertThatThrownBy(() -> runner.execute("ALTER TABLE evolution_refusals ALTER COLUMN id SET DATA TYPE double"))
                .hasMessageContaining("SQL column type changes are not supported");
        assertThatThrownBy(() -> runner.execute("ALTER TABLE evolution_refusals ADD COLUMN required bigint NOT NULL"))
                .hasMessageContaining("nullable columns at the end");
        assertThatThrownBy(() -> runner.execute("ALTER TABLE evolution_refusals ADD COLUMN nested array(bigint)"))
                .hasMessageContaining("no hoglake equivalent");
        assertThatThrownBy(() -> runner.execute("ALTER TABLE evolution_refusals ADD COLUMN first_column bigint FIRST"))
                .hasMessageContaining("nullable columns at the end");
    }

    @Test
    void testReplacementSql()
    {
        runner.execute("CREATE OR REPLACE TABLE replacement AS SELECT BIGINT '7' AS id");
        String original = tables.get("replacement").tableUuid();
        runner.execute("CREATE OR REPLACE TABLE replacement AS SELECT id + 1 AS id FROM replacement");
        assertQuery("SELECT * FROM replacement", "VALUES BIGINT '8'");
        assertThat(tables.get("replacement").tableUuid()).isNotEqualTo(original);
        corruptCommitResponse = true;
        try {
            runner.execute("CREATE OR REPLACE TABLE replacement AS SELECT BIGINT '9' AS id");
        }
        finally {
            corruptCommitResponse = false;
        }
        assertQuery("SELECT * FROM replacement", "VALUES BIGINT '9'");
        runner.execute("CREATE OR REPLACE TABLE replacement (name varchar)");
        assertThat(tables.get("replacement").columns().getFirst().name()).isEqualTo("name");
        assertThat(runner.execute("SELECT count(*) FROM replacement").getOnlyValue()).isEqualTo(0L);
        replacementSupport = false;
        try {
            assertThatThrownBy(() -> runner.execute("CREATE OR REPLACE TABLE replacement (id bigint)"))
                    .hasMessageContaining("atomic-table-replacement-v1");
        }
        finally {
            replacementSupport = true;
        }
        assertThat(tables.get("replacement").columns().getFirst().name()).isEqualTo("name");
    }

    @Test
    void testTableLifecycleSql()
    {
        runner.execute("CREATE TABLE lifecycle AS SELECT BIGINT '7' AS id");
        HoglakeDtos.Table original = tables.get("lifecycle");
        runner.execute("ALTER TABLE lifecycle RENAME TO renamed_lifecycle");
        assertThat(tables).doesNotContainKey("lifecycle");
        assertThat(tables.get("renamed_lifecycle").tableUuid()).isEqualTo(original.tableUuid());
        assertQuery("SELECT * FROM renamed_lifecycle", "VALUES BIGINT '7'");
        runner.execute("TRUNCATE TABLE renamed_lifecycle");
        assertThat(runner.execute("SELECT count(*) FROM renamed_lifecycle").getOnlyValue()).isEqualTo(0L);
        assertThat(tables.get("renamed_lifecycle").tableUuid()).isEqualTo(original.tableUuid());
        assertThat(tables.get("renamed_lifecycle").columns()).isEqualTo(original.columns());
        runner.execute("INSERT INTO renamed_lifecycle VALUES 9");
        assertQuery("SELECT * FROM renamed_lifecycle", "VALUES BIGINT '9'");
        runner.execute("DROP TABLE renamed_lifecycle");
        assertThat(tables).doesNotContainKey("renamed_lifecycle");
    }

    @Test
    void testLifecycleGuardsAndAmbiguousTruncate()
    {
        runner.execute("CREATE TABLE lifecycle_guards (id bigint)");
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        var session = ConnectorTestFixtures.session();
        var handle = metadata.getTableHandle(session, new SchemaTableName("test", "lifecycle_guards"), Optional.empty(), Optional.empty());
        int before = lifecycleRequests;
        assertThatThrownBy(() -> metadata.renameTable(session, handle, new SchemaTableName("other", "moved"))).hasMessageContaining("between schemas");
        lifecycleSupport = false;
        try {
            assertThatThrownBy(() -> metadata.dropTable(session, handle)).hasMessageContaining("guarded-table-lifecycle-v1");
            assertThatThrownBy(() -> metadata.truncateTable(session, handle)).hasMessageContaining("guarded-table-lifecycle-v1");
            assertThatThrownBy(() -> metadata.renameTable(session, handle, new SchemaTableName("test", "moved"))).hasMessageContaining("guarded-table-lifecycle-v1");
        }
        finally {
            lifecycleSupport = true;
        }
        assertThat(lifecycleRequests).isEqualTo(before);
        runner.execute("DROP TABLE lifecycle_guards");
        runner.execute("CREATE TABLE lifecycle_guards AS SELECT BIGINT '1' AS id");
        assertThatThrownBy(() -> metadata.dropTable(session, handle)).hasMessageContaining("write conflict");
        assertThatThrownBy(() -> metadata.truncateTable(session, handle)).hasMessageContaining("write conflict");
        assertThatThrownBy(() -> metadata.renameTable(session, handle, new SchemaTableName("test", "moved"))).hasMessageContaining("write conflict");
        assertQuery("SELECT * FROM lifecycle_guards", "VALUES BIGINT '1'");
        before = lifecycleRequests;
        corruptLifecycleResponse = true;
        try {
            assertThatThrownBy(() -> runner.execute("TRUNCATE TABLE lifecycle_guards")).hasMessageContaining("outcome may be unknown");
        }
        finally {
            corruptLifecycleResponse = false;
        }
        assertThat(lifecycleRequests).isEqualTo(before + 1);
        runner.execute("INSERT INTO lifecycle_guards VALUES 2");
        assertQuery("SELECT * FROM lifecycle_guards", "VALUES BIGINT '2'");
    }

    @Test
    void testCreateInsertAndProjection()
    {
        runner.execute("CREATE TABLE writes (id bigint, label varchar, amount decimal(12,2))");
        assertThat(runner.execute("SELECT count(*) FROM writes").getOnlyValue()).isEqualTo(0L);
        assertThat(runner.execute("INSERT INTO writes VALUES (1, 'hello', 12.34), (2, NULL, -0.01)").getUpdateCount()).hasValue(2);
        runner.execute("INSERT INTO writes (label, id) VALUES ('reordered', 3)");
        assertQuery("SELECT id, label, amount FROM writes", "VALUES (BIGINT '1', 'hello', DECIMAL '12.34'), (BIGINT '2', NULL, DECIMAL '-0.01'), (BIGINT '3', 'reordered', CAST(NULL AS decimal(12,2)))");
        assertThat(runner.execute("SELECT count(*) FROM writes").getOnlyValue()).isEqualTo(3L);
        assertQuery("SELECT id FROM writes WHERE amount > 0", "VALUES BIGINT '1'");
        runner.execute("CREATE TABLE IF NOT EXISTS writes (ignored bigint)");
        assertThatThrownBy(() -> runner.execute("CREATE TABLE writes (ignored bigint)")).hasMessageContaining("already exists");
    }

    @Test
    void testCtasAndEmptyWrites()
    {
        assertThat(runner.execute("CREATE TABLE copied AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, label)").getUpdateCount()).hasValue(2);
        assertQuery("SELECT * FROM copied", "VALUES (1, 'a'), (2, 'b')");
        runner.execute("INSERT INTO copied SELECT * FROM copied");
        assertThat(runner.execute("SELECT count(*) FROM copied").getOnlyValue()).isEqualTo(4L);
        int before = commits;
        runner.execute("CREATE TABLE empty_copy AS SELECT * FROM copied WHERE false");
        runner.execute("INSERT INTO empty_copy SELECT * FROM copied WHERE false");
        assertThat(commits).isEqualTo(before + 1);
        assertThat(runner.execute("SELECT count(*) FROM empty_copy").getOnlyValue()).isEqualTo(0L);
        runner.execute("CREATE TABLE no_data AS SELECT * FROM copied WITH NO DATA");
        assertThat(runner.execute("SELECT count(*) FROM no_data").getOnlyValue()).isEqualTo(0L);
    }

    @Test
    void testAllTypes()
    {
        String values =
                """
                SELECT true AS b, INTEGER '-42' AS i, BIGINT '9223372036854775807' AS l,
                    REAL '1.25' AS f, DOUBLE '-3.5' AS d, 'héllo' AS s, X'00ff01' AS bytes,
                    DATE '1960-01-02' AS dt, TIME '23:59:59.123456' AS tm,
                    TIMESTAMP '1960-01-02 03:04:05.123456' AS ts,
                    TIMESTAMP '2024-01-02 03:04:05.123456 UTC' AS tz,
                    UUID '12345678-1234-5678-90ab-1234567890ab' AS u,
                    DECIMAL '-1234567890.12' AS short_decimal,
                    DECIMAL '123456789012345678901234567890.12345678' AS long_decimal
                """;
        runner.execute("CREATE TABLE all_types AS " + values);
        assertQuery("SELECT * FROM all_types", values);
        runner.execute("INSERT INTO all_types " + values);
        assertThat(runner.execute("SELECT count(*) FROM all_types").getOnlyValue()).isEqualTo(2L);
        runner.execute("INSERT INTO all_types VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)");
        assertThat(runner.execute("SELECT count(*) FROM all_types WHERE b IS NULL").getOnlyValue()).isEqualTo(1L);
    }

    @Test
    void testTemporalPrecisionWidening()
    {
        runner.execute("CREATE TABLE temporal_defaults (tm time, ts timestamp, tz timestamp with time zone)");
        String values = "VALUES (TIME '12:34:56.123456', TIMESTAMP '1960-01-02 03:04:05.123456', TIMESTAMP '2024-01-02 03:04:05.123456 UTC')";
        runner.execute("INSERT INTO temporal_defaults " + values);
        assertQuery("SELECT * FROM temporal_defaults", values);
        String lowPrecision = "SELECT TIME '12:34:56.123' AS tm, TIMESTAMP '1960-01-02 03:04:05' AS ts, TIMESTAMP '2024-01-02 03:04:05 UTC' AS tz";
        runner.execute("CREATE TABLE temporal_ctas AS " + lowPrecision);
        assertQuery("SELECT * FROM temporal_ctas", "SELECT CAST(tm AS time(6)), CAST(ts AS timestamp(6)), CAST(tz AS timestamp(6) with time zone) FROM (" + lowPrecision + ")");
    }

    @Test
    void testRowDeletesAndLostResponse()
    {
        idempotentDelete = true;
        try {
            runner.execute("CREATE TABLE row_deletes (id bigint, label varchar)");
            runner.execute("INSERT INTO row_deletes VALUES (1, 'a'), (2, 'b'), (3, NULL)");
            runner.execute("INSERT INTO row_deletes VALUES (4, 'a'), (5, 'b'), (6, NULL)");
            assertThat(runner.execute("DELETE FROM row_deletes WHERE id % 2 = 0 OR label IS NULL").getUpdateCount()).hasValue(4);
            assertQuery("SELECT * FROM row_deletes", "VALUES (BIGINT '1', 'a'), (BIGINT '5', 'b')");
            assertQuery("SELECT count(*) FROM row_deletes", "VALUES BIGINT '2'");
            assertThat(runner.execute("DELETE FROM row_deletes WHERE id % 2 = 0 OR label IS NULL").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("DELETE FROM row_deletes WHERE false").getUpdateCount()).hasValue(0);
            corruptCommitResponse = true;
            int before = commits;
            assertThat(runner.execute("DELETE FROM row_deletes WHERE label = 'a'").getUpdateCount()).hasValue(1);
            assertThat(commits).isEqualTo(before + 1);
            corruptCommitResponse = false;
            assertQuery("SELECT * FROM row_deletes", "VALUES (BIGINT '5', 'b')");
            commitStatus = 409;
            assertThatThrownBy(() -> runner.execute("DELETE FROM row_deletes")).hasMessageContaining("conflict");
            commitStatus = 200;
            assertQuery("SELECT count(*) FROM row_deletes", "VALUES BIGINT '1'");
            assertThat(runner.execute("DELETE FROM row_deletes").getUpdateCount()).hasValue(1);
            assertThat(runner.execute("DELETE FROM row_deletes").getUpdateCount()).hasValue(0);
            assertQuery("SELECT count(*) FROM row_deletes", "VALUES BIGINT '0'");
            assertThatThrownBy(() -> runner.execute("MERGE INTO row_deletes t USING (VALUES 1) s(id) ON t.id=s.id WHEN MATCHED THEN DELETE"))
                    .hasMessageContaining("does not support UPDATE or MERGE");
        }
        finally {
            idempotentDelete = false;
            corruptCommitResponse = false;
            commitStatus = 200;
        }
    }

    @Test
    void testWriteRejection()
    {
        assertThatThrownBy(() -> runner.execute("CREATE TABLE unsupported (a array(bigint))")).hasMessageContaining("no hoglake equivalent");
        assertThat(tables).doesNotContainKey("unsupported");
        runner.execute("CREATE TABLE required (id bigint NOT NULL, label varchar)");
        assertThatThrownBy(() -> runner.execute("INSERT INTO required VALUES (NULL, 'x')")).hasMessageContaining("NULL");
        assertThatThrownBy(() -> runner.execute("INSERT INTO required (label) VALUES ('x')")).hasMessageContaining("NOT NULL column: id");
        runner.execute("CREATE TABLE rejected (id bigint)");
        commitStatus = 409;
        int before = commits;
        try {
            assertThatThrownBy(() -> runner.execute("INSERT INTO rejected VALUES 1")).hasMessageContaining("write conflict");
        }
        finally {
            commitStatus = 200;
        }
        assertThat(commits).isEqualTo(before + 1);
        commitStatus = 409;
        try {
            assertThatThrownBy(() -> runner.execute("CREATE TABLE failed_ctas AS SELECT 1 AS id")).hasMessageContaining("write conflict");
        }
        finally {
            commitStatus = 200;
        }
        assertThat(tables).doesNotContainKey("failed_ctas");
        assertEventually(() -> assertThat(tables.keySet()).noneMatch(name -> name.startsWith("_trino_ctas_")));
        assertThat(runner.execute("SELECT count(*) FROM rejected").getOnlyValue()).isEqualTo(0L);
        assertThatThrownBy(() -> runner.execute("DELETE FROM rejected")).hasMessageContaining("does not support");
        assertThatThrownBy(() -> runner.execute("UPDATE rejected SET id = 2")).hasMessageContaining("does not support");
    }

    @Test
    void testMultipleWritersAndIncarnationGuard()
            throws Exception
    {
        runner.execute("CREATE TABLE multiple_writers (id bigint)");
        var session = ConnectorTestFixtures.session();
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        var table = metadata.getTableHandle(session, new SchemaTableName("test", "multiple_writers"), Optional.empty(), Optional.empty());
        var columns = List.copyOf(metadata.getColumnHandles(session, table).values());
        HoglakeWriteHandle handle = (HoglakeWriteHandle) metadata.beginInsert(session, table, columns, RetryMode.NO_RETRIES);
        HoglakePageSink first = new HoglakePageSink(storage.create(session), handle, "test");
        HoglakePageSink second = new HoglakePageSink(storage.create(session), handle, "test");
        first.appendPage(new Page(RunLengthEncodedBlock.create(BIGINT, 10L, 1)));
        second.appendPage(new Page(RunLengthEncodedBlock.create(BIGINT, 20L, 1)));
        var fragments = new ArrayList<>(first.finish().get());
        fragments.addAll(second.finish().get());
        metadata.finishInsert(session, handle, List.of(), fragments, List.of());
        assertQuery("SELECT * FROM multiple_writers", "VALUES BIGINT '10', BIGINT '20'");
        assertThat(files.get("multiple_writers")).hasSize(2);

        HoglakePageSink stale = new HoglakePageSink(storage.create(session), handle, "test");
        stale.appendPage(new Page(RunLengthEncodedBlock.create(BIGINT, 30L, 1)));
        var staleFragments = stale.finish().get();
        HoglakeDtos.Table original = tables.get("multiple_writers");
        tables.put("multiple_writers", new HoglakeDtos.Table(original.name(), original.namespace(), UUID.randomUUID().toString(), original.columns(), 0, 0, 0));
        files.put("multiple_writers", new ArrayList<>());
        assertThatThrownBy(() -> metadata.finishInsert(session, handle, List.of(), staleFragments, List.of())).hasMessageContaining("write conflict");
        assertThat(runner.execute("SELECT count(*) FROM multiple_writers").getOnlyValue()).isEqualTo(0L);
    }

    @Test
    void testAmbiguousCtasCommitResolvesReceipt()
    {
        corruptCommitResponse = true;
        try {
            runner.execute("CREATE TABLE uncertain_ctas AS SELECT BIGINT '42' AS id");
            assertQuery("SELECT id FROM uncertain_ctas", "VALUES BIGINT '42'");
            assertThat(files.get("uncertain_ctas")).hasSize(1);
            assertThat(tables.keySet()).noneMatch(name -> name.startsWith("_trino_ctas_"));
        }
        finally {
            corruptCommitResponse = false;
        }
    }

    @Test
    void testAtomicCreationCapabilityRequired()
    {
        atomicCreation = false;
        int before = creations.size();
        try {
            assertThatThrownBy(() -> runner.execute("CREATE TABLE old_server (id bigint)"))
                    .hasMessageContaining("does not support atomic-table-creation-v1");
            assertThat(creations).hasSize(before);
            assertThat(tables).doesNotContainKey("old_server");
        }
        finally {
            atomicCreation = true;
        }
    }

    @Test
    void testInsertOperationCapabilityAndEmptyInsert()
    {
        runner.execute("CREATE TABLE insert_operations (id bigint)");
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        var session = ConnectorTestFixtures.session();
        var table = metadata.getTableHandle(session, new SchemaTableName("test", "insert_operations"), Optional.empty(), Optional.empty());
        var columns = List.copyOf(metadata.getColumnHandles(session, table).values());
        HoglakeWriteHandle legacy = (HoglakeWriteHandle) metadata.beginInsert(session, table, columns, RetryMode.NO_RETRIES);
        assertThat(legacy.insertOperation()).isEmpty();
        idempotentAppend = true;
        try {
            HoglakeWriteHandle first = (HoglakeWriteHandle) metadata.beginInsert(session, table, columns, RetryMode.NO_RETRIES);
            HoglakeWriteHandle second = (HoglakeWriteHandle) metadata.beginInsert(session, table, columns, RetryMode.NO_RETRIES);
            assertThat(first.insertOperation()).isPresent().isNotEqualTo(second.insertOperation());
            assertThat(first.creationOperation()).isEmpty();
            int before = commits;
            metadata.finishInsert(session, first, List.of(), List.of(), List.of());
            assertThat(commits).isEqualTo(before);
        }
        finally {
            idempotentAppend = false;
        }
    }

    @Test
    void testUnsupportedWriteModes()
    {
        runner.execute("CREATE TABLE partitioned (id bigint)");
        HoglakeDtos.Table original = tables.get("partitioned");
        tables.put("partitioned", new HoglakeDtos.Table(
                original.name(),
                original.namespace(),
                original.tableUuid(),
                original.columns(),
                0,
                0,
                0,
                Map.of("spec_id", 1, "fields", List.of(Map.of("source_field_id", 1, "transform", "identity")))));
        assertThatThrownBy(() -> runner.execute("INSERT INTO partitioned VALUES 1")).hasMessageContaining("partitioned Hoglake tables");
        var session = ConnectorTestFixtures.session();
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        var table = metadata.getTableHandle(session, new SchemaTableName("test", "partitioned"), Optional.empty(), Optional.empty());
        assertThatThrownBy(() -> metadata.beginInsert(session, table, List.of(), RetryMode.RETRIES_ENABLED)).hasMessageContaining("do not support query retries");
    }

    private void assertQuery(String actual, String expected)
    {
        assertThat(runner.execute(actual).getMaterializedRows()).containsExactlyInAnyOrderElementsOf(runner.execute(expected).getMaterializedRows());
    }
}
