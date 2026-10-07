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
import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Opt-in integration test of shredded VARIANT columns. Requires an isolated Hoglake
 * server and S3-compatible test bucket.
 *
 * <p>Table {@code events} declares a shredded layout in the catalog, and its twin
 * {@code events_plain} does not. The same statements write both tables, so every read
 * of the shredded table is compared with the same read of its twin.
 */
@EnabledIfSystemProperty(named = "hoglake.test.uri", matches = ".+")
final class TestHoglakeLiveShreddedVariant
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, Object> SHREDDING = Map.of("type", "object", "fields", List.of(
            Map.of("name", "$browser", "type", "string"),
            Map.of("name", "$screen_width", "type", "int64"),
            Map.of("name", "price", "type", "decimal8", "precision", 18, "scale", 2),
            Map.of("name", "big", "type", "decimal16", "precision", 38, "scale", 0),
            Map.of("name", "$set", "type", "object", "fields", List.of(Map.of("name", "plan", "type", "string"))),
            Map.of("name", "tags", "type", "array", "element", Map.of("type", "string")),
            Map.of("name", "payload", "type", "variant")));

    // The decimals come from casts of a ROW, from a plain cast and from JSON integers outside BIGINT, which
    // give them different widths. JSON numbers with a fraction are doubles, so they stay out of decimal columns.
    private static final String ROWS =
            """
            (1, CAST(JSON '{"$browser": "Chrome", "$screen_width": 1920, "$set": {"plan": "free"}, "tags": ["a", "b", null], "price": 12.34, "big": 18446744073709551616, "other": 1.5}' AS variant)),
            (2, CAST(JSON '{"$browser": 42, "$screen_width": "wide", "$set": "not an object", "tags": "not an array", "price": "free"}' AS variant)),
            (3, CAST(JSON '{"$Browser": "Safari", "$BROWSER": "Edge", "$set": {"Plan": "team", "plan": "pro"}, "$screen_width": 5000000000}' AS variant)),
            (4, CAST(JSON 'null' AS variant)),
            (5, NULL),
            (6, CAST(JSON '"a string"' AS variant)),
            (7, CAST(JSON '{}' AS variant)),
            (8, CAST(CAST(ROW(DECIMAL '12.34', 'Firefox') AS row(price decimal(18, 2), "$browser" varchar)) AS variant)),
            (9, CAST(CAST(ROW(CAST(DECIMAL '-0.05' AS variant)) AS row(price variant)) AS variant)),
            (10, CAST(CAST(ROW(DECIMAL '12.34', DECIMAL '18446744073709551616') AS row(price decimal(38, 2), big decimal(20, 0))) AS variant)),
            (11, CAST(CAST(ROW(DECIMAL '1.5') AS row(price decimal(10, 1))) AS variant)),
            (12, CAST(CAST(ROW(DECIMAL '12345678901234567890.12') AS row(price decimal(38, 2))) AS variant)),
            (13, CAST(JSON '{"payload": {"nested": [1, {"k": "v"}]}, "tags": []}' AS variant))
            """;

    // The number of values that the rows above have in typed columns
    private static final Map<String, Long> TYPED_VALUES = ImmutableMap.of(
            "properties.typed_value.$browser.typed_value", 2L,
            "properties.typed_value.$screen_width.typed_value", 2L,
            "properties.typed_value.price.typed_value", 3L,
            "properties.typed_value.big.typed_value", 2L,
            "properties.typed_value.$set.typed_value.plan.typed_value", 2L);

    private static final String WHOLE_VALUES = "SELECT id, properties IS NULL, json_format(CAST(properties AS json)) FROM %s WHERE id < 100000";
    private static final String FIELDS =
            """
            SELECT id, json_format(CAST(properties['$browser'] AS json)), json_format(CAST(properties['$Browser'] AS json)),
                json_format(CAST(properties['$screen_width'] AS json)), json_format(CAST(properties['price'] AS json)),
                json_format(CAST(properties['tags'] AS json)), json_format(CAST(properties['payload'] AS json)),
                json_format(CAST(properties['big'] AS json)), json_format(CAST(properties['other'] AS json))
            FROM %s
            WHERE id IN (1, 2, 3, 7, 8, 9, 10, 11, 12, 13)
            """;
    private static final String NESTED_FIELDS = "SELECT id, CAST(properties['$set']['plan'] AS varchar), CAST(properties['$set']['Plan'] AS varchar), CAST(properties['tags'][1] AS varchar) FROM %s WHERE id IN (1, 3, 7)";
    private static final String CHECKSUMS = "SELECT count(*), to_hex(checksum(id)), to_hex(checksum(json_format(CAST(properties AS json)))) FROM %s";

    @Test
    void testShreddedWrites()
            throws Exception
    {
        try (QueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("test").build())) {
            verifyShreddedWrites(runner);
        }
    }

    @Test
    void testDistributedShreddedWrites()
            throws Exception
    {
        try (QueryRunner runner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("hoglake").setSchema("test").build())
                .setWorkerCount(2)
                .setExtraProperties(Map.of("node-scheduler.include-coordinator", "false"))
                .build()) {
            verifyShreddedWrites(runner);
        }
    }

    private static void verifyShreddedWrites(QueryRunner runner)
            throws Exception
    {
        String uri = System.getProperty("hoglake.test.uri");
        String endpoint = System.getProperty("hoglake.test.s3-endpoint");
        String catalog = "trino_shredded_" + UUID.randomUUID().toString().replace("-", "");
        try (HttpClient http = HttpClient.newHttpClient()) {
            post(http, uri + "/v1/catalogs", Map.of("name", catalog, "data_path", System.getProperty("hoglake.test.data-path", "s3://trino-write-test/") + catalog + "/"));
            post(http, uri + "/v1/catalogs/" + catalog + "/namespaces", Map.of("name", "test"));
        }
        runner.installPlugin(new HoglakePlugin());
        runner.createCatalog("hoglake", "hoglake", Map.of(
                "hoglake.uri", uri,
                "hoglake.catalog", catalog,
                "s3.endpoint", endpoint,
                "s3.region", "us-east-1",
                "s3.path-style-access", "true",
                "s3.aws-access-key", "synthetic-test",
                "s3.aws-secret-key", "synthetic-test-password"));
        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setEndpoint(endpoint)
                .setRegion("us-east-1")
                .setPathStyleAccess(true)
                .setAwsAccessKey("synthetic-test")
                .setAwsSecretKey("synthetic-test-password"), new S3FileSystemStats());
        try (HttpClient http = HttpClient.newHttpClient(); HoglakeClient client = new HoglakeClient(uri, catalog)) {
            TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));

            // The server keeps the declaration with the column, and Trino writes with it
            client.createTable("test", "events", List.of(
                    new HoglakeDtos.ColumnDefinition("id", "long", null, true),
                    new HoglakeDtos.ColumnDefinition("properties", "variant", Map.of("shredding", SHREDDING), true)));
            runner.execute("CREATE TABLE events_plain (id bigint, properties variant)");
            assertDeclaration(client, "properties");

            long snapshot = client.getCatalog().headSnapshotId();
            execute(runner, "INSERT INTO %s VALUES " + ROWS);
            assertSameAsTwin(runner, WHOLE_VALUES);
            assertSameAsTwin(runner, FIELDS);
            assertSameAsTwin(runner, NESTED_FIELDS);
            assertThat(runner.execute("SELECT id FROM events WHERE properties IS NULL").getOnlyValue()).isEqualTo(5L);
            assertRows(
                    runner,
                    "SELECT id, CAST(properties['$browser'] AS varchar), CAST(properties['$Browser'] AS varchar), CAST(properties['$screen_width'] AS bigint), CAST(properties['price'] AS decimal(38, 2)), CAST(properties['big'] AS decimal(38, 0)) FROM events WHERE id IN (1, 3, 8, 9, 10)",
                    "VALUES (BIGINT '1', 'Chrome', NULL, BIGINT '1920', CAST(12.34 AS decimal(38, 2)), CAST(DECIMAL '18446744073709551616' AS decimal(38, 0))), (3, NULL, 'Safari', 5000000000, NULL, NULL), (8, 'Firefox', NULL, NULL, 12.34, NULL), (9, NULL, NULL, NULL, -0.05, NULL), (10, NULL, NULL, NULL, 12.34, DECIMAL '18446744073709551616')");

            // The files have the shredded layout, and the values that match a typed column are in it
            List<HoglakeDtos.ScanFile> files = filesAddedAfter(client, "events", snapshot);
            long fieldId = client.getTable("test", "events").orElseThrow().columns().get(1).fieldId();
            for (HoglakeDtos.ScanFile file : files) {
                assertShreddedLayout(footer(fileSystem, file), fieldId);
            }
            assertThat(typedValueCounts(fileSystem, files)).containsAllEntriesOf(TYPED_VALUES);
            assertThat(typedValueCounts(fileSystem, filesAddedAfter(client, "events_plain", snapshot))).isEmpty();

            // Subscripts read only the columns of their fields, with the same results
            assertThat((String) runner.execute("EXPLAIN " + FIELDS.formatted("events")).getOnlyValue()).contains("pruned to");
            Session withoutPushdown = Session.builder(runner.getDefaultSession())
                    .setSystemProperty("complex_expression_pushdown", "false")
                    .build();
            assertThat(runner.execute(withoutPushdown, FIELDS.formatted("events")).getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute(FIELDS.formatted("events")).getMaterializedRows());

            // The input repeats each value many times, as dictionary and run-length encoded blocks
            snapshot = client.getCatalog().headSnapshotId();
            execute(runner, "INSERT INTO %1$s SELECT t.id * 100000 + s.n, t.properties FROM %1$s t CROSS JOIN UNNEST(sequence(1, 5000)) s(n)");
            assertThat(typedValueCounts(fileSystem, filesAddedAfter(client, "events", snapshot)))
                    .containsAllEntriesOf(TYPED_VALUES.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue() * 5000)));
            assertSameAsTwin(runner, CHECKSUMS);

            execute(runner, "UPDATE %s SET properties = CAST(JSON '{\"$browser\": \"Opera\", \"price\": 0.10, \"extra\": true}' AS variant) WHERE id = 2");
            execute(runner,
                    """
                    MERGE INTO %s t
                    USING (VALUES (8, CAST(JSON '{"$browser": "Brave"}' AS variant)), (14, CAST(JSON '{"$screen_width": 7}' AS variant)), (6, NULL)) s(id, properties)
                    ON t.id = s.id
                    WHEN MATCHED AND s.properties IS NULL THEN DELETE
                    WHEN MATCHED THEN UPDATE SET properties = s.properties
                    WHEN NOT MATCHED THEN INSERT (id, properties) VALUES (s.id, s.properties)
                    """);
            execute(runner, "DELETE FROM %s WHERE id = 7");
            assertSameAsTwin(runner, WHOLE_VALUES);
            assertSameAsTwin(runner, CHECKSUMS);
            assertRows(
                    runner,
                    "SELECT id, CAST(properties['$browser'] AS varchar), CAST(properties['price'] AS decimal(38, 2)), CAST(properties['$screen_width'] AS bigint) FROM events WHERE id IN (2, 6, 7, 8, 14)",
                    "VALUES (BIGINT '2', 'Opera', CAST(0.10 AS decimal(38, 2)), NULL), (8, 'Brave', NULL, NULL), (14, NULL, NULL, BIGINT '7')");

            // Enough small files of the same size for compaction, also in a table without a VARIANT column
            runner.execute("CREATE TABLE compaction_control (id bigint)");
            for (int id = 20; id < 28; id++) {
                execute(runner, "INSERT INTO %s VALUES (" + id + ", CAST(JSON '{\"$browser\": \"Chrome\"}' AS variant))");
                runner.execute("INSERT INTO compaction_control VALUES " + id);
            }
            // The server computes the statistics and the totals of the tables asynchronously, and it
            // plans compaction and changes schemas only after that
            assertStatistics(client);
            List<Long> filesBeforeCompaction = dataFileIds(client, "events");
            assertEventually(() -> {
                assertThat(client.scan("test", "compaction_control", client.getCatalog().headSnapshotId())).allMatch(file -> file.dataFile().statsState().equals("provided"));
                assertThat(client.getTable("test", "compaction_control").orElseThrow().fileCount()).isEqualTo(8);
                assertThat(client.getTable("test", "events").orElseThrow().fileCount()).isEqualTo(filesBeforeCompaction.size());
            });

            // Compaction skips tables with VARIANT columns
            JsonNode compaction = post(http, uri + "/v1/catalogs/" + catalog + "/maintenance/compact?batch=100", Map.of());
            assertThat(dataFileIds(client, "compaction_control")).describedAs(compaction.toString()).hasSizeLessThan(8);
            assertThat(dataFileIds(client, "events")).describedAs(compaction.toString()).isEqualTo(filesBeforeCompaction);
            assertSameAsTwin(runner, CHECKSUMS);

            // A renamed column keeps its declaration, and reads find the column in files written before the rename
            execute(runner, "ALTER TABLE %s RENAME COLUMN properties TO props");
            assertDeclaration(client, "props");
            snapshot = client.getCatalog().headSnapshotId();
            execute(runner, "INSERT INTO %s VALUES (100, CAST(JSON '{\"$browser\": \"Edge\"}' AS variant))");
            assertThat(typedValueCounts(fileSystem, filesAddedAfter(client, "events", snapshot))).containsEntry("props.typed_value.$browser.typed_value", 1L);
            assertStatistics(client);
            execute(runner, "ALTER TABLE %s ADD COLUMN note varchar");
            assertSameAsTwin(runner, "SELECT id, note, json_format(CAST(props AS json)) FROM %s WHERE id < 100000");
            assertRows(runner, "SELECT id, CAST(props['$browser'] AS varchar) FROM events WHERE id IN (1, 100)", "VALUES (BIGINT '1', 'Chrome'), (100, 'Edge')");

            // The server refuses the declarations that Trino cannot write
            assertThatThrownBy(() -> client.createTable("test", "invalid_layout", List.of(
                    new HoglakeDtos.ColumnDefinition("properties", "variant", Map.of("shredding", Map.of("type", "text")), true))))
                    .hasMessageContaining("variant column 'properties' has an invalid type_params.shredding: $ has an unknown type 'text'");
            assertThatThrownBy(() -> client.createTable("test", "nested_layout", List.of(new HoglakeDtos.ColumnDefinition("r", "struct", null, true, List.of(
                    new HoglakeDtos.ColumnDefinition("x", "variant", Map.of("shredding", Map.of("type", "string")), true))))))
                    .hasMessageContaining("variant column 'r.x' is nested, and only a top-level variant column can declare type_params.shredding");
            assertThat(client.getTable("test", "invalid_layout")).isEmpty();
            assertThat(client.getTable("test", "nested_layout")).isEmpty();

            // SQL declares a layout when it creates a table or adds a column, and the server keeps it
            runner.execute("CREATE TABLE sql_declared (id bigint, properties variant WITH (shredding = '{\"type\": \"object\", \"fields\": [{\"name\": \"$browser\", \"type\": \"string\"}]}'))");
            runner.execute("ALTER TABLE sql_declared ADD COLUMN extra variant WITH (shredding = '{\"type\": \"int64\"}')");
            assertThat((String) runner.execute("SHOW CREATE TABLE sql_declared").getOnlyValue()).contains(
                    "shredding = '{\"type\":\"object\",\"fields\":[{\"name\":\"$browser\",\"type\":\"string\"}]}'",
                    "shredding = '{\"type\":\"int64\"}'");
            snapshot = client.getCatalog().headSnapshotId();
            runner.execute("INSERT INTO sql_declared VALUES (1, CAST(JSON '{\"$browser\": \"Chrome\"}' AS variant), CAST(JSON '7' AS variant))");
            assertThat(typedValueCounts(fileSystem, filesAddedAfter(client, "sql_declared", snapshot)))
                    .containsEntry("properties.typed_value.$browser.typed_value", 1L)
                    .containsEntry("extra.typed_value", 1L);
            assertRows(runner, "SELECT CAST(properties['$browser'] AS varchar), CAST(extra AS bigint) FROM sql_declared", "VALUES ('Chrome', BIGINT '7')");
        }
        finally {
            fileSystemFactory.destroy();
        }
    }

    /**
     * Executes a statement on the shredded table and then on its twin. The shredded table
     * goes first, so its new files are those after the snapshot before the statement.
     */
    private static void execute(QueryRunner runner, String statement)
    {
        for (String table : List.of("events", "events_plain")) {
            runner.execute(statement.formatted(table));
        }
    }

    private static void assertSameAsTwin(QueryRunner runner, String query)
    {
        List<MaterializedRow> rows = runner.execute(query.formatted("events")).getMaterializedRows();
        assertThat(rows).isNotEmpty();
        assertThat(rows).containsExactlyInAnyOrderElementsOf(runner.execute(query.formatted("events_plain")).getMaterializedRows());
    }

    private static void assertRows(QueryRunner runner, String actual, String expected)
    {
        assertThat(runner.execute(actual).getMaterializedRows()).containsExactlyInAnyOrderElementsOf(runner.execute(expected).getMaterializedRows());
    }

    private static void assertDeclaration(HoglakeClient client, String column)
    {
        HoglakeDtos.Column declared = client.getTable("test", "events").orElseThrow().columns().stream()
                .filter(candidate -> candidate.name().equals(column))
                .collect(onlyElement());
        // JSON trees, because the server can return the numbers as other Java types
        JsonNode actual = MAPPER.valueToTree(declared.typeParams());
        JsonNode expected = MAPPER.valueToTree(Map.of("shredding", SHREDDING));
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Waits until the server has computed the statistics of every file of both tables
     * from their footers, and checks the statistics of the id column.
     */
    private static void assertStatistics(HoglakeClient client)
    {
        long head = client.getCatalog().headSnapshotId();
        for (String table : List.of("events", "events_plain")) {
            long idFieldId = client.getTable("test", table).orElseThrow().columns().getFirst().fieldId();
            assertEventually(() -> assertThat(client.planningScan("test", table, head, Set.of(idFieldId))).allSatisfy(file -> {
                assertThat(file.dataFile().statsState()).isEqualTo("provided");
                assertThat(file.dataFile().columnStats()).anySatisfy(stats -> {
                    assertThat(stats.fieldId()).isEqualTo(idFieldId);
                    assertThat(stats.valueCount()).isEqualTo(file.dataFile().recordCount());
                    assertThat(stats.lowerBound()).isNotNull();
                    assertThat(stats.lowerBound().isNull()).isFalse();
                });
            }));
        }
    }

    private static void assertShreddedLayout(FileMetaData footer, long fieldId)
    {
        List<SchemaElement> schema = footer.getSchema();
        SchemaElement group = schema.stream().filter(element -> element.getName().equals("properties")).collect(onlyElement());
        assertThat(group.getLogicalType()).isEqualTo(LogicalTypes.VARIANT((byte) 1));
        assertThat((long) group.getField_id()).isEqualTo(fieldId);
        List<SchemaElement> children = schema.subList(schema.indexOf(group) + 1, schema.size());
        assertThat(children).allSatisfy(element -> assertThat(element.isSetField_id()).isFalse());
        assertThat(children).extracting(SchemaElement::getName).contains("metadata", "value", "typed_value", "$browser", "$screen_width", "price", "big", "$set", "plan", "tags", "list", "element", "payload");
        assertThat(children).filteredOn(element -> Set.of("$browser", "$screen_width", "price", "big", "$set", "plan", "tags", "element", "payload").contains(element.getName()))
                .allSatisfy(element -> assertThat(element.getRepetition_type()).isEqualTo(FieldRepetitionType.REQUIRED));
    }

    private static List<HoglakeDtos.ScanFile> filesAddedAfter(HoglakeClient client, String table, long snapshot)
    {
        List<HoglakeDtos.ScanFile> files = client.scan("test", table, client.getCatalog().headSnapshotId()).stream()
                .filter(file -> file.dataFile().beginSnapshot() > snapshot)
                .collect(toImmutableList());
        assertThat(files).isNotEmpty();
        return files;
    }

    private static List<Long> dataFileIds(HoglakeClient client, String table)
    {
        return client.scan("test", table, client.getCatalog().headSnapshotId()).stream()
                .map(file -> file.dataFile().dataFileId())
                .sorted()
                .collect(toImmutableList());
    }

    /**
     * The number of values in each column under a typed_value group, in all row groups of the files.
     */
    private static Map<String, Long> typedValueCounts(TrinoFileSystem fileSystem, List<HoglakeDtos.ScanFile> files)
            throws IOException
    {
        Map<String, Long> counts = new HashMap<>();
        for (HoglakeDtos.ScanFile file : files) {
            for (RowGroup rowGroup : footer(fileSystem, file).getRow_groups()) {
                for (ColumnChunk column : rowGroup.getColumns()) {
                    ColumnMetaData metadata = column.getMeta_data();
                    if (metadata.getPath_in_schema().contains("typed_value")) {
                        counts.merge(String.join(".", metadata.getPath_in_schema()), metadata.getNum_values() - metadata.getStatistics().getNull_count(), Long::sum);
                    }
                }
            }
        }
        return ImmutableMap.copyOf(counts);
    }

    private static FileMetaData footer(TrinoFileSystem fileSystem, HoglakeDtos.ScanFile file)
            throws IOException
    {
        try (TrinoInputStream input = fileSystem.newInputFile(Location.of(file.dataFile().path())).newStream()) {
            return ConnectorTestFixtures.fileMetaData(input.readAllBytes());
        }
    }

    private static JsonNode post(HttpClient http, String uri, Object body)
            throws Exception
    {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(uri))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(MAPPER.writeValueAsBytes(body)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).describedAs(response.body()).isIn(200, 201);
        return MAPPER.readTree(response.body());
    }
}
