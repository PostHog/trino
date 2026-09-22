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
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Opt-in integration test. Requires an isolated Hoglake server and S3-compatible test bucket.
 */
@EnabledIfSystemProperty(named = "hoglake.test.uri", matches = ".+")
final class TestHoglakeLiveWrites
{
    @Test
    void testLiveRoundTrip()
            throws Exception
    {
        roundTrip(false);
    }

    @Test
    @EnabledIfSystemProperty(named = "hoglake.test.root-path", matches = ".+")
    void testBucketRoot()
            throws Exception
    {
        roundTrip(true);
    }

    @Test
    void testDistributedRoundTrip()
            throws Exception
    {
        roundTrip(false, true);
    }

    private void roundTrip(boolean bucketRoot)
            throws Exception
    {
        roundTrip(bucketRoot, false);
    }

    private void roundTrip(boolean bucketRoot, boolean distributed)
            throws Exception
    {
        String uri = System.getProperty("hoglake.test.uri");
        String catalog = "trino_write_" + UUID.randomUUID().toString().replace("-", "");
        String dataPath = bucketRoot ? System.getProperty("hoglake.test.root-path") : System.getProperty("hoglake.test.data-path", "s3://trino-write-test/") + catalog + "/";
        try (HttpClient http = HttpClient.newHttpClient()) {
            post(http, uri + "/v1/catalogs", Map.of("name", catalog, "data_path", dataPath));
            post(http, uri + "/v1/catalogs/" + catalog + "/namespaces", Map.of("name", "test"));
        }
        try (QueryRunner runner = distributed
                ? DistributedQueryRunner.builder(testSessionBuilder().setCatalog("hoglake").setSchema("test").build())
                  .setWorkerCount(2)
                  .setExtraProperties(Map.of("node-scheduler.include-coordinator", "false"))
                  .build()
                : new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("test").build());
                HoglakeClient client = new HoglakeClient(uri, catalog)) {
            runner.installPlugin(new HoglakePlugin());
            runner.createCatalog("hoglake", "hoglake", Map.of(
                    "hoglake.uri", uri,
                    "hoglake.catalog", catalog,
                    "s3.endpoint", System.getProperty("hoglake.test.s3-endpoint"),
                    "s3.region", "us-east-1",
                    "s3.path-style-access", "true",
                    "s3.aws-access-key", "synthetic-test",
                    "s3.aws-secret-key", "synthetic-test-password"));
            runner.execute("CREATE TABLE deletions (id bigint, label varchar)");
            runner.execute("INSERT INTO deletions SELECT id, IF(id % 3 = 0, NULL, 'keep') FROM UNNEST(sequence(1, 10000)) t(id)");
            runner.execute("INSERT INTO deletions SELECT id, IF(id % 3 = 0, NULL, 'keep') FROM UNNEST(sequence(10001, 20000)) t(id)");
            long beforeDelete = client.getCatalog().headSnapshotId();
            String deleteUuid = client.getTable("test", "deletions").orElseThrow().tableUuid();
            assertThat(runner.execute("DELETE FROM deletions WHERE id > 10000 AND (id % 2 = 0 OR label IS NULL)").getUpdateCount()).hasValue(6666);
            assertThat(runner.execute("SELECT count(*) FROM deletions").getOnlyValue()).isEqualTo(13334L);
            assertThat(runner.execute("SELECT count(*) FROM deletions WHERE id > 10000 AND (id % 2 = 0 OR label IS NULL)").getOnlyValue()).isEqualTo(0L);
            long firstDelete = client.getCatalog().headSnapshotId();
            assertThat(client.getTable("test", "deletions", beforeDelete).orElseThrow().recordCount()).isEqualTo(20000);
            assertThat(client.scan("test", "deletions", beforeDelete)).allMatch(file -> file.deleteFile() == null);
            assertThat(runner.execute("DELETE FROM deletions WHERE id > 10000 AND (id % 2 = 0 OR label IS NULL)").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("DELETE FROM deletions WHERE id % 2 = 1").getUpdateCount()).hasValue(8334);
            assertThat(runner.execute("SELECT count(*) FROM deletions").getOnlyValue()).isEqualTo(5000L);
            assertThat(runner.execute("SELECT sum(id) FROM deletions").getOnlyValue()).isEqualTo(25005000L);
            assertThat(client.scan("test", "deletions", firstDelete).stream()
                    .mapToLong(file -> file.dataFile().recordCount() - (file.deleteFile() == null ? 0 : file.deleteFile().deleteCount())).sum()).isEqualTo(13334);
            assertThat(client.getTable("test", "deletions").orElseThrow().tableUuid()).isEqualTo(deleteUuid);
            assertThat(runner.execute("DELETE FROM deletions WHERE false").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("DELETE FROM deletions").getUpdateCount()).hasValue(5000);
            assertThat(runner.execute("DELETE FROM deletions").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("SELECT count(*) FROM deletions").getOnlyValue()).isEqualTo(0L);
            assertThat(runner.execute("UPDATE deletions SET id = 0").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("MERGE INTO deletions t USING (VALUES 1) s(id) ON t.id=s.id WHEN MATCHED THEN DELETE").getUpdateCount()).hasValue(0);
            runner.execute("CREATE TABLE mutations (id bigint, label varchar, untouched bigint)");
            runner.execute("INSERT INTO mutations SELECT id, 'old', id * 10 FROM UNNEST(sequence(1, 10000)) t(id)");
            runner.execute("INSERT INTO mutations VALUES (10001, 'extra', 100010)");
            runner.execute("DELETE FROM mutations WHERE id = 2");
            long beforeUpdate = client.getCatalog().headSnapshotId();
            var beforeUpdateFiles = client.scan("test", "mutations", beforeUpdate);
            assertThat(runner.execute("UPDATE mutations SET label = NULL, id = id + 20000 WHERE id > 5000").getUpdateCount()).hasValue(5001);
            assertThat(runner.execute("SELECT count(*) FROM mutations").getOnlyValue()).isEqualTo(10000L);
            assertThat(runner.execute("SELECT count(*) FROM mutations WHERE id > 20000 AND label IS NULL AND untouched = (id - 20000) * 10").getOnlyValue()).isEqualTo(5001L);
            assertThat(client.scan("test", "mutations", beforeUpdate)).usingRecursiveComparison().ignoringFieldsMatchingRegexes(".*statsState").isEqualTo(beforeUpdateFiles);
            assertThat(beforeUpdateFiles.stream().mapToLong(file -> file.dataFile().recordCount() - (file.deleteFile() == null ? 0 : file.deleteFile().deleteCount())).sum()).isEqualTo(10000L);
            long beforeMerge = client.getCatalog().headSnapshotId();
            var beforeMergeFiles = client.scan("test", "mutations", beforeMerge);
            String merge = "MERGE INTO mutations t USING (VALUES (1, 'updated'), (3, 'deleted'), (40000, 'inserted')) s(id, label) ON t.id=s.id " +
                    "WHEN MATCHED AND s.id = 3 THEN DELETE WHEN MATCHED THEN UPDATE SET label=s.label " +
                    "WHEN NOT MATCHED THEN INSERT (id, label, untouched) VALUES (s.id, s.label, 42)";
            assertThat(runner.execute(merge).getUpdateCount()).hasValue(3);
            assertThat(client.getCatalog().headSnapshotId()).isEqualTo(beforeMerge + 1);
            assertThat(runner.execute("SELECT count(*) FROM mutations").getOnlyValue()).isEqualTo(10000L);
            assertThat(runner.execute("SELECT id, label, untouched FROM mutations WHERE id IN (1, 2, 3, 40000)").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute("VALUES (BIGINT '1', 'updated', BIGINT '10'), (BIGINT '40000', 'inserted', BIGINT '42')").getMaterializedRows());
            assertThat(client.scan("test", "mutations", beforeMerge)).usingRecursiveComparison().ignoringFieldsMatchingRegexes(".*statsState").isEqualTo(beforeMergeFiles);
            long beforeAmbiguous = client.getCatalog().headSnapshotId();
            assertThatThrownBy(() -> runner.execute("MERGE INTO mutations t USING (VALUES 1, 1) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET label='ambiguous'"))
                    .hasMessageContaining("matched more than one source row");
            assertThat(client.getCatalog().headSnapshotId()).isEqualTo(beforeAmbiguous);
            assertThat(runner.execute("UPDATE mutations SET label = 'no' WHERE false").getUpdateCount()).hasValue(0);
            assertThat(runner.execute("MERGE INTO mutations t USING (VALUES 40001) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)").getUpdateCount()).hasValue(1);
            assertThat(runner.execute("MERGE INTO mutations t USING (VALUES 40001) s(id) ON t.id=s.id WHEN MATCHED THEN DELETE").getUpdateCount()).hasValue(1);
            client.createTable("test", "external_scalars", List.of(
                    new HoglakeDtos.ColumnDefinition("u8", "uint8", null, true),
                    new HoglakeDtos.ColumnDefinition("u16", "uint16", null, true),
                    new HoglakeDtos.ColumnDefinition("u32", "uint32", null, true),
                    new HoglakeDtos.ColumnDefinition("u64", "uint64", null, true),
                    new HoglakeDtos.ColumnDefinition("j", "json", null, true),
                    new HoglakeDtos.ColumnDefinition("s", "timestamp_s", null, true),
                    new HoglakeDtos.ColumnDefinition("ms", "timestamp_ms", null, true),
                    new HoglakeDtos.ColumnDefinition("nested", "list", null, true, List.of(new HoglakeDtos.ColumnDefinition("element", "uint64", null, true)))));
            runner.execute("INSERT INTO external_scalars VALUES (255, 65535, 4294967295, DECIMAL '18446744073709551615', '{\"k\":1}', TIMESTAMP '1969-12-31 23:59:59', TIMESTAMP '1969-12-31 23:59:59.999', ARRAY[DECIMAL '18446744073709551615', NULL])");
            assertThat(runner.execute("SELECT CAST(u8 AS varchar), CAST(u16 AS varchar), CAST(u32 AS varchar), CAST(u64 AS varchar), j, CAST(s AS varchar), CAST(ms AS varchar), CAST(nested[1] AS varchar) FROM external_scalars").getMaterializedRows())
                    .containsExactlyElementsOf(runner.execute("VALUES ('255', '65535', '4294967295', '18446744073709551615', '{\"k\":1}', '1969-12-31 23:59:59.000000', '1969-12-31 23:59:59.999000', '18446744073709551615')").getMaterializedRows());
            assertThatThrownBy(() -> runner.execute("INSERT INTO external_scalars (u64) VALUES DECIMAL '18446744073709551616'"))
                    .hasMessageContaining("outside uint64 range");
            assertThatThrownBy(() -> runner.execute("INSERT INTO external_scalars (u8) VALUES -1"))
                    .hasMessageContaining("outside uint8 range");
            assertThatThrownBy(() -> runner.execute("INSERT INTO external_scalars (j) VALUES 'invalid'"))
                    .hasMessageContaining("Invalid JSON");
            assertThatThrownBy(() -> runner.execute("INSERT INTO external_scalars (s) VALUES TIMESTAMP '2020-01-01 00:00:00.001'"))
                    .hasMessageContaining("precision exceeds");
            assertEventually(() -> assertThat(client.scan("test", "external_scalars", client.getCatalog().headSnapshotId()))
                    .allMatch(file -> file.dataFile().statsState().equals("provided")));
            client.createTable("test", "nested_unsigned", List.of(new HoglakeDtos.ColumnDefinition("r", "struct", null, true, List.of(
                    new HoglakeDtos.ColumnDefinition("required", "uint64", null, false),
                    new HoglakeDtos.ColumnDefinition("m", "map", null, true, List.of(
                            new HoglakeDtos.ColumnDefinition("key", "uint64", null, false),
                            new HoglakeDtos.ColumnDefinition("value", "uint64", null, true)))))));
            runner.execute("INSERT INTO nested_unsigned SELECT ROW(DECIMAL '18446744073709551615', MAP(ARRAY[DECIMAL '18446744073709551615'], ARRAY[DECIMAL '9223372036854775808']))");
            assertThat(runner.execute("SELECT CAST(r.required AS varchar), CAST(r.m[DECIMAL '18446744073709551615'] AS varchar) FROM nested_unsigned").getMaterializedRows())
                    .containsExactlyElementsOf(runner.execute("VALUES ('18446744073709551615', '9223372036854775808')").getMaterializedRows());
            assertThatThrownBy(() -> runner.execute("INSERT INTO nested_unsigned SELECT CAST(ROW(NULL, NULL) AS row(required decimal(20,0), m map(decimal(20,0),decimal(20,0))))"))
                    .hasMessageContaining("NULL value for required column");
            runner.execute("INSERT INTO nested_unsigned VALUES NULL");
            assertThat(runner.execute("SELECT count(*) FROM nested_unsigned WHERE r IS NULL").getOnlyValue()).isEqualTo(1L);
            runner.execute("CREATE TABLE native_variant AS SELECT CAST(42 AS variant) AS v");
            assertThat(runner.execute("SELECT CAST(v AS integer) FROM native_variant").getOnlyValue()).isEqualTo(42);
            runner.execute("CREATE TABLE nested_values (id bigint, a array(row(x integer, y varchar)), m map(varchar, array(integer)), r row(x tinyint, y smallint), ts timestamp(9))");
            runner.execute("INSERT INTO nested_values VALUES (1, ARRAY[ROW(7, 'old'), NULL], MAP(ARRAY['k'], ARRAY[ARRAY[4, NULL]]), ROW(TINYINT '-128', SMALLINT '32767'), TIMESTAMP '1969-12-31 23:59:59.999999999'), (2, ARRAY[], MAP(), NULL, NULL), (3, NULL, NULL, ROW(NULL, NULL), NULL)");
            assertEventually(() -> assertThat(client.scan("test", "nested_values", client.getCatalog().headSnapshotId()))
                    .allMatch(file -> file.dataFile().statsState().equals("provided")));
            var nestedColumns = client.getTable("test", "nested_values").orElseThrow().columns();
            long nestedFieldId = nestedColumns.get(3).children().getFirst().fieldId();
            client.alterColumns("test", "nested_values", client.getTable("test", "nested_values").orElseThrow().tableUuid(), client.getCatalog().headSnapshotId(), Map.of("op", "rename_column", "from", "r.x", "to", "renamed"));
            assertThat(client.getTable("test", "nested_values").orElseThrow().columns().get(3).children().getFirst().fieldId()).isEqualTo(nestedFieldId);
            assertThat(runner.execute("SELECT r.renamed FROM nested_values WHERE id=1").getOnlyValue()).isEqualTo((byte) -128);
            runner.execute("UPDATE nested_values SET a = ARRAY[ROW(8, 'new')] WHERE id=1");
            runner.execute("MERGE INTO nested_values t USING (VALUES 2) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET ts=TIMESTAMP '2020-01-01 00:00:00.123456789'");
            assertThat(runner.execute("SELECT a[1].x FROM nested_values WHERE id=1").getOnlyValue()).isEqualTo(8);
            assertThat(runner.execute("SELECT CAST(ts AS varchar) FROM nested_values WHERE id=2").getOnlyValue()).isEqualTo("2020-01-01 00:00:00.123456789");
            assertThat(runner.execute("SELECT count(*) FROM nested_values WHERE r IS NULL").getOnlyValue()).isEqualTo(1L);
            assertThat(runner.execute("SELECT count(*) FROM nested_values WHERE r IS NOT NULL AND r.renamed IS NULL").getOnlyValue()).isEqualTo(1L);
            runner.execute("CREATE TABLE promotions AS SELECT INTEGER '-2147483648' AS i, REAL '1.5' AS r");
            long beforePromotion = client.getCatalog().headSnapshotId();
            var originalColumns = client.getTable("test", "promotions").orElseThrow().columns();
            assertEventually(() -> assertThat(client.scan("test", "promotions", client.getCatalog().headSnapshotId()))
                    .allMatch(file -> file.dataFile().statsState().equals("provided")));
            runner.execute("ALTER TABLE promotions ALTER COLUMN i SET DATA TYPE bigint");
            runner.execute("ALTER TABLE promotions ALTER COLUMN r SET DATA TYPE double");
            assertThat(client.getTable("test", "promotions", beforePromotion).orElseThrow().columns()).isEqualTo(originalColumns);
            runner.execute("INSERT INTO promotions VALUES (BIGINT '2147483648', DOUBLE '2.25')");
            assertThat(runner.execute("SELECT i, r FROM promotions ORDER BY i").getMaterializedRows())
                    .containsExactlyElementsOf(runner.execute("VALUES (BIGINT '-2147483648', DOUBLE '1.5'), (BIGINT '2147483648', DOUBLE '2.25')").getMaterializedRows());
            runner.execute("UPDATE promotions SET i = i + 1 WHERE r = 1.5");
            runner.execute("MERGE INTO promotions t USING (VALUES (BIGINT '2147483648', DOUBLE '3.5')) s(i, r) ON t.i=s.i WHEN MATCHED THEN UPDATE SET r=s.r");
            assertThat(runner.execute("SELECT i, r FROM promotions ORDER BY i").getMaterializedRows())
                    .containsExactlyElementsOf(runner.execute("VALUES (BIGINT '-2147483647', DOUBLE '1.5'), (BIGINT '2147483648', DOUBLE '3.5')").getMaterializedRows());
            runner.execute("CREATE SCHEMA evolved");
            runner.execute("CREATE TABLE evolved.records (id bigint, label varchar, discarded bigint)");
            runner.execute("INSERT INTO evolved.records VALUES (1, 'old', 100)");
            long labelId = client.getTable("evolved", "records").orElseThrow().columns().get(1).fieldId();
            assertEventually(() -> assertThat(client.scan("evolved", "records", client.getCatalog().headSnapshotId()))
                    .allMatch(file -> file.dataFile().statsState().equals("provided")));
            runner.execute("ALTER TABLE evolved.records ADD COLUMN extra decimal(12,2)");
            runner.execute("ALTER TABLE evolved.records RENAME COLUMN label TO renamed");
            assertThat(client.getTable("evolved", "records").orElseThrow().columns().get(1).fieldId()).isEqualTo(labelId);
            runner.execute("ALTER TABLE evolved.records DROP COLUMN discarded");
            runner.execute("INSERT INTO evolved.records VALUES (2, 'new', 12.34)");
            assertThat(runner.execute("SELECT * FROM evolved.records").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute("VALUES (BIGINT '1', 'old', CAST(NULL AS decimal(12,2))), (BIGINT '2', 'new', DECIMAL '12.34')").getMaterializedRows());
            assertEventually(() -> assertThat(client.scan("evolved", "records", client.getCatalog().headSnapshotId()))
                    .allMatch(file -> file.dataFile().statsState().equals("provided")));
            runner.execute("ALTER TABLE evolved.records ADD COLUMN label varchar");
            runner.execute("INSERT INTO evolved.records (id, label) VALUES (3, 'reused')");
            assertThat(runner.execute("SELECT id, renamed, label FROM evolved.records").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute("VALUES (BIGINT '1', 'old', NULL), (BIGINT '2', 'new', NULL), (BIGINT '3', NULL, 'reused')").getMaterializedRows());
            assertThatThrownBy(() -> runner.execute("ALTER TABLE evolved.records ALTER COLUMN id SET DATA TYPE double"))
                    .hasMessageContaining("Unsupported Hoglake column type change");
            assertThatThrownBy(() -> runner.execute("DROP SCHEMA evolved"))
                    .hasMessageContaining("non-empty");
            assertThatThrownBy(() -> runner.execute("DROP SCHEMA evolved CASCADE"))
                    .hasMessageContaining("CASCADE is not supported");
            runner.execute("DROP TABLE evolved.records");
            runner.execute("DROP SCHEMA evolved");
            assertThat(client.listNamespaces()).noneMatch(namespace -> namespace.name().equals("evolved"));
            runner.execute("CREATE TABLE measurements (id bigint NOT NULL, label varchar, amount decimal(12,2))");
            runner.execute("INSERT INTO measurements VALUES (1, 'hello', 12.34), (2, NULL, -0.01)");
            runner.execute("INSERT INTO measurements (label, id) VALUES ('omitted', 3)");
            assertThat(runner.execute("SELECT * FROM measurements").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute("VALUES (BIGINT '1', 'hello', DECIMAL '12.34'), (BIGINT '2', NULL, DECIMAL '-0.01'), (BIGINT '3', 'omitted', CAST(NULL AS decimal(12,2)))").getMaterializedRows());
            runner.execute("CREATE TABLE copied AS SELECT * FROM measurements");
            assertThat(runner.execute("SELECT * FROM copied").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute("SELECT * FROM measurements").getMaterializedRows());
            runner.execute("INSERT INTO copied SELECT * FROM copied");
            assertThat(runner.execute("SELECT count(*) FROM copied").getOnlyValue()).isEqualTo(6L);
            runner.execute("CREATE TABLE empty_table AS SELECT * FROM copied WITH NO DATA");
            runner.execute("INSERT INTO empty_table SELECT * FROM copied WHERE false");
            assertThat(runner.execute("SELECT count(*) FROM empty_table").getOnlyValue()).isEqualTo(0L);
            String scalarValues =
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
            runner.execute("CREATE TABLE scalar_types AS " + scalarValues);
            assertThat(runner.execute("SELECT * FROM scalar_types").getMaterializedRows())
                    .containsExactlyInAnyOrderElementsOf(runner.execute(scalarValues).getMaterializedRows());
            runner.execute("INSERT INTO scalar_types " + scalarValues);
            runner.execute("INSERT INTO scalar_types VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)");
            assertThat(runner.execute("SELECT count(*) FROM scalar_types WHERE ts IS NOT NULL").getOnlyValue()).isEqualTo(2L);
            if (!distributed && !bucketRoot) {
                // Enough equal-sized files to reach the server's default compaction tier quota.
                for (int index = 0; index < 8; index++) {
                    runner.execute("INSERT INTO scalar_types " + scalarValues);
                }
                var beforeCompaction = runner.execute("SELECT * FROM scalar_types").getMaterializedRows();
                long filesBefore = client.getTable("test", "scalar_types").orElseThrow().fileCount();
                try (HttpClient http = HttpClient.newHttpClient()) {
                    JsonNode result = post(http, uri + "/v1/catalogs/" + catalog + "/maintenance/compact?batch=100", Map.of());
                    assertThat(result.path("unconvertible_schema").asLong()).isZero();
                }
                assertThat(client.getTable("test", "scalar_types").orElseThrow().fileCount()).isLessThan(filesBefore);
                assertThat(runner.execute("SELECT * FROM scalar_types").getMaterializedRows())
                        .containsExactlyInAnyOrderElementsOf(beforeCompaction);
            }
            if (distributed) {
                assertThat(runner.execute("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'").getOnlyValue()).isEqualTo(3L);
                runner.execute("CREATE TABLE distributed_source AS SELECT CAST(id AS bigint) id FROM UNNEST(sequence(1, 10000)) AS t(id)");
                for (int index = 0; index < 4; index++) {
                    runner.execute("INSERT INTO distributed_source SELECT CAST(id AS bigint) FROM UNNEST(sequence(1, 10000)) AS t(id)");
                }
                runner.execute("CREATE TABLE distributed_copy AS SELECT * FROM distributed_source");
                assertThat(runner.execute("SELECT count(*) FROM distributed_copy").getOnlyValue()).isEqualTo(50000L);
                assertThat(runner.execute("SELECT sum(id) FROM distributed_copy").getOnlyValue()).isEqualTo(250025000L);
            }
            assertThat(client.listTables("test")).noneMatch(table -> table.name().startsWith("_trino_ctas_"));
            assertThat(client.getTable("test", "measurements").orElseThrow().recordCount()).isEqualTo(3);
        }
    }

    private static JsonNode post(HttpClient http, String uri, Object body)
            throws Exception
    {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(uri))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new ObjectMapper().writeValueAsBytes(body)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).describedAs(response.body()).isIn(200, 201);
        return new ObjectMapper().readTree(response.body());
    }
}
