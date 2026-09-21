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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
            assertThatThrownBy(() -> runner.execute("UPDATE deletions SET id = 0")).hasMessageContaining("does not support UPDATE or MERGE");
            assertThatThrownBy(() -> runner.execute("MERGE INTO deletions t USING (VALUES 1) s(id) ON t.id=s.id WHEN MATCHED THEN DELETE"))
                    .hasMessageContaining("does not support UPDATE or MERGE");
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
                    .hasMessageContaining("SQL column type changes are not supported");
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
