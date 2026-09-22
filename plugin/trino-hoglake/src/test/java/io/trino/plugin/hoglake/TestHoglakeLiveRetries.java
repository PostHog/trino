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
import com.google.inject.Scopes;
import io.trino.Session;
import io.trino.client.StageStats;
import io.trino.execution.FailureInjector;
import io.trino.execution.TestingFailureInjectionConfig;
import io.trino.execution.TestingFailureInjector;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfSystemProperty(named = "hoglake.test.uri", matches = ".+")
final class TestHoglakeLiveRetries
{
    @Test
    void testTaskRetries()
            throws Exception
    {
        verifyRetries("TASK");
    }

    @Test
    void testQueryRetries()
            throws Exception
    {
        verifyRetries("QUERY");
    }

    private static void verifyRetries(String policy)
            throws Exception
    {
        String uri = System.getProperty("hoglake.test.uri");
        String catalog = "trino_retry_" + UUID.randomUUID().toString().replace("-", "");
        String dataPath = System.getProperty("hoglake.test.data-path") + catalog + "/";
        try (HttpClient http = HttpClient.newHttpClient()) {
            post(http, uri + "/v1/catalogs", Map.of("name", catalog, "data_path", dataPath));
            post(http, uri + "/v1/catalogs/" + catalog + "/namespaces", Map.of("name", "test"));
        }
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("hoglake").setSchema("test").build())
                .setWorkerCount(2)
                .setExtraProperties(Map.of(
                        "node-scheduler.include-coordinator", "false",
                        "retry-policy", policy,
                        "query-retry-attempts", "1",
                        "retry-initial-delay", "0s",
                        "fault-tolerant-execution-max-partition-count", "4"))
                .setAdditionalModule(binder -> {
                    configBinder(binder).bindConfig(TestingFailureInjectionConfig.class);
                    newOptionalBinder(binder, FailureInjector.class).setBinding().to(TestingFailureInjector.class).in(Scopes.SINGLETON);
                })
                .withExchange("filesystem", Map.of("exchange.base-directories", System.getProperty("java.io.tmpdir") + "/" + catalog))
                .build()) {
            runner.installPlugin(new HoglakePlugin());
            runner.createCatalog("hoglake", "hoglake", Map.of(
                    "hoglake.uri", uri,
                    "hoglake.catalog", catalog,
                    "s3.endpoint", System.getProperty("hoglake.test.s3-endpoint"),
                    "s3.region", "us-east-1",
                    "s3.path-style-access", "true",
                    "s3.aws-access-key", "synthetic-test",
                    "s3.aws-secret-key", "synthetic-test-password"));
            runner.execute("CREATE TABLE source AS SELECT CAST(id AS bigint) id FROM UNNEST(sequence(1, 10000)) t(id)");
            executeWithFailure(runner, "CREATE TABLE target WITH (partitioning=ARRAY['bucket(id, 4)'], sorted_by=ARRAY['id']) AS SELECT id FROM source");
            assertThat(runner.execute("SELECT count(*), sum(id) FROM target").getMaterializedRows())
                    .isEqualTo(runner.execute("VALUES (BIGINT '10000', BIGINT '50005000')").getMaterializedRows());
            executeWithFailure(runner, "INSERT INTO target SELECT id FROM source");
            assertThat(runner.execute("SELECT count(*), sum(id) FROM target").getMaterializedRows())
                    .isEqualTo(runner.execute("VALUES (BIGINT '20000', BIGINT '100010000')").getMaterializedRows());
            executeWithFailure(runner, "UPDATE target SET id=id+10000 WHERE id<=5000");
            assertThat(runner.execute("SELECT count(*), sum(id) FROM target").getMaterializedRows())
                    .isEqualTo(runner.execute("VALUES (BIGINT '20000', BIGINT '200010000')").getMaterializedRows());
            executeWithFailure(runner, "DELETE FROM target WHERE id>10000");
            assertThat(runner.execute("SELECT count(*) FROM target").getOnlyValue()).isEqualTo(10000L);
            executeWithFailure(runner, "MERGE INTO target t USING source s ON t.id=s.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)");
            assertThat(runner.execute("SELECT count(*), sum(id) FROM target").getMaterializedRows())
                    .isEqualTo(runner.execute("VALUES (BIGINT '5000', BIGINT '12502500')").getMaterializedRows());
        }
    }

    private static void executeWithFailure(DistributedQueryRunner runner, String sql)
    {
        String token = UUID.randomUUID().toString();
        // Stage zero is the coordinator finish stage. Fail the distributed stage
        // on its first attempt, leaving the coordinator and planned write handle alive.
        runner.injectTaskFailure(token, 1, 0, 0, TASK_FAILURE, Optional.of(INTERNAL_ERROR));
        Session session = Session.builder(runner.getDefaultSession()).setTraceToken(Optional.of(token)).build();
        var result = runner.execute(session, sql);
        assertThat(failedTasks(result.getStatementStats().orElseThrow().getRootStage())).isGreaterThan(0);
    }

    private static int failedTasks(StageStats stage)
    {
        return stage.getFailedTasks() + stage.getSubStages().stream().mapToInt(TestHoglakeLiveRetries::failedTasks).sum();
    }

    private static void post(HttpClient http, String uri, Object body)
            throws Exception
    {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(uri))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new ObjectMapper().writeValueAsBytes(body)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).describedAs(response.body()).isIn(200, 201);
    }
}
