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

import com.sun.net.httpserver.HttpServer;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.variant.Variant;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hoglake.TestHoglakeShreddedVariant.METADATA;
import static io.trino.plugin.hoglake.TestHoglakeShreddedVariant.row;
import static io.trino.plugin.hoglake.TestHoglakeShreddedVariant.typedValue;
import static io.trino.plugin.hoglake.TestHoglakeShreddedVariant.write;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Pushdown of subscripts on a shredded VARIANT column, through the engine's
 * planner and a catalog served over HTTP.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeVariantPushdown
{
    private static final String EVENTS_PATH = "memory:///variant-pushdown-events.parquet";
    private static final String MIXED_PATH = "memory:///variant-pushdown-mixed.parquet";

    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        // v has field id 1, and its typed_value shreds "a" as a bigint and "$Browser" as a string
        byte[] events = write(
                List.of(new FileColumn(optional(INT64).id(2).named("id"), BIGINT, Arrays.asList(0L, 1L, 2L))),
                Arrays.asList(
                        row(METADATA, null, typedValue(Variant.ofLong(1), "Chrome")),
                        row(METADATA, null, typedValue(Variant.ofString("one"), null)),
                        null),
                Optional.empty());
        // The second row is not an object, so a subscript fails on it
        byte[] mixed = write(
                List.of(new FileColumn(optional(INT64).id(2).named("id"), BIGINT, Arrays.asList(0L, 1L))),
                List.of(
                        row(METADATA, null, typedValue(Variant.ofLong(1), "Chrome")),
                        row(METADATA, Variant.ofInt(42).data(), null)),
                Optional.empty());
        TrinoFileSystemFactory storage = ConnectorTestFixtures.memoryFileSystem(Map.of(EVENTS_PATH, events, MIXED_PATH, mixed));
        HoglakePageSourceProvider pageSources = new HoglakePageSourceProvider(storage);

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body;
            if (path.equals("/v1/catalogs/lake")) {
                body = "{\"name\":\"lake\", \"head_snapshot_id\":7, \"schema_version\":1}";
            }
            else if (path.endsWith("/scan")) {
                String file = path.contains("/mixed/") ? MIXED_PATH : EVENTS_PATH;
                int size = path.contains("/mixed/") ? mixed.length : events.length;
                int records = path.contains("/mixed/") ? 2 : 3;
                body =
                        """
                        [{"data_file":{"data_file_id":1, "path":"%s", "record_count":%d, "file_size_bytes":%d,
                         "file_format":"parquet", "begin_snapshot":1}}]
                        """.formatted(file, records, size);
            }
            else {
                String table = path.substring(path.lastIndexOf('/') + 1);
                // Table "collide" reads the events file, with a column named like a pruned column
                String idName = table.equals("collide") ? "v_pruned" : "id";
                body =
                        """
                        {"name":"%s", "namespace":"test", "table_uuid":"synthetic-%s",
                         "columns":[{"field_id":2, "ordinal":0, "name":"%s", "type":"long", "nullable":true},
                                    {"field_id":1, "ordinal":1, "name":"v", "type":"variant", "nullable":true}],
                         "record_count":0, "file_count":0, "file_size_bytes":0}
                        """.formatted(table, table, idName);
            }
            byte[] bytes = body.getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake");
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("test").build());
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return List.of(new ConnectorFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "hoglake_variant_pushdown_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new Connector()
                        {
                            @Override
                            public void shutdown() {}

                            @Override
                            public List<PropertyMetadata<?>> getSessionProperties()
                            {
                                return new HoglakeSessionProperties(new HoglakeConfig()).getSessionProperties();
                            }

                            @Override
                            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
                            {
                                return HoglakeTransactionHandle.INSTANCE;
                            }

                            @Override
                            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
                            {
                                return new HoglakeMetadata(client);
                            }

                            @Override
                            public ConnectorSplitManager getSplitManager()
                            {
                                return new HoglakeSplitManager(client, _ -> HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE);
                            }

                            @Override
                            public ConnectorPageSourceProvider getPageSourceProvider()
                            {
                                return pageSources;
                            }
                        };
                    }
                });
            }
        });
        queryRunner.createCatalog("hoglake", "hoglake_variant_pushdown_test", Map.of());
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

    @Test
    void subscriptsArePushedDown()
    {
        String query = "SELECT id, CAST(v['a'] AS varchar), CAST(v['$Browser'] AS varchar), v['missing'] IS NULL FROM events";
        assertThat(explain(query)).contains("v:variant pruned to [").contains("['a']").contains("['$Browser']").contains("['missing']");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of(0L, "1", "Chrome", true),
                Arrays.asList(1L, "one", null, true),
                Arrays.asList(2L, null, null, true));
    }

    @Test
    void nestedSubscriptsArePushedDown()
    {
        String query = "SELECT CAST(v['a']['b'] AS varchar), CAST(v['a'][1] AS varchar) FROM events";
        assertThat(explain(query)).contains("v:variant pruned to [").contains("['a']['b']").contains("['a'][*]");
    }

    @Test
    void columnUsedWholeIsNotPruned()
    {
        String query = "SELECT CAST(v['a'] AS varchar), v FROM events";
        assertThat(explain(query))
                .contains("v:variant")
                .doesNotContain("pruned to");
    }

    @Test
    void subscriptsAbovePredicateArePushedDown()
    {
        // The connector keeps the filter for the engine, between the projection and the table scan
        String query = "SELECT CAST(v['a'] AS varchar) FROM events WHERE id > 0";
        assertThat(explain(query)).contains("v:variant pruned to [['a']]");
        assertThat(rows(query)).containsExactlyInAnyOrder(List.of("one"), Arrays.asList((Object) null));
    }

    @Test
    void subscriptsInPredicateArePushedDown()
    {
        String query = "SELECT id FROM events WHERE CAST(v['$Browser'] AS varchar) = 'Chrome'";
        assertThat(explain(query)).contains("v:variant pruned to [['$Browser']]");
        assertThat(rows(query)).containsExactly(List.of(0L));

        query = "SELECT CAST(v['a'] AS varchar) FROM events WHERE CAST(v['$Browser'] AS varchar) = 'Chrome'";
        assertThat(explain(query)).contains("pruned to").contains("['a']").contains("['$Browser']");
        assertThat(rows(query)).containsExactly(List.of("1"));
    }

    @Test
    void subscriptIsNotEvaluatedOnFilteredRows()
    {
        // The subscript fails on the second row, which the filter removes
        String query = "SELECT CAST(v['a'] AS varchar) FROM mixed WHERE id = 0";
        assertThat(explain(query)).contains("pruned to");
        assertThat(rows(query)).containsExactly(List.of("1"));
    }

    @Test
    void predicateWithLocalVariableOfEngine()
    {
        // The engine binds the non-trivial operand of BETWEEN SYMMETRIC to a local variable,
        // which is not offered to the connector
        String query = "SELECT CAST(v['a'] AS varchar) FROM events WHERE (id + 1) BETWEEN SYMMETRIC id AND 1";
        assertThat(explain(query)).contains("v:variant pruned to [['a']]");
        assertThat(rows(query)).containsExactly(List.of("1"));
    }

    @Test
    void sampledSubscriptsArePushedDown()
    {
        // TABLESAMPLE BERNOULLI becomes a filter with random(), which stays in the engine
        assertThat(explain("SELECT CAST(v['a'] AS varchar) FROM events TABLESAMPLE BERNOULLI (50)")).contains("v:variant pruned to [['a']]");
    }

    @Test
    void columnUsedWholeInPredicateIsNotPruned()
    {
        String query = "SELECT CAST(v['a'] AS varchar) FROM events WHERE v IS NOT NULL";
        assertThat(explain(query)).doesNotContain("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(List.of("1"), List.of("one"));
    }

    @Test
    void subscriptErrorIsUnchanged()
    {
        String query = "SELECT v['a'] FROM mixed";
        assertThat(explain(query)).contains("pruned to");
        assertThatThrownBy(() -> queryRunner.execute(query))
                .hasMessageContaining("VARIANT value is int32, not an object");
    }

    @Test
    void prunedColumnNameIsFresh()
    {
        String query = "SELECT CAST(v['a'] AS varchar), v_pruned FROM collide";
        assertThat(explain(query)).contains("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of("1", 0L),
                List.of("one", 1L),
                Arrays.asList(null, 2L));
    }

    @Test
    void prunedColumnNameDoesNotCaptureLambdaArgument()
    {
        String query = "SELECT transform(ARRAY[7], v_pruned -> CAST(v['a'] AS varchar)) FROM events";
        assertThat(explain(query)).contains("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of(List.of("1")),
                List.of(List.of("one")),
                List.of(Arrays.asList((Object) null)));
    }

    @Test
    void columnCapturedByLambda()
    {
        // The engine offers the captured columns of a lambda that it does not translate
        String query = "SELECT CAST(v['a'] AS varchar), transform(ARRAY[1, 2], x -> IF(x > 1, id, -1)) FROM events";
        assertThat(explain(query)).contains("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of("1", List.of(-1L, 0L)),
                List.of("one", List.of(-1L, 1L)),
                Arrays.asList(null, List.of(-1L, 2L)));
    }

    @Test
    void prunedColumnCapturedByLambda()
    {
        // The lambda captures the whole column
        String query = "SELECT CAST(v['a'] AS varchar), transform(ARRAY[1, -1], x -> IF(x > 0, CAST(v['$Browser'] AS varchar), '-')) FROM events";
        assertThat(explain(query)).doesNotContain("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of("1", List.of("Chrome", "-")),
                Arrays.asList("one", Arrays.asList(null, "-")),
                Arrays.asList(null, Arrays.asList(null, "-")));
    }

    @Test
    void localVariableOfEngine()
    {
        // The engine binds the non-trivial operand of BETWEEN SYMMETRIC to a local variable
        String query = "SELECT CAST(v['a'] AS varchar), (id + 1) BETWEEN SYMMETRIC id AND 10 FROM events";
        assertThat(explain(query)).doesNotContain("pruned to");
        assertThat(rows(query)).containsExactlyInAnyOrder(
                List.of("1", true),
                List.of("one", true),
                Arrays.asList(null, true));
    }

    @Test
    void pathAssemblyGivesTheSameResults()
    {
        Session disabled = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty("hoglake", "variant_path_assembly_enabled", "false")
                .build();
        List<String> queries = List.of(
                "SELECT CAST(v['$Browser'] AS varchar) AS browser, count(*) FROM events WHERE CAST(v['$Browser'] AS varchar) IS NOT NULL GROUP BY 1",
                "SELECT id FROM events WHERE CAST(v['$Browser'] AS varchar) = 'Chrome'",
                "SELECT id, CAST(v['a'] AS varchar), v['missing'] IS NULL FROM events",
                "SELECT id, CAST(v['a'] AS varchar) FROM mixed WHERE id = 0",
                "SELECT id, v FROM events");
        for (String query : queries) {
            assertThat(rows(query)).as(query).containsExactlyInAnyOrderElementsOf(rows(disabled, query));
        }
        // The same errors, for the first row of each table
        for (Session session : List.of(queryRunner.getDefaultSession(), disabled)) {
            assertThatThrownBy(() -> queryRunner.execute(session, "SELECT v['a'] FROM mixed"))
                    .hasMessageContaining("VARIANT value is int32, not an object");
            assertThatThrownBy(() -> queryRunner.execute(session, "SELECT v['a']['b'] FROM events WHERE id = 0"))
                    .hasMessageContaining("VARIANT value is int64, not an object");
        }
    }

    @Test
    void countIsNotAffected()
    {
        assertThat(rows("SELECT count(*) FROM events")).containsExactly(List.of(3L));
    }

    private String explain(String query)
    {
        return (String) queryRunner.execute("EXPLAIN " + query).getOnlyValue();
    }

    private List<List<Object>> rows(String query)
    {
        return rows(queryRunner.getDefaultSession(), query);
    }

    private List<List<Object>> rows(Session session, String query)
    {
        return queryRunner.execute(session, query).getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .toList();
    }
}
