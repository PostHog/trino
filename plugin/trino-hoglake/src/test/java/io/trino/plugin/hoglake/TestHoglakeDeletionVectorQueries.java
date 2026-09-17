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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * The engine-level contract of deletion-vector reads: aggregates, filters,
 * projections, and COUNT(*) over a table whose file has a live deletion
 * vector, with the plan coming from a real /scan response and the rows read
 * through the real page-source path.
 *
 * <p>The assertions compare returned values, not row counts: a mask applied
 * to the wrong positions can keep the count right and the values wrong.
 */
@TestInstance(PER_CLASS)
final class TestHoglakeDeletionVectorQueries
{
    private static final String DATA_PATH = "memory:///queries/data.parquet";
    private static final String DV_PATH = "memory:///queries/data.dv";
    private static final String CORRUPT_DV_PATH = "memory:///queries/corrupt.dv";
    private static final long[] VALUES = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};
    // File rows 1, 5, and 9 (values 10, 50, and 90) are deleted.
    private static final long[] DELETED = {1, 5, 9};

    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner queryRunner;
    private byte[] dataFile;

    @BeforeAll
    void setUp()
            throws IOException
    {
        dataFile = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                optional(INT64).id(1).named("value"),
                BIGINT,
                boxed(VALUES))));
        byte[] deletionVector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, DELETED);
        byte[] corruptVector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, DELETED);
        // Offset 4 is the blob's length prefix, +4 the magic, so offset 20 is
        // inside the checksummed bitmap.
        corruptVector[20] = (byte) (corruptVector[20] ^ 0x40);
        TrinoFileSystemFactory storage = ConnectorTestFixtures.memoryFileSystem(
                Map.of(DATA_PATH, dataFile, DV_PATH, deletionVector, CORRUPT_DV_PATH, corruptVector));
        HoglakePageSourceProvider pageSources = new HoglakePageSourceProvider(storage);

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body;
            if (path.equals("/v1/catalogs/lake")) {
                body =
                        """
                        {"name":"lake", "head_snapshot_id":7, "schema_version":1}
                        """;
            }
            else if (!"snapshot=7".equals(exchange.getRequestURI().getQuery())) {
                // A pinned-snapshot read must ask for the handle's snapshot;
                // anything else would be reading a different snapshot's vector.
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }
            else if (path.endsWith("/scan")) {
                boolean corrupt = path.contains("/corrupt/");
                body =
                        """
                        [{"data_file":{"data_file_id":1, "path":"%s", "record_count":%d, "file_size_bytes":%d,
                                       "file_format":"parquet", "begin_snapshot":1},
                          "delete_file":{"delete_file_id":1, "data_file_id":1, "path":"%s", "file_format":"puffin-dv",
                                         "delete_count":%d, "file_size_bytes":%d, "begin_snapshot":6}}]
                        """.formatted(
                        DATA_PATH,
                        VALUES.length,
                        dataFile.length,
                        corrupt ? CORRUPT_DV_PATH : DV_PATH,
                        DELETED.length,
                        corruptVector.length);
            }
            else {
                String table = path.contains("/corrupt/") ? "corrupt" : "queries";
                body =
                        """
                        {"name":"%s", "namespace":"test", "table_uuid":"synthetic-table",
                         "columns":[{"field_id":1, "ordinal":0, "name":"value", "type":"long", "nullable":true}],
                         "record_count":0, "file_count":0, "file_size_bytes":0}
                        """.formatted(table);
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
                        return "hoglake_dv_query_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new Connector()
                        {
                            @Override
                            public void shutdown() {}

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
                                return new HoglakeSplitManager(client);
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
        queryRunner.createCatalog("hoglake", "hoglake_dv_query_test", Map.of());
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
    void countsOnlyVisibleRows()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM queries").getOnlyValue()).isEqualTo(7L);
    }

    @Test
    void returnsSurvivingValues()
    {
        assertThat(values(queryRunner.execute("SELECT value FROM queries ORDER BY value").getMaterializedRows()))
                .containsExactly(0L, 20L, 30L, 40L, 60L, 70L, 80L);
    }

    @Test
    void filtersNeverSeeDeletedRows()
    {
        // value 50 is deleted, so it must not come back through the filter
        // that selects it; 20 and 80 survive.
        assertThat(values(queryRunner.execute("SELECT value FROM queries WHERE value IN (20, 50, 80) ORDER BY value").getMaterializedRows()))
                .containsExactly(20L, 80L);
        assertThat(queryRunner.execute("SELECT count(*) FROM queries WHERE value = 50").getOnlyValue()).isEqualTo(0L);
    }

    @Test
    void aggregatesUseVisibleRows()
    {
        assertThat(queryRunner.execute("SELECT sum(value), min(value), max(value) FROM queries").getMaterializedRows())
                .containsExactly(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, 300L, 0L, 80L));
    }

    @Test
    void projectionOfAnUnrelatedExpressionStillDropsDeletedRows()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM (SELECT value + 1 FROM queries)").getOnlyValue())
                .isEqualTo(7L);
    }

    @Test
    void corruptDeletionVectorFailsTheQueryWithItsOwnCode()
    {
        // The server rewrites the scan response with a corrupt vector for the
        // 'corrupt' table, so the failure comes from a real query plan.
        // The engine wraps connector failures in QueryFailedException; the
        // connector's own error code is pinned by the unit-level tests.
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM corrupt"))
                .hasMessageContaining(CORRUPT_DV_PATH)
                .hasMessageContaining("CRC mismatch");
    }

    private static List<Long> values(List<MaterializedRow> rows)
    {
        List<Long> values = new ArrayList<>(rows.size());
        for (MaterializedRow row : rows) {
            values.add((Long) row.getField(0));
        }
        return values;
    }

    private static List<Object> boxed(long[] values)
    {
        List<Object> boxed = new ArrayList<>(values.length);
        for (long value : values) {
            boxed.add(value);
        }
        return boxed;
    }
}
