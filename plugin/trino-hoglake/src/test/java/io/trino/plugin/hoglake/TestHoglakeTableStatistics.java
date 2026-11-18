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

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.Session;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The optimizer's statistics for a Hoglake table: the row count the catalog reports
 * exactly at the snapshot the handle pinned, taken from the read that resolved the
 * handle, and unknown whenever the catalog does not report one for that snapshot.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Tests mutate the shared HTTP server responses.
final class TestHoglakeTableStatistics
{
    private static final String CATALOG_PATH = "/v1/catalogs/lake";
    private static final String TABLE_PATH = CATALOG_PATH + "/namespaces/analytics/tables/events";
    private static final SchemaTableName EVENTS = new SchemaTableName("analytics", "events");

    private final Map<String, String> responses = new ConcurrentHashMap<>();
    private final List<String> requests = new CopyOnWriteArrayList<>();
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
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("analytics").build());
        queryRunner.installPlugin(new HoglakePlugin());
        queryRunner.createCatalog("hoglake", "hoglake", ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", "lake")
                // Storage is never reached: statistics come from the catalog
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
        responses.clear();
        requests.clear();
        responses.put(CATALOG_PATH, catalog(4));
        // A head read's totals are a sample of an older snapshot, never what a query reads
        responses.put(TABLE_PATH, table(", \"record_count\":900, \"file_count\":2, \"file_size_bytes\":2048, \"totals_snapshot_id\":2, \"read_snapshot_id\":4"));
        responses.put(TABLE_PATH + "?snapshot=4", table(exactTotals(1200, 4)));
    }

    @Test
    void testRowCountIsTheCatalogsTotalAtThePinnedSnapshot()
    {
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        HoglakeTableHandle handle = (HoglakeTableHandle) metadata.getTableHandle(session(), EVENTS, Optional.empty(), Optional.empty());
        assertThat(handle.snapshotId()).isEqualTo(4);
        assertThat(requests).containsExactly(CATALOG_PATH, TABLE_PATH + "?snapshot=4");

        // A commit moves head on: the statistics still describe the pinned snapshot,
        // from the read that resolved the handle, without another request
        responses.put(CATALOG_PATH, catalog(9));
        responses.put(TABLE_PATH + "?snapshot=9", table(exactTotals(5000, 9)));
        requests.clear();
        TableStatistics statistics = metadata.getTableStatistics(session(), handle);
        assertThat(statistics.getRowCount()).isEqualTo(Estimate.of(1200));
        assertThat(statistics.getColumnStatistics()).isEmpty();
        assertThat(requests).isEmpty();
        // So do the handles that pushdown derives from it
        assertThat(metadata.getTableStatistics(session(), handle.withCountOnly()).getRowCount()).isEqualTo(Estimate.of(1200));

        HoglakeTableHandle next = (HoglakeTableHandle) metadata.getTableHandle(session(), EVENTS, Optional.empty(), Optional.empty());
        assertThat(next.snapshotId()).isEqualTo(9);
        assertThat(metadata.getTableStatistics(session(), next).getRowCount()).isEqualTo(Estimate.of(5000));
    }

    @Test
    void testEmptyTableHasZeroRows()
    {
        responses.put(TABLE_PATH + "?snapshot=4", table(exactTotals(0, 4)));
        assertThat(statistics(session()).getRowCount()).isEqualTo(Estimate.of(0));
    }

    /**
     * No total, a sampled total, or a total for another snapshot is no answer for
     * the pinned snapshot. A mount, and a remote table, leave the totals out.
     */
    @Test
    void testUnknownWithoutAnExactTotalForThePinnedSnapshot()
    {
        responses.put(TABLE_PATH + "?snapshot=4", table(", \"read_snapshot_id\":4"));
        assertThat(statistics(session())).isEqualTo(TableStatistics.empty());

        // Neither totals nor read_snapshot_id
        responses.put(TABLE_PATH + "?snapshot=4", table(""));
        assertThat(statistics(session())).isEqualTo(TableStatistics.empty());

        responses.put(TABLE_PATH + "?snapshot=4", table(", \"record_count\":1200, \"file_count\":3, \"file_size_bytes\":4096, \"totals_snapshot_id\":3, \"read_snapshot_id\":4"));
        assertThat(statistics(session())).isEqualTo(TableStatistics.empty());

        responses.put(TABLE_PATH + "?snapshot=4", table(exactTotals(1200, 3)));
        assertThat(statistics(session())).isEqualTo(TableStatistics.empty());
    }

    @Test
    void testServerWithoutReadSnapshotId()
    {
        responses.put(TABLE_PATH + "?snapshot=4", table(", \"record_count\":1200, \"file_count\":3, \"file_size_bytes\":4096"));
        assertThat(statistics(session()).getRowCount()).isEqualTo(Estimate.of(1200));
    }

    @Test
    void testDisabledBySessionProperty()
    {
        assertThat(statistics(ConnectorTestFixtures.session(Map.of("statistics_enabled", false)))).isEqualTo(TableStatistics.empty());
    }

    @Test
    void testStagedRowsAreCounted()
    {
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        HoglakeTableHandle handle = (HoglakeTableHandle) metadata.getTableHandle(session(), EVENTS, Optional.empty(), Optional.empty());
        HoglakeDtos.ScanFile staged = new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(-1, "memory:///staged.parquet", "parquet", 30, 100, 10L, 0, "pending", 4), null);
        assertThat(metadata.getTableStatistics(session(), handle.withStaged(List.of(staged), List.of())).getRowCount()).isEqualTo(Estimate.of(1230));
    }

    @Test
    void testShowStats()
    {
        assertThat(rowCount(queryRunner.getDefaultSession())).isEqualTo(1200.0);
        assertThat(requests).doesNotContain(TABLE_PATH);

        Session disabled = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty("hoglake", "statistics_enabled", "false")
                .build();
        assertThat(rowCount(disabled)).isNull();

        responses.put(TABLE_PATH + "?snapshot=4", table(", \"read_snapshot_id\":4"));
        assertThat(rowCount(queryRunner.getDefaultSession())).isNull();
    }

    private Object rowCount(Session session)
    {
        // The summary row has no column name; row_count is the fifth column
        MaterializedRow summary = queryRunner.execute(session, "SHOW STATS FOR events").getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .collect(onlyElement());
        return summary.getField(4);
    }

    private TableStatistics statistics(ConnectorSession session)
    {
        HoglakeMetadata metadata = new HoglakeMetadata(client);
        return metadata.getTableStatistics(session, metadata.getTableHandle(session, EVENTS, Optional.empty(), Optional.empty()));
    }

    private static ConnectorSession session()
    {
        return ConnectorTestFixtures.session();
    }

    private static String catalog(long head)
    {
        return "{\"name\":\"lake\", \"head_snapshot_id\":%d, \"schema_version\":1}".formatted(head);
    }

    private static String exactTotals(long recordCount, long readSnapshot)
    {
        return ", \"record_count\":%d, \"file_count\":3, \"file_size_bytes\":4096, \"read_snapshot_id\":%d".formatted(recordCount, readSnapshot);
    }

    private static String table(String totals)
    {
        return """
               {"name":"events", "namespace":"analytics", "table_uuid":"synthetic-events",
                "columns":[{"field_id":1, "ordinal":0, "name":"team_id", "type":"long", "nullable":true},
                           {"field_id":2, "ordinal":1, "name":"event", "type":"string", "nullable":true}]%s}
               """.formatted(totals);
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        URI uri = exchange.getRequestURI();
        String key = uri.getPath() + (uri.getQuery() == null ? "" : "?" + uri.getQuery());
        requests.add(key);
        String body = responses.get(key);
        byte[] payload = (body == null ? "{\"error\":\"not_found\"}" : body).getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(body == null ? 404 : 200, payload.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }
}
