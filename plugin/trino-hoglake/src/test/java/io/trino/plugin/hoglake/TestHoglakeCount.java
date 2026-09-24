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
import io.airlift.units.DataSize;
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
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeCount
{
    private static final String FIRST_FILE_PATH = "memory:///counts-first.parquet";
    private static final String SECOND_FILE_PATH = "memory:///counts-second.parquet";
    // Registered in the catalog but absent from storage: a query that reads
    // it fails, so a passing query proves the file was pruned at planning.
    private static final String UNREADABLE_FILE_PATH = "memory:///pruned-never-read.parquet";

    private final AtomicBoolean storageAllowed = new AtomicBoolean();
    private final Map<String, String> scanQueries = new ConcurrentHashMap<>();
    private HttpServer server;
    private HoglakeClient client;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        byte[] parquet = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                optional(INT64).id(1).named("value"), BIGINT, Arrays.asList(1L, 1L, 2L, null))));
        TrinoFileSystemFactory storage = ConnectorTestFixtures.memoryFileSystem(Map.of(FIRST_FILE_PATH, parquet, SECOND_FILE_PATH, parquet));
        HoglakePageSourceProvider pageSources = new HoglakePageSourceProvider(identity -> {
            if (!storageAllowed.get()) {
                throw new AssertionError("Object storage must not be accessed for catalog counts");
            }
            return storage.create(identity);
        });
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
            else if (!exchange.getRequestURI().getQuery().split("&")[0].equals("snapshot=7")) {
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }
            else if (path.endsWith("/scan")) {
                String query = exchange.getRequestURI().getQuery();
                scanQueries.put(path, query);
                boolean withStats = query.contains("include=column_stats");
                if (path.contains("/empty/")) {
                    body = "[]";
                }
                else if (path.contains("/bounded/") || path.contains("/mixed/")) {
                    // A provided file holding [1, 2] (and a null), a provided
                    // file holding [100, 200] that storage does not have, and
                    // in `mixed` a pending file with no statistics at all.
                    String bounded =
                            """
                            {"data_file":{"data_file_id":%d, "path":"%s", "record_count":4, "file_size_bytes":%d,
                             "file_format":"parquet", "stats_state":"provided", "begin_snapshot":1%s}}
                            """;
                    String first = bounded.formatted(1, FIRST_FILE_PATH, parquet.length, withStats
                            ? ", \"column_stats\":[{\"field_id\":1, \"value_count\":4, \"null_count\":1, \"lower_bound\":1, \"upper_bound\":2}]"
                            : "");
                    String unreadable = bounded.formatted(3, UNREADABLE_FILE_PATH, parquet.length, withStats
                            ? ", \"column_stats\":[{\"field_id\":1, \"value_count\":4, \"null_count\":0, \"lower_bound\":100, \"upper_bound\":200}]"
                            : "");
                    String pending =
                            """
                            {"data_file":{"data_file_id":2, "path":"%s", "record_count":4, "file_size_bytes":%d,
                             "file_format":"parquet", "stats_state":"pending", "begin_snapshot":1}}
                            """.formatted(SECOND_FILE_PATH, parquet.length);
                    body = path.contains("/mixed/")
                            ? "[" + first + "," + unreadable + "," + pending + "]"
                            : "[" + first + "," + unreadable + "]";
                }
                else {
                    // Two catalog files, each containing four rows, including a null.
                    String file =
                            """
                            {"data_file":{"data_file_id":%d, "path":"%s", "record_count":4, "file_size_bytes":%d,
                             "file_format":"parquet", "begin_snapshot":1}}
                            """;
                    body = "[" + file.formatted(1, FIRST_FILE_PATH, parquet.length) + "," + file.formatted(2, SECOND_FILE_PATH, parquet.length) + "]";
                }
            }
            else {
                body =
                        """
                        {"name":"counts", "namespace":"test", "table_uuid":"synthetic-table",
                         "columns":[{"field_id":1, "ordinal":0, "name":"value", "type":"long", "nullable":true}],
                         "record_count":0, "file_count":0, "file_size_bytes":0}
                        """;
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
                        return "hoglake_count_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        // A catalog configured with a split size smaller than the files plans byte ranges.
                        DataSize maxSplitSize = DataSize.valueOf(config.getOrDefault("max-split-size", HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE.toString()));
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
                                return new HoglakeSplitManager(client, _ -> maxSplitSize);
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
        queryRunner.createCatalog("hoglake", "hoglake_count_test", Map.of());
        queryRunner.createCatalog("hoglake_ranges", "hoglake_count_test", Map.of("max-split-size", "64B"));
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
    void testCountsWithoutObjectStorage()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM counts").getOnlyValue()).isEqualTo(8L);
        assertThat(queryRunner.execute("SELECT count(*) FROM empty").getOnlyValue()).isEqualTo(0L);
    }

    /**
     * The catalog's record count describes a whole file, and every file is
     * cut from offset 0: the file's first range answers an unfiltered count
     * from the catalog and its other ranges report no rows, so a count over
     * byte-range splits still touches no object storage. A count that needs
     * a column or a filter reads the files through every range.
     */
    @Test
    void testRangeSplitsCountFromTheCatalog()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM hoglake_ranges.test.counts").getOnlyValue()).isEqualTo(8L);
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(value) FROM hoglake_ranges.test.counts"))
                .hasStackTraceContaining("Object storage must not be accessed for catalog counts");
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM hoglake_ranges.test.counts WHERE value = 1"))
                .hasStackTraceContaining("Object storage must not be accessed for catalog counts");

        storageAllowed.set(true);
        try {
            assertThat(queryRunner.execute("SELECT count(value) FROM hoglake_ranges.test.counts").getOnlyValue()).isEqualTo(6L);
            assertThat(queryRunner.execute("SELECT count(*) FROM hoglake_ranges.test.counts WHERE value = 1").getOnlyValue()).isEqualTo(4L);
        }
        finally {
            storageAllowed.set(false);
        }
    }

    /**
     * A filtered read asks the catalog for the predicate's bounds and plans
     * no splits for files those bounds exclude. The excluded file is not in
     * storage, so any read of it would fail the query.
     */
    @Test
    void testFilesOutsideThePredicateArePrunedAtPlanning()
    {
        // Every file excluded: no split, so no object storage at all.
        assertThat(queryRunner.execute("SELECT count(*) FROM bounded WHERE value > 1000").getOnlyValue()).isEqualTo(0L);
        assertThat(scanQueries.get("/v1/catalogs/lake/namespaces/test/tables/bounded/scan"))
                .isEqualTo("snapshot=7&include=column_stats&stats_fields=1");
        assertThat(queryRunner.execute("SELECT count(*) FROM bounded WHERE value IS NULL AND value > 1000").getOnlyValue()).isEqualTo(0L);

        storageAllowed.set(true);
        try {
            // Only the file whose bounds cover the value is read.
            assertThat(queryRunner.execute("SELECT count(*) FROM bounded WHERE value = 1").getOnlyValue()).isEqualTo(2L);
            assertThat(queryRunner.execute("SELECT count(*) FROM bounded WHERE value BETWEEN 2 AND 50").getOnlyValue()).isEqualTo(1L);
            // A file without statistics is always read, beside a pruned one.
            assertThat(queryRunner.execute("SELECT count(*) FROM mixed WHERE value = 1").getOnlyValue()).isEqualTo(4L);
            assertThat(queryRunner.execute("SELECT count(*) FROM mixed WHERE value > 1000").getOnlyValue()).isEqualTo(0L);
        }
        finally {
            storageAllowed.set(false);
        }
        // A read with no prunable predicate requests no statistics.
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(value) FROM bounded"))
                .hasStackTraceContaining("Object storage must not be accessed for catalog counts");
        assertThat(scanQueries.get("/v1/catalogs/lake/namespaces/test/tables/bounded/scan")).isEqualTo("snapshot=7");
    }

    /**
     * An unfiltered count(*) projects no column: the engine hands the
     * connector an empty projection, the handle becomes count-only, and
     * planning gives each file one whole-file split however small the split
     * size. A count that needs a column or a predicate keeps byte ranges.
     */
    @Test
    void testUnfilteredCountPlansOneWholeFileSplitPerFile()
            throws Exception
    {
        assertThat(explain("SELECT count(*) FROM hoglake_ranges.test.counts")).contains("counts@7 countOnly");
        assertThat(explain("SELECT count(value) FROM hoglake_ranges.test.counts")).doesNotContain("countOnly");
        assertThat(explain("SELECT count(*) FROM hoglake_ranges.test.counts WHERE value = 1")).doesNotContain("countOnly");
        assertThat(queryRunner.execute("SELECT count(*) FROM hoglake_ranges.test.counts").getOnlyValue()).isEqualTo(8L);

        HoglakeColumnHandle value = new HoglakeColumnHandle("value", 1, BIGINT, true);
        HoglakeTableHandle table = new HoglakeTableHandle("test", "counts", 7, "synthetic-table", List.of(value));
        HoglakeSplitManager splitManager = new HoglakeSplitManager(client, _ -> DataSize.ofBytes(64));

        List<HoglakeSplit> countOnly = splits(splitManager, table.withCountOnly());
        assertThat(countOnly).hasSize(2);
        assertThat(countOnly).allSatisfy(split -> {
            assertThat(split.start()).isZero();
            assertThat(split.length()).isEqualTo(split.fileSizeBytes());
        });
        assertThat(countOnly).extracting(HoglakeSplit::path).containsExactly(FIRST_FILE_PATH, SECOND_FILE_PATH);

        // Byte ranges otherwise, and for a count-only handle with a predicate.
        assertThat(splits(splitManager, table)).hasSizeGreaterThan(2);
        assertThat(splits(splitManager, table.withConstraint(TupleDomain.withColumnDomains(Map.of(value, Domain.singleValue(BIGINT, 1L)))).withCountOnly()))
                .hasSizeGreaterThan(2);
    }

    private String explain(String query)
    {
        return (String) queryRunner.execute("EXPLAIN " + query).getOnlyValue();
    }

    private static List<HoglakeSplit> splits(HoglakeSplitManager splitManager, HoglakeTableHandle table)
            throws Exception
    {
        ConnectorSplitSource source = splitManager.getSplits(HoglakeTransactionHandle.INSTANCE, ConnectorTestFixtures.session(), table, Set.of(), Constraint.alwaysTrue());
        return source.getNextBatch(1_000, DynamicFilterSnapshot.EMPTY).get().stream()
                .map(HoglakeSplit.class::cast)
                .toList();
    }

    @Test
    void testColumnDependentCounts()
    {
        storageAllowed.set(true);
        try {
            assertThat(queryRunner.execute("SELECT count(*) FROM counts WHERE value = 1").getOnlyValue()).isEqualTo(4L);
            assertThat(queryRunner.execute("SELECT count(*) FROM counts WHERE value > 10").getOnlyValue()).isEqualTo(0L);
            assertThat(queryRunner.execute("SELECT count(value) FROM counts").getOnlyValue()).isEqualTo(6L);
            assertThat(queryRunner.execute("SELECT count(DISTINCT value) FROM counts").getOnlyValue()).isEqualTo(2L);
            assertThat(queryRunner.execute("SELECT value, count(*) FROM counts GROUP BY value ORDER BY value NULLS LAST").getMaterializedRows())
                    .isEqualTo(queryRunner.execute("VALUES (BIGINT '1', BIGINT '4'), (BIGINT '2', BIGINT '2'), (CAST(NULL AS BIGINT), BIGINT '2')").getMaterializedRows());
        }
        finally {
            storageAllowed.set(false);
        }
    }
}
