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
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.plugin.hoglake.rest.HoglakeClient.REJECTED_INCLUDE_RETENTION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * REST-client parsing against openapi-shaped fixtures (snake_case wire,
 * exactly the response shapes of openapi/hoglake.yaml), served from an
 * in-JVM HttpServer.
 */
class TestHoglakeClient
{
    private static final Set<String> COLUMN_STATS_ONLY = ImmutableSet.of("column_stats");
    private static final Map<String, String> RESPONSES = new HashMap<>();
    private static final Map<String, String> QUERIES = new ConcurrentHashMap<>();
    private static HttpServer server;
    private static HoglakeClient client;

    @BeforeAll
    static void startServer()
            throws IOException
    {
        RESPONSES.put("/v1/catalogs/lake",
                """
                {"name": "lake", "data_path": "s3://lake/", "head_snapshot_id": 7, "schema_version": 3}
                """);
        RESPONSES.put("/v1/catalogs/lake/namespaces",
                """
                [{"name": "analytics"}, {"name": "raw"}]
                """);
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables",
                """
                [{"name": "events", "table_uuid": "0193c2b1-0000-7000-8000-000000000001"}]
                """);
        // A Table with every column shape: plain, decimal params, explicit
        // non-nullable, plus fields the client does not model (partition_spec).
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables/events",
                """
                {
                  "name": "events",
                  "namespace": "analytics",
                  "table_uuid": "0193c2b1-0000-7000-8000-000000000001",
                  "columns": [
                    {"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false},
                    {"field_id": 2, "ordinal": 1, "name": "amount", "type": "decimal",
                     "type_params": {"precision": 10, "scale": 2}},
                    {"field_id": 3, "ordinal": 2, "name": "name", "type": "string"}
                  ],
                  "record_count": 42,
                  "file_count": 2,
                  "file_size_bytes": 4096,
                  "partition_spec": {"spec_id": 1, "fields": [{"source_field_id": 1, "transform": "identity"}]}
                }
                """);
        // /scan: one bare file, one file paired with a deletion vector.
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables/events/scan",
                """
                [
                  {
                    "data_file": {
                      "data_file_id": 10, "path": "s3://lake/events/a.parquet",
                      "file_format": "parquet", "record_count": 25,
                      "file_size_bytes": 2048, "footer_size": 321,
                      "row_id_start": 0, "stats_state": "provided", "begin_snapshot": 3
                    }
                  },
                  {
                    "data_file": {
                      "data_file_id": 11, "path": "s3://lake/events/b.parquet",
                      "file_format": "parquet", "record_count": 17,
                      "file_size_bytes": 1024, "row_id_start": 25,
                      "stats_state": "pending", "begin_snapshot": 4
                    },
                    "delete_file": {
                      "delete_file_id": 5, "data_file_id": 11,
                      "path": "s3://lake/events/b.dv", "file_format": "puffin-dv",
                      "delete_count": 3, "file_size_bytes": 64, "begin_snapshot": 6
                    }
                  }
                ]
                """);

        // /scan with column statistics: absent (pending), empty (provided,
        // nothing for the requested columns) and present, with a long bound
        // past 2^53 that must arrive as its exact token.
        RESPONSES.put("/v1/catalogs/lake/namespaces/analytics/tables/stats/scan",
                """
                [
                  {"data_file": {"data_file_id": 20, "path": "s3://lake/stats/pending.parquet", "file_format": "parquet",
                    "record_count": 5, "file_size_bytes": 512, "row_id_start": 0, "stats_state": "pending", "begin_snapshot": 3}},
                  {"data_file": {"data_file_id": 21, "path": "s3://lake/stats/empty.parquet", "file_format": "parquet",
                    "record_count": 5, "file_size_bytes": 512, "row_id_start": 5, "stats_state": "provided", "begin_snapshot": 3,
                    "column_stats": []}},
                  {"data_file": {"data_file_id": 22, "path": "s3://lake/stats/full.parquet", "file_format": "parquet",
                    "record_count": 5, "file_size_bytes": 512, "row_id_start": 10, "stats_state": "provided", "begin_snapshot": 3,
                    "split_offsets": [4, 200],
                    "column_stats": [
                      {"field_id": 1, "value_count": 5, "null_count": 0, "lower_bound": 9007199254740993, "upper_bound": 9223372036854775807},
                      {"field_id": 7, "value_count": 5, "null_count": 5, "nan_count": 0, "lower_bound": null, "upper_bound": null}]}}
                ]
                """);

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String rawQuery = exchange.getRequestURI().getRawQuery();
            QUERIES.put(exchange.getRequestURI().getPath(), rawQuery == null ? "" : rawQuery);
            String body = RESPONSES.get(exchange.getRequestURI().getPath());
            byte[] payload = (body == null ? "{\"error\":\"not_found\"}" : body).getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(body == null ? 404 : 200, payload.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        });
        server.start();
        client = new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake");
    }

    @AfterAll
    static void stopServer()
    {
        server.stop(0);
    }

    @Test
    void parsesCatalogInfoWithHeadSnapshot()
    {
        HoglakeDtos.Catalog catalog = client.getCatalog();
        assertThat(catalog.name()).isEqualTo("lake");
        assertThat(catalog.headSnapshotId()).isEqualTo(7);
    }

    @Test
    void parsesNamespaces()
    {
        assertThat(client.listNamespaces())
                .extracting(HoglakeDtos.Namespace::name)
                .containsExactly("analytics", "raw");
    }

    @Test
    void parsesTableSummaries()
    {
        assertThat(client.listTables("analytics"))
                .extracting(HoglakeDtos.TableSummary::name)
                .containsExactly("events");
    }

    @Test
    void unknownNamespaceIsSchemaNotFound()
    {
        assertThatThrownBy(() -> client.listTables("nope"))
                .isInstanceOf(SchemaNotFoundException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void parsesTableWithColumnsAndDecimalParams()
    {
        HoglakeDtos.Table table = client.getTable("analytics", "events").orElseThrow();
        assertThat(table.namespace()).isEqualTo("analytics");
        assertThat(table.recordCount()).isEqualTo(42);
        assertThat(table.columns()).hasSize(3);

        HoglakeDtos.Column id = table.columns().get(0);
        assertThat(id.fieldId()).isEqualTo(1);
        assertThat(id.type()).isEqualTo("long");
        assertThat(id.isNullable()).isFalse();

        HoglakeDtos.Column amount = table.columns().get(1);
        assertThat(amount.type()).isEqualTo("decimal");
        assertThat(amount.typeParams()).containsEntry("precision", 10).containsEntry("scale", 2);
        assertThat(amount.isNullable()).isTrue();

        // Omitted nullable defaults to true (openapi: default true).
        assertThat(table.columns().get(2).isNullable()).isTrue();
    }

    @Test
    void missingTableIsEmpty()
    {
        assertThat(client.getTable("analytics", "nope")).isEmpty();
    }

    @Test
    void missingCatalogFailsLoudly()
    {
        HoglakeClient wrongCatalog =
                new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "puddle");
        assertThatThrownBy(wrongCatalog::listNamespaces)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("puddle");
    }

    @Test
    void parsesScanWithDataAndDeleteFilePairing()
    {
        List<HoglakeDtos.ScanFile> scan = client.scan("analytics", "events", 6);
        assertThat(scan).hasSize(2);

        HoglakeDtos.ScanFile bare = scan.get(0);
        assertThat(bare.dataFile().path()).isEqualTo("s3://lake/events/a.parquet");
        assertThat(bare.dataFile().recordCount()).isEqualTo(25);
        assertThat(bare.dataFile().fileSizeBytes()).isEqualTo(2048);
        assertThat(bare.deleteFile()).isNull();

        HoglakeDtos.ScanFile paired = scan.get(1);
        assertThat(paired.dataFile().statsState()).isEqualTo("pending");
        assertThat(paired.deleteFile()).isNotNull();
        assertThat(paired.deleteFile().path()).isEqualTo("s3://lake/events/b.dv");
        assertThat(paired.deleteFile().deleteCount()).isEqualTo(3);
    }

    @Test
    void planningScanRequestsOffsetsAndStatisticsOnlyForTheGivenFields()
    {
        String scanPath = "/v1/catalogs/lake/namespaces/analytics/tables/stats/scan";
        client.planningScan("analytics", "stats", 6, Set.of());
        assertThat(QUERIES.get(scanPath)).isEqualTo("snapshot=6&include=split_offsets");
        client.planningScan("analytics", "stats", 6, Set.of(7L, 1L));
        assertThat(QUERIES.get(scanPath)).isEqualTo("snapshot=6&include=column_stats,split_offsets&stats_fields=1,7");
    }

    @Test
    void writerScanRequestsNoOptionalParts()
    {
        String scanPath = "/v1/catalogs/lake/namespaces/analytics/tables/stats/scan";
        client.scan("analytics", "stats", 6);
        assertThat(QUERIES.get(scanPath)).isEqualTo("snapshot=6");
    }

    @Test
    void readPlanningAndWriterScansUseTheirOwnRequests()
    {
        String scanPath = "/v1/catalogs/lake/namespaces/analytics/tables/stats/scan";
        HoglakeTableHandle handle = new HoglakeTableHandle("analytics", "stats", 6, "uuid-stats", List.of());
        HoglakeSplitManager.prunedScan(client, handle);
        assertThat(QUERIES.get(scanPath)).isEqualTo("snapshot=6&include=split_offsets");
        // HoglakeDeletePublisher reads the table through this scan.
        HoglakeSplitManager.scan(client, handle);
        assertThat(QUERIES.get(scanPath)).isEqualTo("snapshot=6");
    }

    @Test
    void parsesAbsentEmptyAndPresentColumnStatistics()
            throws Exception
    {
        List<HoglakeDtos.ScanFile> scan = client.planningScan("analytics", "stats", 6, Set.of(1L, 7L));
        assertThat(scan).hasSize(3);
        assertThat(scan.get(0).dataFile().columnStats()).isNull();
        assertThat(scan.get(1).dataFile().columnStats()).isEmpty();
        assertThat(scan.get(0).dataFile().splitOffsets()).isEmpty();
        assertThat(scan.get(2).dataFile().splitOffsets()).containsExactly(4L, 200L);

        List<HoglakeDtos.ScanColumnStats> stats = scan.get(2).dataFile().columnStats();
        assertThat(stats).extracting(HoglakeDtos.ScanColumnStats::fieldId).containsExactly(1L, 7L);
        assertThat(stats.get(0).lowerBound().bigIntegerValue()).hasToString("9007199254740993");
        assertThat(stats.get(0).nanCount()).isNull();
        assertThat(stats.get(1).nullCount()).isEqualTo(stats.get(1).valueCount());
        assertThat(stats.get(1).nanCount()).isZero();
        assertThat(stats.get(1).lowerBound().isNull()).isTrue();

        // Staged files ride the table handle as JSON: absent and empty must
        // stay distinct through a round trip.
        ObjectMapper mapper = new ObjectMapper();
        for (HoglakeDtos.ScanFile file : scan) {
            HoglakeDtos.DataFile roundTripped = mapper.readValue(mapper.writeValueAsString(file.dataFile()), HoglakeDtos.DataFile.class);
            assertThat(roundTripped.columnStats()).isEqualTo(file.dataFile().columnStats());
        }
        assertThat(mapper.writeValueAsString(scan.get(0).dataFile())).doesNotContain("column_stats");
    }

    @Test
    void catalogWithoutSplitOffsetsIsPlannedWithColumnStatisticsOnly()
            throws IOException
    {
        try (IncludeValidatingCatalog catalog = new IncludeValidatingCatalog(COLUMN_STATS_ONLY)) {
            HoglakeClient client = catalog.client(new AtomicLong());
            List<HoglakeDtos.ScanFile> scan = client.planningScan("analytics", "stats", 6, Set.of(7L, 1L));
            assertThat(scan).extracting(file -> file.dataFile().path()).containsExactly("s3://lake/stats/file.parquet");
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=column_stats,split_offsets&stats_fields=1,7",
                    "snapshot=6&include=column_stats&stats_fields=1,7");
        }
    }

    @Test
    void catalogWithoutAnyIncludeIsPlannedWithAPlainScan()
            throws IOException
    {
        try (IncludeValidatingCatalog catalog = new IncludeValidatingCatalog(ImmutableSet.of())) {
            HoglakeClient client = catalog.client(new AtomicLong());
            assertThat(client.planningScan("analytics", "stats", 6, Set.of(7L, 1L))).hasSize(1);
            // The refusal names both values, and stats_fields goes with
            // column_stats: without it, stats_fields is a 422 of its own.
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=column_stats,split_offsets&stats_fields=1,7",
                    "snapshot=6");
        }
        try (IncludeValidatingCatalog catalog = new IncludeValidatingCatalog(ImmutableSet.of())) {
            HoglakeClient client = catalog.client(new AtomicLong());
            assertThat(client.planningScan("analytics", "stats", 6, Set.of())).hasSize(1);
            assertThat(client.planningScan("analytics", "stats", 6, Set.of(3L))).hasSize(1);
            // Each part is remembered on its own: the first scan learned
            // nothing about column_stats, so the second still asks for it.
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=split_offsets",
                    "snapshot=6",
                    "snapshot=6&include=column_stats&stats_fields=3",
                    "snapshot=6");
        }
    }

    @Test
    void rejectedIncludeIsNotRequestedAgainWithinTheRetention()
            throws IOException
    {
        try (IncludeValidatingCatalog catalog = new IncludeValidatingCatalog(COLUMN_STATS_ONLY)) {
            AtomicLong now = new AtomicLong();
            HoglakeClient client = catalog.client(now);
            client.planningScan("analytics", "stats", 6, Set.of(1L));
            catalog.clearQueries();

            now.addAndGet(REJECTED_INCLUDE_RETENTION.minusNanos(1).toNanos());
            client.planningScan("analytics", "stats", 6, Set.of(1L));
            client.planningScan("analytics", "stats", 6, Set.of());
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=column_stats&stats_fields=1",
                    "snapshot=6");
        }
    }

    @Test
    void rejectedIncludeIsRequestedAgainAfterTheRetention()
            throws IOException
    {
        try (IncludeValidatingCatalog catalog = new IncludeValidatingCatalog(COLUMN_STATS_ONLY)) {
            AtomicLong now = new AtomicLong();
            HoglakeClient client = catalog.client(now);
            client.planningScan("analytics", "stats", 6, Set.of());

            // Not upgraded yet: the probe is refused and the retention restarts.
            now.addAndGet(REJECTED_INCLUDE_RETENTION.toNanos());
            catalog.clearQueries();
            client.planningScan("analytics", "stats", 6, Set.of());
            now.addAndGet(REJECTED_INCLUDE_RETENTION.minusNanos(1).toNanos());
            client.planningScan("analytics", "stats", 6, Set.of());
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=split_offsets",
                    "snapshot=6",
                    "snapshot=6");

            // Upgraded: the next probe is served, and later scans keep asking.
            catalog.support(ImmutableSet.of("column_stats", "split_offsets"));
            now.addAndGet(1);
            catalog.clearQueries();
            List<HoglakeDtos.ScanFile> scan = client.planningScan("analytics", "stats", 6, Set.of());
            client.planningScan("analytics", "stats", 6, Set.of(1L));
            assertThat(scan.getFirst().dataFile().splitOffsets()).containsExactly(4L);
            assertThat(catalog.queries()).containsExactly(
                    "snapshot=6&include=split_offsets",
                    "snapshot=6&include=column_stats,split_offsets&stats_fields=1");
        }
    }

    @Test
    void scanOfMissingTableFailsLoudly()
    {
        assertThatThrownBy(() -> client.scan("analytics", "nope", 6))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessageContaining("analytics.nope");
    }

    /**
     * A catalog serving one table's scan and validating {@code include} as
     * Hoglake's scan route does: an unknown value is a 422 naming it, and
     * {@code stats_fields} without {@code column_stats} is a 422 of its own.
     */
    private static final class IncludeValidatingCatalog
            implements AutoCloseable
    {
        private final HttpServer server;
        private final List<String> queries = new CopyOnWriteArrayList<>();
        private volatile Set<String> supported;

        public IncludeValidatingCatalog(Set<String> supported)
                throws IOException
        {
            this.supported = ImmutableSet.copyOf(supported);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/catalogs/lake/namespaces/analytics/tables/stats/scan", exchange -> {
                String query = exchange.getRequestURI().getRawQuery();
                queries.add(query);
                Map<String, String> parameters = new HashMap<>();
                for (String parameter : query.split("&")) {
                    String[] parts = parameter.split("=", 2);
                    parameters.put(parts[0], parts[1]);
                }
                Set<String> includes = ImmutableSet.of();
                if (parameters.containsKey("include")) {
                    includes = ImmutableSet.copyOf(parameters.get("include").split(","));
                }
                Set<String> known = this.supported;
                List<String> unknown = includes.stream()
                        .filter(value -> !known.contains(value))
                        .sorted()
                        .toList();
                int status = 200;
                String splitOffsets = includes.contains("split_offsets") ? ", \"split_offsets\": [4]" : "";
                String body =
                        """
                        [{"data_file": {"data_file_id": 30, "path": "s3://lake/stats/file.parquet", "file_format": "parquet",
                          "record_count": 5, "file_size_bytes": 512, "row_id_start": 0, "stats_state": "provided", "begin_snapshot": 3%s}}]
                        """.formatted(splitOffsets);
                if (!unknown.isEmpty()) {
                    status = 422;
                    body =
                            """
                            {"error": "validation", "detail": "include: unknown value(s) %s; supported: %s"}
                            """.formatted(
                            unknown.stream().map(value -> "'" + value + "'").collect(joining(", ")),
                            known.stream().sorted().collect(joining(", ")));
                }
                else if (parameters.containsKey("stats_fields") && !includes.contains("column_stats")) {
                    status = 422;
                    body =
                            """
                            {"error": "validation", "detail": "stats_fields requires include=column_stats"}
                            """;
                }
                byte[] payload = body.getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, payload.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(payload);
                }
            });
            server.start();
        }

        public HoglakeClient client(AtomicLong now)
        {
            Ticker ticker = new Ticker()
            {
                @Override
                public long read()
                {
                    return now.get();
                }
            };
            return new HoglakeClient("http://127.0.0.1:" + server.getAddress().getPort(), "lake", Duration.ofSeconds(30), ticker);
        }

        public void support(Set<String> values)
        {
            supported = ImmutableSet.copyOf(values);
        }

        public List<String> queries()
        {
            return ImmutableList.copyOf(queries);
        }

        public void clearQueries()
        {
            queries.clear();
        }

        @Override
        public void close()
        {
            server.stop(0);
        }
    }
}
