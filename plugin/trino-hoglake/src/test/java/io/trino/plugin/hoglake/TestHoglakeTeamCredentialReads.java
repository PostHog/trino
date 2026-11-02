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
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE;
import static io.trino.plugin.hoglake.TestHoglakeTeamCredentials.assertErrorCode;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.TABLE_LOCATION;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.accessKey;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.secretKey;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.sessionToken;
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
 * Per-team reads end to end: splits travel to tasks as JSON, and the S3
 * filesystem talks to an in-process S3 that serves an object only to the
 * key of the team in its path. The catalog-wide key it is configured with is
 * a key S3 refuses, so any fallback would fail.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakeTeamCredentialReads
{
    private static final String T4_DATA = TABLE_LOCATION + "team_id=4/a.parquet";
    private static final String T4_DV = TABLE_LOCATION + "team_id=4/a.puffin";
    private static final String T42_DATA = TABLE_LOCATION + "team_id=42/b.parquet";
    private static final Pattern TEAM = Pattern.compile("/team_id=(\\d+)/");
    private static final Pattern CREDENTIAL = Pattern.compile("Credential=([^/]+)/");
    private static final HoglakeColumnHandle VALUE = new HoglakeColumnHandle("value", 1, BIGINT, true);
    private static final HoglakeTableHandle TABLE = new HoglakeTableHandle("test", "events", 7, "synthetic", List.of(VALUE));

    private final Map<String, byte[]> objects = new HashMap<>();
    // Method, object path and access key of every S3 request, and whether S3 served it.
    private final List<String> s3Requests = new CopyOnWriteArrayList<>();
    private final TestingHoglakeTeamCredentialSource source = new TestingHoglakeTeamCredentialSource();
    private HttpServer s3;
    private HttpServer rest;
    private HoglakeClient client;
    private S3FileSystemFactory fileSystemFactory;
    private HoglakePageSourceProvider pageSources;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        objects.put(T4_DATA, parquet(1L, 2L, 3L, 4L));
        objects.put(T4_DV, PuffinDeletionVectorFixtures.deletionVector(T4_DATA, 1));
        objects.put(T42_DATA, parquet(42L, 43L));
        s3 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s3.createContext("/", exchange -> {
            try (exchange) {
                String path = "s3:/" + exchange.getRequestURI().getPath();
                Matcher team = TEAM.matcher(path);
                Matcher key = CREDENTIAL.matcher(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
                String accessKey = key.find() ? key.group(1) : "none";
                boolean allowed = team.find() && accessKey.equals(accessKey(Long.parseLong(team.group(1))));
                s3Requests.add("%s %s %s %s".formatted(exchange.getRequestMethod(), path, accessKey, allowed ? "served" : "denied"));
                byte[] content = objects.get(path);
                if (!allowed || content == null) {
                    exchange.sendResponseHeaders(allowed ? 404 : 403, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Last-Modified", DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.parse("2026-01-01T00:00:00Z").atZone(ZoneOffset.UTC)));
                if (exchange.getRequestMethod().equals("HEAD")) {
                    exchange.getResponseHeaders().set("Content-Length", Integer.toString(content.length));
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                int start = 0;
                int end = content.length - 1;
                String range = exchange.getRequestHeaders().getFirst("Range");
                if (range != null) {
                    String[] offsets = range.substring("bytes=".length()).split("-", -1);
                    start = offsets[0].isEmpty() ? Math.max(0, content.length - Integer.parseInt(offsets[1])) : Integer.parseInt(offsets[0]);
                    end = offsets[0].isEmpty() || offsets[1].isEmpty() ? end : Math.min(end, Integer.parseInt(offsets[1]));
                    exchange.getResponseHeaders().set("Content-Range", "bytes %s-%s/%s".formatted(start, end, content.length));
                }
                exchange.sendResponseHeaders(range == null ? 200 : 206, end - start + 1);
                exchange.getResponseBody().write(content, start, end - start + 1);
            }
        });
        s3.start();
        rest = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        rest.createContext("/", exchange -> {
            try (exchange) {
                String path = exchange.getRequestURI().getPath();
                String body;
                if (path.equals("/v1/catalogs/lake")) {
                    body = "{\"name\":\"lake\", \"head_snapshot_id\":7, \"schema_version\":1, \"capabilities\":[\"atomic-table-creation-v1\"]}";
                }
                else if (path.endsWith("/namespaces")) {
                    body = "[{\"name\":\"test\", \"namespace_id\":1}]";
                }
                else if (path.endsWith("/scan")) {
                    body = switch (path.split("/", -1)[7]) {
                        case "events" -> "[" + scanFile(1, T4_DATA, 4, Optional.of(T4_DV)) + "," + scanFile(2, T42_DATA, 2, Optional.empty()) + "]";
                        case "flat" -> "[" + scanFile(3, "s3://test-bucket/lake/data/test/flat/a.parquet", 4, Optional.empty()) + "]";
                        default -> "[" + scanFile(1, T4_DATA, 4, Optional.of(TABLE_LOCATION + "hoglake-dv-1.puffin")) + "]";
                    };
                }
                else if (path.matches(".*/tables/(events|flat|stray_dv)")) {
                    body =
                            """
                            {"name":"t", "namespace":"test", "table_uuid":"synthetic",
                             "columns":[{"field_id":1, "ordinal":0, "name":"value", "type":"long", "nullable":true}],
                             "record_count":0, "file_count":0, "file_size_bytes":0}
                            """;
                }
                else {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] bytes = body.getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        rest.start();
        client = new HoglakeClient("http://127.0.0.1:" + rest.getAddress().getPort(), "lake");
        fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setEndpoint("http://127.0.0.1:" + s3.getAddress().getPort())
                .setRegion("us-east-1")
                .setPathStyleAccess(true)
                .setAwsAccessKey("catalog-key")
                .setAwsSecretKey("catalog-secret")
                .setMaxErrorRetries(1), new S3FileSystemStats());
        pageSources = new HoglakePageSourceProvider(fileSystemFactory, HoglakeParquetFooterCache.disabled(), Optional.empty(), false, true);
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
                        return "hoglake_team_credential_test";
                    }

                    @Override
                    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                    {
                        return new HoglakeConnector(
                                new HoglakeMetadata(client, fileSystemFactory, true),
                                new HoglakeSplitManager(client, _ -> HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE, new NoopSplitAffinityProvider(), Optional.of(source)),
                                pageSources,
                                new HoglakePageSinkProvider(fileSystemFactory, "test"),
                                new Bootstrap().quiet().initialize().getInstance(LifeCycleManager.class),
                                Set.of(new HoglakeSessionProperties(new HoglakeConfig())));
                    }
                });
            }
        });
        queryRunner.createCatalog("hoglake", "hoglake_team_credential_test", Map.of());
    }

    @AfterAll
    void tearDown()
    {
        queryRunner.close();
        client.close();
        fileSystemFactory.destroy();
        rest.stop(0);
        s3.stop(0);
    }

    @BeforeEach
    void resetRequests()
    {
        s3Requests.clear();
    }

    @Test
    void testEveryRequestCarriesTheKeyOfItsPathsTeam()
    {
        assertThat(queryRunner.execute("SELECT value FROM events ORDER BY value").getOnlyColumnAsSet()).containsExactlyInAnyOrder(1L, 3L, 4L, 42L, 43L);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(" served"));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).startsWith("GET " + T4_DATA + " " + accessKey(4)));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).startsWith("GET " + T4_DV + " " + accessKey(4)));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).startsWith("GET " + T42_DATA + " " + accessKey(42)));
        // Each split that reads its data file checks its credential with one HEAD.
        assertThat(s3Requests).filteredOn(request -> request.startsWith("HEAD ") && request.contains(".parquet"))
                .containsExactlyInAnyOrder("HEAD %s %s served".formatted(T4_DATA, accessKey(4)), "HEAD %s %s served".formatted(T42_DATA, accessKey(42)));
    }

    @Test
    void testCountReadsOnlyTheDeletionVectorWithItsTeamsKey()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM events").getOnlyValue()).isEqualTo(5L);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(" " + T4_DV + " " + accessKey(4) + " served"));
    }

    @Test
    void testPlanningFailuresEmitNoSplit()
    {
        int vends = source.vends.size();
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM flat"))
                .hasMessageContaining("s3://test-bucket/lake/data/test/flat/a.parquet is not under a team prefix");
        assertThat(source.vends).hasSize(vends);
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM stray_dv"))
                .hasMessageContaining(TABLE_LOCATION + "hoglake-dv-1.puffin is not under the prefix " + TABLE_LOCATION + "team_id=4/");
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testWritesAreRefusedBeforeAnyStorageCall()
    {
        for (String write : List.of(
                "INSERT INTO events VALUES 5",
                "CREATE TABLE fresh AS SELECT BIGINT '1' AS value",
                "DELETE FROM events WHERE value = 1",
                "UPDATE events SET value = 2",
                "MERGE INTO events t USING (VALUES BIGINT '1') s(v) ON t.value = s.v WHEN MATCHED THEN DELETE")) {
            assertThatThrownBy(() -> queryRunner.execute(write))
                    .as(write)
                    .hasMessageContaining("Hoglake writes are not supported when hoglake.s3-credentials.source is set");
        }
        assertThat(s3Requests).isEmpty();
    }

    /**
     * The worker's own checks, on the read and the count paths: a split
     * without a credential, or with one that does not cover its data file or
     * its deletion vector, fails before any S3 request.
     */
    @Test
    void testWorkerRefusesUncoveredSplitsWithoutAnyRequest()
    {
        HoglakeSplit bare = new HoglakeSplit(1, T4_DATA, objects.get(T4_DATA).length, 4, Optional.of(T4_DV), 1, Optional.empty());
        HoglakeSplitCredential team4 = new HoglakeSplitCredential(4, TABLE_LOCATION + "team_id=4/", accessKey(4), secretKey(4), sessionToken(4), 0);
        HoglakeSplit foreignDv = withCredential(new HoglakeSplit(1, T4_DATA, objects.get(T4_DATA).length, 4, Optional.of(TABLE_LOCATION + "team_id=42/a.puffin"), 1, Optional.empty()), team4);
        HoglakeSplit foreignData = withCredential(new HoglakeSplit(2, T42_DATA, objects.get(T42_DATA).length, 2, Optional.empty(), 0, Optional.empty()), team4);
        for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(VALUE), List.of())) {
            assertErrorCode(() -> pageSource(bare, columns), HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE);
            assertErrorCode(() -> pageSource(foreignDv, columns), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
            assertErrorCode(() -> pageSource(foreignData, columns), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        }
        assertThat(s3Requests).isEmpty();
    }

    private void pageSource(HoglakeSplit split, List<ColumnHandle> columns)
            throws IOException
    {
        pageSources.createPageSource(HoglakeTransactionHandle.INSTANCE, ConnectorTestFixtures.session(), split, TABLE, Optional.empty(), columns, DynamicFilter.EMPTY, MemoryContext.NO_LIMIT).close();
    }

    // Attaches a credential without the split manager's check, as a forged or corrupted split would arrive.
    private static HoglakeSplit withCredential(HoglakeSplit split, HoglakeSplitCredential credential)
    {
        return new HoglakeSplit(
                split.dataFileId(),
                split.path(),
                split.fileSizeBytes(),
                split.recordCount(),
                split.deleteFilePath(),
                split.deleteCount(),
                split.deleteFileFormat(),
                split.start(),
                split.length(),
                split.splitWeight(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(credential));
    }

    private String scanFile(long id, String path, long records, Optional<String> deletionVector)
    {
        String dataFile = "{\"data_file_id\":%d, \"path\":\"%s\", \"record_count\":%d, \"file_size_bytes\":%d, \"file_format\":\"parquet\", \"begin_snapshot\":1}"
                .formatted(id, path, records, objects.get(T4_DATA.equals(path) || T42_DATA.equals(path) ? path : T4_DATA).length);
        return deletionVector.map(dv -> "{\"data_file\":%s, \"delete_file\":{\"delete_file_id\":%d, \"data_file_id\":%d, \"path\":\"%s\", \"file_format\":\"puffin-dv\", \"delete_count\":1, \"file_size_bytes\":%d, \"begin_snapshot\":6}}"
                        .formatted(dataFile, id, id, dv, objects.get(T4_DV).length))
                .orElse("{\"data_file\":" + dataFile + "}");
    }

    private static byte[] parquet(Long... values)
    {
        return ConnectorTestFixtures.writeParquet(List.of(new FileColumn(optional(INT64).id(1).named("value"), BIGINT, List.of((Object[]) values))));
    }
}
