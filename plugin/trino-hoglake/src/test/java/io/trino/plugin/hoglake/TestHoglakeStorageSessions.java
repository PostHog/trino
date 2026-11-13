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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import io.airlift.json.JsonCodec;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.plugin.hoglake.HoglakeTableHandle.MountTable;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.variant.Variant;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_EXPIRED;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_STORAGE_SESSION_REFUSED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The worker side of per-team S3 sessions for the posthog schema, against an
 * in-process S3 that serves an object of a team's prefix only to that team's
 * key and an org object only to the catalog's key, and records the key of
 * every request. Splits are read after a trip through the engine's JSON codec.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public final class TestHoglakeStorageSessions
{
    private static final String ROOT = "s3://events-bucket/data/ingest/events/";
    private static final String T4_DATA = ROOT + "team_id=4/a.parquet";
    private static final String T4_DV = ROOT + "team_id=4/a.puffin";
    private static final String T4_BARE = ROOT + "team_id=4/b.parquet";
    private static final String T42_DATA = ROOT + "team_id=42/c.parquet";
    private static final String ORG_DATA = "s3://org-bucket/data/analytics/orders/d.parquet";
    private static final String CATALOG_KEY = "catalog-access-key";
    private static final String USER_KEY = "user-access-key";
    private static final Pattern TEAM = Pattern.compile("/team_id=(\\d+)/");
    private static final Pattern CREDENTIAL = Pattern.compile("Credential=([^/]+)/");
    private static final long NOW = Instant.parse("2026-11-13T00:00:00Z").toEpochMilli();
    private static final long FIFTEEN_MINUTES = 15 * 60 * 1000;
    private static final HoglakeColumnHandle VALUE = new HoglakeColumnHandle("value", 1, BIGINT, true);
    private static final HoglakeTableHandle MOUNT_TABLE = new HoglakeTableHandle(
            "posthog", "events", 7, "synthetic", List.of(VALUE), TupleDomain.all(), List.of(), List.of(), false, Optional.of(new MountTable("org-events", "ingest")));
    private static final HoglakeTableHandle ORG_TABLE = new HoglakeTableHandle("analytics", "orders", 7, "synthetic", List.of(VALUE));
    private static final JsonCodec<HoglakeSplit> SPLIT_CODEC = JsonCodec.jsonCodec(HoglakeSplit.class);
    private static final String REFUSED = "A read of the posthog schema has no storage session";
    private static final String EXPIRED = "A read of the posthog schema did not finish before its storage session expired; run the query again";
    private static final String OUTSIDE = "A file of the posthog schema is outside its team's storage prefix";

    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    // Method, object path and access key of every S3 request, and whether S3 served it
    private final List<String> s3Requests = new CopyOnWriteArrayList<>();
    private final List<String> logged = new CopyOnWriteArrayList<>();
    private final MutableClock clock = new MutableClock();
    // When set, S3 refuses every request and moves the clock to this time first
    private volatile OptionalLong refuseAndMoveClockTo = OptionalLong.empty();
    private final Logger trinoLogger = Logger.getLogger("io.trino");
    private final Handler logCapture = new Handler()
    {
        @Override
        public void publish(LogRecord record)
        {
            StringWriter text = new StringWriter();
            text.append(String.valueOf(record.getMessage()));
            if (record.getParameters() != null) {
                for (Object parameter : record.getParameters()) {
                    text.append(' ').append(String.valueOf(parameter));
                }
            }
            if (record.getThrown() != null) {
                record.getThrown().printStackTrace(new PrintWriter(text));
            }
            logged.add(text.toString());
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    };
    private Level previousLevel;
    private HttpServer s3;
    private S3FileSystemFactory fileSystemFactory;
    private HoglakePageSourceProvider pageSources;

    @BeforeAll
    void setUp()
            throws IOException
    {
        objects.put(T4_DATA, parquet(1L, 2L, 3L, 4L));
        objects.put(T4_DV, PuffinDeletionVectorFixtures.deletionVector(T4_DATA, 1));
        objects.put(T4_BARE, parquet(5L, 6L));
        objects.put(T42_DATA, parquet(42L, 43L));
        objects.put(ORG_DATA, parquet(7L, 8L));
        s3 = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
        s3.createContext("/", exchange -> {
            try (exchange) {
                String path = "s3:/" + exchange.getRequestURI().getPath();
                Matcher key = CREDENTIAL.matcher(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
                String accessKey = key.find() ? key.group(1) : "none";
                Matcher team = TEAM.matcher(path);
                boolean allowed = team.find() ? accessKey.equals(accessKey(Long.parseLong(team.group(1)))) : accessKey.equals(CATALOG_KEY);
                OptionalLong refusal = refuseAndMoveClockTo;
                if (refusal.isPresent()) {
                    clock.set(refusal.orElseThrow());
                    allowed = false;
                }
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
        fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                .setEndpoint("http://127.0.0.1:" + s3.getAddress().getPort())
                .setRegion("us-east-1")
                .setPathStyleAccess(true)
                .setAwsAccessKey(CATALOG_KEY)
                .setAwsSecretKey("catalog-secret-value")
                .setMaxErrorRetries(1), new S3FileSystemStats());
        pageSources = new HoglakePageSourceProvider(fileSystemFactory, HoglakeParquetFooterCache.disabled(), Optional.empty(), false, clock);
        previousLevel = trinoLogger.getLevel();
        trinoLogger.setLevel(Level.ALL);
        trinoLogger.addHandler(logCapture);
    }

    @AfterAll
    void tearDown()
    {
        trinoLogger.removeHandler(logCapture);
        trinoLogger.setLevel(previousLevel);
        if (fileSystemFactory != null) {
            fileSystemFactory.destroy();
        }
        if (s3 != null) {
            s3.stop(0);
        }
    }

    @BeforeEach
    void reset()
    {
        s3Requests.clear();
        clock.set(NOW);
        refuseAndMoveClockTo = OptionalLong.empty();
    }

    /**
     * Checked after every test: nothing logged names a session value or the user's key.
     */
    @AfterEach
    void verifyNothingSecretWasLogged()
    {
        assertThat(logged).allSatisfy(text -> assertThat(text).doesNotContain(secrets()));
    }

    @Test
    void testContainmentMatchesHoglakeVectors()
            throws IOException
    {
        JsonNode cases = vectors().get("containment_cases");
        assertThat(cases).hasSize(14);
        for (JsonNode vector : cases) {
            assertThat(HoglakeStorageSession.pathStaysUnder(vector.get("path").asText(), vector.get("prefix").asText()))
                    .as(vector.get("path").asText())
                    .isEqualTo(vector.get("under").asBoolean());
        }
    }

    /**
     * hoglake's canonical team prefixes are team prefixes of their own team
     * and of no other; team 0 is never vended, so it is not one.
     */
    @Test
    void testTeamPrefixMatchesHoglakeVectors()
            throws IOException
    {
        int checked = 0;
        for (JsonNode vector : vectors().get("prefix_cases")) {
            if (!vector.has("expected")) {
                continue;
            }
            String prefix = vector.get("expected").asText();
            long team = Long.parseLong(vector.get("value").asText());
            assertThat(HoglakeStorageSession.isTeamPrefix(prefix, team)).as(prefix).isEqualTo(team > 0);
            assertThat(HoglakeStorageSession.isTeamPrefix(prefix, team + 1)).as(prefix).isFalse();
            checked++;
        }
        assertThat(checked).isEqualTo(5);
    }

    @Test
    void testPrefixCheck()
    {
        HoglakeStorageSession team4 = session(4);
        team4.checkCovers(split(T4_DATA, Optional.of(T4_DV)));
        team4.checkCovers(split(ROOT + "team_id=4/sub/a.parquet", Optional.empty()));

        List<HoglakeSplit> outside = List.of(
                // Another team's data file, and another team's or the table's deletion vector
                split(T42_DATA, Optional.empty()),
                split(T4_DATA, Optional.of(ROOT + "team_id=42/a.puffin")),
                split(T4_DATA, Optional.of(ROOT + "hoglake-dv-1.puffin")),
                // A sibling prefix that shares a string prefix
                split(ROOT + "team_id=42/a.parquet", Optional.empty()),
                split(ROOT + "team_id=4x/a.parquet", Optional.empty()),
                split(ROOT.substring(0, ROOT.length() - 1) + "_v2/team_id=4/a.parquet", Optional.empty()),
                // Path tricks
                split(ROOT + "team_id=4/../team_id=42/a.parquet", Optional.empty()),
                split(ROOT + "team_id=4/./a.parquet", Optional.empty()),
                split(ROOT + "team_id=4//a.parquet", Optional.empty()),
                split(ROOT + "team_id=4/%2e%2e/team_id=42/a.parquet", Optional.empty()),
                split(ROOT + "team_id=4/..%2Fteam_id=42%2fa.parquet", Optional.empty()),
                split(ROOT + "team_id=4/a\\..\\b.parquet", Optional.empty()),
                split(ROOT + "team_id=4/a b.parquet", Optional.empty()),
                split(T4_DATA, Optional.of(ROOT + "team_id=4/../team_id=42/a.puffin")),
                split(T4_DATA, Optional.of(ROOT + "team_id=4/%2E%2E/a.puffin")));
        for (HoglakeSplit split : outside) {
            assertRefused(() -> team4.checkCovers(split), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
        }

        // A prefix that is not the session's own team's
        HoglakeSplit inside = split(T4_DATA, Optional.empty());
        for (String prefix : List.of(
                ROOT,
                ROOT + "team_id=4",
                ROOT + "team_id=42/",
                ROOT + "xteam_id=4/",
                ROOT + "team_id=04/",
                ROOT + "team_id=4/sub/",
                "s3:///team_id=4/",
                "s3://events-bucket/data/../team_id=4/",
                "s3://events-bucket/data/%2e%2e/team_id=4/",
                "s3://events-bucket//team_id=4/",
                "gs://events-bucket/data/ingest/events/team_id=4/",
                "events-bucket/data/ingest/events/team_id=4/")) {
            HoglakeStorageSession wrong = new HoglakeStorageSession(4, prefix, accessKey(4), secretKey(4), sessionToken(4), NOW + FIFTEEN_MINUTES);
            assertRefused(() -> wrong.checkCovers(inside), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
            // Also for a file under that prefix: the prefix itself is refused
            assertRefused(() -> wrong.checkCovers(split(prefix + (prefix.endsWith("/") ? "" : "/") + "a.parquet", Optional.empty())), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
        }
        for (long team : List.of(0L, -4L)) {
            HoglakeStorageSession notATeam = new HoglakeStorageSession(team, ROOT + "team_id=" + team + "/", accessKey(4), secretKey(4), sessionToken(4), NOW + FIFTEEN_MINUTES);
            assertRefused(() -> notATeam.checkCovers(split(ROOT + "team_id=" + team + "/a.parquet", Optional.empty())), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
        }
    }

    @Test
    void testSplitsTravelAsJsonWithAndWithoutASession()
            throws IOException
    {
        HoglakeSplit bare = new HoglakeSplit(3, T4_DATA, 100, 4, Optional.of(T4_DV), 1, Optional.of("puffin-dv"), 10, 50, SplitWeight.fromProportion(0.5), OptionalLong.of(20), Optional.of("affinity"));
        HoglakeSplit withSession = bare.withSession(session(4));
        assertThat(bare.session()).isEmpty();
        assertThat(withSession.session()).contains(session(4));
        assertThat(withSession.withRange(0, 10, SplitWeight.standard()).session()).contains(session(4));

        assertThat(SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(bare))).isEqualTo(bare);
        HoglakeSplit copy = SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(withSession));
        assertThat(copy).isEqualTo(withSession);
        assertThat(copy.session().orElseThrow().extraCredentials()).isEqualTo(ImmutableMap.of(
                EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKey(4),
                EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretKey(4),
                EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken(4)));

        // A split without a session is written exactly as a node without the field writes it
        JsonCodec<OldSplit> oldCodec = JsonCodec.jsonCodec(OldSplit.class);
        OldSplit oldShape = new OldSplit(3, T4_DATA, 100, 4, Optional.of(T4_DV), 1, Optional.of("puffin-dv"), 10, 50, SplitWeight.fromProportion(0.5), OptionalLong.of(20), Optional.of("affinity"));
        assertThat(SPLIT_CODEC.toJson(bare)).isEqualTo(oldCodec.toJson(oldShape));
        // and a split written by such a node decodes with no session, as does an explicit null
        HoglakeSplit old = SPLIT_CODEC.fromJson(oldCodec.toJson(oldShape));
        assertThat(old).isEqualTo(bare);
        assertThat(old.session()).isEmpty();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode withNull = (ObjectNode) mapper.readTree(oldCodec.toJson(oldShape));
        withNull.putNull("session");
        assertThat(SPLIT_CODEC.fromJson(mapper.writeValueAsString(withNull)).session()).isEmpty();

        // A node without the field ignores it when it reads a new split
        assertThat(oldCodec.fromJson(SPLIT_CODEC.toJson(withSession))).isEqualTo(oldShape);

        // A session's accounting includes each of its values
        String large = "x".repeat(4096);
        for (HoglakeStorageSession sized : List.of(
                new HoglakeStorageSession(4, ROOT + "team_id=4/", large, "s", "t", NOW),
                new HoglakeStorageSession(4, ROOT + "team_id=4/", "a", large, "t", NOW),
                new HoglakeStorageSession(4, ROOT + "team_id=4/", "a", "s", large, NOW))) {
            assertThat(bare.withSession(sized).getRetainedSizeInBytes()).isGreaterThan(bare.getRetainedSizeInBytes() + large.length());
        }
        // A split built with a null session, as a decoder may build it, has none
        Optional<HoglakeStorageSession> missing = null;
        assertThat(new HoglakeSplit(3, T4_DATA, 100, 4, Optional.empty(), 0, Optional.empty(), 0, 100, SplitWeight.standard(), OptionalLong.empty(), Optional.empty(), missing).session()).isEmpty();
    }

    /**
     * The split shape of a node that does not know storage sessions.
     */
    public record OldSplit(
            @JsonProperty("dataFileId") long dataFileId,
            @JsonProperty("path") String path,
            @JsonProperty("fileSizeBytes") long fileSizeBytes,
            @JsonProperty("recordCount") long recordCount,
            @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
            @JsonProperty("deleteCount") long deleteCount,
            @JsonProperty("deleteFileFormat") Optional<String> deleteFileFormat,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("footerSize") OptionalLong footerSize,
            @JsonProperty("affinityKey") Optional<String> affinityKey) {}

    @Test
    void testNothingSecretIsPrinted()
    {
        HoglakeSplit split = split(T4_DATA, Optional.of(T4_DV)).withSession(session(4));
        assertThat(session(4).toString()).isEqualTo("HoglakeStorageSession[teamId=4, prefix=%steam_id=4/, expiration=2026-11-13T00:15:00Z]".formatted(ROOT));
        assertThat(split.toString()).contains("teamId=4", ROOT + "team_id=4/");
        List<String> rendered = new ArrayList<>(List.of(split.toString(), session(4).toString(), String.valueOf(split.session())));
        for (TrinoException failure : List.of(
                failure(() -> session(4).checkCovers(split(T42_DATA, Optional.empty()))),
                failure(() -> read(MOUNT_TABLE, split(T42_DATA, Optional.empty()).withSession(session(4)))),
                failure(() -> read(MOUNT_TABLE, split(T4_DATA, Optional.empty()).withSession(session(4, NOW)))),
                failure(() -> read(ORG_TABLE, split(ORG_DATA, Optional.empty()).withSession(session(4)))),
                failure(() -> read(MOUNT_TABLE, split(T4_DATA, Optional.empty()))))) {
            rendered.add(stackTrace(failure));
        }
        assertThat(rendered).allSatisfy(text -> assertThat(text).doesNotContain(secrets()));
    }

    @Test
    void testSessionSplitReadsWithTheSessionAlone()
    {
        // The user's own extra credentials are not used: the identity holds the session's values only
        ConnectorSession user = ConnectorTestFixtures.session(ConnectorIdentity.forUser("wh1.alice")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, USER_KEY,
                        EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "user-secret-value",
                        EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "user-session-token"))
                .build());
        assertThat(read(user, MOUNT_TABLE, split(T4_DATA, Optional.of(T4_DV)).withSession(session(4)), List.of(VALUE)))
                .containsExactly(List.of(1L), List.of(3L), List.of(4L));
        assertThat(read(user, MOUNT_TABLE, split(T42_DATA, Optional.empty()).withSession(session(42)), List.of(VALUE)))
                .containsExactly(List.of(42L), List.of(43L));
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(" served"));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).isEqualTo("GET %s %s served".formatted(T4_DV, accessKey(4))));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).startsWith("GET %s %s ".formatted(T4_DATA, accessKey(4))));
        assertThat(s3Requests).anySatisfy(request -> assertThat(request).startsWith("GET %s %s ".formatted(T42_DATA, accessKey(42))));
        assertThat(s3Requests).noneMatch(request -> request.contains(CATALOG_KEY) || request.contains(USER_KEY));
        // No access check of the data file without a file system cache: the read itself is S3's decision
        assertThat(s3Requests).noneMatch(request -> request.startsWith("HEAD ") && request.contains(".parquet"));

        // The count path reads the deletion vector with the session
        s3Requests.clear();
        assertThat(read(user, MOUNT_TABLE, split(T4_DATA, Optional.of(T4_DV)).withSession(session(4)), List.of()))
                .hasSize(3);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(" %s %s served".formatted(T4_DV, accessKey(4))));
    }

    /**
     * A posthog schema split without a session is refused before any storage
     * call, on the read path and on the count path when the count must read
     * a deletion vector. A count that reads no storage needs no session.
     */
    @Test
    void testMountSplitWithoutASessionIsRefusedBeforeStorage()
    {
        for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(VALUE), List.of())) {
            assertRefused(() -> read(MOUNT_TABLE, split(T4_DATA, Optional.of(T4_DV)), columns), HOGLAKE_STORAGE_SESSION_REFUSED, REFUSED);
        }
        assertRefused(() -> read(MOUNT_TABLE, split(T4_BARE, Optional.empty()), List.of(VALUE)), HOGLAKE_STORAGE_SESSION_REFUSED, REFUSED);
        assertThat(s3Requests).isEmpty();

        assertThat(read(MOUNT_TABLE, split(T4_BARE, Optional.empty()), List.of())).hasSize(2);
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testSessionIsNeverUsedForAnOrgTable()
    {
        String message = "A split of a table outside the posthog schema carries a storage session";
        for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(VALUE), List.of())) {
            assertRefused(() -> read(ORG_TABLE, split(T4_DATA, Optional.of(T4_DV)).withSession(session(4)), columns), GENERIC_INTERNAL_ERROR, message);
            assertRefused(() -> read(ORG_TABLE, split(ORG_DATA, Optional.of(T4_DV)).withSession(session(4)), columns), GENERIC_INTERNAL_ERROR, message);
        }
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testOrgTableReadsWithTheCatalogsCredentialsAsBefore()
    {
        assertThat(read(ORG_TABLE, split(ORG_DATA, Optional.empty()), List.of(VALUE))).containsExactly(List.of(7L), List.of(8L));
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(" " + ORG_DATA + " " + CATALOG_KEY + " served"));
        // The org path is not checked against any team prefix
        s3Requests.clear();
        assertThatThrownBy(() -> read(ORG_TABLE, split(T4_DATA, Optional.empty()), List.of(VALUE)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessage("Failed to open parquet file: " + T4_DATA);
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(" " + T4_DATA + " " + CATALOG_KEY + " denied"));
    }

    @Test
    void testFileOutsideTheTeamsPrefixIsRefusedBeforeStorage()
    {
        for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(VALUE), List.of())) {
            for (HoglakeSplit split : List.of(
                    split(T42_DATA, Optional.of(T4_DV)).withSession(session(4)),
                    split(T4_DATA, Optional.of(ROOT + "team_id=42/a.puffin")).withSession(session(4)),
                    split(ROOT + "team_id=4/../team_id=42/c.parquet", Optional.of(T4_DV)).withSession(session(4)),
                    split(T4_DATA, Optional.of(T4_DV)).withSession(new HoglakeStorageSession(4, ROOT, accessKey(4), secretKey(4), sessionToken(4), NOW + FIFTEEN_MINUTES)),
                    split(T4_DATA, Optional.of(T4_DV)).withSession(session(42)))) {
                assertRefused(() -> read(MOUNT_TABLE, split, columns), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
            }
        }
        assertThat(s3Requests).isEmpty();
    }

    @Test
    void testExpiredSessionFailsBeforeStorage()
    {
        for (List<ColumnHandle> columns : List.<List<ColumnHandle>>of(List.of(VALUE), List.of())) {
            for (long expiration : List.of(NOW, NOW - 1, NOW - FIFTEEN_MINUTES)) {
                HoglakeSplit split = split(T4_DATA, Optional.of(T4_DV)).withSession(session(4, expiration));
                TrinoException failure = assertRefused(() -> read(MOUNT_TABLE, split, columns), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);
                assertThat(failure).hasNoCause();
            }
        }
        assertThat(s3Requests).isEmpty();

        // One millisecond left is enough to start: the worker does not ask for more
        assertThat(read(MOUNT_TABLE, split(T4_BARE, Optional.empty()).withSession(session(4, NOW + 1)), List.of(VALUE))).hasSize(2);
    }

    /**
     * S3 refuses an expired session; that refusal, when opening, while
     * reading or while counting, is reported as the expired error, and
     * the same refusal of a live session as before.
     */
    @Test
    void testStorageFailureAfterExpiryIsTheExpiredError()
            throws IOException
    {
        long expiration = NOW + FIFTEEN_MINUTES;
        HoglakeSplit split = split(T4_DATA, Optional.of(T4_DV)).withSession(session(4, expiration));

        // Opening
        refuseAndMoveClockTo = OptionalLong.of(expiration);
        TrinoException failure = assertRefused(() -> read(MOUNT_TABLE, split, List.of(VALUE)), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);
        assertThat(failure).hasNoCause();
        assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).contains(accessKey(4) + " denied"));

        // Counting
        clock.set(NOW);
        assertRefused(() -> read(MOUNT_TABLE, split, List.of()), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);

        // Reading: the footer is fetched alone, so the row groups are read after the page source opens
        clock.set(NOW);
        refuseAndMoveClockTo = OptionalLong.empty();
        byte[] data = objects.get(T4_BARE);
        HoglakeSplit sized = new HoglakeSplit(2, T4_BARE, data.length, 2, Optional.empty(), 0, Optional.empty(), 0, data.length, SplitWeight.standard(), OptionalLong.of(ConnectorTestFixtures.footerSize(data)), Optional.empty())
                .withSession(session(4, expiration));
        try (ConnectorPageSource source = pageSource(ConnectorTestFixtures.session(), MOUNT_TABLE, SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(sized)), List.of(VALUE))) {
            refuseAndMoveClockTo = OptionalLong.of(expiration + 1);
            assertRefused(() -> ConnectorTestFixtures.readAll(source, List.of(BIGINT)), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);
        }

        // The same refusals of a live session keep today's errors
        refuseAndMoveClockTo = OptionalLong.of(NOW + 1);
        clock.set(NOW);
        assertThatThrownBy(() -> read(MOUNT_TABLE, split, List.of(VALUE)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessage("Failed to open parquet file: " + T4_DATA);
        clock.set(NOW);
        assertThatThrownBy(() -> read(MOUNT_TABLE, split, List.of()))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()));
        clock.set(NOW);
        refuseAndMoveClockTo = OptionalLong.empty();
        try (ConnectorPageSource source = pageSource(ConnectorTestFixtures.session(), MOUNT_TABLE, sized, List.of(VALUE))) {
            refuseAndMoveClockTo = OptionalLong.of(NOW + 1);
            assertThatThrownBy(() -> ConnectorTestFixtures.readAll(source, List.of(BIGINT)))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
        }
    }

    /**
     * A failure of a session split's read that is not a storage failure, here
     * a shredded VARIANT that cannot be assembled, is reported as the expired
     * error too once the session has expired: every storage call of the split
     * fails from then on, so the read cannot finish either way.
     */
    @Test
    void testAnyReadFailureAfterExpiryIsTheExpiredError()
            throws IOException
    {
        // Field "a" has both a value and a typed_value, but it is not an object
        String path = ROOT + "team_id=4/unreadable.parquet";
        byte[] file = TestHoglakeShreddedVariant.write(
                List.of(),
                List.of(TestHoglakeShreddedVariant.row(TestHoglakeShreddedVariant.METADATA, null, Arrays.asList(Arrays.asList(Variant.ofLong(5).data(), 5L), Arrays.asList(null, null)))),
                Optional.empty());
        objects.put(path, file);
        HoglakeColumnHandle variant = new HoglakeColumnHandle("v", 1, VARIANT, true);
        HoglakeTableHandle table = new HoglakeTableHandle(
                "posthog", "events", 7, "synthetic", List.of(variant), TupleDomain.all(), List.of(), List.of(), false, Optional.of(new MountTable("org-events", "ingest")));
        long expiration = NOW + FIFTEEN_MINUTES;
        HoglakeSplit split = new HoglakeSplit(3, path, file.length, 1, Optional.empty(), 0, Optional.empty()).withSession(session(4, expiration));
        try {
            // While the session lives, the failure is the file's own
            try (ConnectorPageSource source = pageSource(ConnectorTestFixtures.session(), table, split, List.of(variant))) {
                assertThatThrownBy(() -> ConnectorTestFixtures.readAll(source, List.of(VARIANT)))
                        .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                        .hasMessage("Corrupt parquet data: " + path);
            }
            // The same read, opened in time and continued after the expiry; S3 serves every request it gets
            try (ConnectorPageSource source = pageSource(ConnectorTestFixtures.session(), table, split, List.of(variant))) {
                clock.set(expiration);
                TrinoException failure = assertRefused(() -> ConnectorTestFixtures.readAll(source, List.of(VARIANT)), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);
                assertThat(failure).hasNoCause();
            }
            assertThat(s3Requests).isNotEmpty().allSatisfy(request -> assertThat(request).endsWith(accessKey(4) + " served"));
        }
        finally {
            objects.remove(path);
        }
    }

    /**
     * How long a session has left when a worker starts a split with it: for
     * each split in the page source's metrics, and the least of all splits in
     * the catalog's counters.
     */
    @Test
    void testTimeLeftAtStartIsReported()
            throws IOException
    {
        // A provider of its own: the counters are those of a node since it started
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(fileSystemFactory, HoglakeParquetFooterCache.disabled(), Optional.empty(), false, clock);
        HoglakeStorageSessionStats stats = provider.storageSessionStats();
        assertThat(stats.getSplitsStarted()).isZero();
        assertThat(stats.getLeastMillisLeftAtStart()).isEqualTo(-1);
        // Ten minutes left for one split, and four for the next
        for (long minutesLeft : List.of(10L, 4L)) {
            HoglakeSplit split = split(T4_BARE, Optional.empty()).withSession(session(4, NOW + minutesLeft * 60_000));
            try (ConnectorPageSource source = pageSource(provider, MOUNT_TABLE, split, List.of(VALUE))) {
                assertThat(ConnectorTestFixtures.readAll(source, List.of(BIGINT))).hasSize(2);
                assertThat(source.getMetrics().getMetrics().get(HoglakePageSourceProvider.STORAGE_SESSION_SECONDS_LEFT_AT_START))
                        .isInstanceOfSatisfying(TDigestHistogram.class, left -> {
                            assertThat(left.getTotal()).isEqualTo(1);
                            assertThat(left.getMin()).isEqualTo(minutesLeft * 60.0);
                        });
            }
        }
        assertThat(stats.getSplitsStarted()).isEqualTo(2);
        assertThat(stats.getLeastMillisLeftAtStart()).isEqualTo(4 * 60_000);
        // A count that reads a deletion vector starts with its session too
        try (ConnectorPageSource source = pageSource(provider, MOUNT_TABLE, split(T4_DATA, Optional.of(T4_DV)).withSession(session(4, NOW + 3 * 60_000)), List.of())) {
            assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(3);
        }
        assertThat(stats.getSplitsStarted()).isEqualTo(3);
        assertThat(stats.getLeastMillisLeftAtStart()).isEqualTo(3 * 60_000);

        // A split that is refused, that has expired, or that has no session starts nothing
        assertRefused(() -> pageSource(provider, MOUNT_TABLE, split(T4_BARE, Optional.empty()).withSession(session(4, NOW)), List.of(VALUE)), HOGLAKE_STORAGE_SESSION_EXPIRED, EXPIRED);
        assertRefused(() -> pageSource(provider, MOUNT_TABLE, split(T42_DATA, Optional.empty()).withSession(session(4)), List.of(VALUE)), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, OUTSIDE);
        assertRefused(() -> pageSource(provider, MOUNT_TABLE, split(T4_BARE, Optional.empty()), List.of(VALUE)), HOGLAKE_STORAGE_SESSION_REFUSED, REFUSED);
        try (ConnectorPageSource source = pageSource(provider, ORG_TABLE, split(ORG_DATA, Optional.empty()), List.of(VALUE))) {
            assertThat(ConnectorTestFixtures.readAll(source, List.of(BIGINT))).hasSize(2);
            assertThat(source.getMetrics().getMetrics()).doesNotContainKey(HoglakePageSourceProvider.STORAGE_SESSION_SECONDS_LEFT_AT_START);
        }
        assertThat(stats.getSplitsStarted()).isEqualTo(3);
        assertThat(stats.getLeastMillisLeftAtStart()).isEqualTo(3 * 60_000);
    }

    private List<List<Object>> read(HoglakeTableHandle table, HoglakeSplit split)
    {
        return read(table, split, List.of(VALUE));
    }

    private List<List<Object>> read(HoglakeTableHandle table, HoglakeSplit split, List<ColumnHandle> columns)
    {
        return read(ConnectorTestFixtures.session(), table, split, columns);
    }

    /**
     * Reads the split after a trip through the engine's JSON codec, as a worker receives it.
     */
    private List<List<Object>> read(ConnectorSession session, HoglakeTableHandle table, HoglakeSplit split, List<ColumnHandle> columns)
    {
        HoglakeSplit received = SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(split));
        assertThat(received).isEqualTo(split);
        try (ConnectorPageSource source = pageSource(session, table, received, columns)) {
            if (columns.isEmpty()) {
                List<List<Object>> rows = new ArrayList<>();
                while (!source.isFinished()) {
                    SourcePage page = source.getNextSourcePage();
                    if (page != null) {
                        for (int position = 0; position < page.getPositionCount(); position++) {
                            rows.add(List.of());
                        }
                    }
                }
                return rows;
            }
            return ConnectorTestFixtures.readAll(source, List.of(BIGINT));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectorPageSource pageSource(HoglakePageSourceProvider provider, HoglakeTableHandle table, HoglakeSplit split, List<ColumnHandle> columns)
    {
        return provider.createPageSource(HoglakeTransactionHandle.INSTANCE, ConnectorTestFixtures.session(), split, table, Optional.empty(), columns, DynamicFilter.EMPTY, MemoryContext.NO_LIMIT);
    }

    private ConnectorPageSource pageSource(ConnectorSession session, HoglakeTableHandle table, HoglakeSplit split, List<ColumnHandle> columns)
    {
        return pageSources.createPageSource(HoglakeTransactionHandle.INSTANCE, session, split, table, Optional.empty(), columns, DynamicFilter.EMPTY, MemoryContext.NO_LIMIT);
    }

    private static TrinoException assertRefused(ThrowingCallable call, ErrorCodeSupplier errorCode, String message)
    {
        TrinoException failure = catchThrowableOfType(TrinoException.class, call);
        assertThat(failure).as("no failure").isNotNull();
        assertThat(failure.getErrorCode()).isEqualTo(errorCode.toErrorCode());
        assertThat(failure).hasMessage(message);
        return failure;
    }

    private static TrinoException failure(ThrowingCallable call)
    {
        return catchThrowableOfType(TrinoException.class, call);
    }

    private static String stackTrace(Throwable throwable)
    {
        StringWriter text = new StringWriter();
        throwable.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    /**
     * A whole-file split; a path S3 does not hold has the size of the four-row file.
     */
    private HoglakeSplit split(String path, Optional<String> deletionVector)
    {
        byte[] data = objects.getOrDefault(path, objects.get(T4_DATA));
        long records = data == objects.get(T4_DATA) ? 4 : 2;
        return new HoglakeSplit(1, path, data.length, records, deletionVector, deletionVector.isPresent() ? 1 : 0, deletionVector.map(_ -> "puffin-dv"));
    }

    private static HoglakeStorageSession session(long team)
    {
        return session(team, NOW + FIFTEEN_MINUTES);
    }

    private static HoglakeStorageSession session(long team, long expiration)
    {
        return new HoglakeStorageSession(team, ROOT + "team_id=" + team + "/", accessKey(team), secretKey(team), sessionToken(team), expiration);
    }

    private static String accessKey(long team)
    {
        return "team-%d-access-key".formatted(team);
    }

    private static String secretKey(long team)
    {
        return "team-%d-secret-value".formatted(team);
    }

    private static String sessionToken(long team)
    {
        return "team-%d-session-token".formatted(team);
    }

    // Every session value of the teams used here, and the user's own key
    private static String[] secrets()
    {
        List<String> secrets = new ArrayList<>(List.of(USER_KEY, "user-secret-value", "user-session-token"));
        for (long team : List.of(4L, 42L)) {
            secrets.add(accessKey(team));
            secrets.add(secretKey(team));
            secrets.add(sessionToken(team));
        }
        return secrets.toArray(String[]::new);
    }

    private static byte[] parquet(Long... values)
    {
        return ConnectorTestFixtures.writeParquet(List.of(new FileColumn(optional(INT64).id(1).named("value"), BIGINT, List.of((Object[]) values))));
    }

    private static JsonNode vectors()
            throws IOException
    {
        try (InputStream input = TestHoglakeStorageSessions.class.getResourceAsStream("/object-prefix-vectors.json")) {
            return new ObjectMapper().readTree(input);
        }
    }

    private static final class MutableClock
            extends Clock
    {
        private final AtomicLong millis = new AtomicLong(NOW);

        public void set(long epochMillis)
        {
            millis.set(epochMillis);
        }

        @Override
        public long millis()
        {
            return millis.get();
        }

        @Override
        public Instant instant()
        {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneId getZone()
        {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            throw new UnsupportedOperationException();
        }
    }
}
