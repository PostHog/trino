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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.FeaturesConfig;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.session;
import static io.trino.spi.StandardErrorCode.EXCEEDED_SCAN_LIMIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * The read-only schema posthog against a fake Hoglake server that serves an org
 * catalog (with its own namespace named posthog, which must never be reached) and
 * the org's events mount, and records every request.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestHoglakePosthogSchema
{
    private static final String ORG = "0b5f0c2e-1111-4222-8333-444455556666";
    private static final String MOUNT = ORG + "-events";
    private static final String ORG_PATH = "/v1/catalogs/" + ORG;
    private static final String MOUNT_PATH = "/v1/catalogs/" + MOUNT;
    private static final String SHADOW_PATH = ORG_PATH + "/namespaces/posthog";
    // Placed in every refusal body: a user-visible message must never carry it
    private static final String BODY_MARKER = "detail-marker-6f1d";

    private enum Mount
    {
        SERVED,
        MISSING,
        TWO_NAMESPACES,
        REFUSING,
    }

    private record Request(String method, String path, String query, String authorization) {}

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private volatile Mount mount = Mount.SERVED;
    private volatile int refusalStatus;
    private volatile String refusalCode;
    private volatile long mountHead = 42;
    private HttpServer server;
    private String uri;
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = bindInPortRange();
        server.createContext("/", this::handle);
        server.start();
        uri = "http://127.0.0.1:" + server.getAddress().getPort();
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("org_on").setSchema("analytics").build());
        queryRunner.installPlugin(new HoglakePlugin());
        queryRunner.createCatalog("org_on", "hoglake", catalogProperties("client-on", true));
        queryRunner.createCatalog("org_off", "hoglake", catalogProperties("client-off", false));
    }

    private Map<String, String> catalogProperties(String clientId, boolean posthogSchema)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hoglake.uri", uri)
                .put("hoglake.catalog", ORG)
                .put("hoglake.auth.token-uri", uri + "/oauth/token")
                .put("hoglake.auth.client-id", clientId)
                .put("hoglake.auth.client-secret", "synthetic-test-secret")
                // Storage is never reached: every read here is answered from catalog counts
                .put("s3.endpoint", "http://127.0.0.1:9")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "synthetic-test")
                .put("s3.aws-secret-key", "synthetic-test-password");
        if (posthogSchema) {
            properties.put("hoglake.posthog-schema.enabled", "true");
        }
        return properties.buildOrThrow();
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @BeforeEach
    void reset()
    {
        mount = Mount.SERVED;
        mountHead = 42;
        refusalCode = null;
        requests.clear();
    }

    @Test
    void testPropertyUnsetChangesNothing()
    {
        // The org's own namespace posthog is an ordinary schema, read from the org catalog
        assertThat(column("SHOW SCHEMAS FROM org_off")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(column("SHOW TABLES FROM org_off.posthog")).containsExactlyInAnyOrder("events", "shadow");
        assertThat(column("DESCRIBE org_off.posthog.events")).containsExactly("org_column");
        assertThat(scalar("SELECT count(*) FROM org_off.posthog.events")).isEqualTo(5L);
        // Writes go where they always went: the org catalog decides (here, by capability)
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("DELETE FROM org_off.posthog.events"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageNotContaining("read-only");
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH));
        assertThat(requests).anyMatch(request -> request.path().startsWith(SHADOW_PATH));
    }

    @Test
    void testSchemaListedAndServedFromTheMountWithTheSameToken()
    {
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactly("events");
        assertThat(column("DESCRIBE org_on.posthog.events")).containsExactly("team_id", "event");
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactlyInAnyOrder("analytics.orders", "posthog.events");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog'"))
                .containsExactlyInAnyOrder("team_id", "event");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog' AND table_name = 'events'"))
                .containsExactlyInAnyOrder("team_id", "event");

        assertThat(requests).anyMatch(request -> request.path().equals(MOUNT_PATH + "/namespaces/ingest/tables/events"));
        // One token for both catalogs of the server: the org catalog's
        Set<String> tokens = requests.stream()
                .filter(request -> request.path().startsWith("/v1/"))
                .map(Request::authorization)
                .collect(toImmutableSet());
        assertThat(tokens).containsExactly("Bearer token-for-client-on");
        assertThat(requests).filteredOn(request -> request.path().equals("/oauth/token")).hasSizeLessThanOrEqualTo(1);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testScanIsPlannedThroughTheMountScanRoute()
    {
        assertThat(scalar("SELECT count(*) FROM org_on.posthog.events")).isEqualTo(7L);
        assertThat(requests)
                .filteredOn(request -> request.path().endsWith("/scan"))
                .singleElement()
                .satisfies(request -> {
                    assertThat(request.path()).isEqualTo(MOUNT_PATH + "/namespaces/ingest/tables/events/scan");
                    assertThat(request.query()).startsWith("snapshot=42");
                    assertThat(request.authorization()).isEqualTo("Bearer token-for-client-on");
                });
        // The org catalog's own table plans through its own scan route, as before
        requests.clear();
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(requests)
                .filteredOn(request -> request.path().endsWith("/scan"))
                .singleElement()
                .satisfies(request -> assertThat(request.path()).isEqualTo(ORG_PATH + "/namespaces/analytics/tables/orders/scan"));
        assertThat(requests).noneMatch(request -> request.path().startsWith(MOUNT_PATH + "/namespaces"));
    }

    @Test
    void testEveryWriteStatementIsRefusedWithoutAWriteRequest()
    {
        List<String> readOnlyRefusals = ImmutableList.of(
                "INSERT INTO org_on.posthog.events VALUES (1, 'x')",
                "CREATE TABLE org_on.posthog.created (x bigint)",
                "CREATE TABLE org_on.posthog.created AS SELECT 1 x",
                "CREATE OR REPLACE TABLE org_on.posthog.events AS SELECT 1 x",
                "DELETE FROM org_on.posthog.events WHERE team_id = 1",
                "DELETE FROM org_on.posthog.events",
                "UPDATE org_on.posthog.events SET event = 'x'",
                "MERGE INTO org_on.posthog.events t USING (VALUES 1) s(x) ON t.team_id = s.x WHEN MATCHED THEN DELETE",
                "MERGE INTO org_on.posthog.events t USING (VALUES 1) s(x) ON t.team_id = s.x WHEN NOT MATCHED THEN INSERT VALUES (s.x, 'x')",
                "DROP TABLE org_on.posthog.events",
                "TRUNCATE TABLE org_on.posthog.events",
                "ALTER TABLE org_on.posthog.events RENAME TO org_on.posthog.renamed",
                "ALTER TABLE org_on.analytics.orders RENAME TO org_on.posthog.orders",
                "ALTER TABLE org_on.posthog.events ADD COLUMN added bigint",
                "ALTER TABLE org_on.posthog.events DROP COLUMN event",
                "ALTER TABLE org_on.posthog.events RENAME COLUMN event TO renamed",
                "ALTER TABLE org_on.posthog.events ALTER COLUMN team_id SET DATA TYPE decimal(38, 0)",
                "COMMENT ON TABLE org_on.posthog.events IS 'x'",
                "COMMENT ON COLUMN org_on.posthog.events.event IS 'x'",
                "ALTER TABLE org_on.posthog.events SET PROPERTIES extra_properties = MAP(ARRAY['a'], ARRAY['b'])");
        for (String statement : readOnlyRefusals) {
            assertTrinoExceptionThrownBy(() -> queryRunner.execute(statement))
                    .as(statement)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("The posthog schema is read-only");
        }
        // Refused by the engine or by connector defaults, for every schema
        List<String> otherRefusals = ImmutableList.of(
                "CREATE SCHEMA org_on.posthog",
                "DROP SCHEMA org_on.posthog",
                "ALTER SCHEMA org_on.posthog RENAME TO renamed",
                "CREATE VIEW org_on.posthog.v AS SELECT 1 x",
                "CREATE MATERIALIZED VIEW org_on.posthog.mv AS SELECT 1 x",
                "ALTER TABLE org_on.posthog.events EXECUTE optimize",
                "ANALYZE org_on.posthog.events",
                "ALTER TABLE org_on.posthog.events SET AUTHORIZATION someone",
                "ALTER TABLE org_on.posthog.events ALTER COLUMN event DROP NOT NULL");
        for (String statement : otherRefusals) {
            assertThatThrownBy(() -> queryRunner.execute(statement)).as(statement).isInstanceOf(RuntimeException.class);
        }
        // With no mount, CREATE SCHEMA reaches the connector, which refuses the name
        mount = Mount.MISSING;
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("CREATE SCHEMA org_on.posthog"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("The posthog schema is read-only");
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testOrgNamespacePosthogIsNeverReachedOrMerged()
    {
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactly("events");
        assertThat(column("DESCRIBE org_on.posthog.events")).containsExactly("team_id", "event");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasMessageContaining("Table 'org_on.posthog.shadow' does not exist");
        assertThat(column("SELECT table_name FROM org_on.information_schema.tables WHERE table_schema = 'posthog'")).containsExactly("events");

        // No mount: the user sees no schema posthog, never the org's
        mount = Mount.MISSING;
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.events"))
                .hasErrorCode(SCHEMA_NOT_FOUND);

        // Mount down: a typed error, never the org's
        refuseMount(503, "mount_host_unavailable");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT * FROM org_on.posthog.shadow"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SHOW TABLES FROM org_on.posthog"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testMissingMountLeavesTheCatalogWorking()
    {
        mount = Mount.MISSING;
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema");
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactly("analytics.orders");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema <> 'information_schema'"))
                .containsExactly("amount");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SHOW TABLES FROM org_on.posthog"))
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testUnavailableMountLeavesTheCatalogWorking()
    {
        refuseMount(503, "mount_host_unavailable");
        // Listed, since whether it exists is unknown, and other schemas answer as before
        assertThat(column("SHOW SCHEMAS FROM org_on")).containsExactlyInAnyOrder("analytics", "information_schema", "posthog");
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        assertThat(column("SELECT table_schema || '.' || table_name FROM org_on.information_schema.tables WHERE table_schema <> 'information_schema'"))
                .containsExactly("analytics.orders");
        assertThat(column("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema <> 'information_schema'"))
                .containsExactly("amount");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE)
                .hasMessage("The events host serving the posthog schema is unavailable; retry later");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT column_name FROM org_on.information_schema.columns WHERE table_schema = 'posthog'"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE);

        // The mount answers its catalog but fails planning
        mount = Mount.SERVED;
        refusalCodeOnScanOnly("mount_scan_busy");
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(HOGLAKE_CATALOG_UNAVAILABLE)
                .hasMessage("The Hoglake server is busy planning scans of the posthog schema; retry later");
        assertNoWriteAndNoShadowRequest();
    }

    @Test
    void testMountRefusingTheTokenKeepsTheOrgToken()
    {
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        requests.clear();
        refuseMount(401, "invalid_token");
        assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"));
        assertThat(column("SHOW SCHEMAS FROM org_on")).contains("analytics");
        assertThat(scalar("SELECT count(*) FROM org_on.analytics.orders")).isEqualTo(5L);
        // The mount's 401 neither discarded the shared token nor minted another
        assertThat(requests).noneMatch(request -> request.path().equals("/oauth/token"));
        assertThat(requests).filteredOn(request -> request.path().startsWith(MOUNT_PATH)).allMatch(request -> "Bearer token-for-client-on".equals(request.authorization()));
    }

    @Test
    void testMountServingSeveralNamespaces()
    {
        mount = Mount.TWO_NAMESPACES;
        assertThat(column("SHOW TABLES FROM org_on.posthog")).containsExactlyInAnyOrder("events", "sessions");
        assertThat(scalar("SELECT count(*) FROM org_on.posthog.sessions")).isEqualTo(7L);
        assertThat(requests).anyMatch(request -> request.path().equals(MOUNT_PATH + "/namespaces/other/tables/sessions/scan"));
        assertTrinoExceptionThrownBy(() -> queryRunner.execute("SELECT count(*) FROM org_on.posthog.events"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Table 'events' is ambiguous in the posthog schema");
    }

    @Test
    void testErrorMapping()
    {
        record Expected(int status, String code, ErrorCodeSupplier errorCode) {}

        List<Expected> cases = ImmutableList.of(
                new Expected(403, "read_only_catalog", NOT_SUPPORTED),
                new Expected(501, "mount_operation_unavailable", NOT_SUPPORTED),
                new Expected(422, "mount_scan_too_many_files", EXCEEDED_SCAN_LIMIT),
                new Expected(503, "mount_scan_timeout", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_scan_busy", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_host_unavailable", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_proxy_busy", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(502, "mount_host_invalid_response", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(503, "mount_exchange_unavailable", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(500, "mount_exchange_misconfigured", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(502, "mount_exchange_invalid_response", HOGLAKE_CATALOG_UNAVAILABLE),
                // Not typed: the status decides, as for any answer, still without the body or URL
                new Expected(500, "internal", HOGLAKE_CATALOG_UNAVAILABLE),
                new Expected(403, "forbidden", HOGLAKE_INVALID_RESPONSE));
        HoglakeClient client = HoglakeClient.forMount(uri, MOUNT);
        for (Expected expected : cases) {
            refuseMount(expected.status(), expected.code());
            for (Executable call : List.<Executable>of(
                    client::findCatalog,
                    client::listNamespaces,
                    () -> client.listTables("ingest"),
                    () -> client.getTable("ingest", "events", 42),
                    () -> client.planningScan("ingest", "events", 42, Set.of(1L)))) {
                assertThatThrownBy(call::execute)
                        .as(expected.code())
                        .isInstanceOfSatisfying(TrinoException.class, failure -> {
                            assertThat(failure.getErrorCode()).isEqualTo(expected.errorCode().toErrorCode());
                            assertThat(failure.getMessage())
                                    .doesNotContain(BODY_MARKER)
                                    .doesNotContain("127.0.0.1")
                                    .doesNotContain(MOUNT)
                                    .doesNotContain("http");
                        });
            }
        }
        // The org catalog's client is unchanged: no typed mapping, the body in the message as before
        refuseMount(403, "read_only_catalog");
        HoglakeClient orgShapedClient = new HoglakeClient(uri, MOUNT);
        assertTrinoExceptionThrownBy(() -> orgShapedClient.getTable("ingest", "events", 42))
                .hasErrorCode(HOGLAKE_INVALID_RESPONSE)
                .hasMessageContaining(BODY_MARKER);

        // A server that cannot be reached: no URL in the message, and no cause that could carry one
        HoglakeClient unreachable = HoglakeClient.forMount("http://127.0.0.1:9", MOUNT);
        assertThatThrownBy(unreachable::findCatalog)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(HOGLAKE_CATALOG_UNAVAILABLE.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("hoglake request failed: a request for the posthog schema");
                    assertThat(failure.getCause()).isNull();
                });
    }

    @Test
    void testWriteEntryPointsSendNothing()
    {
        HoglakeClient orgClient = new HoglakeClient(uri, ORG);
        HoglakePosthogSchema posthogSchema = new HoglakePosthogSchema(ORG, HoglakeClient.forMount(uri, MOUNT));
        HoglakeMetadata metadata = new HoglakeMetadata(orgClient, null, Optional.of(posthogSchema));
        ConnectorSession session = session();
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle mountHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        // A handle claiming the schema without the mount, as no planning would build it
        HoglakeTableHandle schemaHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column));
        HoglakeTableHandle orgHandle = new HoglakeTableHandle("analytics", "orders", 7, "org-table", List.of(column));
        ConnectorTableMetadata created = new ConnectorTableMetadata(new SchemaTableName("posthog", "created"), List.of(new ColumnMetadata("x", BIGINT)));
        HoglakeWriteHandle writeHandle = new HoglakeWriteHandle("posthog", "events", "mount-table", 42, "s3://bucket/path/", List.of(column), List.of(column), Optional.of("operation"), Optional.empty(), List.of(), List.of(), true);
        TrinoPrincipal owner = new TrinoPrincipal(PrincipalType.USER, session.getUser());

        requests.clear();
        for (ConnectorTableHandle handle : List.<ConnectorTableHandle>of(mountHandle, schemaHandle)) {
            List<Executable> writes = ImmutableList.of(
                    () -> metadata.dropTable(session, handle),
                    () -> metadata.truncateTable(session, handle),
                    () -> metadata.renameTable(session, handle, new SchemaTableName("posthog", "renamed")),
                    () -> metadata.addColumn(session, handle, new ColumnMetadata("added", BIGINT), new ColumnPosition.Last()),
                    () -> metadata.dropColumn(session, handle, column),
                    () -> metadata.renameColumn(session, handle, column, "renamed"),
                    () -> metadata.setColumnType(session, handle, column, BIGINT),
                    () -> metadata.setTableComment(session, handle, Optional.of("x")),
                    () -> metadata.setColumnComment(session, handle, column, Optional.of("x")),
                    () -> metadata.setTableProperties(session, handle, Map.of("extra_properties", Optional.of(Map.of()))),
                    () -> metadata.beginInsert(session, handle, List.<ColumnHandle>of(column), RetryMode.NO_RETRIES),
                    () -> metadata.beginMerge(session, handle, Map.of(), RetryMode.NO_RETRIES),
                    () -> metadata.finishMerge(session, new HoglakeDeleteHandle((HoglakeTableHandle) handle, "s3://bucket/path/", "operation"), List.of(), List.of(), List.of()));
            for (Executable write : writes) {
                assertReadOnly(write);
            }
        }
        List<Executable> writes = ImmutableList.of(
                () -> metadata.createSchema(session, "posthog", Map.of(), owner),
                () -> metadata.dropSchema(session, "posthog", false),
                () -> metadata.createTable(session, created, SaveMode.FAIL),
                () -> metadata.createTable(session, created, SaveMode.IGNORE),
                () -> metadata.beginCreateTable(session, created, Optional.empty(), RetryMode.NO_RETRIES, false),
                () -> metadata.beginCreateTable(session, created, Optional.empty(), RetryMode.NO_RETRIES, true),
                () -> metadata.renameTable(session, orgHandle, new SchemaTableName("posthog", "orders")),
                () -> metadata.finishInsert(session, writeHandle, List.of(), List.of(), List.of()),
                () -> metadata.finishCreateTable(session, writeHandle, List.of(), List.of()));
        for (Executable write : writes) {
            assertReadOnly(write);
        }
        // Out of the schema, too: refused as read-only, not only as a move between schemas
        assertReadOnly(() -> metadata.renameTable(session, mountHandle, new SchemaTableName("analytics", "moved")));
        // A mount handle is refused whatever schema name it carries
        HoglakeTableHandle relabeled = new HoglakeTableHandle("analytics", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        assertReadOnly(() -> metadata.beginInsert(session, relabeled, List.<ColumnHandle>of(column), RetryMode.NO_RETRIES));
        assertReadOnly(() -> metadata.dropTable(session, relabeled));
        assertThat(requests).isEmpty();

        // A configured name is refused in any spelling hoglake could hold
        assertReadOnly(() -> metadata.createSchema(session, "PostHog", Map.of(), owner));
        assertThat(requests).isEmpty();
    }

    @Test
    void testPlanningRoutesByTheHandle()
    {
        HoglakeClient orgClient = new HoglakeClient(uri, ORG);
        HoglakePosthogSchema posthogSchema = new HoglakePosthogSchema(ORG, HoglakeClient.forMount(uri, MOUNT));
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle otherMount = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable("1b5f0c2e-1111-4222-8333-444455556666-events", "ingest"));
        assertTrinoExceptionThrownBy(() -> HoglakeMetadata.clientFor(orgClient, Optional.of(posthogSchema), otherMount))
                .hasErrorCode(NOT_SUPPORTED);
        assertTrinoExceptionThrownBy(() -> HoglakeMetadata.clientFor(orgClient, Optional.empty(), otherMount.withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"))))
                .hasErrorCode(NOT_SUPPORTED);
        assertThat(requests).isEmpty();
    }

    @Test
    void testMountSnapshotIsPinnedPerTransaction()
    {
        HoglakeMetadata metadata = new HoglakeMetadata(new HoglakeClient(uri, ORG), null, Optional.of(new HoglakePosthogSchema(ORG, HoglakeClient.forMount(uri, MOUNT))));
        HoglakeMetadata transaction = metadata.newTransaction(false);
        SchemaTableName events = new SchemaTableName("posthog", "events");
        HoglakeTableHandle first = (HoglakeTableHandle) transaction.getTableHandle(session(), events, Optional.empty(), Optional.empty());
        mountHead = 43;
        HoglakeTableHandle second = (HoglakeTableHandle) transaction.getTableHandle(session(), events, Optional.empty(), Optional.empty());
        assertThat(first.snapshotId()).isEqualTo(42);
        assertThat(second.snapshotId()).isEqualTo(42);
        assertThat(second.mount()).contains(new HoglakeTableHandle.MountTable(MOUNT, "ingest"));
        assertThat(second.stagedFiles()).isEmpty();
        HoglakeTableHandle autocommit = (HoglakeTableHandle) metadata.newTransaction(true).getTableHandle(session(), events, Optional.empty(), Optional.empty());
        assertThat(autocommit.snapshotId()).isEqualTo(43);
        // System-table names are not asked for
        requests.clear();
        assertThat(metadata.newTransaction(true).getTableHandle(session(), new SchemaTableName("posthog", "events$files"), Optional.empty(), Optional.empty())).isNull();
        assertThat(requests).isEmpty();
    }

    @Test
    void testTableHandleJson()
    {
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(new BlockEncodingSimdSupport(new FeaturesConfig())), TESTING_TYPE_MANAGER);
        JsonCodec<HoglakeTableHandle> codec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)))
                .withJsonDeserializers(Map.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde))).get())
                .jsonCodec(HoglakeTableHandle.class);
        HoglakeColumnHandle column = HoglakeMetadata.toColumnHandle(new HoglakeDtos.Column(1, 0, "team_id", "long", null, true));
        HoglakeTableHandle orgHandle = new HoglakeTableHandle("analytics", "orders", 7, "org-table", List.of(column));
        HoglakeTableHandle mountHandle = new HoglakeTableHandle("posthog", "events", 42, "mount-table", List.of(column))
                .withMount(new HoglakeTableHandle.MountTable(MOUNT, "ingest"))
                .withCountOnly();
        assertThat(codec.fromJson(codec.toJson(orgHandle))).isEqualTo(orgHandle);
        assertThat(codec.fromJson(codec.toJson(mountHandle))).isEqualTo(mountHandle);
        assertThat(codec.fromJson(codec.toJson(mountHandle)).namespace()).isEqualTo("ingest");
        assertThat(mountHandle.toString()).isEqualTo("posthog.events@42 mount countOnly");
        // A handle from a build without the posthog schema has no mount
        String withoutMount = codec.toJson(orgHandle).replaceAll(",\\s*\"mount\"\\s*:\\s*null", "");
        assertThat(withoutMount).doesNotContain("\"mount\"");
        assertThat(codec.fromJson(withoutMount)).isEqualTo(orgHandle);
        assertThatThrownBy(() -> mountHandle.withStaged(List.of(), List.of(new HoglakeDtos.DeleteFile(1, 1, "s3://bucket/dv", 1, 1, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertReadOnly(Executable write)
    {
        assertThatThrownBy(write::execute)
                .isInstanceOfSatisfying(TrinoException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(failure.getMessage()).isEqualTo("The posthog schema is read-only");
                });
    }

    private void assertNoWriteAndNoShadowRequest()
    {
        Predicate<Request> write = request -> !request.method().equals("GET") && !request.path().equals("/oauth/token");
        assertThat(requests).noneMatch(write);
        assertThat(requests).noneMatch(request -> request.path().startsWith(SHADOW_PATH));
    }

    private void refuseMount(int status, String code)
    {
        mount = Mount.REFUSING;
        refusalStatus = status;
        refusalCode = code;
    }

    private void refusalCodeOnScanOnly(String code)
    {
        refusalStatus = 503;
        refusalCode = code;
    }

    private List<Object> column(String sql)
    {
        MaterializedResult result = queryRunner.execute(sql);
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .toList();
    }

    private Object scalar(String sql)
    {
        List<MaterializedRow> rows = queryRunner.execute(sql).getMaterializedRows();
        assertThat(rows).hasSize(1);
        return rows.getFirst().getField(0);
    }

    // ---- the fake server ---------------------------------------------------

    private void handle(HttpExchange exchange)
            throws IOException
    {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = Optional.ofNullable(exchange.getRequestURI().getRawQuery()).orElse("");
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        requests.add(new Request(method, path, query, authorization));
        exchange.getRequestBody().readAllBytes();
        if (path.equals("/oauth/token")) {
            // One token per client: which catalog's token a request carries is visible
            String credentials = new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())), UTF_8);
            String clientId = credentials.substring(0, credentials.indexOf(':'));
            respond(exchange, 200, "{\"access_token\":\"token-for-%s\", \"token_type\":\"Bearer\", \"expires_in\":3600}".formatted(clientId));
            return;
        }
        if (path.equals(MOUNT_PATH) || path.startsWith(MOUNT_PATH + "/")) {
            mountResponse(exchange, method, path.substring(MOUNT_PATH.length()));
            return;
        }
        if (path.equals(ORG_PATH) || path.startsWith(ORG_PATH + "/")) {
            orgResponse(exchange, method, path.substring(ORG_PATH.length()));
            return;
        }
        notFound(exchange);
    }

    private void mountResponse(HttpExchange exchange, String method, String route)
            throws IOException
    {
        Mount state = mount;
        if (state == Mount.MISSING) {
            notFound(exchange);
            return;
        }
        if (state == Mount.REFUSING || (refusalCode != null && route.endsWith("/scan"))) {
            refuse(exchange);
            return;
        }
        if (!method.equals("GET")) {
            respond(exchange, 403, "{\"error\":\"read_only_catalog\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
            return;
        }
        boolean two = state == Mount.TWO_NAMESPACES;
        switch (route) {
            case "" -> respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://mount-bucket/\", \"head_snapshot_id\":%d, \"schema_version\":1, \"read_only\":true}".formatted(MOUNT, mountHead));
            case "/namespaces" -> respond(exchange, 200, two
                    ? "[{\"name\":\"ingest\", \"namespace_id\":1}, {\"name\":\"other\", \"namespace_id\":2}]"
                    : "[{\"name\":\"ingest\", \"namespace_id\":1}]");
            case "/namespaces/ingest/tables" -> respond(exchange, 200, "[{\"name\":\"events\"}]");
            case "/namespaces/other/tables" -> respond(exchange, two ? 200 : 404, two ? "[{\"name\":\"events\"}, {\"name\":\"sessions\"}]" : "{\"error\":\"not_found\"}");
            case "/namespaces/ingest/tables/events" -> respond(exchange, 200, eventsTable("events"));
            case "/namespaces/other/tables/events", "/namespaces/other/tables/sessions" -> {
                if (two) {
                    respond(exchange, 200, eventsTable(route.substring(route.lastIndexOf('/') + 1)));
                }
                else {
                    notFound(exchange);
                }
            }
            case "/namespaces/ingest/tables/events/scan", "/namespaces/other/tables/sessions/scan", "/namespaces/other/tables/events/scan" -> respond(exchange, 200, files("s3://mount-bucket/data/ingest/events/team_id=2/", 3, 4));
            default -> notFound(exchange);
        }
    }

    private void orgResponse(HttpExchange exchange, String method, String route)
            throws IOException
    {
        if (!method.equals("GET")) {
            respond(exchange, 409, "{\"error\":\"conflict\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
            return;
        }
        switch (route) {
            case "" -> respond(exchange, 200, "{\"name\":\"%s\", \"data_path\":\"s3://org-bucket/\", \"head_snapshot_id\":7, \"schema_version\":1, \"capabilities\":[]}".formatted(ORG));
            case "/namespaces" -> respond(exchange, 200, "[{\"name\":\"analytics\", \"namespace_id\":1}, {\"name\":\"posthog\", \"namespace_id\":2}]");
            case "/namespaces/analytics/tables" -> respond(exchange, 200, "[{\"name\":\"orders\"}]");
            case "/namespaces/posthog/tables" -> respond(exchange, 200, "[{\"name\":\"shadow\"}, {\"name\":\"events\"}]");
            case "/namespaces/analytics/tables/orders" -> respond(exchange, 200, orgTable("orders", "amount"));
            case "/namespaces/posthog/tables/shadow", "/namespaces/posthog/tables/events" -> respond(exchange, 200, orgTable(route.substring(route.lastIndexOf('/') + 1), "org_column"));
            case "/namespaces/analytics/tables/orders/scan", "/namespaces/posthog/tables/events/scan" -> respond(exchange, 200, files("s3://org-bucket/data/analytics/orders/", 5));
            default -> notFound(exchange);
        }
    }

    private static String eventsTable(String name)
    {
        return """
               {"name":"%s", "namespace":"ingest", "table_uuid":"mount-%s",
                "columns":[{"field_id":1, "ordinal":0, "name":"team_id", "type":"long", "nullable":true},
                           {"field_id":2, "ordinal":1, "name":"event", "type":"string", "nullable":true}],
                "record_count":0, "file_count":0, "file_size_bytes":0}
               """.formatted(name, name);
    }

    private static String orgTable(String name, String column)
    {
        return """
               {"name":"%s", "namespace":"org", "table_uuid":"org-%s",
                "columns":[{"field_id":1, "ordinal":0, "name":"%s", "type":"long", "nullable":true}],
                "record_count":0, "file_count":0, "file_size_bytes":0}
               """.formatted(name, name, column);
    }

    private static String files(String prefix, long... recordCounts)
    {
        StringBuilder body = new StringBuilder("[");
        for (int index = 0; index < recordCounts.length; index++) {
            if (index > 0) {
                body.append(',');
            }
            body.append(
                    """
                    {"data_file":{"data_file_id":%d, "path":"%s%d.parquet", "record_count":%d, "file_size_bytes":1000,
                     "file_format":"parquet", "begin_snapshot":1}}
                    """.formatted(index + 1, prefix, index, recordCounts[index]));
        }
        return body.append(']').toString();
    }

    private void refuse(HttpExchange exchange)
            throws IOException
    {
        respond(exchange, refusalStatus, "{\"error\":\"%s\", \"detail\":\"%s http://10.0.0.1:8080/v1/catalogs/%s\"}".formatted(refusalCode, BODY_MARKER, MOUNT));
    }

    private static void notFound(HttpExchange exchange)
            throws IOException
    {
        respond(exchange, 404, "{\"error\":\"not_found\", \"detail\":\"%s\"}".formatted(BODY_MARKER));
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException
    {
        byte[] bytes = body.getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    /**
     * Local test servers use ports 50000-50999 only.
     */
    private static HttpServer bindInPortRange()
            throws IOException
    {
        int start = ThreadLocalRandom.current().nextInt(1000);
        for (int attempt = 0; attempt < 1000; attempt++) {
            int port = 50000 + (start + attempt) % 1000;
            try {
                return HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
            }
            catch (BindException e) {
                // Taken; try the next one
            }
        }
        throw new IOException("No free port in 50000-50999");
    }
}
