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
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorWritableTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.BACKEND_ID;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.ISSUER;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSharedConnector
{
    @TempDir
    Path directory;

    @Test
    void testFactoryUsesExplicitDefaultOffMode()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            for (Map<String, String> properties : List.of(
                    Map.of("hoglake.uri", fixture.endpoint().toString(), "hoglake.s3.region", "us-east-1"),
                    Map.of("hoglake.uri", fixture.endpoint().toString(), "hoglake.s3.region", "us-east-1", "hoglake.multi-tenant", "false"))) {
                Connector connector = create(properties);
                try {
                    assertThat(connector).isInstanceOf(HoglakeConnector.class);
                    assertThat(connector.getSessionProperties()).isNotEmpty();
                }
                finally {
                    connector.shutdown();
                }
            }
            assertThat(fixture.requests).isEmpty();
            fixture.assertHealthy();
        }
    }

    @Test
    void testFactoryRejectsMalformedModeBeforeBootstrap()
    {
        for (String value : List.of("TRUE", "False", " true", "false ", "", "1", "enabled")) {
            assertThatThrownBy(() -> create(Map.of("hoglake.multi-tenant", value)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("hoglake.multi-tenant must be exactly true or false");
        }
    }

    @Test
    void testSharedBootstrapRejectsLegacyAuthorityAndFilesystemProperties()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            Map<String, String> forbidden = ImmutableMap.<String, String>builder()
                    .put("hoglake.catalog", "fixed-catalog")
                    .put("hoglake.auth.token", "synthetic-fixed-token")
                    .put("hoglake.auth.token-file", directory.resolve("absent-token").toString())
                    .put("hoglake.auth.token-uri", fixture.endpoint() + "/oauth/token")
                    .put("hoglake.auth.client-id", "synthetic-client")
                    .put("hoglake.auth.client-secret", "synthetic-secret")
                    .put("hoglake.auth.audience", "hoglake")
                    .put("hoglake.posthog-schema.enabled", "false")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", "synthetic-access-key")
                    .put("s3.use-web-identity-token-credentials-provider", "true")
                    .put("hoglake.s3.region", "us-east-1")
                    .buildOrThrow();
            forbidden.forEach((name, value) -> {
                Map<String, String> properties = ImmutableMap.<String, String>builder()
                        .putAll(fixture.properties())
                        .put(name, value)
                        .buildOrThrow();
                assertThatThrownBy(() -> create(properties))
                        .isInstanceOf(RuntimeException.class)
                        .hasStackTraceContaining(name);
            });
            assertThat(fixture.requests).isEmpty();
            fixture.assertHealthy();
        }
    }

    @Test
    void testSharedMetadataUsesOnlyApprovedRoutesWithDistinctCallers()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            Connector connector = create(fixture.properties());
            try {
                assertThat(connector).isInstanceOf(HoglakeSharedConnector.class);
                assertThat(connector.getSessionProperties()).isEmpty();
                assertThat(connector.getProcedures()).isEmpty();
                assertThat(connector.getTableProcedures()).isEmpty();
                assertThat(connector.getTableFunctions()).isEmpty();
                assertThat(fixture.requests).isEmpty();
                String firstToken = fixture.jwt("catalog-one", "synthetic-one.login-one");
                String secondToken = fixture.jwt("catalog-two", "synthetic-two.login-two");
                ConnectorSession first = session("synthetic-one.mapped-one", "synthetic-one.login-one", firstToken);
                ConnectorSession second = session("synthetic-two.mapped-two", "synthetic-two.login-two", secondToken);
                ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, true, true);
                ConnectorMetadata metadata = connector.getMetadata(first, transaction);
                assertThat(connector.getMetadata(second, transaction)).isNotSameAs(metadata);
                assertThat(metadata.listSchemaNames(first)).containsExactly("team_7");
                assertThat(metadata.schemaExists(first, "team_7")).isTrue();
                assertThat(metadata.listTables(second, Optional.empty())).containsExactly(new SchemaTableName("team_7", "events"));
                var columns = ImmutableList.copyOf(metadata.streamRelationColumns(first, Optional.empty(), UnaryOperator.identity()));
                assertThat(columns).singleElement().satisfies(table -> {
                    assertThat(table.name()).isEqualTo(new SchemaTableName("team_7", "events"));
                    assertThat(table.tableColumns().orElseThrow()).singleElement().satisfies(column -> {
                        assertThat(column.getName()).isEqualTo("value");
                        assertThat(column.getType().getDisplayName()).isEqualTo("bigint");
                        assertThat(column.getComment()).contains("synthetic column");
                    });
                });
                var comments = ImmutableList.copyOf(metadata.streamRelationComments(second, Optional.of("team_7"), UnaryOperator.identity()));
                assertThat(comments).singleElement().satisfies(comment -> assertThat(comment.comment()).contains("synthetic table"));
                assertThat(metadata.listTableColumns(first, new SchemaTablePrefix("team_7")))
                        .containsKey(new SchemaTableName("team_7", "events"));
                assertThat(fixture.requests).allSatisfy(request -> {
                    assertThat(request.method()).isEqualTo("GET");
                    if (request.path().startsWith("/v1/catalogs/catalog-one/")) {
                        assertThat(request.authorization()).isEqualTo("Bearer " + firstToken);
                    }
                    else {
                        assertThat(request.path()).startsWith("/v1/catalogs/catalog-two/");
                        assertThat(request.authorization()).isEqualTo("Bearer " + secondToken);
                    }
                });
                int requests = fixture.requests.size();
                connector.commit(transaction);
                connector.rollback(transaction);
                assertThat(fixture.requests).hasSize(requests);
                assertThat(transaction.toString()).doesNotContain(firstToken, secondToken);
                fixture.assertHealthy();
            }
            finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void testMetadataRejectsMissingInvalidExpiredAndChangedAuthorityWithoutHttp()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            Connector connector = create(fixture.properties());
            try {
                String jwt = fixture.jwt("catalog-one", "synthetic-one.login-one");
                ConnectorSession valid = session("synthetic-one.mapped-one", "synthetic-one.login-one", jwt);
                ConnectorMetadata metadata = connector.getMetadata(valid, connector.beginTransaction(READ_COMMITTED, true, true));
                Map<String, Object> expiredClaims = claims("catalog-one", "synthetic-one.login-one", "route-one", Instant.now().minusSeconds(1200));
                String expired = token(fixture.key, "synthetic-key", expiredClaims);
                for (ConnectorSession invalid : List.of(
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", null),
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", "invalid-jwt"),
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", expired),
                        session("synthetic-one.mapped-one", null, jwt),
                        session("synthetic-one.mapped-one", "synthetic-one.changed-login", jwt))) {
                    assertError(() -> connector.getMetadata(invalid, null), PERMISSION_DENIED);
                    assertError(() -> metadata.schemaExists(invalid, null), PERMISSION_DENIED);
                    assertError(() -> metadata.listSchemaNames(invalid), PERMISSION_DENIED);
                    assertError(() -> metadata.listTables(invalid, Optional.empty()), PERMISSION_DENIED);
                    assertError(() -> metadata.streamRelationColumns(invalid, Optional.empty(), UnaryOperator.identity()), PERMISSION_DENIED);
                    assertError(() -> metadata.streamRelationComments(invalid, Optional.empty(), UnaryOperator.identity()), PERMISSION_DENIED);
                    assertError(() -> metadata.getTableHandle(invalid, null, Optional.empty(), Optional.empty()), PERMISSION_DENIED);
                    assertError(() -> metadata.listTableColumns(invalid, null), PERMISSION_DENIED);
                    assertMetadataExecutionDenied(metadata, invalid, PERMISSION_DENIED);
                }
                assertError(() -> metadata.getTableHandle(valid, null, Optional.empty(), Optional.empty()), NOT_SUPPORTED);
                assertError(() -> metadata.listTableColumns(valid, new SchemaTablePrefix("team_7", "events")), NOT_SUPPORTED);
                assertMetadataExecutionDenied(metadata, valid, NOT_SUPPORTED);
                assertError(() -> metadata.createSchema(valid, "synthetic-schema", Map.of(), null), NOT_SUPPORTED);
                assertThat(fixture.requests).isEmpty();
                fixture.assertHealthy();
            }
            finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void testWorkerProvidersVerifyBeforeInspectingHandlesOrShortcuts()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory)) {
            Connector coordinator = create(fixture.properties());
            Connector worker = create(fixture.properties());
            try {
                String jwt = fixture.jwt("catalog-one", "synthetic-one.login-one");
                ConnectorSession valid = session("synthetic-one.mapped-one", "synthetic-one.login-one", jwt);
                for (ConnectorSession caller : List.of(
                        valid,
                        session("synthetic-one.mapped-one", "synthetic-one.changed-login", jwt),
                        session("synthetic-one.mapped-one", null, jwt),
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", null),
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", "invalid-jwt"),
                        session("synthetic-one.mapped-one", "synthetic-one.login-one", token(
                                fixture.key,
                                "synthetic-key",
                                claims("catalog-one", "synthetic-one.login-one", "synthetic-route", Instant.now().minusSeconds(1200)))))) {
                    StandardErrorCode expected = NOT_SUPPORTED;
                    if (caller != valid) {
                        expected = PERMISSION_DENIED;
                    }
                    for (Connector connector : List.of(coordinator, worker)) {
                        assertError(() -> connector.getSplitManager().getSplits(null, caller, null, null, Constraint.alwaysFalse()), expected);
                        assertError(() -> connector.getSplitManager().getSplits(null, caller, (ConnectorTableFunctionHandle) null), expected);
                        assertError(() -> connector.getPageSourceProvider().createPageSource(null, caller, null, null, Optional.empty(), List.of(), null), expected);
                        assertError(() -> connector.getPageSourceProvider().createPageSource(null, caller, null, null, Optional.empty(), List.of(), null, null), expected);
                        assertError(() -> connector.getPageSinkProvider().createPageSink(null, caller, (ConnectorOutputTableHandle) null, Optional.empty(), null), expected);
                        assertError(() -> connector.getPageSinkProvider().createPageSink(null, caller, (ConnectorInsertTableHandle) null, Optional.empty(), null), expected);
                        assertError(() -> connector.getPageSinkProvider().createPageSink(null, caller, (ConnectorTableExecuteHandle) null, Optional.empty(), null), expected);
                        assertError(() -> connector.getPageSinkProvider().createMergeSink(null, caller, null, Optional.empty(), null, null), expected);
                    }
                }
                assertThat(fixture.requests).isEmpty();
                fixture.assertHealthy();
            }
            finally {
                coordinator.shutdown();
                worker.shutdown();
            }
        }
    }

    private static Connector create(Map<String, String> properties)
    {
        return new HoglakeConnectorFactory().create("synthetic-shared", properties, new TestingConnectorContext());
    }

    private static void assertMetadataExecutionDenied(ConnectorMetadata metadata, ConnectorSession session, StandardErrorCode code)
    {
        assertError(() -> metadata.getTableMetadata(session, null), code);
        assertError(() -> metadata.getTableSchema(session, null), code);
        assertError(() -> metadata.getTableName(session, null), code);
        assertError(() -> metadata.getColumnHandles(session, null), code);
        assertError(() -> metadata.getColumnMetadata(session, null, null), code);
        assertError(() -> metadata.getTableProperties(session, null), code);
        assertError(() -> metadata.getTableStatistics(session, null), code);
        assertError(() -> metadata.applyLimit(session, null, 0), code);
        assertError(() -> metadata.applyFilter(session, null, Constraint.alwaysFalse()), code);
        assertError(() -> metadata.applyProjection(session, null, List.of(), Map.of()), code);
        assertError(() -> metadata.applyAggregation(session, null, List.of(), Map.of(), List.of(List.of())), code);
        assertError(() -> metadata.applyTopN(session, null, 0, List.of(), Map.of()), code);
        assertError(() -> metadata.validateScan(session, null), code);
        assertError(() -> metadata.beginCreateTable(session, null, Optional.empty(), null, false), code);
        assertError(() -> metadata.finishCreateTable(session, null, null, null), code);
        assertError(() -> metadata.beginInsert(session, null, List.of(), null), code);
        assertError(() -> metadata.finishInsert(session, null, null, null, null), code);
        assertError(() -> metadata.beginMerge(session, null, null, null), code);
        assertError(() -> metadata.finishMerge(session, null, null, null, null), code);
        assertError(() -> metadata.finishMerge(session, null, null, null, null, null), code);
        assertError(() -> metadata.executeUpdate(session, null), code);
        assertError(() -> metadata.executeDelete(session, null), code);
        assertError(() -> metadata.beginTableExecute(session, null, null), code);
        assertError(() -> metadata.executeTableExecute(session, null), code);
        assertError(() -> metadata.finishTableExecute(session, null, null, null), code);
        assertError(() -> metadata.getTableCredentials(session, (ConnectorTableHandle) null), code);
        assertError(() -> metadata.getTableCredentials(session, (ConnectorWritableTableHandle) null), code);
        assertError(() -> metadata.getTableCredentials(session, (ConnectorTableFunctionHandle) null), code);
    }

    private static void assertError(Runnable operation, StandardErrorCode code)
    {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(code.toErrorCode());
                    assertThat(exception.getMessage()).doesNotContain("Bearer", "eyJ", "invalid-jwt");
                });
    }

    private static final class Fixture
            implements AutoCloseable
    {
        private final KeyPair key = keyPair();
        private final HttpServer server;
        private final Path jwks;
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private final AtomicReference<AssertionError> failure = new AtomicReference<>();
        private final AtomicReference<IOException> disconnect = new AtomicReference<>();

        public Fixture(Path directory)
                throws IOException
        {
            jwks = directory.resolve("synthetic-public.jwks");
            writeJwks(jwks, key, "synthetic-key");
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::respond);
            server.start();
        }

        public URI endpoint()
        {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        }

        public Map<String, String> properties()
        {
            return Map.of(
                    "hoglake.uri", endpoint().toString(),
                    "hoglake.multi-tenant", "true",
                    "hoglake.shared.jwks-file", jwks.toString(),
                    "hoglake.shared.issuer", ISSUER,
                    "hoglake.shared.backend-bindings", "{\"%s\":\"hoglite\"}".formatted(BACKEND_ID));
        }

        public String jwt(String catalog, String principal)
        {
            return token(key, "synthetic-key", claims(catalog, principal, "synthetic-route", Instant.now()));
        }

        public void assertHealthy()
        {
            assertThat(failure.get()).isNull();
            assertThat(disconnect.get()).isNull();
        }

        @Override
        public void close()
        {
            server.stop(0);
        }

        private void respond(HttpExchange exchange)
        {
            try (exchange) {
                String path = exchange.getRequestURI().getPath();
                requests.add(new Request(exchange.getRequestMethod(), path, exchange.getRequestHeaders().getFirst("Authorization")));
                assertThat(exchange.getRequestMethod()).isEqualTo("GET");
                String response;
                if (path.matches("/v1/catalogs/catalog-(one|two)/namespaces")) {
                    assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                    response = "[{\"name\":\"team_7\",\"namespace_id\":7}]";
                }
                else if (path.matches("/v1/catalogs/catalog-(one|two)/namespaces/team_7/tables")) {
                    assertThat(exchange.getRequestURI().getRawQuery()).isNull();
                    response = "[{\"name\":\"events\",\"table_uuid\":\"synthetic-table\"}]";
                }
                else {
                    assertThat(path).matches("/v1/catalogs/catalog-(one|two)/namespaces/team_7/tables/events");
                    assertThat(exchange.getRequestURI().getRawQuery()).isEqualTo("totals=false");
                    response =
                            """
                            {"name":"events","namespace":"team_7","table_uuid":"synthetic-table",
                             "columns":[{"field_id":1,"ordinal":0,"name":"value","type":"long","nullable":true,"comment":"synthetic column"}],
                             "comment":"synthetic table"}
                            """;
                }
                byte[] body = response.getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            catch (AssertionError error) {
                failure.compareAndSet(null, error);
            }
            catch (IOException error) {
                disconnect.compareAndSet(null, error);
            }
        }
    }

    private record Request(String method, String path, String authorization)
    {
        @Override
        public String toString()
        {
            return "Request{redacted}";
        }
    }
}
