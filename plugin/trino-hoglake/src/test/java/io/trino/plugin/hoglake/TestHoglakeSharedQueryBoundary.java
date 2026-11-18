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
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.Constraint;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.claims;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.config;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.keyPair;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.session;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.token;
import static io.trino.plugin.hoglake.HoglakeSharedSessionTestSupport.writeJwks;
import static io.trino.plugin.hoglake.HoglakeSharedSessionVerifier.SESSION_TOKEN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSharedQueryBoundary
{
    private static final String KEY_ID = "synthetic-query-key";
    private static final String DISCOVERY_ONLY = "Shared Hoglake supports metadata discovery only";
    private static final String INVALID_AUTHORITY = "Shared Hoglake session authority is invalid or unavailable";

    @TempDir
    Path directory;

    @Test
    void testRealQueriesKeepMappedCallersAndPhysicalCatalogsIsolated()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner();
                ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            assertSharedGraph(runner.getCoordinator().getConnector("hoglake"));
            assertThat(fixture.requests).isEmpty();
            Caller first = fixture.caller("a", "example.alice", "example.mapped_alice");
            Caller second = fixture.caller("b", "example.bob", "example.mapped_bob");

            fixture.concurrentRequests.set(new CountDownLatch(2));
            var firstSchemas = callers.submit(() -> runner.execute(first.querySession(), "SHOW SCHEMAS FROM hoglake"));
            var secondSchemas = callers.submit(() -> runner.execute(second.querySession(), "SHOW SCHEMAS FROM hoglake"));
            assertThat(firstSchemas.get(30, SECONDS).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("information_schema", first.schema());
            assertThat(secondSchemas.get(30, SECONDS).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("information_schema", second.schema());
            fixture.concurrentRequests.set(null);

            for (Caller caller : List.of(first, second, first, second)) {
                assertDiscovery(runner, caller);
            }
            for (Caller caller : List.of(first, second)) {
                assertThat(fixture.requests.stream()
                        .filter(request -> request.authorization().equals("Bearer " + caller.jwt()))
                        .map(Request::target)
                        .collect(toImmutableSet()))
                        .containsExactlyInAnyOrder(
                                caller.catalogPath() + "/namespaces",
                                caller.catalogPath() + "/namespaces/" + caller.schema() + "/tables",
                                caller.catalogPath() + "/namespaces/" + caller.schema() + "/tables/" + caller.table() + "?totals=false");
            }
        }
    }

    @Test
    void testTableAnalysisDeniesEvenQueriesThatNeedNoRows()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller caller = fixture.caller("a", "example.alice", "example.mapped_alice");
            for (String sql : List.of(
                    "SELECT * FROM hoglake.team_a.events_a",
                    "SELECT count(*) FROM hoglake.team_a.events_a",
                    "SELECT * FROM hoglake.team_a.events_a WHERE false",
                    "SELECT count(*) FROM hoglake.team_a.events_a WHERE false",
                    "SELECT * FROM hoglake.team_a.events_a LIMIT 0",
                    "SELECT count(*) FROM hoglake.team_a.events_a LIMIT 0",
                    "EXPLAIN SELECT * FROM hoglake.team_a.events_a WHERE false",
                    "EXPLAIN SELECT count(*) FROM hoglake.team_a.events_a")) {
                int requests = fixture.requests.size();
                assertThatThrownBy(() -> runner.execute(caller.querySession(), sql))
                        .as(sql)
                        .isInstanceOf(QueryFailedException.class)
                        .hasMessageContaining(DISCOVERY_ONLY);
                assertThat(fixture.requests).as(sql).hasSize(requests);
            }
        }
    }

    @Test
    void testUseCanCheckOneNamespaceWithoutListing()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner(true)) {
            Caller alice = fixture.caller("a", "example.alice", "example.mapped_alice");
            Caller bob = fixture.caller("b", "example.bob", "example.bob");
            runner.execute(alice.querySession(), "USE hoglake.team_a");
            runner.execute(bob.querySession(), "USE hoglake.team_b");
            assertThatThrownBy(() -> runner.execute(alice.querySession(), "USE hoglake.missing"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("Schema does not exist");
            assertThatThrownBy(() -> runner.execute(alice.querySession(), "USE hoglake.unavailable"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("Shared Hoglake service unavailable");
            assertThatThrownBy(() -> runner.execute(alice.querySession(), "USE hoglake.wrong_name"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("Invalid shared Hoglake metadata response");
            assertThat(fixture.requests.stream().map(Request::target)).containsExactly(
                    alice.catalogPath() + "/namespaces/team_a",
                    bob.catalogPath() + "/namespaces/team_b",
                    alice.catalogPath() + "/namespaces/missing",
                    alice.catalogPath() + "/namespaces/unavailable",
                    alice.catalogPath() + "/namespaces/wrong_name");
            assertThatThrownBy(() -> runner.execute(alice.querySession(), "SELECT count(*) FROM hoglake.team_a.events_a"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining(DISCOVERY_ONLY);
            assertThat(fixture.requests).hasSize(5);

            runner.execute(alice.querySession(), "SHOW SCHEMAS FROM hoglake");
            assertThat(fixture.requests.stream().map(Request::target)).endsWith(alice.catalogPath() + "/namespaces");
        }
    }

    @Test
    void testUseListsNamespacesByDefault()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller alice = fixture.caller("a", "example.alice", "example.alice");
            runner.execute(alice.querySession(), "USE hoglake.team_a");
            assertThat(fixture.requests.stream().map(Request::target)).containsExactly(alice.catalogPath() + "/namespaces");
        }
    }

    @Test
    void testDescribeExactJdbcAndMutationsNeverAcquireExecutionAuthority()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller caller = fixture.caller("a", "example.alice", "example.mapped_alice");
            for (String sql : List.of(
                    "DESCRIBE hoglake.team_a.events_a",
                    "SHOW CREATE TABLE hoglake.team_a.events_a",
                    "CREATE TABLE hoglake.team_a.new_table (value bigint)",
                    "CREATE TABLE hoglake.team_a.new_table AS SELECT BIGINT '1' AS value",
                    "DROP TABLE hoglake.team_a.events_a",
                    "ALTER TABLE hoglake.team_a.events_a ADD COLUMN other_value bigint",
                    "INSERT INTO hoglake.team_a.events_a VALUES (1)",
                    "UPDATE hoglake.team_a.events_a SET value_a = 1",
                    "DELETE FROM hoglake.team_a.events_a",
                    "MERGE INTO hoglake.team_a.events_a t USING (VALUES 1) s(value) ON false WHEN NOT MATCHED THEN INSERT (value_a) VALUES (s.value)",
                    "ALTER TABLE hoglake.team_a.events_a EXECUTE add_files(location => 's3://synthetic-bucket/data/')")) {
                assertThatThrownBy(() -> runner.execute(caller.querySession(), sql))
                        .as(sql)
                        .isInstanceOf(QueryFailedException.class)
                        .hasMessageContaining(DISCOVERY_ONLY);
            }

            assertThatThrownBy(() -> runner.execute(caller.querySession(), "CREATE SCHEMA hoglake.new_schema"))
                    .isInstanceOf(QueryFailedException.class)
                    .hasMessageContaining("This connector does not support creating schemas");

            // The engine converts exact-table listing failures into an empty result.
            int requests = fixture.requests.size();
            assertThat(runner.execute(caller.querySession(),
                    """
                    SELECT table_name FROM system.jdbc.tables
                    WHERE table_cat = 'hoglake' AND table_schem = 'team_a' AND table_name = 'events_a'
                    """).getMaterializedRows()).isEmpty();
            assertThat(runner.execute(caller.querySession(),
                    """
                    SELECT column_name FROM system.jdbc.columns
                    WHERE table_cat = 'hoglake' AND table_schem = 'team_a' AND table_name = 'events_a'
                    """).getMaterializedRows()).isEmpty();
            assertThat(fixture.requests).hasSize(requests);
            assertThat(fixture.requests).allSatisfy(request -> {
                assertThat(request.method()).isEqualTo("GET");
                assertThat(request.target()).isEqualTo(caller.catalogPath() + "/namespaces");
            });
        }
    }

    @Test
    void testRealQueriesRejectMissingInvalidExpiredAndChangedPrincipalWithoutHttp()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller caller = fixture.caller("a", "example.alice", "example.mapped_alice");
            assertDiscovery(runner, caller);
            String expired = token(fixture.key, KEY_ID, claims(caller.catalog(), caller.principal(), "route-a", Instant.now().minusSeconds(1200)));
            String wrongSignature = token(keyPair(), KEY_ID, claims(caller.catalog(), caller.principal(), "route-a", Instant.now()));
            String invalid = "synthetic-invalid-session-token";
            for (Identity identity : List.of(
                    identity(caller.mappedUser(), caller.principal(), null),
                    identity(caller.mappedUser(), caller.principal(), invalid),
                    identity(caller.mappedUser(), caller.principal(), expired),
                    identity(caller.mappedUser(), caller.principal(), wrongSignature),
                    identity(caller.mappedUser(), null, caller.jwt()),
                    identity(caller.mappedUser(), "example.changed_principal", caller.jwt()))) {
                Session querySession = querySession(identity, identity);
                for (String sql : List.of("SHOW SCHEMAS FROM hoglake", "SHOW TABLES FROM hoglake.team_a")) {
                    int requests = fixture.requests.size();
                    assertAuthorityDenied(
                            runner,
                            querySession,
                            sql,
                            caller.jwt(),
                            expired,
                            wrongSignature,
                            invalid,
                            caller.principal(),
                            caller.catalog(),
                            "route-a");
                    assertThat(fixture.requests).hasSize(requests);
                }
            }
            assertDiscovery(runner, caller);
        }
    }

    @Test
    void testDerivedEffectiveIdentitiesCannotBorrowAnOriginalToken()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller caller = fixture.caller("a", "example.alice", "example.mapped_alice");
            assertDiscovery(runner, caller);
            Identity original = identity(caller.mappedUser(), caller.principal(), caller.jwt());
            for (String effectiveUser : List.of("example.definer", "example.impersonated")) {
                Identity effective = Identity.from(original).withUser(effectiveUser).build();
                Session derived = querySession(effective, original);
                assertThat(derived.getIdentity().getExtraCredentials()).containsEntry(SESSION_TOKEN, caller.jwt());
                int requests = fixture.requests.size();
                assertAuthorityDenied(runner, derived, "SHOW SCHEMAS FROM hoglake", caller.jwt(), caller.principal(), caller.catalog());
                assertAuthorityDenied(runner, derived, "SELECT count(*) FROM hoglake.team_a.events_a", caller.jwt(), caller.principal(), caller.catalog());
                assertThat(fixture.requests).hasSize(requests);
            }
            assertDiscovery(runner, caller);
        }
    }

    @Test
    void testSeparateWorkerFactoryUsesItsOwnMountedPublicKeys()
            throws Exception
    {
        try (Fixture fixture = new Fixture(directory);
                StandaloneQueryRunner runner = fixture.queryRunner()) {
            Caller coordinator = fixture.caller("a", "example.alice", "example.mapped_alice");
            assertDiscovery(runner, coordinator);
            KeyPair workerKey = keyPair();
            Path workerJwks = directory.resolve("synthetic-worker-public.jwks");
            writeJwks(workerJwks, workerKey, KEY_ID);
            Caller worker = fixture.caller(workerKey, "b", "example.bob", "example.mapped_bob");
            Connector workerConnector = new HoglakeConnectorFactory().create(
                    "synthetic-worker", fixture.properties(workerJwks), new TestingConnectorContext());
            try {
                assertSharedGraph(workerConnector);
                ConnectorSession coordinatorSession = session(coordinator.mappedUser(), coordinator.principal(), coordinator.jwt());
                ConnectorSession workerSession = session(worker.mappedUser(), worker.principal(), worker.jwt());
                int requests = fixture.requests.size();
                assertThatThrownBy(() -> workerConnector.getSplitManager().getSplits(null, coordinatorSession, null, null, Constraint.alwaysFalse()))
                        .isInstanceOfSatisfying(TrinoException.class, exception -> {
                            assertThat(exception.getErrorCode()).isEqualTo(PERMISSION_DENIED.toErrorCode());
                            assertThat(exception.getMessage()).isEqualTo(INVALID_AUTHORITY);
                        });
                assertAuthorityDenied(runner, worker.querySession(), "SHOW SCHEMAS FROM hoglake", worker.jwt(), worker.principal(), worker.catalog());
                assertThat(fixture.requests).hasSize(requests);

                var transaction = workerConnector.beginTransaction(READ_COMMITTED, true, true);
                assertThat(workerConnector.getMetadata(workerSession, transaction).listSchemaNames(workerSession)).containsExactly(worker.schema());
                assertThatThrownBy(() -> workerConnector.getSplitManager().getSplits(transaction, workerSession, null, null, Constraint.alwaysFalse()))
                        .isInstanceOfSatisfying(TrinoException.class, exception -> {
                            assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                            assertThat(exception.getMessage()).isEqualTo(DISCOVERY_ONLY);
                        });
                requests = fixture.requests.size();
                workerConnector.commit(transaction);
                workerConnector.rollback(transaction);
                assertThat(fixture.requests).hasSize(requests);
            }
            finally {
                workerConnector.shutdown();
            }
        }
    }

    private static void assertDiscovery(StandaloneQueryRunner runner, Caller caller)
    {
        assertThat(runner.execute(caller.querySession(), "SHOW SCHEMAS FROM hoglake").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("information_schema", caller.schema());
        assertThat(runner.execute(caller.querySession(), "SHOW TABLES FROM hoglake." + caller.schema()).getOnlyColumnAsSet())
                .containsExactly(caller.table());
        assertThat(runner.execute(caller.querySession(),
                """
                SELECT table_schem, table_name, column_name, type_name
                FROM system.jdbc.columns
                WHERE table_cat = 'hoglake' AND table_schem = '%s'
                """.formatted(caller.schema())).getMaterializedRows())
                .extracting(MaterializedRow::getFields)
                .containsExactly(List.of(caller.schema(), caller.table(), caller.column(), "bigint"));
    }

    private static void assertAuthorityDenied(StandaloneQueryRunner runner, Session querySession, String sql, String... sensitiveValues)
    {
        assertThatThrownBy(() -> runner.execute(querySession, sql))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageContaining(INVALID_AUTHORITY)
                .satisfies(exception -> {
                    StringWriter trace = new StringWriter();
                    exception.printStackTrace(new PrintWriter(trace));
                    assertThat(trace.toString()).doesNotContain(sensitiveValues).doesNotContain("Bearer ");
                });
    }

    private static void assertSharedGraph(Connector connector)
    {
        // The runner can initialize unrelated services. These checks cover only the shared connector graph.
        assertThat(connector).isExactlyInstanceOf(HoglakeSharedConnector.class);
        assertThat(connector.getSessionProperties()).isEmpty();
        assertThat(connector.getProcedures()).isEmpty();
        assertThat(connector.getTableProcedures()).isEmpty();
        assertThat(connector.getTableFunctions()).isEmpty();
        assertThat(HoglakeSharedConnector.class.getDeclaredConstructors()).singleElement().satisfies(constructor ->
                assertThat(constructor.getParameterTypes()).containsExactly(
                        HoglakeSharedHttpClient.class, HoglakeSharedSessionVerifier.class, LifeCycleManager.class));
        assertThat(Arrays.stream(HoglakeSharedConnector.class.getDeclaredFields()).map(Field::getType).collect(toImmutableSet()))
                .containsExactlyInAnyOrder(
                        HoglakeSharedHttpClient.class,
                        HoglakeSharedSessionVerifier.class,
                        LifeCycleManager.class,
                        ConnectorSplitManager.class,
                        ConnectorPageSourceProvider.class,
                        ConnectorPageSinkProvider.class);
    }

    private static Identity identity(String mappedUser, String principal, String jwt)
    {
        Identity.Builder identity = Identity.forUser(mappedUser);
        if (principal != null) {
            identity.withPrincipal(new BasicPrincipal(principal));
        }
        if (jwt != null) {
            identity.withExtraCredentials(Map.of(SESSION_TOKEN, jwt));
        }
        return identity.build();
    }

    private static Session querySession(Identity effective, Identity original)
    {
        // Direct injection tests connector authority. It does not establish external authentication.
        return testSessionBuilder()
                .setCatalog("hoglake")
                .setSchema("team_a")
                .setIdentity(effective)
                .setOriginalIdentity(original)
                .build();
    }

    private record Caller(String catalog, String principal, String mappedUser, String schema, String table, String column, String jwt)
    {
        public Session querySession()
        {
            Identity identity = identity(mappedUser, principal, jwt);
            return TestHoglakeSharedQueryBoundary.querySession(identity, identity);
        }

        public String catalogPath()
        {
            return "/v1/catalogs/" + catalog;
        }
    }

    private record Request(String method, String target, String authorization) {}

    private static final class Fixture
            implements AutoCloseable
    {
        private final KeyPair key = keyPair();
        private final Path jwks;
        private final HttpServer server;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final Map<String, Caller> callers = new ConcurrentHashMap<>();
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private final AtomicReference<AssertionError> failure = new AtomicReference<>();
        private final AtomicReference<CountDownLatch> concurrentRequests = new AtomicReference<>();

        public Fixture(Path directory)
                throws IOException
        {
            jwks = directory.resolve("synthetic-query-public.jwks");
            writeJwks(jwks, key, KEY_ID);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        }

        public Caller caller(String suffix, String principal, String mappedUser)
        {
            return caller(key, suffix, principal, mappedUser);
        }

        public Caller caller(KeyPair signingKey, String suffix, String principal, String mappedUser)
        {
            String catalog = "catalog-" + suffix;
            String jwt = token(signingKey, KEY_ID, claims(catalog, principal, "route-" + suffix, Instant.now()));
            Caller caller = new Caller(catalog, principal, mappedUser, "team_" + suffix, "events_" + suffix, "value_" + suffix, jwt);
            callers.put("Bearer " + jwt, caller);
            return caller;
        }

        public StandaloneQueryRunner queryRunner()
        {
            return queryRunner(false);
        }

        public StandaloneQueryRunner queryRunner(boolean pointLookup)
        {
            StandaloneQueryRunner runner = new StandaloneQueryRunner(testSessionBuilder().setCatalog("hoglake").setSchema("team_a").build());
            try {
                runner.installPlugin(new HoglakePlugin());
                Map<String, String> properties = properties(jwks);
                if (pointLookup) {
                    properties = ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .put("hoglake.shared.namespace-point-lookup-enabled", "true")
                            .buildOrThrow();
                }
                runner.createCatalog("hoglake", "hoglake", properties);
                return runner;
            }
            catch (RuntimeException | Error exception) {
                runner.close();
                throw exception;
            }
        }

        public Map<String, String> properties(Path publicKeys)
        {
            HoglakeSharedConfig config = config(publicKeys, URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            return ImmutableMap.<String, String>builder()
                    .put("hoglake.multi-tenant", "true")
                    .put("hoglake.uri", config.getUri().toString())
                    .put("hoglake.shared.jwks-file", config.getJwksFile().getPath())
                    .put("hoglake.shared.issuer", config.getIssuer())
                    .put("hoglake.shared.backend-bindings", config.getBackendBindings())
                    .buildOrThrow();
        }

        private void handle(HttpExchange exchange)
        {
            try (exchange) {
                try {
                    respond(exchange);
                }
                catch (AssertionError error) {
                    failure.compareAndSet(null, error);
                    exchange.sendResponseHeaders(500, -1);
                }
            }
            catch (IOException _) {
                // A cancelled query can close its connection before the response.
            }
        }

        private void respond(HttpExchange exchange)
                throws IOException
        {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            String target = exchange.getRequestURI().toASCIIString();
            requests.add(new Request(exchange.getRequestMethod(), target, authorization));
            assertThat(exchange.getRequestMethod()).isEqualTo("GET");
            assertThat(exchange.getRequestHeaders().get("Authorization")).containsExactly(authorization);
            assertThat(authorization).isNotNull();
            Caller caller = callers.get(authorization);
            assertThat(caller).as("The request must use an exact registered JWT").isNotNull();
            assertThat(authorization).isEqualTo("Bearer " + caller.jwt());
            assertThat(exchange.getRequestBody().readAllBytes()).isEmpty();

            String namespacePath = caller.catalogPath() + "/namespaces/";
            if (target.equals(namespacePath + caller.schema()) || target.equals(namespacePath + "wrong_name")) {
                byte[] body = "{\"name\":\"%s\",\"namespace_id\":7}".formatted(caller.schema()).getBytes(UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                return;
            }
            if (target.equals(namespacePath + "missing")) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            if (target.equals(namespacePath + "unavailable")) {
                exchange.sendResponseHeaders(503, -1);
                return;
            }
            String body;
            if (target.equals(caller.catalogPath() + "/namespaces")) {
                awaitConcurrentRequests();
                body = "[{\"name\":\"%s\",\"namespace_id\":7}]".formatted(caller.schema());
            }
            else if (target.equals(caller.catalogPath() + "/namespaces/" + caller.schema() + "/tables")) {
                body = "[{\"name\":\"%s\",\"table_uuid\":\"synthetic-table\"}]".formatted(caller.table());
            }
            else {
                assertThat(target).isEqualTo(
                        caller.catalogPath() + "/namespaces/" + caller.schema() + "/tables/" + caller.table() + "?totals=false");
                body =
                        """
                        {"name":"%s","namespace":"%s","table_uuid":"synthetic-table",
                         "columns":[{"field_id":1,"ordinal":0,"name":"%s","type":"long","nullable":true}],
                         "record_count":0,"file_count":0,"file_size_bytes":0}
                        """.formatted(caller.table(), caller.schema(), caller.column());
            }
            byte[] bytes = body.getBytes(UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }

        private void awaitConcurrentRequests()
        {
            CountDownLatch concurrent = concurrentRequests.get();
            if (concurrent != null) {
                concurrent.countDown();
                try {
                    assertThat(concurrent.await(10, SECONDS)).as("Both caller queries must reach HTTP concurrently").isTrue();
                }
                catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted while waiting for concurrent queries", exception);
                }
            }
        }

        @Override
        public void close()
        {
            server.stop(0);
            executor.close();
            assertThat(failure.get()).as("The discovery fixture rejects scan, mutation, lease, cleanup, issuer, and storage requests").isNull();
        }
    }
}
