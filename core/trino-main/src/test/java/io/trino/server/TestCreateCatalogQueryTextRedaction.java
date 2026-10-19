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
package io.trino.server;

import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.execution.CatalogPropertyRedaction.REDACTED_STATEMENT;
import static io.trino.server.TestQueryResource.QUERY_RESULTS_JSON_CODEC;
import static io.trino.server.TestQueryResource.REQUEST_USER_HEADER;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * The text Trino keeps about a {@code CREATE CATALOG} query - query info, query listings and
 * query events - must never contain a catalog property value, whether the statement succeeds,
 * fails, cannot be parsed, or is executed from a string.
 */
@TestInstance(PER_CLASS)
public class TestCreateCatalogQueryTextRedaction
{
    private static final String SECRET = "hunter2-synthetic-secret";
    private static final HeaderName PREPARED_STATEMENT_HEADER = HeaderName.of(TRINO_HEADERS.requestPreparedStatement());

    private final Map<String, QueryCreatedEvent> createdEvents = new ConcurrentHashMap<>();
    private final Map<String, QueryCompletedEvent> completedEvents = new ConcurrentHashMap<>();
    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeAll
    public void setUp()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setEventListeners(List.of(new EventListener()
                {
                    @Override
                    public void queryCreated(QueryCreatedEvent event)
                    {
                        createdEvents.put(event.getMetadata().getQueryId(), event);
                    }

                    @Override
                    public void queryCompleted(QueryCompletedEvent event)
                    {
                        completedEvents.put(event.getMetadata().getQueryId(), event);
                    }
                }))
                .build();
        server.installPlugin(new TpchPlugin());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testSuccessfulCreateCatalog()
    {
        // tpch.splits-per-node stands in for a credential: its value must not be recorded
        QueryResults results = run("CREATE CATALOG redaction_ok USING tpch WITH (\"tpch.splits-per-node\" = '7171', \"tpch.partitioning-enabled\" = 'false')");
        assertThat(results.getError()).isNull();
        String recorded = assertRedacted(results.getId(), "7171");
        assertThat(recorded)
                .contains("CREATE CATALOG redaction_ok USING tpch")
                .contains("\"tpch.splits-per-node\" = '***'")
                .doesNotContain("'false'");
        // Execution saw the real values
        assertThat(run("SELECT count(*) FROM redaction_ok.tiny.nation").getError()).isNull();
    }

    @Test
    public void testFailingCreateCatalog()
    {
        QueryResults results = run("CREATE CATALOG redaction_failing USING tpch WITH (\"connection-password\" = '" + SECRET + "')");
        assertThat(results.getError()).isNotNull();
        assertThat(results.getError().getMessage()).doesNotContain(SECRET);
        assertThat(assertRedacted(results.getId(), SECRET)).contains("\"connection-password\" = '***'");
    }

    @Test
    public void testUnparseableCreateCatalog()
    {
        QueryResults results = run("/* leading comment */ -- and another\n  create\n/* between */ catalog redaction_broken USING tpch WITH (\"connection-password\" = '" + SECRET + "' '" + SECRET + "')");
        QueryError error = results.getError();
        assertThat(error).isNotNull();
        assertThat(error.getErrorName()).isEqualTo("SYNTAX_ERROR");
        assertThat(error.getMessage()).doesNotContain(SECRET);
        assertThat(assertRedacted(results.getId(), SECRET)).isEqualTo(REDACTED_STATEMENT);
    }

    @Test
    public void testExecuteImmediate()
    {
        QueryResults results = run("EXECUTE IMMEDIATE 'CREATE CATALOG redaction_immediate USING tpch WITH (\"tpch.splits-per-node\" = ''8282'')'");
        assertThat(results.getError()).isNull();
        assertThat(assertRedacted(results.getId(), "8282"))
                .startsWith("EXECUTE IMMEDIATE")
                .contains("redaction_immediate")
                .contains("''***''");

        // Placeholders are not supported in catalog properties; the rejected statement is replaced as a whole
        results = run("EXECUTE IMMEDIATE 'CREATE CATALOG redaction_parameter USING tpch WITH (\"tpch.splits-per-node\" = ?)' USING '9393'");
        assertThat(results.getError()).isNotNull();
        assertThat(assertRedacted(results.getId(), "9393")).isEqualTo(REDACTED_STATEMENT);

        results = run("EXECUTE IMMEDIATE 'CREATE CATALOG redaction_immediate_failing USING tpch WITH (\"connection-password\" = ''" + SECRET + "'')'");
        assertThat(results.getError()).isNotNull();
        assertRedacted(results.getId(), SECRET);
    }

    @Test
    public void testPrepareIsRejected()
    {
        QueryResults results = run("PREPARE redaction_prepared FROM CREATE CATALOG redaction_prepared USING tpch WITH (\"connection-password\" = '" + SECRET + "')");
        assertThat(results.getError()).isNotNull();
        assertThat(results.getError().getErrorName()).isEqualTo("NOT_SUPPORTED");
        assertThat(results.getError().getMessage()).isEqualTo("CREATE CATALOG cannot be used in a prepared statement");
        assertRedacted(results.getId(), SECRET);
    }

    @Test
    public void testClientPreparedStatementIsRejected()
    {
        String prepared = "CREATE CATALOG redaction_header USING tpch WITH (\"connection-password\" = '" + SECRET + "')";
        for (String sql : List.of("EXECUTE redaction_header", "SELECT 1")) {
            QueryResults results = run(sql, "redaction_header=" + URLEncoder.encode(prepared, UTF_8));
            assertThat(results.getError()).isNotNull();
            assertThat(results.getError().getErrorName()).isEqualTo("NOT_SUPPORTED");
            assertRedacted(results.getId(), SECRET);
        }
        // A harmless client prepared statement still works
        QueryResults results = run("EXECUTE harmless", "harmless=" + URLEncoder.encode("SELECT 'create catalog'", UTF_8));
        assertThat(results.getError()).isNull();
    }

    @Test
    public void testOtherStatementsAreRecordedUnchanged()
    {
        String sql = "SELECT 'CREATE CATALOG is mentioned here' AS text";
        QueryResults results = run(sql);
        assertThat(results.getError()).isNull();
        assertThat(server.getDispatchManager().getQueryInfo(new QueryId(results.getId())).getQuery()).isEqualTo(sql);
    }

    /**
     * Asserts that no recorded form of the query contains the secret and returns its recorded text.
     */
    private String assertRedacted(String queryId, String secret)
    {
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(createdEvents).containsKey(queryId);
            assertThat(completedEvents).containsKey(queryId);
        });

        String recorded = server.getDispatchManager().getQueryInfo(new QueryId(queryId)).getQuery();
        assertThat(recorded).doesNotContain(secret);
        assertThat(server.getDispatchManager().getQueries())
                .filteredOn(info -> info.getQueryId().id().equals(queryId))
                .singleElement()
                .satisfies(info -> assertThat(info.getQuery()).isEqualTo(recorded));

        // Query info and query listing as served to clients and the web UI, session included
        assertThat(get("/v1/query/" + queryId)).doesNotContain(secret).contains(queryId);
        assertThat(get("/v1/query")).doesNotContain(secret).contains(queryId);

        QueryCreatedEvent created = createdEvents.get(queryId);
        assertThat(created.getMetadata().getQuery()).isEqualTo(recorded);
        assertThat(created.getMetadata().getPreparedQuery().orElse("")).doesNotContain(secret);

        QueryCompletedEvent completed = completedEvents.get(queryId);
        assertThat(completed.getMetadata().getQuery()).isEqualTo(recorded);
        assertThat(completed.getMetadata().getPreparedQuery().orElse("")).doesNotContain(secret);
        completed.getFailureInfo().ifPresent(failure -> {
            assertThat(failure.getFailureMessage().orElse("")).doesNotContain(secret);
            assertThat(failure.getFailuresJson()).doesNotContain(secret);
        });
        return recorded;
    }

    private String get(String path)
    {
        return client.execute(
                prepareGet()
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setUri(server.getBaseUrl().resolve(path))
                        .build(),
                createStringResponseHandler()).getBody();
    }

    private QueryResults run(String sql)
    {
        return run(sql, null);
    }

    private QueryResults run(String sql, String preparedStatement)
    {
        Request.Builder builder = preparePost()
                .setHeader(REQUEST_USER_HEADER, "user")
                .setUri(server.getBaseUrl().resolve("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8));
        if (preparedStatement != null) {
            builder.setHeader(PREPARED_STATEMENT_HEADER, preparedStatement);
        }
        QueryResults results = client.execute(builder.build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        while (results.getNextUri() != null) {
            results = client.execute(
                    prepareGet()
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .setUri(results.getNextUri())
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        }
        return results;
    }
}
