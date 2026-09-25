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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodecFactory;
import io.airlift.units.Duration;
import io.trino.client.QueryResults;
import io.trino.execution.QueryManager;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.server.TestQueryResource.JSON_MAPPER;
import static io.trino.server.TestQueryResource.QUERY_RESULTS_JSON_CODEC;
import static io.trino.server.TestQueryResource.REQUEST_USER_HEADER;
import static io.trino.testing.assertions.Assert.assertConsistently;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCompletedResultRetention
{
    private final HttpClient client = new JettyHttpClient();
    private TestingTrinoServer server;

    @AfterEach
    public void close()
            throws Exception
    {
        closeAll(server, client);
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMMIT", "ROLLBACK"})
    public void testOpenTransactionProtectsStartAndStatementResults(String completion)
            throws Exception
    {
        start(Map.of());
        CompletedResult begin = complete("START TRANSACTION", Optional.empty());
        TransactionId transaction = server.getInstance(Key.get(QueryManager.class)).getFullQueryInfo(begin.queryId()).getStartedTransactionId().orElseThrow();
        CompletedResult statement = complete("SELECT 1", Optional.of(transaction));
        CompletedResult independent = complete("SELECT 2", Optional.empty());

        assertConsistently(new Duration(8, SECONDS), new Duration(100, MILLISECONDS), () -> {
            assertThat(absent(begin.queryId())).isFalse();
            assertThat(absent(statement.queryId())).isFalse();
        });
        assertThat(absent(independent.queryId())).isTrue();
        assertThat(status(prepareGet().setUri(begin.resultUri()))).isEqualTo(200);
        assertThat(status(prepareGet().setUri(statement.resultUri()))).isEqualTo(200);

        complete(completion, Optional.of(transaction));
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(absent(begin.queryId())).isTrue();
            assertThat(absent(statement.queryId())).isTrue();
        });
    }

    @Test
    public void testTransactionTimeoutReleasesProtectedResults()
            throws Exception
    {
        start(Map.of("transaction.idle-timeout", "8s", "transaction.idle-check-interval", "100ms"));
        CompletedResult begin = complete("START TRANSACTION", Optional.empty());
        assertConsistently(new Duration(6, SECONDS), new Duration(100, MILLISECONDS), () -> assertThat(absent(begin.queryId())).isFalse());
        assertEventually(new Duration(10, SECONDS), () -> assertThat(absent(begin.queryId())).isTrue());
    }

    @Test
    public void testHeadRenewsOnlyAvailableResultTokens()
            throws Exception
    {
        start(Map.of());
        CompletedResult result = complete("SELECT 1", Optional.empty());
        assertThat(result.previousUri()).isNotEqualTo(result.resultUri());
        assertConsistently(new Duration(8, SECONDS), new Duration(100, MILLISECONDS), () -> {
            assertThat(status(prepareHead().setUri(result.resultUri()))).isEqualTo(200);
            assertThat(absent(result.queryId())).isFalse();
        });
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(status(prepareHead().setUri(result.previousUri()))).isEqualTo(404);
            assertThat(status(prepareGet().setUri(result.previousUri()))).isIn(404, 410);
            assertThat(absent(result.queryId())).isTrue();
        });
    }

    @Test
    public void testDisabledPolicyKeepsLegacyHeadBehavior()
    {
        server = TestingTrinoServer.create();
        CompletedResult result = complete("SELECT 1", Optional.empty());
        assertThat(result.previousUri().getPath()).contains("/executing/");
        assertThat(status(prepareHead().setUri(result.previousUri()))).isEqualTo(200);
        assertThat(status(prepareGet().setUri(result.previousUri()))).isEqualTo(410);
    }

    @Test
    public void testInvalidCapabilitiesAndManagementProbesDoNotRenewOrResurrectResults()
            throws Exception
    {
        start(Map.of());
        CompletedResult result = complete("SELECT 1", Optional.empty());
        String token = result.resultUri().getPath().substring(result.resultUri().getPath().lastIndexOf('/') + 1);
        URI invalid = result.resultUri().resolve("../invalid/" + token);
        assertEventually(new Duration(10, SECONDS), () -> {
            assertThat(status(prepareGet().setUri(invalid))).isEqualTo(404);
            assertThat(status(prepareHead().setUri(invalid))).isEqualTo(404);
            assertThat(status(prepareGet().setHeader(REQUEST_USER_HEADER, "user").setUri(server.resolve("/v1/query/" + result.queryId()))))
                    .isIn(200, 410);
            assertThat(absent(result.queryId())).isTrue();
        });
        assertConsistently(new Duration(2, SECONDS), new Duration(50, MILLISECONDS), () -> {
            assertThat(status(prepareGet().setUri(result.resultUri()))).isEqualTo(404);
            assertThat(status(prepareGet().setUri(result.queuedUri()))).isEqualTo(404);
            assertThat(absent(result.queryId())).isTrue();
        });
    }

    @Test
    public void testUnconsumedRunningQueryDoesNotExpire()
            throws Exception
    {
        start(Map.of());
        QueryResults results = submit("SELECT 1", Optional.empty());
        while (results.getNextUri().getPath().contains("/queued/")) {
            results = fetch(results.getNextUri());
        }
        QueryId queryId = new QueryId(results.getId());
        assertConsistently(new Duration(8, SECONDS), new Duration(100, MILLISECONDS), () -> {
            assertThat(absent(queryId)).isFalse();
            assertThat(server.getInstance(Key.get(QueryManager.class)).getQueryState(queryId).isDone()).isFalse();
        });
        while (results.getNextUri() != null) {
            results = fetch(results.getNextUri());
        }
        assertThat(results.getError()).isNull();
        assertEventually(new Duration(10, SECONDS), () -> assertThat(absent(queryId)).isTrue());
    }

    private void start(Map<String, String> properties)
    {
        var config = new java.util.HashMap<>(properties);
        config.put("query.completed-result-idle-timeout", "1s");
        config.put("query.max-history-age", "1s");
        config.put("query.max-history", "0");
        server = TestingTrinoServer.builder().setProperties(config).build();
    }

    private QueryResults submit(String sql, Optional<TransactionId> transaction)
    {
        return client.execute(
                preparePost()
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setHeader(TRINO_HEADERS.requestTransactionId(), transaction.map(TransactionId::toString).orElse("NONE"))
                        .setUri(server.resolve("/v1/statement"))
                        .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                        .build(),
                createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
    }

    private CompletedResult complete(String sql, Optional<TransactionId> transaction)
    {
        QueryResults results = submit(sql, transaction);
        URI queued = results.getNextUri();
        URI previous = null;
        URI uri = null;
        while (results.getNextUri() != null) {
            previous = uri;
            uri = results.getNextUri();
            results = fetch(uri);
        }
        assertThat(results.getError()).isNull();
        assertThat(uri).isNotNull();
        return new CompletedResult(new QueryId(results.getId()), queued, previous, uri);
    }

    private QueryResults fetch(URI uri)
    {
        return client.execute(prepareGet().setUri(uri).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
    }

    private int status(Request.Builder request)
    {
        return client.execute(request.build(), createStatusResponseHandler()).getStatusCode();
    }

    private boolean absent(QueryId queryId)
    {
        return client.execute(
                        prepareGet().setHeader(REQUEST_USER_HEADER, "user").setUri(server.resolve("/v1/query/" + queryId + "/drain-status")).build(),
                        createJsonResponseHandler(new JsonCodecFactory(JSON_MAPPER).jsonCodec(JsonNode.class)))
                .get("absent").asBoolean();
    }

    private record CompletedResult(QueryId queryId, URI queuedUri, URI previousUri, URI resultUri) {}
}
