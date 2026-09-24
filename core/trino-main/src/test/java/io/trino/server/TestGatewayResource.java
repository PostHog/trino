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

import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.client.QueryResults;
import io.trino.execution.QueryIdGenerator;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.server.TestQueryResource.QUERY_RESULTS_JSON_CODEC;
import static io.trino.server.TestQueryResource.REQUEST_USER_HEADER;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGatewayResource
{
    @Test
    public void testQueuedQueryIsPresentWithoutDispatchOrHeartbeat()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.create(); HttpClient client = new JettyHttpClient()) {
            QueryResults results = submit(server, client);
            QueryId queryId = new QueryId(results.getId());
            assertThat(server.getDispatchManager().isQueryRegistered(queryId)).isFalse();
            for (int i = 0; i < 3; i++) {
                Lifecycle lifecycle = observe(server, client, results.getId());
                assertThat(lifecycle).isEqualTo(new Lifecycle(
                        results.getId(),
                        server.getInstance(Key.get(NodeInfo.class)).getNodeId(),
                        server.getInstance(Key.get(QueryIdGenerator.class)).getCoordinatorId(),
                        true));
            }
            assertThat(server.getDispatchManager().isQueryRegistered(queryId)).isFalse();
            assertThat(server.getQueryManager().hasQuery(queryId)).isFalse();
        }
    }

    @Test
    public void testCompletedQueryAndRetainedResultsRemainPresent()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.create(); HttpClient client = new JettyHttpClient()) {
            QueryResults results = submit(server, client);
            URI lastUri = null;
            while (results.getNextUri() != null) {
                lastUri = results.getNextUri();
                results = poll(client, lastUri);
                assertThat(observe(server, client, results.getId()).queryPresent()).isTrue();
            }
            assertThat(results.getError()).isNull();
            QueryId queryId = new QueryId(results.getId());
            var heartbeat = server.getQueryManager().getFullQueryInfo(queryId).getQueryStats().getLastHeartbeat();
            assertThat(observe(server, client, results.getId()).queryPresent()).isTrue();
            assertThat(server.getQueryManager().getFullQueryInfo(queryId).getQueryStats().getLastHeartbeat()).isEqualTo(heartbeat);
            assertThat(poll(client, lastUri).getId()).isEqualTo(results.getId());
        }
    }

    @Test
    public void testActuallyEvictedQueryBecomesAbsent()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(Map.of("query.max-history", "0"))
                .build(); HttpClient client = new JettyHttpClient()) {
            QueryResults results = submit(server, client);
            while (results.getNextUri() != null) {
                results = poll(client, results.getNextUri());
            }
            assertThat(results.getError()).isNull();
            String queryId = results.getId();
            assertEventually(new Duration(15, SECONDS),
                    () -> assertThat(observe(server, client, queryId).queryPresent()).isFalse());
            assertThat(server.getDispatchManager().isQueryRegistered(new QueryId(queryId))).isFalse();
            assertThat(server.getQueryManager().hasQuery(new QueryId(queryId))).isFalse();
        }
    }

    @Test
    public void testRetainedResultOutlivesQueryMetadata()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(Map.of("query.max-history", "0"))
                .build(); HttpClient client = new JettyHttpClient()) {
            server.getInstance(Key.get(ExecutingStatementResource.class)).stop();
            QueryResults results = submit(server, client);
            URI lastUri = null;
            while (results.getNextUri() != null) {
                lastUri = results.getNextUri();
                results = poll(client, lastUri);
            }
            assertThat(results.getError()).isNull();
            QueryId queryId = new QueryId(results.getId());
            assertEventually(new Duration(15, SECONDS), () -> {
                assertThat(server.getDispatchManager().isQueryRegistered(queryId)).isFalse();
                assertThat(server.getQueryManager().hasQuery(queryId)).isFalse();
            });
            assertThat(observe(server, client, queryId.toString()).queryPresent()).isTrue();
            assertThat(poll(client, lastUri).getId()).isEqualTo(queryId.toString());
        }
    }

    @Test
    public void testInvalidAndForeignQueryIdsAreNotAbsenceEvidence()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.create(); HttpClient client = new JettyHttpClient()) {
            assertThat(status(server, client, "not-a-query", "observer")).isEqualTo(400);
            assertThat(status(server, client, "20200101_000000_00000_other", "observer")).isEqualTo(409);
        }
    }

    @Test
    public void testRequiresManagementReadPermission()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setSystemAccessControl(new AllowAllSystemAccessControl()
                {
                    @Override
                    public void checkCanReadSystemInformation(Identity identity)
                    {
                        if (!identity.getUser().equals("observer")) {
                            denyReadSystemInformationAccess();
                        }
                    }
                })
                .build(); HttpClient client = new JettyHttpClient()) {
            String queryId = submit(server, client).getId();
            assertThat(status(server, client, queryId, "user")).isEqualTo(403);
            assertThat(status(server, client, queryId, "observer")).isEqualTo(200);
            assertThat(client.execute(prepareGet().setUri(lifecycleUri(server, queryId)).build(), createStatusResponseHandler()).getStatusCode()).isEqualTo(401);
        }
    }

    private static QueryResults submit(TestingTrinoServer server, HttpClient client)
    {
        return client.execute(preparePost()
                        .setUri(server.getBaseUrl().resolve("/v1/statement"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setBodyGenerator(createStaticBodyGenerator("SELECT 1", UTF_8)).build(),
                createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
    }

    private static QueryResults poll(HttpClient client, URI uri)
    {
        return client.execute(prepareGet().setUri(uri).setHeader(REQUEST_USER_HEADER, "user").build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
    }

    private static Lifecycle observe(TestingTrinoServer server, HttpClient client, String queryId)
    {
        return client.execute(prepareGet().setUri(lifecycleUri(server, queryId)).setHeader(REQUEST_USER_HEADER, "observer").build(), createJsonResponseHandler(jsonCodec(Lifecycle.class)));
    }

    private static int status(TestingTrinoServer server, HttpClient client, String queryId, String user)
    {
        return client.execute(prepareGet().setUri(lifecycleUri(server, queryId)).setHeader(REQUEST_USER_HEADER, user).build(), createStatusResponseHandler()).getStatusCode();
    }

    private static URI lifecycleUri(TestingTrinoServer server, String queryId)
    {
        return server.getBaseUrl().resolve("/v1/integrations/gateway/query/" + queryId + "/lifecycle");
    }

    public record Lifecycle(String queryId, String nodeId, String coordinatorId, boolean queryPresent) {}
}
