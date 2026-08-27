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
package io.trino.dispatcher;

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestHogQlStatementResource
{
    private static final HeaderName REQUEST_USER_HEADER = HeaderName.of(TRINO_HEADERS.requestUser());
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(new JsonMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule()))
            .get())
            .jsonCodec(QueryResults.class);

    @Test
    public void testEndpointIsDisabledByDefault()
            throws Exception
    {
        HttpClient client = new JettyHttpClient();
        TestingTrinoServer server = TestingTrinoServer.create();
        try {
            StatusResponse response = client.execute(
                    preparePost()
                            .setUri(server.resolve("/v1/hogql"))
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .setBodyGenerator(createStaticBodyGenerator("SELECT 1", UTF_8))
                            .build(),
                    createStatusResponseHandler());

            assertThat(response.getStatusCode()).isEqualTo(404);
        }
        finally {
            closeAll(server, client);
        }
    }

    @Test
    public void testEnabledEndpointUsesStatementProtocol()
            throws Exception
    {
        HttpClient client = new JettyHttpClient();
        TestingTrinoServer server = TestingTrinoServer.builder()
                .setProperties(Map.of("hogql.enabled", "true"))
                .build();
        try {
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");

            List<QueryResults> hogqlResults = runToCompletion(client, server, "/v1/hogql", "SELECT 1");
            assertThat(hogqlResults.getFirst().getNextUri().getPath()).startsWith("/v1/statement/queued/");
            assertThat(rows(hogqlResults)).containsExactly(ImmutableList.of(1));

            List<QueryResults> tableScanResults = runToCompletion(client, server, "/v1/hogql", "SELECT nationkey FROM tpch.tiny.nation");
            assertThat(rows(tableScanResults)).hasSize(25);

            List<QueryResults> sqlResults = runToCompletion(client, server, "/v1/statement", "SELECT 2");
            assertThat(rows(sqlResults)).containsExactly(ImmutableList.of(2));

            List<QueryResults> invalidResults = runToCompletion(client, server, "/v1/hogql", "SELECT FROM");
            assertThat(invalidResults.getLast().getError().getErrorName()).isEqualTo("SYNTAX_ERROR");
        }
        finally {
            closeAll(server, client);
        }
    }

    private static List<QueryResults> runToCompletion(HttpClient client, TestingTrinoServer server, String path, String query)
    {
        ImmutableList.Builder<QueryResults> results = ImmutableList.builder();
        QueryResults current = client.execute(
                preparePost()
                        .setUri(server.resolve(path))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setBodyGenerator(createStaticBodyGenerator(query, UTF_8))
                        .build(),
                createJsonResponseHandler(QUERY_RESULTS_CODEC));
        results.add(current);

        while (current.getNextUri() != null) {
            current = client.execute(
                    prepareGet()
                            .setUri(current.getNextUri())
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_CODEC));
            results.add(current);
        }
        return results.build();
    }

    private static List<List<Object>> rows(List<QueryResults> results)
            throws Exception
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            for (QueryResults result : results) {
                if (result.getData() != null) {
                    rows.addAll(decoder.toRows(result));
                }
            }
        }
        return rows.build();
    }
}
