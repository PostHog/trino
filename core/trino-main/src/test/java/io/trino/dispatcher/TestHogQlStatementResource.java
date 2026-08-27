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
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

class TestHogQlStatementResource
{
    private static final HeaderName REQUEST_USER_HEADER = HeaderName.of(TRINO_HEADERS.requestUser());
    private static final HeaderName CONTENT_TYPE_HEADER = HeaderName.of("Content-Type");
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(new JsonMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule()))
            .get())
            .jsonCodec(QueryResults.class);
    private static final JsonCodec<String> STRING_CODEC = JsonCodec.jsonCodec(String.class);
    private static final String SECRET = "do-not-echo-this-value";

    private static HttpClient client;
    private static TestingTrinoServer server;

    @BeforeAll
    static void setUp()
            throws Exception
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setProperties(Map.of("hogql.enabled", "true"))
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterAll
    static void tearDown()
            throws Exception
    {
        closeAll(server, client);
    }

    @Test
    public void testEndpointIsDisabledByDefault()
            throws Exception
    {
        HttpClient disabledClient = new JettyHttpClient();
        TestingTrinoServer disabledServer = TestingTrinoServer.create();
        try {
            StatusResponse response = disabledClient.execute(
                    preparePost()
                            .setUri(disabledServer.resolve("/v1/hogql"))
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                            .setBodyGenerator(createStaticBodyGenerator(hogQlRequest("SELECT 1"), UTF_8))
                            .build(),
                    createStatusResponseHandler());

            assertThat(response.getStatusCode()).isEqualTo(404);
        }
        finally {
            closeAll(disabledServer, disabledClient);
        }
    }

    @Test
    public void testEnabledEndpointUsesStatementProtocol()
            throws Exception
    {
        List<QueryResults> hogqlResults = runHogQlToCompletion(hogQlRequestWithTypedValues("SELECT 1"));
        assertThat(hogqlResults.getFirst().getNextUri().getPath()).startsWith("/v1/statement/queued/");
        assertThat(rows(hogqlResults)).containsExactly(ImmutableList.of(1));

        List<QueryResults> tableScanResults = runHogQlToCompletion(hogQlRequest("SELECT nationkey FROM tpch.tiny.nation"));
        assertThat(rows(tableScanResults)).hasSize(25);

        List<QueryResults> sqlResults = runToCompletion("/v1/statement", "SELECT 2", false);
        assertThat(rows(sqlResults)).containsExactly(ImmutableList.of(2));
    }

    @Test
    public void testEndpointRequiresJsonContentType()
    {
        StringResponse response = post(hogQlRequest("SELECT 1"), "text/plain");

        assertThat(response.getStatusCode()).isEqualTo(415);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRequests")
    public void testInvalidRequestFailsClosed(String name, String request)
    {
        StringResponse response = post(request, APPLICATION_JSON);

        assertThat(response.getStatusCode()).isEqualTo(400);
        assertThat(response.getBody()).doesNotContain(SECRET);
        assertThat(response.getBody()).hasSizeLessThan(1024);
    }

    private static Stream<Arguments> invalidRequests()
    {
        String languageVersion = HogQlLanguageContract.current().languageVersion().toString();
        return Stream.of(
                Arguments.of("unsupported protocol version", """
                        {"query":"SELECT '%s'","protocolVersion":2,"languageVersion":"%s"}
                        """.formatted(SECRET, languageVersion)),
                Arguments.of("unsupported language version", """
                        {"query":"SELECT '%s'","protocolVersion":1,"languageVersion":"999.0.0"}
                        """.formatted(SECRET)),
                Arguments.of("unknown top-level field", """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","unknown":"%s"}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("unknown typed-value field", """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{"p":{"type":"string","value":"%s","unknown":true}}}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("ambiguous typed value", """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{"p":{"type":"string","unknown":"%s"}}}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("nonpositive catalog generation", """
                        {"query":"SELECT '%s'","protocolVersion":1,"languageVersion":"%s","catalogGeneration":0}
                        """.formatted(SECRET, languageVersion)),
                Arguments.of("too many parameter bindings", requestWithParameterCount(languageVersion, 1_001)),
                Arguments.of("malformed JSON", """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{"p":{"type":"string","value":"%s"}}
                        """.formatted(languageVersion, SECRET)));
    }

    private static String requestWithParameterCount(String languageVersion, int parameterCount)
    {
        String parameters = range(0, parameterCount)
                .mapToObj(index -> "\"p%s\":{\"type\":\"integer\",\"value\":%s}".formatted(index, index))
                .collect(joining(","));
        return """
                {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{%s}}
                """.formatted(languageVersion, parameters);
    }

    private static List<QueryResults> runHogQlToCompletion(String request)
    {
        return runToCompletion("/v1/hogql", request, true);
    }

    private static List<QueryResults> runToCompletion(String path, String body, boolean json)
    {
        ImmutableList.Builder<QueryResults> results = ImmutableList.builder();
        Request.Builder request = preparePost()
                .setUri(server.resolve(path))
                .setHeader(REQUEST_USER_HEADER, "user")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8));
        if (json) {
            request.setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON);
        }
        QueryResults current = client.execute(
                request.build(),
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

    private static StringResponse post(String body, String contentType)
    {
        return client.execute(
                preparePost()
                        .setUri(server.resolve("/v1/hogql"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setHeader(CONTENT_TYPE_HEADER, contentType)
                        .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                        .build(),
                createStringResponseHandler());
    }

    private static String hogQlRequest(String query)
    {
        return """
                {"query":%s,"protocolVersion":1,"languageVersion":"%s"}
                """.formatted(STRING_CODEC.toJson(query), HogQlLanguageContract.current().languageVersion());
    }

    private static String hogQlRequestWithTypedValues(String query)
    {
        return """
                {
                  "query": %s,
                  "protocolVersion": 1,
                  "languageVersion": "%s",
                  "parameters": {},
                  "variables": {"array": {"type": "array", "value": [true, "value", null]}},
                  "filters": {"object": {"type": "object", "value": {"nested": 2.5}}},
                  "modifiers": {"missing": {"type": "nullable", "value": null}},
                  "catalogGeneration": 1
                }
                """.formatted(STRING_CODEC.toJson(query), HogQlLanguageContract.current().languageVersion());
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
