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
import io.trino.client.Column;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.hogql.HogQlPhysicalCatalog;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
class TestHogQlStatementResource
{
    private static final HeaderName REQUEST_USER_HEADER = HeaderName.of(TRINO_HEADERS.requestUser());
    private static final HeaderName REQUEST_SESSION_HEADER = HeaderName.of(TRINO_HEADERS.requestSession());
    private static final HeaderName CONTENT_TYPE_HEADER = HeaderName.of("Content-Type");
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(new JsonMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule()))
            .get())
            .jsonCodec(QueryResults.class);
    private static final JsonCodec<String> STRING_CODEC = JsonCodec.jsonCodec(String.class);
    private static final JsonCodec<HogQlPhysicalCatalog> PHYSICAL_CATALOG_CODEC = JsonCodec.jsonCodec(HogQlPhysicalCatalog.class);
    private static final String SECRET = "do-not-echo-this-value";

    private static HttpClient client;
    private static TestingTrinoServer server;

    @BeforeAll
    static void setUp()
            throws Exception
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setProperties(Map.of(
                        "hogql.enabled", "true",
                        "sql.default-catalog", "tpch"))
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        SchemaTableName physicalTable = new SchemaTableName("analytics", "events");
        server.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("physical_metadata_connector")
                .withListSchemaNames(_ -> ImmutableList.of(physicalTable.getSchemaName()))
                .withStreamRelationColumns((_, schema, relationFilter) -> {
                    if (schema.isPresent() && !schema.orElseThrow().equals(physicalTable.getSchemaName())) {
                        return ImmutableList.<RelationColumnsMetadata>of().iterator();
                    }
                    if (!relationFilter.apply(Set.of(physicalTable)).contains(physicalTable)) {
                        return ImmutableList.<RelationColumnsMetadata>of().iterator();
                    }
                    return ImmutableList.of(RelationColumnsMetadata.forTable(
                                    physicalTable,
                                    ImmutableList.of(
                                            ColumnMetadata.builder()
                                                    .setName("event_id")
                                                    .setType(BIGINT)
                                                    .setNullable(false)
                                                    .build(),
                                            ColumnMetadata.builder()
                                                    .setName("tags")
                                                    .setType(new ArrayType(createVarcharType(7)))
                                                    .setHidden(true)
                                                    .build())))
                            .iterator();
                })
                .build()));
        server.createCatalog("physical_metadata", "physical_metadata_connector");
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

            StatusResponse physicalCatalogResponse = disabledClient.execute(
                    prepareGet()
                            .setUri(disabledServer.resolve("/v1/hogql/compatibility/physical-catalog?catalog=missing&protocolVersion=1"))
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .build(),
                    createStatusResponseHandler());
            assertThat(physicalCatalogResponse.getStatusCode()).isEqualTo(404);
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("differentialQueries")
    public void testHogQlExecutionMatchesTrinoSql(String name, String hogql, String trinoSql)
            throws Exception
    {
        List<QueryResults> hogqlResults = runHogQlToCompletion(hogQlRequest(hogql));
        List<QueryResults> trinoResults = runToCompletion("/v1/statement", trinoSql, false);

        assertThat(hogqlResults.getLast().getError()).describedAs("HogQL result for %s", name).isNull();
        assertThat(trinoResults.getLast().getError()).describedAs("Trino SQL result for %s", name).isNull();
        assertThat(resultColumns(hogqlResults)).isEqualTo(resultColumns(trinoResults));
        assertThat(rows(hogqlResults)).isEqualTo(rows(trinoResults));
    }

    private static Stream<Arguments> differentialQueries()
    {
        return Stream.of(
                Arguments.of(
                        "nulls, cases, and nested casts",
                        "SELECT CAST(NULL AS Nullable(Int32)) AS value, CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END AS marker",
                        "SELECT CAST(NULL AS integer) AS value, CASE WHEN NULL IS NULL THEN 'yes' ELSE 'no' END AS marker"),
                Arguments.of(
                        "joins, grouping, and ordering",
                        "SELECT r.regionkey, count(n.nationkey) AS nations " +
                                "FROM tpch.tiny.region r LEFT JOIN tpch.tiny.nation n ON r.regionkey = n.regionkey " +
                                "GROUP BY r.regionkey ORDER BY r.regionkey",
                        "SELECT r.regionkey, count(n.nationkey) AS nations " +
                                "FROM tpch.tiny.region r LEFT JOIN tpch.tiny.nation n ON r.regionkey = n.regionkey " +
                                "GROUP BY r.regionkey ORDER BY r.regionkey"),
                Arguments.of(
                        "window frames",
                        "SELECT nationkey, sum(nationkey) OVER (ORDER BY nationkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running " +
                                "FROM tpch.tiny.nation WHERE nationkey < 5 ORDER BY nationkey",
                        "SELECT nationkey, sum(nationkey) OVER (ORDER BY nationkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running " +
                                "FROM tpch.tiny.nation WHERE nationkey < 5 ORDER BY nationkey"),
                Arguments.of(
                        "values and set operations",
                        "SELECT value FROM (VALUES (2), (1), (2)) AS numbers(value) UNION SELECT 3 ORDER BY value",
                        "SELECT value FROM (VALUES (2), (1), (2)) AS numbers(value) UNION SELECT 3 ORDER BY value"),
                Arguments.of(
                        "arrays, maps, and rows",
                        "SELECT ARRAY[1, 2, 3][2] AS array_value, MAP(ARRAY['k'], ARRAY[7])['k'] AS map_value, (11, 'x').1 AS row_value",
                        "SELECT ARRAY[1, 2, 3][2] AS array_value, MAP(ARRAY['k'], ARRAY[7])['k'] AS map_value, ROW(11, 'x')[1] AS row_value"),
                Arguments.of(
                        "timestamp precision",
                        "SELECT CAST('2024-03-10 01:59:59.123456' AS Timestamp(6)) AS value",
                        "SELECT CAST('2024-03-10 01:59:59.123456' AS timestamp(6)) AS value"));
    }

    @Test
    public void testPhysicalCatalogCompatibilityEndpoint()
    {
        HogQlPhysicalCatalog catalog = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=physical_metadata&protocolVersion=1"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createJsonResponseHandler(PHYSICAL_CATALOG_CODEC));

        assertThat(catalog.protocolVersion()).isEqualTo(1);
        assertThat(catalog.schemaVersion()).isEqualTo(1);
        assertThat(catalog.catalog()).isEqualTo(new HogQlPhysicalCatalog.Identifier("physical_metadata", false));
        assertThat(catalog.catalogHandleVersion()).isNotBlank();
        assertThat(catalog.tables()).singleElement().satisfies(table -> {
            assertThat(table.schema()).isEqualTo(new HogQlPhysicalCatalog.Identifier("analytics", false));
            assertThat(table.table()).isEqualTo(new HogQlPhysicalCatalog.Identifier("events", false));
            assertThat(table.columns()).containsExactly(
                    new HogQlPhysicalCatalog.Column(
                            new HogQlPhysicalCatalog.Identifier("event_id", false),
                            1,
                            "bigint",
                            false,
                            false,
                            true),
                    new HogQlPhysicalCatalog.Column(
                            new HogQlPhysicalCatalog.Identifier("tags", false),
                            2,
                            "array(varchar(7))",
                            true,
                            true,
                            false));
        });
    }

    @Test
    public void testPhysicalCatalogCompatibilityEndpointFailsClosed()
    {
        StatusResponse missingCatalog = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createStatusResponseHandler());
        assertThat(missingCatalog.getStatusCode()).isEqualTo(400);

        StatusResponse missingProtocolVersion = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=physical_metadata"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createStatusResponseHandler());
        assertThat(missingProtocolVersion.getStatusCode()).isEqualTo(400);

        StatusResponse unsupportedProtocolVersion = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=physical_metadata&protocolVersion=2"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createStatusResponseHandler());
        assertThat(unsupportedProtocolVersion.getStatusCode()).isEqualTo(400);

        StatusResponse unknownCatalog = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=missing&protocolVersion=1"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createStatusResponseHandler());
        assertThat(unknownCatalog.getStatusCode()).isEqualTo(404);
    }

    @Test
    public void testPhysicalCatalogCompatibilityEndpointRequiresAuthentication()
    {
        StatusResponse response = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=physical_metadata&protocolVersion=1"))
                        .build(),
                createStatusResponseHandler());

        assertThat(response.getStatusCode()).isEqualTo(401);
    }

    @Test
    public void testPhysicalCatalogCompatibilityEndpointAppliesVisibilityFilters()
    {
        server.getAccessControl().deny(privilege("events.event_id", SELECT_COLUMN));
        try {
            HogQlPhysicalCatalog catalog = client.execute(
                    prepareGet()
                            .setUri(server.resolve("/v1/hogql/compatibility/physical-catalog?catalog=physical_metadata&protocolVersion=1"))
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .build(),
                    createJsonResponseHandler(PHYSICAL_CATALOG_CODEC));

            assertThat(catalog.tables()).singleElement().satisfies(table -> {
                assertThat(table.columns()).extracting(column -> column.name().value()).containsExactly("tags");
                assertThat(table.columns()).extracting(HogQlPhysicalCatalog.Column::ordinal).containsExactly(2);
            });
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testEmptyResultsAndCompilerDiagnostics()
            throws Exception
    {
        List<QueryResults> emptyResults = runHogQlToCompletion(hogQlRequest("SELECT 1 WHERE false"));
        assertThat(rows(emptyResults)).isEmpty();
        assertThat(emptyResults.getLast().getWarnings()).isEmpty();

        List<QueryResults> invalidResults = runHogQlToCompletion(hogQlRequest("SELECT ("));
        assertThat(invalidResults.getLast().getError().getErrorName()).isEqualTo("HOGQL_SYNTAX_ERROR");
    }

    @Test
    public void testQueuedQueryCanBeCancelled()
    {
        QueryResults queued = postQuery(hogQlRequest("SELECT nationkey FROM tpch.tiny.nation"));

        StatusResponse response = client.execute(
                prepareDelete()
                        .setUri(queued.getNextUri())
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .build(),
                createStatusResponseHandler());

        assertThat(response.getStatusCode()).isEqualTo(204);
    }

    @Test
    public void testQueryUsesNativeExecutionTimeout()
            throws Exception
    {
        List<QueryResults> results = runHogQlToCompletion(
                hogQlRequest("SELECT count(*) FROM tpch.tiny.lineitem a CROSS JOIN tpch.tiny.lineitem b CROSS JOIN tpch.tiny.lineitem c"),
                "query_max_execution_time=1ms");

        assertThat(results.getLast().getError().getErrorName())
                .describedAs("query error: %s", results.getLast().getError())
                .isEqualTo("EXCEEDED_TIME_LIMIT");
    }

    @Test
    public void testExplainUsesNativePlanner()
            throws Exception
    {
        List<QueryResults> results = runHogQlToCompletion(hogQlExplainRequest(
                "SELECT nationkey FROM tpch.tiny.nation",
                "DISTRIBUTED",
                "TEXT"));

        assertThat(results.getLast().getError()).isNull();
        assertThat(rows(results)).singleElement().satisfies(row ->
                assertThat(row).singleElement().asString().contains("TableScan"));
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
                Arguments.of("unsupported protocol version",
                        """
                        {"query":"SELECT '%s'","protocolVersion":2,"languageVersion":"%s"}
                        """.formatted(SECRET, languageVersion)),
                Arguments.of("unsupported language version",
                        """
                        {"query":"SELECT '%s'","protocolVersion":1,"languageVersion":"999.0.0"}
                        """.formatted(SECRET)),
                Arguments.of("unknown top-level field",
                        """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","unknown":"%s"}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("unknown typed-value field",
                        """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{"p":{"type":"string","value":"%s","unknown":true}}}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("ambiguous typed value",
                        """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{"p":{"type":"string","unknown":"%s"}}}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("nonpositive catalog generation",
                        """
                        {"query":"SELECT '%s'","protocolVersion":1,"languageVersion":"%s","catalogGeneration":0}
                        """.formatted(SECRET, languageVersion)),
                Arguments.of("unknown explain field",
                        """
                        {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","explain":{"type":"LOGICAL","format":"TEXT","unknown":"%s"}}
                        """.formatted(languageVersion, SECRET)),
                Arguments.of("invalid explain type",
                        """
                        {"query":"SELECT '%s'","protocolVersion":1,"languageVersion":"%s","explain":{"type":"UNKNOWN","format":"TEXT"}}
                        """.formatted(SECRET, languageVersion)),
                Arguments.of("too many parameter bindings", requestWithParameterCount(languageVersion, 1_001)),
                Arguments.of("too many total bindings", requestWithTotalBindingCount(languageVersion)),
                Arguments.of("request exceeds byte limit", oversizedRequest(languageVersion)),
                Arguments.of("malformed JSON",
                        """
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

    private static String requestWithTotalBindingCount(String languageVersion)
    {
        String parameters = range(0, 1_000)
                .mapToObj(index -> "\"p%s\":{\"type\":\"integer\",\"value\":%s}".formatted(index, index))
                .collect(joining(","));
        return """
               {"query":"SELECT 1","protocolVersion":1,"languageVersion":"%s","parameters":{%s},"variables":{"extra":{"type":"integer","value":1}}}
               """.formatted(languageVersion, parameters);
    }

    private static String oversizedRequest(String languageVersion)
    {
        return """
               {"query":"SELECT '%s%s'","protocolVersion":1,"languageVersion":"%s"}
               """.formatted(SECRET, "x".repeat(2 * 1024 * 1024), languageVersion);
    }

    private static List<QueryResults> runHogQlToCompletion(String request)
    {
        return runToCompletion("/v1/hogql", request, true, Optional.empty());
    }

    private static List<QueryResults> runHogQlToCompletion(String request, String session)
    {
        return runToCompletion("/v1/hogql", request, true, Optional.of(session));
    }

    private static List<QueryResults> runToCompletion(String path, String body, boolean json)
    {
        return runToCompletion(path, body, json, Optional.empty());
    }

    private static List<QueryResults> runToCompletion(String path, String body, boolean json, Optional<String> session)
    {
        ImmutableList.Builder<QueryResults> results = ImmutableList.builder();
        Request.Builder request = preparePost()
                .setUri(server.resolve(path))
                .setHeader(REQUEST_USER_HEADER, "user")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8));
        if (json) {
            request.setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON);
        }
        session.ifPresent(value -> request.setHeader(REQUEST_SESSION_HEADER, value));
        QueryResults current = client.execute(request.build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
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

    private static QueryResults postQuery(String request)
    {
        return client.execute(
                preparePost()
                        .setUri(server.resolve("/v1/hogql"))
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                        .setBodyGenerator(createStaticBodyGenerator(request, UTF_8))
                        .build(),
                createJsonResponseHandler(QUERY_RESULTS_CODEC));
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
                 "modifiers": {},
                 "catalogGeneration": 1
               }
               """.formatted(STRING_CODEC.toJson(query), HogQlLanguageContract.current().languageVersion());
    }

    private static String hogQlExplainRequest(String query, String type, String format)
    {
        return """
               {"query":%s,"protocolVersion":1,"languageVersion":"%s","explain":{"type":%s,"format":%s}}
               """.formatted(
                STRING_CODEC.toJson(query),
                HogQlLanguageContract.current().languageVersion(),
                STRING_CODEC.toJson(type),
                STRING_CODEC.toJson(format));
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

    private static List<Column> resultColumns(List<QueryResults> results)
    {
        return results.stream()
                .map(QueryResults::getColumns)
                .filter(columns -> columns != null)
                .findFirst()
                .orElseThrow();
    }
}
