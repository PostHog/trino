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
package io.trino.tracing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.airlift.opentelemetry.OpenTelemetryExporterConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.sdk.logs.LogRecordProcessor;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.server.testing.TestingTrinoServer;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.opentelemetry.OpenTelemetryExporterConfig.Protocol.GRPC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoTracingModule
{
    @Test
    public void testTracingDisabledByDefault()
            throws Exception
    {
        Injector injector = bootstrap(Map.of());
        try {
            assertThat(spanProcessors(injector)).isEmpty();
            assertNoMetricOrLogExporters(injector);
            assertThat(injector.getInstance(Tracer.class).spanBuilder("disabled").startSpan().isRecording()).isFalse();
            assertThat(injector.getInstance(OpenTelemetry.class)).isSameAs(OpenTelemetry.noop());
        }
        finally {
            stop(injector);
        }
    }

    @Test
    public void testGrpcAndLegacyConfiguration()
            throws Exception
    {
        Injector injector = bootstrap(ImmutableMap.of(
                "tracing.enabled", "true",
                "tracing.exporter.endpoint", "http://localhost:4317",
                "otel.exporter.max-export-batch-size", "16",
                "otel.exporter.span.max-queue-size", "32",
                "otel.exporter.span.schedule-delay", "10ms"));
        try {
            assertThat(spanProcessors(injector)).hasSize(1);
            assertNoMetricOrLogExporters(injector);
            OpenTelemetryExporterConfig config = injector.getInstance(OpenTelemetryExporterConfig.class);
            assertThat(config.getProtocol()).isEqualTo(GRPC);
            assertThat(config.getSpanMaxExportBatchSize()).contains(16);
            assertThat(config.getSpanMaxQueueSize()).contains(32);
        }
        finally {
            stop(injector);
        }
    }

    @Test
    public void testHttpTraceExport()
            throws Exception
    {
        var requests = new LinkedBlockingQueue<ExportRequest>();
        TestingHttpServer server = createCollector(requests);
        try {
            server.start();
            String path = "/insert/opentelemetry/v1/traces";
            Injector injector = bootstrap(ImmutableMap.of(
                    "tracing.enabled", "true",
                    "otel.exporter.protocol", "http/protobuf",
                    "otel.exporter.endpoint", server.getBaseUrl().resolve(path).toString(),
                    "otel.exporter.span.max-export-batch-size", "16",
                    "otel.exporter.span.max-queue-size", "32",
                    "otel.exporter.span.schedule-delay", "1h",
                    "otel.exporter.interval", "1s"));
            try {
                assertThat(spanProcessors(injector)).hasSize(1);
                assertThat(spanProcessors(injector).iterator().next()).isSameAs(spanProcessors(injector).iterator().next());
                assertNoMetricOrLogExporters(injector);
                OpenTelemetry telemetry = injector.getInstance(OpenTelemetry.class);
                telemetry.getMeter("test").counterBuilder("test-metric").build().add(1);
                telemetry.getLogsBridge().get("test").logRecordBuilder().setBody("test-log").emit();
                injector.getInstance(Tracer.class).spanBuilder("test-trace-export").startSpan().end();
                assertThat(injector.getInstance(SdkTracerProvider.class).forceFlush().join(10, SECONDS).isSuccess()).isTrue();
                assertThat(injector.getInstance(SdkMeterProvider.class).forceFlush().join(10, SECONDS).isSuccess()).isTrue();
                assertThat(injector.getInstance(SdkLoggerProvider.class).forceFlush().join(10, SECONDS).isSuccess()).isTrue();
                // Leave this span queued: only normal lifecycle shutdown should export it.
                injector.getInstance(Tracer.class).spanBuilder("shutdown-trace").startSpan().end();
            }
            finally {
                stop(injector);
            }
            ExportRequest request = requests.poll(10, SECONDS);
            assertThat(request).isNotNull();
            assertThat(request.path()).isEqualTo(path);
            assertThat(request.contentType()).isEqualTo("application/x-protobuf");
            ExportTraceServiceRequest export = ExportTraceServiceRequest.parseFrom(request.body());
            assertThat(export.getResourceSpansList()).singleElement().satisfies(resourceSpans -> {
                assertThat(resourceSpans.getResource().getAttributesList()).anySatisfy(attribute -> {
                    assertThat(attribute.getKey()).isEqualTo("service.name");
                    assertThat(attribute.getValue().getStringValue()).isEqualTo("trino");
                });
                assertThat(resourceSpans.getResource().getAttributesList()).anySatisfy(attribute -> {
                    assertThat(attribute.getKey()).isEqualTo("service.version");
                    assertThat(attribute.getValue().getStringValue()).isEqualTo("test-version");
                });
                assertThat(resourceSpans.getScopeSpansList()).singleElement().satisfies(scopeSpans ->
                        assertThat(scopeSpans.getSpansList()).singleElement().satisfies(span ->
                                assertThat(span.getName()).isEqualTo("test-trace-export")));
            });
            ExportRequest shutdownRequest = requests.poll(10, SECONDS);
            assertThat(shutdownRequest).isNotNull();
            assertThat(shutdownRequest.path()).isEqualTo(path);
            assertThat(ExportTraceServiceRequest.parseFrom(shutdownRequest.body()).getResourceSpansList())
                    .singleElement().satisfies(resourceSpans -> assertThat(resourceSpans.getScopeSpansList())
                            .singleElement().satisfies(scopeSpans -> assertThat(scopeSpans.getSpansList())
                                    .singleElement().satisfies(span -> assertThat(span.getName()).isEqualTo("shutdown-trace"))));
            assertThat(injector.getInstance(Tracer.class).spanBuilder("after-shutdown").startSpan().isRecording()).isFalse();
            // Flush and shutdown completed for every signal, and no other exporter was registered.
            assertThat(requests).isEmpty();
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testServerTraceExport()
            throws Exception
    {
        var requests = new LinkedBlockingQueue<ExportRequest>();
        TestingHttpServer collector = createCollector(requests);
        try {
            collector.start();
            String path = "/insert/opentelemetry/v1/traces";
            String traceId = "12345678901234567890123456789012";
            String parentSpanId = "1234567890123456";
            try (TestingTrinoServer server = TestingTrinoServer.builder()
                    .setProperties(ImmutableMap.of(
                            "tracing.enabled", "true",
                            "otel.exporter.protocol", "http/protobuf",
                            "otel.exporter.endpoint", collector.getBaseUrl().resolve(path).toString(),
                            "otel.exporter.span.schedule-delay", "1h"))
                    .build()) {
                assertThat(server.getInstance(Key.get(new TypeLiteral<Set<MetricReader>>() {}))).isEmpty();
                assertThat(server.getInstance(Key.get(new TypeLiteral<Set<LogRecordProcessor>>() {}))).isEmpty();
                OkHttpClient client = new OkHttpClient();
                try (Response response = client.newCall(new Request.Builder()
                        .url(server.getBaseUrl().resolve("/v1/info").toString())
                        .header("Authorization", Credentials.basic("test-user", ""))
                        .header("traceparent", "00-" + traceId + "-" + parentSpanId + "-01")
                        .build()).execute()) {
                    assertThat(response.code()).isEqualTo(200);
                }
                try (Response response = client.newCall(new Request.Builder()
                        .url(server.getBaseUrl().resolve("/metrics").toString())
                        .header("Authorization", Credentials.basic("test-user", ""))
                        .build()).execute()) {
                    assertThat(response.code()).isEqualTo(200);
                    assertThat(response.body().string()).contains("# TYPE");
                }
                OpenTelemetry telemetry = server.getInstance(Key.get(OpenTelemetry.class));
                telemetry.getMeter("test").counterBuilder("test-metric").build().add(1);
                telemetry.getLogsBridge().get("test").logRecordBuilder().setBody("test-log").emit();
                assertThat(server.getInstance(Key.get(SdkMeterProvider.class)).forceFlush().join(10, SECONDS).isSuccess()).isTrue();
                assertThat(server.getInstance(Key.get(SdkLoggerProvider.class)).forceFlush().join(10, SECONDS).isSuccess()).isTrue();
            }
            // Closing the server must flush the HTTP server span without manually closing the SDK.
            assertThat(requests).isNotEmpty();
            var spans = ImmutableList.<io.opentelemetry.proto.trace.v1.Span>builder();
            for (ExportRequest request : requests) {
                assertThat(request.path()).isEqualTo(path);
                assertThat(request.contentType()).isEqualTo("application/x-protobuf");
                ExportTraceServiceRequest.parseFrom(request.body()).getResourceSpansList().forEach(resourceSpans ->
                        resourceSpans.getScopeSpansList().forEach(scopeSpans -> spans.addAll(scopeSpans.getSpansList())));
            }
            assertThat(spans.build()).anySatisfy(span -> {
                assertThat(HexFormat.of().formatHex(span.getTraceId().toByteArray())).isEqualTo(traceId);
                assertThat(HexFormat.of().formatHex(span.getParentSpanId().toByteArray())).isEqualTo(parentSpanId);
            });
        }
        finally {
            collector.stop();
        }
    }

    @Test
    public void testSamplingAndPropagation()
            throws Exception
    {
        var exporter = InMemorySpanExporter.create();
        Injector injector = bootstrap(
                ImmutableMap.of(
                        "tracing.enabled", "false",
                        "otel.tracing.sampling-ratio", "0",
                        "otel.tracing.baggage.allowed-keys", "test-key"),
                binder -> newSetBinder(binder, SpanProcessor.class).addBinding().toInstance(SimpleSpanProcessor.create(exporter)));
        try {
            Tracer tracer = injector.getInstance(Tracer.class);
            Span root = tracer.spanBuilder("root").setNoParent().startSpan();
            assertThat(root.isRecording()).isFalse();
            root.end();

            SpanContext parent = SpanContext.createFromRemoteParent(
                    "12345678901234567890123456789012", "1234567890123456", TraceFlags.getSampled(), TraceState.getDefault());
            Span child = tracer.spanBuilder("child").setParent(Context.root().with(Span.wrap(parent))).startSpan();
            try {
                assertThat(child.isRecording()).isTrue();
                assertThat(child.getSpanContext().getTraceId()).isEqualTo(parent.getTraceId());
                ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
                Span deserialized = mapper.readValue(mapper.writeValueAsString(child), Span.class);
                assertThat(deserialized.getSpanContext().getTraceId()).isEqualTo(child.getSpanContext().getTraceId());
                assertThat(deserialized.getSpanContext().getSpanId()).isEqualTo(child.getSpanContext().getSpanId());
                assertThat(deserialized.getSpanContext().isSampled()).isTrue();
                assertThat(deserialized.getSpanContext().isRemote()).isTrue();

                Map<String, String> headers = new HashMap<>();
                Context context = Baggage.builder().put("test-key", "value").put("excluded", "value").build().storeInContext(Context.root().with(child));
                injector.getInstance(OpenTelemetry.class).getPropagators().getTextMapPropagator().inject(context, headers, Map::put);
                assertThat(headers).containsEntry("baggage", "test-key=value");
                assertThat(headers.get("traceparent")).contains(parent.getTraceId(), child.getSpanContext().getSpanId());
            }
            finally {
                child.end();
            }
            assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
                assertThat(span.getName()).isEqualTo("child");
                assertThat(span.getParentSpanId()).isEqualTo(parent.getSpanId());
            });
        }
        finally {
            stop(injector);
        }
    }

    @Test
    public void testIndependentMetricReader()
            throws Exception
    {
        try (var reader = InMemoryMetricReader.create()) {
            Injector injector = bootstrap(
                    ImmutableMap.of("tracing.enabled", "true"),
                    binder -> newSetBinder(binder, MetricReader.class).addBinding().toInstance(reader));
            try {
                injector.getInstance(OpenTelemetry.class).getMeter("test").counterBuilder("independent-metric").build().add(7);
                assertThat(reader.collectAllMetrics()).anySatisfy(metric -> {
                    assertThat(metric.getName()).isEqualTo("independent-metric");
                    assertThat(metric.getLongSumData().getPoints()).singleElement().satisfies(point -> assertThat(point.getValue()).isEqualTo(7));
                });
            }
            finally {
                stop(injector);
            }
        }
    }

    private static TestingHttpServer createCollector(LinkedBlockingQueue<ExportRequest> requests)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig serverConfig = new HttpServerConfig();
        HttpServerInfo serverInfo = new HttpServerInfo(serverConfig, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        return new TestingHttpServer("trace-export", serverInfo, nodeInfo, serverConfig, new HttpServlet()
        {
            @Override
            protected void doPost(HttpServletRequest request, HttpServletResponse response)
                    throws IOException
            {
                requests.add(new ExportRequest(request.getRequestURI(), request.getContentType(), request.getInputStream().readAllBytes()));
                response.setContentType("application/x-protobuf");
                response.setStatus(HttpServletResponse.SC_OK);
            }
        });
    }

    private static Injector bootstrap(Map<String, String> properties, Module... additionalModules)
    {
        return new Bootstrap(ImmutableList.<Module>builder()
                .add(new JsonModule())
                .add(new TrinoTracingModule("trino", "test-version"))
                .add(binder -> binder.bind(NodeInfo.class).toInstance(new NodeInfo("test")))
                .add(additionalModules)
                .build())
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(properties)
                .initialize();
    }

    private static Set<SpanProcessor> spanProcessors(Injector injector)
    {
        return injector.getInstance(Key.get(new TypeLiteral<Set<SpanProcessor>>() {}));
    }

    private static void assertNoMetricOrLogExporters(Injector injector)
    {
        assertThat(injector.getInstance(Key.get(new TypeLiteral<Set<MetricReader>>() {}))).isEmpty();
        assertThat(injector.getInstance(Key.get(new TypeLiteral<Set<LogRecordProcessor>>() {}))).isEmpty();
    }

    private static void stop(Injector injector)
            throws Exception
    {
        injector.getInstance(SdkMeterProvider.class).close();
        injector.getInstance(SdkLoggerProvider.class).close();
        injector.getInstance(LifeCycleManager.class).stop();
    }

    private record ExportRequest(String path, String contentType, byte[] body) {}
}
