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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.opentelemetry.OpenTelemetryExporterConfig;
import io.airlift.opentelemetry.OpenTelemetryExporterModule;
import io.airlift.opentelemetry.OpenTelemetryModule;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.airlift.tracing.TracingEnabledConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;

import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

public class TrinoTracingModule
        extends AbstractConfigurationAwareModule
{
    private final String serviceName;
    private final String serviceVersion;

    public TrinoTracingModule(String serviceName, String serviceVersion)
    {
        this.serviceName = requireNonNull(serviceName, "serviceName is null");
        this.serviceVersion = requireNonNull(serviceVersion, "serviceVersion is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new OpenTelemetryModule(serviceName, serviceVersion));
        // The tracer provider owns its processors and flushes pending spans on shutdown.
        closingBinder(binder).registerCloseable(SdkTracerProvider.class);
        if (buildConfigObject(TracingEnabledConfig.class).isEnabled()) {
            install(new TracingExporterModule());
        }

        jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanDeserializer.class);
    }

    private static class TracingExporterModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(OpenTelemetryExporterConfig.class);
        }

        @ProvidesIntoSet
        @Singleton
        public static SpanProcessor createSpanProcessor(OpenTelemetryExporterConfig config, SdkMeterProvider meterProvider)
        {
            // Installing OpenTelemetryExporterModule also exports metrics and logs to the tracing endpoint.
            // Reuse its span processor factory to preserve protocol, TLS, and batching configuration.
            return OpenTelemetryExporterModule.createSpanProcessor(config, meterProvider);
        }
    }
}
