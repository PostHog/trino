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
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSharedConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HoglakeSharedConfig.class)
                .setUri(null)
                .setRequestTimeout(new Duration(120, SECONDS))
                .setMultiTenant(false)
                .setJwksFile(null)
                .setIssuer(null)
                .setBackendBindings(null)
                .setClockSkew(new Duration(60, SECONDS))
                .setScopedMaxLifetime(new Duration(900, SECONDS)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        assertFullMapping(ImmutableMap.<String, String>builder()
                        .put("hoglake.uri", "https://metadata.example.test")
                        .put("hoglake.client.request-timeout", "45s")
                        .put("hoglake.multi-tenant", "true")
                        .put("hoglake.shared.jwks-file", "/tmp/synthetic-public-jwks.json")
                        .put("hoglake.shared.issuer", "https://issuer.example.test")
                        .put("hoglake.shared.backend-bindings", "{\"11111111-2222-4333-8444-555555555555\":\"postgres\"}")
                        .put("hoglake.shared.clock-skew", "30s")
                        .put("hoglake.shared.scoped-max-lifetime", "600s")
                        .buildOrThrow(),
                new HoglakeSharedConfig()
                        .setUri(URI.create("https://metadata.example.test"))
                        .setRequestTimeout(new Duration(45, SECONDS))
                        .setMultiTenant(true)
                        .setJwksFile(new File("/tmp/synthetic-public-jwks.json"))
                        .setIssuer("https://issuer.example.test")
                        .setBackendBindings("{\"11111111-2222-4333-8444-555555555555\":\"postgres\"}")
                        .setClockSkew(new Duration(30, SECONDS))
                        .setScopedMaxLifetime(new Duration(600, SECONDS)));
    }

    @Test
    void testMandatorySettings()
    {
        assertThatThrownBy(() -> new ConfigurationFactory(Map.of("hoglake.multi-tenant", "true")).build(HoglakeSharedConfig.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testFixedCeilings()
    {
        assertThat(HoglakeSharedConfig.MAX_TOKEN_BYTES).isEqualTo(65_536);
        assertThat(HoglakeSharedConfig.MAX_HEADER_BYTES).isEqualTo(8192);
        assertThat(HoglakeSharedConfig.MAX_VERIFICATION_FILE_BYTES).isEqualTo(1_048_576);
        assertThat(HoglakeSharedConfig.MAX_SIGNING_KEYS).isEqualTo(32);
        assertThat(HoglakeSharedConfig.MAX_RESPONSE_BYTES).isEqualTo(16_777_216);
    }

    @Test
    void testUriRefusalIsRedacted()
    {
        assertThatThrownBy(() -> new HoglakeSharedConfig().setUri(URI.create("https://synthetic-secret@metadata.example.test")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid shared Hoglake URI")
                .hasNoCause();
    }
}
