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
package io.trino.server.security;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoCallerTokenConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TrinoCallerTokenConfig.class)
                .setEnabled(false)
                .setIssuerUri(null)
                .setCellTokenFile(null)
                .setTimeout(new Duration(5, SECONDS))
                .setMaxResponseSize(DataSize.of(64, KILOBYTE))
                .setAllowInsecureHttpForDevelopment(false));
        assertValidates(new TrinoCallerTokenConfig());
    }

    @Test
    void testExplicitPropertyMappings(@TempDir Path directory)
            throws IOException
    {
        Path tokenFile = Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.password.caller-token.enabled", "true")
                .put("http-server.authentication.password.caller-token.issuer-uri", "http://issuer.example.test/oauth/token")
                .put("http-server.authentication.password.caller-token.cell-token-file", tokenFile.toString())
                .put("http-server.authentication.password.caller-token.timeout", "3s")
                .put("http-server.authentication.password.caller-token.max-response-size", "32kB")
                .put("http-server.authentication.password.caller-token.allow-insecure-http-for-development", "true")
                .buildOrThrow();

        assertFullMapping(properties, new TrinoCallerTokenConfig()
                .setEnabled(true)
                .setIssuerUri(URI.create("http://issuer.example.test/oauth/token"))
                .setCellTokenFile(tokenFile.toFile())
                .setTimeout(new Duration(3, SECONDS))
                .setMaxResponseSize(DataSize.of(32, KILOBYTE))
                .setAllowInsecureHttpForDevelopment(true));
    }

    @Test
    void testRequiredSettingsAreConditional(@TempDir Path directory)
    {
        TrinoCallerTokenConfig config = new TrinoCallerTokenConfig()
                .setIssuerUri(URI.create("http://issuer.example.test/oauth/token?forbidden=value"))
                .setCellTokenFile(directory.resolve("missing").toFile());
        assertValidates(config);
        config.setEnabled(true);
        assertThat(config.isIssuerUriValid()).isFalse();
        assertThat(config.isIssuerTransportValid()).isFalse();
        assertThat(config.isCellTokenFileValid()).isFalse();
    }

    @Test
    void testIssuerValidation(@TempDir Path directory)
            throws IOException
    {
        TrinoCallerTokenConfig config = new TrinoCallerTokenConfig()
                .setEnabled(true)
                .setCellTokenFile(Files.writeString(directory.resolve("cell-token"), "synthetic-cell-token").toFile());
        for (String uri : new String[] {
                "/oauth/token",
                "https:///oauth/token",
                "https://user:synthetic@issuer.example.test/oauth/token",
                "https://issuer.example.test/oauth/token?password=synthetic",
                "https://issuer.example.test/oauth/token#fragment",
        }) {
            assertThat(config.setIssuerUri(URI.create(uri)).isIssuerUriValid()).isFalse();
        }
        assertThat(config.setIssuerUri(URI.create("https://issuer.example.test/oauth/token")).isIssuerUriValid()).isTrue();
        assertThat(config.isIssuerTransportValid()).isTrue();
        assertThat(config.setIssuerUri(URI.create("http://issuer.example.test/oauth/token")).isIssuerTransportValid()).isFalse();
        assertThat(config.setAllowInsecureHttpForDevelopment(true).isIssuerTransportValid()).isTrue();
        assertThat(config.setIssuerUri(URI.create("file:///oauth/token")).isIssuerTransportValid()).isFalse();
        assertThat(config.setCellTokenFile(directory.toFile()).isCellTokenFileValid()).isFalse();
    }

    @Test
    void testNetworkBounds()
    {
        assertFailsValidation(
                new TrinoCallerTokenConfig().setTimeout(new Duration(0, MILLISECONDS)),
                "timeout",
                "must be greater than or equal to 1ms",
                MinDuration.class);
        assertFailsValidation(
                new TrinoCallerTokenConfig().setTimeout(new Duration(31, SECONDS)),
                "timeout",
                "must be less than or equal to 30s",
                MaxDuration.class);
        assertFailsValidation(
                new TrinoCallerTokenConfig().setMaxResponseSize(DataSize.of(0, BYTE)),
                "maxResponseSize",
                "must be greater than or equal to 1B",
                MinDataSize.class);
        assertFailsValidation(
                new TrinoCallerTokenConfig().setMaxResponseSize(DataSize.of(2, MEGABYTE)),
                "maxResponseSize",
                "must be less than or equal to 1MB",
                MaxDataSize.class);
    }
}
