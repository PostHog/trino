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
package io.trino.plugin.password.duckgres;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestDuckgresServiceCredentialConfig
{
    @TempDir
    Path temporaryDirectory;

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DuckgresServiceCredentialConfig.class)
                .setEndpoint(null)
                .setTokenFile(null)
                .setAllowInsecureHttp(false));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path tokenFile = Files.createFile(temporaryDirectory.resolve("token"));
        assertFullMapping(ImmutableMap.of(
                        "duckgres-service-credential.endpoint", "https://auth.example.com/auth/trino/service-credentials",
                        "duckgres-service-credential.token-file", tokenFile.toString(),
                        "duckgres-service-credential.allow-insecure-http", "true"),
                new DuckgresServiceCredentialConfig()
                        .setEndpoint(URI.create("https://auth.example.com/auth/trino/service-credentials"))
                        .setTokenFile(tokenFile.toFile())
                        .setAllowInsecureHttp(true));
    }
}
