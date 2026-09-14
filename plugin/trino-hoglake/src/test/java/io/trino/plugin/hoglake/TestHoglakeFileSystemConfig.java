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

import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeFileSystemConfig
{
    @Test
    void testLegacyProperties()
    {
        Map<String, String> normalized = HoglakeFileSystemConfig.normalize(Map.of(
                "hoglake.uri", "http://localhost:8080",
                "hoglake.s3.endpoint", "http://localhost:9000",
                "hoglake.s3.region", "eu-west-1",
                "hoglake.s3.access-key", "test-access",
                "hoglake.s3.secret-key", "test-secret",
                "hoglake.s3.path-style", "true",
                "fs.cache.enabled", "true"));
        assertThat(normalized).containsExactlyInAnyOrderEntriesOf(Map.of(
                "hoglake.uri", "http://localhost:8080",
                "s3.endpoint", "http://localhost:9000",
                "s3.region", "eu-west-1",
                "s3.aws-access-key", "test-access",
                "s3.aws-secret-key", "test-secret",
                "s3.path-style-access", "true",
                "fs.cache.enabled", "true"));
        S3FileSystemConfig config = new ConfigurationFactory(normalized).build(S3FileSystemConfig.class);
        assertThat(config.getEndpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getAwsAccessKey()).isEqualTo("test-access");
        assertThat(config.getAwsSecretKey()).isEqualTo("test-secret");
        assertThat(config.isPathStyleAccess()).isTrue();
    }

    @Test
    void testBlankLegacyCredentialsAndEndpointUseDefaults()
    {
        Map<String, String> normalized = HoglakeFileSystemConfig.normalize(Map.of(
                "hoglake.s3.endpoint", " ",
                "hoglake.s3.access-key", "",
                "hoglake.s3.secret-key", " "));
        assertThat(normalized).isEmpty();
        S3FileSystemConfig config = new ConfigurationFactory(normalized).build(S3FileSystemConfig.class);
        assertThat(config.getEndpoint()).isNull();
        assertThat(config.getAwsAccessKey()).isNull();
        assertThat(config.getAwsSecretKey()).isNull();
    }

    @Test
    void testEquivalentAliases()
    {
        assertThat(HoglakeFileSystemConfig.normalize(Map.of(
                "hoglake.s3.region", "eu-west-1",
                "s3.region", "eu-west-1",
                "hoglake.s3.path-style", "TRUE",
                "s3.path-style-access", "true")))
                .containsExactlyInAnyOrderEntriesOf(Map.of("s3.region", "eu-west-1", "s3.path-style-access", "true"));
    }

    @Test
    void testEquivalentBlankAliasesUseDefaults()
    {
        assertThat(HoglakeFileSystemConfig.normalize(Map.of(
                "hoglake.s3.endpoint", "",
                "s3.endpoint", " ",
                "hoglake.s3.access-key", " ",
                "s3.aws-access-key", "",
                "hoglake.s3.secret-key", "",
                "s3.aws-secret-key", " ")))
                .isEmpty();
    }

    @Test
    void testConflictsDoNotExposeSecrets()
    {
        assertThatThrownBy(() -> HoglakeFileSystemConfig.normalize(Map.of(
                "hoglake.s3.secret-key", "legacy-secret", "s3.aws-secret-key", "standard-secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hoglake.s3.secret-key")
                .hasMessageContaining("s3.aws-secret-key")
                .hasMessageNotContaining("legacy-secret")
                .hasMessageNotContaining("standard-secret");
    }
}
