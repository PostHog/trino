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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckLakeConfig
{
    @TempDir
    private Path temporaryDirectory;

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DuckLakeConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setConnectionPasswordFile(null)
                .setConnectionPoolMaxSize(10)
                .setConnectionPoolIdleTimeout(new Duration(1, MINUTES))
                .setConnectionPoolAcquisitionTimeout(new Duration(30, SECONDS))
                .setMetadataSchema("public")
                .setDataPath(null)
                .setFileStatisticsPruningEnabled(true)
                .setMaxSplitSize(DataSize.of(64, MEGABYTE)));
    }

    /**
     * {@code ducklake.metadata.connection-password} and
     * {@code ducklake.metadata.connection-password-file} are mutually exclusive, so the two
     * variants are mapped separately instead of with a single
     * {@link io.airlift.configuration.testing.ConfigAssertions#assertFullMapping} call.
     */
    @Test
    void testExplicitPropertyMappings()
    {
        DuckLakeConfig config = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .putAll(commonProperties())
                .put("ducklake.metadata.connection-password", "secret")
                .buildOrThrow())
                .build(DuckLakeConfig.class);

        assertCommonProperties(config);
        assertThat(config.getConnectionPassword()).isEqualTo(Optional.of("secret"));
        assertThat(config.getConnectionPasswordFile()).isEmpty();
    }

    @Test
    void testExplicitPropertyMappingsWithPasswordFile()
            throws IOException
    {
        File passwordFile = Files.createFile(temporaryDirectory.resolve("password")).toFile();

        DuckLakeConfig config = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .putAll(commonProperties())
                .put("ducklake.metadata.connection-password-file", passwordFile.getPath())
                .buildOrThrow())
                .build(DuckLakeConfig.class);

        assertCommonProperties(config);
        assertThat(config.getConnectionPassword()).isEmpty();
        assertThat(config.getConnectionPasswordFile()).isEqualTo(Optional.of(passwordFile));
    }

    private static Map<String, String> commonProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("ducklake.metadata.connection-url", "jdbc:postgresql://example.net:5432/lake")
                .put("ducklake.metadata.connection-user", "alice")
                .put("ducklake.metadata.connection-pool.max-size", "3")
                .put("ducklake.metadata.connection-pool.idle-timeout", "17s")
                .put("ducklake.metadata.connection-pool.acquisition-timeout", "5s")
                .put("ducklake.metadata.schema", "ducklake")
                .put("ducklake.data-path", "s3://bucket/prefix/")
                .put("ducklake.file-statistics-pruning.enabled", "false")
                .put("ducklake.max-split-size", "32MB")
                .buildOrThrow();
    }

    private static void assertCommonProperties(DuckLakeConfig config)
    {
        assertThat(config.getConnectionUrl()).isEqualTo("jdbc:postgresql://example.net:5432/lake");
        assertThat(config.getConnectionUser()).isEqualTo(Optional.of("alice"));
        assertThat(config.getConnectionPoolMaxSize()).isEqualTo(3);
        assertThat(config.getConnectionPoolIdleTimeout()).isEqualTo(new Duration(17, SECONDS));
        assertThat(config.getConnectionPoolAcquisitionTimeout()).isEqualTo(new Duration(5, SECONDS));
        assertThat(config.getMetadataSchema()).isEqualTo("ducklake");
        assertThat(config.getDataPath()).isEqualTo("s3://bucket/prefix/");
        assertThat(config.isFileStatisticsPruningEnabled()).isFalse();
        assertThat(config.getMaxSplitSize()).isEqualTo(DataSize.of(32, MEGABYTE));
    }

    @Test
    void testPasswordAndPasswordFileAreMutuallyExclusive()
            throws IOException
    {
        File passwordFile = Files.createFile(temporaryDirectory.resolve("mutually-exclusive")).toFile();

        assertValidates(validConfig()
                .setConnectionPassword("secret"));
        assertValidates(validConfig()
                .setConnectionPasswordFile(passwordFile));
        // a catalog database that needs no password at all remains valid
        assertValidates(validConfig());

        assertFailsValidation(
                validConfig()
                        .setConnectionPassword("secret")
                        .setConnectionPasswordFile(passwordFile),
                "connectionPasswordConfigurationValid",
                "ducklake.metadata.connection-password and ducklake.metadata.connection-password-file cannot both be set",
                AssertTrue.class);
    }

    @Test
    void testPasswordFileMustExist()
    {
        File missing = temporaryDirectory.resolve("does-not-exist").toFile();

        assertFailsValidation(
                validConfig().setConnectionPasswordFile(missing),
                "connectionPasswordFile",
                "file does not exist: " + missing.getPath(),
                FileExists.class);
    }

    private DuckLakeConfig validConfig()
    {
        return new DuckLakeConfig()
                .setConnectionUrl("jdbc:postgresql://example.net:5432/lake")
                .setDataPath("s3://bucket/prefix/");
    }
}
