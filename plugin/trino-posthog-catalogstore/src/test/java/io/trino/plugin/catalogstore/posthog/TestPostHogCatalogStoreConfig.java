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
package io.trino.plugin.catalogstore.posthog;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
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
import static org.assertj.core.api.Assertions.assertThat;

final class TestPostHogCatalogStoreConfig
{
    @TempDir
    private Path temporaryDirectory;

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PostHogCatalogStoreConfig.class)
                .setCellId(null)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setConnectionPasswordFile(null));
    }

    /**
     * {@code catalog-store.connection-password} and {@code catalog-store.connection-password-file}
     * are mutually exclusive, so the two variants are mapped separately instead of with a single
     * {@link io.airlift.configuration.testing.ConfigAssertions#assertFullMapping} call.
     */
    @Test
    void testExplicitPropertyMappings()
    {
        PostHogCatalogStoreConfig config = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .putAll(commonProperties())
                .put("catalog-store.connection-password", "secret")
                .buildOrThrow())
                .build(PostHogCatalogStoreConfig.class);

        assertCommonProperties(config);
        assertThat(config.getConnectionPassword()).isEqualTo(Optional.of("secret"));
        assertThat(config.getConnectionPasswordFile()).isEmpty();
    }

    @Test
    void testExplicitPropertyMappingsWithPasswordFile()
            throws IOException
    {
        File passwordFile = Files.createFile(temporaryDirectory.resolve("password")).toFile();

        PostHogCatalogStoreConfig config = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .putAll(commonProperties())
                .put("catalog-store.connection-password-file", passwordFile.getPath())
                .buildOrThrow())
                .build(PostHogCatalogStoreConfig.class);

        assertCommonProperties(config);
        assertThat(config.getConnectionPassword()).isEmpty();
        assertThat(config.getConnectionPasswordFile()).isEqualTo(Optional.of(passwordFile));
    }

    private static Map<String, String> commonProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("catalog-store.cell-id", "cell-eu-1")
                .put("catalog-store.connection-url", "jdbc:postgresql://example.net:5432/catalogs")
                .put("catalog-store.connection-user", "alice")
                .buildOrThrow();
    }

    private static void assertCommonProperties(PostHogCatalogStoreConfig config)
    {
        assertThat(config.getCellId()).isEqualTo("cell-eu-1");
        assertThat(config.getConnectionUrl()).isEqualTo("jdbc:postgresql://example.net:5432/catalogs");
        assertThat(config.getConnectionUser()).isEqualTo(Optional.of("alice"));
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
        // a catalog store database that needs no password at all remains valid
        assertValidates(validConfig());

        assertFailsValidation(
                validConfig()
                        .setConnectionPassword("secret")
                        .setConnectionPasswordFile(passwordFile),
                "connectionPasswordConfigurationValid",
                "catalog-store.connection-password and catalog-store.connection-password-file cannot both be set",
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

    @Test
    void testCellIdIsRequired()
    {
        assertFailsValidation(
                validConfig().setCellId(""),
                "cellId",
                "must not be empty",
                NotEmpty.class);
    }

    private static PostHogCatalogStoreConfig validConfig()
    {
        return new PostHogCatalogStoreConfig()
                .setCellId("cell-eu-1")
                .setConnectionUrl("jdbc:postgresql://example.net:5432/catalogs");
    }
}
