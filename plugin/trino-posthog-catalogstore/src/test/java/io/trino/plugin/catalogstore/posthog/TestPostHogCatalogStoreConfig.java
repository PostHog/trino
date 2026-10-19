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
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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
                .setConnectionPasswordFile(null)
                .setReadOnly(false)
                .setSnapshotTimeout(new Duration(5, SECONDS))
                .setDelegateEndpoint(null)
                .setDelegateTokenFile(null)
                .setDelegateAllowInsecureHttp(false)
                .setDelegateRequestTimeout(new Duration(15, SECONDS))
                .setDelegateVisibilityTimeout(new Duration(3, MINUTES)));
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

    @Test
    void testDelegatePropertyMappings()
            throws IOException
    {
        File tokenFile = Files.createFile(temporaryDirectory.resolve("token")).toFile();

        PostHogCatalogStoreConfig config = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .putAll(commonProperties())
                .put("catalog-store.delegate.endpoint", "https://control-plane.example.net/catalog-requests/v1")
                .put("catalog-store.delegate.token-file", tokenFile.getPath())
                .put("catalog-store.delegate.allow-insecure-http", "true")
                .put("catalog-store.delegate.request-timeout", "7s")
                .put("catalog-store.delegate.visibility-timeout", "45s")
                .buildOrThrow())
                .build(PostHogCatalogStoreConfig.class);

        assertCommonProperties(config);
        assertThat(config.getDelegateEndpoint()).isEqualTo(Optional.of(URI.create("https://control-plane.example.net/catalog-requests/v1")));
        assertThat(config.getDelegateTokenFile()).isEqualTo(Optional.of(tokenFile));
        assertThat(config.isDelegateAllowInsecureHttp()).isTrue();
        assertThat(config.getDelegateRequestTimeout()).isEqualTo(new Duration(7, SECONDS));
        assertThat(config.getDelegateVisibilityTimeout()).isEqualTo(new Duration(45, SECONDS));
    }

    @Test
    void testDelegationRequiresReadOnlyStoreAndToken()
            throws IOException
    {
        File tokenFile = Files.createFile(temporaryDirectory.resolve("delegate-token")).toFile();
        URI endpoint = URI.create("https://control-plane.example.net/catalog-requests/v1");

        assertValidates(validConfig()
                .setReadOnly(true)
                .setDelegateEndpoint(endpoint)
                .setDelegateTokenFile(tokenFile));
        assertFailsValidation(
                validConfig()
                        .setDelegateEndpoint(endpoint)
                        .setDelegateTokenFile(tokenFile),
                "delegateOnlyForReadOnlyStore",
                "catalog-store.delegate.endpoint requires catalog-store.read-only=true",
                AssertTrue.class);
        assertFailsValidation(
                validConfig()
                        .setReadOnly(true)
                        .setDelegateEndpoint(endpoint),
                "delegateTokenFileConfigured",
                "catalog-store.delegate.endpoint requires catalog-store.delegate.token-file",
                AssertTrue.class);
    }

    private static Map<String, String> commonProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("catalog-store.cell-id", "cell-eu-1")
                .put("catalog-store.connection-url", "jdbc:postgresql://example.net:5432/catalogs")
                .put("catalog-store.connection-user", "alice")
                .put("catalog-store.read-only", "true")
                .put("catalog-store.snapshot-timeout", "9s")
                .buildOrThrow();
    }

    private static void assertCommonProperties(PostHogCatalogStoreConfig config)
    {
        assertThat(config.getCellId()).isEqualTo("cell-eu-1");
        assertThat(config.getConnectionUrl()).isEqualTo("jdbc:postgresql://example.net:5432/catalogs");
        assertThat(config.getConnectionUser()).isEqualTo(Optional.of("alice"));
        assertThat(config.isReadOnly()).isTrue();
        assertThat(config.getSnapshotTimeout()).isEqualTo(new Duration(9, SECONDS));
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
