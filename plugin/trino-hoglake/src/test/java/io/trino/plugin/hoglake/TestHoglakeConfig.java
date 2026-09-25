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
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HoglakeConfig.class)
                .setUri(null)
                .setCatalog("hoglake")
                .setRequestTimeout("2m")
                .setMaxSplitSize(DataSize.of(256, MEGABYTE))
                .setParquetFooterCacheMaxSize(DataSize.of(64, MEGABYTE)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        assertFullMapping(Map.of(
                        "hoglake.uri", "http://localhost:8080",
                        "hoglake.catalog", "lake",
                        "hoglake.client.request-timeout", "45s",
                        "hoglake.max-split-size", "1GB",
                        "hoglake.parquet-footer-cache.max-size", "16MB"),
                new HoglakeConfig()
                        .setUri("http://localhost:8080")
                        .setCatalog("lake")
                        .setRequestTimeout("45s")
                        .setMaxSplitSize(DataSize.of(1, GIGABYTE))
                        .setParquetFooterCacheMaxSize(DataSize.of(16, MEGABYTE)));
    }

    @Test
    void maxSplitSizeBelowOneMegabyteIsRejected()
    {
        assertThatThrownBy(() -> parse(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.max-split-size", "512kB")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("maxSplitSize");
        assertThat(parse(Map.of("hoglake.uri", "http://h:1", "hoglake.max-split-size", "1MB")).getMaxSplitSize())
                .isEqualTo(DataSize.of(1, MEGABYTE));
    }

    @Test
    void parquetFooterCacheCanBeDisabled()
    {
        assertThat(parse(Map.of("hoglake.uri", "http://h:1", "hoglake.parquet-footer-cache.max-size", "0B")).getParquetFooterCacheMaxSize())
                .isEqualTo(DataSize.ofBytes(0));
    }

    @Test
    void parsesFullConfig()
    {
        HoglakeConfig config = parse(Map.of(
                "hoglake.uri", "http://hoglake:8080/",
                "hoglake.catalog", "lake",
                "hoglake.client.request-timeout", "45s"));
        assertThat(config.getUri()).isEqualTo("http://hoglake:8080"); // trailing slash trimmed
        assertThat(config.getCatalog()).isEqualTo("lake");
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    void appliesDefaults()
    {
        HoglakeConfig config = parse(Map.of("hoglake.uri", "http://h:1"));
        assertThat(config.getCatalog()).isEqualTo("hoglake");
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void requiresUri()
    {
        assertThatThrownBy(() -> parse(Map.of("hoglake.catalog", "lake")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("uri");
    }

    @Test
    void allTrailingSlashesAreCollapsed()
    {
        // Regression: single-trim used to leave "http://h:8080//" with
        // one trailing slash, producing "...//v1/..." request paths.
        assertThat(parse(Map.of("hoglake.uri", "http://h:8080//")).getUri())
                .isEqualTo("http://h:8080");
        assertThat(parse(Map.of("hoglake.uri", "http://h:8080///")).getUri())
                .isEqualTo("http://h:8080");
    }

    @Test
    void emptyCatalogValueIsRejectedAtLoadTime()
    {
        // Regression: `hoglake.catalog=` (present but empty) used to be
        // kept as "", producing /v1/catalogs//namespaces requests. An
        // explicit empty value is a typo — reject at load, not per query.
        assertThatThrownBy(() -> parse(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.catalog", "")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("hoglake.catalog");
    }

    @Test
    void malformedUriIsRejectedAtLoadTime()
    {
        // Regression: a bad port or missing scheme used to be accepted
        // here and fail every query at request-build time instead.
        assertThatThrownBy(() -> parse(Map.of("hoglake.uri", "http://h:notaport")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("uri");
        assertThatThrownBy(() -> parse(Map.of("hoglake.uri", "not a uri at all")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("uri");
        assertThatThrownBy(() -> parse(Map.of("hoglake.uri", "ftp://h:1")))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("scheme");
    }

    @Test
    void requestTimeoutParsesAirliftStyleDurations()
    {
        assertThat(timeout("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(timeout("30s")).isEqualTo(Duration.ofSeconds(30));
        assertThat(timeout("2m")).isEqualTo(Duration.ofMinutes(2));
        assertThat(timeout("1h")).isEqualTo(Duration.ofHours(1));
        assertThat(timeout("1.5s")).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void malformedRequestTimeoutIsRejectedAtLoadTime()
    {
        assertThatThrownBy(() -> timeout("30"))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("hoglake.client.request-timeout");
        assertThatThrownBy(() -> timeout("fast"))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("hoglake.client.request-timeout");
        assertThatThrownBy(() -> timeout("0s"))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("positive");
    }

    private static Duration timeout(String value)
    {
        return parse(Map.of(
                "hoglake.uri", "http://h:1",
                "hoglake.client.request-timeout", value)).getRequestTimeout();
    }

    private static HoglakeConfig parse(Map<String, String> properties)
    {
        return new ConfigurationFactory(properties).build(HoglakeConfig.class);
    }
}
