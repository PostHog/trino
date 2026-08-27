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
package io.trino.hogql.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.trino.hogql.parser.HogQlCompatibilityManifest.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestHogQlCompatibilityManifest
{
    private static final String OVERRIDES_RESOURCE = "/io/trino/hogql/parser/language/1.0.0/trino-compatibility-overrides.json";
    private static final JsonCodec<CompatibilityOverrides> OVERRIDES_CODEC = jsonCodec(CompatibilityOverrides.class);

    @Test
    public void testAccountsForEveryPublishedGrammarFeature()
    {
        HogQlCompatibilityManifest manifest = HogQlCompatibilityManifest.current();

        assertThat(manifest.schemaVersion()).isEqualTo(1);
        assertThat(manifest.languageVersion()).isEqualTo(HogQlLanguageContract.current().languageVersion());
        assertThat(manifest.features()).hasSize(583);
        assertThat(manifest.sourceUnlabeledAlternativeRules()).hasSize(21);
        assertThat(manifest.features())
                .allSatisfy(feature -> assertThat(feature.testCaseIds()).isNotEmpty());
    }

    @Test
    public void testUsesOnlyExplicitDispositionsForUnsupportedFeatures()
    {
        HogQlCompatibilityManifest manifest = HogQlCompatibilityManifest.current();

        assertThat(manifest.features())
                .filteredOn(feature -> !feature.queryReachable())
                .allSatisfy(feature -> assertThat(feature.status()).isEqualTo(Status.NOT_QUERY_LANGUAGE));
        assertThat(manifest.features())
                .filteredOn(feature -> feature.status() == Status.EXPLICIT_CURRENT_ERROR)
                .allSatisfy(feature -> assertThat(feature.loweringHandler()).isEqualTo("UnsupportedFeature"));
    }

    @Test
    public void testSupportedFeaturesMatchTheSparseOverrides()
            throws IOException
    {
        CompatibilityOverrides overrides;
        try (InputStream input = TestHogQlCompatibilityManifest.class.getResourceAsStream(OVERRIDES_RESOURCE)) {
            if (input == null) {
                fail("missing test resource: " + OVERRIDES_RESOURCE);
            }
            overrides = OVERRIDES_CODEC.fromJson(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }

        Map<String, CompatibilityOverride> overridesById = overrides.features().stream()
                .collect(toMap(CompatibilityOverride::id, feature -> feature));
        assertThat(HogQlCompatibilityManifest.current().features())
                .filteredOn(feature -> feature.status() == Status.SUPPORTED)
                .allSatisfy(feature -> assertThat(overridesById.remove(feature.id()))
                        .isEqualTo(new CompatibilityOverride(feature.id(), feature.parseHandler(), feature.loweringHandler(), feature.testCaseIds())));
        assertThat(overridesById).isEmpty();
    }

    public record CompatibilityOverrides(List<CompatibilityOverride> features)
    {
        @JsonCreator
        public CompatibilityOverrides(@JsonProperty("features") List<CompatibilityOverride> features)
        {
            this.features = List.copyOf(requireNonNull(features, "features is null"));
        }
    }

    public record CompatibilityOverride(String id, String parseHandler, String loweringHandler, List<String> testCaseIds)
    {
        @JsonCreator
        public CompatibilityOverride(
                @JsonProperty("id") String id,
                @JsonProperty("parseHandler") String parseHandler,
                @JsonProperty("loweringHandler") String loweringHandler,
                @JsonProperty("testCaseIds") List<String> testCaseIds)
        {
            this.id = requireNonNull(id, "id is null");
            this.parseHandler = requireNonNull(parseHandler, "parseHandler is null");
            this.loweringHandler = requireNonNull(loweringHandler, "loweringHandler is null");
            this.testCaseIds = List.copyOf(requireNonNull(testCaseIds, "testCaseIds is null"));
        }
    }
}
