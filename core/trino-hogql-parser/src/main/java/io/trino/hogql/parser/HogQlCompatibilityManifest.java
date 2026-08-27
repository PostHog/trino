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
import com.fasterxml.jackson.annotation.JsonValue;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public record HogQlCompatibilityManifest(
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        String grammarSha256,
        String grammarFeatureManifestSha256,
        List<UnresolvedGrammarAlternativeRule> unresolvedGrammarAlternativeRules,
        List<Feature> features)
{
    private static final String LANGUAGE_RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/";
    private static final String CURRENT_MANIFEST_RESOURCE = LANGUAGE_RESOURCE_ROOT + "trino-compatibility.json";
    private static final JsonCodec<HogQlCompatibilityManifest> MANIFEST_CODEC = jsonCodec(HogQlCompatibilityManifest.class);
    private static final JsonCodec<PublishedGrammarFeatureManifest> GRAMMAR_FEATURE_MANIFEST_CODEC = jsonCodec(PublishedGrammarFeatureManifest.class);
    private static final HogQlCompatibilityManifest CURRENT = loadCurrent();

    @JsonCreator
    public HogQlCompatibilityManifest(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
            @JsonProperty("grammarSha256") String grammarSha256,
            @JsonProperty("grammarFeatureManifestSha256") String grammarFeatureManifestSha256,
            @JsonProperty("unresolvedGrammarAlternativeRules") List<UnresolvedGrammarAlternativeRule> unresolvedGrammarAlternativeRules,
            @JsonProperty("features") List<Feature> features)
    {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported HogQL compatibility manifest schema: " + schemaVersion);
        }
        this.schemaVersion = schemaVersion;
        this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
        this.grammarFeatureManifestSha256 = requireNonNull(grammarFeatureManifestSha256, "grammarFeatureManifestSha256 is null");
        this.unresolvedGrammarAlternativeRules = List.copyOf(requireNonNull(unresolvedGrammarAlternativeRules, "unresolvedGrammarAlternativeRules is null"));
        this.features = List.copyOf(requireNonNull(features, "features is null"));
    }

    public static HogQlCompatibilityManifest current()
    {
        return CURRENT;
    }

    private static HogQlCompatibilityManifest loadCurrent()
    {
        HogQlCompatibilityManifest manifest = MANIFEST_CODEC.fromJson(readResource(CURRENT_MANIFEST_RESOURCE));
        HogQlLanguageContract languageContract = HogQlLanguageContract.current();
        if (!manifest.languageVersion().equals(languageContract.languageVersion())) {
            throw new IllegalStateException("HogQL compatibility manifest language version does not match the language contract");
        }
        if (!manifest.grammarSha256().equals(languageContract.grammarSha256())) {
            throw new IllegalStateException("HogQL compatibility manifest grammar hash does not match the language contract");
        }
        if (!manifest.grammarFeatureManifestSha256().equals(languageContract.grammarFeatureManifest().sha256())) {
            throw new IllegalStateException("HogQL compatibility manifest feature hash does not match the language contract");
        }

        String featureManifestResource = LANGUAGE_RESOURCE_ROOT + languageContract.grammarFeatureManifest().path();
        byte[] featureManifestBytes = readResourceBytes(featureManifestResource);
        if (!sha256(featureManifestBytes).equals(manifest.grammarFeatureManifestSha256())) {
            throw new IllegalStateException("published HogQL grammar feature manifest checksum does not match the compatibility manifest");
        }
        PublishedGrammarFeatureManifest published = GRAMMAR_FEATURE_MANIFEST_CODEC.fromJson(new String(featureManifestBytes, StandardCharsets.UTF_8));
        validateFeatures(manifest, published);
        return manifest;
    }

    private static void validateFeatures(HogQlCompatibilityManifest manifest, PublishedGrammarFeatureManifest published)
    {
        if (!published.languageVersion().equals(manifest.languageVersion()) || !published.grammarSha256().equals(manifest.grammarSha256())) {
            throw new IllegalStateException("published HogQL grammar features do not match the compatibility manifest language");
        }

        Map<String, PublishedFeature> publishedById = new HashMap<>();
        for (PublishedFeature feature : published.features()) {
            if (publishedById.put(feature.id(), feature) != null) {
                throw new IllegalStateException("duplicate published HogQL grammar feature: " + feature.id());
            }
        }

        Map<String, Feature> compatibilityById = new HashMap<>();
        for (Feature feature : manifest.features()) {
            if (compatibilityById.put(feature.id(), feature) != null) {
                throw new IllegalStateException("duplicate HogQL compatibility feature: " + feature.id());
            }
            PublishedFeature source = publishedById.get(feature.id());
            if (source == null) {
                throw new IllegalStateException("unknown HogQL compatibility feature: " + feature.id());
            }
            if (!feature.kind().equals(source.kind()) || feature.queryReachable() != source.queryReachable()) {
                throw new IllegalStateException("HogQL compatibility feature does not match its published definition: " + feature.id());
            }
            if (feature.queryReachable() == (feature.status() == Status.NOT_QUERY_LANGUAGE)) {
                throw new IllegalStateException("invalid HogQL query-language status for feature: " + feature.id());
            }
        }
        if (!compatibilityById.keySet().equals(publishedById.keySet())) {
            throw new IllegalStateException("HogQL compatibility manifest does not account for every published grammar feature");
        }
        if (!manifest.unresolvedGrammarAlternativeRules().equals(published.validationErrors())) {
            throw new IllegalStateException("HogQL compatibility manifest does not acknowledge every unresolved grammar alternative rule");
        }
    }

    private static String readResource(String path)
    {
        return new String(readResourceBytes(path), StandardCharsets.UTF_8);
    }

    private static byte[] readResourceBytes(String path)
    {
        try (InputStream input = HogQlCompatibilityManifest.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new IllegalStateException("HogQL compatibility resource is missing: " + path);
            }
            return input.readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to read HogQL compatibility resource", e);
        }
    }

    private static String sha256(byte[] content)
    {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    public record Feature(
            String id,
            String kind,
            boolean queryReachable,
            Status status,
            String parseHandler,
            String loweringHandler,
            List<String> testCaseIds)
    {
        @JsonCreator
        public Feature(
                @JsonProperty("id") String id,
                @JsonProperty("kind") String kind,
                @JsonProperty("queryReachable") boolean queryReachable,
                @JsonProperty("status") Status status,
                @JsonProperty("parseHandler") String parseHandler,
                @JsonProperty("loweringHandler") String loweringHandler,
                @JsonProperty("testCaseIds") List<String> testCaseIds)
        {
            this.id = requireNonNull(id, "id is null");
            this.kind = requireNonNull(kind, "kind is null");
            this.queryReachable = queryReachable;
            this.status = requireNonNull(status, "status is null");
            this.parseHandler = requireNonNull(parseHandler, "parseHandler is null");
            this.loweringHandler = requireNonNull(loweringHandler, "loweringHandler is null");
            this.testCaseIds = List.copyOf(requireNonNull(testCaseIds, "testCaseIds is null"));
            if (id.isBlank() || kind.isBlank() || parseHandler.isBlank() || loweringHandler.isBlank() || testCaseIds.isEmpty() || testCaseIds.stream().anyMatch(String::isBlank)) {
                throw new IllegalArgumentException("HogQL compatibility feature fields must be non-empty: " + id);
            }
            if (status == Status.SUPPORTED && (parseHandler.equals("UnsupportedFeature") || loweringHandler.equals("UnsupportedFeature"))) {
                throw new IllegalArgumentException("supported HogQL feature must have parse and lowering handlers: " + id);
            }
            if (status == Status.EXPLICIT_CURRENT_ERROR && !loweringHandler.equals("UnsupportedFeature")) {
                throw new IllegalArgumentException("unsupported HogQL feature must use the explicit error handler: " + id);
            }
            if (status == Status.NOT_QUERY_LANGUAGE && (!parseHandler.equals("NotQueryLanguage") || !loweringHandler.equals("NotQueryLanguage"))) {
                throw new IllegalArgumentException("non-query HogQL feature must not have query handlers: " + id);
            }
        }
    }

    public enum Status
    {
        SUPPORTED("supported"),
        EXPLICIT_CURRENT_ERROR("explicitCurrentError"),
        NOT_QUERY_LANGUAGE("notQueryLanguage");

        private final String value;

        Status(String value)
        {
            this.value = value;
        }

        @JsonCreator
        public static Status fromJson(String value)
        {
            for (Status status : values()) {
                if (status.value.equals(value)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("unknown HogQL compatibility status: " + value);
        }

        @JsonValue
        public String toJson()
        {
            return value;
        }
    }

    public record UnresolvedGrammarAlternativeRule(
            int alternativeCount,
            String code,
            boolean queryReachable,
            String rule)
    {
        @JsonCreator
        public UnresolvedGrammarAlternativeRule(
                @JsonProperty("alternativeCount") int alternativeCount,
                @JsonProperty("code") String code,
                @JsonProperty("queryReachable") boolean queryReachable,
                @JsonProperty("rule") String rule)
        {
            if (alternativeCount < 2) {
                throw new IllegalArgumentException("unresolved grammar rule must have multiple alternatives: " + rule);
            }
            this.alternativeCount = alternativeCount;
            this.code = requireNonNull(code, "code is null");
            this.queryReachable = queryReachable;
            this.rule = requireNonNull(rule, "rule is null");
        }
    }

    public record PublishedGrammarFeatureManifest(
            HogQlLanguageVersion languageVersion,
            String grammarSha256,
            List<PublishedFeature> features,
            List<UnresolvedGrammarAlternativeRule> validationErrors)
    {
        @JsonCreator
        public PublishedGrammarFeatureManifest(
                @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
                @JsonProperty("grammarSha256") String grammarSha256,
                @JsonProperty("features") List<PublishedFeature> features,
                @JsonProperty("validationErrors") List<UnresolvedGrammarAlternativeRule> validationErrors)
        {
            this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
            this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
            this.features = List.copyOf(requireNonNull(features, "features is null"));
            this.validationErrors = List.copyOf(requireNonNull(validationErrors, "validationErrors is null"));
        }
    }

    public record PublishedFeature(String id, String kind, boolean queryReachable)
    {
        @JsonCreator
        public PublishedFeature(
                @JsonProperty("id") String id,
                @JsonProperty("kind") String kind,
                @JsonProperty("queryReachable") boolean queryReachable)
        {
            this.id = requireNonNull(id, "id is null");
            this.kind = requireNonNull(kind, "kind is null");
            this.queryReachable = queryReachable;
        }
    }
}
