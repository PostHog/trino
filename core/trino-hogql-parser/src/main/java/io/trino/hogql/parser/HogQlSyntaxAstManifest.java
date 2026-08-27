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

public record HogQlSyntaxAstManifest(
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        String grammarSha256,
        String grammarFeatureManifestSha256,
        String grammarAlternativeIdentityManifestSha256,
        SyntaxNodeKind syntaxTreeKind,
        SourceSpanGuarantee sourceSpanGuarantee,
        List<Feature> features)
{
    private static final String LANGUAGE_RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/";
    private static final String CURRENT_RESOURCE = LANGUAGE_RESOURCE_ROOT + "trino-syntax-ast.json";
    private static final JsonCodec<HogQlSyntaxAstManifest> MANIFEST_CODEC = jsonCodec(HogQlSyntaxAstManifest.class);
    private static final JsonCodec<HogQlCompatibilityManifest.PublishedGrammarFeatureManifest> GRAMMAR_FEATURE_MANIFEST_CODEC = jsonCodec(HogQlCompatibilityManifest.PublishedGrammarFeatureManifest.class);
    private static final HogQlSyntaxAstManifest CURRENT = loadCurrent();

    @JsonCreator
    public HogQlSyntaxAstManifest(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
            @JsonProperty("grammarSha256") String grammarSha256,
            @JsonProperty("grammarFeatureManifestSha256") String grammarFeatureManifestSha256,
            @JsonProperty("grammarAlternativeIdentityManifestSha256") String grammarAlternativeIdentityManifestSha256,
            @JsonProperty("syntaxTreeKind") SyntaxNodeKind syntaxTreeKind,
            @JsonProperty("sourceSpanGuarantee") SourceSpanGuarantee sourceSpanGuarantee,
            @JsonProperty("features") List<Feature> features)
    {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported HogQL syntax AST manifest schema: " + schemaVersion);
        }
        this.schemaVersion = schemaVersion;
        this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
        this.grammarFeatureManifestSha256 = requireNonNull(grammarFeatureManifestSha256, "grammarFeatureManifestSha256 is null");
        this.grammarAlternativeIdentityManifestSha256 = requireNonNull(grammarAlternativeIdentityManifestSha256, "grammarAlternativeIdentityManifestSha256 is null");
        this.syntaxTreeKind = requireNonNull(syntaxTreeKind, "syntaxTreeKind is null");
        this.sourceSpanGuarantee = requireNonNull(sourceSpanGuarantee, "sourceSpanGuarantee is null");
        this.features = List.copyOf(requireNonNull(features, "features is null"));
        if (syntaxTreeKind != SyntaxNodeKind.TREE) {
            throw new IllegalArgumentException("HogQL syntax AST root must use the tree node kind");
        }
    }

    public static HogQlSyntaxAstManifest current()
    {
        return CURRENT;
    }

    private static HogQlSyntaxAstManifest loadCurrent()
    {
        HogQlSyntaxAstManifest manifest = MANIFEST_CODEC.fromJson(readResource(CURRENT_RESOURCE));
        HogQlLanguageContract languageContract = HogQlLanguageContract.current();
        if (!manifest.languageVersion().equals(languageContract.languageVersion())) {
            throw new IllegalStateException("HogQL syntax AST manifest language version does not match the language contract");
        }
        if (!manifest.grammarSha256().equals(languageContract.grammarSha256())) {
            throw new IllegalStateException("HogQL syntax AST manifest grammar hash does not match the language contract");
        }
        if (!manifest.grammarFeatureManifestSha256().equals(languageContract.grammarFeatureManifest().sha256())) {
            throw new IllegalStateException("HogQL syntax AST manifest feature hash does not match the language contract");
        }
        if (!manifest.grammarAlternativeIdentityManifestSha256().equals(HogQlGrammarAlternativeIdentities.currentSha256())) {
            throw new IllegalStateException("HogQL syntax AST manifest alternative identity hash does not match the published sidecar");
        }

        String featureManifestResource = LANGUAGE_RESOURCE_ROOT + languageContract.grammarFeatureManifest().path();
        byte[] featureManifestBytes = readResourceBytes(featureManifestResource);
        if (!sha256(featureManifestBytes).equals(manifest.grammarFeatureManifestSha256())) {
            throw new IllegalStateException("published HogQL grammar feature manifest checksum does not match the syntax AST manifest");
        }
        HogQlCompatibilityManifest.PublishedGrammarFeatureManifest published = GRAMMAR_FEATURE_MANIFEST_CODEC.fromJson(new String(featureManifestBytes, StandardCharsets.UTF_8));
        validateFeatureCoverage(manifest.features(), published, HogQlGrammarAlternativeIdentities.current());
        return manifest;
    }

    static void validateFeatureCoverage(
            List<Feature> features,
            HogQlCompatibilityManifest.PublishedGrammarFeatureManifest published,
            HogQlGrammarAlternativeIdentities alternativeIdentities)
    {
        HogQlLanguageContract languageContract = HogQlLanguageContract.current();
        if (!published.languageVersion().equals(languageContract.languageVersion()) || !published.grammarSha256().equals(languageContract.grammarSha256())) {
            throw new IllegalStateException("published HogQL grammar features do not match the language contract");
        }

        Map<String, SyntaxNodeKind> expectedById = new HashMap<>();
        for (HogQlCompatibilityManifest.PublishedFeature feature : published.features()) {
            SyntaxNodeKind nodeKind = switch (feature.kind()) {
                case "token" -> SyntaxNodeKind.TOKEN;
                case "parserRule", "parserAlternative" -> SyntaxNodeKind.RULE;
                default -> throw new IllegalStateException("unknown published HogQL grammar feature kind: " + feature.kind());
            };
            if (expectedById.put(feature.id(), nodeKind) != null) {
                throw new IllegalStateException("duplicate published HogQL grammar feature: " + feature.id());
            }
        }

        Map<String, HogQlCompatibilityManifest.SourceUnlabeledAlternativeRule> sourceUnlabeledByRule = new HashMap<>();
        for (HogQlCompatibilityManifest.SourceUnlabeledAlternativeRule rule : published.validationErrors()) {
            if (sourceUnlabeledByRule.put(rule.rule(), rule) != null) {
                throw new IllegalStateException("duplicate source-unlabeled HogQL grammar alternative rule: " + rule.rule());
            }
        }
        for (HogQlGrammarAlternativeIdentities.AlternativeRule rule : alternativeIdentities.rules()) {
            HogQlCompatibilityManifest.SourceUnlabeledAlternativeRule sourceRule = sourceUnlabeledByRule.remove(rule.rule());
            if (sourceRule == null || sourceRule.alternativeCount() != rule.alternatives().size() || sourceRule.queryReachable() != rule.queryReachable()) {
                throw new IllegalStateException("HogQL grammar alternative identities do not match the published rule: " + rule.rule());
            }
            for (HogQlGrammarAlternativeIdentities.Alternative alternative : rule.alternatives()) {
                if (expectedById.put(alternative.id(), SyntaxNodeKind.RULE) != null) {
                    throw new IllegalStateException("duplicate published HogQL grammar feature: " + alternative.id());
                }
            }
        }
        if (!sourceUnlabeledByRule.isEmpty()) {
            throw new IllegalStateException("HogQL grammar alternative identities do not cover every source-unlabeled rule");
        }

        Map<String, Feature> manifestById = new HashMap<>();
        for (Feature feature : features) {
            if (manifestById.put(feature.id(), feature) != null) {
                throw new IllegalStateException("duplicate HogQL syntax AST feature: " + feature.id());
            }
            SyntaxNodeKind expected = expectedById.get(feature.id());
            if (expected == null) {
                throw new IllegalStateException("unknown HogQL syntax AST feature: " + feature.id());
            }
            if (feature.syntaxNodeKind() != expected) {
                throw new IllegalStateException("HogQL syntax AST feature has the wrong node kind: " + feature.id());
            }
        }
        if (!manifestById.keySet().equals(expectedById.keySet())) {
            throw new IllegalStateException("HogQL syntax AST manifest does not account for every grammar feature");
        }
    }

    private static String readResource(String path)
    {
        return new String(readResourceBytes(path), StandardCharsets.UTF_8);
    }

    private static byte[] readResourceBytes(String path)
    {
        try (InputStream input = HogQlSyntaxAstManifest.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new IllegalStateException("HogQL syntax AST resource is missing: " + path);
            }
            return input.readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to read HogQL syntax AST resource", e);
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

    public record Feature(String id, SyntaxNodeKind syntaxNodeKind)
    {
        @JsonCreator
        public Feature(
                @JsonProperty("id") String id,
                @JsonProperty("syntaxNodeKind") SyntaxNodeKind syntaxNodeKind)
        {
            this.id = requireNonNull(id, "id is null");
            this.syntaxNodeKind = requireNonNull(syntaxNodeKind, "syntaxNodeKind is null");
            if (id.isBlank() || syntaxNodeKind == SyntaxNodeKind.TREE) {
                throw new IllegalArgumentException("invalid HogQL syntax AST feature: " + id);
            }
        }
    }

    public enum SyntaxNodeKind
    {
        TREE("tree"),
        RULE("rule"),
        TOKEN("token");

        private final String value;

        SyntaxNodeKind(String value)
        {
            this.value = value;
        }

        @JsonCreator
        public static SyntaxNodeKind fromJson(String value)
        {
            for (SyntaxNodeKind kind : values()) {
                if (kind.value.equals(value)) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown HogQL syntax node kind: " + value);
        }

        @JsonValue
        public String toJson()
        {
            return value;
        }
    }

    public enum SourceSpanGuarantee
    {
        CODE_POINT_OFFSETS_END_EXCLUSIVE_ONE_BASED_LINE_COLUMNS("codePointOffsetsEndExclusiveOneBasedLineColumns");

        private final String value;

        SourceSpanGuarantee(String value)
        {
            this.value = value;
        }

        @JsonCreator
        public static SourceSpanGuarantee fromJson(String value)
        {
            for (SourceSpanGuarantee guarantee : values()) {
                if (guarantee.value.equals(value)) {
                    return guarantee;
                }
            }
            throw new IllegalArgumentException("unknown HogQL source span guarantee: " + value);
        }

        @JsonValue
        public String toJson()
        {
            return value;
        }
    }
}
