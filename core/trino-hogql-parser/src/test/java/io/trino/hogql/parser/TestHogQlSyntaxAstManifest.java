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

import io.airlift.json.JsonCodec;
import io.trino.hogql.parser.HogQlCompatibilityManifest.PublishedGrammarFeatureManifest;
import io.trino.hogql.parser.HogQlSyntaxAstManifest.Feature;
import io.trino.hogql.parser.HogQlSyntaxAstManifest.SourceSpanGuarantee;
import io.trino.hogql.parser.HogQlSyntaxAstManifest.SyntaxNodeKind;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSyntaxAstManifest
{
    private static final String LANGUAGE_RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/";
    private static final String GRAMMAR_FEATURE_RESOURCE = LANGUAGE_RESOURCE_ROOT + "grammar-features.json";
    private static final String ALTERNATIVE_IDENTITIES_RESOURCE = LANGUAGE_RESOURCE_ROOT + "grammar-alternative-identities.json";
    private static final JsonCodec<PublishedGrammarFeatureManifest> GRAMMAR_FEATURE_MANIFEST_CODEC = jsonCodec(PublishedGrammarFeatureManifest.class);

    @Test
    public void testBindsTheCompletePrivateSyntaxAstContract()
    {
        HogQlSyntaxAstManifest manifest = HogQlSyntaxAstManifest.current();

        assertThat(manifest.schemaVersion()).isEqualTo(1);
        assertThat(manifest.languageVersion()).isEqualTo(HogQlLanguageContract.current().languageVersion());
        assertThat(manifest.grammarSha256()).isEqualTo(HogQlLanguageContract.current().grammarSha256());
        assertThat(manifest.grammarFeatureManifestSha256()).isEqualTo(sha256(readResourceBytes(GRAMMAR_FEATURE_RESOURCE)));
        assertThat(manifest.grammarAlternativeIdentityManifestSha256()).isEqualTo(sha256(readResourceBytes(ALTERNATIVE_IDENTITIES_RESOURCE)));
        assertThat(manifest.syntaxTreeKind()).isEqualTo(SyntaxNodeKind.TREE);
        assertThat(manifest.sourceSpanGuarantee()).isEqualTo(SourceSpanGuarantee.CODE_POINT_OFFSETS_END_EXCLUSIVE_ONE_BASED_LINE_COLUMNS);
        assertThat(manifest.features()).hasSize(583);
        assertThat(manifest.features()).extracting(Feature::id).doesNotHaveDuplicates();
        assertThat(manifest.features()).filteredOn(feature -> feature.syntaxNodeKind() == SyntaxNodeKind.TOKEN).hasSize(207);
        assertThat(manifest.features()).filteredOn(feature -> feature.syntaxNodeKind() == SyntaxNodeKind.RULE).hasSize(376);
        assertThat(manifest.features())
                .allSatisfy(feature -> assertThat(feature.syntaxNodeKind() == SyntaxNodeKind.TOKEN)
                        .isEqualTo(feature.id().startsWith("token:")));
    }

    @Test
    public void testRejectsIncompleteOrInvalidFeatureCoverage()
    {
        PublishedGrammarFeatureManifest published = loadPublishedGrammarFeatures();
        HogQlGrammarAlternativeIdentities alternativeIdentities = HogQlGrammarAlternativeIdentities.current();
        List<Feature> current = HogQlSyntaxAstManifest.current().features();

        assertThatThrownBy(() -> HogQlSyntaxAstManifest.validateFeatureCoverage(current.subList(1, current.size()), published, alternativeIdentities))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("HogQL syntax AST manifest does not account for every grammar feature");

        List<Feature> duplicate = new ArrayList<>(current);
        duplicate.add(current.getFirst());
        assertThatThrownBy(() -> HogQlSyntaxAstManifest.validateFeatureCoverage(duplicate, published, alternativeIdentities))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("duplicate HogQL syntax AST feature: " + current.getFirst().id());

        List<Feature> unknown = new ArrayList<>(current);
        unknown.add(new Feature("token:NOT_A_CANONICAL_HOGQL_TOKEN", SyntaxNodeKind.TOKEN));
        assertThatThrownBy(() -> HogQlSyntaxAstManifest.validateFeatureCoverage(unknown, published, alternativeIdentities))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("unknown HogQL syntax AST feature: token:NOT_A_CANONICAL_HOGQL_TOKEN");

        List<Feature> wrongNodeKind = new ArrayList<>(current);
        Feature original = wrongNodeKind.getFirst();
        SyntaxNodeKind replacementKind = original.syntaxNodeKind() == SyntaxNodeKind.TOKEN ? SyntaxNodeKind.RULE : SyntaxNodeKind.TOKEN;
        wrongNodeKind.set(0, new Feature(original.id(), replacementKind));
        assertThatThrownBy(() -> HogQlSyntaxAstManifest.validateFeatureCoverage(wrongNodeKind, published, alternativeIdentities))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("HogQL syntax AST feature has the wrong node kind: " + original.id());
    }

    private static PublishedGrammarFeatureManifest loadPublishedGrammarFeatures()
    {
        return GRAMMAR_FEATURE_MANIFEST_CODEC.fromJson(new String(readResourceBytes(GRAMMAR_FEATURE_RESOURCE), StandardCharsets.UTF_8));
    }

    private static byte[] readResourceBytes(String path)
    {
        try (InputStream input = TestHogQlSyntaxAstManifest.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return input.readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to read test resource: " + path, e);
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
}
