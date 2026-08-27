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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public record HogQlGrammarAlternativeIdentities(
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        String grammarSha256,
        List<AlternativeRule> rules)
{
    private static final String CURRENT_RESOURCE = "/io/trino/hogql/parser/language/1.0.0/grammar-alternative-identities.json";
    private static final JsonCodec<HogQlGrammarAlternativeIdentities> CODEC = jsonCodec(HogQlGrammarAlternativeIdentities.class);
    private static final LoadedIdentities CURRENT = loadCurrent();

    @JsonCreator
    public HogQlGrammarAlternativeIdentities(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
            @JsonProperty("grammarSha256") String grammarSha256,
            @JsonProperty("rules") List<AlternativeRule> rules)
    {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported HogQL grammar alternative identity schema: " + schemaVersion);
        }
        this.schemaVersion = schemaVersion;
        this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
        this.rules = List.copyOf(requireNonNull(rules, "rules is null"));

        Set<String> ruleNames = new HashSet<>();
        Set<String> featureIds = new HashSet<>();
        for (AlternativeRule rule : this.rules) {
            if (!ruleNames.add(rule.rule())) {
                throw new IllegalArgumentException("duplicate HogQL grammar alternative identity rule: " + rule.rule());
            }
            for (Alternative alternative : rule.alternatives()) {
                if (!featureIds.add(alternative.id())) {
                    throw new IllegalArgumentException("duplicate HogQL grammar alternative identity: " + alternative.id());
                }
            }
        }
    }

    public static HogQlGrammarAlternativeIdentities current()
    {
        return CURRENT.identities();
    }

    public static String currentSha256()
    {
        return CURRENT.sha256();
    }

    private static LoadedIdentities loadCurrent()
    {
        byte[] content = readResourceBytes();
        HogQlGrammarAlternativeIdentities identities = CODEC.fromJson(new String(content, StandardCharsets.UTF_8));
        HogQlLanguageContract languageContract = HogQlLanguageContract.current();
        if (!identities.languageVersion().equals(languageContract.languageVersion()) || !identities.grammarSha256().equals(languageContract.grammarSha256())) {
            throw new IllegalStateException("HogQL grammar alternative identities do not match the language contract");
        }
        return new LoadedIdentities(identities, sha256(content));
    }

    private static byte[] readResourceBytes()
    {
        try (InputStream input = HogQlGrammarAlternativeIdentities.class.getResourceAsStream(CURRENT_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("HogQL grammar alternative identities are missing: " + CURRENT_RESOURCE);
            }
            return input.readAllBytes();
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to read HogQL grammar alternative identities", e);
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

    private record LoadedIdentities(HogQlGrammarAlternativeIdentities identities, String sha256) {}

    public record AlternativeRule(String rule, boolean queryReachable, List<Alternative> alternatives)
    {
        @JsonCreator
        public AlternativeRule(
                @JsonProperty("rule") String rule,
                @JsonProperty("queryReachable") boolean queryReachable,
                @JsonProperty("alternatives") List<Alternative> alternatives)
        {
            this.rule = requireNonNull(rule, "rule is null");
            this.queryReachable = queryReachable;
            this.alternatives = List.copyOf(requireNonNull(alternatives, "alternatives is null"));
            if (rule.isBlank() || this.alternatives.size() < 2) {
                throw new IllegalArgumentException("HogQL grammar alternative identity rule must have multiple alternatives: " + rule);
            }

            Set<String> fingerprints = new HashSet<>();
            for (Alternative alternative : this.alternatives) {
                String prefix = "alternative:" + rule + ":";
                if (!alternative.id().startsWith(prefix)) {
                    throw new IllegalArgumentException("HogQL grammar alternative identity does not match its rule: " + alternative.id());
                }
                String semanticId = alternative.id().substring(prefix.length());
                if (semanticId.isBlank() || semanticId.matches("(?:alt(?:ernative)?[-:]?)?\\d+")) {
                    throw new IllegalArgumentException("HogQL grammar alternative identity must not be positional: " + alternative.id());
                }
                if (!fingerprints.add(alternative.structuralFingerprint())) {
                    throw new IllegalArgumentException("duplicate structural fingerprint for HogQL grammar rule: " + rule);
                }
            }
        }
    }

    public record Alternative(String id, String structuralFingerprint)
    {
        @JsonCreator
        public Alternative(
                @JsonProperty("id") String id,
                @JsonProperty("structuralFingerprint") String structuralFingerprint)
        {
            this.id = requireNonNull(id, "id is null");
            this.structuralFingerprint = requireNonNull(structuralFingerprint, "structuralFingerprint is null");
            if (id.isBlank() || !structuralFingerprint.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("invalid HogQL grammar alternative identity: " + id);
            }
        }
    }
}
