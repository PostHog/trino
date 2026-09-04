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
import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public record HogQlLanguageContract(
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        String antlrVersion,
        String canonicalParser,
        List<Corpus> corpora,
        Map<String, String> entryPoints,
        List<GrammarFile> files,
        GrammarFeatureManifest grammarFeatureManifest,
        String grammarSha256)
{
    private static final String CURRENT_CONTRACT_RESOURCE = "/io/trino/hogql/parser/language/1.0.0/language.json";
    private static final JsonCodec<HogQlLanguageContract> CONTRACT_CODEC = jsonCodec(HogQlLanguageContract.class);
    private static final HogQlLanguageContract CURRENT = loadCurrent();

    @JsonCreator
    public HogQlLanguageContract(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
            @JsonProperty("antlrVersion") String antlrVersion,
            @JsonProperty("canonicalParser") String canonicalParser,
            @JsonProperty("corpora") List<Corpus> corpora,
            @JsonProperty("entryPoints") Map<String, String> entryPoints,
            @JsonProperty("files") List<GrammarFile> files,
            @JsonProperty("grammarFeatureManifest") GrammarFeatureManifest grammarFeatureManifest,
            @JsonProperty("grammarSha256") String grammarSha256)
    {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported HogQL language contract schema: " + schemaVersion);
        }
        this.schemaVersion = schemaVersion;
        this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        this.antlrVersion = requireNonNull(antlrVersion, "antlrVersion is null");
        this.canonicalParser = requireNonNull(canonicalParser, "canonicalParser is null");
        this.corpora = List.copyOf(requireNonNull(corpora, "corpora is null"));
        this.entryPoints = Map.copyOf(requireNonNull(entryPoints, "entryPoints is null"));
        this.files = List.copyOf(requireNonNull(files, "files is null"));
        this.grammarFeatureManifest = requireNonNull(grammarFeatureManifest, "grammarFeatureManifest is null");
        this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
    }

    public static HogQlLanguageContract current()
    {
        return CURRENT;
    }

    private static HogQlLanguageContract loadCurrent()
    {
        try (InputStream input = HogQlLanguageContract.class.getResourceAsStream(CURRENT_CONTRACT_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("HogQL language contract resource is missing: " + CURRENT_CONTRACT_RESOURCE);
            }
            return CONTRACT_CODEC.fromJson(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to read HogQL language contract", e);
        }
    }

    public record GrammarFile(String path, String sha256)
    {
        @JsonCreator
        public GrammarFile(
                @JsonProperty("path") String path,
                @JsonProperty("sha256") String sha256)
        {
            this.path = requireNonNull(path, "path is null");
            this.sha256 = requireNonNull(sha256, "sha256 is null");
        }
    }

    public record GrammarFeatureManifest(String path, int schemaVersion, String sha256)
    {
        @JsonCreator
        public GrammarFeatureManifest(
                @JsonProperty("path") String path,
                @JsonProperty("schemaVersion") int schemaVersion,
                @JsonProperty("sha256") String sha256)
        {
            this.path = requireNonNull(path, "path is null");
            if (schemaVersion != 1) {
                throw new IllegalArgumentException("unsupported HogQL grammar feature schema: " + schemaVersion);
            }
            this.schemaVersion = schemaVersion;
            this.sha256 = requireNonNull(sha256, "sha256 is null");
        }
    }

    public record Corpus(
            String manifestPath,
            String manifestSha256,
            String oraclePath,
            String oracleSha256,
            int schemaVersion,
            String slice)
    {
        @JsonCreator
        public Corpus(
                @JsonProperty("manifestPath") String manifestPath,
                @JsonProperty("manifestSha256") String manifestSha256,
                @JsonProperty("oraclePath") String oraclePath,
                @JsonProperty("oracleSha256") String oracleSha256,
                @JsonProperty("schemaVersion") int schemaVersion,
                @JsonProperty("slice") String slice)
        {
            this.manifestPath = requireNonNull(manifestPath, "manifestPath is null");
            this.manifestSha256 = requireNonNull(manifestSha256, "manifestSha256 is null");
            this.oraclePath = requireNonNull(oraclePath, "oraclePath is null");
            this.oracleSha256 = requireNonNull(oracleSha256, "oracleSha256 is null");
            if (schemaVersion != 1) {
                throw new IllegalArgumentException("unsupported HogQL corpus schema: " + schemaVersion);
            }
            this.schemaVersion = schemaVersion;
            this.slice = requireNonNull(slice, "slice is null");
        }
    }
}
