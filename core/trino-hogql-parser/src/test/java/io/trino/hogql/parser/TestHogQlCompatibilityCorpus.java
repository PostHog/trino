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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class TestHogQlCompatibilityCorpus
{
    private static final String RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/";
    private static final JsonCodec<CorpusManifest> CORPUS_MANIFEST_CODEC = jsonCodec(CorpusManifest.class);
    private static final JsonCodec<OracleCase> ORACLE_CASE_CODEC = jsonCodec(OracleCase.class);
    private static final JsonCodec<CompatibilityOverrides> OVERRIDES_CODEC = jsonCodec(CompatibilityOverrides.class);

    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testOracleMetadataMatchesThePublishedCorpus()
            throws IOException
    {
        CorpusManifest manifest = loadCorpusManifest();
        Map<String, OracleCase> oracleById = loadOracleCases().stream()
                .collect(toMap(OracleCase::id, oracleCase -> oracleCase));

        assertThat(oracleById).hasSize(manifest.cases().size());
        for (CorpusCase corpusCase : manifest.cases()) {
            OracleCase oracleCase = oracleById.remove(corpusCase.id());
            assertThat(oracleCase).isNotNull();
            assertThat(oracleCase.source()).isEqualTo(corpusCase.source());
            assertThat(oracleCase.entryPoint()).isEqualTo(corpusCase.entryPoint());
            assertThat(oracleCase.accepted()).isEqualTo(corpusCase.accepted());
            assertThat(oracleCase.languageVersion()).isEqualTo(HogQlLanguageContract.current().languageVersion());
            assertThat(oracleCase.grammarSha256()).isEqualTo(HogQlLanguageContract.current().grammarSha256());
        }
        assertThat(oracleById).isEmpty();
    }

    @TestFactory
    public Stream<DynamicTest> testTrinoDispositionForEveryCorpusCase()
            throws IOException
    {
        CorpusManifest manifest = loadCorpusManifest();
        Map<String, CorpusDisposition> dispositionById = loadCompatibilityOverrides().corpusCases().stream()
                .collect(toMap(CorpusDisposition::id, disposition -> disposition));

        assertThat(dispositionById.keySet())
                .containsExactlyInAnyOrderElementsOf(manifest.cases().stream().map(CorpusCase::id).toList());
        return manifest.cases().stream()
                .map(corpusCase -> dynamicTest(corpusCase.id(), () -> assertDisposition(corpusCase, dispositionById.get(corpusCase.id()))));
    }

    private void assertDisposition(CorpusCase corpusCase, CorpusDisposition disposition)
    {
        String query = switch (corpusCase.entryPoint()) {
            case "expression" -> "SELECT " + corpusCase.source();
            case "query" -> corpusCase.source();
            default -> throw new IllegalArgumentException("unknown corpus entry point: " + corpusCase.entryPoint());
        };
        switch (disposition.status()) {
            case "supported" -> assertThatCode(() -> parser.parseStatement(query)).doesNotThrowAnyException();
            case "explicitCurrentError" -> assertThatThrownBy(() -> parser.parseStatement(query))
                    .isInstanceOf(HogQlParsingException.class)
                    .hasMessageContaining("HogQL feature is not lowered yet");
            case "canonicalRejection" -> assertThatThrownBy(() -> parser.parseStatement(query))
                    .isInstanceOf(HogQlParsingException.class)
                    .hasMessageStartingWith("line ");
            default -> throw new IllegalArgumentException("unknown corpus disposition: " + disposition.status());
        }
    }

    private static CorpusManifest loadCorpusManifest()
            throws IOException
    {
        return CORPUS_MANIFEST_CODEC.fromJson(readResource("corpus/expr_select_cases.json"));
    }

    private static List<OracleCase> loadOracleCases()
            throws IOException
    {
        return Arrays.stream(readResource("corpus/expr_select_cpp_oracle.jsonl").split("\\R"))
                .filter(line -> !line.isBlank())
                .map(ORACLE_CASE_CODEC::fromJson)
                .toList();
    }

    private static CompatibilityOverrides loadCompatibilityOverrides()
            throws IOException
    {
        return OVERRIDES_CODEC.fromJson(readResource("trino-compatibility-overrides.json"));
    }

    private static String readResource(String relativePath)
            throws IOException
    {
        String path = RESOURCE_ROOT + relativePath;
        try (InputStream input = TestHogQlCompatibilityCorpus.class.getResourceAsStream(path)) {
            if (input == null) {
                fail("missing test resource: " + path);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public record CorpusManifest(List<CorpusCase> cases)
    {
        @JsonCreator
        public CorpusManifest(@JsonProperty("cases") List<CorpusCase> cases)
        {
            this.cases = List.copyOf(requireNonNull(cases, "cases is null"));
        }
    }

    public record CorpusCase(String id, String source, String entryPoint, boolean accepted)
    {
        @JsonCreator
        public CorpusCase(
                @JsonProperty("id") String id,
                @JsonProperty("source") String source,
                @JsonProperty("entryPoint") String entryPoint,
                @JsonProperty("accepted") boolean accepted)
        {
            this.id = requireNonNull(id, "id is null");
            this.source = requireNonNull(source, "source is null");
            this.entryPoint = requireNonNull(entryPoint, "entryPoint is null");
            this.accepted = accepted;
        }
    }

    public record OracleCase(
            String id,
            String source,
            String entryPoint,
            boolean accepted,
            HogQlLanguageVersion languageVersion,
            String grammarSha256)
    {
        @JsonCreator
        public OracleCase(
                @JsonProperty("id") String id,
                @JsonProperty("source") String source,
                @JsonProperty("entryPoint") String entryPoint,
                @JsonProperty("accepted") boolean accepted,
                @JsonProperty("languageVersion") HogQlLanguageVersion languageVersion,
                @JsonProperty("grammarSha256") String grammarSha256)
        {
            this.id = requireNonNull(id, "id is null");
            this.source = requireNonNull(source, "source is null");
            this.entryPoint = requireNonNull(entryPoint, "entryPoint is null");
            this.accepted = accepted;
            this.languageVersion = requireNonNull(languageVersion, "languageVersion is null");
            this.grammarSha256 = requireNonNull(grammarSha256, "grammarSha256 is null");
        }
    }

    public record CompatibilityOverrides(List<CorpusDisposition> corpusCases)
    {
        @JsonCreator
        public CompatibilityOverrides(@JsonProperty("corpusCases") List<CorpusDisposition> corpusCases)
        {
            this.corpusCases = List.copyOf(requireNonNull(corpusCases, "corpusCases is null"));
        }
    }

    public record CorpusDisposition(String id, String status)
    {
        @JsonCreator
        public CorpusDisposition(
                @JsonProperty("id") String id,
                @JsonProperty("status") String status)
        {
            this.id = requireNonNull(id, "id is null");
            this.status = requireNonNull(status, "status is null");
        }
    }
}
