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

import io.trino.hogql.parser.HogQlGrammarAlternativeIdentities.AlternativeRule;
import org.antlr.v4.Tool;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.LexerGrammar;
import org.antlr.v4.tool.Rule;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlGrammarAlternativeIdentities
{
    private static final String GRAMMAR_RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/grammar/";
    private static final Map<String, Set<String>> CANONICAL_FINGERPRINTS = loadCanonicalFingerprints();

    @ParameterizedTest(name = "{0}")
    @MethodSource("alternativeRules")
    public void testSemanticIdentitiesMatchCanonicalAlternativeStructure(String ruleName, AlternativeRule expected)
    {
        assertThat(CANONICAL_FINGERPRINTS.get(ruleName))
                .containsExactlyInAnyOrderElementsOf(expected.alternatives().stream()
                        .map(HogQlGrammarAlternativeIdentities.Alternative::structuralFingerprint)
                        .toList());
    }

    private static Stream<Arguments> alternativeRules()
    {
        return HogQlGrammarAlternativeIdentities.current().rules().stream()
                .map(rule -> Arguments.of(rule.rule(), rule));
    }

    private static Map<String, Set<String>> loadCanonicalFingerprints()
    {
        try {
            assertThat(Tool.VERSION).isEqualTo(HogQlLanguageContract.current().antlrVersion());
            String javaLexer = readResource("HogQLLexer.java.g4");
            String commonLexer = readResource("HogQLLexer.common.g4");
            String completeLexer = javaLexer + commonLexer.substring(commonLexer.indexOf('\n') + 1);
            LexerGrammar lexer = new LexerGrammar(completeLexer);
            Grammar parser = new Grammar(readResource("HogQLParser.g4"), lexer);

            Map<String, Set<String>> fingerprints = new HashMap<>();
            for (AlternativeRule expected : HogQlGrammarAlternativeIdentities.current().rules()) {
                Rule rule = parser.getRule(expected.rule());
                Set<String> alternatives = new HashSet<>();
                for (int alternative = 1; alternative <= rule.numberOfAlts; alternative++) {
                    alternatives.add(sha256(rule.alt[alternative].ast.toStringTree()));
                }
                fingerprints.put(expected.rule(), Set.copyOf(alternatives));
            }
            return Map.copyOf(fingerprints);
        }
        catch (Exception e) {
            throw new AssertionError("failed to inspect canonical HogQL grammar", e);
        }
    }

    private static String readResource(String name)
            throws IOException
    {
        try (InputStream input = TestHogQlGrammarAlternativeIdentities.class.getResourceAsStream(GRAMMAR_RESOURCE_ROOT + name)) {
            if (input == null) {
                throw new IOException("missing canonical HogQL grammar resource: " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String sha256(String value)
    {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
