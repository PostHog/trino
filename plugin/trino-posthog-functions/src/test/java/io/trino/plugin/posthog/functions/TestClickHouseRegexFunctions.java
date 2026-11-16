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
package io.trino.plugin.posthog.functions;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * The fixtures come from real ClickHouse (26.10.3.7; 25.8.33.6 returns the same) by
 * {@code src/test/resources/generate-multimatchanyindex-fixtures.py}, whose docstring has the
 * command. Every expected value below that is not from a fixture was also measured on ClickHouse.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestClickHouseRegexFunctions
{
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().build();
    private static final HexFormat HEX = HexFormat.of();

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new PostHogFunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testBotPatternsMatchClickHouse()
            throws IOException
    {
        // The built-in bot patterns of PostHog's traffic classification against synthetic user
        // agents, with and without the '^$' pattern it appends for the empty user agent
        List<String> patterns = readJsonStrings(resource("/multimatchanyindex-bot-patterns.json"));
        assertThat(patterns).hasSize(485);
        String withEmpty = sqlArray(concat(patterns, "^$"));
        String withoutEmpty = sqlArray(patterns);

        List<String> lines = resource("/multimatchanyindex-bots-clickhouse.tsv").lines().toList();
        assertThat(lines).hasSize(56);

        // one query over all user agents, so the generated projection compiles the constant array once
        String rows = lines.stream()
                .map(line -> line.split("\t", -1))
                .map(fields -> "(from_utf8(from_hex('%s')), BIGINT '%s', BIGINT '%s')".formatted(fields[0], fields[1], fields[2]))
                .collect(Collectors.joining(", "));
        assertThat(assertions.query(
                """
                SELECT count_if(ch_multimatchanyindex(user_agent, %s) = with_empty),
                    count_if(ch_multimatchanyindex(user_agent, %s) = without_empty)
                FROM (VALUES %s) AS t(user_agent, with_empty, without_empty)
                """.formatted(withEmpty, withoutEmpty, rows)))
                .matches("VALUES (BIGINT '56', BIGINT '56')");

        for (String line : lines) {
            String[] fields = line.split("\t", -1);
            String userAgent = "from_utf8(from_hex('" + fields[0] + "'))";
            assertThat(assertions.function("ch_multimatchanyindex", userAgent, withEmpty))
                    .describedAs("user agent %s", decodeUtf8(fields[0]))
                    .isEqualTo(Long.parseLong(fields[1]));
            assertThat(assertions.function("ch_multimatchanyindex", userAgent, withoutEmpty))
                    .describedAs("user agent %s without '^$'", decodeUtf8(fields[0]))
                    .isEqualTo(Long.parseLong(fields[2]));
        }
    }

    @Test
    public void testRandomPatternsMatchClickHouse()
            throws IOException
    {
        // Random pattern sets (anchors, word boundaries, classes, repeats, escapes, flags) and
        // haystacks (multi-byte UTF-8, newlines, case variants), each set against 8 haystacks
        List<String> lines = resource("/multimatchanyindex-fuzz-clickhouse.jsonl").lines().toList();
        assertThat(lines).hasSize(400);
        int checked = 0;
        for (int line = 0; line < lines.size(); line++) {
            FuzzCase fuzzCase = parseFuzzCase(lines.get(line));
            ClickHouseMultiMatch matcher = ClickHouseMultiMatch.compile(fuzzCase.patterns().stream().map(Slices::utf8Slice).toList());
            for (int i = 0; i < fuzzCase.haystacks().size(); i++) {
                String haystack = fuzzCase.haystacks().get(i);
                assertThat(matcher.find(Slices.utf8Slice(haystack)))
                        .describedAs("haystack %s, patterns %s", haystack, fuzzCase.patterns())
                        .isEqualTo(fuzzCase.results().get(i));
                checked++;
                // and some through SQL, for the whole function
                if (line % 25 == 0) {
                    assertThat(assertions.function("ch_multimatchanyindex", sqlString(haystack), sqlArray(fuzzCase.patterns())))
                            .describedAs("haystack %s, patterns %s", haystack, fuzzCase.patterns())
                            .isEqualTo(fuzzCase.results().get(i));
                }
            }
        }
        assertThat(checked).isEqualTo(3200);
    }

    @Test
    public void testEarliestEndWins()
    {
        // the pattern whose match ends first, not the first pattern in the array that matches
        assertMatch("abc", 2, "c", "a", "b");
        assertMatch("Mozilla bot", 2, "bot", "Mozilla");
        // the end of the shortest match: a+ matches "a" at offset 1, before "aa" ends
        assertMatch("aaa", 2, "aa", "a+");
        assertMatch("xaaab", 2, "a+b", "aa");
        assertMatch("aaab", 2, "a+?b", "aaa");
        assertMatch("aaaa", 2, "a{3}", "a{2,}");
        assertMatch("xabcd", 2, "abcd", "b.?c");
        assertMatch("xx Googlebot curl/", 3, "curl/", "Googlebot", "x+");
        // ties go to the lower index
        assertMatch("abc", 1, "bc", "c");
        assertMatch("abc", 1, "c", "bc");
        assertMatch("abc", 1, "b+c", "c");
        assertMatch("abc", 1, "^abc", "c");
        // an empty match is confirmed at offset 0
        assertMatch("abc", 2, "b", "");
        assertMatch("ab", 2, "b", "x*");
        assertMatch("ab", 2, "a", "^");
    }

    @Test
    public void testMatchesThatNeedTheEndOfTheDataComeLast()
    {
        // $, \Z and \z are only confirmed after the whole haystack is scanned
        assertMatch("abc", 2, "c$", "abc");
        assertMatch("abc", 2, "c\\z", "c");
        assertMatch("ab\n", 2, "b$", "\n");
        assertMatch("ab\n", 2, "b\\Z", "\n");
        assertMatch("ab\n", 2, "b$", "b");
        assertMatch("ab\n", 1, "b", "b$");
        assertMatch("Googlebot", 2, "bot$", "Google");
        // among themselves they keep index order
        assertMatch("abc", 1, "c\\b", "c$", "bc$", "c\\z");
        assertMatch("abc", 1, "bc$", "c\\b", "c$");
        // a word boundary at the end of the haystack also needs the end of the data
        assertMatch("a bot", 2, "bot\\b", "bot");
        assertMatch("a bot", 1, "bot", "bot\\b");
    }

    @Test
    public void testTrailingAssertionsNeedTheNextByte()
    {
        // a trailing \b or \B is confirmed one byte after the match ends
        assertMatch("a bot x", 2, "bot\\b", "bot");
        assertMatch("bot!", 1, "bot\\b", "bot!");
        assertMatch("bot!", 1, "bot!", "bot\\b");
        assertMatch("ab", 1, "a", "\\b");
        assertMatch("ab", 1, "\\b", "a");
        assertMatch("abc", 1, "b\\B", "bc");
        assertMatch("ab", 2, "b\\B", "ab");
        // and so is a multi-line $, which needs to see the newline
        assertMatch("ab\nc", 1, "(?m)b$", "\n");
        assertMatch("ab\nc", 1, "\n", "(?m)b$");
        assertMatch("ab\nc", 1, "b\n", "(?m)b$");
        // one byte, not one code point: the first byte of é is enough
        assertMatch("botéx", 2, "boté", "bot\\b");
        assertMatch("bot€x", 2, "bot€", "bot\\b");
        assertMatch("bot😀x", 2, "bot😀", "bot\\b");
        assertMatch("botax", 1, "bota", "bot\\B");
    }

    @Test
    public void testEmptyInputs()
    {
        assertMatch("", 1, "^$");
        assertMatch("", 0, "a");
        assertMatch("", 2, "a", "");
        // with an empty haystack every match comes at the end of the data, in index order
        assertMatch("", 1, "$", "^");
        assertMatch("", 1, "^$", "");
        assertMatch("", 3, "a", "\\b", "\\B", "$");
        assertMatch("", 1, "(?m)^S?");
        assertMatch("abc", 0);
        assertMatch("abc", 0, "x");
        assertMatch("abc", 1, "", "a");
        assertThat(assertions.function("ch_multimatchanyindex", "'abc'", "CAST(ARRAY[] AS array(varchar))")).isEqualTo(0L);
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.function("ch_multimatchanyindex", "CAST(NULL AS varchar)", "ARRAY['a']")).isNull(BIGINT);
        assertThat(assertions.function("ch_multimatchanyindex", "'a'", "CAST(NULL AS array(varchar))")).isNull(BIGINT);
        // ClickHouse rejects Array(Nullable(String)) as the pattern type
        assertTrinoExceptionThrownBy(assertions.function("ch_multimatchanyindex", "'a'", "ARRAY['a', NULL]")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("must not contain NULL");
    }

    @Test
    public void testFlagsAndAnchors()
    {
        // case-sensitive by default, (?i) folds Unicode letters
        assertMatch("ABC", 0, "abc");
        assertMatch("ABC", 1, "(?i)abc");
        assertMatch("É", 1, "(?i)é");
        assertMatch("ſ", 1, "(?i)s");
        assertMatch("\u212A", 1, "(?i)k");
        assertMatch("ẞ", 1, "(?i)ß");
        assertMatch("\u212A", 1, "(?i)^[a-z]$");
        assertMatch("\u017F", 1, "(?i)^[r-t]$");
        // class escapes and POSIX classes fold only ASCII
        assertMatch("\u212A", 0, "(?i)^\\w$");
        assertMatch("\u212A", 0, "(?i)^[[:alpha:]]$");
        assertMatch("k", 1, "(?i)^[[:upper:]]$");
        assertMatch("İ", 0, "(?i)i");
        assertMatch("ı", 0, "(?i)I");
        // an inline flag lasts to the end of its group, across later branches
        assertMatch("C", 1, "(a(?i)b|c)");
        assertMatch("Ab", 0, "(a(?i)b|c)");
        assertMatch("xC", 0, "x(?:(?i)z|y)|c");
        // . matches a newline unless (?-s)
        assertMatch("a\nb", 1, "a.b");
        assertMatch("a\nb", 0, "(?-s)a.b");
        // $ and \Z match at the end or before one final \n only, not before \r or other line terminators
        assertMatch("ab\n", 1, "b$");
        assertMatch("ab\n", 0, "b\\z");
        assertMatch("ab\r", 0, "b$");
        assertMatch("ab\r\n", 0, "b$");
        assertMatch("ab\n\n", 0, "b$");
        assertMatch("ab\u0085", 0, "b$");
        assertMatch("ab\u2028", 0, "b$");
        assertMatch("ab\nx", 0, "ab$");
        assertMatch("x\nab", 0, "^ab");
        assertMatch("ab\nx", 1, "(?m)ab$");
        assertMatch("x\nab", 1, "(?m)^ab");
        assertMatch("ab\n", 0, "(?m)^$");
        assertMatch("a\n\nb", 1, "(?m)^$");
    }

    @Test
    public void testUtf8()
    {
        // . and classes match one code point, \d \w \s \b and POSIX classes are ASCII only
        assertMatch("é", 1, "^.$");
        assertMatch("é", 0, "^..$");
        assertMatch("é", 1, "^[^a]$");
        assertMatch("é", 1, "^[é]$");
        assertMatch("é", 0, "\\w");
        assertMatch("aé", 1, "a\\b");
        assertMatch("٣", 0, "\\d");
        assertMatch("a\u00A0b", 0, "a\\sb");
        assertMatch("a\u000Bb", 1, "a\\sb");
        assertMatch("a\u00A0b", 1, "a\\hb");
        assertMatch("a\u3000b", 1, "a\\hb");
        assertMatch("a\u2028b", 1, "a\\vb");
        assertMatch("a\u0085b", 1, "a\\vb");
        assertMatch("é", 0, "[[:alpha:]]");
        assertMatch("é", 1, "\\pL");
        assertMatch("x", 1, "\\p{Latin}");
        assertMatch("é", 1, "\\x{e9}");
        assertMatch("é", 0, "\\xA9");
    }

    @Test
    public void testPcreSyntax()
    {
        // octal escapes and back-reference numbers follow PCRE, not Java
        assertMatch("a", 1, "\\141");
        assertMatch("\u000C1", 1, "^\\0141$");
        assertMatch("\t", 1, "\\11");
        assertMatch("8", 1, "\\8");
        assertMatch("y", 1, "\\y");
        assertMatch("\u0001", 1, "\\cA");
        assertMatch("\u0001", 1, "\\ca");
        // a brace that does not start {n}, {n,} or {n,m} is a literal
        assertMatch("a{,2}", 1, "a{,2}");
        assertMatch("aa", 0, "^a{,2}$");
        assertMatch("a{", 1, "a{");
        assertMatch("a.b", 1, "\\Qa.b\\E");
        assertMatch("a+b", 0, "\\Qa.b\\E");
        assertMatch("]", 1, "^[]a]$");
        assertMatch("b", 1, "^[^]]$");
        assertMatch("]", 0, "^[^]]$");
        assertMatch("-", 1, "^[\\d-z]$");
        assertMatch("x", 1, "^[[:alpha:]-z]$");
        assertMatch("ab", 1, "(?x) a b # comment\n");
        assertMatch("ab", 1, "(?#comment)ab");
        assertMatch("ab", 1, "(?<n>a)(?P<m>b)");
        assertMatch("x", 1, "(?'n'x)");
        assertMatch("x", 1, "a|");
        assertMatch("x", 1, "()");
        // ClickHouse passes patterns to Hyperscan as C strings, so a pattern ends at a NUL
        assertThat(assertions.function("ch_multimatchanyindex", "'ab'", "ARRAY['zzz', 'b' || chr(0) || 'zzz']")).isEqualTo(2L);
        assertThat(assertions.function("ch_multimatchanyindex", "'zzz'", "ARRAY[chr(0) || 'abc']")).isEqualTo(1L);
    }

    @Test
    public void testPatternsClickHouseRejects()
    {
        assertInvalid("(a)\\1", "Back-references are unsupported");
        assertInvalid("(?P=n)", "Back-references are unsupported");
        assertInvalid("\\g1", "Back-references are unsupported");
        assertInvalid("(?=a)b", "Zero-width assertions are not supported");
        assertInvalid("a(?!b)", "Zero-width assertions are not supported");
        assertInvalid("(?<=a)b", "Zero-width assertions are not supported");
        assertInvalid("(?<!a)b", "Zero-width assertions are not supported");
        assertInvalid("(?>a)", "Atomic groups are unsupported");
        assertInvalid("a*+", "Possessive quantifiers are not supported");
        assertInvalid("a{2}+", "Possessive quantifiers are not supported");
        assertInvalid("(?(1)a|b)", "Conditional subpatterns are unsupported");
        assertInvalid("\\Ka", "\\K not supported");
        assertInvalid("a$b?", "Embedded end anchors not supported");
        assertInvalid("(a$|b)c", "Embedded end anchors not supported");
        assertInvalid("a$\\b", "Embedded end anchors not supported");
        assertInvalid("(a$)+", "Embedded end anchors not supported");
        assertInvalid("a^b", "Embedded start anchors not supported");
        assertInvalid("b*^a", "Embedded start anchors not supported");
        assertInvalid("\\b^a", "Embedded start anchors not supported");
        assertInvalid("(?m)^^a", "Embedded start anchors not supported");
        assertInvalid("[a&&b]", "Character class intersection/subtraction is not supported");
        assertInvalid("[[a]", "Nested character classes are not supported");
        assertInvalid("[[:foo:]]", "Invalid POSIX named class");
        assertInvalid("[z-a]", "Range out of order in character class");
        assertInvalid("[a-\\d]", "Invalid range in character class");
        assertInvalid("(", "Missing close parenthesis");
        assertInvalid(")", "Unmatched parentheses");
        assertInvalid("[a", "Unterminated character class");
        assertInvalid("[\\E]", "Unterminated character class");
        assertInvalid("\\", "Unescaped \\ at end of input");
        assertInvalid("*a", "Invalid repeat");
        assertInvalid("a**", "Invalid repeat");
        assertInvalid("^*", "Invalid repeat");
        assertInvalid("a{0}", "Invalid repeat");
        assertInvalid("x{2,1}", "Invalid repeat");
        assertInvalid("(?U)a", "Unrecognised character after (?");
        assertInvalid("(?P<1a>x)", "Group name cannot begin with a digit");
        assertInvalid("(?<n>x)(?<n>y)", "Two named subpatterns use the name 'n'");
        assertInvalid("\\p{IsGreek}", "Unknown property");
        assertInvalid("(?x)a # comment", "Unterminated comment");
        // reject_expensive_hyperscan_regexps, on by default, checks the pattern text for repeats over 50
        assertInvalid("a{51}", "will be too slow");
        assertInvalid("a{1,51}", "will be too slow");
        assertInvalid("\\{51}", "will be too slow");
        assertInvalid("[a{ 51 }]", "will be too slow");
        assertMatch("aaa", 0, "a{50}");
        assertMatch("x{050}", 0, "x{050}");
        assertTrinoExceptionThrownBy(() -> ClickHouseMultiMatch.compile(List.of(Slices.wrappedBuffer((byte) 0xFF))))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Expression is not valid UTF-8");
        assertTrinoExceptionThrownBy(assertions.function("ch_multimatchanyindex", "'a'", "ARRAY['a', '(?=a)']")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Zero-width assertions are not supported");
    }

    @Test
    public void testPatternsJavaCannotReproduce()
    {
        // ClickHouse 26.10 matches these wrongly (^\W$ matches "a"), ClickHouse 25.8 correctly
        assertNotSupported("^\\W$");
        assertNotSupported("^\\S$");
        assertNotSupported("\\H");
        assertNotSupported("[^ac]");
        assertNotSupported("[[:^alpha:]]");
        assertNotSupported("\\P{L}");
        // Hyperscan folds negated classes inconsistently: (?i)^[^a]$ matches "a" but not "A"
        assertNotSupported("(?i)^[^a]$");
        assertNotSupported("(?i)\\W");
        // a single excluded range works in both versions
        assertMatch("b", 1, "^[^a]$");
        assertMatch("a", 0, "^[^a]$");
        assertMatch("d", 1, "^[^a-c]$");
        assertMatch("a", 1, "^\\D$");
        assertMatch("1", 0, "^\\D$");
        assertMatch("A", 1, "(?i)^[^\\d]$");
    }

    private void assertMatch(String haystack, long expected, String... patterns)
    {
        List<Slice> slices = Arrays.stream(patterns).map(Slices::utf8Slice).toList();
        assertThat(ClickHouseMultiMatch.compile(slices).find(Slices.utf8Slice(haystack)))
                .describedAs("haystack %s, patterns %s", haystack, Arrays.toString(patterns))
                .isEqualTo(expected);
        String array = patterns.length == 0 ? "CAST(ARRAY[] AS array(varchar))" : sqlArray(Arrays.asList(patterns));
        assertThat(assertions.function("ch_multimatchanyindex", sqlString(haystack), array))
                .describedAs("haystack %s, patterns %s", haystack, Arrays.toString(patterns))
                .isEqualTo(expected);
    }

    private static void assertInvalid(String pattern, String message)
    {
        assertTrinoExceptionThrownBy(() -> ClickHouseMultiMatch.compile(List.of(Slices.utf8Slice(pattern))))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining(message);
    }

    private static void assertNotSupported(String pattern)
    {
        assertTrinoExceptionThrownBy(() -> ClickHouseMultiMatch.compile(List.of(Slices.utf8Slice(pattern))))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("does not support");
    }

    private static String sqlString(String value)
    {
        // from_hex keeps control characters and NUL out of the SQL text
        return "from_utf8(from_hex('" + HEX.formatHex(value.getBytes(UTF_8)) + "'))";
    }

    private static String sqlArray(List<String> values)
    {
        return values.stream()
                .map(value -> "'" + value.replace("'", "''") + "'")
                .collect(Collectors.joining(", ", "ARRAY[", "]"));
    }

    private static List<String> concat(List<String> values, String value)
    {
        List<String> result = new ArrayList<>(values);
        result.add(value);
        return result;
    }

    private String resource(String name)
            throws IOException
    {
        try (InputStream input = requireNonNull(getClass().getResourceAsStream(name), name)) {
            return decodeUtf8(input.readAllBytes());
        }
    }

    private static String decodeUtf8(String hex)
    {
        return decodeUtf8(HEX.parseHex(hex));
    }

    // strict, because a replacement character would test different text than ClickHouse saw
    private static String decodeUtf8(byte[] bytes)
    {
        try {
            return UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        }
        catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Fixture is not valid UTF-8", e);
        }
    }

    private static List<String> readJsonStrings(String json)
            throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            return readStringArray(parser, parser.nextToken());
        }
    }

    private static List<String> readStringArray(JsonParser parser, JsonToken start)
            throws IOException
    {
        assertThat(start).isEqualTo(JsonToken.START_ARRAY);
        List<String> values = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            values.add(parser.getText());
        }
        return values;
    }

    private static FuzzCase parseFuzzCase(String line)
            throws IOException
    {
        List<String> patterns = null;
        List<String> haystacks = null;
        List<Long> results = new ArrayList<>();
        try (JsonParser parser = JSON_FACTORY.createParser(line)) {
            assertThat(parser.nextToken()).isEqualTo(JsonToken.START_OBJECT);
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String field = parser.currentName();
                JsonToken start = parser.nextToken();
                switch (field) {
                    case "patterns" -> patterns = readStringArray(parser, start).stream().map(TestClickHouseRegexFunctions::decodeUtf8).toList();
                    case "haystacks" -> haystacks = readStringArray(parser, start).stream().map(TestClickHouseRegexFunctions::decodeUtf8).toList();
                    case "results" -> {
                        assertThat(start).isEqualTo(JsonToken.START_ARRAY);
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            results.add(parser.getLongValue());
                        }
                    }
                    default -> throw new IllegalArgumentException("Unknown field: " + field);
                }
            }
        }
        return new FuzzCase(requireNonNull(patterns, "patterns"), requireNonNull(haystacks, "haystacks"), results);
    }

    private record FuzzCase(List<String> patterns, List<String> haystacks, List<Long> results) {}
}
