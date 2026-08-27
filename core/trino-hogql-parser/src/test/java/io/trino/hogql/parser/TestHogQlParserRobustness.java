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
import io.trino.hogql.parser.tree.HogQlQuery.SourceSpan;
import io.trino.hogql.parser.tree.HogQlSyntaxTree;
import io.trino.hogql.parser.tree.HogQlSyntaxTree.Element;
import io.trino.hogql.parser.tree.HogQlSyntaxTree.Node;
import io.trino.hogql.parser.tree.HogQlSyntaxTree.Token;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestHogQlParserRobustness
{
    private static final long RANDOM_SEED = 0x484C0FFEEL;
    private static final int GENERATED_ROUNDS = 64;
    private static final int MUTATIONS_PER_INPUT = 2;
    private static final String MALFORMED_RESOURCE = "/io/trino/hogql/parser/language/1.0.0/robustness/malformed-regressions.jsonl";
    private static final JsonCodec<MalformedRegression> MALFORMED_REGRESSION_CODEC = jsonCodec(MalformedRegression.class);

    private static final List<String> ATOMS = List.of(
            "event",
            "value",
            "enabled",
            "properties.plan",
            "0",
            "17",
            "true",
            "NULL",
            "'example'",
            "'😀'");
    private static final List<String> INSERTIONS = List.of("(", ")", "]", "'", "@", ",");
    private static final List<String> APPENDAGES = List.of(" +", " )", " '", " FROM", " WHERE", " /*");

    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testGeneratedAndMutatedInputsHaveOnlyDocumentedOutcomes()
    {
        Random random = new Random(RANDOM_SEED);
        List<GeneratedInput> generated = IntStream.range(0, GENERATED_ROUNDS)
                .boxed()
                .flatMap(index -> generatedInputs(random, index))
                .toList();

        assertThat(generated).hasSize(GENERATED_ROUNDS * 2);
        for (GeneratedInput input : generated) {
            Optional<HogQlSyntaxTree> outcome = parseSafely(input.source(), input.id());
            assertThat(outcome)
                    .as("generated input %s must be valid: %s", input.id(), display(input.source()))
                    .isPresent();
            outcome.ifPresent(tree -> assertValidTree(input.source(), tree));

            for (int mutation = 0; mutation < MUTATIONS_PER_INPUT; mutation++) {
                String mutated = mutate(random, input.source());
                parseSafely(mutated, input.id() + "-mutation-" + mutation)
                        .ifPresent(tree -> assertValidTree(mutated, tree));
            }
        }
    }

    @Test
    public void testMinimizedMalformedRegressionSeedsAreRejectedCleanly()
            throws IOException
    {
        List<MalformedRegression> regressions = loadMalformedRegressions();

        assertThat(regressions)
                .extracting(MalformedRegression::id)
                .containsExactly(
                        "missing-projection",
                        "trailing-binary-operator",
                        "unclosed-tuple",
                        "unexpected-character",
                        "missing-source",
                        "missing-predicate",
                        "unclosed-string",
                        "second-statement");
        for (MalformedRegression regression : regressions) {
            assertThat(regression.source().codePointCount(0, regression.source().length()))
                    .as("regression %s remains minimized", regression.id())
                    .isLessThanOrEqualTo(32);
            assertThat(parseSafely(regression.source(), regression.id()))
                    .as("malformed regression must be rejected: %s", regression.id())
                    .isEmpty();
        }
    }

    private Optional<HogQlSyntaxTree> parseSafely(String source, String id)
    {
        try {
            return Optional.of(parser.parseSyntax(source));
        }
        catch (HogQlParsingException _) {
            return Optional.empty();
        }
        catch (RuntimeException | Error unexpected) {
            throw new AssertionError("unexpected parser failure for " + id + ": " + display(source), unexpected);
        }
    }

    private static Stream<GeneratedInput> generatedInputs(Random random, int index)
    {
        String expression = expression(random, 3);
        return Stream.of(
                new GeneratedInput("expression-" + index, "SELECT " + expression),
                new GeneratedInput("query-" + index, query(random, expression)));
    }

    private static String expression(Random random, int depth)
    {
        if (depth == 0) {
            return ATOMS.get(random.nextInt(ATOMS.size()));
        }
        return switch (random.nextInt(8)) {
            case 0 -> "(" + expression(random, depth - 1) + ")";
            case 1 -> "NOT (" + expression(random, depth - 1) + ")";
            case 2 -> "-(" + expression(random, depth - 1) + ")";
            case 3 -> "(" + expression(random, depth - 1) + " + " + expression(random, depth - 1) + ")";
            case 4 -> "(" + expression(random, depth - 1) + " = " + expression(random, depth - 1) + ")";
            case 5 -> "coalesce(" + expression(random, depth - 1) + ", " + expression(random, depth - 1) + ")";
            case 6 -> "[" + expression(random, depth - 1) + ", " + expression(random, depth - 1) + "]";
            default -> "CASE WHEN " + predicate(random, depth - 1) + " THEN " + expression(random, depth - 1) + " ELSE 0 END";
        };
    }

    private static String predicate(Random random, int depth)
    {
        if (depth == 0) {
            return switch (random.nextInt(4)) {
                case 0 -> "event = '$pageview'";
                case 1 -> "value >= 0";
                case 2 -> "enabled";
                default -> "properties.plan IS NOT NULL";
            };
        }
        return switch (random.nextInt(4)) {
            case 0 -> "NOT (" + predicate(random, depth - 1) + ")";
            case 1 -> "(" + predicate(random, depth - 1) + " AND " + predicate(random, depth - 1) + ")";
            case 2 -> "(" + predicate(random, depth - 1) + " OR " + predicate(random, depth - 1) + ")";
            default -> "(" + expression(random, depth - 1) + " = " + expression(random, depth - 1) + ")";
        };
    }

    private static String query(Random random, String projection)
    {
        return switch (random.nextInt(6)) {
            case 0 -> "SELECT " + projection;
            case 1 -> "SELECT " + projection + " AS result FROM events";
            case 2 -> "SELECT " + projection + ", " + expression(random, 2) + " FROM events WHERE " + predicate(random, 2);
            case 3 -> "SELECT\n    " + projection + "\r\nFROM events\nWHERE " + predicate(random, 2);
            case 4 -> "WITH sample AS (SELECT " + projection + " AS result) SELECT result FROM sample";
            default -> "SELECT " + projection + " FROM events ORDER BY event LIMIT " + (random.nextInt(20) + 1);
        };
    }

    private static String mutate(Random random, String source)
    {
        int codePointLength = source.codePointCount(0, source.length());
        return switch (random.nextInt(6)) {
            case 0 -> source.substring(0, source.offsetByCodePoints(0, random.nextInt(codePointLength + 1)));
            case 1 -> deleteCodePoint(source, random.nextInt(codePointLength));
            case 2 -> insertAtCodePoint(source, random.nextInt(codePointLength + 1), INSERTIONS.get(random.nextInt(INSERTIONS.size())));
            case 3 -> replaceCodePoint(source, random.nextInt(codePointLength), ')');
            case 4 -> duplicateCodePoint(source, random.nextInt(codePointLength));
            default -> source + APPENDAGES.get(random.nextInt(APPENDAGES.size()));
        };
    }

    private static String deleteCodePoint(String source, int codePointOffset)
    {
        int start = source.offsetByCodePoints(0, codePointOffset);
        int end = source.offsetByCodePoints(start, 1);
        return source.substring(0, start) + source.substring(end);
    }

    private static String insertAtCodePoint(String source, int codePointOffset, String insertion)
    {
        int offset = source.offsetByCodePoints(0, codePointOffset);
        return source.substring(0, offset) + insertion + source.substring(offset);
    }

    private static String replaceCodePoint(String source, int codePointOffset, int replacement)
    {
        int start = source.offsetByCodePoints(0, codePointOffset);
        int end = source.offsetByCodePoints(start, 1);
        return source.substring(0, start) + Character.toString(replacement) + source.substring(end);
    }

    private static String duplicateCodePoint(String source, int codePointOffset)
    {
        int start = source.offsetByCodePoints(0, codePointOffset);
        int end = source.offsetByCodePoints(start, 1);
        return source.substring(0, end) + source.substring(start);
    }

    private static void assertValidTree(String source, HogQlSyntaxTree tree)
    {
        int sourceLength = source.codePointCount(0, source.length());
        Deque<PendingElement> pending = new ArrayDeque<>();
        pending.add(new PendingElement(tree.root(), Optional.empty()));

        while (!pending.isEmpty()) {
            PendingElement current = pending.removeFirst();
            Element element = current.element();
            SourceSpan span = element.span();
            assertThat(span.startOffset()).isBetween(0, sourceLength);
            assertThat(span.endOffset()).isBetween(span.startOffset(), sourceLength);
            assertPosition(source, span.startOffset(), span.startLine(), span.startColumn());
            assertPosition(source, span.endOffset(), span.endLine(), span.endColumn());
            current.parentSpan().ifPresent(parent -> {
                assertThat(span.startOffset()).isGreaterThanOrEqualTo(parent.startOffset());
                assertThat(span.endOffset()).isLessThanOrEqualTo(parent.endOffset());
            });

            if (element instanceof Node node) {
                node.children().forEach(child -> pending.addLast(new PendingElement(child, Optional.of(span))));
            }
            else if (element instanceof Token token) {
                int start = source.offsetByCodePoints(0, span.startOffset());
                int end = source.offsetByCodePoints(0, span.endOffset());
                assertThat(source.substring(start, end)).isEqualTo(token.text());
            }
        }
    }

    private static void assertPosition(String source, int targetOffset, int expectedLine, int expectedColumn)
    {
        Position position = positionAt(source, targetOffset);
        assertThat(expectedLine).isEqualTo(position.line());
        assertThat(expectedColumn).isEqualTo(position.column());
    }

    private static Position positionAt(String source, int targetOffset)
    {
        int line = 1;
        int column = 1;
        int codePointOffset = 0;
        boolean previousWasCarriageReturn = false;
        for (int charOffset = 0; codePointOffset < targetOffset; codePointOffset++) {
            int codePoint = source.codePointAt(charOffset);
            charOffset += Character.charCount(codePoint);
            if (codePoint == '\n' && previousWasCarriageReturn) {
                previousWasCarriageReturn = false;
            }
            else if (codePoint == '\n' || codePoint == '\r') {
                line++;
                column = 1;
                previousWasCarriageReturn = codePoint == '\r';
            }
            else {
                column++;
                previousWasCarriageReturn = false;
            }
        }
        return new Position(line, column);
    }

    private static List<MalformedRegression> loadMalformedRegressions()
            throws IOException
    {
        try (InputStream input = TestHogQlParserRobustness.class.getResourceAsStream(MALFORMED_RESOURCE)) {
            if (input == null) {
                fail("missing test resource: " + MALFORMED_RESOURCE);
            }
            return Arrays.stream(new String(input.readAllBytes(), StandardCharsets.UTF_8).split("\\R"))
                    .filter(line -> !line.isBlank())
                    .map(MALFORMED_REGRESSION_CODEC::fromJson)
                    .toList();
        }
    }

    private static String display(String source)
    {
        return source.replace("\r", "\\r").replace("\n", "\\n");
    }

    private record GeneratedInput(String id, String source) {}

    private record PendingElement(Element element, Optional<SourceSpan> parentSpan) {}

    private record Position(int line, int column) {}

    public record MalformedRegression(String id, String source)
    {
        @JsonCreator
        public MalformedRegression(
                @JsonProperty("id") String id,
                @JsonProperty("source") String source)
        {
            this.id = requireNonNull(id, "id is null");
            this.source = requireNonNull(source, "source is null");
        }
    }
}
