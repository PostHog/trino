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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Translates a pattern in the regular expression dialect of Hyperscan (Vectorscan), as
 * ClickHouse compiles it for {@code multiMatchAnyIndex}, into Java regular expressions.
 * <p>
 * ClickHouse compiles every pattern with {@code HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH |
 * HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8}: {@code .} matches a newline, the pattern and the
 * haystack are UTF-8, and {@code .} and character classes match one code point. Without
 * {@code HS_FLAG_UCP}, {@code \d}, {@code \w}, {@code \s}, {@code \b} and the POSIX classes
 * are ASCII only. Case-insensitive matching ({@code (?i)}) folds Unicode letters.
 * <p>
 * The pattern is parsed here rather than handed to {@link Pattern}, because the two dialects
 * read some syntax differently: octal escapes such as {@code \141}, a literal brace as in
 * {@code a{,2}}, {@code [[} and {@code &&} in a class, {@code (?U)}, and case folding. Every
 * literal is written out as {@code \x{...}} and every class as explicit ranges, so Java
 * applies no syntax or flags of its own. Patterns that Hyperscan rejects (back-references,
 * lookaround, atomic groups, possessive quantifiers, embedded anchors, invalid syntax) are
 * rejected with the same message ClickHouse reports. Constructs whose Hyperscan behaviour
 * Java cannot reproduce exactly are rejected with {@code NOT_SUPPORTED}.
 */
final class HyperscanPatternTranslator
{
    private static final int UNBOUNDED = -1;
    // Hyperscan rejects larger repeat counts with "Number is too big"
    private static final int MAX_REPEAT = 65535;

    private static final CodePointSet DIGIT = CodePointSet.of('0', '9');
    private static final CodePointSet WORD = CodePointSet.ofRanges('0', '9', 'A', 'Z', '_', '_', 'a', 'z');
    private static final CodePointSet SPACE = CodePointSet.ofRanges('\t', '\r', ' ', ' ');
    private static final CodePointSet HORIZONTAL_SPACE = CodePointSet.ofRanges(
            '\t', '\t', ' ', ' ', 0xA0, 0xA0, 0x1680, 0x1680, 0x180E, 0x180E, 0x2000, 0x200A, 0x202F, 0x202F, 0x205F, 0x205F, 0x3000, 0x3000);
    private static final CodePointSet VERTICAL_SPACE = CodePointSet.ofRanges('\n', '\r', 0x85, 0x85, 0x2028, 0x2029);
    private static final CodePointSet NOT_NEWLINE = CodePointSet.of('\n').complement();

    private static final Set<String> GENERAL_CATEGORIES = Set.of(
            "C",
            "Cc",
            "Cf",
            "Cn",
            "Co",
            "Cs",
            "L",
            "Ll",
            "Lm",
            "Lo",
            "Lt",
            "Lu",
            "M",
            "Mc",
            "Me",
            "Mn",
            "N",
            "Nd",
            "Nl",
            "No",
            "P",
            "Pc",
            "Pd",
            "Pe",
            "Pf",
            "Pi",
            "Po",
            "Ps",
            "S",
            "Sc",
            "Sk",
            "Sm",
            "So",
            "Z",
            "Zl",
            "Zp",
            "Zs");
    private static final Pattern SCRIPT_NAME = Pattern.compile("[A-Z][a-z]*(?:_[A-Z][a-z]*)*");

    // ClickHouse's SlowWithHyperscanChecker, which uses RE2, where \s is [\t\n\f\r ]
    private static final Pattern ONE_REPEAT = Pattern.compile("\\{[\\t\\n\\f\\r ]*([0-9]+)[\\t\\n\\f\\r ]*,?[\\t\\n\\f\\r ]*}");
    private static final Pattern TWO_REPEATS = Pattern.compile("\\{[\\t\\n\\f\\r ]*([0-9]+)[\\t\\n\\f\\r ]*,[\\t\\n\\f\\r ]*([0-9]+)[\\t\\n\\f\\r ]*}");

    private HyperscanPatternTranslator() {}

    /**
     * One branch of a translated pattern: a byte literal, or Java patterns for the two phases in
     * which Hyperscan reports a match (see {@link ClickHouseMultiMatch}). A pattern's earliest
     * match is the earliest match of any of its top-level alternatives, so each is a branch.
     *
     * @param literal the UTF-8 bytes of a case-sensitive literal, or null
     * @param scan the pattern for matches confirmed while scanning: end anchors never match, and
     *         a word boundary or multi-line end anchor must see the code point after it
     * @param scanFollowed {@code scan} followed by one more code point of lookahead
     * @param endOfData the pattern with the usual meaning of every anchor
     * @param requiredLiteral UTF-8 bytes that every match contains, or null; a haystack without
     *         them cannot match, so the regular expression need not run
     * @param anchoredAtStart every match starts at the start of the haystack
     * @param endOfDataOnly every match ends with an end anchor, so is never confirmed while scanning
     * @param maxLength the most chars a match spans, or -1 if unbounded
     */
    record Branch(
            Slice literal,
            Pattern scan,
            Pattern scanFollowed,
            Pattern endOfData,
            Slice requiredLiteral,
            boolean anchoredAtStart,
            boolean endOfDataOnly,
            int maxLength)
    {
        boolean isLiteral()
        {
            return literal != null;
        }
    }

    /**
     * ClickHouse's {@code reject_expensive_hyperscan_regexps} check, which is on by default:
     * any {@code {n}}, {@code {n,}} or {@code {n,m}} with a number above 50 in the pattern text,
     * escaped or not, is rejected.
     */
    static boolean isSlow(String pattern)
    {
        Matcher one = ONE_REPEAT.matcher(pattern);
        while (one.find()) {
            if (isLargerThanFifty(one.group(1))) {
                return true;
            }
        }
        Matcher two = TWO_REPEATS.matcher(pattern);
        while (two.find()) {
            if (isLargerThanFifty(two.group(1)) || isLargerThanFifty(two.group(2))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLargerThanFifty(String digits)
    {
        // ClickHouse reads the number with std::from_chars into an int and treats overflow as not slow
        try {
            return Integer.parseInt(digits) > 50;
        }
        catch (NumberFormatException _) {
            return false;
        }
    }

    static List<Branch> translate(Slice pattern)
    {
        // ClickHouse passes each pattern to Hyperscan as a C string, so it ends at the first NUL
        int length = pattern.indexOfByte(0);
        Slice expression = length < 0 ? pattern : pattern.slice(0, length);
        if (!SliceUtf8.isUtf8(expression)) {
            throw invalid(pattern.toStringUtf8(), "Expression is not valid UTF-8.");
        }
        String text = expression.toStringUtf8();
        Node root = new Parser(text).parse();
        checkAnchors(text, root);
        return topLevelBranches(root).stream()
                .map(HyperscanPatternTranslator::branch)
                .toList();
    }

    private static List<Node> topLevelBranches(Node node)
    {
        if (node instanceof Sequence sequence && sequence.items().size() == 1) {
            return topLevelBranches(sequence.items().getFirst());
        }
        if (node instanceof Alternation alternation) {
            List<Node> branches = new ArrayList<>();
            alternation.branches().forEach(branch -> branches.addAll(topLevelBranches(branch)));
            return branches;
        }
        return List.of(node);
    }

    private static Branch branch(Node node)
    {
        List<Integer> literal = literalCodePoints(node);
        if (literal != null) {
            return new Branch(utf8(literal), null, null, null, null, false, false, -1);
        }
        String scan = render(node, true);
        List<Integer> required = requiredLiteral(node);
        Shape shape = shape(node);
        return new Branch(
                null,
                Pattern.compile(scan, Pattern.UNIX_LINES),
                Pattern.compile("(?:" + scan + ")(?=(?s:.))", Pattern.UNIX_LINES),
                Pattern.compile(render(node, false), Pattern.UNIX_LINES),
                required.isEmpty() ? null : utf8(required),
                !shape.empty() && shape.first() == Token.START_ANCHOR.bit(),
                !shape.empty() && shape.last() == Token.END_ANCHOR.bit(),
                maxLength(node));
    }

    private static Slice utf8(List<Integer> codePoints)
    {
        StringBuilder builder = new StringBuilder();
        codePoints.forEach(builder::appendCodePoint);
        return Slices.utf8Slice(builder.toString());
    }

    // the most chars a match of the node spans, or -1 if unbounded
    private static int maxLength(Node node)
    {
        long length = switch (node) {
            case Literal literal -> Character.charCount(literal.codePoint());
            case CharacterClass _ -> 2;
            case Anchor _ -> 0;
            case Sequence sequence -> {
                long sum = 0;
                for (Node item : sequence.items()) {
                    int itemLength = maxLength(item);
                    if (itemLength < 0) {
                        yield -1;
                    }
                    sum += itemLength;
                }
                yield sum;
            }
            case Alternation alternation -> {
                long max = 0;
                for (Node branch : alternation.branches()) {
                    int branchLength = maxLength(branch);
                    if (branchLength < 0) {
                        yield -1;
                    }
                    max = Math.max(max, branchLength);
                }
                yield max;
            }
            case Repeat repeat -> {
                int body = maxLength(repeat.body());
                if (body < 0 || repeat.max() == UNBOUNDED) {
                    yield -1;
                }
                yield (long) body * repeat.max();
            }
        };
        if (length > 100_000) {
            return -1;
        }
        return (int) length;
    }

    /**
     * The longest run of case-sensitive code points that every match of the node contains,
     * possibly empty: a run of literals in a sequence, or one inside a group or a repeat that
     * must match at least once.
     */
    private static List<Integer> requiredLiteral(Node node)
    {
        return switch (node) {
            case Literal literal -> List.of(literal.codePoint());
            case Sequence sequence -> {
                List<Integer> best = List.of();
                List<Integer> run = new ArrayList<>();
                for (Node item : sequence.items()) {
                    if (item instanceof Literal literal) {
                        run.add(literal.codePoint());
                        continue;
                    }
                    if (item instanceof Anchor) {
                        // zero width, so the literals on both sides are adjacent
                        continue;
                    }
                    best = longer(best, run);
                    run = new ArrayList<>();
                    best = longer(best, requiredLiteral(item));
                }
                yield longer(best, run);
            }
            case Repeat repeat -> repeat.min() > 0 ? requiredLiteral(repeat.body()) : List.of();
            case CharacterClass _, Anchor _, Alternation _ -> List.of();
        };
    }

    private static List<Integer> longer(List<Integer> left, List<Integer> right)
    {
        return right.size() > left.size() ? List.copyOf(right) : left;
    }

    private static TrinoException invalid(String pattern, String error)
    {
        return new TrinoException(INVALID_FUNCTION_ARGUMENT, "Pattern '%s' failed with error '%s'".formatted(pattern, error));
    }

    private static TrinoException notSupported(String pattern, String feature)
    {
        return new TrinoException(NOT_SUPPORTED, "Pattern '%s' uses %s, which ch_multimatchanyindex does not support because Java cannot reproduce ClickHouse's result exactly".formatted(pattern, feature));
    }

    private sealed interface Node
            permits Alternation,
                    Anchor,
                    CharacterClass,
                    Literal,
                    Repeat,
                    Sequence {}

    // one case-sensitive code point
    private record Literal(int codePoint)
            implements Node {}

    // one code point in codePoints or in one of the Java properties, or not in any when negated
    private record CharacterClass(CodePointSet codePoints, List<String> properties, boolean negated)
            implements Node
    {
        CharacterClass
        {
            requireNonNull(codePoints, "codePoints is null");
            properties = List.copyOf(properties);
        }
    }

    private record Anchor(AnchorKind kind)
            implements Node {}

    private record Sequence(List<Node> items)
            implements Node
    {
        Sequence
        {
            items = List.copyOf(items);
        }
    }

    private record Alternation(List<Node> branches)
            implements Node
    {
        Alternation
        {
            branches = List.copyOf(branches);
        }
    }

    private record Repeat(Node body, int min, int max, boolean lazy)
            implements Node {}

    private enum AnchorKind
    {
        START,
        START_OF_LINE,
        END,
        END_OF_LINE,
        END_OF_INPUT,
        WORD_BOUNDARY,
        NOT_WORD_BOUNDARY;

        Token token()
        {
            return switch (this) {
                case START -> Token.START_ANCHOR;
                case START_OF_LINE -> Token.LINE_START_ANCHOR;
                case END, END_OF_INPUT -> Token.END_ANCHOR;
                case END_OF_LINE -> Token.LINE_END_ANCHOR;
                case WORD_BOUNDARY, NOT_WORD_BOUNDARY -> Token.WORD_BOUNDARY;
            };
        }
    }

    private enum Token
    {
        CONSUMING(0),
        WORD_BOUNDARY(1),
        START_ANCHOR(2),
        LINE_START_ANCHOR(3),
        END_ANCHOR(4),
        LINE_END_ANCHOR(5);

        private static final int COUNT = 6;

        private final int id;

        Token(int id)
        {
            this.id = id;
        }

        int bit()
        {
            return 1 << id;
        }

        long pairBit(Token next)
        {
            return 1L << (id * COUNT + next.id);
        }
    }

    /**
     * Which tokens can begin and end a match of a node, and which token pairs can follow each other.
     */
    private record Shape(boolean empty, int first, int last, long follow)
    {
        static final Shape EMPTY = new Shape(true, 0, 0, 0);

        static Shape of(Token token)
        {
            return new Shape(false, token.bit(), token.bit(), 0);
        }

        Shape then(Shape next)
        {
            return new Shape(
                    empty && next.empty,
                    first | (empty ? next.first : 0),
                    next.last | (next.empty ? last : 0),
                    follow | next.follow | pairs(last, next.first));
        }

        Shape or(Shape other)
        {
            return new Shape(empty || other.empty, first | other.first, last | other.last, follow | other.follow);
        }

        boolean follows(Token token, Token next)
        {
            return (follow & token.pairBit(next)) != 0;
        }

        private static long pairs(int tokens, int nextTokens)
        {
            long pairs = 0;
            for (Token token : Token.values()) {
                for (Token next : Token.values()) {
                    if ((tokens & token.bit()) != 0 && (nextTokens & next.bit()) != 0) {
                        pairs |= token.pairBit(next);
                    }
                }
            }
            return pairs;
        }
    }

    /**
     * Hyperscan requires start anchors to come first and end anchors to come last. Only a start
     * anchor may come before a start anchor ({@code ^^a} and {@code \\A(?m)^a} are accepted,
     * {@code (?m)^^a} and {@code \\b^a} are not), and only an end anchor after an end anchor.
     */
    private static void checkAnchors(String pattern, Node root)
    {
        Shape shape = shape(root);
        for (Token previous : List.of(Token.CONSUMING, Token.WORD_BOUNDARY, Token.LINE_START_ANCHOR)) {
            if (shape.follows(previous, Token.START_ANCHOR) || shape.follows(previous, Token.LINE_START_ANCHOR)) {
                throw invalid(pattern, "Embedded start anchors not supported.");
            }
        }
        for (Token end : List.of(Token.END_ANCHOR, Token.LINE_END_ANCHOR)) {
            for (Token next : List.of(Token.CONSUMING, Token.WORD_BOUNDARY, Token.START_ANCHOR, Token.LINE_START_ANCHOR)) {
                if (shape.follows(end, next)) {
                    throw invalid(pattern, "Embedded end anchors not supported.");
                }
            }
        }
    }

    private static Shape shape(Node node)
    {
        return switch (node) {
            case Literal _, CharacterClass _ -> Shape.of(Token.CONSUMING);
            case Anchor anchor -> Shape.of(anchor.kind().token());
            case Sequence sequence -> {
                Shape shape = Shape.EMPTY;
                for (Node item : sequence.items()) {
                    shape = shape.then(shape(item));
                }
                yield shape;
            }
            case Alternation alternation -> {
                Shape shape = shape(alternation.branches().getFirst());
                for (Node branch : alternation.branches()) {
                    shape = shape.or(shape(branch));
                }
                yield shape;
            }
            case Repeat repeat -> {
                Shape body = shape(repeat.body());
                long follow = body.follow();
                if (repeat.max() != 1) {
                    follow |= Shape.pairs(body.last(), body.first());
                }
                yield new Shape(body.empty() || repeat.min() == 0, body.first(), body.last(), follow);
            }
        };
    }

    private static List<Integer> literalCodePoints(Node node)
    {
        return switch (node) {
            case Literal literal -> List.of(literal.codePoint());
            case CharacterClass characterClass -> {
                int codePoint = characterClass.codePoints().singleCodePoint();
                if (characterClass.negated() || !characterClass.properties().isEmpty() || codePoint < 0) {
                    yield null;
                }
                yield List.of(codePoint);
            }
            case Sequence sequence -> {
                List<Integer> codePoints = new ArrayList<>();
                for (Node item : sequence.items()) {
                    List<Integer> itemCodePoints = literalCodePoints(item);
                    if (itemCodePoints == null) {
                        yield null;
                    }
                    codePoints.addAll(itemCodePoints);
                }
                yield codePoints;
            }
            case Anchor _, Alternation _, Repeat _ -> null;
        };
    }

    private static String render(Node node, boolean scan)
    {
        StringBuilder builder = new StringBuilder();
        render(node, scan, builder);
        return builder.toString();
    }

    private static void render(Node node, boolean scan, StringBuilder builder)
    {
        switch (node) {
            case Literal literal -> CodePointSet.appendCodePoint(builder, literal.codePoint());
            case CharacterClass characterClass -> renderClass(characterClass, builder);
            case Anchor anchor -> builder.append(renderAnchor(anchor.kind(), scan));
            case Sequence sequence -> sequence.items().forEach(item -> render(item, scan, builder));
            case Alternation alternation -> {
                builder.append("(?:");
                for (int i = 0; i < alternation.branches().size(); i++) {
                    if (i > 0) {
                        builder.append('|');
                    }
                    render(alternation.branches().get(i), scan, builder);
                }
                builder.append(')');
            }
            case Repeat repeat -> {
                builder.append("(?:");
                render(repeat.body(), scan, builder);
                builder.append("){").append(repeat.min());
                if (repeat.max() != repeat.min()) {
                    builder.append(',');
                    if (repeat.max() != UNBOUNDED) {
                        builder.append(repeat.max());
                    }
                }
                builder.append('}');
                if (repeat.lazy()) {
                    builder.append('?');
                }
            }
        }
    }

    private static void renderClass(CharacterClass characterClass, StringBuilder builder)
    {
        CodePointSet codePoints = characterClass.codePoints();
        if (characterClass.properties().isEmpty()) {
            if (characterClass.negated()) {
                codePoints = codePoints.complement();
            }
            int single = codePoints.singleCodePoint();
            if (single >= 0) {
                CodePointSet.appendCodePoint(builder, single);
                return;
            }
            if (codePoints.isEmpty()) {
                builder.append("(?!)");
                return;
            }
            builder.append('[').append(codePoints.toJavaClassBody()).append(']');
            return;
        }
        builder.append('[');
        if (characterClass.negated()) {
            builder.append('^');
        }
        builder.append(codePoints.toJavaClassBody());
        characterClass.properties().forEach(builder::append);
        builder.append(']');
    }

    // In the scan phase an anchor must be decided by the code points read so far, see ClickHouseMultiMatch
    private static String renderAnchor(AnchorKind kind, boolean scan)
    {
        return switch (kind) {
            case START -> "\\A";
            // Java's multi-line ^ never matches at the end of the input, so not in an empty one either
            case START_OF_LINE -> "(?:\\A|(?m:^))";
            case END -> scan ? "(?!)" : "(?-m:$)";
            case END_OF_INPUT -> scan ? "(?!)" : "\\z";
            case END_OF_LINE -> scan ? "(?=\\x{a})" : "(?m:$)";
            case WORD_BOUNDARY -> scan ? "\\b(?=(?s:.))" : "\\b";
            case NOT_WORD_BOUNDARY -> scan ? "\\B(?=(?s:.))" : "\\B";
        };
    }

    private static final class Flags
    {
        private boolean caseless;
        private boolean multiline;
        private boolean dotAll;
        private boolean extended;

        Flags copy()
        {
            Flags copy = new Flags();
            copy.caseless = caseless;
            copy.multiline = multiline;
            copy.dotAll = dotAll;
            copy.extended = extended;
            return copy;
        }

        void set(int letter, boolean value)
        {
            switch (letter) {
                case 'i' -> caseless = value;
                case 'm' -> multiline = value;
                case 's' -> dotAll = value;
                case 'x' -> extended = value;
                default -> throw new IllegalArgumentException("Unknown flag: " + Character.toString(letter));
            }
        }
    }

    // A parsed property: code points plus Java property escapes, negated as a whole when negated is set
    private record Property(CodePointSet codePoints, List<String> javaProperties, boolean negated) {}

    private record Quantifier(int min, int max, boolean lazy) {}

    private static final class Parser
    {
        private final String source;
        private final int[] pattern;
        private int position;
        private int captureCount;
        private final Set<String> groupNames = new HashSet<>();

        Parser(String source)
        {
            this.source = source;
            this.pattern = source.codePoints().toArray();
        }

        Node parse()
        {
            Flags flags = new Flags();
            flags.dotAll = true;
            return parseAlternation(flags, false);
        }

        private TrinoException invalid(String error)
        {
            return HyperscanPatternTranslator.invalid(source, error);
        }

        private TrinoException notSupported(String feature)
        {
            return HyperscanPatternTranslator.notSupported(source, feature);
        }

        private boolean atEnd()
        {
            return position >= pattern.length;
        }

        private int peek(int offset)
        {
            int index = position + offset;
            if (index < pattern.length) {
                return pattern[index];
            }
            return -1;
        }

        private Node parseAlternation(Flags outerFlags, boolean inGroup)
        {
            // an inline flag such as (?i) lasts to the end of the group, across later branches
            Flags flags = outerFlags.copy();
            List<Node> branches = new ArrayList<>();
            List<Node> items = new ArrayList<>();
            boolean quantifiable = false;
            while (true) {
                if (flags.extended) {
                    skipExtendedWhitespace();
                }
                if (atEnd()) {
                    if (inGroup) {
                        throw invalid("Missing close parenthesis for group.");
                    }
                    break;
                }
                int character = pattern[position];
                if (character == '|') {
                    position++;
                    branches.add(new Sequence(items));
                    items = new ArrayList<>();
                    quantifiable = false;
                    continue;
                }
                if (character == ')') {
                    if (!inGroup) {
                        throw invalid("Unmatched parentheses.");
                    }
                    position++;
                    break;
                }
                Quantifier quantifier = tryParseQuantifier();
                if (quantifier != null) {
                    if (!quantifiable) {
                        throw invalid("Invalid repeat.");
                    }
                    items.add(new Repeat(items.removeLast(), quantifier.min(), quantifier.max(), quantifier.lazy()));
                    quantifiable = false;
                    continue;
                }
                quantifiable = parseAtom(flags, items);
            }
            branches.add(new Sequence(items));
            if (branches.size() == 1) {
                return branches.getFirst();
            }
            return new Alternation(branches);
        }

        private void skipExtendedWhitespace()
        {
            while (!atEnd()) {
                int character = pattern[position];
                if (character == ' ' || (character >= '\t' && character <= '\r')) {
                    position++;
                }
                else if (character == '#') {
                    while (!atEnd() && pattern[position] != '\n') {
                        position++;
                    }
                    if (atEnd()) {
                        throw invalid("Unterminated comment.");
                    }
                    position++;
                }
                else {
                    return;
                }
            }
        }

        private Quantifier tryParseQuantifier()
        {
            int character = pattern[position];
            int min;
            int max;
            switch (character) {
                case '*' -> {
                    min = 0;
                    max = UNBOUNDED;
                    position++;
                }
                case '+' -> {
                    min = 1;
                    max = UNBOUNDED;
                    position++;
                }
                case '?' -> {
                    min = 0;
                    max = 1;
                    position++;
                }
                case '{' -> {
                    // only {n}, {n,} and {n,m} are quantifiers, anything else is a literal brace
                    int end = position + 1;
                    int minStart = end;
                    while (end < pattern.length && isDigit(pattern[end])) {
                        end++;
                    }
                    if (end == minStart || end == pattern.length) {
                        return null;
                    }
                    int minEnd = end;
                    int maxStart = -1;
                    int maxEnd = -1;
                    if (pattern[end] == ',') {
                        end++;
                        maxStart = end;
                        while (end < pattern.length && isDigit(pattern[end])) {
                            end++;
                        }
                        maxEnd = end;
                    }
                    if (end == pattern.length || pattern[end] != '}') {
                        return null;
                    }
                    min = parseRepeatCount(minStart, minEnd);
                    if (maxStart < 0) {
                        max = min;
                    }
                    else if (maxStart == maxEnd) {
                        max = UNBOUNDED;
                    }
                    else {
                        max = parseRepeatCount(maxStart, maxEnd);
                    }
                    if (max == 0 || (max != UNBOUNDED && max < min)) {
                        throw invalid("Invalid repeat.");
                    }
                    position = end + 1;
                }
                default -> {
                    return null;
                }
            }
            boolean lazy = false;
            if (peek(0) == '?') {
                lazy = true;
                position++;
            }
            else if (peek(0) == '+') {
                throw invalid("Possessive quantifiers are not supported.");
            }
            return new Quantifier(min, max, lazy);
        }

        private int parseRepeatCount(int start, int end)
        {
            long value = 0;
            for (int i = start; i < end; i++) {
                value = value * 10 + (pattern[i] - '0');
                if (value > MAX_REPEAT) {
                    throw invalid("Number is too big.");
                }
            }
            return (int) value;
        }

        // Parses one atom into items and returns whether a quantifier may follow it
        private boolean parseAtom(Flags flags, List<Node> items)
        {
            int character = pattern[position++];
            return switch (character) {
                case '(' -> parseGroup(flags, items);
                case '[' -> {
                    items.add(parseClass(flags));
                    yield true;
                }
                case '.' -> {
                    items.add(new CharacterClass(flags.dotAll ? CodePointSet.ALL : NOT_NEWLINE, List.of(), false));
                    yield true;
                }
                case '^' -> {
                    items.add(new Anchor(flags.multiline ? AnchorKind.START_OF_LINE : AnchorKind.START));
                    yield false;
                }
                case '$' -> {
                    items.add(new Anchor(flags.multiline ? AnchorKind.END_OF_LINE : AnchorKind.END));
                    yield false;
                }
                case '\\' -> parseEscape(flags, items);
                default -> {
                    items.add(character(character, flags));
                    yield true;
                }
            };
        }

        private Node character(int codePoint, Flags flags)
        {
            if (!flags.caseless) {
                return new Literal(codePoint);
            }
            CodePointSet variants = CodePointSet.of(codePoint).caseClosure();
            if (variants.singleCodePoint() >= 0) {
                return new Literal(codePoint);
            }
            return new CharacterClass(variants, List.of(), false);
        }

        private boolean parseGroup(Flags flags, List<Node> items)
        {
            if (peek(0) != '?') {
                captureCount++;
                items.add(parseAlternation(flags, true));
                return true;
            }
            position++;
            int character = peek(0);
            switch (character) {
                case '#' -> {
                    while (!atEnd() && pattern[position] != ')') {
                        position++;
                    }
                    if (atEnd()) {
                        throw invalid("Missing ) after comment.");
                    }
                    position++;
                    return false;
                }
                case ':' -> {
                    position++;
                    items.add(parseAlternation(flags, true));
                    return true;
                }
                case '=', '!' -> throw invalid("Zero-width assertions are not supported.");
                case '<' -> {
                    if (peek(1) == '=' || peek(1) == '!') {
                        throw invalid("Zero-width assertions are not supported.");
                    }
                    position++;
                    return parseNamedGroup('>', flags, items);
                }
                case '\'' -> {
                    position++;
                    return parseNamedGroup('\'', flags, items);
                }
                case 'P' -> {
                    switch (peek(1)) {
                        case '<' -> {
                            position += 2;
                            return parseNamedGroup('>', flags, items);
                        }
                        case '=' -> throw invalid("Back-references are unsupported.");
                        case '>' -> throw invalid("Subpattern reference unsupported.");
                        default -> throw invalid("Unrecognised character after (?P.");
                    }
                }
                case '>' -> throw invalid("Atomic groups are unsupported.");
                case '(' -> throw invalid("Conditional subpatterns are unsupported.");
                default -> {
                    return parseFlagGroup(flags, items);
                }
            }
        }

        private boolean parseFlagGroup(Flags flags, List<Node> items)
        {
            List<Integer> enable = new ArrayList<>();
            List<Integer> disable = new ArrayList<>();
            boolean negate = false;
            while (!atEnd()) {
                int character = pattern[position];
                if (character == '-' && !negate) {
                    negate = true;
                }
                else if (character == 'i' || character == 'm' || character == 's' || character == 'x') {
                    (negate ? disable : enable).add(character);
                }
                else {
                    break;
                }
                position++;
            }
            int terminator = peek(0);
            if (terminator != ')' && terminator != ':') {
                throw invalid("Unrecognised character after (?.");
            }
            position++;
            Flags target = terminator == ')' ? flags : flags.copy();
            enable.forEach(letter -> target.set(letter, true));
            disable.forEach(letter -> target.set(letter, false));
            if (terminator == ')') {
                return false;
            }
            items.add(parseAlternation(target, true));
            return true;
        }

        private boolean parseNamedGroup(int terminator, Flags flags, List<Node> items)
        {
            int start = position;
            while (!atEnd() && pattern[position] != terminator) {
                int character = pattern[position];
                if (!(isDigit(character) || character == '_' || (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z'))) {
                    throw invalid("Unrecognised character after (?.");
                }
                position++;
            }
            if (atEnd() || position == start) {
                throw invalid("Unrecognised character after (?.");
            }
            if (isDigit(pattern[start])) {
                throw invalid("Group name cannot begin with a digit.");
            }
            String name = new String(pattern, start, position - start);
            if (!groupNames.add(name)) {
                throw invalid("Two named subpatterns use the name '%s'.".formatted(name));
            }
            position++;
            captureCount++;
            items.add(parseAlternation(flags, true));
            return true;
        }

        private boolean parseEscape(Flags flags, List<Node> items)
        {
            if (atEnd()) {
                throw invalid("Unescaped \\ at end of input.");
            }
            int character = pattern[position++];
            switch (character) {
                case 'A' -> items.add(new Anchor(AnchorKind.START));
                case 'z' -> items.add(new Anchor(AnchorKind.END_OF_INPUT));
                case 'Z' -> items.add(new Anchor(AnchorKind.END));
                case 'b' -> items.add(new Anchor(AnchorKind.WORD_BOUNDARY));
                case 'B' -> items.add(new Anchor(AnchorKind.NOT_WORD_BOUNDARY));
                case 'd', 'D', 'w', 'W', 's', 'S', 'h', 'H', 'v', 'V' -> {
                    CodePointSet codePoints = escapeClass(character);
                    boolean negated = Character.isUpperCase(character);
                    if (negated) {
                        checkExcluded(codePoints);
                        checkCaselessNegation(codePoints, flags);
                    }
                    items.add(new CharacterClass(codePoints, List.of(), negated));
                    return true;
                }
                case 'p', 'P' -> {
                    Property property = parseProperty(character == 'P');
                    if (property.negated() || property.javaProperties().stream().anyMatch(javaProperty -> javaProperty.startsWith("\\P"))) {
                        throw notSupported("a negated Unicode property");
                    }
                    items.add(new CharacterClass(property.codePoints(), property.javaProperties(), false));
                    return true;
                }
                case 'Q' -> {
                    boolean added = false;
                    while (!atEnd()) {
                        if (pattern[position] == '\\' && peek(1) == 'E') {
                            position += 2;
                            break;
                        }
                        items.add(character(pattern[position++], flags));
                        added = true;
                    }
                    return added;
                }
                case 'E' -> {
                    return false;
                }
                default -> {
                    items.add(character(escapedCharacter(character, false), flags));
                    return true;
                }
            }
            return false;
        }

        // the code points matched by \d \w \s \h \v, or by the negated forms before negation
        private static CodePointSet escapeClass(int character)
        {
            return switch (Character.toLowerCase(character)) {
                case 'd' -> DIGIT;
                case 'w' -> WORD;
                case 's' -> SPACE;
                case 'h' -> HORIZONTAL_SPACE;
                case 'v' -> VERTICAL_SPACE;
                default -> throw new IllegalArgumentException("Not a class escape: " + Character.toString(character));
            };
        }

        /**
         * Checks a class escape or POSIX class in case-insensitive mode. Hyperscan folds these
         * only within ASCII, so {@code (?i)[[:upper:]]} matches "k", but {@code (?i)^\\w$} and {@code (?i)^[[:alpha:]]$} do not match the
         * Kelvin sign, although {@code (?i)k} and {@code (?i)[a-z]} do. Hyperscan folds a negated
         * class inconsistently (in ClickHouse {@code (?i)^[^a]$} matches "a" but not "A", while
         * {@code (?i)^[^k]$} matches "K" but not "k"), so negating cased letters is rejected.
         */
        private void checkCaselessNegation(CodePointSet excluded, Flags flags)
        {
            if (flags.caseless && excluded.containsCasedCodePoint()) {
                throw notSupported("a negated class of cased letters in case-insensitive mode");
            }
        }

        /**
         * ClickHouse 26.10 (Vectorscan in UTF-8 mode) matches a negated class wrongly when the
         * excluded code points form more than one range: {@code ^\\W$} matches "a" and
         * {@code ^\\S$} matches " ", while ClickHouse 25.8 gets both right. A single excluded
         * range, as in {@code [^a]}, {@code [^a-c]}, {@code \\D} or {@code [^\\n]}, is right in both.
         */
        private void checkExcluded(CodePointSet excluded)
        {
            if (excluded.rangeCount() > 1) {
                throw notSupported("a negated class that excludes more than one range, which ClickHouse 26.10 matches incorrectly");
            }
        }

        private int escapedCharacter(int character, boolean inClass)
        {
            return switch (character) {
                case 'x' -> parseHex();
                case 'o' -> parseBracedOctal();
                case 'c' -> {
                    if (atEnd() || pattern[position] >= 0x80) {
                        throw invalid("\\c must be followed by an ASCII character.");
                    }
                    yield Character.toUpperCase(pattern[position++]) ^ 0x40;
                }
                case 'e' -> 0x1B;
                case 'a' -> 0x07;
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'G', 'K', 'R', 'X', 'N', 'C', 'L', 'l', 'U', 'u' -> throw invalid("\\%s not supported.".formatted(Character.toString(character)));
                case 'g', 'k' -> throw invalid("Back-references are unsupported.");
                case '8', '9' -> character;
                case '0', '1', '2', '3', '4', '5', '6', '7' -> parseOctalOrBackReference(character, inClass);
                default -> {
                    if (inClass && ((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z'))) {
                        throw notSupported("the escape \\" + Character.toString(character) + " in a character class");
                    }
                    // an unknown letter escape is the letter itself, and an escaped symbol is the symbol
                    yield character;
                }
            };
        }

        private int parseHex()
        {
            if (peek(0) == '{') {
                int end = position + 1;
                long value = 0;
                while (end < pattern.length && Character.digit(pattern[end], 16) >= 0) {
                    value = value * 16 + Character.digit(pattern[end], 16);
                    if (value > Character.MAX_CODE_POINT) {
                        throw invalid("Value in \\x{...} sequence is too large.");
                    }
                    end++;
                }
                if (end == position + 1 || end == pattern.length || pattern[end] != '}') {
                    throw notSupported("an incomplete \\x{...} escape");
                }
                position = end + 1;
                return (int) value;
            }
            int value = 0;
            for (int digits = 0; digits < 2 && !atEnd() && Character.digit(pattern[position], 16) >= 0 && pattern[position] < 0x80; digits++) {
                value = value * 16 + Character.digit(pattern[position++], 16);
            }
            return value;
        }

        private int parseBracedOctal()
        {
            if (peek(0) != '{') {
                throw notSupported("\\o without braces");
            }
            int end = position + 1;
            long value = 0;
            while (end < pattern.length && pattern[end] >= '0' && pattern[end] <= '7') {
                value = value * 8 + (pattern[end] - '0');
                if (value > Character.MAX_CODE_POINT) {
                    throw invalid("Value in \\o{...} sequence is too large.");
                }
                end++;
            }
            if (end == position + 1 || end == pattern.length || pattern[end] != '}') {
                throw notSupported("an incomplete \\o{...} escape");
            }
            position = end + 1;
            return (int) value;
        }

        /**
         * PCRE's rule: outside a class, a backslash and decimal number below 10, or no more
         * than the capturing groups opened so far, is a back-reference, which Hyperscan
         * rejects. Otherwise, and always in a class, up to three octal digits are a character.
         */
        private int parseOctalOrBackReference(int firstDigit, boolean inClass)
        {
            int start = position - 1;
            if (firstDigit != '0' && !inClass) {
                long number = 0;
                for (int i = start; i < pattern.length && isDigit(pattern[i]) && number < 1000; i++) {
                    number = number * 10 + (pattern[i] - '0');
                }
                if (number < 10 || number <= captureCount) {
                    throw invalid("Back-references are unsupported.");
                }
            }
            int value = firstDigit - '0';
            // \0 takes up to two more octal digits, \1 to \7 make up to three in total
            for (int digits = 1; digits < 3 && !atEnd() && pattern[position] >= '0' && pattern[position] <= '7'; digits++) {
                value = value * 8 + (pattern[position++] - '0');
            }
            return value;
        }

        private Property parseProperty(boolean negatedEscape)
        {
            String name;
            if (peek(0) == '{') {
                int end = position + 1;
                while (end < pattern.length && pattern[end] != '}') {
                    end++;
                }
                if (end == pattern.length) {
                    throw invalid("Malformed property.");
                }
                name = new String(pattern, position + 1, end - position - 1);
                position = end + 1;
            }
            else {
                if (atEnd() || !Character.isLetter(pattern[position])) {
                    throw invalid("Malformed property.");
                }
                name = Character.toString(pattern[position++]);
            }
            boolean negated = negatedEscape;
            if (name.startsWith("^")) {
                negated = !negated;
                name = name.substring(1);
            }
            String escape = negated ? "\\P" : "\\p";
            if (GENERAL_CATEGORIES.contains(name)) {
                return new Property(CodePointSet.EMPTY, List.of(escape + "{" + name + "}"), false);
            }
            return switch (name) {
                case "L&" -> new Property(CodePointSet.EMPTY, List.of(escape + "{LC}"), false);
                case "Any" -> new Property(negated ? CodePointSet.EMPTY : CodePointSet.ALL, List.of(), false);
                case "Xan" -> new Property(CodePointSet.EMPTY, List.of("\\p{L}", "\\p{N}"), negated);
                case "Xwd" -> new Property(CodePointSet.of('_'), List.of("\\p{L}", "\\p{N}"), negated);
                case "Xsp", "Xps" -> new Property(SPACE, List.of("\\p{Z}"), negated);
                default -> script(name, escape);
            };
        }

        // a script, by its full Unicode name as PCRE spells it, such as Latin or Old_Italic
        private Property script(String name, String escape)
        {
            if (SCRIPT_NAME.matcher(name).matches()) {
                try {
                    Character.UnicodeScript.valueOf(name.toUpperCase(Locale.ROOT));
                    return new Property(CodePointSet.EMPTY, List.of(escape + "{sc=" + name + "}"), false);
                }
                catch (IllegalArgumentException _) {
                    // not a script
                }
            }
            throw invalid("Unknown property.");
        }

        private CharacterClass parseClass(Flags flags)
        {
            boolean negated = false;
            if (peek(0) == '^') {
                negated = true;
                position++;
            }
            // characters and ranges, which case-insensitive mode folds
            CodePointSet.Builder members = new CodePointSet.Builder();
            // class escapes and POSIX classes, which it does not
            CodePointSet.Builder classes = new CodePointSet.Builder();
            List<String> properties = new ArrayList<>();
            boolean first = true;
            boolean complemented = negated;
            while (true) {
                if (atEnd()) {
                    throw invalid("Unterminated character class.");
                }
                int character = pattern[position];
                if (character == ']' && !first) {
                    position++;
                    break;
                }
                if (character == '[') {
                    if (peek(1) == ':') {
                        complemented |= peek(2) == '^';
                        classes.add(parsePosixClass(flags));
                        first = false;
                        continue;
                    }
                    if (peek(1) == '.' || peek(1) == '=') {
                        throw invalid("Unsupported POSIX collating element.");
                    }
                    throw invalid("Nested character classes are not supported.");
                }
                if (character == '&' && peek(1) == '&') {
                    throw invalid("Character class intersection/subtraction is not supported.");
                }
                int single;
                if (character == '\\') {
                    position++;
                    if (atEnd()) {
                        throw invalid("Unescaped \\ at end of input.");
                    }
                    int escape = pattern[position++];
                    switch (escape) {
                        case 'd', 'D', 'w', 'W', 's', 'S', 'h', 'H', 'v', 'V' -> {
                            boolean negatedEscape = Character.isUpperCase(escape);
                            CodePointSet codePoints = escapeClass(escape);
                            if (negatedEscape) {
                                checkCaselessNegation(codePoints, flags);
                                codePoints = codePoints.complement();
                                complemented = true;
                            }
                            classes.add(codePoints);
                            first = false;
                            continue;
                        }
                        case 'p', 'P' -> {
                            Property property = parseProperty(escape == 'P');
                            if (property.negated() || property.javaProperties().stream().anyMatch(javaProperty -> javaProperty.startsWith("\\P"))) {
                                throw notSupported("a negated Unicode property");
                            }
                            classes.add(property.codePoints());
                            properties.addAll(property.javaProperties());
                            first = false;
                            continue;
                        }
                        case 'Q' -> {
                            while (!atEnd()) {
                                if (pattern[position] == '\\' && peek(1) == 'E') {
                                    position += 2;
                                    break;
                                }
                                int quoted = pattern[position++];
                                members.add(quoted, quoted);
                                first = false;
                            }
                            continue;
                        }
                        case 'E' -> {
                            continue;
                        }
                        case 'b' -> single = '\b';
                        default -> single = escapedCharacter(escape, true);
                    }
                }
                else {
                    position++;
                    single = character;
                }
                first = false;
                if (peek(0) == '-' && peek(1) != ']' && peek(1) != -1) {
                    position++;
                    int end = parseRangeEnd();
                    if (end < single) {
                        throw invalid("Range out of order in character class.");
                    }
                    members.add(single, end);
                }
                else {
                    members.add(single, single);
                }
            }
            CodePointSet folded = members.build();
            if (flags.caseless) {
                folded = folded.caseClosure();
            }
            CodePointSet unfolded = classes.build();
            if (flags.caseless) {
                unfolded = unfolded.asciiCaseClosure();
            }
            CodePointSet codePoints = folded.union(unfolded);
            if (flags.caseless && negated && (codePoints.containsCasedCodePoint() || !properties.isEmpty())) {
                throw notSupported("a negated class of cased letters in case-insensitive mode");
            }
            if (complemented) {
                if (!properties.isEmpty()) {
                    throw notSupported("a negated class with a Unicode property");
                }
                checkExcluded(negated ? codePoints : codePoints.complement());
            }
            return new CharacterClass(codePoints, properties, negated);
        }

        private int parseRangeEnd()
        {
            int character = pattern[position++];
            if (character == '[') {
                throw invalid("Invalid range in character class.");
            }
            if (character != '\\') {
                return character;
            }
            if (atEnd()) {
                throw invalid("Unescaped \\ at end of input.");
            }
            int escape = pattern[position++];
            return switch (escape) {
                case 'd', 'D', 'w', 'W', 's', 'S', 'h', 'H', 'v', 'V', 'p', 'P' -> throw invalid("Invalid range in character class.");
                case 'Q', 'E' -> throw notSupported("\\Q or \\E at the end of a range");
                case 'b' -> '\b';
                default -> escapedCharacter(escape, true);
            };
        }

        private CodePointSet parsePosixClass(Flags flags)
        {
            // position is at "[:"
            int start = position + 2;
            int end = start;
            while (end + 1 < pattern.length && !(pattern[end] == ':' && pattern[end + 1] == ']')) {
                end++;
            }
            if (end + 1 >= pattern.length) {
                throw invalid("Nested character classes are not supported.");
            }
            String name = new String(pattern, start, end - start);
            position = end + 2;
            boolean negated = name.startsWith("^");
            if (negated) {
                name = name.substring(1);
            }
            CodePointSet codePoints = switch (name) {
                case "alpha" -> CodePointSet.ofRanges('A', 'Z', 'a', 'z');
                case "digit" -> DIGIT;
                case "alnum" -> CodePointSet.ofRanges('0', '9', 'A', 'Z', 'a', 'z');
                case "upper" -> CodePointSet.of('A', 'Z');
                case "lower" -> CodePointSet.of('a', 'z');
                case "space" -> SPACE;
                case "blank" -> CodePointSet.ofRanges('\t', '\t', ' ', ' ');
                case "cntrl" -> CodePointSet.ofRanges(0, 0x1F, 0x7F, 0x7F);
                case "graph" -> CodePointSet.of('!', '~');
                case "print" -> CodePointSet.of(' ', '~');
                case "punct" -> CodePointSet.ofRanges('!', '/', ':', '@', '[', '`', '{', '~');
                case "xdigit" -> CodePointSet.ofRanges('0', '9', 'A', 'F', 'a', 'f');
                case "word" -> WORD;
                case "ascii" -> CodePointSet.of(0, 0x7F);
                default -> throw invalid("Invalid POSIX named class.");
            };
            if (negated) {
                checkCaselessNegation(codePoints, flags);
                return codePoints.complement();
            }
            return codePoints;
        }

        private static boolean isDigit(int character)
        {
            return character >= '0' && character <= '9';
        }
    }
}
