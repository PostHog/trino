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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Character.MAX_CODE_POINT;

/**
 * An immutable set of code points, held as sorted, disjoint, non-adjacent inclusive ranges.
 */
final class CodePointSet
{
    static final CodePointSet EMPTY = new CodePointSet(new int[0]);
    static final CodePointSet ALL = of(0, MAX_CODE_POINT);

    // start and end (inclusive) of each range, in order
    private final int[] ranges;

    private CodePointSet(int[] ranges)
    {
        this.ranges = ranges;
    }

    static CodePointSet of(int start, int end)
    {
        return new CodePointSet(new int[] {start, end});
    }

    static CodePointSet of(int codePoint)
    {
        return of(codePoint, codePoint);
    }

    static CodePointSet ofRanges(int... ranges)
    {
        Builder builder = new Builder();
        for (int i = 0; i < ranges.length; i += 2) {
            builder.add(ranges[i], ranges[i + 1]);
        }
        return builder.build();
    }

    int rangeCount()
    {
        return ranges.length / 2;
    }

    boolean isEmpty()
    {
        return ranges.length == 0;
    }

    boolean contains(int codePoint)
    {
        int low = 0;
        int high = ranges.length / 2 - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (codePoint < ranges[middle * 2]) {
                high = middle - 1;
            }
            else if (codePoint > ranges[middle * 2 + 1]) {
                low = middle + 1;
            }
            else {
                return true;
            }
        }
        return false;
    }

    /**
     * The single code point in this set, or -1 when the set does not hold exactly one.
     */
    int singleCodePoint()
    {
        if (ranges.length == 2 && ranges[0] == ranges[1]) {
            return ranges[0];
        }
        return -1;
    }

    CodePointSet union(CodePointSet other)
    {
        Builder builder = new Builder();
        builder.add(this);
        builder.add(other);
        return builder.build();
    }

    CodePointSet complement()
    {
        Builder builder = new Builder();
        int next = 0;
        for (int i = 0; i < ranges.length; i += 2) {
            if (ranges[i] > next) {
                builder.add(next, ranges[i] - 1);
            }
            next = ranges[i + 1] + 1;
        }
        if (next <= MAX_CODE_POINT) {
            builder.add(next, MAX_CODE_POINT);
        }
        return builder.build();
    }

    /**
     * Whether this set holds a code point that has another case.
     */
    boolean containsCasedCodePoint()
    {
        for (int codePoint : CaseFolding.CASED_CODE_POINTS) {
            if (contains(codePoint)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This set plus every other case of each code point in it.
     */
    CodePointSet caseClosure()
    {
        Builder builder = new Builder();
        builder.add(this);
        for (int codePoint : CaseFolding.CASED_CODE_POINTS) {
            if (contains(codePoint)) {
                for (int variant : CaseFolding.variants(codePoint)) {
                    builder.add(variant, variant);
                }
            }
        }
        return builder.build();
    }

    /**
     * This set plus the other case of each ASCII letter in it.
     */
    CodePointSet asciiCaseClosure()
    {
        Builder builder = new Builder();
        builder.add(this);
        for (int letter = 'a'; letter <= 'z'; letter++) {
            int upper = letter - 'a' + 'A';
            if (contains(letter) || contains(upper)) {
                builder.add(letter, letter);
                builder.add(upper, upper);
            }
        }
        return builder.build();
    }

    /**
     * The members of this set as the body of a Java character class, without the brackets.
     */
    String toJavaClassBody()
    {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < ranges.length; i += 2) {
            appendCodePoint(body, ranges[i]);
            if (ranges[i + 1] != ranges[i]) {
                body.append('-');
                appendCodePoint(body, ranges[i + 1]);
            }
        }
        return body.toString();
    }

    static void appendCodePoint(StringBuilder builder, int codePoint)
    {
        builder.append("\\x{").append(Integer.toHexString(codePoint)).append('}');
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof CodePointSet set && Arrays.equals(ranges, set.ranges);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(ranges);
    }

    @Override
    public String toString()
    {
        return "[" + toJavaClassBody() + "]";
    }

    static final class Builder
    {
        private final List<int[]> ranges = new ArrayList<>();

        Builder add(int start, int end)
        {
            ranges.add(new int[] {start, end});
            return this;
        }

        Builder add(CodePointSet set)
        {
            for (int i = 0; i < set.ranges.length; i += 2) {
                add(set.ranges[i], set.ranges[i + 1]);
            }
            return this;
        }

        CodePointSet build()
        {
            if (ranges.isEmpty()) {
                return EMPTY;
            }
            ranges.sort((left, right) -> Integer.compare(left[0], right[0]));
            int[] merged = new int[ranges.size() * 2];
            int count = 0;
            for (int[] range : ranges) {
                if (count > 0 && range[0] <= merged[count - 1] + 1) {
                    merged[count - 1] = Math.max(merged[count - 1], range[1]);
                }
                else {
                    merged[count++] = range[0];
                    merged[count++] = range[1];
                }
            }
            return new CodePointSet(Arrays.copyOf(merged, count));
        }
    }

    /**
     * Case-insensitive equivalence: two code points are equivalent when they fold to the same
     * code point. Folding is Java's simple upper-then-lower mapping, except that the Turkish
     * dotted capital I and dotless small i fold only to themselves, as in Unicode simple case
     * folding and in Hyperscan.
     */
    private static final class CaseFolding
    {
        static final int[] CASED_CODE_POINTS;
        private static final Map<Integer, int[]> VARIANTS;

        static {
            Map<Integer, List<Integer>> byFold = new HashMap<>();
            for (int codePoint = 0; codePoint <= MAX_CODE_POINT; codePoint++) {
                int fold = fold(codePoint);
                if (fold != codePoint || Character.toUpperCase(codePoint) != codePoint || Character.toTitleCase(codePoint) != codePoint) {
                    byFold.computeIfAbsent(fold, _ -> new ArrayList<>()).add(codePoint);
                }
            }
            Map<Integer, int[]> variants = new HashMap<>();
            for (Map.Entry<Integer, List<Integer>> entry : byFold.entrySet()) {
                List<Integer> members = new ArrayList<>(entry.getValue());
                if (!members.contains(entry.getKey())) {
                    members.add(entry.getKey());
                }
                if (members.size() < 2) {
                    continue;
                }
                int[] group = members.stream().mapToInt(Integer::intValue).sorted().toArray();
                for (int member : group) {
                    variants.put(member, group);
                }
            }
            VARIANTS = variants;
            CASED_CODE_POINTS = variants.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        }

        private CaseFolding() {}

        static int[] variants(int codePoint)
        {
            return VARIANTS.getOrDefault(codePoint, new int[] {codePoint});
        }

        private static int fold(int codePoint)
        {
            if (codePoint == 0x130 || codePoint == 0x131) {
                return codePoint;
            }
            int fold = Character.toLowerCase(Character.toUpperCase(codePoint));
            if (fold == 0x130 || fold == 0x131) {
                return codePoint;
            }
            return fold;
        }
    }
}
