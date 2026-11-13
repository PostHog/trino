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
import io.trino.plugin.posthog.functions.HyperscanPatternTranslator.Branch;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * A compiled pattern set that returns what ClickHouse's {@code multiMatchAnyIndex} returns.
 * <p>
 * ClickHouse scans the haystack with Hyperscan and returns the pattern of the first match
 * Hyperscan reports, then stops. Hyperscan reports each pattern once, at its earliest match,
 * in order of the time the match is confirmed, with ties going to the lower index. That time
 * is measured in bytes read:
 * <ul>
 * <li>A match ending at byte {@code e} is confirmed after reading {@code e} bytes. This is
 * the end of the shortest match, not of the leftmost greedy one, so {@code a+} on "aaa"
 * is confirmed at 1.</li>
 * <li>A match that ends with {@code \b}, {@code \B} or a multi-line {@code $} needs the
 * next byte, so it is confirmed one byte later. On "a bot x", {@code bot\b} loses to
 * {@code bot}.</li>
 * <li>A match that needs the end of the data ({@code $}, {@code \Z}, {@code \z}, or a
 * {@code \b} at the end) is confirmed only after the whole haystack is scanned, after every
 * other match. On "ab\n", {@code b$} loses to {@code \n}, and on "abc", {@code c$} loses
 * to {@code abc}. In an empty haystack every match is of this kind, so {@code ['$', '^']}
 * gives 1.</li>
 * </ul>
 * Literal patterns, most of a typical list, are found in one pass with Aho-Corasick. Each
 * regular expression is then checked in index order, only up to the best time so far, and
 * only when the haystack contains the literal that all of its matches contain. Its
 * confirmation time is the smallest {@code t} for which the scan form of the pattern
 * matches in the region [0, t) with opaque bounds. A match in [0, t) is also one in
 * [0, t + 1), so a binary search finds {@code t}; if there is none, the pattern is tried on
 * the whole haystack as Hyperscan does at the end of the data.
 */
final class ClickHouseMultiMatch
{
    private static final int NONE = Integer.MAX_VALUE;

    private final List<Slice> patterns;
    private final LiteralSearcher literals;
    private final RegexPattern[] regexes;

    private ClickHouseMultiMatch(List<Slice> patterns, LiteralSearcher literals, RegexPattern[] regexes)
    {
        this.patterns = List.copyOf(patterns);
        this.literals = literals;
        this.regexes = regexes;
    }

    static ClickHouseMultiMatch compile(List<Slice> patterns)
    {
        // ClickHouse checks every pattern for expensive repeats before compiling any
        for (Slice pattern : patterns) {
            if (HyperscanPatternTranslator.isSlow(pattern.toStringUtf8())) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, ("Pattern '%s': Regular expression evaluation in vectorscan will be too slow. " +
                        "ClickHouse rejects it unless setting 'reject_expensive_hyperscan_regexps' is disabled").formatted(pattern.toStringUtf8()));
            }
        }
        List<byte[]> literalPatterns = new ArrayList<>();
        List<Integer> literalIndexes = new ArrayList<>();
        List<RegexPattern> regexes = new ArrayList<>();
        for (int i = 0; i < patterns.size(); i++) {
            for (Branch branch : HyperscanPatternTranslator.translate(patterns.get(i))) {
                if (branch.isLiteral()) {
                    literalPatterns.add(branch.literal().getBytes());
                    literalIndexes.add(i + 1);
                }
                else {
                    regexes.add(new RegexPattern(i + 1, branch));
                }
            }
        }
        LiteralSearcher literals = literalPatterns.isEmpty() ? null : new LiteralSearcher(literalPatterns, literalIndexes);
        return new ClickHouseMultiMatch(patterns, literals, regexes.toArray(new RegexPattern[0]));
    }

    List<Slice> patterns()
    {
        return patterns;
    }

    /**
     * The 1-based index of the pattern ClickHouse reports, or 0 when none matches.
     */
    long find(Slice haystack)
    {
        Slice bytes = haystack;
        boolean ascii = SliceUtf8.isAscii(bytes);
        if (!ascii && !SliceUtf8.isUtf8(bytes)) {
            // Hyperscan's UTF-8 mode is undefined for invalid UTF-8; match the text Trino would show
            bytes = SliceUtf8.fixInvalidUtf8(bytes);
        }

        if (bytes.length() == 0) {
            return findInEmpty();
        }

        int bestTime = NONE;
        int bestIndex = NONE;
        if (literals != null) {
            long found = literals.find(bytes);
            if (found >= 0) {
                bestTime = (int) (found >>> 32);
                bestIndex = (int) found;
            }
        }
        if (regexes.length == 0) {
            return bestIndex == NONE ? 0 : bestIndex;
        }

        Haystack text = null;
        for (RegexPattern regex : regexes) {
            // a tie goes to the lower index, and a later branch of the same pattern must be earlier;
            // times are at most length + 1, so NONE - 1 still allows every time
            int limit = regex.index() < bestIndex ? bestTime : bestTime - 1;
            if (limit < 0 || (regex.requiredLiteral() != null && bytes.indexOf(regex.requiredLiteral()) < 0)) {
                continue;
            }
            if (text == null) {
                text = new Haystack(bytes, ascii);
            }
            int time = regex.confirmationTime(text, limit);
            if (time >= 0) {
                bestTime = time;
                bestIndex = regex.index();
            }
        }
        return bestIndex == NONE ? 0 : bestIndex;
    }

    // Hyperscan scans nothing in empty data, so every match comes at the end of the data, in index order
    private long findInEmpty()
    {
        int bestIndex = NONE;
        if (literals != null) {
            // only an empty literal matches, at time 0, so the packed result is just its index
            long found = literals.find(Slices.EMPTY_SLICE);
            if (found >= 0) {
                bestIndex = (int) found;
            }
        }
        for (RegexPattern regex : regexes) {
            if (regex.index() > bestIndex) {
                break;
            }
            if (regex.endOfData().matcher("").find()) {
                return regex.index();
            }
        }
        return bestIndex == NONE ? 0 : bestIndex;
    }

    /**
     * The haystack as Java text, with the UTF-8 byte offset of each char index.
     */
    private static final class Haystack
    {
        private final String text;
        private final int byteLength;
        // null when the haystack is ASCII and char indexes are byte offsets
        private final int[] byteOffsets;

        Haystack(Slice bytes, boolean ascii)
        {
            this.text = bytes.toStringUtf8();
            this.byteLength = bytes.length();
            if (ascii) {
                byteOffsets = null;
                return;
            }
            int[] offsets = new int[text.length() + 1];
            int offset = 0;
            for (int i = 0; i < text.length(); i++) {
                offsets[i] = offset;
                char character = text.charAt(i);
                if (Character.isHighSurrogate(character) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                    // the middle of a surrogate pair maps to the start of the code point
                    offsets[i + 1] = offset;
                    offset += 4;
                    i++;
                }
                else if (character < 0x80) {
                    offset++;
                }
                else if (character < 0x800) {
                    offset += 2;
                }
                else {
                    offset += 3;
                }
            }
            offsets[text.length()] = offset;
            this.byteOffsets = offsets;
        }

        int charLength()
        {
            return text.length();
        }

        int byteOffset(int charIndex)
        {
            if (byteOffsets == null) {
                return charIndex;
            }
            return byteOffsets[charIndex];
        }

        // the smallest char index whose byte offset is at least byteOffset
        int charIndexAtOrAfter(int byteOffset)
        {
            if (byteOffsets == null) {
                return Math.min(byteOffset, text.length());
            }
            int low = 0;
            int high = text.length();
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (byteOffsets[middle] < byteOffset) {
                    low = middle + 1;
                }
                else {
                    high = middle;
                }
            }
            return low;
        }

        // the start of the code point that ends at char index end
        int codePointStartBefore(int end)
        {
            if (end >= 2 && Character.isLowSurrogate(text.charAt(end - 1)) && Character.isHighSurrogate(text.charAt(end - 2))) {
                return end - 2;
            }
            return end - 1;
        }
    }

    private static final class RegexPattern
    {
        private final int index;
        private final Pattern scan;
        private final Pattern scanFollowed;
        private final Pattern endOfData;
        private final Slice requiredLiteral;
        private final boolean anchoredAtStart;
        private final boolean endOfDataOnly;
        private final int maxLength;

        RegexPattern(int index, Branch branch)
        {
            this.index = index;
            this.scan = branch.scan();
            this.scanFollowed = branch.scanFollowed();
            this.endOfData = branch.endOfData();
            this.requiredLiteral = branch.requiredLiteral();
            this.anchoredAtStart = branch.anchoredAtStart();
            this.endOfDataOnly = branch.endOfDataOnly();
            this.maxLength = branch.maxLength();
        }

        int index()
        {
            return index;
        }

        Pattern endOfData()
        {
            return endOfData;
        }

        Slice requiredLiteral()
        {
            return requiredLiteral;
        }

        /**
         * The time in bytes at which Hyperscan confirms the earliest match, or -1 when
         * that is later than limit. Matches confirmed at the end of the data get length + 1.
         */
        int confirmationTime(Haystack haystack, int limit)
        {
            int byteLength = haystack.byteLength;
            if (!endOfDataOnly) {
                int scanLimit = Math.min(limit, byteLength);
                int charLimit = haystack.charIndexAtOrAfter(scanLimit);
                Matcher matcher = scan.matcher(haystack.text);
                matcher.useTransparentBounds(false);
                matcher.useAnchoringBounds(false);
                if (find(matcher.region(0, charLimit))) {
                    // every match in a smaller region starts at or after this leftmost one
                    int low = matcher.start();
                    // this match is confirmed by the end of the code point after it
                    int high = Math.min(charLimit, matcher.end() + 2);
                    while (low < high) {
                        int middle = (low + high) >>> 1;
                        if (find(matcher.region(0, middle))) {
                            high = middle;
                        }
                        else {
                            low = middle + 1;
                        }
                    }
                    int time = byteTime(haystack, low);
                    return time <= scanLimit ? time : -1;
                }
                if (charLimit < haystack.charLength()) {
                    return -1;
                }
            }
            if (limit <= byteLength) {
                return -1;
            }
            // Only a match that ends at the end, or before a final newline, needs the end of the data
            Matcher matcher = endOfData.matcher(haystack.text);
            matcher.useTransparentBounds(true);
            matcher.useAnchoringBounds(false);
            if (maxLength >= 0 && !anchoredAtStart) {
                matcher.region(Math.max(0, haystack.charLength() - maxLength - 1), haystack.charLength());
            }
            if (find(matcher)) {
                return byteLength + 1;
            }
            return -1;
        }

        private boolean find(Matcher matcher)
        {
            if (anchoredAtStart) {
                return matcher.lookingAt();
            }
            return matcher.find();
        }

        /**
         * Converts the smallest region end with a match to bytes. A match that ends just before a
         * multi-byte code point and needs to see it is confirmed by that code point's first
         * byte, which is earlier than the end of the code point.
         */
        private int byteTime(Haystack haystack, int charEnd)
        {
            if (charEnd == 0 || haystack.byteOffsets == null) {
                return haystack.byteOffset(charEnd);
            }
            int previous = haystack.codePointStartBefore(charEnd);
            if (haystack.text.charAt(previous) >= 0x80) {
                Matcher followed = scanFollowed.matcher(haystack.text);
                followed.useTransparentBounds(false);
                followed.useAnchoringBounds(false);
                if (find(followed.region(0, charEnd))) {
                    return haystack.byteOffset(previous) + 1;
                }
            }
            return haystack.byteOffset(charEnd);
        }
    }

    /**
     * Aho-Corasick over UTF-8 bytes. Finds the earliest end of any literal, and the lowest
     * index of the literals that end there.
     */
    private static final class LiteralSearcher
    {
        private final int[] rootTransitions = new int[256];
        // children of each node, sorted by byte: childBytes and childNodes from childStart[node] to childStart[node + 1]
        private final int[] childStart;
        private final byte[] childBytes;
        private final int[] childNodes;
        private final int[] failure;
        // lowest index of a literal that ends at this node or at a suffix of it, NONE if none
        private final int[] output;

        LiteralSearcher(List<byte[]> literals, List<Integer> indexes)
        {
            List<TreeMap<Integer, Integer>> children = new ArrayList<>();
            List<Integer> own = new ArrayList<>();
            children.add(new TreeMap<>());
            own.add(NONE);
            for (int i = 0; i < literals.size(); i++) {
                int node = 0;
                for (byte value : literals.get(i)) {
                    int key = value & 0xFF;
                    Integer child = children.get(node).get(key);
                    if (child == null) {
                        child = children.size();
                        children.add(new TreeMap<>());
                        own.add(NONE);
                        children.get(node).put(key, child);
                    }
                    node = child;
                }
                own.set(node, Math.min(own.get(node), indexes.get(i)));
            }

            int nodeCount = children.size();
            childStart = new int[nodeCount + 1];
            int edgeCount = 0;
            for (int node = 0; node < nodeCount; node++) {
                childStart[node] = edgeCount;
                edgeCount += children.get(node).size();
            }
            childStart[nodeCount] = edgeCount;
            childBytes = new byte[edgeCount];
            childNodes = new int[edgeCount];
            for (int node = 0; node < nodeCount; node++) {
                int edge = childStart[node];
                for (var entry : children.get(node).entrySet()) {
                    childBytes[edge] = (byte) (int) entry.getKey();
                    childNodes[edge] = entry.getValue();
                    edge++;
                }
            }

            failure = new int[nodeCount];
            output = new int[nodeCount];
            output[0] = own.get(0);
            int[] queue = new int[nodeCount];
            int head = 0;
            int tail = 0;
            for (int edge = childStart[0]; edge < childStart[1]; edge++) {
                int child = childNodes[edge];
                failure[child] = 0;
                output[child] = Math.min(own.get(child), output[0]);
                queue[tail++] = child;
            }
            while (head < tail) {
                int node = queue[head++];
                for (int edge = childStart[node]; edge < childStart[node + 1]; edge++) {
                    int child = childNodes[edge];
                    int key = childBytes[edge] & 0xFF;
                    int fallback = failure[node];
                    int next = child(fallback, key);
                    while (next < 0 && fallback != 0) {
                        fallback = failure[fallback];
                        next = child(fallback, key);
                    }
                    failure[child] = next < 0 ? 0 : next;
                    output[child] = Math.min(own.get(child), output[failure[child]]);
                    queue[tail++] = child;
                }
            }

            for (int edge = childStart[0]; edge < childStart[1]; edge++) {
                rootTransitions[childBytes[edge] & 0xFF] = childNodes[edge];
            }
        }

        private int child(int node, int key)
        {
            int low = childStart[node];
            int high = childStart[node + 1] - 1;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                int value = childBytes[middle] & 0xFF;
                if (value < key) {
                    low = middle + 1;
                }
                else if (value > key) {
                    high = middle - 1;
                }
                else {
                    return childNodes[middle];
                }
            }
            return -1;
        }

        /**
         * The earliest end (high 32 bits) and the lowest index ending there (low 32 bits), or -1.
         */
        long find(Slice bytes)
        {
            if (output[0] != NONE) {
                // an empty literal matches at offset 0
                return output[0];
            }
            int node = 0;
            int length = bytes.length();
            for (int position = 0; position < length; position++) {
                int key = bytes.getByte(position) & 0xFF;
                while (true) {
                    if (node == 0) {
                        node = rootTransitions[key];
                        break;
                    }
                    int next = child(node, key);
                    if (next >= 0) {
                        node = next;
                        break;
                    }
                    node = failure[node];
                }
                if (output[node] != NONE) {
                    return ((long) (position + 1) << 32) | output[node];
                }
            }
            return -1;
        }
    }
}
