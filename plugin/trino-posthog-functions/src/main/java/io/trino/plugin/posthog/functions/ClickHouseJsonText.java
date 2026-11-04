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

/**
 * JSON text as ClickHouse writes it when it extracts a JSON value as a String.
 */
final class ClickHouseJsonText
{
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ClickHouseJsonText() {}

    /**
     * ClickHouse prints an integer that fits Int64 or UInt64 as a number, and any other integer as
     * written; inside an object or array it quotes that other integer. It prints any other number as
     * the shortest text that reads back as the same double, laid out like JavaScript's Number#toString.
     */
    static String number(String text, boolean isInteger, boolean nested)
    {
        if (isInteger) {
            try {
                return Long.toString(Long.parseLong(text));
            }
            catch (NumberFormatException _) {
                // fall through to UInt64
            }
            if (text.charAt(0) != '-') {
                try {
                    return Long.toUnsignedString(Long.parseUnsignedLong(text));
                }
                catch (NumberFormatException _) {
                    // fall through to the text as written
                }
            }
            return nested ? '"' + text + '"' : text;
        }
        return shortest(Double.parseDouble(text));
    }

    static String shortest(double value)
    {
        // An overflowing float rejects its document, so this text is never returned.
        if (!Double.isFinite(value)) {
            return "inf";
        }
        if (value == 0) {
            return 1 / value < 0 ? "-0" : "0";
        }
        String sign = value < 0 ? "-" : "";
        String java = Double.toString(Math.abs(value));
        int exponentIndex = java.indexOf('E');
        String mantissa = exponentIndex < 0 ? java : java.substring(0, exponentIndex);
        int exponent = exponentIndex < 0 ? 0 : Integer.parseInt(java.substring(exponentIndex + 1));
        int point = mantissa.indexOf('.');
        String allDigits = mantissa.substring(0, point) + mantissa.substring(point + 1);
        int leadingZeros = 0;
        while (leadingZeros < allDigits.length() - 1 && allDigits.charAt(leadingZeros) == '0') {
            leadingZeros++;
        }
        int end = allDigits.length();
        while (end > leadingZeros + 1 && allDigits.charAt(end - 1) == '0') {
            end--;
        }
        String digits = allDigits.substring(leadingZeros, end);
        // the decimal point sits after this many digits
        int n = point + exponent - leadingZeros;
        int k = digits.length();
        StringBuilder out = new StringBuilder(sign);
        if (k <= n && n <= 21) {
            out.append(digits).repeat('0', n - k);
        }
        else if (0 < n && n <= 21) {
            out.append(digits, 0, n).append('.').append(digits, n, k);
        }
        else if (-6 < n && n <= 0) {
            out.append("0.").repeat('0', -n).append(digits);
        }
        else {
            out.append(digits.charAt(0));
            if (k > 1) {
                out.append('.').append(digits, 1, k);
            }
            out.append('e').append(n - 1);
        }
        return out.toString();
    }

    static void appendString(StringBuilder out, String value)
    {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                // ClickHouse escapes these two line separators so the output is valid JavaScript.
                case ' ', ' ' -> appendUnicodeEscape(out, c);
                default -> {
                    if (c < 0x20) {
                        appendUnicodeEscape(out, c);
                    }
                    else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    private static void appendUnicodeEscape(StringBuilder out, char c)
    {
        out.append("\\u")
                .append(HEX[(c >> 12) & 0xF])
                .append(HEX[(c >> 8) & 0xF])
                .append(HEX[(c >> 4) & 0xF])
                .append(HEX[c & 0xF]);
    }
}
