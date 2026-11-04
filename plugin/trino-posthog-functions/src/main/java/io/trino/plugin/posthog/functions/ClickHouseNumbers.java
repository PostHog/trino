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
 * How ClickHouse's JSON functions read a number, a numeric string, or a boolean (as 1 or 0)
 * into a numeric type.
 */
final class ClickHouseNumbers
{
    private static final double TWO_POW_63 = 0x1p63;
    private static final double TWO_POW_64 = 0x1p64;

    private ClickHouseNumbers() {}

    /**
     * Returns the value, or null when ClickHouse would drop it. Accepts a sign, digits with an
     * optional fraction and exponent, and inf, infinity or nan in any case.
     */
    static Double parseFloat(String text)
    {
        int length = text.length();
        int start = 0;
        if (length > 0 && (text.charAt(0) == '+' || text.charAt(0) == '-')) {
            start = 1;
        }
        String unsigned = text.substring(start);
        if (unsigned.equalsIgnoreCase("inf") || unsigned.equalsIgnoreCase("infinity")) {
            return text.charAt(0) == '-' ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        if (unsigned.equalsIgnoreCase("nan")) {
            return Double.NaN;
        }
        if (!isDecimal(text, start)) {
            return null;
        }
        return Double.parseDouble(text);
    }

    // [0-9]+(\.[0-9]*)?|\.[0-9]+, then an optional exponent
    private static boolean isDecimal(String text, int position)
    {
        int length = text.length();
        int digits = 0;
        while (position < length && isDigit(text.charAt(position))) {
            position++;
            digits++;
        }
        if (position < length && text.charAt(position) == '.') {
            position++;
            while (position < length && isDigit(text.charAt(position))) {
                position++;
                digits++;
            }
        }
        if (digits == 0) {
            return false;
        }
        if (position < length && (text.charAt(position) == 'e' || text.charAt(position) == 'E')) {
            position++;
            if (position < length && (text.charAt(position) == '+' || text.charAt(position) == '-')) {
                position++;
            }
            int exponentDigits = 0;
            while (position < length && isDigit(text.charAt(position))) {
                position++;
                exponentDigits++;
            }
            if (exponentDigits == 0) {
                return false;
            }
        }
        return position == length;
    }

    /**
     * Returns the value in [low, high], or null when ClickHouse would drop it. ClickHouse reads an
     * integer exactly when it fits in Int64 or UInt64, and reads anything else as a double, which
     * converts only when it is inside the range, truncated toward zero. NaN converts to zero.
     */
    static Long parseInteger(String text, long low, long high)
    {
        if (isInteger(text)) {
            Long exact = parseExactSigned(text);
            if (exact != null) {
                return exact >= low && exact <= high ? exact : null;
            }
            if (parseExactUnsigned(text) != null) {
                return null;
            }
        }
        Double value = parseFloat(text);
        if (value == null) {
            return null;
        }
        if (value.isNaN()) {
            return 0L;
        }
        boolean belowHigh = high == Long.MAX_VALUE ? value < TWO_POW_63 : value <= high;
        if (value >= low && belowHigh) {
            return (long) value.doubleValue();
        }
        return null;
    }

    /**
     * Like {@link #parseInteger} for UInt64, returning the unsigned value's bits.
     */
    static Long parseUInt64(String text)
    {
        if (isInteger(text)) {
            Long exact = parseExactSigned(text);
            if (exact != null) {
                return exact >= 0 ? exact : null;
            }
            Long unsigned = parseExactUnsigned(text);
            if (unsigned != null) {
                return unsigned;
            }
        }
        Double value = parseFloat(text);
        if (value == null) {
            return null;
        }
        if (value.isNaN()) {
            return 0L;
        }
        if (value < 0 || value >= TWO_POW_64) {
            return null;
        }
        if (value < TWO_POW_63) {
            return (long) value.doubleValue();
        }
        return (long) (value - TWO_POW_63) ^ Long.MIN_VALUE;
    }

    /**
     * Returns an integer that fits Int64 or UInt64 as a double, or null for any other text.
     */
    static Double integerAsDouble(String text)
    {
        if (!isInteger(text)) {
            return null;
        }
        Long signed = parseExactSigned(text);
        if (signed != null) {
            return (double) signed;
        }
        Long unsigned = parseExactUnsigned(text);
        if (unsigned != null) {
            return unsigned >= 0 ? (double) unsigned : (double) (unsigned >>> 1) * 2 + (unsigned & 1);
        }
        return null;
    }

    private static boolean isInteger(String text)
    {
        int start = !text.isEmpty() && (text.charAt(0) == '+' || text.charAt(0) == '-') ? 1 : 0;
        if (start == text.length()) {
            return false;
        }
        for (int i = start; i < text.length(); i++) {
            if (!isDigit(text.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static Long parseExactSigned(String text)
    {
        try {
            return Long.parseLong(text);
        }
        catch (NumberFormatException _) {
            return null;
        }
    }

    private static Long parseExactUnsigned(String text)
    {
        if (text.charAt(0) == '-') {
            return null;
        }
        try {
            return Long.parseUnsignedLong(text.charAt(0) == '+' ? text.substring(1) : text);
        }
        catch (NumberFormatException _) {
            return null;
        }
    }

    private static boolean isDigit(char c)
    {
        return c >= '0' && c <= '9';
    }
}
