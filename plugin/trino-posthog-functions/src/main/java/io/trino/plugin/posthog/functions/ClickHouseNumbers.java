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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * How ClickHouse's JSON functions read a JSON number, a numeric string, or a boolean (as 1 or 0)
 * into a numeric type. A JSON number is read exactly. A string is read with ClickHouse's
 * {@code readFloatTextFastImpl}, which {@code precise_float_parsing = 0} selects and which can differ
 * from the closest double in the last bits.
 */
final class ClickHouseNumbers
{
    private static final double TWO_POW_63 = 0x1p63;
    private static final double TWO_POW_64 = 0x1p64;

    // readFloatTextFastImpl scales by a table of long double powers of ten. On aarch64 Linux, where
    // ClickHouse's fixtures for these tests come from, long double is IEEE binary128 with 113 bits.
    private static final int LONG_DOUBLE_PRECISION = 113;
    private static final int MIN_POWER = -323;
    private static final int MAX_POWER = 308;
    private static final int SIGNIFICANT_DIGITS = 19;
    private static final BigDecimal[] LONG_DOUBLE_POWERS = new BigDecimal[MAX_POWER - MIN_POWER + 1];
    private static final double[] DOUBLE_POWERS = new double[MAX_POWER - MIN_POWER + 1];
    private static final float[] FLOAT_POWERS = new float[MAX_POWER - MIN_POWER + 1];

    static {
        for (int exponent = MIN_POWER; exponent <= MAX_POWER; exponent++) {
            BigDecimal power = roundToLongDouble(BigDecimal.ONE.scaleByPowerOfTen(exponent));
            LONG_DOUBLE_POWERS[exponent - MIN_POWER] = power;
            DOUBLE_POWERS[exponent - MIN_POWER] = power.doubleValue();
            FLOAT_POWERS[exponent - MIN_POWER] = power.floatValue();
        }
    }

    private ClickHouseNumbers() {}

    static double float64FromNumber(String text, boolean isInteger)
    {
        // ClickHouse reads a JSON integer that fits 64 bits as an integer first, so -0 becomes +0.
        if (isInteger) {
            Long signed = parseLong(text);
            if (signed != null) {
                return signed;
            }
            Long unsigned = parseUnsignedLong(text);
            if (unsigned != null) {
                return unsignedToDouble(unsigned);
            }
        }
        return Double.parseDouble(text);
    }

    /**
     * Returns the value in [low, high], or null when ClickHouse would drop it. ClickHouse reads an
     * integer exactly when it fits Int64 or UInt64, and reads anything else as a double, which
     * converts only when it is inside the range, truncated toward zero.
     */
    static Long integerFromNumber(String text, boolean isInteger, long low, long high)
    {
        if (isInteger) {
            Long signed = parseLong(text);
            if (signed != null) {
                return signed >= low && signed <= high ? signed : null;
            }
            if (parseUnsignedLong(text) != null) {
                return null;
            }
        }
        return integerFromDouble(Double.parseDouble(text), low, high);
    }

    static Long uint64FromNumber(String text, boolean isInteger)
    {
        if (isInteger) {
            Long signed = parseLong(text);
            if (signed != null) {
                return signed >= 0 ? signed : null;
            }
            Long unsigned = parseUnsignedLong(text);
            if (unsigned != null) {
                return unsigned;
            }
        }
        return uint64FromDouble(Double.parseDouble(text));
    }

    static Double float64FromString(String text)
    {
        return ClickHouseFloatText.parse(text, false);
    }

    static Float float32FromString(String text)
    {
        Double value = ClickHouseFloatText.parse(text, true);
        return value == null ? null : value.floatValue();
    }

    // An integer string is read exactly when it fits the type; anything else is read as a Float64.
    static Long integerFromString(String text, long low, long high)
    {
        Long exact = isInteger(text) ? parseLong(text) : null;
        if (exact != null && exact >= low && exact <= high) {
            return exact;
        }
        Double value = float64FromString(text);
        return value == null ? null : integerFromDouble(value, low, high);
    }

    static Long uint64FromString(String text)
    {
        if (isInteger(text)) {
            Long signed = parseLong(text);
            if (signed != null && signed >= 0) {
                return signed;
            }
            Long unsigned = parseUnsignedLong(text);
            if (unsigned != null) {
                return unsigned;
            }
        }
        Double value = float64FromString(text);
        return value == null ? null : uint64FromDouble(value);
    }

    // A double converts only inside the type's range, truncated toward zero, and NaN converts to zero.
    private static Long integerFromDouble(double value, long low, long high)
    {
        if (Double.isNaN(value)) {
            return 0L;
        }
        boolean belowHigh = high == Long.MAX_VALUE ? value < TWO_POW_63 : value <= high;
        if (value >= low && belowHigh) {
            return (long) value;
        }
        return null;
    }

    private static Long uint64FromDouble(double value)
    {
        if (Double.isNaN(value)) {
            return 0L;
        }
        if (value < 0 || value >= TWO_POW_64) {
            return null;
        }
        if (value < TWO_POW_63) {
            return (long) value;
        }
        return (long) (value - TWO_POW_63) ^ Long.MIN_VALUE;
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

    private static Long parseLong(String text)
    {
        try {
            return Long.parseLong(text);
        }
        catch (NumberFormatException _) {
            return null;
        }
    }

    private static Long parseUnsignedLong(String text)
    {
        if (text.isEmpty() || text.charAt(0) == '-') {
            return null;
        }
        try {
            return Long.parseUnsignedLong(text.charAt(0) == '+' ? text.substring(1) : text);
        }
        catch (NumberFormatException _) {
            return null;
        }
    }

    private static double unsignedToDouble(long value)
    {
        return value >= 0 ? value : new BigDecimal(new BigInteger(Long.toUnsignedString(value))).doubleValue();
    }

    private static boolean isDigit(char c)
    {
        return c >= '0' && c <= '9';
    }

    // Rounds to the nearest long double, ties to even.
    private static BigDecimal roundToLongDouble(BigDecimal value)
    {
        if (value.signum() == 0) {
            return value;
        }
        // value lies in [2^exponent, 2^(exponent + 1))
        int exponent = (int) Math.floor((value.precision() - value.scale() - 1) * 3.321928094887362);
        while (pow2(exponent).compareTo(value) > 0) {
            exponent--;
        }
        while (pow2(exponent + 1).compareTo(value) <= 0) {
            exponent++;
        }
        int shift = LONG_DOUBLE_PRECISION - 1 - exponent;
        BigInteger mantissa = value.multiply(pow2(shift)).setScale(0, RoundingMode.HALF_EVEN).toBigIntegerExact();
        return new BigDecimal(mantissa).multiply(pow2(-shift));
    }

    private static BigDecimal pow2(int exponent)
    {
        if (exponent >= 0) {
            return new BigDecimal(BigInteger.ONE.shiftLeft(exponent));
        }
        return new BigDecimal(BigInteger.valueOf(5).pow(-exponent), -exponent);
    }

    /**
     * A port of ClickHouse's {@code readFloatTextFastImpl}, for a whole string. Returns null when
     * ClickHouse rejects the string.
     */
    private static final class ClickHouseFloatText
    {
        private final String text;
        private int position;

        private ClickHouseFloatText(String text)
        {
            this.text = text;
        }

        static Double parse(String text, boolean float32)
        {
            ClickHouseFloatText reader = new ClickHouseFloatText(text);
            Double value = reader.read(float32);
            return value != null && reader.position == text.length() ? value : null;
        }

        private boolean eof()
        {
            return position >= text.length();
        }

        private char peek()
        {
            return text.charAt(position);
        }

        private boolean check(char expected)
        {
            if (!eof() && peek() == expected) {
                position++;
                return true;
            }
            return false;
        }

        // Accumulates the first `limit` digits, leading zeros included, and skips any after them.
        private long readDigits(int limit)
        {
            long value = 0;
            for (int i = 0; i < limit && !eof() && isDigit(peek()); i++) {
                value = value * 10 + (peek() - '0');
                position++;
            }
            while (!eof() && isDigit(peek())) {
                position++;
            }
            return value;
        }

        private Double read(boolean float32)
        {
            if (eof()) {
                return null;
            }
            boolean negative = false;
            if (peek() == '-') {
                negative = true;
                position++;
            }
            else if (peek() == '+') {
                position++;
            }
            int afterSign = position;

            long beforePoint = readDigits(SIGNIFICANT_DIGITS);
            int readDigits = position - afterSign;
            double x;
            if (readDigits > SIGNIFICANT_DIGITS) {
                x = narrow(scaleLongDouble(beforePoint, readDigits - SIGNIFICANT_DIGITS), float32);
            }
            else {
                x = narrow(unsignedValue(beforePoint), float32);
                if (readDigits > 0 && (eof() || peek() < '.')) {
                    return negative ? -x : x;
                }
            }

            long afterPoint = 0;
            int afterPointExponent = 0;
            if (check('.')) {
                int afterPointStart = position;
                while (!eof() && peek() == '0') {
                    position++;
                }
                int leadingZeros = position - afterPointStart;
                int digitsStart = position;
                afterPoint = readDigits(SIGNIFICANT_DIGITS);
                int fractionDigits = position - digitsStart;
                afterPointExponent = (fractionDigits > SIGNIFICANT_DIGITS ? -SIGNIFICANT_DIGITS : -fractionDigits) - leadingZeros;
            }

            int exponent = 0;
            if (check('e') || check('E')) {
                if (eof()) {
                    return null;
                }
                boolean exponentNegative = false;
                if (peek() == '-') {
                    exponentNegative = true;
                    position++;
                }
                else if (peek() == '+') {
                    position++;
                }
                exponent = (int) readDigits(4);
                if (exponentNegative) {
                    exponent = -exponent;
                }
            }

            if (afterPoint != 0) {
                double fraction = narrow(scaleLongDouble(afterPoint, afterPointExponent), float32);
                x = float32 ? (float) ((float) x + (float) fraction) : x + fraction;
            }
            if (exponent != 0) {
                x = scale(x, exponent, float32);
            }
            if (negative) {
                x = -x;
            }

            if (position == afterSign) {
                if (eof()) {
                    return null;
                }
                if (peek() == '+') {
                    position++;
                    if (eof() || negative) {
                        return null;
                    }
                }
                if (peek() == 'i' || peek() == 'I') {
                    return parseInfinity() ? (negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY) : null;
                }
                if (peek() == 'n' || peek() == 'N') {
                    return parseCaseInsensitive("nan") ? Double.NaN : null;
                }
            }
            return x;
        }

        private boolean parseInfinity()
        {
            if (!parseCaseInsensitive("inf")) {
                return false;
            }
            if (eof() || !isWordChar(peek())) {
                return true;
            }
            return parseCaseInsensitive("inity");
        }

        private boolean parseCaseInsensitive(String expected)
        {
            for (int i = 0; i < expected.length(); i++) {
                if (eof() || Character.toLowerCase(peek()) != expected.charAt(i)) {
                    return false;
                }
                position++;
            }
            return true;
        }

        private static boolean isWordChar(char c)
        {
            return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        }
    }

    // The unsigned value of a long, as a long double; every such value fits one exactly.
    private static Object unsignedValue(long value)
    {
        if (value >= 0 && value < (1L << 53)) {
            return (double) value;
        }
        return unsignedDecimal(value);
    }

    private static BigDecimal unsignedDecimal(long value)
    {
        return value >= 0 ? BigDecimal.valueOf(value) : new BigDecimal(new BigInteger(Long.toUnsignedString(value)));
    }

    // shift10(UInt64, exponent): the product in long double. Returns NaN or infinity as a double.
    private static Object scaleLongDouble(long value, int exponent)
    {
        if (exponent < MIN_POWER) {
            return BigDecimal.ZERO;
        }
        if (exponent > MAX_POWER) {
            return value == 0 ? Double.NaN : Double.POSITIVE_INFINITY;
        }
        return roundToLongDouble(unsignedDecimal(value).multiply(LONG_DOUBLE_POWERS[exponent - MIN_POWER]));
    }

    // A Double here is a value a long double holds exactly, or NaN or infinity.
    private static double narrow(Object longDouble, boolean float32)
    {
        if (longDouble instanceof Double exact) {
            return float32 ? (float) exact.doubleValue() : exact;
        }
        BigDecimal value = (BigDecimal) longDouble;
        return float32 ? value.floatValue() : value.doubleValue();
    }

    // shift10(T, exponent), in the arithmetic of T
    private static double scale(double x, int exponent, boolean float32)
    {
        if (exponent < MIN_POWER) {
            return float32 ? (float) x * 0.0f : x * 0.0;
        }
        if (exponent > MAX_POWER) {
            return float32 ? (float) x * Float.POSITIVE_INFINITY : x * Double.POSITIVE_INFINITY;
        }
        if (float32) {
            return (float) x * FLOAT_POWERS[exponent - MIN_POWER];
        }
        return x * DOUBLE_POWERS[exponent - MIN_POWER];
    }
}
