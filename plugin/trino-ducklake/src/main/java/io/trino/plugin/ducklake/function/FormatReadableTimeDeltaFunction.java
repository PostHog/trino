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
package io.trino.plugin.ducklake.function;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class FormatReadableTimeDeltaFunction
{
    private FormatReadableTimeDeltaFunction() {}

    @Description("Formats a numeric duration in seconds using ClickHouse time delta wording")
    @ScalarFunction("format_readable_time_delta")
    @SqlType(VARCHAR)
    public static Slice formatReadableTimeDelta(@SqlType(DOUBLE) double value)
    {
        return utf8Slice(format(value, Unit.YEARS, Unit.SECONDS));
    }

    @Description("Formats a numeric duration in seconds using ClickHouse time delta wording")
    @ScalarFunction("format_readable_time_delta")
    @SqlType(VARCHAR)
    public static Slice formatReadableTimeDelta(
            @SqlType(DOUBLE) double value,
            @SqlType(VARCHAR) Slice maximumUnit)
    {
        Unit maximum = parseUnit(maximumUnit, "maximum");
        Unit minimum = maximum.ordinal() < Unit.SECONDS.ordinal() ? Unit.NANOSECONDS : Unit.SECONDS;
        return utf8Slice(format(value, maximum, minimum));
    }

    @Description("Formats a numeric duration in seconds using ClickHouse time delta wording")
    @ScalarFunction("format_readable_time_delta")
    @SqlType(VARCHAR)
    public static Slice formatReadableTimeDelta(
            @SqlType(DOUBLE) double value,
            @SqlType(VARCHAR) Slice maximumUnit,
            @SqlType(VARCHAR) Slice minimumUnit)
    {
        Unit maximum = parseUnit(maximumUnit, "maximum");
        Unit minimum = parseUnit(minimumUnit, "minimum");
        if (minimum.ordinal() > maximum.ordinal()) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Minimum unit (%s) must not be greater than maximum unit (%s)"
                            .formatted(minimum.name().toLowerCase(Locale.ENGLISH), maximum.name().toLowerCase(Locale.ENGLISH)));
        }
        return utf8Slice(format(value, maximum, minimum));
    }

    private static String format(double value, Unit maximum, Unit minimum)
    {
        if (Double.isNaN(value)) {
            return "nan";
        }
        if (value == Double.POSITIVE_INFINITY) {
            return "inf";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return "-inf";
        }

        boolean negative = value < 0;
        value = Math.abs(value);
        double wholeSeconds = Math.floor(value);
        long fractionalNanoseconds = roundedFractionalNanoseconds(value - wholeSeconds);
        List<String> parts = new ArrayList<>();

        for (int index = maximum.ordinal(); index >= minimum.ordinal(); index--) {
            Unit unit = Unit.values()[index];
            if (wholeSeconds + 1.0 == wholeSeconds) {
                double units = Math.floor(wholeSeconds * unit.scaleMultiplier() / unit.secondsMultiplier());
                parts.add(formatDouble(units) + " " + unit.singular() + "s");
                break;
            }

            long units;
            if (unit.scale() == 0) {
                units = (long) Math.floor(wholeSeconds / unit.secondsMultiplier());
                if (units == 0 && (unit != Unit.SECONDS || !parts.isEmpty())) {
                    continue;
                }
                wholeSeconds -= units * unit.secondsMultiplier();
            }
            else {
                long scaleMultiplier = unit.scaleMultiplier();
                units = (long) wholeSeconds * scaleMultiplier;
                wholeSeconds = 0;
                long divisor = pow10(9 - unit.scale());
                units += fractionalNanoseconds / divisor;
                fractionalNanoseconds %= divisor;
                if (units == 0 && (unit != minimum || !parts.isEmpty())) {
                    continue;
                }
            }
            parts.add(units + " " + unit.singular() + (units == 1 ? "" : "s"));
        }

        String result = joinParts(parts);
        return negative ? "-" + result : result;
    }

    private static long roundedFractionalNanoseconds(double fractional)
    {
        return Math.round(fractional * 1_000_000_000) % 1_000_000_000;
    }

    private static String joinParts(List<String> parts)
    {
        if (parts.isEmpty()) {
            return "";
        }
        if (parts.size() == 1) {
            return parts.getFirst();
        }
        if (parts.size() == 2) {
            return parts.getFirst() + " and " + parts.getLast();
        }
        return String.join(", ", parts.subList(0, parts.size() - 1)) + " and " + parts.getLast();
    }

    private static String formatDouble(double value)
    {
        if (Double.isInfinite(value)) {
            return "inf";
        }
        if (value < 1e21) {
            return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
        }
        String formatted = Double.toString(value).replace("E+", "e").replace('E', 'e');
        int exponent = formatted.indexOf('e');
        if (exponent > 1 && formatted.substring(0, exponent).endsWith(".0")) {
            return formatted.substring(0, exponent - 2) + formatted.substring(exponent);
        }
        return formatted;
    }

    private static long pow10(int exponent)
    {
        return switch (exponent) {
            case 0 -> 1;
            case 3 -> 1_000;
            case 6 -> 1_000_000;
            case 9 -> 1_000_000_000;
            default -> throw new IllegalArgumentException("unsupported decimal exponent: " + exponent);
        };
    }

    private static Unit parseUnit(Slice unit, String bound)
    {
        String value = unit.toStringUtf8();
        for (Unit candidate : Unit.values()) {
            if (candidate.name().toLowerCase(Locale.ENGLISH).equals(value)) {
                return candidate;
            }
        }
        throw new TrinoException(
                INVALID_FUNCTION_ARGUMENT,
                "Unexpected value of %s unit argument (%s); expected nanoseconds, microseconds, milliseconds, seconds, minutes, hours, days, months, or years"
                        .formatted(bound, value));
    }

    private enum Unit
    {
        NANOSECONDS(1, 9, "nanosecond"),
        MICROSECONDS(1, 6, "microsecond"),
        MILLISECONDS(1, 3, "millisecond"),
        SECONDS(1, 0, "second"),
        MINUTES(60, 0, "minute"),
        HOURS(3_600, 0, "hour"),
        DAYS(86_400, 0, "day"),
        MONTHS(2_635_200, 0, "month"),
        YEARS(31_536_000, 0, "year");

        private final long secondsMultiplier;
        private final int scale;
        private final String singular;

        Unit(long secondsMultiplier, int scale, String singular)
        {
            this.secondsMultiplier = secondsMultiplier;
            this.scale = scale;
            this.singular = singular;
        }

        public long secondsMultiplier()
        {
            return secondsMultiplier;
        }

        public int scale()
        {
            return scale;
        }

        public long scaleMultiplier()
        {
            return pow10(scale);
        }

        public String singular()
        {
            return singular;
        }
    }
}
