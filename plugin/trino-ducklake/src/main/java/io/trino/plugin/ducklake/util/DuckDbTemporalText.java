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
package io.trino.plugin.ducklake.util;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Parses the text DuckDB writes for dates, times and timestamps, which is how DuckLake stores such
 * values of inlined rows in a PostgreSQL catalog database: PostgreSQL cannot hold the range DuckDB
 * can, so the values are kept as the strings DuckDB casts them to.
 * <p>
 * DuckDB writes a year before 1 AD as the year counted backwards followed by {@code (BC)}, so that
 * {@code 0001-01-01 (BC)} is the year 0 of the proleptic Gregorian calendar; years past 9999 take
 * as many digits as they need; fractions of a second have their trailing zeros removed; and a
 * timestamp with time zone carries the offset of the zone it was written in, such as {@code +00},
 * {@code -05} or {@code +05:30}. PostgreSQL text differs only in writing {@code BC} without the
 * parentheses, which is accepted as well. Infinite values have no Trino counterpart and are
 * rejected, as is anything else that is not one of these shapes.
 */
public final class DuckDbTemporalText
{
    private static final String DATE = "(\\d{4,})-(\\d{2})-(\\d{2})";
    private static final String TIME = "(\\d{2}):(\\d{2}):(\\d{2})(?:\\.(\\d{1,9}))?";
    private static final String OFFSET = "(?:([+-])(\\d{2})(?::?(\\d{2}))?(?::?(\\d{2}))?)?";
    private static final String ERA = "( \\(BC\\)| BC)?";

    private static final Pattern DATE_PATTERN = Pattern.compile(DATE + ERA);
    private static final Pattern TIME_PATTERN = Pattern.compile(TIME);
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(DATE + "[ T]" + TIME + OFFSET + ERA);

    private static final long SECONDS_PER_DAY = 86_400L;

    private DuckDbTemporalText() {}

    /**
     * A timestamp as written, the date and time it names on the calendar and, for a timestamp with
     * time zone, the offset from UTC it was written in.
     *
     * @param epochSecond the seconds of the date and time since 1970-01-01 00:00:00, taken as if it
     *         were in UTC
     * @param nanoOfSecond the fraction of the second, in nanoseconds
     * @param offsetSeconds the offset from UTC the text gives, if any
     */
    public record Timestamp(long epochSecond, int nanoOfSecond, OptionalInt offsetSeconds)
    {
        public Timestamp
        {
            requireNonNull(offsetSeconds, "offsetSeconds is null");
        }

        /**
         * The seconds since the epoch of the instant the timestamp names, applying the offset.
         */
        public long utcEpochSecond()
        {
            return epochSecond - offsetSeconds.orElseThrow(() -> new IllegalArgumentException("timestamp has no offset"));
        }
    }

    public static long parseEpochDay(String text)
    {
        Matcher matcher = DATE_PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw invalid("date", text);
        }
        return date(text, matcher, 4).toEpochDay();
    }

    /**
     * The time of day in nanoseconds.
     */
    public static long parseNanoOfDay(String text)
    {
        Matcher matcher = TIME_PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw invalid("time", text);
        }
        int hour = Integer.parseInt(matcher.group(1));
        int minute = Integer.parseInt(matcher.group(2));
        int second = Integer.parseInt(matcher.group(3));
        try {
            // 24:00:00, which both DuckDB and PostgreSQL accept, is not a time Trino can hold
            return LocalTime.of(hour, minute, second).toNanoOfDay() + fraction(matcher.group(4));
        }
        catch (DateTimeException e) {
            throw invalid("time", text);
        }
    }

    public static Timestamp parseTimestamp(String text)
    {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw invalid("timestamp", text);
        }
        LocalDate date = date(text, matcher, 12);
        int hour = Integer.parseInt(matcher.group(4));
        int minute = Integer.parseInt(matcher.group(5));
        int second = Integer.parseInt(matcher.group(6));
        long secondOfDay;
        try {
            secondOfDay = LocalTime.of(hour, minute, second).toSecondOfDay();
        }
        catch (DateTimeException e) {
            throw invalid("timestamp", text);
        }
        int nanoOfSecond = (int) fraction(matcher.group(7));
        OptionalInt offset = OptionalInt.empty();
        if (matcher.group(8) != null) {
            int offsetHours = Integer.parseInt(matcher.group(9));
            int offsetMinutes = matcher.group(10) == null ? 0 : Integer.parseInt(matcher.group(10));
            int offsetSeconds = matcher.group(11) == null ? 0 : Integer.parseInt(matcher.group(11));
            if (offsetMinutes >= 60 || offsetSeconds >= 60) {
                throw invalid("timestamp", text);
            }
            int total = offsetHours * 3600 + offsetMinutes * 60 + offsetSeconds;
            offset = OptionalInt.of(matcher.group(8).equals("-") ? -total : total);
        }
        try {
            return new Timestamp(addExact(multiplyExact(date.toEpochDay(), SECONDS_PER_DAY), secondOfDay), nanoOfSecond, offset);
        }
        catch (ArithmeticException e) {
            throw invalid("timestamp", text);
        }
    }

    private static LocalDate date(String text, Matcher matcher, int eraGroup)
    {
        long year;
        try {
            year = Long.parseLong(matcher.group(1));
        }
        catch (NumberFormatException e) {
            throw invalid("date", text);
        }
        if (matcher.group(eraGroup) != null) {
            // a year written backwards from 1 BC, which is the year 0 counted forwards
            year = 1 - year;
        }
        try {
            return LocalDate.of(toIntExact(year), Integer.parseInt(matcher.group(2)), Integer.parseInt(matcher.group(3)));
        }
        catch (DateTimeException | ArithmeticException e) {
            throw invalid("date", text);
        }
    }

    private static long fraction(String digits)
    {
        if (digits == null) {
            return 0;
        }
        long value = Long.parseLong(digits);
        for (int i = digits.length(); i < 9; i++) {
            value *= 10;
        }
        return value;
    }

    private static IllegalArgumentException invalid(String kind, String text)
    {
        return new IllegalArgumentException("Not a DuckDB %s value: '%s'".formatted(kind, text));
    }
}
