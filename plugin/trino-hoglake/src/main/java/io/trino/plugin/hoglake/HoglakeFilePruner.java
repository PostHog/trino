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
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Planning-time file pruning from the catalog's per-file column bounds
 * ({@code GET /scan?include=column_stats}): a file whose bounds cannot
 * satisfy the pushed-down predicate is never scheduled. Pruning is only an
 * optimization, so every doubt keeps the file: no statistics, no entry for
 * a column, a JSON null bound, an undecodable token, or a type whose bound
 * order differs from Trino's.
 *
 * <p>Bounds are the server's decoded wire tokens (openapi/hoglake.yaml
 * FileColumnStats): exact integers, ISO-8601 temporals that elide trailing
 * zero units, UTF-8 strings. They follow Iceberg single-value semantics, so
 * a writer-truncated string bound is still a valid bound to prune on.
 */
public final class HoglakeFilePruner
{
    private static final Logger log = Logger.get(HoglakeFilePruner.class);

    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger INTEGER_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INTEGER_MAX = BigInteger.valueOf(Integer.MAX_VALUE);

    private HoglakeFilePruner() {}

    /**
     * The field ids whose bounds could prune a file under {@code constraint}:
     * constrained columns of a type this pruner decodes. Empty when nothing
     * can be pruned, in which case the scan requests no statistics at all.
     */
    public static Set<Long> prunableFieldIds(TupleDomain<HoglakeColumnHandle> constraint)
    {
        if (constraint.isNone() || constraint.isAll()) {
            return ImmutableSet.of();
        }
        return constraint.getDomains().orElseThrow().entrySet().stream()
                .filter(entry -> !entry.getValue().isAll() && isPrunable(entry.getKey()))
                .map(entry -> entry.getKey().fieldId())
                .collect(toImmutableSet());
    }

    /**
     * Whether {@code file} may contain a row satisfying {@code constraint},
     * judged from its catalog statistics alone. False only when some
     * constrained column's file domain provably does not overlap the
     * predicate's.
     */
    public static boolean mayContain(TupleDomain<HoglakeColumnHandle> constraint, HoglakeDtos.DataFile file)
    {
        if (constraint.isNone()) {
            return false;
        }
        if (constraint.isAll() || file.columnStats() == null) {
            return true;
        }
        Map<Long, HoglakeDtos.ScanColumnStats> statsByFieldId = file.columnStats().stream()
                .collect(toMap(HoglakeDtos.ScanColumnStats::fieldId, identity(), (first, _) -> first));
        for (Map.Entry<HoglakeColumnHandle, Domain> entry : constraint.getDomains().orElseThrow().entrySet()) {
            HoglakeColumnHandle column = entry.getKey();
            Domain predicate = entry.getValue();
            if (predicate.isAll() || !isPrunable(column)) {
                continue;
            }
            HoglakeDtos.ScanColumnStats stats = statsByFieldId.get(column.fieldId());
            if (stats == null) {
                continue;
            }
            Optional<Domain> fileDomain;
            try {
                fileDomain = fileDomain(column.type(), stats);
            }
            catch (RuntimeException e) {
                // A bound this connector cannot read is a disagreement with the
                // server, never evidence: the file stays.
                log.debug(e, "Keeping %s: cannot decode statistics for column %s", file.path(), column.name());
                return true;
            }
            if (fileDomain.isPresent() && !fileDomain.get().overlaps(predicate)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The values one column takes in the file, or empty when its statistics
     * cannot bound them.
     */
    static Optional<Domain> fileDomain(Type type, HoglakeDtos.ScanColumnStats stats)
    {
        if (stats.nullCount() < 0 || stats.nullCount() > stats.valueCount()) {
            return Optional.empty();
        }
        // An all-null column needs no bounds: it can satisfy only predicates
        // that admit nulls. value_count includes nulls.
        if (stats.valueCount() > 0 && stats.nullCount() == stats.valueCount()) {
            return Optional.of(Domain.onlyNull(type));
        }
        // A null bound means "do not prune on this column" (openapi FileColumnStats).
        if (isNull(stats.lowerBound()) || isNull(stats.upperBound())) {
            return Optional.empty();
        }
        // Bounds exclude NaN, so a column that may hold NaNs is unbounded.
        // The hydrator cannot count NaNs from a footer and reports none.
        if ((type.equals(DOUBLE) || type.equals(REAL)) && !Long.valueOf(0).equals(stats.nanCount())) {
            return Optional.empty();
        }
        Object low = decode(type, stats.lowerBound());
        Object high = decode(type, stats.upperBound());
        return Optional.of(Domain.create(ValueSet.ofRanges(Range.range(type, low, true, high, true)), stats.nullCount() > 0));
    }

    /**
     * Top-level columns of the types this pruner decodes. Excluded because
     * their bound order is not Trino's, or their bounds are not decoded here:
     * UUID and the unsigned integer types (as in the Parquet predicate),
     * decimals, time, nanosecond timestamps, binary, and nested or variant
     * columns.
     */
    static boolean isPrunable(HoglakeColumnHandle column)
    {
        if (column.fieldId() < 0 || !column.children().isEmpty() || column.hoglakeType().startsWith("uint")) {
            return false;
        }
        Type type = column.type();
        return type.equals(BOOLEAN) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type.equals(DATE) ||
                type.equals(VARCHAR) ||
                type.equals(TIMESTAMP_MICROS) ||
                type.equals(TIMESTAMP_TZ_MICROS);
    }

    /**
     * One bound as the Trino native value of {@code type}. Throws on any token
     * the wire conventions do not produce for that type.
     */
    static Object decode(Type type, JsonNode bound)
    {
        if (type.equals(BOOLEAN)) {
            if (!bound.isBoolean()) {
                throw new IllegalArgumentException("Not a boolean bound: " + bound);
            }
            return bound.booleanValue();
        }
        if (type.equals(INTEGER)) {
            return exactInteger(bound, INTEGER_MIN, INTEGER_MAX);
        }
        if (type.equals(BIGINT)) {
            return exactInteger(bound, LONG_MIN, LONG_MAX);
        }
        if (type.equals(DOUBLE)) {
            return floatingPoint(bound);
        }
        if (type.equals(REAL)) {
            double value = floatingPoint(bound);
            // A float bound renders either as its shortest decimal or widened
            // to a double; both narrow back to exactly the stored float.
            float narrowed = (float) value;
            if (Float.isInfinite(narrowed) && !Double.isInfinite(value)) {
                throw new IllegalArgumentException("Float bound out of range: " + bound);
            }
            return (long) floatToRawIntBits(narrowed);
        }
        if (type.equals(DATE)) {
            return LocalDate.parse(text(bound), ISO_LOCAL_DATE).toEpochDay();
        }
        if (type.equals(VARCHAR)) {
            return Slices.utf8Slice(text(bound));
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return epochMicros(LocalDateTime.parse(text(bound), ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC));
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            Instant instant = OffsetDateTime.parse(text(bound), ISO_OFFSET_DATE_TIME).toInstant();
            if (instant.getNano() % 1_000 != 0) {
                throw new IllegalArgumentException("Not a microsecond bound: " + bound);
            }
            long epochMillis = Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000), instant.getNano() / 1_000_000);
            int picosOfMilli = (instant.getNano() % 1_000_000) * 1_000;
            return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY);
        }
        throw new IllegalArgumentException("Unsupported pruning type: " + type);
    }

    private static boolean isNull(JsonNode bound)
    {
        return bound == null || bound.isNull() || bound.isMissingNode();
    }

    private static long exactInteger(JsonNode bound, BigInteger min, BigInteger max)
    {
        // Integral JSON tokens parse to exact int, long or BigInteger nodes;
        // a token written with a fraction or exponent is not an integer bound.
        if (!bound.isIntegralNumber()) {
            throw new IllegalArgumentException("Not an integer bound: " + bound);
        }
        BigInteger value = bound.bigIntegerValue();
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
            throw new IllegalArgumentException("Integer bound out of range: " + bound);
        }
        return value.longValueExact();
    }

    private static double floatingPoint(JsonNode bound)
    {
        if (bound.isTextual()) {
            return switch (bound.textValue()) {
                case "Infinity" -> Double.POSITIVE_INFINITY;
                case "-Infinity" -> Double.NEGATIVE_INFINITY;
                default -> throw new IllegalArgumentException("Not a floating-point bound: " + bound);
            };
        }
        if (!bound.isNumber()) {
            throw new IllegalArgumentException("Not a floating-point bound: " + bound);
        }
        // Infinities arrive only as the string sentinels, and NaN is never a bound.
        double value = bound.doubleValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("Not a finite floating-point bound: " + bound);
        }
        return value;
    }

    private static String text(JsonNode bound)
    {
        if (!bound.isTextual()) {
            throw new IllegalArgumentException("Not a textual bound: " + bound);
        }
        return bound.textValue();
    }

    private static long epochMicros(Instant instant)
    {
        if (instant.getNano() % 1_000 != 0) {
            throw new IllegalArgumentException("Not a microsecond bound: " + instant);
        }
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000), instant.getNano() / 1_000);
    }
}
