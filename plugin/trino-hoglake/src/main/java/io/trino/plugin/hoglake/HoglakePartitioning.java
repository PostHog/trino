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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

final class HoglakePartitioning
{
    private static final Set<String> BUCKETABLE = Set.of("int8", "int16", "int", "long", "uint8", "uint16", "date", "time", "timestamp", "timestamptz", "string", "uuid", "binary", "decimal");
    private static final Set<String> TEMPORAL = Set.of("date", "timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns", "timestamptz");
    private static final Pattern EXPRESSION = Pattern.compile("(?i)(identity|bucket|year|month|day|hour)\\(\\s*([a-z_][a-z_0-9]*(?:\\.[a-z_][a-z_0-9]*)*)(?:\\s*,\\s*([0-9]+))?\\s*\\)");
    private static final Pattern PATH = Pattern.compile("(?i)[a-z_][a-z_0-9]*(?:\\.[a-z_][a-z_0-9]*)*");
    private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    private HoglakePartitioning() {}

    static List<HoglakeDtos.PartitionField> read(Map<String, Object> spec)
    {
        if (spec == null) {
            return List.of();
        }
        if (!(spec.get("fields") instanceof List<?> fields)) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Hoglake partition spec");
        }
        ObjectMapper mapper = new ObjectMapper();
        return fields.stream().map(field -> mapper.convertValue(field, HoglakeDtos.PartitionField.class)).toList();
    }

    static List<HoglakeColumnHandle> initialColumns(List<HoglakeDtos.ColumnDefinition> definitions)
    {
        AtomicLong next = new AtomicLong(1);
        return definitions.stream().map(definition -> initialColumn(definition, next)).toList();
    }

    private static HoglakeColumnHandle initialColumn(HoglakeDtos.ColumnDefinition definition, AtomicLong next)
    {
        long id = next.getAndIncrement();
        List<HoglakeColumnHandle> children = definition.children().stream().map(child -> initialColumn(child, next)).toList();
        var type = switch (definition.type()) {
            case "struct" -> RowType.from(children.stream().map(child -> RowType.field(child.name(), child.type())).toList());
            case "list" -> new io.trino.spi.type.ArrayType(children.getFirst().type());
            case "map" -> new io.trino.spi.type.MapType(children.get(0).type(), children.get(1).type(), new io.trino.spi.type.TypeOperators());
            default -> HoglakeTypes.toTrinoType(definition.type(), definition.typeParams());
        };
        return new HoglakeColumnHandle(definition.name(), id, type, definition.nullable(), children, definition.type());
    }

    static List<HoglakeDtos.PartitionField> parse(List<String> expressions, List<HoglakeColumnHandle> columns)
    {
        List<HoglakeDtos.PartitionField> fields = new ArrayList<>();
        for (String expression : expressions) {
            var matcher = EXPRESSION.matcher(expression.trim());
            String path;
            String transform;
            Integer param = null;
            if (matcher.matches()) {
                transform = matcher.group(1).toLowerCase(Locale.ROOT);
                path = matcher.group(2);
                if (matcher.group(3) != null) {
                    try {
                        param = Integer.valueOf(matcher.group(3));
                    }
                    catch (NumberFormatException e) {
                        throw new TrinoException(NOT_SUPPORTED, "Invalid partition parameter: " + expression, e);
                    }
                }
            }
            else if (PATH.matcher(expression.trim()).matches()) {
                path = expression.trim();
                transform = "identity";
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported partition expression: " + expression);
            }
            List<HoglakeColumnHandle> siblings = columns;
            HoglakeColumnHandle column = null;
            for (String name : path.split("\\.")) {
                column = siblings.stream().filter(candidate -> candidate.name().equals(name)).findFirst()
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unknown partition source: " + path));
                siblings = column.hoglakeType().equals("struct") ? column.children() : List.of();
            }
            fields.add(new HoglakeDtos.PartitionField(column.fieldId(), transform, param));
        }
        validate(fields, columns);
        return List.copyOf(fields);
    }

    static void validate(List<HoglakeDtos.PartitionField> fields, List<HoglakeColumnHandle> columns)
    {
        for (var field : fields) {
            List<HoglakeColumnHandle> chain = find(columns, field.sourceFieldId());
            if (chain.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Partition source is not a scalar or struct leaf: " + field.sourceFieldId());
            }
            String type = chain.getLast().hoglakeType();
            if (!chain.getLast().children().isEmpty() || type.equals("variant")) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported partition source type: " + type);
            }
            if (field.transform() == null) {
                throw new TrinoException(NOT_SUPPORTED, "Missing partition transform");
            }
            switch (field.transform()) {
                case "identity" -> {}
                case "bucket" -> {
                    if (field.transformParam() == null || field.transformParam() < 1 || !BUCKETABLE.contains(type)) {
                        throw new TrinoException(NOT_SUPPORTED, "Unsupported bucket partition for " + type);
                    }
                }
                case "year", "month", "day", "hour" -> {
                    if (!TEMPORAL.contains(type) || (field.transform().equals("hour") && type.equals("date"))) {
                        throw new TrinoException(NOT_SUPPORTED, "Unsupported temporal partition for " + type);
                    }
                }
                default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported partition transform: " + field.transform());
            }
            if (!field.transform().equals("bucket") && field.transformParam() != null) {
                throw new TrinoException(NOT_SUPPORTED, "Partition transform does not take a parameter: " + field.transform());
            }
        }
    }

    static List<String> expressions(List<HoglakeDtos.PartitionField> fields, List<HoglakeColumnHandle> columns)
    {
        return fields.stream().map(field -> {
            var chain = find(columns, field.sourceFieldId());
            if (chain.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Unknown partition source: " + field.sourceFieldId());
            }
            String path = String.join(".", chain.stream().map(HoglakeColumnHandle::name).toList());
            return field.transform().equals("identity") ? path : field.transform() + "(" + path + (field.transformParam() == null ? "" : ", " + field.transformParam()) + ")";
        }).toList();
    }

    private static List<HoglakeColumnHandle> find(List<HoglakeColumnHandle> columns, long id)
    {
        for (var column : columns) {
            if (column.fieldId() == id) {
                return List.of(column);
            }
            if (column.hoglakeType().equals("struct")) {
                var tail = find(column.children(), id);
                if (!tail.isEmpty()) {
                    List<HoglakeColumnHandle> result = new ArrayList<>();
                    result.add(column);
                    result.addAll(tail);
                    return result;
                }
            }
        }
        return List.of();
    }

    static List<String> values(List<HoglakeDtos.PartitionField> fields, List<HoglakeColumnHandle> columns, Page page, int position)
    {
        List<String> result = new ArrayList<>();
        for (var field : fields) {
            var chain = find(columns, field.sourceFieldId());
            Block block = page.getBlock(columns.indexOf(chain.getFirst()));
            int offset = position;
            for (int depth = 1; depth < chain.size() && !block.isNull(offset); depth++) {
                var parent = chain.get(depth - 1);
                var row = ((RowType) parent.type()).getObject(block, offset);
                block = row.getRawFieldBlock(parent.children().indexOf(chain.get(depth)));
                offset = row.getRawIndex();
            }
            result.add(block.isNull(offset) ? null : value(field, chain.getLast(), block, offset));
        }
        return Collections.unmodifiableList(result);
    }

    static String value(HoglakeDtos.PartitionField field, HoglakeColumnHandle column, Block block, int position)
    {
        String type = column.hoglakeType();
        if (field.transform().equals("bucket")) {
            byte[] encoded = switch (type) {
                case "string", "binary" -> column.type().getSlice(block, position).getBytes();
                case "uuid" -> {
                    var uuid = trinoUuidToJavaUuid(column.type().getSlice(block, position));
                    yield ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
                }
                case "decimal" -> unscaled(column, block, position).toByteArray();
                default -> ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(numeric(column, block, position)).array();
            };
            return Integer.toString((Hashing.murmur3_32_fixed().hashBytes(encoded).asInt() & Integer.MAX_VALUE) % field.transformParam());
        }
        if (!field.transform().equals("identity")) {
            long days = type.equals("date") ? column.type().getLong(block, position) : floorDiv(numeric(column, block, position), 86_400_000_000L);
            return switch (field.transform()) {
                case "day" -> Long.toString(days);
                case "hour" -> Long.toString(floorDiv(numeric(column, block, position), 3_600_000_000L));
                case "year" -> Long.toString((long) LocalDate.ofEpochDay(days).getYear() - 1970);
                case "month" -> {
                    LocalDate date = LocalDate.ofEpochDay(days);
                    yield Long.toString(((long) date.getYear() - 1970) * 12 + date.getMonthValue() - 1);
                }
                default -> throw new IllegalArgumentException("Unexpected transform");
            };
        }
        return switch (type) {
            case "boolean" -> Boolean.toString(column.type().getBoolean(block, position));
            case "string", "json" -> column.type().getSlice(block, position).toStringUtf8();
            case "binary" -> Base64.getEncoder().encodeToString(column.type().getSlice(block, position).getBytes());
            case "uuid" -> trinoUuidToJavaUuid(column.type().getSlice(block, position)).toString();
            case "float" -> floating(Float.intBitsToFloat((int) column.type().getLong(block, position)));
            case "double" -> floating(column.type().getDouble(block, position));
            case "decimal" -> new BigDecimal(unscaled(column, block, position), ((DecimalType) column.type()).getScale()).toString();
            case "uint64" -> unscaled(column, block, position).toString();
            case "date" -> isoDate(LocalDate.ofEpochDay(column.type().getLong(block, position)));
            case "time" -> isoTime(LocalTime.ofNanoOfDay(column.type().getLong(block, position) / 1000));
            case "timestamp_ns" -> {
                LongTimestamp timestamp = (LongTimestamp) column.type().getObject(block, position);
                yield BigInteger.valueOf(timestamp.getEpochMicros()).multiply(BigInteger.valueOf(1000)).add(BigInteger.valueOf(timestamp.getPicosOfMicro() / 1000)).toString();
            }
            case "timestamp_s", "timestamp_ms", "timestamp", "timestamptz" -> {
                long micros = numeric(column, block, position);
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(floorDiv(micros, 1_000_000), (int) floorMod(micros, 1_000_000) * 1000, ZoneOffset.UTC);
                isoDate(timestamp.toLocalDate());
                yield timestamp.format(DATE_TIME) + fraction(timestamp.getNano()) + (type.equals("timestamptz") ? "+00:00" : "");
            }
            default -> Long.toString(column.type().getLong(block, position));
        };
    }

    private static long numeric(HoglakeColumnHandle column, Block block, int position)
    {
        return switch (column.hoglakeType()) {
            case "time" -> column.type().getLong(block, position) / 1_000_000;
            case "timestamp_ns" -> ((LongTimestamp) column.type().getObject(block, position)).getEpochMicros();
            case "timestamptz" -> {
                var timestamp = (LongTimestampWithTimeZone) column.type().getObject(block, position);
                yield Math.addExact(Math.multiplyExact(timestamp.getEpochMillis(), 1000), timestamp.getPicosOfMilli() / 1_000_000);
            }
            default -> column.type().getLong(block, position);
        };
    }

    private static BigInteger unscaled(HoglakeColumnHandle column, Block block, int position)
    {
        return ((DecimalType) column.type()).isShort() ? BigInteger.valueOf(column.type().getLong(block, position)) : ((Int128) column.type().getObject(block, position)).toBigInteger();
    }

    private static String isoDate(LocalDate date)
    {
        if (date.getYear() < 1 || date.getYear() > 9999) {
            throw new TrinoException(NOT_SUPPORTED, "Identity date partition requires year 1 through 9999 for cross-client compatibility");
        }
        return date.toString();
    }

    private static String isoTime(LocalTime time)
    {
        return time.format(TIME) + fraction(time.getNano());
    }

    private static String fraction(int nanos)
    {
        return nanos == 0 ? "" : String.format(Locale.ROOT, ".%06d", nanos / 1000);
    }

    private static String floating(double value)
    {
        if (Double.isNaN(value)) {
            return "nan";
        }
        if (Double.isInfinite(value)) {
            return value < 0 ? "-inf" : "inf";
        }
        if (value == 0) {
            return Double.toString(value);
        }
        BigDecimal decimal = BigDecimal.valueOf(value).stripTrailingZeros();
        // Java's spelling may keep two significant digits where Python's
        // canonical partition spelling uses one (notably the smallest subnormal).
        BigDecimal oneDigit = new BigDecimal(value).round(new java.math.MathContext(1));
        if (oneDigit.doubleValue() == value) {
            decimal = oneDigit.stripTrailingZeros();
        }
        int exponent = decimal.precision() - decimal.scale() - 1;
        if (exponent >= -4 && exponent < 16) {
            String plain = decimal.toPlainString();
            return plain.contains(".") ? plain : plain + ".0";
        }
        String digits = decimal.unscaledValue().abs().toString();
        return (value < 0 ? "-" : "") + digits.charAt(0) + (digits.length() == 1 ? "" : "." + digits.substring(1)) +
                "e" + (exponent < 0 ? "-" : "+") + String.format(Locale.ROOT, "%02d", Math.abs(exponent));
    }
}
