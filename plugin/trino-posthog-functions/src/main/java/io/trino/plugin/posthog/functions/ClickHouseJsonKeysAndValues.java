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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;

/**
 * ClickHouse's {@code JSONExtractKeysAndValues}: the members of a JSON object, in document order and
 * with duplicate keys kept, with each value converted the way ClickHouse converts it.
 */
final class ClickHouseJsonKeysAndValues
{
    enum Target
    {
        FLOAT64(DOUBLE),
        FLOAT32(REAL),
        INTEGER(BIGINT),
        UINT64(createDecimalType(20, 0)),
        BOOL(BOOLEAN),
        STRING(VARCHAR);

        private final ArrayType resultType;

        Target(Type valueType)
        {
            this.resultType = new ArrayType(RowType.anonymous(List.of(VARCHAR, valueType)));
        }
    }

    // ClickHouse rejects a document nested 1024 levels deep.
    private static final int MAX_DEPTH = 1023;

    private static final JsonFactory JSON_FACTORY = JsonFactory.builder()
            .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
            .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNestingDepth(MAX_DEPTH + 1)
                    .maxNumberLength(Integer.MAX_VALUE)
                    .maxStringLength(Integer.MAX_VALUE)
                    .build())
            .build();

    private ClickHouseJsonKeysAndValues() {}

    static Block extract(Slice json, Slice path, boolean keepNulls, Target target, long low, long high)
    {
        Object[] steps = parsePath(path);
        Entries entries = new Entries(target, keepNulls, low, high);
        try {
            if (!extractEntries(json, resolveNegativeSteps(json, steps), entries)) {
                entries.clear();
            }
        }
        catch (IOException _) {
            // Jackson reports malformed JSON as exceptions; ClickHouse returns no entries for it.
            entries.clear();
        }
        return entries.build();
    }

    private static boolean extractEntries(Slice json, Object[] steps, Entries entries)
            throws IOException
    {
        try (Reader reader = new Reader(json)) {
            if (reader.next() == null) {
                return false;
            }
            for (Object step : steps) {
                if (!reader.descend(step)) {
                    return false;
                }
            }
            if (reader.token() != JsonToken.START_OBJECT) {
                return false;
            }
            while (reader.next() == JsonToken.FIELD_NAME) {
                Slice key = Slices.utf8Slice(reader.parser.currentName());
                reader.next();
                entries.add(key, reader);
            }
            return reader.finish();
        }
    }

    // A negative index counts from the end, so it needs the size of its container first.
    private static Object[] resolveNegativeSteps(Slice json, Object[] steps)
            throws IOException
    {
        Object[] resolved = steps;
        for (int i = 0; i < steps.length; i++) {
            if (steps[i] instanceof Long index && index < 0) {
                if (resolved == steps) {
                    resolved = steps.clone();
                }
                long size = containerSize(json, Arrays.copyOf(resolved, i));
                resolved[i] = size + index + 1;
            }
        }
        return resolved;
    }

    private static long containerSize(Slice json, Object[] steps)
            throws IOException
    {
        try (Reader reader = new Reader(json)) {
            if (reader.next() == null) {
                return 0;
            }
            for (Object step : steps) {
                if (!reader.descend(step)) {
                    return 0;
                }
            }
            return reader.countChildren();
        }
    }

    private static Object[] parsePath(Slice path)
    {
        try (JsonParser parser = JSON_FACTORY.createParser(path.byteArray(), path.byteArrayOffset(), path.length())) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw invalidPath(path);
            }
            List<Object> steps = new ArrayList<>();
            for (JsonToken token = parser.nextToken(); token != JsonToken.END_ARRAY; token = parser.nextToken()) {
                switch (token) {
                    case VALUE_STRING -> steps.add(parser.getText());
                    case VALUE_NUMBER_INT -> steps.add(parser.getLongValue());
                    case null, default -> throw invalidPath(path);
                }
            }
            return steps.toArray();
        }
        catch (IOException e) {
            throw invalidPath(path);
        }
    }

    private static TrinoException invalidPath(Slice path)
    {
        return new TrinoException(INVALID_FUNCTION_ARGUMENT, "Path must be a JSON array of keys and indexes: " + path.toStringUtf8());
    }

    private static final class Reader
            implements AutoCloseable
    {
        private final JsonParser parser;
        private int depth;
        private boolean valid = true;

        Reader(Slice json)
                throws IOException
        {
            parser = JSON_FACTORY.createParser(json.byteArray(), json.byteArrayOffset(), json.length());
        }

        JsonToken token()
        {
            return parser.currentToken();
        }

        // ClickHouse rejects the whole document for a float that overflows, a lone surrogate escape,
        // or nesting of 1024 levels, which Jackson accepts.
        JsonToken next()
                throws IOException
        {
            JsonToken token = parser.nextToken();
            if (token == null) {
                return null;
            }
            switch (token) {
                case START_OBJECT, START_ARRAY -> {
                    depth++;
                    if (depth > MAX_DEPTH) {
                        valid = false;
                    }
                }
                case END_OBJECT, END_ARRAY -> depth--;
                case VALUE_NUMBER_FLOAT -> {
                    if (Double.isInfinite(parser.getDoubleValue())) {
                        valid = false;
                    }
                }
                case VALUE_STRING, FIELD_NAME -> {
                    if (hasLoneSurrogate(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength())) {
                        valid = false;
                    }
                }
                default -> {}
            }
            return token;
        }

        void skipValue()
                throws IOException
        {
            if (!token().isStructStart()) {
                return;
            }
            int target = depth - 1;
            while (depth > target) {
                next();
            }
        }

        boolean descend(Object step)
                throws IOException
        {
            JsonToken container = token();
            if (step instanceof String key) {
                if (container != JsonToken.START_OBJECT) {
                    return false;
                }
                while (next() == JsonToken.FIELD_NAME) {
                    String name = parser.currentName();
                    next();
                    if (name.equals(key)) {
                        return true;
                    }
                    skipValue();
                }
                return false;
            }
            long index = (Long) step;
            if (index <= 0 || !container.isStructStart()) {
                return false;
            }
            long position = 0;
            while (true) {
                JsonToken token = next();
                if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                    return false;
                }
                if (token == JsonToken.FIELD_NAME) {
                    next();
                }
                position++;
                if (position == index) {
                    return true;
                }
                skipValue();
            }
        }

        long countChildren()
                throws IOException
        {
            if (!token().isStructStart()) {
                return 0;
            }
            long count = 0;
            while (true) {
                JsonToken token = next();
                if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                    return count;
                }
                if (token == JsonToken.FIELD_NAME) {
                    next();
                }
                count++;
                skipValue();
            }
        }

        // Reads to the end of the document, so that a problem after the object still rejects it.
        boolean finish()
                throws IOException
        {
            while (depth > 0) {
                next();
            }
            return parser.nextToken() == null && valid;
        }

        @Override
        public void close()
                throws IOException
        {
            parser.close();
        }
    }

    private static boolean hasLoneSurrogate(char[] chars, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++) {
            char c = chars[i];
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= offset + length || !Character.isLowSurrogate(chars[i + 1])) {
                    return true;
                }
                i++;
            }
            else if (Character.isLowSurrogate(c)) {
                return true;
            }
        }
        return false;
    }

    private static final class Entries
    {
        private final Target target;
        private final boolean keepNulls;
        private final long low;
        private final long high;
        private final List<Slice> keys = new ArrayList<>();
        private final List<Slice> strings = new ArrayList<>();
        private long[] values = new long[8];
        private boolean[] nulls = new boolean[8];

        Entries(Target target, boolean keepNulls, long low, long high)
        {
            this.target = target;
            this.keepNulls = keepNulls;
            this.low = low;
            this.high = high;
        }

        void add(Slice key, Reader reader)
                throws IOException
        {
            JsonParser parser = reader.parser;
            JsonToken token = reader.token();
            if (token == JsonToken.VALUE_NULL) {
                if (keepNulls) {
                    append(key, 0, null, true);
                }
                return;
            }
            if (target == Target.STRING) {
                String text = switch (token) {
                    case VALUE_STRING -> parser.getText();
                    case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> ClickHouseJsonText.number(parser.getText(), token == JsonToken.VALUE_NUMBER_INT, false);
                    case VALUE_TRUE -> "true";
                    case VALUE_FALSE -> "false";
                    default -> serialize(reader);
                };
                append(key, 0, Slices.utf8Slice(text), false);
                return;
            }
            // A boolean converts as the JSON integer 1 or 0.
            String text = switch (token) {
                case VALUE_STRING, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> parser.getText();
                case VALUE_TRUE -> "1";
                case VALUE_FALSE -> "0";
                default -> null;
            };
            if (text == null) {
                reader.skipValue();
                return;
            }
            boolean isString = token == JsonToken.VALUE_STRING;
            boolean isInteger = token != JsonToken.VALUE_NUMBER_FLOAT;
            switch (target) {
                case FLOAT64 -> {
                    Double value = isString ? ClickHouseNumbers.float64FromString(text) : Double.valueOf(ClickHouseNumbers.float64FromNumber(text, isInteger));
                    if (value != null) {
                        append(key, Double.doubleToRawLongBits(value), null, false);
                    }
                }
                case FLOAT32 -> {
                    Float value = isString ? ClickHouseNumbers.float32FromString(text) : Float.valueOf((float) ClickHouseNumbers.float64FromNumber(text, isInteger));
                    if (value != null) {
                        append(key, floatToRawIntBits(value), null, false);
                    }
                }
                case INTEGER -> appendInteger(key, isString ? ClickHouseNumbers.integerFromString(text, low, high) : ClickHouseNumbers.integerFromNumber(text, isInteger, low, high));
                case UINT64 -> appendInteger(key, isString ? ClickHouseNumbers.uint64FromString(text) : ClickHouseNumbers.uint64FromNumber(text, isInteger));
                case BOOL -> {
                    Long value = isString ? ClickHouseNumbers.integerFromString(text, 0, 255) : ClickHouseNumbers.integerFromNumber(text, isInteger, 0, 255);
                    if (value != null) {
                        append(key, value != 0 ? 1 : 0, null, false);
                    }
                }
                case STRING -> throw new IllegalStateException();
            }
        }

        private void appendInteger(Slice key, Long value)
        {
            if (value != null) {
                append(key, value, null, false);
            }
        }

        private static String serialize(Reader reader)
                throws IOException
        {
            StringBuilder out = new StringBuilder();
            JsonParser parser = reader.parser;
            JsonToken token = reader.token();
            int depth = 0;
            boolean needsComma = false;
            while (true) {
                if (needsComma && token != JsonToken.END_OBJECT && token != JsonToken.END_ARRAY) {
                    out.append(',');
                }
                needsComma = true;
                switch (token) {
                    case START_OBJECT -> {
                        out.append('{');
                        depth++;
                        needsComma = false;
                    }
                    case START_ARRAY -> {
                        out.append('[');
                        depth++;
                        needsComma = false;
                    }
                    case END_OBJECT -> {
                        out.append('}');
                        depth--;
                    }
                    case END_ARRAY -> {
                        out.append(']');
                        depth--;
                    }
                    case FIELD_NAME -> {
                        ClickHouseJsonText.appendString(out, parser.currentName());
                        out.append(':');
                        needsComma = false;
                    }
                    case VALUE_STRING -> ClickHouseJsonText.appendString(out, parser.getText());
                    case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> out.append(ClickHouseJsonText.number(parser.getText(), token == JsonToken.VALUE_NUMBER_INT, true));
                    case VALUE_TRUE -> out.append("true");
                    case VALUE_FALSE -> out.append("false");
                    case VALUE_NULL -> out.append("null");
                    default -> throw new IllegalStateException("Unexpected token " + token);
                }
                if (depth == 0) {
                    return out.toString();
                }
                token = reader.next();
            }
        }

        private void append(Slice key, long value, Slice string, boolean isNull)
        {
            int size = keys.size();
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
                nulls = Arrays.copyOf(nulls, size * 2);
            }
            keys.add(key);
            values[size] = value;
            nulls[size] = isNull;
            if (target == Target.STRING) {
                strings.add(string);
            }
        }

        void clear()
        {
            keys.clear();
            strings.clear();
        }

        Block build()
        {
            int size = keys.size();
            return buildArrayValue(target.resultType, size, elementBuilder -> {
                for (int i = 0; i < size; i++) {
                    int index = i;
                    ((RowBlockBuilder) elementBuilder).buildEntry(fields -> {
                        VARCHAR.writeSlice(fields.get(0), keys.get(index));
                        writeValue(fields.get(1), index);
                    });
                }
            });
        }

        private void writeValue(BlockBuilder builder, int index)
        {
            if (nulls[index]) {
                builder.appendNull();
                return;
            }
            long value = values[index];
            switch (target) {
                case FLOAT64 -> DOUBLE.writeDouble(builder, Double.longBitsToDouble(value));
                case FLOAT32 -> REAL.writeLong(builder, value);
                case INTEGER -> BIGINT.writeLong(builder, value);
                case UINT64 -> createDecimalType(20, 0).writeObject(builder, Int128.valueOf(0, value));
                case BOOL -> BOOLEAN.writeBoolean(builder, value != 0);
                case STRING -> VARCHAR.writeSlice(builder, strings.get(index));
            }
        }
    }
}
