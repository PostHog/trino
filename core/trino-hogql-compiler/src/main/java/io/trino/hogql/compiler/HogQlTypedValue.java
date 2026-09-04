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
package io.trino.hogql.compiler;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record HogQlTypedValue(String type, Value value)
{
    public HogQlTypedValue
    {
        type = requireNonNull(type, "type is null");
        value = requireNonNull(value, "value is null");
        if (type.isBlank()) {
            throw new IllegalArgumentException("type is empty");
        }
    }

    @Override
    public String toString()
    {
        return "HogQlTypedValue[type=%s, value=<redacted>]".formatted(type);
    }

    public sealed interface Value
            permits ArrayValue,
                    BooleanValue,
                    NullValue,
                    NumberValue,
                    ObjectValue,
                    StringValue {}

    public enum NullValue
            implements Value
    {
        NULL;

        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }

    public record BooleanValue(boolean value)
            implements Value
    {
        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }

    public record NumberValue(String value)
            implements Value
    {
        public NumberValue
        {
            value = requireNonNull(value, "number value is null");
            try {
                new BigDecimal(value);
            }
            catch (NumberFormatException _) {
                throw new IllegalArgumentException("number value is invalid");
            }
        }

        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }

    public record StringValue(String value)
            implements Value
    {
        public StringValue
        {
            value = requireNonNull(value, "string value is null");
        }

        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }

    public record ArrayValue(List<Value> value)
            implements Value
    {
        public ArrayValue
        {
            requireNonNull(value, "array value is null");
            if (value.stream().anyMatch(element -> element == null)) {
                throw new IllegalArgumentException("array value contains a missing element");
            }
            value = List.copyOf(value);
        }

        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }

    public record ObjectValue(Map<String, Value> value)
            implements Value
    {
        public ObjectValue
        {
            requireNonNull(value, "object value is null");
            for (Map.Entry<String, Value> entry : value.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) {
                    throw new IllegalArgumentException("object value contains an empty field name");
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException("object value contains a missing field value");
                }
            }
            value = Map.copyOf(value);
        }

        @Override
        public String toString()
        {
            return "<redacted>";
        }
    }
}
