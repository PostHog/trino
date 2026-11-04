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
import io.trino.spi.type.SqlDecimal;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestClickHouseJsonFunctions
{
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().build();

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new PostHogFunctionsPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testMatchesClickHouse()
            throws IOException
    {
        // Each line is a ClickHouse type, a path, the hex of a UTF-8 document, and ClickHouse 26.9's
        // JSONExtractKeysAndValues of it as [key, value] pairs, with 64-bit integers and non-finite
        // floats quoted.
        List<String> lines;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                requireNonNull(getClass().getResourceAsStream("/json-keys-and-values-clickhouse.tsv")), UTF_8))) {
            lines = reader.lines().toList();
        }
        assertThat(lines).hasSizeGreaterThan(900);

        List<String> mismatches = new ArrayList<>();
        for (String line : lines) {
            String[] fields = line.split("\t", -1);
            String type = fields[0];
            String document = "from_utf8(from_hex('" + fields[2] + "'))";
            List<?> actual = (List<?>) assertions.execute("SELECT " + call(type, document, fields[1])).getOnlyValue();
            List<List<Object>> expected = new ArrayList<>();
            for (List<String> pair : parsePairs(fields[3])) {
                expected.add(Arrays.asList(pair.get(0), expectedValue(type, pair.get(1))));
            }
            if (!expected.equals(normalize(actual))) {
                mismatches.add("%s %s %s: expected %s but was %s".formatted(
                        type, fields[1], new String(HexFormat.of().parseHex(fields[2]), UTF_8), expected, actual));
            }
        }
        assertThat(mismatches).isEmpty();
    }

    private static List<List<Object>> normalize(List<?> actual)
    {
        List<List<Object>> pairs = new ArrayList<>();
        for (Object pair : actual) {
            List<?> fields = (List<?>) pair;
            Object value = fields.get(1) instanceof SqlDecimal decimal ? decimal.toBigDecimal() : fields.get(1);
            pairs.add(Arrays.asList(fields.get(0), value));
        }
        return pairs;
    }

    @Test
    public void testNull()
    {
        assertThat(assertions.execute("SELECT ch_json_extract_keys_and_values_float64(CAST(NULL AS varchar), '[]', false)").getOnlyValue())
                .isNull();
    }

    @Test
    public void testInvalidPath()
    {
        assertThatThrownBy(() -> assertions.execute("SELECT ch_json_extract_keys_and_values_float64('{}', '[1.5]', false)"))
                .hasMessageContaining("Path must be a JSON array of keys and indexes");
        assertThatThrownBy(() -> assertions.execute("SELECT ch_json_extract_keys_and_values_float64('{}', 'a', false)"))
                .hasMessageContaining("Path must be a JSON array of keys and indexes");
    }

    private static String call(String type, String document, String path)
    {
        boolean nullable = type.startsWith("Nullable(");
        String base = nullable ? type.substring("Nullable(".length(), type.length() - 1) : type;
        String arguments = document + ", '" + path + "', " + nullable;
        return switch (base) {
            case "Float64" -> "ch_json_extract_keys_and_values_float64(" + arguments + ")";
            case "Float32", "Float" -> "ch_json_extract_keys_and_values_float32(" + arguments + ")";
            case "Int8" -> "ch_json_extract_keys_and_values_int(" + arguments + ", -128, 127)";
            case "Int" -> "ch_json_extract_keys_and_values_int(" + arguments + ", -2147483648, 2147483647)";
            case "Int64" -> "ch_json_extract_keys_and_values_int(" + arguments + ", -9223372036854775807 - 1, 9223372036854775807)";
            case "UInt8" -> "ch_json_extract_keys_and_values_int(" + arguments + ", 0, 255)";
            case "UInt32" -> "ch_json_extract_keys_and_values_int(" + arguments + ", 0, 4294967295)";
            case "UInt64" -> "ch_json_extract_keys_and_values_uint64(" + arguments + ")";
            case "Bool" -> "ch_json_extract_keys_and_values_bool(" + arguments + ")";
            case "String" -> "ch_json_extract_keys_and_values_string(" + arguments + ")";
            default -> throw new IllegalArgumentException(type);
        };
    }

    private static Object expectedValue(String type, String text)
    {
        if (text == null) {
            return null;
        }
        String base = type.startsWith("Nullable(") ? type.substring("Nullable(".length(), type.length() - 1) : type;
        return switch (base) {
            case "Float64" -> parseDouble(text);
            case "Float32", "Float" -> (float) parseDouble(text);
            case "UInt64" -> new BigDecimal(text);
            case "Bool" -> Boolean.parseBoolean(text);
            case "String" -> text;
            default -> Long.parseLong(text);
        };
    }

    private static double parseDouble(String text)
    {
        return switch (text) {
            case "nan", "-nan" -> Double.NaN;
            case "inf" -> Double.POSITIVE_INFINITY;
            case "-inf" -> Double.NEGATIVE_INFINITY;
            default -> Double.parseDouble(text);
        };
    }

    // Reads [[key, value], ...] keeping each value's text, so that numbers are not rounded on the way.
    private static List<List<String>> parsePairs(String json)
            throws IOException
    {
        List<List<String>> pairs = new ArrayList<>();
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            parser.nextToken();
            while (parser.nextToken() == JsonToken.START_ARRAY) {
                parser.nextToken();
                String key = parser.getText();
                JsonToken token = parser.nextToken();
                List<String> pair = new ArrayList<>();
                pair.add(key);
                pair.add(token == JsonToken.VALUE_NULL ? null : parser.getText());
                pairs.add(pair);
                parser.nextToken();
            }
        }
        return pairs;
    }
}
