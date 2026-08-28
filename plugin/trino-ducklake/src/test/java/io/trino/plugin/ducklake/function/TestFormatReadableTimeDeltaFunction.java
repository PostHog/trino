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

import io.airlift.slice.Slices;
import io.trino.plugin.ducklake.DuckLakePlugin;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestFormatReadableTimeDeltaFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new DuckLakePlugin());
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testClickHouseCompatibility()
    {
        Map<String, String> expectedResults = Map.ofEntries(
                Map.entry("5.5", "5 seconds"),
                Map.entry("330.0", "5 minutes and 30 seconds"),
                Map.entry("475200.0", "5 days and 12 hours"),
                Map.entry("CAST(14256000 AS double)", "5 months, 12 days and 12 hours"),
                Map.entry("CAST(173448000 AS double)", "5 years, 5 months and 30 days"),
                Map.entry("-34261261.0", "-1 year, 1 month, 1 day, 1 hour, 1 minute and 1 second"),
                Map.entry("CAST(0 AS double)", "0 seconds"),
                Map.entry("CAST('Infinity' AS double)", "inf"),
                Map.entry("CAST('-Infinity' AS double)", "-inf"),
                Map.entry("nan()", "nan"),
                Map.entry("1e100", "3.1709791983764585e92 years"),
                Map.entry("CAST(1152921504606846976 AS double)", "36558901084 years"));

        expectedResults.forEach((input, expected) -> assertThat(assertions.function("format_readable_time_delta", input))
                .hasType(VARCHAR)
                .isEqualTo(expected));
    }

    @Test
    void testMaximumAndMinimumUnits()
    {
        assertThat(assertions.function("format_readable_time_delta", "475200.0", "'minutes'"))
                .isEqualTo("7920 minutes");
        assertThat(assertions.function("format_readable_time_delta", "67.79797979", "'milliseconds'"))
                .isEqualTo("67797 milliseconds, 979 microseconds and 790 nanoseconds");
        assertThat(assertions.function("format_readable_time_delta", "3601.000000003", "'hours'", "'microseconds'"))
                .isEqualTo("1 hour and 1 second");
        assertThat(assertions.function("format_readable_time_delta", "1.0005", "'milliseconds'"))
                .isEqualTo("1000 milliseconds and 500 microseconds");
        assertThat(assertions.function("format_readable_time_delta", "CAST(0 AS double)", "'milliseconds'"))
                .isEqualTo("0 nanoseconds");
        assertThat(assertions.function("format_readable_time_delta", "CAST(0 AS double)", "'years'", "'years'"))
                .isEqualTo("");
    }

    @Test
    void testInvalidUnits()
    {
        assertThatThrownBy(() -> FormatReadableTimeDeltaFunction.formatReadableTimeDelta(
                1.0,
                Slices.utf8Slice("second")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unexpected value of maximum unit argument");
        assertThatThrownBy(() -> FormatReadableTimeDeltaFunction.formatReadableTimeDelta(
                1.0,
                Slices.utf8Slice("seconds"),
                Slices.utf8Slice("hours")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("must not be greater than maximum unit");
    }

    @Test
    void testNullPropagation()
    {
        assertThat(assertions.function("format_readable_time_delta", "CAST(NULL AS double)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_readable_time_delta", "1.0", "CAST(NULL AS varchar)"))
                .isNull(VARCHAR);
        assertThat(assertions.function("format_readable_time_delta", "1.0", "'hours'", "CAST(NULL AS varchar)"))
                .isNull(VARCHAR);
    }
}
