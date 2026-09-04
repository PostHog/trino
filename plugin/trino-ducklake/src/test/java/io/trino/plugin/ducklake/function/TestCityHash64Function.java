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
import io.trino.spi.type.DecimalType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestCityHash64Function
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
        Map<String, String> expectedHashes = Map.ofEntries(
                Map.entry("", "11160318154034397263"),
                Map.entry("a", "2603192927274642682"),
                Map.entry("abc", "4220206313085259313"),
                Map.entry("abcd", "17823623939509273229"),
                Map.entry("12345678", "7177601938557627951"),
                Map.entry("123456789", "12390271160407166709"),
                Map.entry("1234567890abcdef", "10283158570132023530"),
                Map.entry("1234567890abcdefg", "4637769396780685212"),
                Map.entry("1234567890abcdef1234567890abcdef", "5983709264297605835"),
                Map.entry("1234567890abcdef1234567890abcdefg", "1434880753424560409"),
                Map.entry("x".repeat(64), "6437053381938498259"),
                Map.entry("x".repeat(65), "5260653789997849295"),
                Map.entry("x".repeat(128), "1142585757146322653"),
                Map.entry("y".repeat(129), "15472941566376925510"),
                Map.entry("z".repeat(200), "14427141409027721128"),
                Map.entry("q".repeat(257), "14786733977854578775"),
                Map.entry("Moscow", "12507901496292878638"),
                Map.entry("Grüße", "17364701079286602353"));

        expectedHashes.forEach((input, expected) -> assertThat(ClickHouseCityHash64.hash(Slices.utf8Slice(input)))
                .isEqualTo(Long.parseUnsignedLong(expected)));
    }

    @Test
    void testFunctionRegistrationAndUnsignedResult()
    {
        DecimalType resultType = createDecimalType(20, 0);
        assertThat(assertions.function("cityHash64", "'Moscow'"))
                .hasType(resultType)
                .isEqualTo(decimal("12507901496292878638", resultType));

        assertThat(assertions.function("cityHash64", "CAST(NULL AS varchar)"))
                .isNull(resultType);
    }
}
