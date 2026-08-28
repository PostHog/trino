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
package io.trino.plugin.ducklake.function.currency;

import io.trino.plugin.ducklake.function.currency.ExchangeRateSnapshot.ExchangeRate;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestExchangeRateConversionEngine
{
    private static final long GENERATION = 17;
    private static final LocalDate FIRST_DATE = LocalDate.parse("2024-01-01");
    private static final LocalDate SECOND_DATE = LocalDate.parse("2024-01-10");

    private final ExchangeRateConversionEngine engine = new ExchangeRateConversionEngine(new ExchangeRateSnapshot(
            1,
            1,
            GENERATION,
            "USD",
            10,
            List.of(
                    rate("EUR", FIRST_DATE, "0.9049"),
                    rate("EUR", SECOND_DATE, "0.95"),
                    rate("JPY", FIRST_DATE, "108.1234"),
                    rate("USD", FIRST_DATE, "1"),
                    rate("ZZZ", FIRST_DATE, "0"))));

    @Test
    void testFloorLookupUsesLatestRateNotAfterDate()
    {
        assertThat(engine.rate(GENERATION, "EUR", FIRST_DATE.minusDays(1))).isEmpty();
        assertThat(engine.rate(GENERATION, "EUR", FIRST_DATE)).contains(new BigDecimal("0.9049000000"));
        assertThat(engine.rate(GENERATION, "EUR", SECOND_DATE.minusDays(1))).contains(new BigDecimal("0.9049000000"));
        assertThat(engine.rate(GENERATION, "EUR", SECOND_DATE.plusYears(1))).contains(new BigDecimal("0.9500000000"));
    }

    @Test
    void testConversionMatchesPostHogDecimalSemantics()
    {
        assertDecimal(engine.convert(GENERATION, "USD", "EUR", new BigDecimal("100"), FIRST_DATE), "90.4900000000");
        assertDecimal(engine.convert(GENERATION, "EUR", "USD", new BigDecimal("90.49"), FIRST_DATE), "100.0000000000");
        assertDecimal(engine.convert(GENERATION, "AAA", "AAA", new BigDecimal("1.23456789019"), FIRST_DATE), "1.2345678901");
        assertDecimal(engine.convert(GENERATION, "USD", "JPY", new BigDecimal("-2.5"), FIRST_DATE), "-270.3085000000");
    }

    @Test
    void testDivisionRoundsToScaleBeforeMultiplication()
    {
        ExchangeRateConversionEngine roundingEngine = new ExchangeRateConversionEngine(new ExchangeRateSnapshot(
                1,
                1,
                GENERATION,
                "USD",
                10,
                List.of(rate("AAA", FIRST_DATE, "3"), rate("BBB", FIRST_DATE, "2"), rate("USD", FIRST_DATE, "1"))));

        assertDecimal(roundingEngine.convert(GENERATION, "AAA", "BBB", BigDecimal.ONE, FIRST_DATE), "0.6666666666");
    }

    @Test
    void testMissingOrZeroRateReturnsZero()
    {
        assertDecimal(engine.convert(GENERATION, "MISSING", "EUR", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert(GENERATION, "USD", "MISSING", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert(GENERATION, "ZZZ", "EUR", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert(GENERATION, "USD", "ZZZ", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert(GENERATION, "EUR", "USD", BigDecimal.TEN, FIRST_DATE.minusDays(1)), "0.0000000000");
    }

    @Test
    void testRejectsAnotherGeneration()
    {
        assertThatThrownBy(() -> engine.convert(GENERATION + 1, "USD", "EUR", BigDecimal.TEN, FIRST_DATE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("exchange-rate snapshot generation mismatch");
        assertThatThrownBy(() -> engine.rate(GENERATION + 1, "EUR", FIRST_DATE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("exchange-rate snapshot generation mismatch");
    }

    private static ExchangeRate rate(String currency, LocalDate date, String value)
    {
        return new ExchangeRate(currency, date.toString(), new BigDecimal(value).movePointRight(10).toBigIntegerExact().toString());
    }

    private static void assertDecimal(BigDecimal actual, String expected)
    {
        assertThat(actual.scale()).isEqualTo(10);
        assertThat(actual).isEqualByComparingTo(expected);
    }
}
