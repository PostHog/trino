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
package io.trino.hogql;

import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot.ExchangeRate;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.type.Int128;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.testing.TestingResponse.contentType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.SqlDecimal.decimal;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHogQlExchangeRateConversionEngine
{
    private static final LocalDate FIRST_DATE = LocalDate.parse("2024-01-01");
    private static final LocalDate SECOND_DATE = LocalDate.parse("2024-01-10");

    private final HogQlExchangeRateConversionEngine engine = new HogQlExchangeRateConversionEngine(new HogQlExchangeRateSnapshot(
            1,
            1,
            17,
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
        assertThat(engine.rate("EUR", FIRST_DATE.minusDays(1))).isEmpty();
        assertThat(engine.rate("EUR", FIRST_DATE)).contains(new BigDecimal("0.9049000000"));
        assertThat(engine.rate("EUR", SECOND_DATE.minusDays(1))).contains(new BigDecimal("0.9049000000"));
        assertThat(engine.rate("EUR", SECOND_DATE.plusYears(1))).contains(new BigDecimal("0.9500000000"));
    }

    @Test
    void testConversionMatchesPostHogDecimalSemantics()
    {
        assertDecimal(engine.convert("USD", "EUR", new BigDecimal("100"), FIRST_DATE), "90.4900000000");
        assertDecimal(engine.convert("EUR", "USD", new BigDecimal("90.49"), FIRST_DATE), "100.0000000000");
        assertDecimal(engine.convert("AAA", "AAA", new BigDecimal("1.23456789019"), FIRST_DATE), "1.2345678901");
        assertDecimal(engine.convert("USD", "JPY", new BigDecimal("-2.5"), FIRST_DATE), "-270.3085000000");
    }

    @Test
    void testMissingOrZeroRateReturnsZero()
    {
        assertDecimal(engine.convert("MISSING", "EUR", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert("USD", "MISSING", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert("ZZZ", "EUR", BigDecimal.TEN, FIRST_DATE), "0.0000000000");
        assertDecimal(engine.convert("EUR", "USD", BigDecimal.TEN, FIRST_DATE.minusDays(1)), "0.0000000000");
    }

    @Test
    void testRuntimeStateAndCompilerPinsReuseCachedGeneration()
    {
        String snapshot = """
                {"protocolVersion":1,"schemaVersion":1,"generation":17,"baseCurrency":"USD","decimalScale":10,"rates":[
                {"currency":"EUR","effectiveDate":"2024-01-01","unscaledRate":"9049000000"},
                {"currency":"USD","effectiveDate":"2024-01-01","unscaledRate":"10000000000"}]}
                """;
        AtomicInteger requests = new AtomicInteger();
        TestingHttpClient client = new TestingHttpClient(_ -> {
            requests.incrementAndGet();
            return new TestingResponse(
                    OK,
                    contentType(JSON_UTF_8),
                    snapshot.getBytes(StandardCharsets.UTF_8));
        });
        HogQlExchangeRateHttpTransport transport = new HogQlExchangeRateHttpTransport(
                URI.create("https://duckgres.example/"),
                client,
                () -> "test-token");
        HogQlExchangeRateManager manager = new HogQlExchangeRateManager(
                new HogQlSemanticCatalogConfig(),
                transport,
                new HogQlExchangeRateSnapshotJsonDecoder());
        try {
            manager.pin(OptionalLong.empty());
            manager.pin(OptionalLong.empty());
            HogQlExchangeRateFunction.State state = new HogQlExchangeRateFunction.State(manager);
            Int128 result = state.convert(
                    17,
                    utf8Slice("USD"),
                    utf8Slice("EUR"),
                    encodeScaledValue(new BigDecimal("100"), 10),
                    FIRST_DATE.toEpochDay());

            assertDecimal(new BigDecimal(result.toBigInteger(), 10), "90.4900000000");
            assertThat(requests).hasValue(1);
        }
        finally {
            manager.shutdown();
        }
    }

    @Test
    void testRegisteredScalarFunctionExecutesPinnedConversion()
    {
        String snapshot = """
                {"protocolVersion":1,"schemaVersion":1,"generation":17,"baseCurrency":"USD","decimalScale":10,"rates":[
                {"currency":"EUR","effectiveDate":"2024-01-01","unscaledRate":"9049000000"},
                {"currency":"USD","effectiveDate":"2024-01-01","unscaledRate":"10000000000"}]}
                """;
        TestingHttpClient client = new TestingHttpClient(_ -> new TestingResponse(
                OK,
                contentType(JSON_UTF_8),
                snapshot.getBytes(StandardCharsets.UTF_8)));
        HogQlExchangeRateManager manager = new HogQlExchangeRateManager(
                new HogQlSemanticCatalogConfig(),
                new HogQlExchangeRateHttpTransport(URI.create("https://duckgres.example/"), client, () -> "test-token"),
                new HogQlExchangeRateSnapshotJsonDecoder());
        try {
            manager.pin(OptionalLong.empty());
            try (QueryAssertions assertions = new QueryAssertions()) {
                assertions.addFunctions(new InternalFunctionBundle(new HogQlExchangeRateFunction(manager)));

                assertThat(assertions.expression(
                        "hogql_convert_currency(17, 'USD', 'EUR', DECIMAL '100.0000000000', DATE '2024-01-01')"))
                        .hasType(createDecimalType(38, 10))
                        .isEqualTo(decimal("90.4900000000", createDecimalType(38, 10)));
            }
        }
        finally {
            manager.shutdown();
        }
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
