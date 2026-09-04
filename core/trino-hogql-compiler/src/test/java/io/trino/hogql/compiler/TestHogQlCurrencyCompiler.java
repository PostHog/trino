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

import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot.ExchangeRate;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotProvider;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlCurrencyCompiler
{
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testPinsOneGenerationAndLowersAllCurrencyCalls()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlExchangeRateSnapshotProvider provider = expectedGeneration -> {
            assertThat(expectedGeneration).isEmpty();
            pins.incrementAndGet();
            return new HogQlExchangeRateSnapshotProvider.PinnedSnapshot(snapshot(42));
        };

        HogQlCompilationResult result = new HogQlCompiler(provider).compile(
                "SELECT convertCurrency('USD', 'EUR', 100), " +
                        "convertCurrency(source_currency, target_currency, amount, event_date), " +
                        "convertCurrency('USD', 'EUR', 100, _toDate('2024-01-01')) FROM events",
                java.util.Map.of());

        assertThat(pins).hasValue(1);
        assertThat(result.exchangeRateGeneration()).hasValue(42);
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT " +
                        "hogql_convert_currency(42, CAST('USD' AS varchar), CAST('EUR' AS varchar), CAST(100 AS decimal(38, 10)), CAST(now() AS date)), " +
                        "hogql_convert_currency(42, CAST(source_currency AS varchar), CAST(target_currency AS varchar), CAST(amount AS decimal(38, 10)), CAST(event_date AS date)), " +
                        "hogql_convert_currency(42, CAST('USD' AS varchar), CAST('EUR' AS varchar), CAST(100 AS decimal(38, 10)), CAST(CAST('2024-01-01' AS date) AS date)) " +
                        "FROM events"));
    }

    @Test
    public void testFailsClosedWithoutExchangeRateProvider()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compile("SELECT convertCurrency('USD', 'EUR', 100)"));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception).hasMessage("line 1:8: HogQL function convertCurrency requires an exchange-rate snapshot");
    }

    private static HogQlExchangeRateSnapshot snapshot(long generation)
    {
        return new HogQlExchangeRateSnapshot(
                HogQlExchangeRateSnapshot.PROTOCOL_VERSION,
                HogQlExchangeRateSnapshot.SCHEMA_VERSION,
                generation,
                HogQlExchangeRateSnapshot.BASE_CURRENCY,
                HogQlExchangeRateSnapshot.DECIMAL_SCALE,
                List.of(new ExchangeRate("USD", "1970-01-01", "10000000000")));
    }
}
