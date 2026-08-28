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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.function.currency.ExchangeRateSnapshot.ExchangeRate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.plugin.ducklake.function.currency.ExchangeRateSnapshot.DECIMAL_SCALE;
import static java.util.Objects.requireNonNull;

public final class ExchangeRateConversionEngine
{
    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(DECIMAL_SCALE);

    private final long generation;
    private final Map<String, NavigableMap<LocalDate, BigDecimal>> ratesByCurrency;

    public ExchangeRateConversionEngine(ExchangeRateSnapshot snapshot)
    {
        requireNonNull(snapshot, "snapshot is null");
        generation = snapshot.generation();

        Map<String, NavigableMap<LocalDate, BigDecimal>> mutableRates = new HashMap<>();
        for (ExchangeRate rate : snapshot.rates()) {
            mutableRates.computeIfAbsent(rate.currency(), _ -> new TreeMap<>())
                    .put(LocalDate.parse(rate.effectiveDate()), new BigDecimal(new BigInteger(rate.unscaledRate()), DECIMAL_SCALE));
        }
        ImmutableMap.Builder<String, NavigableMap<LocalDate, BigDecimal>> immutableRates = ImmutableMap.builder();
        mutableRates.forEach((currency, rates) -> immutableRates.put(currency, Collections.unmodifiableNavigableMap(rates)));
        ratesByCurrency = immutableRates.buildOrThrow();
    }

    public long generation()
    {
        return generation;
    }

    public Optional<BigDecimal> rate(long expectedGeneration, String currency, LocalDate date)
    {
        verifyGeneration(expectedGeneration);
        requireNonNull(currency, "currency is null");
        requireNonNull(date, "date is null");
        NavigableMap<LocalDate, BigDecimal> currencyRates = ratesByCurrency.get(currency);
        if (currencyRates == null) {
            return Optional.empty();
        }
        Map.Entry<LocalDate, BigDecimal> rate = currencyRates.floorEntry(date);
        return rate == null ? Optional.empty() : Optional.of(rate.getValue());
    }

    public BigDecimal convert(long expectedGeneration, String sourceCurrency, String targetCurrency, BigDecimal amount, LocalDate date)
    {
        verifyGeneration(expectedGeneration);
        requireNonNull(sourceCurrency, "sourceCurrency is null");
        requireNonNull(targetCurrency, "targetCurrency is null");
        requireNonNull(amount, "amount is null");
        requireNonNull(date, "date is null");

        BigDecimal decimalAmount = amount.setScale(DECIMAL_SCALE, RoundingMode.DOWN);
        if (sourceCurrency.equals(targetCurrency)) {
            return decimalAmount;
        }

        BigDecimal sourceRate = rate(expectedGeneration, sourceCurrency, date).orElse(ZERO);
        BigDecimal targetRate = rate(expectedGeneration, targetCurrency, date).orElse(ZERO);
        if (sourceRate.signum() == 0 || targetRate.signum() == 0) {
            return ZERO;
        }

        return decimalAmount
                .divide(sourceRate, DECIMAL_SCALE, RoundingMode.DOWN)
                .multiply(targetRate)
                .setScale(DECIMAL_SCALE, RoundingMode.DOWN);
    }

    private void verifyGeneration(long expectedGeneration)
    {
        if (expectedGeneration != generation) {
            throw new IllegalArgumentException("exchange-rate snapshot generation mismatch");
        }
    }
}
