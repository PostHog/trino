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

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public record ExchangeRateSnapshot(
        int protocolVersion,
        int schemaVersion,
        long generation,
        String baseCurrency,
        int decimalScale,
        List<ExchangeRate> rates)
{
    public static final int PROTOCOL_VERSION = 1;
    public static final int SCHEMA_VERSION = 1;
    public static final String BASE_CURRENCY = "USD";
    public static final int DECIMAL_SCALE = 10;
    public static final int MAXIMUM_RATES = 1_000_000;

    private static final Pattern CURRENCY_PATTERN = Pattern.compile("[A-Z]{3}");
    private static final Pattern UNSCALED_RATE_PATTERN = Pattern.compile("0|[1-9][0-9]{0,17}");
    private static final String UNSCALED_ONE = "10000000000";

    public ExchangeRateSnapshot
    {
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported exchange-rate snapshot protocol");
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported exchange-rate snapshot schema");
        }
        if (generation <= 0) {
            throw new IllegalArgumentException("exchange-rate snapshot generation must be positive");
        }
        baseCurrency = requireNonNull(baseCurrency, "baseCurrency is null");
        if (!baseCurrency.equals(BASE_CURRENCY)) {
            throw new IllegalArgumentException("unsupported exchange-rate base currency");
        }
        if (decimalScale != DECIMAL_SCALE) {
            throw new IllegalArgumentException("unsupported exchange-rate decimal scale");
        }
        rates = List.copyOf(requireNonNull(rates, "rates is null"));
        if (rates.isEmpty()) {
            throw new IllegalArgumentException("exchange-rate snapshot has no rates");
        }
        if (rates.size() > MAXIMUM_RATES) {
            throw new IllegalArgumentException("exchange-rate snapshot exceeds rate limit");
        }

        boolean baseCurrencyPresent = false;
        ExchangeRate previous = null;
        for (ExchangeRate rate : rates) {
            requireNonNull(rate, "rate is null");
            if (rate.currency().equals(BASE_CURRENCY)) {
                baseCurrencyPresent = true;
                if (!rate.unscaledRate().equals(UNSCALED_ONE)) {
                    throw new IllegalArgumentException("base-currency rate must equal one");
                }
            }
            if (previous != null && compare(previous, rate) >= 0) {
                throw new IllegalArgumentException("exchange rates are not strictly sorted");
            }
            previous = rate;
        }
        if (!baseCurrencyPresent) {
            throw new IllegalArgumentException("exchange-rate snapshot has no base-currency rate");
        }
    }

    private static int compare(ExchangeRate left, ExchangeRate right)
    {
        int currencyComparison = left.currency().compareTo(right.currency());
        if (currencyComparison != 0) {
            return currencyComparison;
        }
        return left.effectiveDate().compareTo(right.effectiveDate());
    }

    public record ExchangeRate(String currency, String effectiveDate, String unscaledRate)
    {
        public ExchangeRate
        {
            currency = requireNonNull(currency, "currency is null");
            effectiveDate = requireNonNull(effectiveDate, "effectiveDate is null");
            unscaledRate = requireNonNull(unscaledRate, "unscaledRate is null");
            if (!CURRENCY_PATTERN.matcher(currency).matches()) {
                throw new IllegalArgumentException("invalid exchange-rate currency");
            }
            try {
                if (!LocalDate.parse(effectiveDate).toString().equals(effectiveDate)) {
                    throw new IllegalArgumentException("noncanonical exchange-rate date");
                }
            }
            catch (DateTimeParseException e) {
                throw new IllegalArgumentException("invalid exchange-rate date", e);
            }
            if (!UNSCALED_RATE_PATTERN.matcher(unscaledRate).matches()) {
                throw new IllegalArgumentException("invalid exchange-rate value");
            }
        }
    }
}
