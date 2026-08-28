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
import io.trino.plugin.ducklake.function.currency.ExchangeRateSnapshotJsonDecoder.DecodeFailure;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestExchangeRateSnapshotJsonDecoder
{
    @Test
    void testDecodesExactGenerationSnapshot()
    {
        ExchangeRateSnapshot snapshot = new ExchangeRateSnapshotJsonDecoder().decode(bytes(validSnapshot()), 17);

        assertThat(snapshot.protocolVersion()).isEqualTo(1);
        assertThat(snapshot.schemaVersion()).isEqualTo(1);
        assertThat(snapshot.generation()).isEqualTo(17);
        assertThat(snapshot.baseCurrency()).isEqualTo("USD");
        assertThat(snapshot.decimalScale()).isEqualTo(10);
        assertThat(snapshot.rates()).containsExactly(
                new ExchangeRate("EUR", "2024-01-01", "9049000000"),
                new ExchangeRate("EUR", "2024-01-10", "9500000000"),
                new ExchangeRate("USD", "2024-01-01", "10000000000"));
    }

    @Test
    void testSnapshotDefensivelyCopiesRates()
    {
        List<ExchangeRate> rates = new ArrayList<>();
        rates.add(new ExchangeRate("USD", "2024-01-01", "10000000000"));
        ExchangeRateSnapshot snapshot = new ExchangeRateSnapshot(1, 1, 17, "USD", 10, rates);

        rates.clear();

        assertThat(snapshot.rates()).hasSize(1);
        assertThatThrownBy(() -> snapshot.rates().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRejectsIncompatibleSnapshots()
    {
        compatibilityFailures().forEach(testCase -> assertDecodeFailure(
                testCase.name(),
                testCase.payload(),
                testCase.expectedGeneration(),
                testCase.failure()));
    }

    private static List<DecodeFailureCase> compatibilityFailures()
    {
        return List.of(
                new DecodeFailureCase("protocol", validSnapshot().replace("\"protocolVersion\": 1", "\"protocolVersion\": 2"), 17, DecodeFailure.UNSUPPORTED_PROTOCOL),
                new DecodeFailureCase("schema", validSnapshot().replace("\"schemaVersion\": 1", "\"schemaVersion\": 2"), 17, DecodeFailure.UNSUPPORTED_SCHEMA),
                new DecodeFailureCase("generation", validSnapshot(), 18, DecodeFailure.GENERATION_MISMATCH),
                new DecodeFailureCase("nonpositive expected generation", validSnapshot(), 0, DecodeFailure.GENERATION_MISMATCH),
                new DecodeFailureCase("nonpositive snapshot generation", validSnapshot().replace("\"generation\": 17", "\"generation\": 0"), 17, DecodeFailure.GENERATION_MISMATCH));
    }

    @Test
    void testRejectsMalformedOrExtendedPayloads()
    {
        strictFailures().forEach(testCase -> assertDecodeFailure(
                testCase.name(),
                testCase.payload(),
                testCase.expectedGeneration(),
                testCase.failure()));
    }

    private static List<DecodeFailureCase> strictFailures()
    {
        return List.of(
                invalid("unknown root field", validSnapshot().replace("\"rates\":", "\"unknown\": true, \"rates\":")),
                invalid("missing root field", validSnapshot().replace("\"schemaVersion\": 1,", "")),
                invalid("duplicate root field", validSnapshot().replace("\"schemaVersion\": 1,", "\"schemaVersion\": 1, \"schemaVersion\": 1,")),
                invalid("unsupported base", validSnapshot().replace("\"baseCurrency\": \"USD\"", "\"baseCurrency\": \"EUR\"")),
                invalid("unsupported scale", validSnapshot().replace("\"decimalScale\": 10", "\"decimalScale\": 9")),
                invalid("numeric unscaled rate", validSnapshot().replace("\"9049000000\"", "9049000000")),
                invalid("negative unscaled rate", validSnapshot().replace("\"9049000000\"", "\"-9049000000\"")),
                invalid("leading zero unscaled rate", validSnapshot().replace("\"9049000000\"", "\"09049000000\"")),
                invalid("oversized unscaled rate", validSnapshot().replace("\"9049000000\"", "\"1234567890123456789\"")),
                invalid("invalid currency", validSnapshot().replace("\"EUR\"", "\"eur\"")),
                invalid("invalid date", validSnapshot().replace("2024-01-10", "2024-02-30")),
                invalid("duplicate currency date", validSnapshot().replace("2024-01-10", "2024-01-01")),
                invalid("unknown rate field", validSnapshot().replace("\"currency\": \"USD\"", "\"currency\": \"USD\", \"unknown\": true")),
                invalid("trailing document", validSnapshot() + " {}"));
    }

    private static DecodeFailureCase invalid(String name, String payload)
    {
        return new DecodeFailureCase(name, payload, 17, DecodeFailure.INVALID_PAYLOAD);
    }

    private static void assertDecodeFailure(String name, String payload, long expectedGeneration, DecodeFailure failure)
    {
        assertThatThrownBy(() -> new ExchangeRateSnapshotJsonDecoder().decode(bytes(payload), expectedGeneration))
                .as(name)
                .isInstanceOfSatisfying(ExchangeRateSnapshotJsonDecoder.DecodeException.class, exception -> assertThat(exception.failure()).isEqualTo(failure));
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String validSnapshot()
    {
        return """
               {
                  "protocolVersion": 1,
                  "schemaVersion": 1,
                  "generation": 17,
                  "baseCurrency": "USD",
                  "decimalScale": 10,
                  "rates": [
                    {"currency": "EUR", "effectiveDate": "2024-01-01", "unscaledRate": "9049000000"},
                    {"currency": "EUR", "effectiveDate": "2024-01-10", "unscaledRate": "9500000000"},
                    {"currency": "USD", "effectiveDate": "2024-01-01", "unscaledRate": "10000000000"}
                  ]
                }
               """;
    }

    private record DecodeFailureCase(String name, String payload, long expectedGeneration, DecodeFailure failure) {}
}
