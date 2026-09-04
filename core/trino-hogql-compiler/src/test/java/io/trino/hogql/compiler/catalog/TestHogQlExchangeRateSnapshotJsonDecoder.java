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
package io.trino.hogql.compiler.catalog;

import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder.DecodeException;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder.DecodeFailure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlExchangeRateSnapshotJsonDecoder
{
    private final HogQlExchangeRateSnapshotJsonDecoder decoder = new HogQlExchangeRateSnapshotJsonDecoder();

    @Test
    public void testDecodesCanonicalLatestAndPinnedSnapshots()
    {
        HogQlExchangeRateSnapshot latest = decoder.decode(snapshotJson(7).getBytes(StandardCharsets.UTF_8), LoadRequest.latest());
        HogQlExchangeRateSnapshot pinned = decoder.decode(snapshotJson(7).getBytes(StandardCharsets.UTF_8), LoadRequest.pinned(7));

        assertThat(latest).isEqualTo(pinned);
        assertThat(latest.generation()).isEqualTo(7);
        assertThat(latest.rates()).containsExactly(
                new HogQlExchangeRateSnapshot.ExchangeRate("EUR", "2024-01-01", "9049000000"),
                new HogQlExchangeRateSnapshot.ExchangeRate("USD", "1970-01-01", "10000000000"));
        assertThatThrownBy(() -> latest.rates().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidDocuments")
    public void testRejectsNoncanonicalDocuments(String name, String document, LoadRequest request, DecodeFailure failure)
    {
        assertThatThrownBy(() -> decoder.decode(document.getBytes(StandardCharsets.UTF_8), request))
                .isInstanceOfSatisfying(DecodeException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(failure);
                    assertThat(exception.getMessage()).hasSizeLessThan(128);
                });
    }

    @Test
    public void testRejectsPayloadOver32MiB()
    {
        byte[] payload = new byte[HogQlExchangeRateSnapshotJsonDecoder.MAXIMUM_PAYLOAD_BYTES + 1];

        assertThatThrownBy(() -> decoder.decode(payload, LoadRequest.latest()))
                .isInstanceOfSatisfying(DecodeException.class, exception -> assertThat(exception.failure()).isEqualTo(DecodeFailure.LIMIT_EXCEEDED));
    }

    private static Stream<Arguments> invalidDocuments()
    {
        String valid = snapshotJson(7);
        return Stream.of(
                Arguments.of("unknown field", valid.replace("\"generation\": 7", "\"generation\": 7, \"unknown\": true"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("missing field", valid.replace("\"decimalScale\": 10,", ""), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("duplicate field", valid.replace("\"generation\": 7", "\"generation\": 7, \"generation\": 7"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("trailing document", valid + "{}", LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("null document", "null", LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("unsupported protocol", valid.replace("\"protocolVersion\": 1", "\"protocolVersion\": 2"), LoadRequest.latest(), DecodeFailure.UNSUPPORTED_PROTOCOL),
                Arguments.of("unsupported schema", valid.replace("\"schemaVersion\": 1", "\"schemaVersion\": 2"), LoadRequest.latest(), DecodeFailure.UNSUPPORTED_SCHEMA),
                Arguments.of("generation mismatch", valid, LoadRequest.pinned(8), DecodeFailure.GENERATION_MISMATCH),
                Arguments.of("nonpositive generation", valid.replace("\"generation\": 7", "\"generation\": 0"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("wrong base", valid.replace("\"baseCurrency\": \"USD\"", "\"baseCurrency\": \"EUR\""), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("wrong scale", valid.replace("\"decimalScale\": 10", "\"decimalScale\": 9"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("lowercase currency", valid.replace("\"EUR\"", "\"eur\""), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("invalid date", valid.replace("2024-01-01", "2024-02-30"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("leading zero", valid.replace("9049000000", "09049000000"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("base rate differs", valid.replace("10000000000", "9999999999"), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD),
                Arguments.of("unsorted rates", valid.replace("\"EUR\", \"effectiveDate\": \"2024-01-01\", \"unscaledRate\": \"9049000000\"", "\"ZZZ\", \"effectiveDate\": \"2024-01-01\", \"unscaledRate\": \"9049000000\""), LoadRequest.latest(), DecodeFailure.INVALID_PAYLOAD));
    }

    private static String snapshotJson(long generation)
    {
        return """
               {
                 "protocolVersion": 1,
                 "schemaVersion": 1,
                 "generation": %s,
                 "baseCurrency": "USD",
                 "decimalScale": 10,
                 "rates": [
                   {"currency": "EUR", "effectiveDate": "2024-01-01", "unscaledRate": "9049000000"},
                   {"currency": "USD", "effectiveDate": "1970-01-01", "unscaledRate": "10000000000"}
                 ]
               }
               """.formatted(generation);
    }
}
