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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot.ExchangeRate;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class HogQlExchangeRateSnapshotJsonDecoder
{
    public static final int MAXIMUM_PAYLOAD_BYTES = 32 * 1024 * 1024;

    private static final Set<String> SNAPSHOT_FIELDS = Set.of(
            "protocolVersion",
            "schemaVersion",
            "generation",
            "baseCurrency",
            "decimalScale",
            "rates");
    private static final Set<String> RATE_FIELDS = Set.of("currency", "effectiveDate", "unscaledRate");

    private final ObjectMapper objectMapper;

    public HogQlExchangeRateSnapshotJsonDecoder()
    {
        JsonFactory jsonFactory = JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(16)
                        .maxStringLength(MAXIMUM_PAYLOAD_BYTES)
                        .maxNumberLength(64)
                        .build())
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
        objectMapper = new ObjectMapper(jsonFactory);
    }

    public HogQlExchangeRateSnapshot decode(byte[] payload, LoadRequest request)
    {
        requireNonNull(request, "request is null");
        if (payload == null) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        if (payload.length > MAXIMUM_PAYLOAD_BYTES) {
            throw failure(DecodeFailure.LIMIT_EXCEEDED);
        }

        try (JsonParser parser = objectMapper.createParser(payload)) {
            JsonNode document = objectMapper.readTree(parser);
            if (!(document instanceof ObjectNode root) || parser.nextToken() != null) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
            requireFields(root, SNAPSHOT_FIELDS);
            int protocolVersion = integer(root, "protocolVersion");
            if (protocolVersion != HogQlExchangeRateSnapshot.PROTOCOL_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_PROTOCOL);
            }
            int schemaVersion = integer(root, "schemaVersion");
            if (schemaVersion != HogQlExchangeRateSnapshot.SCHEMA_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_SCHEMA);
            }
            long generation = longInteger(root, "generation");
            if (request.expectedGeneration().isPresent() && generation != request.expectedGeneration().orElseThrow()) {
                throw failure(DecodeFailure.GENERATION_MISMATCH);
            }
            String baseCurrency = text(root, "baseCurrency");
            int decimalScale = integer(root, "decimalScale");
            ArrayNode rateNodes = array(root, "rates");
            if (rateNodes.size() > HogQlExchangeRateSnapshot.MAXIMUM_RATES) {
                throw failure(DecodeFailure.LIMIT_EXCEEDED);
            }
            List<ExchangeRate> rates = new ArrayList<>(rateNodes.size());
            for (JsonNode node : rateNodes) {
                if (!(node instanceof ObjectNode rate)) {
                    throw failure(DecodeFailure.INVALID_PAYLOAD);
                }
                requireFields(rate, RATE_FIELDS);
                rates.add(new ExchangeRate(
                        text(rate, "currency"),
                        text(rate, "effectiveDate"),
                        text(rate, "unscaledRate")));
            }
            return new HogQlExchangeRateSnapshot(protocolVersion, schemaVersion, generation, baseCurrency, decimalScale, rates);
        }
        catch (DecodeException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new DecodeException(DecodeFailure.INVALID_PAYLOAD, e);
        }
    }

    private static void requireFields(ObjectNode object, Set<String> expected)
    {
        Set<String> actual = new HashSet<>();
        object.fieldNames().forEachRemaining(actual::add);
        if (!actual.equals(expected)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
    }

    private static int integer(ObjectNode object, String field)
    {
        JsonNode value = object.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return value.intValue();
    }

    private static long longInteger(ObjectNode object, String field)
    {
        JsonNode value = object.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return value.longValue();
    }

    private static String text(ObjectNode object, String field)
    {
        JsonNode value = object.get(field);
        if (value == null || !value.isTextual()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return value.textValue();
    }

    private static ArrayNode array(ObjectNode object, String field)
    {
        JsonNode value = object.get(field);
        if (!(value instanceof ArrayNode array)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return array;
    }

    private static DecodeException failure(DecodeFailure failure)
    {
        return new DecodeException(failure);
    }

    public enum DecodeFailure
    {
        INVALID_PAYLOAD,
        LIMIT_EXCEEDED,
        UNSUPPORTED_PROTOCOL,
        UNSUPPORTED_SCHEMA,
        GENERATION_MISMATCH,
    }

    public static final class DecodeException
            extends IllegalArgumentException
    {
        private final DecodeFailure failure;

        private DecodeException(DecodeFailure failure)
        {
            super("invalid HogQL exchange-rate snapshot");
            this.failure = requireNonNull(failure, "failure is null");
        }

        private DecodeException(DecodeFailure failure, Throwable cause)
        {
            super("invalid HogQL exchange-rate snapshot", cause);
            this.failure = requireNonNull(failure, "failure is null");
        }

        public DecodeFailure failure()
        {
            return failure;
        }
    }
}
