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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.plugin.ducklake.function.currency.ExchangeRateSnapshot.ExchangeRate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class ExchangeRateSnapshotJsonDecoder
{
    public static final int MAXIMUM_PAYLOAD_BYTES = 32 * 1024 * 1024;

    private static final int MAXIMUM_NESTING_DEPTH = 16;
    private static final Set<String> SNAPSHOT_FIELDS = Set.of("protocolVersion", "schemaVersion", "generation", "baseCurrency", "decimalScale", "rates");
    private static final Set<String> RATE_FIELDS = Set.of("currency", "effectiveDate", "unscaledRate");

    private final ObjectMapper objectMapper;

    public ExchangeRateSnapshotJsonDecoder()
    {
        JsonFactory jsonFactory = JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(MAXIMUM_NESTING_DEPTH)
                        .maxStringLength(64)
                        .maxNumberLength(19)
                        .build())
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
        objectMapper = new ObjectMapper(jsonFactory);
    }

    public ExchangeRateSnapshot decode(byte[] payload, long expectedGeneration)
    {
        if (payload == null) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        if (payload.length > MAXIMUM_PAYLOAD_BYTES) {
            throw failure(DecodeFailure.LIMIT_EXCEEDED);
        }
        if (expectedGeneration <= 0) {
            throw failure(DecodeFailure.GENERATION_MISMATCH);
        }

        try {
            ObjectNode root = parse(payload);
            validateFields(root, SNAPSHOT_FIELDS);

            int protocolVersion = integer(root, "protocolVersion");
            if (protocolVersion != ExchangeRateSnapshot.PROTOCOL_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_PROTOCOL);
            }
            int schemaVersion = integer(root, "schemaVersion");
            if (schemaVersion != ExchangeRateSnapshot.SCHEMA_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_SCHEMA);
            }

            long generation = generation(root);
            if (generation != expectedGeneration) {
                throw failure(DecodeFailure.GENERATION_MISMATCH);
            }

            return new ExchangeRateSnapshot(
                    protocolVersion,
                    schemaVersion,
                    generation,
                    text(root, "baseCurrency"),
                    integer(root, "decimalScale"),
                    rates(required(root, "rates")));
        }
        catch (DecodeException e) {
            throw e;
        }
        catch (StreamConstraintsException e) {
            throw failure(DecodeFailure.LIMIT_EXCEEDED);
        }
        catch (IOException | RuntimeException e) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
    }

    private ObjectNode parse(byte[] payload)
            throws IOException
    {
        try (JsonParser parser = objectMapper.createParser(payload)) {
            JsonNode root = objectMapper.readTree(parser);
            if (parser.nextToken() != null) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
            return object(root);
        }
    }

    private static List<ExchangeRate> rates(JsonNode node)
    {
        ArrayNode array = array(node);
        if (array.size() > ExchangeRateSnapshot.MAXIMUM_RATES) {
            throw failure(DecodeFailure.LIMIT_EXCEEDED);
        }
        List<ExchangeRate> rates = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode rate = object(element);
            validateFields(rate, RATE_FIELDS);
            rates.add(new ExchangeRate(
                    text(rate, "currency"),
                    text(rate, "effectiveDate"),
                    text(rate, "unscaledRate")));
        }
        return List.copyOf(rates);
    }

    private static int integer(ObjectNode object, String field)
    {
        JsonNode node = required(object, field);
        if (!node.isIntegralNumber() || !node.canConvertToInt()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node.intValue();
    }

    private static long generation(ObjectNode object)
    {
        JsonNode node = required(object, "generation");
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() <= 0) {
            throw failure(DecodeFailure.GENERATION_MISMATCH);
        }
        return node.longValue();
    }

    private static String text(ObjectNode object, String field)
    {
        JsonNode node = required(object, field);
        if (!node.isTextual() || node.textValue().isEmpty()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node.textValue();
    }

    private static JsonNode required(ObjectNode object, String field)
    {
        JsonNode node = object.get(field);
        if (node == null || node.isNull()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node;
    }

    private static ObjectNode object(JsonNode node)
    {
        if (!(node instanceof ObjectNode object)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return object;
    }

    private static ArrayNode array(JsonNode node)
    {
        if (!(node instanceof ArrayNode array)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return array;
    }

    private static void validateFields(ObjectNode object, Set<String> fields)
    {
        Iterator<String> names = object.fieldNames();
        int count = 0;
        while (names.hasNext()) {
            if (!fields.contains(names.next())) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
            count++;
        }
        if (count != fields.size()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
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
            super(requireNonNull(failure, "failure is null").name());
            this.failure = failure;
        }

        public DecodeFailure failure()
        {
            return failure;
        }
    }
}
