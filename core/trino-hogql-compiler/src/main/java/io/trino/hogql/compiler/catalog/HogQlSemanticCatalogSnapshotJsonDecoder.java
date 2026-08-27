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
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class HogQlSemanticCatalogSnapshotJsonDecoder
{
    public static final int PROTOCOL_VERSION = 1;

    private static final int SCHEMA_VERSION = 1;
    private static final Limits DEFAULT_LIMITS = new Limits(8 * 1024 * 1024, 64, 100_000);

    private static final Set<String> SNAPSHOT_FIELDS = Set.of(
            "protocolVersion",
            "schemaVersion",
            "languageVersion",
            "catalog",
            "generation",
            "logicalTables");
    private static final Set<String> IDENTIFIER_FIELDS = Set.of("value", "delimited");
    private static final Set<String> QUALIFIED_NAME_FIELDS = Set.of("catalog", "schema", "table");
    private static final Set<String> TABLE_FIELDS = Set.of("name", "physicalTable", "fields", "properties", "relationships");
    private static final Set<String> LOGICAL_FIELD_FIELDS = Set.of("name", "physicalColumn", "trinoTypeSignature", "logicalType", "nullable", "starVisible");
    private static final Set<String> PROPERTY_FIELDS = Set.of("name", "sourceField", "storage", "logicalType", "nullable");
    private static final Set<String> RELATIONSHIP_FIELDS = Set.of("name", "targetTable", "cardinality", "joinKeys");
    private static final Set<String> JOIN_KEY_FIELDS = Set.of("sourceField", "targetField");

    private final Limits limits;
    private final ObjectMapper objectMapper;

    public HogQlSemanticCatalogSnapshotJsonDecoder()
    {
        this(DEFAULT_LIMITS);
    }

    public HogQlSemanticCatalogSnapshotJsonDecoder(Limits limits)
    {
        this.limits = requireNonNull(limits, "limits is null");
        JsonFactory jsonFactory = JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(limits.maximumNestingDepth())
                        .maxStringLength(limits.maximumPayloadBytes())
                        .maxNumberLength(64)
                        .build())
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
        this.objectMapper = new ObjectMapper(jsonFactory);
    }

    public HogQlSemanticCatalogSnapshot decode(byte[] payload, LoadRequest request)
    {
        requireNonNull(request, "request is null");
        if (payload == null) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        if (payload.length > limits.maximumPayloadBytes()) {
            throw failure(DecodeFailure.LIMIT_EXCEEDED);
        }

        try {
            ObjectNode root = parse(payload);
            validateFields(root, SNAPSHOT_FIELDS);

            int protocolVersion = integer(root, "protocolVersion");
            if (protocolVersion != PROTOCOL_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_PROTOCOL);
            }
            int schemaVersion = integer(root, "schemaVersion");
            if (schemaVersion != SCHEMA_VERSION) {
                throw failure(DecodeFailure.UNSUPPORTED_SCHEMA);
            }

            HogQlLanguageVersion languageVersion = languageVersion(root);
            if (!languageVersion.equals(request.languageVersion())) {
                throw failure(DecodeFailure.LANGUAGE_VERSION_MISMATCH);
            }

            PhysicalIdentifier catalog = physicalIdentifier(required(root, "catalog"));
            if (!catalog.equals(request.catalog())) {
                throw failure(DecodeFailure.CATALOG_MISMATCH);
            }

            long generation = positiveGeneration(root);
            if (request.expectedGeneration().isPresent() && request.expectedGeneration().orElseThrow() != generation) {
                throw failure(DecodeFailure.GENERATION_MISMATCH);
            }

            CollectionBudget budget = new CollectionBudget(limits.maximumCollectionEntries());
            List<LogicalTableDefinition> tables = logicalTables(required(root, "logicalTables"), budget);
            return new HogQlSemanticCatalogSnapshot(schemaVersion, languageVersion, catalog, generation, tables);
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

    private static HogQlLanguageVersion languageVersion(ObjectNode root)
    {
        return HogQlLanguageVersion.valueOf(text(root, "languageVersion"));
    }

    private static long positiveGeneration(ObjectNode root)
    {
        JsonNode node = required(root, "generation");
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() <= 0) {
            throw failure(DecodeFailure.GENERATION_MISMATCH);
        }
        return node.longValue();
    }

    private static List<LogicalTableDefinition> logicalTables(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<LogicalTableDefinition> tables = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode table = object(element);
            validateFields(table, TABLE_FIELDS);
            tables.add(new LogicalTableDefinition(
                    text(table, "name"),
                    qualifiedName(required(table, "physicalTable")),
                    logicalFields(required(table, "fields"), budget),
                    properties(required(table, "properties"), budget),
                    relationships(required(table, "relationships"), budget)));
        }
        return List.copyOf(tables);
    }

    private static List<LogicalFieldDefinition> logicalFields(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<LogicalFieldDefinition> fields = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode field = object(element);
            validateFields(field, LOGICAL_FIELD_FIELDS);
            fields.add(new LogicalFieldDefinition(
                    text(field, "name"),
                    physicalIdentifier(required(field, "physicalColumn")),
                    text(field, "trinoTypeSignature"),
                    enumValue(field, "logicalType", LogicalType.class),
                    bool(field, "nullable"),
                    bool(field, "starVisible")));
        }
        return List.copyOf(fields);
    }

    private static List<PropertyDefinition> properties(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<PropertyDefinition> properties = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode property = object(element);
            validateFields(property, PROPERTY_FIELDS);
            properties.add(new PropertyDefinition(
                    text(property, "name"),
                    text(property, "sourceField"),
                    enumValue(property, "storage", PropertyStorage.class),
                    enumValue(property, "logicalType", LogicalType.class),
                    bool(property, "nullable")));
        }
        return List.copyOf(properties);
    }

    private static List<RelationshipDefinition> relationships(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<RelationshipDefinition> relationships = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode relationship = object(element);
            validateFields(relationship, RELATIONSHIP_FIELDS);
            relationships.add(new RelationshipDefinition(
                    text(relationship, "name"),
                    text(relationship, "targetTable"),
                    enumValue(relationship, "cardinality", RelationshipCardinality.class),
                    joinKeys(required(relationship, "joinKeys"), budget)));
        }
        return List.copyOf(relationships);
    }

    private static List<JoinKey> joinKeys(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<JoinKey> joinKeys = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode joinKey = object(element);
            validateFields(joinKey, JOIN_KEY_FIELDS);
            joinKeys.add(new JoinKey(text(joinKey, "sourceField"), text(joinKey, "targetField")));
        }
        return List.copyOf(joinKeys);
    }

    private static PhysicalQualifiedName qualifiedName(JsonNode node)
    {
        ObjectNode name = object(node);
        validateFields(name, QUALIFIED_NAME_FIELDS);
        return new PhysicalQualifiedName(
                physicalIdentifier(required(name, "catalog")),
                physicalIdentifier(required(name, "schema")),
                physicalIdentifier(required(name, "table")));
    }

    private static PhysicalIdentifier physicalIdentifier(JsonNode node)
    {
        ObjectNode identifier = object(node);
        validateFields(identifier, IDENTIFIER_FIELDS);
        return new PhysicalIdentifier(text(identifier, "value"), bool(identifier, "delimited"));
    }

    private static ObjectNode object(JsonNode node)
    {
        if (!(node instanceof ObjectNode object)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return object;
    }

    private static ArrayNode array(JsonNode node, CollectionBudget budget)
    {
        if (!(node instanceof ArrayNode array)) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        budget.add(array.size());
        return array;
    }

    private static JsonNode required(ObjectNode object, String name)
    {
        JsonNode node = object.get(name);
        if (node == null || node.isNull()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node;
    }

    private static String text(ObjectNode object, String name)
    {
        JsonNode node = required(object, name);
        if (!node.isTextual()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node.textValue();
    }

    private static int integer(ObjectNode object, String name)
    {
        JsonNode node = required(object, name);
        if (!node.isIntegralNumber() || !node.canConvertToInt()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node.intValue();
    }

    private static boolean bool(ObjectNode object, String name)
    {
        JsonNode node = required(object, name);
        if (!node.isBoolean()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
        return node.booleanValue();
    }

    private static <E extends Enum<E>> E enumValue(ObjectNode object, String name, Class<E> enumType)
    {
        return Enum.valueOf(enumType, text(object, name));
    }

    private static void validateFields(ObjectNode object, Set<String> allowedFields)
    {
        Iterator<String> fieldNames = object.fieldNames();
        while (fieldNames.hasNext()) {
            if (!allowedFields.contains(fieldNames.next())) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
        }
        if (object.size() != allowedFields.size()) {
            throw failure(DecodeFailure.INVALID_PAYLOAD);
        }
    }

    private static DecodeException failure(DecodeFailure failure)
    {
        return new DecodeException(failure);
    }

    public record Limits(int maximumPayloadBytes, int maximumNestingDepth, int maximumCollectionEntries)
    {
        public Limits
        {
            if (maximumPayloadBytes <= 0 || maximumNestingDepth <= 0 || maximumCollectionEntries <= 0) {
                throw new IllegalArgumentException("HogQL semantic catalog decoder limits must be positive");
            }
        }
    }

    public static final class DecodeException
            extends RuntimeException
    {
        private final DecodeFailure failure;

        private DecodeException(DecodeFailure failure)
        {
            super(requireNonNull(failure, "failure is null").message());
            this.failure = failure;
        }

        public DecodeFailure failure()
        {
            return failure;
        }
    }

    public enum DecodeFailure
    {
        INVALID_PAYLOAD("Invalid HogQL semantic catalog payload"),
        LIMIT_EXCEEDED("HogQL semantic catalog payload limit exceeded"),
        UNSUPPORTED_PROTOCOL("Unsupported HogQL semantic catalog protocol"),
        UNSUPPORTED_SCHEMA("Unsupported HogQL semantic catalog schema"),
        LANGUAGE_VERSION_MISMATCH("HogQL semantic catalog language version mismatch"),
        CATALOG_MISMATCH("HogQL semantic catalog identifier mismatch"),
        GENERATION_MISMATCH("HogQL semantic catalog generation mismatch");

        private final String message;

        DecodeFailure(String message)
        {
            this.message = message;
        }

        private String message()
        {
            return message;
        }
    }

    private static final class CollectionBudget
    {
        private final int maximumEntries;
        private int entries;

        private CollectionBudget(int maximumEntries)
        {
            this.maximumEntries = maximumEntries;
        }

        private void add(int size)
        {
            if (size > maximumEntries - entries) {
                throw failure(DecodeFailure.LIMIT_EXCEEDED);
            }
            entries += size;
        }
    }
}
