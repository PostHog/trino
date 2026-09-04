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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ActionReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ArgumentReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CohortReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionArgument;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipeKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionRewrite;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyProjectionDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LazyTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.MaterializedViewReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ModifierBehavior;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PredicateRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyLookupRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ReferencedField;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipJoinSide;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SavedQueryReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ScopedFieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticEntityKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticModifierDefault;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.VirtualProjection;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.VirtualTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class HogQlSemanticCatalogSnapshotJsonDecoder
{
    public static final int PROTOCOL_VERSION = 1;
    public static final int MAXIMUM_PAYLOAD_BYTES = 8 * 1024 * 1024;

    private static final int SCHEMA_VERSION = 2;
    private static final Limits DEFAULT_LIMITS = new Limits(MAXIMUM_PAYLOAD_BYTES, 256, 100_000);

    private static final Set<String> SNAPSHOT_FIELDS = Set.of(
            "protocolVersion",
            "schemaVersion",
            "languageVersion",
            "catalog",
            "generation",
            "logicalTables",
            "expressionFields",
            "virtualTables",
            "savedQueries",
            "materializedViews",
            "functions",
            "modifierDefaults",
            "lazyTables",
            "actions",
            "cohorts");
    private static final Set<String> REQUIRED_SNAPSHOT_FIELDS = Set.of(
            "protocolVersion",
            "schemaVersion",
            "languageVersion",
            "catalog",
            "generation",
            "logicalTables",
            "expressionFields",
            "virtualTables",
            "savedQueries",
            "materializedViews",
            "functions",
            "modifierDefaults");
    private static final Set<String> IDENTIFIER_FIELDS = Set.of("value", "delimited");
    private static final Set<String> QUALIFIED_NAME_FIELDS = Set.of("catalog", "schema", "table");
    private static final Set<String> TABLE_FIELDS = Set.of("name", "physicalTable", "fields", "properties", "relationships");
    private static final Set<String> LOGICAL_FIELD_FIELDS = Set.of("name", "physicalColumn", "trinoTypeSignature", "logicalType", "nullable", "starVisible");
    private static final Set<String> PROPERTY_FIELDS = Set.of("name", "sourceField", "storage", "logicalType", "nullable", "keyTypeSignature", "valueTypeSignature", "lookupRecipe");
    private static final Set<String> REQUIRED_PROPERTY_FIELDS = Set.of("name", "sourceField", "storage", "logicalType", "nullable");
    private static final Set<String> RELATIONSHIP_FIELDS = Set.of("name", "targetTable", "cardinality", "joinKeys", "joinPredicate");
    private static final Set<String> REQUIRED_RELATIONSHIP_FIELDS = Set.of("name", "targetTable", "cardinality", "joinKeys");
    private static final Set<String> JOIN_KEY_FIELDS = Set.of("sourceField", "targetField");
    private static final Set<String> EXPRESSION_FIELD_FIELDS = Set.of("table", "name", "trinoTypeSignature", "logicalType", "nullable", "starVisible", "recipe");
    private static final Set<String> RECIPE_FIELD_REFERENCE_FIELDS = Set.of("kind", "fieldReference");
    private static final Set<String> RECIPE_LITERAL_FIELDS = Set.of("kind", "literal");
    private static final Set<String> RECIPE_FUNCTION_CALL_FIELDS = Set.of("kind", "functionCall");
    private static final Set<String> RECIPE_OPERATOR_FIELDS = Set.of("kind", "operator");
    private static final Set<String> RECIPE_CAST_FIELDS = Set.of("kind", "cast");
    private static final Set<String> RECIPE_ARGUMENT_REFERENCE_FIELDS = Set.of("kind", "argumentReference");
    private static final Set<String> RECIPE_SCOPED_FIELD_REFERENCE_FIELDS = Set.of("kind", "scopedFieldReference");
    private static final Set<String> RECIPE_PROPERTY_LOOKUP_FIELDS = Set.of("kind", "propertyLookup");
    private static final Set<String> FIELD_REFERENCE_FIELDS = Set.of("table", "field");
    private static final Set<String> TYPED_LITERAL_FIELDS = Set.of("typeSignature", "encoding", "value");
    private static final Set<String> FUNCTION_CALL_FIELDS = Set.of("name", "arguments");
    private static final Set<String> OPERATOR_FIELDS = Set.of("operator", "arguments");
    private static final Set<String> CAST_FIELDS = Set.of("expression", "targetTypeSignature");
    private static final Set<String> ARGUMENT_REFERENCE_FIELDS = Set.of("argument");
    private static final Set<String> SCOPED_FIELD_REFERENCE_FIELDS = Set.of("side", "field");
    private static final Set<String> PROPERTY_LOOKUP_FIELDS = Set.of("table", "property", "key");
    private static final Set<String> VIRTUAL_TABLE_FIELDS = Set.of("name", "source", "projections");
    private static final Set<String> RELATION_REFERENCE_FIELDS = Set.of("kind", "name");
    private static final Set<String> VIRTUAL_PROJECTION_FIELDS = Set.of("name", "sourceField", "starVisible");
    private static final Set<String> SAVED_QUERY_FIELDS = Set.of("name", "queryId", "target", "fields");
    private static final Set<String> MATERIALIZED_VIEW_FIELDS = Set.of("name", "physicalView", "fields");
    private static final Set<String> REFERENCED_FIELD_FIELDS = Set.of("name", "trinoTypeSignature", "logicalType", "nullable", "starVisible");
    private static final Set<String> FUNCTION_FIELDS = Set.of("name", "kind", "implementation", "trinoName", "rewrite", "signatures", "deterministic", "supportsDistinct", "supportsOrderBy", "supportsFilter", "supportsWindow");
    private static final Set<String> REQUIRED_FUNCTION_FIELDS = Set.of("name", "kind", "implementation", "trinoName", "signatures", "deterministic", "supportsDistinct", "supportsOrderBy", "supportsFilter", "supportsWindow");
    private static final Set<String> FUNCTION_SIGNATURE_FIELDS = Set.of("argumentTypes", "returnType", "variadic");
    private static final Set<String> MODIFIER_FIELDS = Set.of("name", "behavior", "defaultValue", "sessionProperty");
    private static final Set<String> MODIFIER_FIELDS_WITHOUT_SESSION_PROPERTY = Set.of("name", "behavior", "defaultValue");
    private static final Set<String> LAZY_TABLE_FIELDS = Set.of("table", "name", "relationshipPath", "projections");
    private static final Set<String> LAZY_PROJECTION_FIELDS = Set.of("name", "trinoTypeSignature", "logicalType", "nullable", "starVisible", "recipe");
    private static final Set<String> ACTION_FIELDS = Set.of("name", "actionId", "table", "representation");
    private static final Set<String> COHORT_FIELDS = Set.of("name", "cohortId", "table", "representation");
    private static final Set<String> PREDICATE_REPRESENTATION_FIELDS = Set.of("kind", "predicate");
    private static final Set<String> RELATION_REPRESENTATION_FIELDS = Set.of("kind", "relation");
    private static final Set<String> RELATION_MEMBERSHIP_FIELDS = Set.of("relation", "sourceField", "targetField");

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
            validateFields(root, SNAPSHOT_FIELDS, REQUIRED_SNAPSHOT_FIELDS);

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
            List<ExpressionFieldDefinition> expressionFields = expressionFields(required(root, "expressionFields"), budget);
            List<VirtualTableDefinition> virtualTables = virtualTables(required(root, "virtualTables"), budget);
            List<SavedQueryReference> savedQueries = savedQueries(required(root, "savedQueries"), budget);
            List<MaterializedViewReference> materializedViews = materializedViews(required(root, "materializedViews"), budget);
            List<FunctionCapabilityDefinition> functions = functions(required(root, "functions"), budget);
            List<SemanticModifierDefault> modifierDefaults = modifierDefaults(required(root, "modifierDefaults"), budget);
            List<LazyTableDefinition> lazyTables = root.has("lazyTables") ? lazyTables(required(root, "lazyTables"), budget) : List.of();
            List<ActionReference> actions = root.has("actions") ? actions(required(root, "actions"), budget) : List.of();
            List<CohortReference> cohorts = root.has("cohorts") ? cohorts(required(root, "cohorts"), budget) : List.of();
            return new HogQlSemanticCatalogSnapshot(
                    protocolVersion,
                    schemaVersion,
                    languageVersion,
                    catalog,
                    generation,
                    tables,
                    expressionFields,
                    virtualTables,
                    savedQueries,
                    materializedViews,
                    functions,
                    modifierDefaults,
                    lazyTables,
                    actions,
                    cohorts);
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
            validateFields(property, PROPERTY_FIELDS, REQUIRED_PROPERTY_FIELDS);
            properties.add(new PropertyDefinition(
                    text(property, "name"),
                    text(property, "sourceField"),
                    enumValue(property, "storage", PropertyStorage.class),
                    enumValue(property, "logicalType", LogicalType.class),
                    bool(property, "nullable"),
                    property.has("keyTypeSignature") ? Optional.of(text(property, "keyTypeSignature")) : Optional.empty(),
                    property.has("valueTypeSignature") ? Optional.of(text(property, "valueTypeSignature")) : Optional.empty(),
                    property.has("lookupRecipe") ? Optional.of(expressionRecipe(required(property, "lookupRecipe"), budget)) : Optional.empty()));
        }
        return List.copyOf(properties);
    }

    private static List<RelationshipDefinition> relationships(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<RelationshipDefinition> relationships = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode relationship = object(element);
            validateFields(relationship, RELATIONSHIP_FIELDS, REQUIRED_RELATIONSHIP_FIELDS);
            relationships.add(new RelationshipDefinition(
                    text(relationship, "name"),
                    text(relationship, "targetTable"),
                    enumValue(relationship, "cardinality", RelationshipCardinality.class),
                    joinKeys(required(relationship, "joinKeys"), budget),
                    relationship.has("joinPredicate") ? Optional.of(expressionRecipe(required(relationship, "joinPredicate"), budget)) : Optional.empty()));
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

    private static List<ExpressionFieldDefinition> expressionFields(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<ExpressionFieldDefinition> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode field = object(element);
            validateFields(field, EXPRESSION_FIELD_FIELDS);
            definitions.add(new ExpressionFieldDefinition(
                    text(field, "table"),
                    text(field, "name"),
                    text(field, "trinoTypeSignature"),
                    enumValue(field, "logicalType", LogicalType.class),
                    bool(field, "nullable"),
                    bool(field, "starVisible"),
                    expressionRecipe(required(field, "recipe"), budget)));
        }
        return List.copyOf(definitions);
    }

    private static ExpressionRecipe expressionRecipe(JsonNode node, CollectionBudget budget)
    {
        ObjectNode recipe = object(node);
        ExpressionRecipeKind kind = enumValue(recipe, "kind", ExpressionRecipeKind.class);
        return switch (kind) {
            case FIELD_REFERENCE -> {
                validateFields(recipe, RECIPE_FIELD_REFERENCE_FIELDS);
                ObjectNode reference = object(required(recipe, "fieldReference"));
                validateFields(reference, FIELD_REFERENCE_FIELDS);
                yield new FieldReferenceRecipe(text(reference, "table"), text(reference, "field"));
            }
            case LITERAL -> {
                validateFields(recipe, RECIPE_LITERAL_FIELDS);
                yield new LiteralRecipe(typedLiteral(required(recipe, "literal")));
            }
            case FUNCTION_CALL -> {
                validateFields(recipe, RECIPE_FUNCTION_CALL_FIELDS);
                ObjectNode call = object(required(recipe, "functionCall"));
                validateFields(call, FUNCTION_CALL_FIELDS);
                yield new FunctionCallRecipe(text(call, "name"), expressionRecipes(required(call, "arguments"), budget));
            }
            case OPERATOR -> {
                validateFields(recipe, RECIPE_OPERATOR_FIELDS);
                ObjectNode operator = object(required(recipe, "operator"));
                validateFields(operator, OPERATOR_FIELDS);
                yield new OperatorRecipe(enumValue(operator, "operator", SemanticOperator.class), expressionRecipes(required(operator, "arguments"), budget));
            }
            case CAST -> {
                validateFields(recipe, RECIPE_CAST_FIELDS);
                ObjectNode cast = object(required(recipe, "cast"));
                validateFields(cast, CAST_FIELDS);
                yield new CastRecipe(expressionRecipe(required(cast, "expression"), budget), text(cast, "targetTypeSignature"));
            }
            case ARGUMENT_REFERENCE -> {
                validateFields(recipe, RECIPE_ARGUMENT_REFERENCE_FIELDS);
                ObjectNode reference = object(required(recipe, "argumentReference"));
                validateFields(reference, ARGUMENT_REFERENCE_FIELDS);
                yield new ArgumentReferenceRecipe(enumValue(reference, "argument", ExpressionArgument.class));
            }
            case SCOPED_FIELD_REFERENCE -> {
                validateFields(recipe, RECIPE_SCOPED_FIELD_REFERENCE_FIELDS);
                ObjectNode reference = object(required(recipe, "scopedFieldReference"));
                validateFields(reference, SCOPED_FIELD_REFERENCE_FIELDS);
                yield new ScopedFieldReferenceRecipe(
                        enumValue(reference, "side", RelationshipJoinSide.class),
                        text(reference, "field"));
            }
            case PROPERTY_LOOKUP -> {
                validateFields(recipe, RECIPE_PROPERTY_LOOKUP_FIELDS);
                ObjectNode lookup = object(required(recipe, "propertyLookup"));
                validateFields(lookup, PROPERTY_LOOKUP_FIELDS);
                yield new PropertyLookupRecipe(
                        text(lookup, "table"),
                        text(lookup, "property"),
                        expressionRecipe(required(lookup, "key"), budget));
            }
        };
    }

    private static List<ExpressionRecipe> expressionRecipes(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<ExpressionRecipe> recipes = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            recipes.add(expressionRecipe(element, budget));
        }
        return List.copyOf(recipes);
    }

    private static TypedLiteral typedLiteral(JsonNode node)
    {
        ObjectNode literal = object(node);
        validateFields(literal, TYPED_LITERAL_FIELDS);
        return new TypedLiteral(
                text(literal, "typeSignature"),
                enumValue(literal, "encoding", LiteralEncoding.class),
                text(literal, "value"));
    }

    private static List<VirtualTableDefinition> virtualTables(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<VirtualTableDefinition> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode table = object(element);
            validateFields(table, VIRTUAL_TABLE_FIELDS);
            definitions.add(new VirtualTableDefinition(
                    text(table, "name"),
                    relationReference(required(table, "source")),
                    virtualProjections(required(table, "projections"), budget)));
        }
        return List.copyOf(definitions);
    }

    private static RelationReference relationReference(JsonNode node)
    {
        ObjectNode reference = object(node);
        validateFields(reference, RELATION_REFERENCE_FIELDS);
        return new RelationReference(enumValue(reference, "kind", RelationKind.class), text(reference, "name"));
    }

    private static List<VirtualProjection> virtualProjections(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<VirtualProjection> projections = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode projection = object(element);
            validateFields(projection, VIRTUAL_PROJECTION_FIELDS);
            projections.add(new VirtualProjection(text(projection, "name"), text(projection, "sourceField"), bool(projection, "starVisible")));
        }
        return List.copyOf(projections);
    }

    private static List<SavedQueryReference> savedQueries(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<SavedQueryReference> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode savedQuery = object(element);
            validateFields(savedQuery, SAVED_QUERY_FIELDS);
            definitions.add(new SavedQueryReference(
                    text(savedQuery, "name"),
                    text(savedQuery, "queryId"),
                    relationReference(required(savedQuery, "target")),
                    referencedFields(required(savedQuery, "fields"), budget)));
        }
        return List.copyOf(definitions);
    }

    private static List<MaterializedViewReference> materializedViews(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<MaterializedViewReference> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode view = object(element);
            validateFields(view, MATERIALIZED_VIEW_FIELDS);
            definitions.add(new MaterializedViewReference(
                    text(view, "name"),
                    qualifiedName(required(view, "physicalView")),
                    referencedFields(required(view, "fields"), budget)));
        }
        return List.copyOf(definitions);
    }

    private static List<ReferencedField> referencedFields(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<ReferencedField> fields = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode field = object(element);
            validateFields(field, REFERENCED_FIELD_FIELDS);
            fields.add(new ReferencedField(
                    text(field, "name"),
                    text(field, "trinoTypeSignature"),
                    enumValue(field, "logicalType", LogicalType.class),
                    bool(field, "nullable"),
                    bool(field, "starVisible")));
        }
        return List.copyOf(fields);
    }

    private static List<FunctionCapabilityDefinition> functions(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<FunctionCapabilityDefinition> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode function = object(element);
            validateFields(function, FUNCTION_FIELDS, REQUIRED_FUNCTION_FIELDS);
            definitions.add(new FunctionCapabilityDefinition(
                    text(function, "name"),
                    enumValue(function, "kind", FunctionKind.class),
                    enumValue(function, "implementation", FunctionImplementation.class),
                    physicalIdentifiers(required(function, "trinoName"), budget),
                    function.has("rewrite") ? Optional.of(enumValue(function, "rewrite", FunctionRewrite.class)) : Optional.empty(),
                    functionSignatures(required(function, "signatures"), budget),
                    bool(function, "deterministic"),
                    bool(function, "supportsDistinct"),
                    bool(function, "supportsOrderBy"),
                    bool(function, "supportsFilter"),
                    bool(function, "supportsWindow")));
        }
        return List.copyOf(definitions);
    }

    private static List<PhysicalIdentifier> physicalIdentifiers(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<PhysicalIdentifier> identifiers = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            identifiers.add(physicalIdentifier(element));
        }
        return List.copyOf(identifiers);
    }

    private static List<FunctionSignature> functionSignatures(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<FunctionSignature> signatures = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode signature = object(element);
            validateFields(signature, FUNCTION_SIGNATURE_FIELDS);
            signatures.add(new FunctionSignature(
                    strings(required(signature, "argumentTypes"), budget),
                    text(signature, "returnType"),
                    bool(signature, "variadic")));
        }
        return List.copyOf(signatures);
    }

    private static List<String> strings(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<String> values = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            if (!element.isTextual()) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
            values.add(element.textValue());
        }
        return List.copyOf(values);
    }

    private static List<SemanticModifierDefault> modifierDefaults(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<SemanticModifierDefault> modifiers = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode modifier = object(element);
            if (modifier.has("sessionProperty")) {
                validateFields(modifier, MODIFIER_FIELDS);
            }
            else {
                validateFields(modifier, MODIFIER_FIELDS_WITHOUT_SESSION_PROPERTY);
            }
            modifiers.add(new SemanticModifierDefault(
                    text(modifier, "name"),
                    enumValue(modifier, "behavior", ModifierBehavior.class),
                    typedLiteral(required(modifier, "defaultValue")),
                    modifier.has("sessionProperty") ? physicalIdentifiers(required(modifier, "sessionProperty"), budget) : List.of()));
        }
        return List.copyOf(modifiers);
    }

    private static List<LazyTableDefinition> lazyTables(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<LazyTableDefinition> definitions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode table = object(element);
            validateFields(table, LAZY_TABLE_FIELDS);
            definitions.add(new LazyTableDefinition(
                    text(table, "table"),
                    text(table, "name"),
                    strings(required(table, "relationshipPath"), budget),
                    lazyProjections(required(table, "projections"), budget)));
        }
        return List.copyOf(definitions);
    }

    private static List<LazyProjectionDefinition> lazyProjections(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<LazyProjectionDefinition> projections = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode projection = object(element);
            validateFields(projection, LAZY_PROJECTION_FIELDS);
            projections.add(new LazyProjectionDefinition(
                    text(projection, "name"),
                    text(projection, "trinoTypeSignature"),
                    enumValue(projection, "logicalType", LogicalType.class),
                    bool(projection, "nullable"),
                    bool(projection, "starVisible"),
                    expressionRecipe(required(projection, "recipe"), budget)));
        }
        return List.copyOf(projections);
    }

    private static List<ActionReference> actions(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<ActionReference> actions = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode action = object(element);
            validateFields(action, ACTION_FIELDS);
            actions.add(new ActionReference(
                    text(action, "name"),
                    text(action, "actionId"),
                    text(action, "table"),
                    semanticEntityRepresentation(required(action, "representation"), budget)));
        }
        return List.copyOf(actions);
    }

    private static List<CohortReference> cohorts(JsonNode node, CollectionBudget budget)
    {
        ArrayNode array = array(node, budget);
        List<CohortReference> cohorts = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            ObjectNode cohort = object(element);
            validateFields(cohort, COHORT_FIELDS);
            cohorts.add(new CohortReference(
                    text(cohort, "name"),
                    text(cohort, "cohortId"),
                    text(cohort, "table"),
                    semanticEntityRepresentation(required(cohort, "representation"), budget)));
        }
        return List.copyOf(cohorts);
    }

    private static HogQlSemanticCatalogSnapshot.SemanticEntityRepresentation semanticEntityRepresentation(JsonNode node, CollectionBudget budget)
    {
        ObjectNode representation = object(node);
        SemanticEntityKind kind = enumValue(representation, "kind", SemanticEntityKind.class);
        return switch (kind) {
            case PREDICATE -> {
                validateFields(representation, PREDICATE_REPRESENTATION_FIELDS);
                yield new PredicateRepresentation(expressionRecipe(required(representation, "predicate"), budget));
            }
            case RELATION -> {
                validateFields(representation, RELATION_REPRESENTATION_FIELDS);
                ObjectNode membership = object(required(representation, "relation"));
                validateFields(membership, RELATION_MEMBERSHIP_FIELDS);
                yield new RelationMembershipRepresentation(new RelationMembershipRecipe(
                        relationReference(required(membership, "relation")),
                        text(membership, "sourceField"),
                        text(membership, "targetField")));
            }
        };
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

    private static void validateFields(ObjectNode object, Set<String> allowedFields, Set<String> requiredFields)
    {
        Iterator<String> fieldNames = object.fieldNames();
        while (fieldNames.hasNext()) {
            if (!allowedFields.contains(fieldNames.next())) {
                throw failure(DecodeFailure.INVALID_PAYLOAD);
            }
        }
        if (!requiredFields.stream().allMatch(object::has)) {
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
