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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public record HogQlSemanticCatalogSnapshot(
        int protocolVersion,
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        PhysicalIdentifier catalog,
        long generation,
        List<LogicalTableDefinition> logicalTables,
        List<ExpressionFieldDefinition> expressionFields,
        List<VirtualTableDefinition> virtualTables,
        List<SavedQueryReference> savedQueries,
        List<MaterializedViewReference> materializedViews,
        List<FunctionCapabilityDefinition> functions,
        List<SemanticModifierDefault> modifierDefaults)
{
    public static final int PROTOCOL_VERSION = 1;
    public static final int SCHEMA_VERSION = 2;
    private static final int MAX_SEMANTIC_DEFINITIONS = 10_000;
    private static final int MAX_RECIPE_DEPTH = 64;
    private static final int MAX_RECIPE_NODES = 4_096;
    private static final int MAX_RELATION_DEPTH = 64;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public HogQlSemanticCatalogSnapshot
    {
        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported HogQL semantic catalog protocol");
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported HogQL semantic catalog schema");
        }
        if (generation <= 0) {
            throw new IllegalArgumentException("HogQL semantic catalog generation must be positive");
        }
        languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        catalog = requireNonNull(catalog, "catalog is null");
        logicalTables = copy(logicalTables, "logicalTables");
        expressionFields = copy(expressionFields, "expressionFields");
        virtualTables = copy(virtualTables, "virtualTables");
        savedQueries = copy(savedQueries, "savedQueries");
        materializedViews = copy(materializedViews, "materializedViews");
        functions = copy(functions, "functions");
        modifierDefaults = copy(modifierDefaults, "modifierDefaults");
        int definitions = expressionFields.size() + virtualTables.size() + savedQueries.size() + materializedViews.size() + functions.size() + modifierDefaults.size();
        if (definitions > MAX_SEMANTIC_DEFINITIONS) {
            throw new IllegalArgumentException("semantic metadata exceeds definition limit");
        }

        Map<String, LogicalTableDefinition> tables = indexTables(catalog, logicalTables);
        validateLogicalReferences(tables);
        Set<String> declaredFunctions = validateFunctions(functions);
        Map<String, ExpressionFieldDefinition> expressions = indexExpressionFields(expressionFields, tables);
        validateExpressionRecipes(expressionFields, tables, expressions, declaredFunctions);
        validateModifiers(modifierDefaults);
        validateRelations(catalog, tables, expressions, virtualTables, savedQueries, materializedViews);
    }

    public HogQlSemanticCatalogSnapshot(
            int schemaVersion,
            HogQlLanguageVersion languageVersion,
            PhysicalIdentifier catalog,
            long generation,
            List<LogicalTableDefinition> logicalTables)
    {
        this(PROTOCOL_VERSION, schemaVersion, languageVersion, catalog, generation, logicalTables, List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }

    public Optional<LogicalTableDefinition> logicalTable(String name)
    {
        String canonical = canonical(name, "logical table");
        return logicalTables.stream().filter(table -> canonical(table.name(), "logical table").equals(canonical)).findFirst();
    }

    private static Map<String, LogicalTableDefinition> indexTables(PhysicalIdentifier catalog, List<LogicalTableDefinition> definitions)
    {
        Map<String, LogicalTableDefinition> tables = new HashMap<>();
        for (LogicalTableDefinition table : definitions) {
            if (!table.physicalTable().catalog().equals(catalog)) {
                throw new IllegalArgumentException("logical table physical reference uses another catalog");
            }
            if (tables.put(canonical(table.name(), "logical table"), table) != null) {
                throw new IllegalArgumentException("duplicate logical table");
            }
            Set<String> members = new HashSet<>();
            table.fields().forEach(field -> addUnique(members, field.name(), "duplicate logical member"));
            table.properties().forEach(property -> addUnique(members, property.name(), "duplicate logical member"));
            table.relationships().forEach(relationship -> addUnique(members, relationship.name(), "duplicate logical member"));
        }
        return Map.copyOf(tables);
    }

    private static void validateLogicalReferences(Map<String, LogicalTableDefinition> tables)
    {
        for (LogicalTableDefinition table : tables.values()) {
            Set<String> fields = fieldNames(table);
            for (PropertyDefinition property : table.properties()) {
                requireReference(fields, property.sourceField(), "property has unknown source field");
            }
            for (RelationshipDefinition relationship : table.relationships()) {
                LogicalTableDefinition target = tables.get(canonical(relationship.targetTable(), "relationship target table"));
                if (target == null) {
                    throw new IllegalArgumentException("relationship has unknown target table");
                }
                Set<String> targetFields = fieldNames(target);
                for (JoinKey joinKey : relationship.joinKeys()) {
                    requireReference(fields, joinKey.sourceField(), "relationship has unknown source field");
                    requireReference(targetFields, joinKey.targetField(), "relationship has unknown target field");
                }
            }
        }
    }

    private static Set<String> validateFunctions(List<FunctionCapabilityDefinition> definitions)
    {
        Set<String> functions = new HashSet<>();
        for (FunctionCapabilityDefinition function : definitions) {
            addUnique(functions, function.name(), "duplicate function");
            if (function.implementation() == FunctionImplementation.REWRITE && !function.trinoName().isEmpty()) {
                throw new IllegalArgumentException("rewrite function cannot name a Trino function");
            }
            if (function.implementation() != FunctionImplementation.REWRITE && function.trinoName().isEmpty()) {
                throw new IllegalArgumentException("function must name a Trino function");
            }
        }
        return Set.copyOf(functions);
    }

    private static Map<String, ExpressionFieldDefinition> indexExpressionFields(List<ExpressionFieldDefinition> definitions, Map<String, LogicalTableDefinition> tables)
    {
        Map<String, ExpressionFieldDefinition> fields = new HashMap<>();
        for (ExpressionFieldDefinition field : definitions) {
            LogicalTableDefinition table = tables.get(canonical(field.table(), "expression field table"));
            if (table == null) {
                throw new IllegalArgumentException("expression field references an unknown table");
            }
            Set<String> members = new HashSet<>(fieldNames(table));
            table.properties().forEach(property -> members.add(canonical(property.name(), "property name")));
            table.relationships().forEach(relationship -> members.add(canonical(relationship.name(), "relationship name")));
            if (members.contains(canonical(field.name(), "expression field name"))) {
                throw new IllegalArgumentException("expression field conflicts with an existing logical member");
            }
            if (fields.put(expressionKey(field.table(), field.name()), field) != null) {
                throw new IllegalArgumentException("duplicate expression field");
            }
        }
        return Map.copyOf(fields);
    }

    private static void validateExpressionRecipes(
            List<ExpressionFieldDefinition> definitions,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Set<String> functions)
    {
        Map<String, List<String>> dependencies = new HashMap<>();
        RecipeCounter counter = new RecipeCounter();
        for (ExpressionFieldDefinition definition : definitions) {
            List<String> fieldDependencies = new ArrayList<>();
            validateRecipe(definition.recipe(), definition.table(), 1, counter, tables, expressions, functions, fieldDependencies);
            dependencies.put(expressionKey(definition.table(), definition.name()), List.copyOf(fieldDependencies));
        }
        Map<String, VisitState> states = new HashMap<>();
        dependencies.keySet().forEach(name -> visitDependency(name, 1, dependencies, states));
    }

    private static void validateRecipe(
            ExpressionRecipe recipe,
            String ownerTable,
            int depth,
            RecipeCounter counter,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Set<String> functions,
            List<String> dependencies)
    {
        requireNonNull(recipe, "expression recipe is null");
        if (depth > MAX_RECIPE_DEPTH) {
            throw new IllegalArgumentException("expression recipe exceeds depth limit");
        }
        counter.add();
        switch (recipe) {
            case FieldReferenceRecipe reference -> {
                if (!canonical(reference.table(), "field reference table").equals(canonical(ownerTable, "expression field table"))) {
                    throw new IllegalArgumentException("field reference crosses tables without a relationship path");
                }
                LogicalTableDefinition table = tables.get(canonical(reference.table(), "field reference table"));
                if (table == null) {
                    throw new IllegalArgumentException("field reference has unknown table");
                }
                String key = expressionKey(reference.table(), reference.field());
                if (expressions.containsKey(key)) {
                    dependencies.add(key);
                }
                else {
                    requireReference(fieldNames(table), reference.field(), "field reference has unknown field");
                }
            }
            case LiteralRecipe _ -> {}
            case FunctionCallRecipe call -> {
                requireReference(functions, call.name(), "function call references an undeclared function");
                call.arguments().forEach(argument -> validateRecipe(argument, ownerTable, depth + 1, counter, tables, expressions, functions, dependencies));
            }
            case OperatorRecipe operator -> operator.arguments().forEach(argument -> validateRecipe(argument, ownerTable, depth + 1, counter, tables, expressions, functions, dependencies));
            case CastRecipe cast -> validateRecipe(cast.expression(), ownerTable, depth + 1, counter, tables, expressions, functions, dependencies);
        }
    }

    private static void visitDependency(String name, int depth, Map<String, List<String>> dependencies, Map<String, VisitState> states)
    {
        if (depth > MAX_RECIPE_DEPTH) {
            throw new IllegalArgumentException("expression field dependency exceeds depth limit");
        }
        if (states.get(name) == VisitState.VISITING) {
            throw new IllegalArgumentException("expression field dependency cycle");
        }
        if (states.putIfAbsent(name, VisitState.VISITING) == VisitState.VISITED) {
            return;
        }
        dependencies.getOrDefault(name, List.of()).forEach(dependency -> visitDependency(dependency, depth + 1, dependencies, states));
        states.put(name, VisitState.VISITED);
    }

    private static void validateModifiers(List<SemanticModifierDefault> modifiers)
    {
        Set<String> names = new HashSet<>();
        for (SemanticModifierDefault modifier : modifiers) {
            addUnique(names, modifier.name(), "duplicate modifier");
            if (modifier.behavior() == ModifierBehavior.TRINO_SESSION_PROPERTY && modifier.sessionProperty().isEmpty()) {
                throw new IllegalArgumentException("modifier must name a session property");
            }
            if (modifier.behavior() != ModifierBehavior.TRINO_SESSION_PROPERTY && !modifier.sessionProperty().isEmpty()) {
                throw new IllegalArgumentException("modifier cannot name a session property");
            }
        }
    }

    private static void validateRelations(
            PhysicalIdentifier catalog,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            List<VirtualTableDefinition> virtualTables,
            List<SavedQueryReference> savedQueries,
            List<MaterializedViewReference> materializedViews)
    {
        Map<String, SemanticRelation> relations = new HashMap<>();
        tables.forEach((name, table) -> {
            Set<String> fields = new HashSet<>(fieldNames(table));
            expressions.values().stream()
                    .filter(expression -> canonical(expression.table(), "expression field table").equals(name))
                    .forEach(expression -> fields.add(canonical(expression.name(), "expression field name")));
            relations.put(name, new SemanticRelation(RelationKind.LOGICAL_TABLE, fields, null, null));
        });
        savedQueries.forEach(saved -> addRelation(relations, saved.name(), new SemanticRelation(RelationKind.SAVED_QUERY, referencedFields(saved.fields()), null, saved)));
        materializedViews.forEach(view -> {
            if (!view.physicalView().catalog().equals(catalog)) {
                throw new IllegalArgumentException("materialized view physical reference uses another catalog");
            }
            addRelation(relations, view.name(), new SemanticRelation(RelationKind.MATERIALIZED_VIEW, referencedFields(view.fields()), null, null));
        });
        virtualTables.forEach(virtual -> addRelation(relations, virtual.name(), new SemanticRelation(RelationKind.VIRTUAL_TABLE, null, virtual, null)));
        Map<String, VisitState> states = new HashMap<>();
        virtualTables.forEach(virtual -> resolveRelation(canonical(virtual.name(), "relation name"), relations, states, 1));
        savedQueries.forEach(saved -> resolveRelation(canonical(saved.name(), "relation name"), relations, states, 1));
    }

    private static void addRelation(Map<String, SemanticRelation> relations, String name, SemanticRelation relation)
    {
        if (relations.put(canonical(name, "relation name"), relation) != null) {
            throw new IllegalArgumentException("duplicate relation");
        }
    }

    private static Set<String> resolveRelation(String name, Map<String, SemanticRelation> relations, Map<String, VisitState> states, int depth)
    {
        if (depth > MAX_RELATION_DEPTH) {
            throw new IllegalArgumentException("virtual table reference exceeds depth limit");
        }
        SemanticRelation relation = relations.get(name);
        if (relation == null) {
            throw new IllegalArgumentException("invalid semantic relation reference");
        }
        if (relation.kind != RelationKind.VIRTUAL_TABLE && relation.kind != RelationKind.SAVED_QUERY) {
            return relation.fields;
        }
        if (states.get(name) == VisitState.VISITING) {
            throw new IllegalArgumentException("semantic relation reference cycle");
        }
        if (states.get(name) == VisitState.VISITED) {
            return requireNonNull(relation.fields, "resolved relation fields are null");
        }
        states.put(name, VisitState.VISITING);
        RelationReference reference = relation.kind == RelationKind.VIRTUAL_TABLE ? relation.virtualTable.source() : relation.savedQuery.target();
        if (relation.kind == RelationKind.SAVED_QUERY && reference.kind() == RelationKind.SAVED_QUERY) {
            throw new IllegalArgumentException("saved query target must be logical, virtual, or materialized");
        }
        SemanticRelation source = relations.get(canonical(reference.name(), "semantic relation source"));
        if (source == null || source.kind != reference.kind()) {
            throw new IllegalArgumentException("semantic relation references an unknown or mismatched source");
        }
        Set<String> sourceFields = source.kind == RelationKind.VIRTUAL_TABLE || source.kind == RelationKind.SAVED_QUERY
                ? resolveRelation(canonical(reference.name(), "semantic relation source"), relations, states, depth + 1)
                : requireNonNull(source.fields, "source relation fields are null");
        if (relation.kind == RelationKind.SAVED_QUERY) {
            if (!sourceFields.containsAll(relation.fields)) {
                throw new IllegalArgumentException("saved query declares a field missing from its target");
            }
        }
        else {
            Set<String> fields = new HashSet<>();
            for (VirtualProjection projection : relation.virtualTable.projections()) {
                requireReference(sourceFields, projection.sourceField(), "virtual table projection references an unknown source field");
                addUnique(fields, projection.name(), "duplicate projection on virtual table");
            }
            relation.fields = Set.copyOf(fields);
        }
        states.put(name, VisitState.VISITED);
        return requireNonNull(relation.fields, "resolved relation fields are null");
    }

    private static Set<String> referencedFields(List<ReferencedField> fields)
    {
        Set<String> names = new HashSet<>();
        fields.forEach(field -> addUnique(names, field.name(), "duplicate referenced field"));
        return names;
    }

    private static Set<String> fieldNames(LogicalTableDefinition table)
    {
        Set<String> fields = new HashSet<>();
        table.fields().forEach(field -> fields.add(canonical(field.name(), "logical field")));
        return fields;
    }

    private static void requireReference(Set<String> names, String name, String message)
    {
        if (!names.contains(canonical(name, "reference"))) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void addUnique(Set<String> names, String name, String message)
    {
        if (!names.add(canonical(name, "definition name"))) {
            throw new IllegalArgumentException(message);
        }
    }

    private static String expressionKey(String table, String field)
    {
        return canonical(table, "expression field table") + "." + canonical(field, "expression field name");
    }

    private static String canonical(String value, String kind)
    {
        return definition(value, kind).toLowerCase(Locale.ENGLISH);
    }

    private static String definition(String value, String kind)
    {
        requireNonNull(value, kind + " is null");
        if (value.isBlank() || value.indexOf(';') >= 0 || value.indexOf('\0') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0 || value.contains("--") || value.contains("/*") || value.contains("*/")) {
            throw new IllegalArgumentException("invalid " + kind);
        }
        return value;
    }

    private static <T> List<T> copy(List<T> values, String name)
    {
        return List.copyOf(requireNonNull(values, name + " is null"));
    }

    public record LogicalTableDefinition(String name, PhysicalQualifiedName physicalTable, List<LogicalFieldDefinition> fields, List<PropertyDefinition> properties, List<RelationshipDefinition> relationships)
    {
        public LogicalTableDefinition
        {
            name = definition(name, "logical table name");
            physicalTable = requireNonNull(physicalTable, "physicalTable is null");
            fields = copy(fields, "fields");
            properties = copy(properties, "properties");
            relationships = copy(relationships, "relationships");
        }
    }

    public record LogicalFieldDefinition(String name, PhysicalIdentifier physicalColumn, String trinoTypeSignature, LogicalType logicalType, boolean nullable, boolean starVisible)
    {
        public LogicalFieldDefinition
        {
            name = definition(name, "logical field name");
            physicalColumn = requireNonNull(physicalColumn, "physicalColumn is null");
            trinoTypeSignature = definition(trinoTypeSignature, "Trino type signature");
            logicalType = requireNonNull(logicalType, "logicalType is null");
        }
    }

    public record PropertyDefinition(String name, String sourceField, PropertyStorage storage, LogicalType logicalType, boolean nullable)
    {
        public PropertyDefinition
        {
            name = definition(name, "property name");
            sourceField = definition(sourceField, "property source field");
            storage = requireNonNull(storage, "storage is null");
            logicalType = requireNonNull(logicalType, "logicalType is null");
        }
    }

    public record RelationshipDefinition(String name, String targetTable, RelationshipCardinality cardinality, List<JoinKey> joinKeys)
    {
        public RelationshipDefinition
        {
            name = definition(name, "relationship name");
            targetTable = definition(targetTable, "relationship target table");
            cardinality = requireNonNull(cardinality, "cardinality is null");
            joinKeys = copy(joinKeys, "joinKeys");
            if (joinKeys.isEmpty()) {
                throw new IllegalArgumentException("relationship must have at least one join key");
            }
        }
    }

    public record JoinKey(String sourceField, String targetField)
    {
        public JoinKey
        {
            sourceField = definition(sourceField, "relationship source field");
            targetField = definition(targetField, "relationship target field");
        }
    }

    public record ExpressionFieldDefinition(String table, String name, String trinoTypeSignature, LogicalType logicalType, boolean nullable, boolean starVisible, ExpressionRecipe recipe)
    {
        public ExpressionFieldDefinition
        {
            table = definition(table, "expression field table");
            name = definition(name, "expression field name");
            trinoTypeSignature = definition(trinoTypeSignature, "expression field type signature");
            logicalType = requireNonNull(logicalType, "logicalType is null");
            recipe = requireNonNull(recipe, "recipe is null");
        }
    }

    public sealed interface ExpressionRecipe
            permits CastRecipe,
                    FieldReferenceRecipe,
                    FunctionCallRecipe,
                    LiteralRecipe,
                    OperatorRecipe
    {
        ExpressionRecipeKind kind();
    }

    public record FieldReferenceRecipe(String table, String field)
            implements ExpressionRecipe
    {
        public FieldReferenceRecipe
        {
            table = definition(table, "field reference table");
            field = definition(field, "field reference field");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.FIELD_REFERENCE;
        }
    }

    public record LiteralRecipe(TypedLiteral literal)
            implements ExpressionRecipe
    {
        public LiteralRecipe
        {
            literal = requireNonNull(literal, "literal is null");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.LITERAL;
        }
    }

    public record FunctionCallRecipe(String name, List<ExpressionRecipe> arguments)
            implements ExpressionRecipe
    {
        public FunctionCallRecipe
        {
            name = definition(name, "function name");
            arguments = copy(arguments, "arguments");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.FUNCTION_CALL;
        }
    }

    public record OperatorRecipe(SemanticOperator operator, List<ExpressionRecipe> arguments)
            implements ExpressionRecipe
    {
        public OperatorRecipe
        {
            operator = requireNonNull(operator, "operator is null");
            arguments = copy(arguments, "arguments");
            if (arguments.isEmpty()) {
                throw new IllegalArgumentException("operator recipe requires arguments");
            }
            int expectedArguments = switch (operator) {
                case NOT, NEGATE, IS_NULL, IS_NOT_NULL -> 1;
                default -> 2;
            };
            if (arguments.size() != expectedArguments) {
                throw new IllegalArgumentException("invalid operator arity");
            }
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.OPERATOR;
        }
    }

    public record CastRecipe(ExpressionRecipe expression, String targetTypeSignature)
            implements ExpressionRecipe
    {
        public CastRecipe
        {
            expression = requireNonNull(expression, "expression is null");
            targetTypeSignature = definition(targetTypeSignature, "cast target type signature");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.CAST;
        }
    }

    public record TypedLiteral(String typeSignature, LiteralEncoding encoding, String value)
    {
        private static final Pattern DECIMAL = Pattern.compile("[+-]?(0|[1-9][0-9]*)(\\.[0-9]+)?");

        public TypedLiteral
        {
            typeSignature = definition(typeSignature, "literal type signature");
            encoding = requireNonNull(encoding, "encoding is null");
            value = requireNonNull(value, "value is null");
            if (value.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("literal value contains NUL");
            }
            try {
                switch (encoding) {
                    case NULL -> {
                        if (!value.isEmpty()) {
                            throw new IllegalArgumentException();
                        }
                    }
                    case STRING -> {}
                    case BOOLEAN -> {
                        if (!value.equals("true") && !value.equals("false")) {
                            throw new IllegalArgumentException();
                        }
                    }
                    case INTEGER -> Long.parseLong(value);
                    case DECIMAL -> {
                        if (!DECIMAL.matcher(value).matches()) {
                            throw new IllegalArgumentException();
                        }
                    }
                    case FLOAT -> {
                        if (!Double.isFinite(Double.parseDouble(value))) {
                            throw new IllegalArgumentException();
                        }
                    }
                    case JSON -> validateJson(value);
                    case BASE64 -> Base64.getDecoder().decode(value);
                }
            }
            catch (IOException | IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid typed literal");
            }
        }

        private static void validateJson(String value)
                throws IOException
        {
            try (JsonParser parser = JSON_MAPPER.createParser(value)) {
                if (JSON_MAPPER.readTree(parser) == null || parser.nextToken() != null) {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    public record VirtualTableDefinition(String name, RelationReference source, List<VirtualProjection> projections)
    {
        public VirtualTableDefinition
        {
            name = definition(name, "virtual table name");
            source = requireNonNull(source, "source is null");
            projections = copy(projections, "projections");
        }
    }

    public record RelationReference(RelationKind kind, String name)
    {
        public RelationReference
        {
            kind = requireNonNull(kind, "kind is null");
            name = definition(name, "semantic relation source");
        }
    }

    public record VirtualProjection(String name, String sourceField, boolean starVisible)
    {
        public VirtualProjection
        {
            name = definition(name, "virtual projection name");
            sourceField = definition(sourceField, "virtual projection source field");
        }
    }

    public record SavedQueryReference(String name, String queryId, RelationReference target, List<ReferencedField> fields)
    {
        public SavedQueryReference
        {
            name = definition(name, "saved query name");
            queryId = definition(queryId, "saved query ID");
            target = requireNonNull(target, "target is null");
            fields = copy(fields, "fields");
        }
    }

    public record MaterializedViewReference(String name, PhysicalQualifiedName physicalView, List<ReferencedField> fields)
    {
        public MaterializedViewReference
        {
            name = definition(name, "materialized view name");
            physicalView = requireNonNull(physicalView, "physicalView is null");
            fields = copy(fields, "fields");
        }
    }

    public record ReferencedField(String name, String trinoTypeSignature, LogicalType logicalType, boolean nullable, boolean starVisible)
    {
        public ReferencedField
        {
            name = definition(name, "referenced field name");
            trinoTypeSignature = definition(trinoTypeSignature, "referenced field type signature");
            logicalType = requireNonNull(logicalType, "logicalType is null");
        }
    }

    public record FunctionCapabilityDefinition(String name, FunctionKind kind, FunctionImplementation implementation, List<PhysicalIdentifier> trinoName, List<FunctionSignature> signatures, boolean deterministic, boolean supportsDistinct, boolean supportsOrderBy, boolean supportsFilter, boolean supportsWindow)
    {
        public FunctionCapabilityDefinition
        {
            name = definition(name, "function name");
            kind = requireNonNull(kind, "kind is null");
            implementation = requireNonNull(implementation, "implementation is null");
            trinoName = copy(trinoName, "trinoName");
            signatures = copy(signatures, "signatures");
            if (signatures.isEmpty()) {
                throw new IllegalArgumentException("function must include signatures");
            }
        }
    }

    public record FunctionSignature(List<String> argumentTypes, String returnType, boolean variadic)
    {
        public FunctionSignature
        {
            argumentTypes = copy(argumentTypes, "argumentTypes").stream().map(value -> definition(value, "function argument type")).toList();
            returnType = definition(returnType, "function return type");
        }
    }

    public record SemanticModifierDefault(String name, ModifierBehavior behavior, TypedLiteral defaultValue, List<PhysicalIdentifier> sessionProperty)
    {
        public SemanticModifierDefault
        {
            name = definition(name, "modifier name");
            behavior = requireNonNull(behavior, "behavior is null");
            defaultValue = requireNonNull(defaultValue, "defaultValue is null");
            sessionProperty = copy(sessionProperty, "sessionProperty");
        }
    }

    public record PhysicalQualifiedName(PhysicalIdentifier catalog, PhysicalIdentifier schema, PhysicalIdentifier table)
    {
        public PhysicalQualifiedName
        {
            catalog = requireNonNull(catalog, "catalog is null");
            schema = requireNonNull(schema, "schema is null");
            table = requireNonNull(table, "table is null");
        }
    }

    public record PhysicalIdentifier(String value, boolean delimited)
    {
        private static final Pattern UNDELIMITED = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

        public PhysicalIdentifier
        {
            value = definition(value, "physical identifier");
            if (!delimited && !UNDELIMITED.matcher(value).matches()) {
                throw new IllegalArgumentException("invalid physical identifier");
            }
            if (!delimited) {
                value = value.toLowerCase(Locale.ENGLISH);
            }
        }
    }

    public enum ExpressionRecipeKind
    {
        FIELD_REFERENCE, LITERAL, FUNCTION_CALL, OPERATOR, CAST
    }

    public enum SemanticOperator
    {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS, EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, AND, OR, NOT, NEGATE, IS_NULL, IS_NOT_NULL
    }

    public enum LiteralEncoding
    {
        NULL, STRING, BOOLEAN, INTEGER, DECIMAL, FLOAT, JSON, BASE64
    }

    public enum RelationKind
    {
        LOGICAL_TABLE, VIRTUAL_TABLE, SAVED_QUERY, MATERIALIZED_VIEW
    }

    public enum FunctionKind
    {
        SCALAR, AGGREGATE, WINDOW, TABLE
    }

    public enum FunctionImplementation
    {
        STOCK, UDF, REWRITE
    }

    public enum ModifierBehavior
    {
        COMPILER, TRINO_SESSION_PROPERTY, SAFE_NOOP, UNSUPPORTED
    }

    public enum LogicalType
    {
        UNKNOWN, BOOLEAN, INTEGER, FLOAT, DECIMAL, STRING, DATE, TIMESTAMP, INTERVAL, UUID, JSON, ARRAY, MAP, ROW
    }

    public enum PropertyStorage
    {
        JSON_OBJECT, MAP
    }

    public enum RelationshipCardinality
    {
        ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY
    }

    private enum VisitState
    {
        VISITING, VISITED
    }

    private static final class RecipeCounter
    {
        private int nodes;

        private void add()
        {
            if (++nodes > MAX_RECIPE_NODES) {
                throw new IllegalArgumentException("expression recipes exceed node limit");
            }
        }
    }

    private static final class SemanticRelation
    {
        private final RelationKind kind;
        private Set<String> fields;
        private final VirtualTableDefinition virtualTable;
        private final SavedQueryReference savedQuery;

        private SemanticRelation(RelationKind kind, Set<String> fields, VirtualTableDefinition virtualTable, SavedQueryReference savedQuery)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.fields = fields == null ? null : Set.copyOf(fields);
            this.virtualTable = virtualTable;
            this.savedQuery = savedQuery;
        }
    }
}
