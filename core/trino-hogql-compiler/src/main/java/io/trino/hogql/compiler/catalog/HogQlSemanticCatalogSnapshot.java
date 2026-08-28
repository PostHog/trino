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
        List<SemanticModifierDefault> modifierDefaults,
        List<LazyTableDefinition> lazyTables,
        List<ActionReference> actions,
        List<CohortReference> cohorts)
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
        lazyTables = copy(lazyTables, "lazyTables");
        actions = copy(actions, "actions");
        cohorts = copy(cohorts, "cohorts");
        int definitions = expressionFields.size() + virtualTables.size() + savedQueries.size() + materializedViews.size() + functions.size() + modifierDefaults.size() + lazyTables.size() + actions.size() + cohorts.size();
        if (definitions > MAX_SEMANTIC_DEFINITIONS) {
            throw new IllegalArgumentException("semantic metadata exceeds definition limit");
        }

        Map<String, LogicalTableDefinition> tables = indexTables(catalog, logicalTables);
        validateLogicalReferences(tables);
        Map<String, FunctionCapabilityDefinition> declaredFunctions = validateFunctions(functions);
        Map<String, ExpressionFieldDefinition> expressions = indexExpressionFields(expressionFields, tables);
        RecipeCounter counter = new RecipeCounter();
        validatePropertyRecipes(tables, expressions, declaredFunctions, counter);
        validateExpressionRecipes(expressionFields, tables, expressions, declaredFunctions, counter);
        validateModifiers(modifierDefaults);
        validateRelationshipPredicates(tables, expressions, declaredFunctions, counter);
        Map<String, SemanticRelation> relations = validateRelations(catalog, tables, expressions, virtualTables, savedQueries, materializedViews);
        validateLazyTables(lazyTables, tables, expressions, declaredFunctions, counter);
        validateSemanticEntities(actions, cohorts, tables, expressions, declaredFunctions, relations, counter);
    }

    public HogQlSemanticCatalogSnapshot(
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
        this(protocolVersion, schemaVersion, languageVersion, catalog, generation, logicalTables, expressionFields, virtualTables, savedQueries, materializedViews, functions, modifierDefaults, List.of(), List.of(), List.of());
    }

    public HogQlSemanticCatalogSnapshot(
            int schemaVersion,
            HogQlLanguageVersion languageVersion,
            PhysicalIdentifier catalog,
            long generation,
            List<LogicalTableDefinition> logicalTables)
    {
        this(PROTOCOL_VERSION, schemaVersion, languageVersion, catalog, generation, logicalTables, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
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
            Set<String> properties = new HashSet<>();
            for (PropertyDefinition property : table.properties()) {
                String name = canonical(property.name(), "property name");
                if (!properties.add(name)) {
                    throw new IllegalArgumentException("duplicate logical member");
                }
                if (members.contains(name) && !name.equals(canonical(property.sourceField(), "property source field"))) {
                    throw new IllegalArgumentException("duplicate logical member");
                }
                members.add(name);
            }
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

    private static void validatePropertyRecipes(
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            RecipeCounter counter)
    {
        for (LogicalTableDefinition table : tables.values()) {
            for (PropertyDefinition property : table.properties()) {
                if (property.lookupRecipe().isEmpty()) {
                    continue;
                }
                Map<ExpressionArgument, Integer> arguments = new HashMap<>();
                validateRecipe(
                        property.lookupRecipe().orElseThrow(),
                        1,
                        counter,
                        new RecipeValidationContext(table.name(), tables, expressions, functions, null, false, false, arguments, Map.of()));
                if (!arguments.containsKey(ExpressionArgument.PROPERTY_SOURCE) || !arguments.containsKey(ExpressionArgument.PROPERTY_KEY)) {
                    throw new IllegalArgumentException("property lookup recipe must reference source and key arguments");
                }
            }
        }
    }

    private static void validateRelationshipPredicates(
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            RecipeCounter counter)
    {
        for (LogicalTableDefinition source : tables.values()) {
            for (RelationshipDefinition relationship : source.relationships()) {
                if (relationship.joinPredicate().isEmpty()) {
                    continue;
                }
                LogicalTableDefinition target = tables.get(canonical(relationship.targetTable(), "relationship target table"));
                validateRecipe(
                        relationship.joinPredicate().orElseThrow(),
                        1,
                        counter,
                        new RecipeValidationContext(
                                source.name(),
                                tables,
                                expressions,
                                functions,
                                null,
                                false,
                                true,
                                null,
                                Map.of(RelationshipJoinSide.SOURCE, source.name(), RelationshipJoinSide.TARGET, target.name())));
            }
        }
    }

    private static Map<String, FunctionCapabilityDefinition> validateFunctions(List<FunctionCapabilityDefinition> definitions)
    {
        Map<String, FunctionCapabilityDefinition> functions = new HashMap<>();
        for (FunctionCapabilityDefinition function : definitions) {
            if (functions.put(canonical(function.name(), "function name"), function) != null) {
                throw new IllegalArgumentException("duplicate function");
            }
            if (function.implementation() == FunctionImplementation.REWRITE && !function.trinoName().isEmpty()) {
                throw new IllegalArgumentException("rewrite function cannot name a Trino function");
            }
            if (function.implementation() == FunctionImplementation.REWRITE) {
                if (function.rewrite().isEmpty()) {
                    throw new IllegalArgumentException("rewrite function must declare a rewrite");
                }
                FunctionRewrite rewrite = function.rewrite().orElseThrow();
                if (rewriteFunctionKind(rewrite) != function.kind()) {
                    throw new IllegalArgumentException("rewrite function kind must be " + rewriteFunctionKind(rewrite).name().toLowerCase(Locale.ENGLISH));
                }
                if (!function.deterministic()) {
                    throw new IllegalArgumentException("rewrite function must be deterministic");
                }
                if (function.supportsDistinct()) {
                    throw new IllegalArgumentException("rewrite function cannot support DISTINCT");
                }
                if (function.supportsOrderBy()) {
                    throw new IllegalArgumentException("rewrite function cannot support ORDER BY");
                }
                if (function.supportsFilter()) {
                    throw new IllegalArgumentException("rewrite function cannot support FILTER");
                }
                if (function.supportsWindow()) {
                    throw new IllegalArgumentException("rewrite function cannot support window invocation");
                }
                if (function.signatures().stream().anyMatch(signature -> !validRewriteSignature(rewrite, signature))) {
                    throw new IllegalArgumentException("rewrite function declares an invalid signature");
                }
                if ((rewrite == FunctionRewrite.IS_NULL || rewrite == FunctionRewrite.IS_NOT_NULL) &&
                        function.signatures().stream().anyMatch(signature -> !signature.returnType().equalsIgnoreCase("boolean"))) {
                    throw new IllegalArgumentException("null predicate rewrite function signatures must return boolean");
                }
            }
            else {
                if (function.rewrite().isPresent()) {
                    throw new IllegalArgumentException("non-rewrite function cannot declare a rewrite");
                }
                if (function.trinoName().isEmpty()) {
                    throw new IllegalArgumentException("function must name a Trino function");
                }
            }
        }
        return Map.copyOf(functions);
    }

    private static FunctionKind rewriteFunctionKind(FunctionRewrite rewrite)
    {
        return switch (rewrite) {
            case ANY_IF, ARG_MAX_IF, AVG_IF, COUNT_DISTINCT, COUNT_IF, GROUP_ARRAY_IF,
                    GROUP_UNIQ_ARRAY, GROUP_UNIQ_ARRAY_IF, MAX_IF, MIN_IF, SUM_IF,
                    UNIQ_EXACT, UNIQ_EXACT_IF, UNIQ_IF -> FunctionKind.AGGREGATE;
            default -> FunctionKind.SCALAR;
        };
    }

    private static boolean validRewriteSignature(FunctionRewrite rewrite, FunctionSignature signature)
    {
        return switch (rewrite) {
            case ARRAY_SUM, CAST_BIGINT, CAST_DATE, CAST_DOUBLE, CAST_VARCHAR, FLOAT_OR_ZERO, RANGE,
                    DATE_TRUNC_DAY, DATE_TRUNC_HOUR, DATE_TRUNC_MONTH, DATE_TRUNC_WEEK,
                    GROUP_UNIQ_ARRAY, INTERVAL_DAY, IS_NOT_NULL, IS_NULL, COUNT_IF,
                    JSON_KEYS_AND_VALUES_RAW, NOT, PARSE_TIMESTAMP, TO_UNIX_TIMESTAMP,
                    COUNT_DISTINCT, UNIQ_EXACT ->
                !signature.variadic() && signature.argumentTypes().size() == 1;
            case ADD_DAYS, ADD_MONTHS, AND, ANY_IF, ARRAY_ELEMENT, ARRAY_FILTER, ARRAY_FIRST, ARRAY_MAP,
                    AVG_IF, DECIMAL_CAST, FLOAT_OR_DEFAULT, GREATER, GROUP_ARRAY_IF, HAS,
                    GROUP_UNIQ_ARRAY_IF, JSON_EXTRACT_TYPED, JSON_KEYS_AND_VALUES,
                    INT_DIV, LIKE, MAX_IF, MIN_IF, REGEX_EXTRACT, SPLIT_CHAR, SUM_IF,
                    TUPLE_ELEMENT, UNIQ_EXACT_IF, UNIQ_IF ->
                !signature.variadic() && signature.argumentTypes().size() == 2;
            case ARG_MAX_IF, REGEX_REPLACE_ALL -> !signature.variadic() && signature.argumentTypes().size() == 3;
            case CAST_TIMESTAMP -> !signature.variadic() &&
                    (signature.argumentTypes().size() == 1 || signature.argumentTypes().size() == 2);
            case DATE_ADD -> !signature.variadic() &&
                    (signature.argumentTypes().size() == 2 || signature.argumentTypes().size() == 3);
            case JSON_EXTRACT_FLOAT, JSON_EXTRACT_INT, JSON_EXTRACT_RAW, JSON_EXTRACT_STRING ->
                signature.variadic() && signature.argumentTypes().size() == 3;
            case JSON_LENGTH -> (!signature.variadic() && signature.argumentTypes().size() == 1) ||
                    (signature.variadic() && signature.argumentTypes().size() == 3);
            case MULTI_IF -> signature.variadic() && signature.argumentTypes().size() == 4;
            case TODAY -> !signature.variadic() && signature.argumentTypes().isEmpty();
        };
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
            Map<String, FunctionCapabilityDefinition> functions,
            RecipeCounter counter)
    {
        Map<String, List<String>> dependencies = new HashMap<>();
        for (ExpressionFieldDefinition definition : definitions) {
            List<String> fieldDependencies = new ArrayList<>();
            validateRecipe(
                    definition.recipe(),
                    1,
                    counter,
                    new RecipeValidationContext(definition.table(), tables, expressions, functions, fieldDependencies, true, true, null, Map.of()));
            dependencies.put(expressionKey(definition.table(), definition.name()), List.copyOf(fieldDependencies));
        }
        Map<String, VisitState> states = new HashMap<>();
        dependencies.keySet().forEach(name -> visitDependency(name, 1, dependencies, states));
    }

    private static void validateRecipe(
            ExpressionRecipe recipe,
            int depth,
            RecipeCounter counter,
            RecipeValidationContext context)
    {
        requireNonNull(recipe, "expression recipe is null");
        if (depth > MAX_RECIPE_DEPTH) {
            throw new IllegalArgumentException("expression recipe exceeds depth limit");
        }
        counter.add();
        switch (recipe) {
            case FieldReferenceRecipe reference -> {
                if (!context.allowFieldReferences()) {
                    throw new IllegalArgumentException("field reference is not valid in this recipe");
                }
                if (!canonical(reference.table(), "field reference table").equals(canonical(context.ownerTable(), "expression field table"))) {
                    throw new IllegalArgumentException("field reference crosses tables without a relationship path");
                }
                LogicalTableDefinition table = context.tables().get(canonical(reference.table(), "field reference table"));
                if (table == null) {
                    throw new IllegalArgumentException("field reference has unknown table");
                }
                String key = expressionKey(reference.table(), reference.field());
                if (context.expressions().containsKey(key)) {
                    if (context.dependencies() != null) {
                        context.dependencies().add(key);
                    }
                }
                else {
                    requireReference(fieldNames(table), reference.field(), "field reference has unknown field");
                }
            }
            case LiteralRecipe _ -> {}
            case FunctionCallRecipe call -> {
                FunctionCapabilityDefinition function = context.functions().get(canonical(call.name(), "function name"));
                if (function == null) {
                    throw new IllegalArgumentException("function call references an undeclared function");
                }
                if (!acceptsArity(function, call.arguments().size())) {
                    throw new IllegalArgumentException("function call has an unsupported argument count");
                }
                call.arguments().forEach(argument -> validateRecipe(argument, depth + 1, counter, context));
            }
            case OperatorRecipe operator -> operator.arguments().forEach(argument -> validateRecipe(argument, depth + 1, counter, context));
            case CastRecipe cast -> validateRecipe(cast.expression(), depth + 1, counter, context);
            case ArgumentReferenceRecipe reference -> {
                if (context.arguments() == null) {
                    throw new IllegalArgumentException("argument reference is not valid in this recipe");
                }
                context.arguments().merge(reference.argument(), 1, Integer::sum);
            }
            case ScopedFieldReferenceRecipe reference -> {
                String tableName = context.scopedTables().get(reference.side());
                if (tableName == null) {
                    throw new IllegalArgumentException("scoped field reference is not valid in this recipe");
                }
                LogicalTableDefinition table = context.tables().get(canonical(tableName, "scoped field table"));
                requireReference(semanticFieldNames(table, context.expressions()), reference.field(), "scoped field reference has unknown field");
            }
            case PropertyLookupRecipe lookup -> {
                if (!context.allowPropertyLookups() || !contextAllowsTable(context, lookup.table())) {
                    throw new IllegalArgumentException("property lookup is not valid in this recipe");
                }
                LogicalTableDefinition table = context.tables().get(canonical(lookup.table(), "property lookup table"));
                if (table == null || table.properties().stream().noneMatch(property -> canonical(property.name(), "property name").equals(canonical(lookup.property(), "property lookup name")))) {
                    throw new IllegalArgumentException("property lookup references an unknown property");
                }
                validateRecipe(lookup.key(), depth + 1, counter, context);
            }
        }
    }

    private static boolean contextAllowsTable(RecipeValidationContext context, String table)
    {
        String canonicalTable = canonical(table, "recipe table");
        if (canonicalTable.equals(canonical(context.ownerTable(), "recipe owner table"))) {
            return true;
        }
        return context.scopedTables().values().stream()
                .map(value -> canonical(value, "scoped recipe table"))
                .anyMatch(canonicalTable::equals);
    }

    private static boolean acceptsArity(FunctionCapabilityDefinition function, int arity)
    {
        return function.signatures().stream().anyMatch(signature -> signature.variadic()
                ? arity >= signature.argumentTypes().size() - 1
                : arity == signature.argumentTypes().size());
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
            if (modifier.behavior() == ModifierBehavior.TRINO_SESSION_PROPERTY && modifier.sessionProperty().size() > 2) {
                throw new IllegalArgumentException("modifier has an invalid session property name");
            }
            if (modifier.behavior() != ModifierBehavior.TRINO_SESSION_PROPERTY && !modifier.sessionProperty().isEmpty()) {
                throw new IllegalArgumentException("modifier cannot name a session property");
            }
        }
    }

    private static Map<String, SemanticRelation> validateRelations(
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
        return Map.copyOf(relations);
    }

    private static void validateLazyTables(
            List<LazyTableDefinition> definitions,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            RecipeCounter counter)
    {
        Map<String, Set<String>> members = new HashMap<>();
        tables.forEach((name, table) -> members.put(name, semanticMemberNames(table, expressions)));
        for (LazyTableDefinition definition : definitions) {
            LogicalTableDefinition owner = tables.get(canonical(definition.table(), "lazy table owner"));
            if (owner == null) {
                throw new IllegalArgumentException("lazy table references an unknown owner table");
            }
            if (!members.get(canonical(owner.name(), "lazy table owner")).add(canonical(definition.name(), "lazy table name"))) {
                throw new IllegalArgumentException("lazy table conflicts with an existing logical member");
            }
            if (definition.relationshipPath().isEmpty()) {
                throw new IllegalArgumentException("lazy table must include a relationship path");
            }
            if (definition.relationshipPath().size() > MAX_RELATION_DEPTH) {
                throw new IllegalArgumentException("lazy table relationship path exceeds depth limit");
            }
            LogicalTableDefinition terminal = owner;
            for (String relationshipName : definition.relationshipPath()) {
                LogicalTableDefinition pathSource = terminal;
                RelationshipDefinition relationship = pathSource.relationships().stream()
                        .filter(candidate -> canonical(candidate.name(), "relationship name").equals(canonical(relationshipName, "relationship path")))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("lazy table references an unknown relationship"));
                terminal = tables.get(canonical(relationship.targetTable(), "relationship target table"));
            }
            if (definition.projections().isEmpty()) {
                throw new IllegalArgumentException("lazy table must include projections");
            }
            Set<String> projectionNames = new HashSet<>();
            for (LazyProjectionDefinition projection : definition.projections()) {
                addUnique(projectionNames, projection.name(), "duplicate projection on lazy table");
                validateRecipe(
                        projection.recipe(),
                        1,
                        counter,
                        new RecipeValidationContext(terminal.name(), tables, expressions, functions, null, true, true, null, Map.of()));
            }
        }
    }

    private static void validateSemanticEntities(
            List<ActionReference> actions,
            List<CohortReference> cohorts,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            Map<String, SemanticRelation> relations,
            RecipeCounter counter)
    {
        Set<String> actionNames = new HashSet<>();
        for (ActionReference action : actions) {
            addUnique(actionNames, action.name(), "duplicate action");
            validateSemanticEntity(action.name(), action.table(), action.representation(), "action", tables, expressions, functions, relations, counter);
        }
        Set<String> cohortNames = new HashSet<>();
        for (CohortReference cohort : cohorts) {
            addUnique(cohortNames, cohort.name(), "duplicate cohort");
            validateSemanticEntity(cohort.name(), cohort.table(), cohort.representation(), "cohort", tables, expressions, functions, relations, counter);
        }
    }

    private static void validateSemanticEntity(
            String name,
            String tableName,
            SemanticEntityRepresentation representation,
            String kind,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            Map<String, SemanticRelation> relations,
            RecipeCounter counter)
    {
        LogicalTableDefinition owner = tables.get(canonical(tableName, kind + " table"));
        if (owner == null) {
            throw new IllegalArgumentException(kind + " references an unknown table");
        }
        switch (representation) {
            case PredicateRepresentation predicate -> validateRecipe(
                    predicate.predicate(),
                    1,
                    counter,
                    new RecipeValidationContext(owner.name(), tables, expressions, functions, null, true, true, null, Map.of()));
            case RelationMembershipRepresentation membershipRepresentation -> {
                RelationMembershipRecipe membership = membershipRepresentation.relation();
                SemanticRelation relation = relations.get(canonical(membership.relation().name(), kind + " relation"));
                if (relation == null || relation.kind != membership.relation().kind()) {
                    throw new IllegalArgumentException(kind + " references an unknown or mismatched relation");
                }
                requireReference(semanticFieldNames(owner, expressions), membership.sourceField(), kind + " references an unknown source field");
                requireReference(requireNonNull(relation.fields, "resolved relation fields are null"), membership.targetField(), kind + " references an unknown target field");
            }
        }
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

    private static Set<String> semanticFieldNames(LogicalTableDefinition table, Map<String, ExpressionFieldDefinition> expressions)
    {
        Set<String> fields = fieldNames(requireNonNull(table, "logical table is null"));
        expressions.values().stream()
                .filter(expression -> canonical(expression.table(), "expression table").equals(canonical(table.name(), "logical table")))
                .forEach(expression -> fields.add(canonical(expression.name(), "expression field")));
        return fields;
    }

    private static Set<String> semanticMemberNames(LogicalTableDefinition table, Map<String, ExpressionFieldDefinition> expressions)
    {
        Set<String> members = semanticFieldNames(table, expressions);
        table.properties().forEach(property -> members.add(canonical(property.name(), "property name")));
        table.relationships().forEach(relationship -> members.add(canonical(relationship.name(), "relationship name")));
        return members;
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

    public record PropertyDefinition(
            String name,
            String sourceField,
            PropertyStorage storage,
            LogicalType logicalType,
            boolean nullable,
            Optional<String> keyTypeSignature,
            Optional<String> valueTypeSignature,
            Optional<ExpressionRecipe> lookupRecipe)
    {
        public PropertyDefinition
        {
            name = definition(name, "property name");
            sourceField = definition(sourceField, "property source field");
            storage = requireNonNull(storage, "storage is null");
            logicalType = requireNonNull(logicalType, "logicalType is null");
            keyTypeSignature = requireNonNull(keyTypeSignature, "keyTypeSignature is null").map(value -> definition(value, "property key type signature"));
            valueTypeSignature = requireNonNull(valueTypeSignature, "valueTypeSignature is null").map(value -> definition(value, "property value type signature"));
            lookupRecipe = requireNonNull(lookupRecipe, "lookupRecipe is null");
            if (lookupRecipe.isPresent() != keyTypeSignature.isPresent() || lookupRecipe.isPresent() != valueTypeSignature.isPresent()) {
                throw new IllegalArgumentException("property lookup recipe and type signatures must be declared together");
            }
        }

        public PropertyDefinition(String name, String sourceField, PropertyStorage storage, LogicalType logicalType, boolean nullable)
        {
            this(name, sourceField, storage, logicalType, nullable, Optional.empty(), Optional.empty(), Optional.empty());
        }
    }

    public record RelationshipDefinition(String name, String targetTable, RelationshipCardinality cardinality, List<JoinKey> joinKeys, Optional<ExpressionRecipe> joinPredicate)
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
            joinPredicate = requireNonNull(joinPredicate, "joinPredicate is null");
        }

        public RelationshipDefinition(String name, String targetTable, RelationshipCardinality cardinality, List<JoinKey> joinKeys)
        {
            this(name, targetTable, cardinality, joinKeys, Optional.empty());
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
            permits ArgumentReferenceRecipe,
                    CastRecipe,
                    FieldReferenceRecipe,
                    FunctionCallRecipe,
                    LiteralRecipe,
                    OperatorRecipe,
                    PropertyLookupRecipe,
                    ScopedFieldReferenceRecipe
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

    public record ArgumentReferenceRecipe(ExpressionArgument argument)
            implements ExpressionRecipe
    {
        public ArgumentReferenceRecipe
        {
            argument = requireNonNull(argument, "argument is null");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.ARGUMENT_REFERENCE;
        }
    }

    public record ScopedFieldReferenceRecipe(RelationshipJoinSide side, String field)
            implements ExpressionRecipe
    {
        public ScopedFieldReferenceRecipe
        {
            side = requireNonNull(side, "side is null");
            field = definition(field, "scoped field reference");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.SCOPED_FIELD_REFERENCE;
        }
    }

    public record PropertyLookupRecipe(String table, String property, ExpressionRecipe key)
            implements ExpressionRecipe
    {
        public PropertyLookupRecipe
        {
            table = definition(table, "property lookup table");
            property = definition(property, "property lookup name");
            key = requireNonNull(key, "property lookup key is null");
        }

        @Override
        public ExpressionRecipeKind kind()
        {
            return ExpressionRecipeKind.PROPERTY_LOOKUP;
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

    public record FunctionCapabilityDefinition(String name, FunctionKind kind, FunctionImplementation implementation, List<PhysicalIdentifier> trinoName, Optional<FunctionRewrite> rewrite, List<FunctionSignature> signatures, boolean deterministic, boolean supportsDistinct, boolean supportsOrderBy, boolean supportsFilter, boolean supportsWindow)
    {
        public FunctionCapabilityDefinition
        {
            name = definition(name, "function name");
            kind = requireNonNull(kind, "kind is null");
            implementation = requireNonNull(implementation, "implementation is null");
            trinoName = copy(trinoName, "trinoName");
            rewrite = requireNonNull(rewrite, "rewrite is null");
            signatures = copy(signatures, "signatures");
            if (signatures.isEmpty()) {
                throw new IllegalArgumentException("function must include signatures");
            }
        }

        public FunctionCapabilityDefinition(
                String name,
                FunctionKind kind,
                FunctionImplementation implementation,
                List<PhysicalIdentifier> trinoName,
                List<FunctionSignature> signatures,
                boolean deterministic,
                boolean supportsDistinct,
                boolean supportsOrderBy,
                boolean supportsFilter,
                boolean supportsWindow)
        {
            this(name, kind, implementation, trinoName, Optional.empty(), signatures, deterministic, supportsDistinct, supportsOrderBy, supportsFilter, supportsWindow);
        }
    }

    public record FunctionSignature(List<String> argumentTypes, String returnType, boolean variadic)
    {
        public FunctionSignature
        {
            argumentTypes = copy(argumentTypes, "argumentTypes").stream().map(value -> definition(value, "function argument type")).toList();
            returnType = definition(returnType, "function return type");
            if (variadic && argumentTypes.isEmpty()) {
                throw new IllegalArgumentException("variadic function signature must declare an argument");
            }
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

    public record LazyTableDefinition(String table, String name, List<String> relationshipPath, List<LazyProjectionDefinition> projections)
    {
        public LazyTableDefinition
        {
            table = definition(table, "lazy table owner");
            name = definition(name, "lazy table name");
            relationshipPath = copy(relationshipPath, "relationshipPath").stream()
                    .map(value -> definition(value, "lazy table relationship path"))
                    .toList();
            projections = copy(projections, "projections");
        }
    }

    public record LazyProjectionDefinition(String name, String trinoTypeSignature, LogicalType logicalType, boolean nullable, boolean starVisible, ExpressionRecipe recipe)
    {
        public LazyProjectionDefinition
        {
            name = definition(name, "lazy projection name");
            trinoTypeSignature = definition(trinoTypeSignature, "lazy projection type signature");
            logicalType = requireNonNull(logicalType, "logicalType is null");
            recipe = requireNonNull(recipe, "recipe is null");
        }
    }

    public record ActionReference(String name, String actionId, String table, SemanticEntityRepresentation representation)
    {
        public ActionReference
        {
            name = definition(name, "action name");
            actionId = definition(actionId, "action ID");
            table = definition(table, "action table");
            representation = requireNonNull(representation, "representation is null");
        }
    }

    public record CohortReference(String name, String cohortId, String table, SemanticEntityRepresentation representation)
    {
        public CohortReference
        {
            name = definition(name, "cohort name");
            cohortId = definition(cohortId, "cohort ID");
            table = definition(table, "cohort table");
            representation = requireNonNull(representation, "representation is null");
        }
    }

    public sealed interface SemanticEntityRepresentation
            permits PredicateRepresentation, RelationMembershipRepresentation
    {
        SemanticEntityKind kind();
    }

    public record PredicateRepresentation(ExpressionRecipe predicate)
            implements SemanticEntityRepresentation
    {
        public PredicateRepresentation
        {
            predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        public SemanticEntityKind kind()
        {
            return SemanticEntityKind.PREDICATE;
        }
    }

    public record RelationMembershipRepresentation(RelationMembershipRecipe relation)
            implements SemanticEntityRepresentation
    {
        public RelationMembershipRepresentation
        {
            relation = requireNonNull(relation, "relation is null");
        }

        @Override
        public SemanticEntityKind kind()
        {
            return SemanticEntityKind.RELATION;
        }
    }

    public record RelationMembershipRecipe(RelationReference relation, String sourceField, String targetField)
    {
        public RelationMembershipRecipe
        {
            relation = requireNonNull(relation, "relation is null");
            sourceField = definition(sourceField, "membership source field");
            targetField = definition(targetField, "membership target field");
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
        FIELD_REFERENCE, LITERAL, FUNCTION_CALL, OPERATOR, CAST, ARGUMENT_REFERENCE, SCOPED_FIELD_REFERENCE, PROPERTY_LOOKUP
    }

    public enum SemanticOperator
    {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS, EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, AND, OR, NOT, NEGATE, IS_NULL, IS_NOT_NULL, SUBSCRIPT, JSON_OBJECT_LOOKUP
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

    public enum FunctionRewrite
    {
        CAST_DATE,
        CAST_DOUBLE,
        CAST_BIGINT,
        CAST_TIMESTAMP,
        CAST_VARCHAR,
        ADD_DAYS,
        ADD_MONTHS,
        AND,
        ANY_IF,
        ARG_MAX_IF,
        ARRAY_ELEMENT,
        ARRAY_FILTER,
        ARRAY_FIRST,
        ARRAY_MAP,
        ARRAY_SUM,
        AVG_IF,
        DATE_ADD,
        DATE_TRUNC_DAY,
        DATE_TRUNC_HOUR,
        DATE_TRUNC_MONTH,
        DATE_TRUNC_WEEK,
        COUNT_IF,
        COUNT_DISTINCT,
        DECIMAL_CAST,
        FLOAT_OR_DEFAULT,
        FLOAT_OR_ZERO,
        GROUP_UNIQ_ARRAY,
        GREATER,
        GROUP_ARRAY_IF,
        GROUP_UNIQ_ARRAY_IF,
        HAS,
        INTERVAL_DAY,
        INT_DIV,
        IS_NULL,
        IS_NOT_NULL,
        JSON_EXTRACT_FLOAT,
        JSON_EXTRACT_INT,
        JSON_EXTRACT_RAW,
        JSON_EXTRACT_STRING,
        JSON_EXTRACT_TYPED,
        JSON_KEYS_AND_VALUES,
        JSON_KEYS_AND_VALUES_RAW,
        JSON_LENGTH,
        LIKE,
        MAX_IF,
        MIN_IF,
        MULTI_IF,
        NOT,
        PARSE_TIMESTAMP,
        REGEX_EXTRACT,
        REGEX_REPLACE_ALL,
        RANGE,
        SPLIT_CHAR,
        SUM_IF,
        TODAY,
        TO_UNIX_TIMESTAMP,
        TUPLE_ELEMENT,
        UNIQ_EXACT,
        UNIQ_EXACT_IF,
        UNIQ_IF
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

    public enum ExpressionArgument
    {
        PROPERTY_SOURCE, PROPERTY_KEY
    }

    public enum RelationshipJoinSide
    {
        SOURCE, TARGET
    }

    public enum SemanticEntityKind
    {
        PREDICATE, RELATION
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

    private record RecipeValidationContext(
            String ownerTable,
            Map<String, LogicalTableDefinition> tables,
            Map<String, ExpressionFieldDefinition> expressions,
            Map<String, FunctionCapabilityDefinition> functions,
            List<String> dependencies,
            boolean allowFieldReferences,
            boolean allowPropertyLookups,
            Map<ExpressionArgument, Integer> arguments,
            Map<RelationshipJoinSide, String> scopedTables)
    {
        private RecipeValidationContext
        {
            ownerTable = definition(ownerTable, "recipe owner table");
            tables = requireNonNull(tables, "tables is null");
            expressions = requireNonNull(expressions, "expressions is null");
            functions = requireNonNull(functions, "functions is null");
            scopedTables = Map.copyOf(requireNonNull(scopedTables, "scopedTables is null"));
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
