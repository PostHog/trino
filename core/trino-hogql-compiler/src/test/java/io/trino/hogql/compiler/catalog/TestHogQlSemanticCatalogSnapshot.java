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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ActionReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ArgumentReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CohortReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionArgument;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipe;
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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ModifierBehavior;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PredicateRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyLookupRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationMembershipRepresentation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipJoinSide;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ScopedFieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticModifierDefault;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogSnapshot
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);

    @Test
    public void testDoesNotExposePublisherMutability()
    {
        List<LogicalFieldDefinition> fields = new ArrayList<>(List.of(field("id")));
        List<PropertyDefinition> properties = new ArrayList<>(List.of(property("properties", "id")));
        List<JoinKey> joinKeys = new ArrayList<>(List.of(new JoinKey("id", "id")));
        List<RelationshipDefinition> relationships = new ArrayList<>(List.of(
                new RelationshipDefinition("person", "persons", RelationshipCardinality.MANY_TO_ONE, joinKeys)));
        List<LogicalTableDefinition> tables = new ArrayList<>(List.of(
                table("events", fields, properties, relationships),
                table("persons")));
        HogQlSemanticCatalogSnapshot snapshot = snapshot(tables);

        fields.add(field("event"));
        properties.clear();
        joinKeys.clear();
        relationships.clear();
        tables.clear();

        assertThat(snapshot.logicalTables()).hasSize(2);
        assertThat(snapshot.logicalTable("events")).get().satisfies(table -> {
            assertThat(table.fields()).hasSize(1);
            assertThat(table.properties()).hasSize(1);
            assertThat(table.relationships()).singleElement().satisfies(relationship -> assertThat(relationship.joinKeys()).hasSize(1));
        });
        assertThatThrownBy(() -> snapshot.logicalTables().add(table("persons")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> snapshot.logicalTable("events").orElseThrow().fields().add(field("event")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> snapshot.logicalTable("events").orElseThrow().relationships().getFirst().joinKeys().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidSnapshots")
    public void testRejectsInvalidDefinitions(String name, List<LogicalTableDefinition> tables, String message)
    {
        assertThatThrownBy(() -> snapshot(tables))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    @Test
    public void testAcceptsBidirectionalLazyRelationships()
    {
        HogQlSemanticCatalogSnapshot snapshot = snapshot(List.of(
                table("events", List.of(field("id")), List.of(), List.of(relationship("person", "persons", "id", "id"))),
                table("persons", List.of(field("id")), List.of(), List.of(relationship("events", "events", "id", "id")))));

        assertThat(snapshot.logicalTable("events")).get()
                .extracting(LogicalTableDefinition::relationships)
                .satisfies(relationships -> assertThat(relationships).singleElement().extracting(RelationshipDefinition::targetTable).isEqualTo("persons"));
        assertThat(snapshot.logicalTable("persons")).get()
                .extracting(LogicalTableDefinition::relationships)
                .satisfies(relationships -> assertThat(relationships).singleElement().extracting(RelationshipDefinition::targetTable).isEqualTo("events"));
    }

    @Test
    public void testSemanticDefinitionsAreDeeplyImmutable()
    {
        List<ExpressionRecipe> arguments = new ArrayList<>(List.of(literal()));
        FunctionCallRecipe recipe = new FunctionCallRecipe("identity", arguments);
        List<ExpressionFieldDefinition> expressionFields = new ArrayList<>(List.of(
                new ExpressionFieldDefinition("events", "derived", "bigint", LogicalType.INTEGER, false, true, recipe)));
        List<FunctionSignature> signatures = new ArrayList<>(List.of(new FunctionSignature(List.of("bigint"), "bigint", false)));
        List<FunctionCapabilityDefinition> functions = new ArrayList<>(List.of(
                new FunctionCapabilityDefinition(
                        "identity",
                        FunctionKind.SCALAR,
                        FunctionImplementation.STOCK,
                        List.of(new PhysicalIdentifier("identity", false)),
                        signatures,
                        true,
                        false,
                        false,
                        false,
                        false)));

        HogQlSemanticCatalogSnapshot snapshot = semanticSnapshot(expressionFields, functions);
        arguments.clear();
        expressionFields.clear();
        signatures.clear();
        functions.clear();

        assertThat(snapshot.expressionFields()).singleElement().extracting(ExpressionFieldDefinition::recipe).isEqualTo(recipe);
        assertThat(recipe.arguments()).singleElement().isEqualTo(literal());
        assertThat(snapshot.functions()).singleElement().extracting(FunctionCapabilityDefinition::signatures).satisfies(values -> assertThat(values).hasSize(1));
        assertThatThrownBy(() -> recipe.arguments().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testValidatesFunctionRewriteContract()
    {
        FunctionCapabilityDefinition isNull = rewriteFunction("isNull", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.IS_NULL), List.of(new FunctionSignature(List.of("varchar"), "boolean", false)));
        FunctionCapabilityDefinition isNotNull = rewriteFunction("isNotNull", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.IS_NOT_NULL), List.of(new FunctionSignature(List.of("varchar"), "boolean", false)));
        FunctionCapabilityDefinition countIf = rewriteFunction("countIf", FunctionKind.AGGREGATE, List.of(), Optional.of(FunctionRewrite.COUNT_IF), List.of(new FunctionSignature(List.of("boolean"), "bigint", false)));
        FunctionCapabilityDefinition multiIf = rewriteFunction("multiIf", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.MULTI_IF), List.of(new FunctionSignature(List.of("boolean", "any", "boolean", "any"), "any", true)));

        HogQlSemanticCatalogSnapshot snapshot = semanticSnapshot(List.of(), List.of(isNull, isNotNull, countIf, multiIf));

        assertThat(snapshot.functions()).extracting(FunctionCapabilityDefinition::rewrite)
                .containsExactly(
                        Optional.of(FunctionRewrite.IS_NULL),
                        Optional.of(FunctionRewrite.IS_NOT_NULL),
                        Optional.of(FunctionRewrite.COUNT_IF),
                        Optional.of(FunctionRewrite.MULTI_IF));
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("missing", FunctionKind.SCALAR, List.of(), Optional.empty(), List.of(new FunctionSignature(List.of("varchar"), "boolean", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must declare a rewrite");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("named", FunctionKind.SCALAR, List.of(new PhysicalIdentifier("named", false)), Optional.of(FunctionRewrite.IS_NULL), List.of(new FunctionSignature(List.of("varchar"), "boolean", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot name a Trino function");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                new FunctionCapabilityDefinition(
                        "stock",
                        FunctionKind.SCALAR,
                        FunctionImplementation.STOCK,
                        List.of(new PhysicalIdentifier("stock", false)),
                        Optional.of(FunctionRewrite.IS_NULL),
                        List.of(new FunctionSignature(List.of("varchar"), "boolean", false)),
                        true,
                        false,
                        false,
                        false,
                        false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot declare a rewrite");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("aggregate", FunctionKind.AGGREGATE, List.of(), Optional.of(FunctionRewrite.IS_NULL), List.of(new FunctionSignature(List.of("varchar"), "boolean", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be scalar");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("binary", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.IS_NULL), List.of(new FunctionSignature(List.of("varchar", "varchar"), "boolean", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid signature");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("scalarCountIf", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.COUNT_IF), List.of(new FunctionSignature(List.of("boolean"), "bigint", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kind must be aggregate");
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(
                rewriteFunction("fixedMultiIf", FunctionKind.SCALAR, List.of(), Optional.of(FunctionRewrite.MULTI_IF), List.of(new FunctionSignature(List.of("boolean", "any", "any"), "any", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid signature");
        assertInvalidRewrite(rewriteFunction(false, false, false, false, false, "boolean"), "must be deterministic");
        assertInvalidRewrite(rewriteFunction(true, true, false, false, false, "boolean"), "cannot support DISTINCT");
        assertInvalidRewrite(rewriteFunction(true, false, true, false, false, "boolean"), "cannot support ORDER BY");
        assertInvalidRewrite(rewriteFunction(true, false, false, true, false, "boolean"), "cannot support FILTER");
        assertInvalidRewrite(rewriteFunction(true, false, false, false, true, "boolean"), "cannot support window invocation");
        assertInvalidRewrite(rewriteFunction(true, false, false, false, false, "varchar"), "must return boolean");
    }

    @Test
    public void testRejectsOverqualifiedModifierSessionProperty()
    {
        assertThatThrownBy(() -> semanticSnapshotWithModifiers(List.of(new SemanticModifierDefault(
                "sampling",
                ModifierBehavior.TRINO_SESSION_PROPERTY,
                new TypedLiteral("boolean", LiteralEncoding.BOOLEAN, "false"),
                List.of(
                        new PhysicalIdentifier("catalog", false),
                        new PhysicalIdentifier("schema", false),
                        new PhysicalIdentifier("property", false))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid session property name");
    }

    @Test
    public void testLogicalSemanticRecipesAreDeeplyImmutable()
    {
        List<ExpressionRecipe> lookupArguments = new ArrayList<>(List.of(
                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_SOURCE),
                new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_KEY)));
        PropertyDefinition property = propertyWithLookup(new OperatorRecipe(SemanticOperator.SUBSCRIPT, lookupArguments));
        List<String> relationshipPath = new ArrayList<>(List.of("self"));
        List<LazyProjectionDefinition> projections = new ArrayList<>(List.of(new LazyProjectionDefinition(
                "browser",
                "varchar",
                LogicalType.STRING,
                true,
                true,
                propertyLookup("events", "properties"))));
        List<LazyTableDefinition> lazyTables = new ArrayList<>(List.of(new LazyTableDefinition("events", "profile", relationshipPath, projections)));
        List<ActionReference> actions = new ArrayList<>(List.of(new ActionReference(
                "paid",
                "action-1",
                "events",
                new PredicateRepresentation(propertyLookup("events", "properties")))));
        List<CohortReference> cohorts = new ArrayList<>(List.of(new CohortReference(
                "active",
                "cohort-1",
                "events",
                new RelationMembershipRepresentation(new RelationMembershipRecipe(
                        new RelationReference(RelationKind.LOGICAL_TABLE, "events"),
                        "properties",
                        "properties")))));

        HogQlSemanticCatalogSnapshot snapshot = logicalSemanticSnapshot(property, relationshipPathPredicate(), lazyTables, actions, cohorts);
        lookupArguments.clear();
        relationshipPath.clear();
        projections.clear();
        lazyTables.clear();
        actions.clear();
        cohorts.clear();

        assertThat(snapshot.logicalTables().getFirst().properties().getFirst().lookupRecipe()).isPresent();
        assertThat(snapshot.logicalTables().getFirst().relationships().getFirst().joinPredicate()).isPresent();
        assertThat(snapshot.lazyTables()).singleElement().satisfies(lazy -> {
            assertThat(lazy.relationshipPath()).containsExactly("self");
            assertThat(lazy.projections()).hasSize(1);
        });
        assertThat(snapshot.actions()).hasSize(1);
        assertThat(snapshot.cohorts()).hasSize(1);
        assertThatThrownBy(() -> snapshot.lazyTables().getFirst().relationshipPath().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRejectsInvalidLogicalSemanticScopes()
    {
        assertThatThrownBy(() -> logicalSemanticSnapshot(
                propertyWithLookup(new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_SOURCE)),
                relationshipPathPredicate(),
                List.of(),
                List.of(),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source and key arguments");

        assertThatThrownBy(() -> logicalSemanticSnapshot(
                propertyWithLookup(validLookupRecipe()),
                Optional.empty(),
                List.of(),
                List.of(new ActionReference(
                        "invalid",
                        "action-1",
                        "events",
                        new PredicateRepresentation(new ScopedFieldReferenceRecipe(RelationshipJoinSide.SOURCE, "properties")))),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scoped field reference");

        assertThatThrownBy(() -> logicalSemanticSnapshot(
                propertyWithLookup(validLookupRecipe()),
                relationshipPathPredicate(),
                List.of(new LazyTableDefinition(
                        "events",
                        "profile",
                        List.of("missing"),
                        List.of(new LazyProjectionDefinition("browser", "varchar", LogicalType.STRING, true, true, propertyLookup("events", "properties"))))),
                List.of(),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown relationship");

        assertThatThrownBy(() -> logicalSemanticSnapshot(
                propertyWithLookup(validLookupRecipe()),
                relationshipPathPredicate(),
                List.of(),
                List.of(),
                List.of(new CohortReference(
                        "invalid",
                        "cohort-1",
                        "events",
                        new RelationMembershipRepresentation(new RelationMembershipRecipe(
                                new RelationReference(RelationKind.LOGICAL_TABLE, "events"),
                                "properties",
                                "missing"))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown target field");
    }

    @Test
    public void testEnforcesExpressionRecipeDepthAndNodeLimits()
    {
        ExpressionRecipe deepRecipe = literal();
        for (int depth = 1; depth < 65; depth++) {
            deepRecipe = new CastRecipe(deepRecipe, "bigint");
        }
        ExpressionRecipe finalDeepRecipe = deepRecipe;
        assertThatThrownBy(() -> semanticSnapshot(
                List.of(new ExpressionFieldDefinition("events", "derived", "bigint", LogicalType.INTEGER, false, true, finalDeepRecipe)),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("depth limit");

        List<ExpressionRecipe> arguments = new ArrayList<>();
        for (int node = 0; node < 4_096; node++) {
            arguments.add(literal());
        }
        assertThatThrownBy(() -> semanticSnapshot(
                List.of(new ExpressionFieldDefinition("events", "derived", "bigint", LogicalType.INTEGER, false, true, new FunctionCallRecipe("identity", arguments))),
                List.of(variadicFunction("identity"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("node limit");
    }

    @ParameterizedTest(name = "{0} with {1} arguments")
    @MethodSource("invalidOperatorArities")
    public void testRejectsInvalidOperatorArity(SemanticOperator operator, int argumentCount)
    {
        List<ExpressionRecipe> arguments = new ArrayList<>();
        for (int index = 0; index < argumentCount; index++) {
            arguments.add(literal());
        }

        assertThatThrownBy(() -> semanticSnapshot(
                List.of(new ExpressionFieldDefinition(
                        "events",
                        "derived",
                        "bigint",
                        LogicalType.INTEGER,
                        false,
                        true,
                        new OperatorRecipe(operator, arguments))),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator arity");
    }

    @Test
    public void testRejectsUnsupportedRecipeFunctionArity()
    {
        assertThatThrownBy(() -> semanticSnapshot(
                List.of(new ExpressionFieldDefinition(
                        "events",
                        "derived",
                        "bigint",
                        LogicalType.INTEGER,
                        false,
                        true,
                        new FunctionCallRecipe("identity", List.of()))),
                List.of(function("identity"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported argument count");
    }

    private static Stream<Arguments> invalidOperatorArities()
    {
        return Stream.concat(
                Stream.of(SemanticOperator.values())
                        .filter(operator -> switch (operator) {
                            case NOT, NEGATE, IS_NULL, IS_NOT_NULL -> false;
                            default -> true;
                        })
                        .map(operator -> Arguments.of(operator, 1)),
                Stream.of(SemanticOperator.NOT, SemanticOperator.NEGATE, SemanticOperator.IS_NULL, SemanticOperator.IS_NOT_NULL)
                        .map(operator -> Arguments.of(operator, 2)));
    }

    private static Stream<Arguments> invalidSnapshots()
    {
        LogicalTableDefinition events = table("events");
        LogicalTableDefinition persons = table("persons");
        return Stream.of(
                Arguments.of("duplicate tables", List.of(events, events), "duplicate logical table"),
                Arguments.of(
                        "duplicate members",
                        List.of(table("events", List.of(field("id"), field("ID")), List.of(), List.of())),
                        "duplicate logical member"),
                Arguments.of(
                        "property collides with another field",
                        List.of(table("events", List.of(field("id"), field("properties")), List.of(property("id", "properties")), List.of())),
                        "duplicate logical member"),
                Arguments.of(
                        "missing property field",
                        List.of(table("events", List.of(field("id")), List.of(property("properties", "missing")), List.of())),
                        "unknown source field"),
                Arguments.of(
                        "missing relationship table",
                        List.of(table("events", List.of(field("id")), List.of(), List.of(relationship("person", "persons", "id", "id")))),
                        "unknown target table"),
                Arguments.of(
                        "missing relationship source field",
                        List.of(
                                table("events", List.of(field("id")), List.of(), List.of(relationship("person", "persons", "missing", "id"))),
                                persons),
                        "unknown source field"),
                Arguments.of(
                        "missing relationship target field",
                        List.of(
                                table("events", List.of(field("id")), List.of(), List.of(relationship("person", "persons", "id", "missing"))),
                                persons),
                        "unknown target field"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "events; DROP TABLE events",
            "events\nSELECT 1",
            "events/* injected */",
            "events-- injected",
    })
    public void testRejectsRawExecutablePhysicalDefinitions(String definition)
    {
        for (boolean delimited : List.of(false, true)) {
            assertThatThrownBy(() -> new PhysicalIdentifier(definition, delimited))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("physical identifier");
        }
    }

    private static HogQlSemanticCatalogSnapshot snapshot(List<LogicalTableDefinition> tables)
    {
        return new HogQlSemanticCatalogSnapshot(2, LANGUAGE_VERSION, CATALOG, 7, tables);
    }

    private static HogQlSemanticCatalogSnapshot semanticSnapshot(
            List<ExpressionFieldDefinition> expressionFields,
            List<FunctionCapabilityDefinition> functions)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                LANGUAGE_VERSION,
                CATALOG,
                7,
                List.of(table("events")),
                expressionFields,
                List.of(),
                List.of(),
                List.of(),
                functions,
                List.of());
    }

    private static HogQlSemanticCatalogSnapshot semanticSnapshotWithModifiers(List<SemanticModifierDefault> modifiers)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                LANGUAGE_VERSION,
                CATALOG,
                7,
                List.of(table("events")),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                modifiers);
    }

    private static LiteralRecipe literal()
    {
        return new LiteralRecipe(new TypedLiteral("bigint", LiteralEncoding.INTEGER, "1"));
    }

    private static FunctionCapabilityDefinition function(String name)
    {
        return new FunctionCapabilityDefinition(
                name,
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of(new PhysicalIdentifier(name, false)),
                List.of(new FunctionSignature(List.of("bigint"), "bigint", false)),
                true,
                false,
                false,
                false,
                false);
    }

    private static FunctionCapabilityDefinition variadicFunction(String name)
    {
        return new FunctionCapabilityDefinition(
                name,
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of(new PhysicalIdentifier(name, false)),
                List.of(new FunctionSignature(List.of("bigint"), "bigint", true)),
                true,
                false,
                false,
                false,
                false);
    }

    private static FunctionCapabilityDefinition rewriteFunction(
            String name,
            FunctionKind kind,
            List<PhysicalIdentifier> trinoName,
            Optional<FunctionRewrite> rewrite,
            List<FunctionSignature> signatures)
    {
        return new FunctionCapabilityDefinition(
                name,
                kind,
                FunctionImplementation.REWRITE,
                trinoName,
                rewrite,
                signatures,
                true,
                false,
                false,
                false,
                false);
    }

    private static FunctionCapabilityDefinition rewriteFunction(
            boolean deterministic,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow,
            String returnType)
    {
        return new FunctionCapabilityDefinition(
                "rewrite",
                FunctionKind.SCALAR,
                FunctionImplementation.REWRITE,
                List.of(),
                Optional.of(FunctionRewrite.IS_NULL),
                List.of(new FunctionSignature(List.of("varchar"), returnType, false)),
                deterministic,
                supportsDistinct,
                supportsOrderBy,
                supportsFilter,
                supportsWindow);
    }

    private static void assertInvalidRewrite(FunctionCapabilityDefinition function, String message)
    {
        assertThatThrownBy(() -> semanticSnapshot(List.of(), List.of(function)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static HogQlSemanticCatalogSnapshot logicalSemanticSnapshot(
            PropertyDefinition property,
            Optional<ExpressionRecipe> joinPredicate,
            List<LazyTableDefinition> lazyTables,
            List<ActionReference> actions,
            List<CohortReference> cohorts)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                LANGUAGE_VERSION,
                CATALOG,
                7,
                List.of(table(
                        "events",
                        List.of(field("properties")),
                        List.of(property),
                        List.of(new RelationshipDefinition(
                                "self",
                                "events",
                                RelationshipCardinality.MANY_TO_ONE,
                                List.of(new JoinKey("properties", "properties")),
                                joinPredicate)))),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                lazyTables,
                actions,
                cohorts);
    }

    private static PropertyDefinition propertyWithLookup(ExpressionRecipe recipe)
    {
        return new PropertyDefinition(
                "properties",
                "properties",
                PropertyStorage.JSON_OBJECT,
                LogicalType.STRING,
                true,
                Optional.of("varchar"),
                Optional.of("varchar"),
                Optional.of(recipe));
    }

    private static OperatorRecipe validLookupRecipe()
    {
        return new OperatorRecipe(
                SemanticOperator.SUBSCRIPT,
                List.of(
                        new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_SOURCE),
                        new ArgumentReferenceRecipe(ExpressionArgument.PROPERTY_KEY)));
    }

    private static Optional<ExpressionRecipe> relationshipPathPredicate()
    {
        return Optional.of(new OperatorRecipe(
                SemanticOperator.EQUAL,
                List.of(
                        new ScopedFieldReferenceRecipe(RelationshipJoinSide.SOURCE, "properties"),
                        new ScopedFieldReferenceRecipe(RelationshipJoinSide.TARGET, "properties"))));
    }

    private static PropertyLookupRecipe propertyLookup(String table, String property)
    {
        return new PropertyLookupRecipe(table, property, new LiteralRecipe(new TypedLiteral("varchar", LiteralEncoding.STRING, "browser")));
    }

    private static LogicalTableDefinition table(String name)
    {
        return table(name, List.of(field("id")), List.of(), List.of());
    }

    private static LogicalTableDefinition table(
            String name,
            List<LogicalFieldDefinition> fields,
            List<PropertyDefinition> properties,
            List<RelationshipDefinition> relationships)
    {
        return new LogicalTableDefinition(
                name,
                new PhysicalQualifiedName(CATALOG, new PhysicalIdentifier("default", false), new PhysicalIdentifier(name, false)),
                fields,
                properties,
                relationships);
    }

    private static LogicalFieldDefinition field(String name)
    {
        return new LogicalFieldDefinition(
                name,
                new PhysicalIdentifier(name, false),
                "varchar",
                LogicalType.STRING,
                true,
                true);
    }

    private static PropertyDefinition property(String name, String sourceField)
    {
        return new PropertyDefinition(name, sourceField, PropertyStorage.JSON_OBJECT, LogicalType.JSON, true);
    }

    private static RelationshipDefinition relationship(String name, String targetTable, String sourceField, String targetField)
    {
        return new RelationshipDefinition(
                name,
                targetTable,
                RelationshipCardinality.MANY_TO_ONE,
                List.of(new JoinKey(sourceField, targetField)));
    }
}
