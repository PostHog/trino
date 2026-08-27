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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.JoinKey;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipDefinition;
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
                List.of(function("identity"))))
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
