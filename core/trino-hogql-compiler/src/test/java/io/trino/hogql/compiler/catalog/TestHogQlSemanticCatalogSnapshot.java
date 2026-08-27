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
        return new HogQlSemanticCatalogSnapshot(1, LANGUAGE_VERSION, CATALOG, 7, tables);
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
