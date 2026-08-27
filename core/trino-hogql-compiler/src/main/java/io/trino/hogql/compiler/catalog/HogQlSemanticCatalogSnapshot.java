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

import io.trino.hogql.parser.HogQlLanguageVersion;

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
        int schemaVersion,
        HogQlLanguageVersion languageVersion,
        PhysicalIdentifier catalog,
        long generation,
        List<LogicalTableDefinition> logicalTables)
{
    public HogQlSemanticCatalogSnapshot
    {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported HogQL semantic catalog schema: " + schemaVersion);
        }
        if (generation <= 0) {
            throw new IllegalArgumentException("HogQL semantic catalog generation must be positive");
        }
        languageVersion = requireNonNull(languageVersion, "languageVersion is null");
        catalog = requireNonNull(catalog, "catalog is null");
        logicalTables = List.copyOf(requireNonNull(logicalTables, "logicalTables is null"));

        Map<String, LogicalTableDefinition> tablesByName = indexTables(catalog, logicalTables);
        validateReferences(tablesByName);
    }

    public Optional<LogicalTableDefinition> logicalTable(String name)
    {
        String canonicalName = canonicalName(name, "logical table");
        return logicalTables.stream()
                .filter(table -> canonicalName(table.name(), "logical table").equals(canonicalName))
                .findFirst();
    }

    private static Map<String, LogicalTableDefinition> indexTables(PhysicalIdentifier catalog, List<LogicalTableDefinition> tables)
    {
        Map<String, LogicalTableDefinition> tablesByName = new HashMap<>();
        for (LogicalTableDefinition table : tables) {
            if (!table.physicalTable().catalog().equals(catalog)) {
                throw new IllegalArgumentException("logical table physical reference uses another catalog: " + table.name());
            }
            String canonicalName = canonicalName(table.name(), "logical table");
            if (tablesByName.put(canonicalName, table) != null) {
                throw new IllegalArgumentException("duplicate logical table: " + table.name());
            }
            validateMemberNames(table);
        }
        return Map.copyOf(tablesByName);
    }

    private static void validateMemberNames(LogicalTableDefinition table)
    {
        Set<String> names = new HashSet<>();
        table.fields().forEach(field -> addMemberName(table, names, field.name()));
        table.properties().forEach(property -> addMemberName(table, names, property.name()));
        table.relationships().forEach(relationship -> addMemberName(table, names, relationship.name()));
    }

    private static void addMemberName(LogicalTableDefinition table, Set<String> names, String name)
    {
        if (!names.add(canonicalName(name, "logical member"))) {
            throw new IllegalArgumentException("duplicate logical member in table " + table.name() + ": " + name);
        }
    }

    private static void validateReferences(Map<String, LogicalTableDefinition> tablesByName)
    {
        for (LogicalTableDefinition table : tablesByName.values()) {
            Set<String> fieldNames = fieldNames(table);
            for (PropertyDefinition property : table.properties()) {
                if (!fieldNames.contains(canonicalName(property.sourceField(), "property source field"))) {
                    throw new IllegalArgumentException("property has unknown source field in table " + table.name() + ": " + property.name());
                }
            }
            for (RelationshipDefinition relationship : table.relationships()) {
                LogicalTableDefinition target = tablesByName.get(canonicalName(relationship.targetTable(), "relationship target table"));
                if (target == null) {
                    throw new IllegalArgumentException("relationship has unknown target table in table " + table.name() + ": " + relationship.name());
                }
                Set<String> targetFieldNames = fieldNames(target);
                for (JoinKey joinKey : relationship.joinKeys()) {
                    if (!fieldNames.contains(canonicalName(joinKey.sourceField(), "relationship source field"))) {
                        throw new IllegalArgumentException("relationship has unknown source field in table " + table.name() + ": " + relationship.name());
                    }
                    if (!targetFieldNames.contains(canonicalName(joinKey.targetField(), "relationship target field"))) {
                        throw new IllegalArgumentException("relationship has unknown target field in table " + table.name() + ": " + relationship.name());
                    }
                }
            }
        }
    }

    private static Set<String> fieldNames(LogicalTableDefinition table)
    {
        Set<String> fieldNames = new HashSet<>();
        for (LogicalFieldDefinition field : table.fields()) {
            fieldNames.add(canonicalName(field.name(), "logical field"));
        }
        return Set.copyOf(fieldNames);
    }

    private static String canonicalName(String name, String kind)
    {
        String value = validateDefinitionText(name, kind);
        return value.toLowerCase(Locale.ENGLISH);
    }

    private static String validateDefinitionText(String value, String kind)
    {
        requireNonNull(value, kind + " is null");
        if (value.isBlank() || containsExecutableDelimiter(value)) {
            throw new IllegalArgumentException("invalid " + kind + ": " + value);
        }
        return value;
    }

    private static boolean containsExecutableDelimiter(String value)
    {
        return value.indexOf(';') >= 0 || value.indexOf('\0') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0 || value.contains("--") || value.contains("/*") || value.contains("*/");
    }

    public record LogicalTableDefinition(
            String name,
            PhysicalQualifiedName physicalTable,
            List<LogicalFieldDefinition> fields,
            List<PropertyDefinition> properties,
            List<RelationshipDefinition> relationships)
    {
        public LogicalTableDefinition
        {
            name = validateDefinitionText(name, "logical table name");
            physicalTable = requireNonNull(physicalTable, "physicalTable is null");
            fields = List.copyOf(requireNonNull(fields, "fields is null"));
            properties = List.copyOf(requireNonNull(properties, "properties is null"));
            relationships = List.copyOf(requireNonNull(relationships, "relationships is null"));
        }
    }

    public record LogicalFieldDefinition(
            String name,
            PhysicalIdentifier physicalColumn,
            String trinoTypeSignature,
            LogicalType logicalType,
            boolean nullable,
            boolean starVisible)
    {
        public LogicalFieldDefinition
        {
            name = validateDefinitionText(name, "logical field name");
            physicalColumn = requireNonNull(physicalColumn, "physicalColumn is null");
            trinoTypeSignature = validateDefinitionText(trinoTypeSignature, "Trino type signature");
            logicalType = requireNonNull(logicalType, "logicalType is null");
        }
    }

    public record PropertyDefinition(
            String name,
            String sourceField,
            PropertyStorage storage,
            LogicalType logicalType,
            boolean nullable)
    {
        public PropertyDefinition
        {
            name = validateDefinitionText(name, "property name");
            sourceField = validateDefinitionText(sourceField, "property source field");
            storage = requireNonNull(storage, "storage is null");
            logicalType = requireNonNull(logicalType, "logicalType is null");
        }
    }

    public record RelationshipDefinition(
            String name,
            String targetTable,
            RelationshipCardinality cardinality,
            List<JoinKey> joinKeys)
    {
        public RelationshipDefinition
        {
            name = validateDefinitionText(name, "relationship name");
            targetTable = validateDefinitionText(targetTable, "relationship target table");
            cardinality = requireNonNull(cardinality, "cardinality is null");
            joinKeys = List.copyOf(requireNonNull(joinKeys, "joinKeys is null"));
            if (joinKeys.isEmpty()) {
                throw new IllegalArgumentException("relationship must have at least one join key: " + name);
            }
        }
    }

    public record JoinKey(String sourceField, String targetField)
    {
        public JoinKey
        {
            sourceField = validateDefinitionText(sourceField, "relationship source field");
            targetField = validateDefinitionText(targetField, "relationship target field");
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
        private static final Pattern UNDELIMITED_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

        public PhysicalIdentifier
        {
            value = validateDefinitionText(value, "physical identifier");
            if (!delimited && !UNDELIMITED_IDENTIFIER.matcher(value).matches()) {
                throw new IllegalArgumentException("invalid physical identifier: " + value);
            }
            if (!delimited) {
                value = value.toLowerCase(Locale.ENGLISH);
            }
        }
    }

    public enum LogicalType
    {
        UNKNOWN,
        BOOLEAN,
        INTEGER,
        FLOAT,
        DECIMAL,
        STRING,
        DATE,
        TIMESTAMP,
        INTERVAL,
        UUID,
        JSON,
        ARRAY,
        MAP,
        ROW,
    }

    public enum PropertyStorage
    {
        JSON_OBJECT,
        MAP,
    }

    public enum RelationshipCardinality
    {
        ONE_TO_ONE,
        ONE_TO_MANY,
        MANY_TO_ONE,
        MANY_TO_MANY,
    }
}
