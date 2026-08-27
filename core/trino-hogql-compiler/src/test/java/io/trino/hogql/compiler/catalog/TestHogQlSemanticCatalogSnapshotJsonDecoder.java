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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ModifierBehavior;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationshipCardinality;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotJsonDecoder.DecodeFailure;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotJsonDecoder.Limits;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogSnapshotJsonDecoder
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);
    private static final String SECRET = "secret-catalog-payload-value";

    @Test
    public void testDecodesCompleteSnapshot()
    {
        HogQlSemanticCatalogSnapshot snapshot = new HogQlSemanticCatalogSnapshotJsonDecoder()
                .decode(bytes(validSnapshotJson()), LoadRequest.latest(CATALOG, LANGUAGE_VERSION));

        assertThat(snapshot.schemaVersion()).isEqualTo(2);
        assertThat(snapshot.languageVersion()).isEqualTo(LANGUAGE_VERSION);
        assertThat(snapshot.catalog()).isEqualTo(CATALOG);
        assertThat(snapshot.generation()).isEqualTo(7);
        assertThat(snapshot.logicalTables()).singleElement().satisfies(table -> {
            assertThat(table.name()).isEqualTo("events");
            assertThat(table.physicalTable().catalog()).isEqualTo(CATALOG);
            assertThat(table.physicalTable().schema()).isEqualTo(new PhysicalIdentifier("Analytics", true));
            assertThat(table.fields()).singleElement().satisfies(field -> {
                assertThat(field.name()).isEqualTo("properties");
                assertThat(field.trinoTypeSignature()).isEqualTo("json");
                assertThat(field.logicalType()).isEqualTo(LogicalType.JSON);
                assertThat(field.nullable()).isTrue();
                assertThat(field.starVisible()).isFalse();
            });
            assertThat(table.properties()).singleElement().satisfies(property -> {
                assertThat(property.storage()).isEqualTo(PropertyStorage.JSON_OBJECT);
                assertThat(property.logicalType()).isEqualTo(LogicalType.STRING);
            });
            assertThat(table.relationships()).singleElement().satisfies(relationship -> {
                assertThat(relationship.cardinality()).isEqualTo(RelationshipCardinality.MANY_TO_ONE);
                assertThat(relationship.joinKeys()).singleElement().satisfies(joinKey -> {
                    assertThat(joinKey.sourceField()).isEqualTo("properties");
                    assertThat(joinKey.targetField()).isEqualTo("properties");
                });
            });
        });
        assertThat(snapshot.expressionFields()).hasSize(2);
        assertThat(snapshot.virtualTables()).singleElement().satisfies(table -> {
            assertThat(table.name()).isEqualTo("visible_events");
            assertThat(table.source().kind()).isEqualTo(RelationKind.LOGICAL_TABLE);
            assertThat(table.projections()).hasSize(2);
        });
        assertThat(snapshot.savedQueries()).singleElement().satisfies(savedQuery -> {
            assertThat(savedQuery.queryId()).isEqualTo("query-7");
            assertThat(savedQuery.target().kind()).isEqualTo(RelationKind.VIRTUAL_TABLE);
            assertThat(savedQuery.fields()).hasSize(2);
        });
        assertThat(snapshot.materializedViews()).singleElement().satisfies(view -> {
            assertThat(view.name()).isEqualTo("daily_events");
            assertThat(view.fields()).singleElement().extracting(HogQlSemanticCatalogSnapshot.ReferencedField::name).isEqualTo("day");
        });
        assertThat(snapshot.functions()).singleElement().satisfies(function -> {
            assertThat(function.name()).isEqualTo("length");
            assertThat(function.signatures()).singleElement().satisfies(signature -> {
                assertThat(signature.argumentTypes()).containsExactly("json");
                assertThat(signature.returnType()).isEqualTo("bigint");
            });
        });
        assertThat(snapshot.modifierDefaults()).singleElement().satisfies(modifier -> {
            assertThat(modifier.behavior()).isEqualTo(ModifierBehavior.TRINO_SESSION_PROPERTY);
            assertThat(modifier.sessionProperty()).extracting(PhysicalIdentifier::value).containsExactly("hogql", "sampling");
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compatibilityFailures")
    public void testRejectsIncompatibleSnapshots(String name, String payload, LoadRequest request, DecodeFailure failure)
    {
        assertDecodeFailure(new HogQlSemanticCatalogSnapshotJsonDecoder(), payload, request, failure);
    }

    private static Stream<Arguments> compatibilityFailures()
    {
        return Stream.of(
                Arguments.of(
                        "protocol",
                        validSnapshotJson().replace("\"protocolVersion\": 1", "\"protocolVersion\": 2"),
                        LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                        DecodeFailure.UNSUPPORTED_PROTOCOL),
                Arguments.of(
                        "schema",
                        validSnapshotJson().replace("\"schemaVersion\": 2", "\"schemaVersion\": 1"),
                        LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                        DecodeFailure.UNSUPPORTED_SCHEMA),
                Arguments.of(
                        "language",
                        validSnapshotJson().replace("\"languageVersion\": \"1.0.0\"", "\"languageVersion\": \"2.0.0\""),
                        LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                        DecodeFailure.LANGUAGE_VERSION_MISMATCH),
                Arguments.of(
                        "catalog",
                        validSnapshotJson().replace("\"value\": \"ducklake\"", "\"value\": \"other\""),
                        LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                        DecodeFailure.CATALOG_MISMATCH),
                Arguments.of(
                        "pinned generation",
                        validSnapshotJson(),
                        LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 8),
                        DecodeFailure.GENERATION_MISMATCH),
                Arguments.of(
                        "nonpositive generation",
                        validSnapshotJson().replace("\"generation\": 7", "\"generation\": 0"),
                        LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                        DecodeFailure.GENERATION_MISMATCH));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("strictFailures")
    public void testRejectsMalformedOrExtendedPayloads(String name, String payload)
    {
        assertDecodeFailure(
                new HogQlSemanticCatalogSnapshotJsonDecoder(),
                payload,
                LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                DecodeFailure.INVALID_PAYLOAD);
    }

    private static Stream<Arguments> strictFailures()
    {
        return Stream.of(
                Arguments.of("unknown root field", validSnapshotJson().replace("\"logicalTables\":", "\"unknown\": true, \"logicalTables\":")),
                Arguments.of("unknown nested field", validSnapshotJson().replace("\"starVisible\": false", "\"starVisible\": false, \"unknown\": true")),
                Arguments.of("unknown recipe field", validSnapshotJson().replace("\"fieldReference\": {", "\"unknown\": true, \"fieldReference\": {")),
                Arguments.of("unknown recipe kind", validSnapshotJson().replace("\"kind\": \"FUNCTION_CALL\"", "\"kind\": \"" + SECRET + "\"")),
                Arguments.of("mismatched recipe payload", validSnapshotJson().replace("\"functionCall\": {", "\"literal\": {")),
                Arguments.of(
                        "unknown function reference",
                        validSnapshotJson().replaceFirst("\"name\": \"length\",", "\"name\": \"missing\",")),
                Arguments.of("unknown virtual source", validSnapshotJson().replace("\"kind\": \"LOGICAL_TABLE\", \"name\": \"events\"", "\"kind\": \"LOGICAL_TABLE\", \"name\": \"missing\"")),
                Arguments.of("saved query target missing field", validSnapshotJson().replace("\"name\": \"browser_length\", \"trinoTypeSignature\": \"bigint\"", "\"name\": \"missing\", \"trinoTypeSignature\": \"bigint\"")),
                Arguments.of("invalid literal encoding", validSnapshotJson().replace("\"encoding\": \"INTEGER\", \"value\": \"1\"", "\"encoding\": \"INTEGER\", \"value\": \"" + SECRET + "\"")),
                Arguments.of(
                        "invalid modifier session property",
                        validSnapshotJson().replace(
                                "\"sessionProperty\": [",
                                "\"unknown\": true,\n                     \"sessionProperty\": [")),
                Arguments.of("duplicate field", validSnapshotJson().replace("\"protocolVersion\": 1", "\"protocolVersion\": 1, \"protocolVersion\": 1")),
                Arguments.of("missing field", validSnapshotJson().replace("\"generation\": 7,", "")),
                Arguments.of("wrong field type", validSnapshotJson().replace("\"nullable\": true", "\"nullable\": \"true\"")),
                Arguments.of("invalid enum", validSnapshotJson().replace("\"logicalType\": \"JSON\"", "\"logicalType\": \"" + SECRET + "\"")),
                Arguments.of("trailing content", validSnapshotJson() + " true"),
                Arguments.of("malformed JSON", "{\"protocolVersion\":" + SECRET));
    }

    @Test
    public void testEnforcesPayloadLimit()
    {
        HogQlSemanticCatalogSnapshotJsonDecoder decoder = new HogQlSemanticCatalogSnapshotJsonDecoder(
                new Limits(bytes(validSnapshotJson()).length - 1, 64, 100));

        assertDecodeFailure(decoder, validSnapshotJson(), LoadRequest.latest(CATALOG, LANGUAGE_VERSION), DecodeFailure.LIMIT_EXCEEDED);
    }

    @Test
    public void testEnforcesDepthLimit()
    {
        String payload = validSnapshotJson().replace(
                "\"logicalTables\":",
                "\"unknown\": [[[[[true]]]]], \"logicalTables\":");
        HogQlSemanticCatalogSnapshotJsonDecoder decoder = new HogQlSemanticCatalogSnapshotJsonDecoder(new Limits(1_000_000, 4, 100));

        assertDecodeFailure(decoder, payload, LoadRequest.latest(CATALOG, LANGUAGE_VERSION), DecodeFailure.LIMIT_EXCEEDED);
    }

    @Test
    public void testEnforcesCumulativeCollectionLimit()
    {
        HogQlSemanticCatalogSnapshotJsonDecoder decoder = new HogQlSemanticCatalogSnapshotJsonDecoder(new Limits(1_000_000, 64, 1));

        assertDecodeFailure(decoder, validSnapshotJson(), LoadRequest.latest(CATALOG, LANGUAGE_VERSION), DecodeFailure.LIMIT_EXCEEDED);
    }

    private static void assertDecodeFailure(
            HogQlSemanticCatalogSnapshotJsonDecoder decoder,
            String payload,
            LoadRequest request,
            DecodeFailure expectedFailure)
    {
        assertThatThrownBy(() -> decoder.decode(bytes(payload), request))
                .isInstanceOfSatisfying(HogQlSemanticCatalogSnapshotJsonDecoder.DecodeException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(expectedFailure);
                    assertThat(exception).hasMessageNotContaining(SECRET);
                    assertThat(exception.getMessage()).hasSizeLessThan(128);
                    assertThat(exception.getCause()).isNull();
                });
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String validSnapshotJson()
    {
        return """
               {
                 "protocolVersion": 1,
                 "schemaVersion": 2,
                 "languageVersion": "1.0.0",
                 "catalog": {"value": "ducklake", "delimited": false},
                 "generation": 7,
                 "logicalTables": [
                   {
                     "name": "events",
                     "physicalTable": {
                       "catalog": {"value": "ducklake", "delimited": false},
                       "schema": {"value": "Analytics", "delimited": true},
                       "table": {"value": "events", "delimited": false}
                     },
                     "fields": [
                       {
                         "name": "properties",
                         "physicalColumn": {"value": "properties", "delimited": false},
                         "trinoTypeSignature": "json",
                         "logicalType": "JSON",
                         "nullable": true,
                         "starVisible": false
                       }
                     ],
                     "properties": [
                       {
                         "name": "browser",
                         "sourceField": "properties",
                         "storage": "JSON_OBJECT",
                         "logicalType": "STRING",
                         "nullable": true
                       }
                     ],
                     "relationships": [
                       {
                         "name": "self",
                         "targetTable": "events",
                         "cardinality": "MANY_TO_ONE",
                         "joinKeys": [{"sourceField": "properties", "targetField": "properties"}]
                       }
                     ]
                   }
                 ],
                 "expressionFields": [
                   {
                     "table": "events",
                     "name": "browser_length",
                     "trinoTypeSignature": "bigint",
                     "logicalType": "INTEGER",
                     "nullable": true,
                     "starVisible": false,
                     "recipe": {
                       "kind": "FUNCTION_CALL",
                       "functionCall": {
                         "name": "length",
                         "arguments": [
                           {"kind": "FIELD_REFERENCE", "fieldReference": {"table": "events", "field": "properties"}}
                         ]
                       }
                     }
                   },
                   {
                     "table": "events",
                     "name": "adjusted_count",
                     "trinoTypeSignature": "bigint",
                     "logicalType": "INTEGER",
                     "nullable": false,
                     "starVisible": false,
                     "recipe": {
                       "kind": "CAST",
                       "cast": {
                         "targetTypeSignature": "bigint",
                         "expression": {
                           "kind": "OPERATOR",
                           "operator": {
                             "operator": "ADD",
                             "arguments": [
                               {"kind": "LITERAL", "literal": {"typeSignature": "bigint", "encoding": "INTEGER", "value": "1"}},
                               {"kind": "LITERAL", "literal": {"typeSignature": "bigint", "encoding": "INTEGER", "value": "2"}}
                             ]
                           }
                         }
                       }
                     }
                   }
                 ],
                 "virtualTables": [
                   {
                     "name": "visible_events",
                     "source": {"kind": "LOGICAL_TABLE", "name": "events"},
                     "projections": [
                       {"name": "properties", "sourceField": "properties", "starVisible": true},
                       {"name": "browser_length", "sourceField": "browser_length", "starVisible": false}
                     ]
                   }
                 ],
                 "savedQueries": [
                   {
                     "name": "saved_events",
                     "queryId": "query-7",
                     "target": {"kind": "VIRTUAL_TABLE", "name": "visible_events"},
                     "fields": [
                       {"name": "properties", "trinoTypeSignature": "json", "logicalType": "JSON", "nullable": true, "starVisible": true},
                       {"name": "browser_length", "trinoTypeSignature": "bigint", "logicalType": "INTEGER", "nullable": true, "starVisible": false}
                     ]
                   }
                 ],
                 "materializedViews": [
                   {
                     "name": "daily_events",
                     "physicalView": {
                       "catalog": {"value": "ducklake", "delimited": false},
                       "schema": {"value": "Analytics", "delimited": true},
                       "table": {"value": "daily_events", "delimited": false}
                     },
                     "fields": [
                       {"name": "day", "trinoTypeSignature": "date", "logicalType": "DATE", "nullable": false, "starVisible": true}
                     ]
                   }
                 ],
                 "functions": [
                   {
                     "name": "length",
                     "kind": "SCALAR",
                     "implementation": "STOCK",
                     "trinoName": [{"value": "length", "delimited": false}],
                     "signatures": [{"argumentTypes": ["json"], "returnType": "bigint", "variadic": false}],
                     "deterministic": true,
                     "supportsDistinct": false,
                     "supportsOrderBy": false,
                     "supportsFilter": false,
                     "supportsWindow": false
                   }
                 ],
                 "modifierDefaults": [
                   {
                     "name": "sampling",
                     "behavior": "TRINO_SESSION_PROPERTY",
                     "defaultValue": {"typeSignature": "bigint", "encoding": "INTEGER", "value": "1"},
                     "sessionProperty": [
                       {"value": "hogql", "delimited": false},
                       {"value": "sampling", "delimited": false}
                     ]
                   }
                 ]
               }
               """;
    }
}
