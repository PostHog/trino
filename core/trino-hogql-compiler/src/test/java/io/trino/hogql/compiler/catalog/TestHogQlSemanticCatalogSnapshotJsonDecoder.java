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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PropertyStorage;
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

        assertThat(snapshot.schemaVersion()).isEqualTo(1);
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
                        validSnapshotJson().replace("\"schemaVersion\": 1", "\"schemaVersion\": 2"),
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
                 "schemaVersion": 1,
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
                 ]
               }
               """;
    }
}
