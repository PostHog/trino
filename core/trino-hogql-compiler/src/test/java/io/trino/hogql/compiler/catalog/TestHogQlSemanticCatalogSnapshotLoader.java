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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.JsonTransport;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogSnapshotLoader
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);

    @Test
    public void testPreservesLatestAndPinnedReadSemanticsThroughJsonTransport()
    {
        List<LoadRequest> requests = new ArrayList<>();
        JsonTransport transport = request -> {
            requests.add(request);
            return CompletableFuture.completedFuture(snapshotJson(request.expectedGeneration().orElse(9)).getBytes(StandardCharsets.UTF_8));
        };
        HogQlSemanticCatalogSnapshotLoader loader = HogQlSemanticCatalogSnapshotLoader.fromJsonTransport(
                transport,
                new HogQlSemanticCatalogSnapshotJsonDecoder());

        HogQlSemanticCatalogSnapshot latest = loader.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join();
        HogQlSemanticCatalogSnapshot pinned = loader.load(LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 7)).toCompletableFuture().join();

        assertThat(latest.generation()).isEqualTo(9);
        assertThat(pinned.generation()).isEqualTo(7);
        assertThat(requests).containsExactly(
                LoadRequest.latest(CATALOG, LANGUAGE_VERSION),
                LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 7));
    }

    @Test
    public void testRejectsInvalidPinnedGenerationBeforeTransport()
    {
        assertThatThrownBy(() -> LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("ducklake");
    }

    private static String snapshotJson(long generation)
    {
        return """
               {
                 "protocolVersion": 1,
                 "schemaVersion": 2,
                 "languageVersion": "1.0.0",
                 "catalog": {"value": "ducklake", "delimited": false},
                 "generation": %s,
                 "logicalTables": [],
                 "expressionFields": [],
                 "virtualTables": [],
                 "savedQueries": [],
                 "materializedViews": [],
                 "functions": [],
                 "modifierDefaults": []
               }
               """.formatted(generation);
    }
}
