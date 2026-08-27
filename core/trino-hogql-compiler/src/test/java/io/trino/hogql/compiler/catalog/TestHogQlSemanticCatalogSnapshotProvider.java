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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException.Failure;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.trino.spi.ErrorType.EXTERNAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogSnapshotProvider
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);

    @Test
    public void testPinsOneSnapshotWithoutCrossGenerationReads()
    {
        AtomicInteger reads = new AtomicInteger();
        HogQlSemanticCatalogSnapshotCache cache = _ -> Optional.of(snapshot(reads.incrementAndGet()));
        HogQlSemanticCatalogSnapshotProvider provider = HogQlSemanticCatalogSnapshotProvider.fromCache(cache);

        PinnedSnapshot pinned = provider.pin(new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.empty()));

        assertThat(reads).hasValue(1);
        assertThat(pinned.generation()).isEqualTo(1);
        assertThat(pinned.snapshot().generation()).isEqualTo(1);
        assertThat(pinned.logicalTable("events")).isPresent();
        assertThat(reads).hasValue(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failClosedCases")
    public void testFailsClosed(String name, HogQlSemanticCatalogSnapshotCache cache, PinRequest request, Failure failure)
    {
        HogQlSemanticCatalogSnapshotProvider provider = HogQlSemanticCatalogSnapshotProvider.fromCache(cache);

        assertThatThrownBy(() -> provider.pin(request))
                .isInstanceOfSatisfying(HogQlSemanticCatalogException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(failure);
                    assertThat(exception.getErrorCode().getName()).isEqualTo(
                            failure == Failure.UNAVAILABLE ? "HOGQL_CATALOG_NOT_READY" : "HOGQL_CATALOG_GENERATION_MISMATCH");
                    assertThat(exception.getErrorCode().getType()).isEqualTo(EXTERNAL);
                });
    }

    private static Stream<Arguments> failClosedCases()
    {
        PhysicalIdentifier otherCatalog = new PhysicalIdentifier("other", false);
        return Stream.of(
                Arguments.of(
                        "unavailable",
                        (HogQlSemanticCatalogSnapshotCache) _ -> Optional.empty(),
                        new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.empty()),
                        Failure.UNAVAILABLE),
                Arguments.of(
                        "cache returned another catalog",
                        (HogQlSemanticCatalogSnapshotCache) _ -> Optional.of(snapshot(otherCatalog, LANGUAGE_VERSION, 1)),
                        new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.empty()),
                        Failure.CATALOG_MISMATCH),
                Arguments.of(
                        "language mismatch",
                        (HogQlSemanticCatalogSnapshotCache) _ -> Optional.of(snapshot(CATALOG, HogQlLanguageVersion.valueOf("2.0.0"), 1)),
                        new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.empty()),
                        Failure.LANGUAGE_VERSION_MISMATCH),
                Arguments.of(
                        "generation mismatch",
                        (HogQlSemanticCatalogSnapshotCache) _ -> Optional.of(snapshot(2)),
                        new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.of(1)),
                        Failure.GENERATION_MISMATCH));
    }

    private static HogQlSemanticCatalogSnapshot snapshot(long generation)
    {
        return snapshot(CATALOG, LANGUAGE_VERSION, generation);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(PhysicalIdentifier catalog, HogQlLanguageVersion languageVersion, long generation)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                languageVersion,
                catalog,
                generation,
                List.of(new LogicalTableDefinition(
                        "events",
                        new PhysicalQualifiedName(catalog, new PhysicalIdentifier("default", false), new PhysicalIdentifier("events", false)),
                        List.of(new LogicalFieldDefinition(
                                "id",
                                new PhysicalIdentifier("id", false),
                                "varchar",
                                LogicalType.STRING,
                                false,
                                true)),
                        List.of(),
                        List.of())));
    }
}
