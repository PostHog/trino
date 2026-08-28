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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface HogQlSemanticCatalogSnapshotProvider
{
    PinnedSnapshot pin(PinRequest request);

    static HogQlSemanticCatalogSnapshotProvider fromCache(HogQlSemanticCatalogSnapshotCache cache)
    {
        requireNonNull(cache, "cache is null");
        return request -> {
            HogQlSemanticCatalogSnapshot snapshot = cache.currentSnapshot(request.catalog(), request.expectedGeneration())
                    .orElseThrow(() -> new HogQlSemanticCatalogException(Failure.UNAVAILABLE, "HogQL semantic catalog snapshot is unavailable"));
            if (!snapshot.catalog().equals(request.catalog())) {
                throw new HogQlSemanticCatalogException(Failure.CATALOG_MISMATCH, "HogQL semantic catalog snapshot does not match the requested catalog");
            }
            if (!snapshot.languageVersion().equals(request.languageVersion())) {
                throw new HogQlSemanticCatalogException(Failure.LANGUAGE_VERSION_MISMATCH, "HogQL semantic catalog snapshot language version does not match the compiler");
            }
            if (request.expectedGeneration().isPresent() && request.expectedGeneration().orElseThrow() != snapshot.generation()) {
                throw new HogQlSemanticCatalogException(Failure.GENERATION_MISMATCH, "HogQL semantic catalog snapshot generation does not match the request");
            }
            return new PinnedSnapshot(snapshot);
        };
    }

    record PinRequest(PhysicalIdentifier catalog, HogQlLanguageVersion languageVersion, OptionalLong expectedGeneration)
    {
        public PinRequest
        {
            catalog = requireNonNull(catalog, "catalog is null");
            languageVersion = requireNonNull(languageVersion, "languageVersion is null");
            expectedGeneration = requireNonNull(expectedGeneration, "expectedGeneration is null");
            if (expectedGeneration.isPresent() && expectedGeneration.orElseThrow() <= 0) {
                throw new IllegalArgumentException("expected HogQL semantic catalog generation must be positive");
            }
        }
    }

    record PinnedSnapshot(HogQlSemanticCatalogSnapshot snapshot)
    {
        public PinnedSnapshot
        {
            snapshot = requireNonNull(snapshot, "snapshot is null");
        }

        public long generation()
        {
            return snapshot.generation();
        }

        public Optional<LogicalTableDefinition> logicalTable(String name)
        {
            return snapshot.logicalTable(name);
        }
    }
}
