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
package io.trino.hogql.compiler;

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider;

import static java.util.Objects.requireNonNull;

public record HogQlSemanticCatalogContext(
        PhysicalIdentifier catalog,
        HogQlSemanticCatalogSnapshotProvider snapshotProvider)
{
    public HogQlSemanticCatalogContext
    {
        catalog = requireNonNull(catalog, "catalog is null");
        snapshotProvider = requireNonNull(snapshotProvider, "snapshotProvider is null");
    }
}
