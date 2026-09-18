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
package io.trino.spi.catalog;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * An optional capability of a {@link CatalogStore} whose catalogs are published by an external
 * writer while Trino only reads them. A store implementing this interface can be polled for a
 * small revision value and, when that revision changes, asked for a complete and consistent
 * snapshot of every catalog of the cluster.
 *
 * <p>A store that implements this interface promises that:
 *
 * <ul>
 *   <li>the revision is monotonically increasing and changes whenever the set of catalogs or any
 *       of their definitions changes,
 *   <li>{@link #fetchSnapshot()} returns the revision and the catalogs of a single consistent
 *       state, never a mixture of two,
 *   <li>an incomplete, partially written or unreadable state is reported by throwing, and never
 *       as a snapshot with missing catalogs. Missing catalogs mean deletion, so guessing there is
 *       not safe.
 * </ul>
 */
public interface RevisionedCatalogStore
{
    /**
     * Revision of the currently published state. This is polled frequently, so it must be cheap.
     * A store that has never been written to reports {@code 0}.
     *
     * @throws RuntimeException if the current revision cannot be determined
     */
    long currentRevision();

    /**
     * Complete published state, read as one consistent snapshot.
     *
     * @throws RuntimeException if the state is incomplete, unreadable or inconsistent
     */
    CatalogSnapshot fetchSnapshot();

    /**
     * A complete set of catalog definitions and the revision they belong to.
     */
    record CatalogSnapshot(long revision, List<CatalogProperties> catalogs)
    {
        public CatalogSnapshot
        {
            if (revision < 0) {
                throw new IllegalArgumentException("revision is negative: " + revision);
            }
            requireNonNull(catalogs, "catalogs is null");
            catalogs = List.copyOf(catalogs);
        }
    }
}
