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

import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.Identity;

import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * An optional capability of a {@link CatalogStore} that does not change catalogs itself, but hands
 * {@code CREATE CATALOG} and {@code DROP CATALOG} to an external writer on behalf of the user who
 * ran the statement. It is meant for a store whose catalogs are published by that writer, typically
 * one that is also a {@link RevisionedCatalogStore}: the writer validates the request, publishes the
 * resulting catalogs, and the coordinator receives them the same way it receives every other
 * published change.
 *
 * <p>When the configured store implements this interface, the engine calls it instead of the
 * store's mutation methods, after its own access control checks and after the catalog properties
 * have been evaluated. A successful call only means that the writer accepted the request; the
 * engine then waits until the change is visible on this coordinator, for at most
 * {@link #visibilityTimeout()}.
 *
 * <p>A store implementing this interface promises that:
 *
 * <ul>
 *   <li>a request either returns normally, meaning the writer accepted it, or throws; an exception
 *       never means that the change was published anyway unless the statement is retried,
 *   <li>repeating an accepted request is safe: {@code IF NOT EXISTS} and {@code IF EXISTS} are
 *       honoured by the writer, so a statement that timed out waiting for visibility can be rerun,
 *   <li>a failure is reported with a message that is safe to show to the user who ran the
 *       statement, and never echoes a catalog property value.
 * </ul>
 */
public interface DelegatedCatalogStore
{
    /**
     * Asks the external writer to create a catalog.
     *
     * @param identity the user who ran the statement; the writer decides what this user may create
     * @param properties the evaluated catalog properties, exactly as the user specified them
     * @param ifNotExists whether an existing catalog of this name is acceptable
     * @throws RuntimeException if the request is rejected or cannot be made
     */
    CatalogRequestResult createCatalog(Identity identity, CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean ifNotExists);

    /**
     * Asks the external writer to drop a catalog.
     *
     * @param identity the user who ran the statement; the writer decides what this user may drop
     * @param ifExists whether a catalog that does not exist is acceptable
     * @throws RuntimeException if the request is rejected or cannot be made
     */
    CatalogRequestResult dropCatalog(Identity identity, CatalogName catalogName, boolean ifExists);

    /**
     * How long the engine waits, after an accepted request, for the change to become visible on
     * this coordinator before failing the statement with a retryable error.
     */
    default Duration visibilityTimeout()
    {
        return Duration.ofMinutes(3);
    }

    /**
     * The outcome of an accepted request.
     *
     * @param changed whether the request changed anything: false when {@code IF NOT EXISTS} matched
     *         an existing catalog, or {@code IF EXISTS} matched none
     */
    record CatalogRequestResult(CatalogName catalogName, boolean changed)
    {
        public CatalogRequestResult
        {
            requireNonNull(catalogName, "catalogName is null");
        }
    }
}
