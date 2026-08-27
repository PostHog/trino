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
package io.trino.connector;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.catalog.CatalogName;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class CatalogLifecycleListeners
{
    private static final Logger log = Logger.get(CatalogLifecycleListeners.class);

    private final Set<CatalogLifecycleListener> listeners;

    @Inject
    public CatalogLifecycleListeners(Set<CatalogLifecycleListener> listeners)
    {
        this.listeners = Set.copyOf(requireNonNull(listeners, "listeners is null"));
    }

    public void catalogLoaded(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        for (CatalogLifecycleListener listener : listeners) {
            try {
                listener.catalogLoaded(catalogName);
            }
            catch (RuntimeException failure) {
                log.warn(failure, "Catalog lifecycle listener failed for %s", catalogName);
            }
        }
    }
}
