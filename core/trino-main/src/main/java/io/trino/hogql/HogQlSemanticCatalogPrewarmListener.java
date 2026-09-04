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
package io.trino.hogql;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.connector.CatalogLifecycleListener;
import io.trino.spi.catalog.CatalogName;

import static io.trino.hogql.HogQlCatalogIdentifiers.physicalCatalog;
import static java.util.Objects.requireNonNull;

public final class HogQlSemanticCatalogPrewarmListener
        implements CatalogLifecycleListener
{
    private static final Logger log = Logger.get(HogQlSemanticCatalogPrewarmListener.class);

    private final HogQlSemanticCatalogManager semanticCatalogManager;

    @Inject
    public HogQlSemanticCatalogPrewarmListener(HogQlSemanticCatalogManager semanticCatalogManager)
    {
        this.semanticCatalogManager = requireNonNull(semanticCatalogManager, "semanticCatalogManager is null");
    }

    @Override
    public void catalogLoaded(CatalogName catalogName)
    {
        semanticCatalogManager.prewarm(physicalCatalog(catalogName.toString()))
                .whenComplete((_, failure) -> {
                    if (failure != null) {
                        log.warn(failure, "Failed to prewarm HogQL semantic catalog for %s", catalogName);
                    }
                });
    }
}
