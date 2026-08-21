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
package io.trino.plugin.catalogstore.posthog;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStore.StoredCatalog;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestPostHogCatalogStore
{
    private static final ConnectorName DUCKLAKE = new ConnectorName("ducklake");

    /**
     * A value that only makes sense once the engine has resolved it on each node. The store must
     * hand it back exactly as it was given.
     */
    private static final Map<String, String> PROPERTIES_WITH_SECRET_REFERENCE = ImmutableMap.of(
            "ducklake.metadata.connection-url", "jdbc:postgresql://lake:5432/org_17",
            "ducklake.metadata.connection-password", "${ENV:FOO}",
            "ducklake.data-path", "s3://bucket/org_17/");

    private TestingCatalogStoreDatabase database;

    @BeforeAll
    void startDatabase()
    {
        database = new TestingCatalogStoreDatabase();
    }

    @AfterAll
    void stopDatabase()
    {
        database.close();
        database = null;
    }

    @Test
    void testCatalogRoundTrip()
    {
        CatalogStore store = createStore(randomCellId());
        CatalogName catalogName = new CatalogName("org_17");

        CatalogProperties catalog = store.createCatalogProperties(catalogName, DUCKLAKE, PROPERTIES_WITH_SECRET_REFERENCE);
        // createCatalogProperties only assigns a version, the engine adds the catalog once it is created
        assertThat(store.getCatalogs()).isEmpty();

        store.addOrReplaceCatalog(catalog);

        StoredCatalog stored = getOnlyElement(store.getCatalogs());
        assertThat(stored.name()).isEqualTo(catalogName);
        assertThat(stored.loadProperties()).isEqualTo(catalog);
        assertThat(stored.loadProperties().properties())
                .containsEntry("ducklake.metadata.connection-password", "${ENV:FOO}");

        Map<String, String> changedProperties = ImmutableMap.<String, String>builder()
                .putAll(PROPERTIES_WITH_SECRET_REFERENCE)
                .put("ducklake.max-split-size", "32MB")
                .buildOrThrow();
        CatalogProperties replacement = store.createCatalogProperties(catalogName, DUCKLAKE, changedProperties);
        assertThat(replacement.version()).isNotEqualTo(catalog.version());

        store.addOrReplaceCatalog(replacement);

        StoredCatalog replaced = getOnlyElement(store.getCatalogs());
        assertThat(replaced.loadProperties()).isEqualTo(replacement);
        assertThat(replaced.loadProperties().properties()).isEqualTo(changedProperties);

        store.removeCatalog(catalogName);
        assertThat(store.getCatalogs()).isEmpty();

        // removing a catalog that is not there is not an error
        store.removeCatalog(catalogName);
        assertThat(store.getCatalogs()).isEmpty();
    }

    @Test
    void testCatalogsAreVisibleToAnotherStoreOfTheSameCell()
    {
        String cellId = randomCellId();
        CatalogStore store = createStore(cellId);
        CatalogName catalogName = new CatalogName("org_23");

        CatalogProperties catalog = store.createCatalogProperties(catalogName, DUCKLAKE, PROPERTIES_WITH_SECRET_REFERENCE);
        store.addOrReplaceCatalog(catalog);

        // a second coordinator of the same cell, starting against the same database
        CatalogStore otherCoordinator = createStore(cellId);
        assertThat(getOnlyElement(otherCoordinator.getCatalogs()).loadProperties()).isEqualTo(catalog);
    }

    @Test
    void testCellsAreIsolated()
    {
        String firstCell = randomCellId();
        String secondCell = randomCellId();
        CatalogStore firstStore = createStore(firstCell);
        CatalogStore secondStore = createStore(secondCell);

        CatalogName sharedName = new CatalogName("org_42");
        CatalogProperties firstCatalog = firstStore.createCatalogProperties(sharedName, DUCKLAKE, ImmutableMap.of("ducklake.data-path", "s3://first/"));
        CatalogProperties secondCatalog = secondStore.createCatalogProperties(sharedName, DUCKLAKE, ImmutableMap.of("ducklake.data-path", "s3://second/"));
        CatalogProperties secondOnlyCatalog = secondStore.createCatalogProperties(new CatalogName("org_43"), DUCKLAKE, ImmutableMap.of("ducklake.data-path", "s3://second/43/"));

        firstStore.addOrReplaceCatalog(firstCatalog);
        secondStore.addOrReplaceCatalog(secondCatalog);
        secondStore.addOrReplaceCatalog(secondOnlyCatalog);

        assertThat(loadAll(firstStore)).containsExactly(firstCatalog);
        assertThat(loadAll(secondStore)).containsExactlyInAnyOrder(secondCatalog, secondOnlyCatalog);

        // removing a catalog of one cell leaves the same catalog name of the other cell alone
        firstStore.removeCatalog(sharedName);

        assertThat(firstStore.getCatalogs()).isEmpty();
        assertThat(loadAll(secondStore)).containsExactlyInAnyOrder(secondCatalog, secondOnlyCatalog);
    }

    /**
     * The engine registers a catalog whose properties fail to load as failed and keeps serving the
     * rest, so a single unreadable row must never keep the healthy catalogs of a cell from loading.
     */
    @Test
    void testMalformedRowDoesNotHideHealthyCatalogs()
    {
        String cellId = randomCellId();
        CatalogStore store = createStore(cellId);

        CatalogProperties healthy = store.createCatalogProperties(new CatalogName("healthy"), DUCKLAKE, PROPERTIES_WITH_SECRET_REFERENCE);
        store.addOrReplaceCatalog(healthy);
        database.execute(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties)
                VALUES ('%s', 'malformed', 'ducklake', 'some-version', 'this is not json')
                """.formatted(cellId));

        Collection<StoredCatalog> catalogs = store.getCatalogs();
        assertThat(catalogs).hasSize(2);

        StoredCatalog malformed = catalogs.stream()
                .filter(catalog -> catalog.name().equals(new CatalogName("malformed")))
                .collect(onlyElement());
        assertThatThrownBy(malformed::loadProperties)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid properties stored for catalog 'malformed'");

        StoredCatalog stored = catalogs.stream()
                .filter(catalog -> catalog.name().equals(new CatalogName("healthy")))
                .collect(onlyElement());
        assertThat(stored.loadProperties()).isEqualTo(healthy);
    }

    /**
     * A row whose catalog name the engine could never accept cannot even be represented, so it is
     * skipped rather than failing the whole startup.
     */
    @Test
    void testUnusableRowIsSkipped()
    {
        String cellId = randomCellId();
        CatalogStore store = createStore(cellId);

        CatalogProperties healthy = store.createCatalogProperties(new CatalogName("healthy"), DUCKLAKE, PROPERTIES_WITH_SECRET_REFERENCE);
        store.addOrReplaceCatalog(healthy);
        database.execute(
                """
                INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties)
                VALUES ('%s', 'NOT_LOWERCASE', 'ducklake', 'some-version', '{}')
                """.formatted(cellId));

        assertThat(loadAll(store)).containsExactly(healthy);
    }

    private CatalogStore createStore(String cellId)
    {
        return getOnlyElement(new PostHogCatalogStorePlugin().getCatalogStoreFactories())
                .create(database.storeProperties(cellId));
    }

    private static Collection<CatalogProperties> loadAll(CatalogStore store)
    {
        return store.getCatalogs().stream()
                .map(StoredCatalog::loadProperties)
                .collect(toImmutableList());
    }

    private static String randomCellId()
    {
        return "cell" + randomNameSuffix();
    }
}
