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
package io.trino.plugin.ducklake;

import io.airlift.units.Duration;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.max;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * A Trino node hosting one DuckLake catalog per tenant must not open a connection to the catalog
 * database for every metastore call, so the connector pools them.
 * <p>
 * Every test runs its own query runner, and each of them is closed before the next one starts, so
 * that the connections counted on the catalog database belong to a single pool.
 */
@TestInstance(PER_CLASS)
final class TestDuckLakeMetastoreConnectionPool
{
    private static final int MAX_POOL_SIZE = 3;
    /**
     * Longer than any test, so that only closing the pool releases its connections.
     */
    private static final String NO_IDLE_TIMEOUT = "1h";

    private TestingDuckLakeCatalog catalog;

    @BeforeAll
    void setUp()
    {
        catalog = new TestingDuckLakeCatalog();
        try {
            catalog.executeInDuckDb("CREATE TABLE region AS SELECT * FROM (VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA')) t(regionkey, name)");
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }
    }

    @AfterAll
    void tearDown()
    {
        catalog.close();
        catalog = null;
    }

    @Test
    void testQueryResultsWithPooledConnections()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(NO_IDLE_TIMEOUT)) {
            assertThat(queryRunner.execute("SELECT count(*) FROM region").getOnlyValue()).isEqualTo(3L);
            assertThat(queryRunner.execute("SELECT name FROM region ORDER BY regionkey").getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("AFRICA", "AMERICA", "ASIA");
            assertThat(queryRunner.execute("SHOW TABLES").getOnlyValue()).isEqualTo("region");
        }
    }

    @Test
    void testConnectionPoolIsBounded()
            throws Exception
    {
        awaitNoConnections();
        long observedConnections;
        try (QueryRunner queryRunner = createQueryRunner(NO_IDLE_TIMEOUT)) {
            int concurrency = 4 * MAX_POOL_SIZE;
            ExecutorService executor = newFixedThreadPool(concurrency);
            AtomicBoolean running = new AtomicBoolean(true);
            List<Future<?>> queries = new ArrayList<>();
            observedConnections = 0;
            try {
                for (int i = 0; i < concurrency; i++) {
                    queries.add(executor.submit(() -> {
                        while (running.get()) {
                            assertThat(queryRunner.execute("SELECT count(*) FROM region").getOnlyValue()).isEqualTo(3L);
                        }
                        return null;
                    }));
                }
                long deadline = System.nanoTime() + SECONDS.toNanos(10);
                while (System.nanoTime() < deadline) {
                    observedConnections = max(observedConnections, catalog.connectorConnectionCount());
                }
            }
            finally {
                running.set(false);
                executor.shutdown();
            }
            for (Future<?> query : queries) {
                query.get();
            }
        }

        // an unpooled connector opens a connection for each of the ~19 metastore calls a query
        // makes, so this many concurrent queries would hold far more than MAX_POOL_SIZE of them
        assertThat(observedConnections).isBetween(1L, (long) MAX_POOL_SIZE);
    }

    @Test
    void testIdleConnectionsAreReleased()
            throws Exception
    {
        awaitNoConnections();
        try (QueryRunner queryRunner = createQueryRunner("1s")) {
            assertThat(queryRunner.execute("SELECT count(*) FROM region").getOnlyValue()).isEqualTo(3L);
            assertThat(catalog.connectorConnectionCount()).isPositive();

            // an idle catalog eventually holds no connection to the catalog database at all
            awaitNoConnections();

            // the pool still works after its connections were released
            assertThat(queryRunner.execute("SELECT count(*) FROM region").getOnlyValue()).isEqualTo(3L);
        }
    }

    @Test
    void testConnectionsAreReleasedOnShutdown()
            throws Exception
    {
        awaitNoConnections();
        try (QueryRunner queryRunner = createQueryRunner(NO_IDLE_TIMEOUT)) {
            assertThat(queryRunner.execute("SELECT count(*) FROM region").getOnlyValue()).isEqualTo(3L);
            assertThat(catalog.connectorConnectionCount()).isPositive();
        }

        // closing the query runner shuts the connector down, which closes the pool; nothing else
        // could have released the connections, because they never time out
        awaitNoConnections();
    }

    private QueryRunner createQueryRunner(String idleTimeout)
            throws Exception
    {
        return DuckLakeQueryRunner.builder(catalog)
                .addConnectorProperty("ducklake.metadata.connection-pool.max-size", Integer.toString(MAX_POOL_SIZE))
                .addConnectorProperty("ducklake.metadata.connection-pool.idle-timeout", idleTimeout)
                // the metastore is only used on the coordinator, so a worker would only add noise
                // to the number of connections counted against the catalog database
                .setWorkerCount(0)
                .build();
    }

    private void awaitNoConnections()
    {
        assertEventually(new Duration(60, SECONDS), () -> assertThat(catalog.connectorConnectionCount()).isEqualTo(0));
    }
}
