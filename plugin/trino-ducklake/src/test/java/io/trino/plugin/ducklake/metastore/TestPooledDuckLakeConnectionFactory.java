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
package io.trino.plugin.ducklake.metastore;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPooledDuckLakeConnectionFactory
{
    private static final Duration NO_IDLE_TIMEOUT = new Duration(1, HOURS);
    private static final Duration LONG_ACQUISITION_TIMEOUT = new Duration(30, SECONDS);

    @Test
    void testConnectionIsReused()
            throws Exception
    {
        CountingConnectionFactory delegate = new CountingConnectionFactory();
        PooledDuckLakeConnectionFactory pool = new PooledDuckLakeConnectionFactory(delegate, 4, NO_IDLE_TIMEOUT, LONG_ACQUISITION_TIMEOUT);
        try {
            Connection first = pool.openConnection();
            pool.closeConnection(first);
            assertThat(pool.idleConnectionCount()).isEqualTo(1);

            Connection second = pool.openConnection();
            assertThat(second).isSameAs(first);
            assertThat(pool.idleConnectionCount()).isEqualTo(0);
            pool.closeConnection(second);

            assertThat(delegate.openedConnectionCount()).isEqualTo(1);
        }
        finally {
            pool.close();
        }
    }

    @Test
    void testPoolIsBounded()
            throws Exception
    {
        CountingConnectionFactory delegate = new CountingConnectionFactory();
        PooledDuckLakeConnectionFactory pool = new PooledDuckLakeConnectionFactory(delegate, 2, NO_IDLE_TIMEOUT, LONG_ACQUISITION_TIMEOUT);
        ExecutorService executor = newCachedThreadPool();
        try {
            Connection first = pool.openConnection();
            Connection second = pool.openConnection();
            assertThat(delegate.openedConnectionCount()).isEqualTo(2);

            CountDownLatch started = new CountDownLatch(1);
            Future<Connection> third = executor.submit(() -> {
                started.countDown();
                return pool.openConnection();
            });
            assertThat(started.await(30, SECONDS)).isTrue();

            // the pool is exhausted, so the third connection cannot be handed out
            assertThatThrownBy(() -> third.get(1, SECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(delegate.openedConnectionCount()).isEqualTo(2);

            // returning a connection unblocks the waiting caller, without opening a new connection
            pool.closeConnection(first);
            assertThat(third.get(30, SECONDS)).isSameAs(first);
            assertThat(delegate.openedConnectionCount()).isEqualTo(2);

            pool.closeConnection(second);
            pool.closeConnection(third.get());
        }
        finally {
            executor.shutdownNow();
            pool.close();
        }
    }

    @Test
    void testAcquisitionTimesOut()
            throws Exception
    {
        CountingConnectionFactory delegate = new CountingConnectionFactory();
        PooledDuckLakeConnectionFactory pool = new PooledDuckLakeConnectionFactory(delegate, 1, NO_IDLE_TIMEOUT, new Duration(50, MILLISECONDS));
        try {
            Connection connection = pool.openConnection();

            assertThatThrownBy(pool::openConnection)
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("waiting for a DuckLake metadata database connection, all 1 pooled connections are in use");

            pool.closeConnection(connection);
            // the pool works again once the connection is returned
            pool.closeConnection(pool.openConnection());
        }
        finally {
            pool.close();
        }
    }

    @Test
    void testIdleConnectionsAreClosed()
            throws Exception
    {
        CountingConnectionFactory delegate = new CountingConnectionFactory();
        PooledDuckLakeConnectionFactory pool = new PooledDuckLakeConnectionFactory(delegate, 4, new Duration(1, MILLISECONDS), LONG_ACQUISITION_TIMEOUT);
        try {
            pool.closeConnection(pool.openConnection());

            assertEventually(new Duration(30, SECONDS), () -> {
                assertThat(pool.idleConnectionCount()).isEqualTo(0);
                assertThat(delegate.openConnectionCount()).isEqualTo(0);
            });

            // an expired connection is never handed out again
            Connection connection = pool.openConnection();
            assertThat(delegate.openedConnectionCount()).isEqualTo(2);
            pool.closeConnection(connection);
        }
        finally {
            pool.close();
        }
    }

    @Test
    void testCloseReleasesIdleConnections()
            throws Exception
    {
        CountingConnectionFactory delegate = new CountingConnectionFactory();
        PooledDuckLakeConnectionFactory pool = new PooledDuckLakeConnectionFactory(delegate, 4, NO_IDLE_TIMEOUT, LONG_ACQUISITION_TIMEOUT);
        pool.closeConnection(pool.openConnection());
        pool.closeConnection(pool.openConnection());
        assertThat(delegate.openConnectionCount()).isEqualTo(1);

        pool.close();

        assertThat(pool.idleConnectionCount()).isEqualTo(0);
        assertThat(delegate.openConnectionCount()).isEqualTo(0);
    }

    private static final class CountingConnectionFactory
            implements ConnectionFactory
    {
        private final List<Connection> connections = new ArrayList<>();

        @Override
        public synchronized Connection openConnection()
                throws SQLException
        {
            // DuckDB serves as a cheap source of real JDBC connections
            Connection connection = DriverManager.getConnection("jdbc:duckdb:");
            connections.add(connection);
            return connection;
        }

        public synchronized int openedConnectionCount()
        {
            return connections.size();
        }

        public int openConnectionCount()
                throws SQLException
        {
            List<Connection> snapshot;
            synchronized (this) {
                snapshot = ImmutableList.copyOf(connections);
            }
            int open = 0;
            for (Connection connection : snapshot) {
                if (!connection.isClosed()) {
                    open++;
                }
            }
            return open;
        }
    }
}
