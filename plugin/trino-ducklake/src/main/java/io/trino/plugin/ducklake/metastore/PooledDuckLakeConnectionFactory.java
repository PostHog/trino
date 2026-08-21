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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.ducklake.DuckLakeConfig;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;
import org.jdbi.v3.core.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Pools the connections to the DuckLake catalog database, so that a Trino node hosting many
 * DuckLake catalogs does not open a new connection for every metastore call.
 * <p>
 * The pool holds at most {@code ducklake.metadata.connection-pool.max-size} connections, keeps no
 * minimum number of idle connections, and closes connections that were idle for longer than
 * {@code ducklake.metadata.connection-pool.idle-timeout}, so an inactive catalog eventually holds
 * no server connections at all. A query that cannot get a connection within
 * {@code ducklake.metadata.connection-pool.acquisition-timeout} fails instead of hanging.
 * <p>
 * Connections are handed back through {@link #closeConnection(Connection)}, which JDBI calls when
 * the {@link org.jdbi.v3.core.Handle} using the connection is closed.
 */
@ThreadSafe
public final class PooledDuckLakeConnectionFactory
        implements ConnectionFactory
{
    private static final Logger log = Logger.get(PooledDuckLakeConnectionFactory.class);

    private static final long MINIMUM_EVICTION_PERIOD_MILLIS = 100;

    private final ConnectionFactory delegate;
    private final int maxSize;
    private final Duration acquisitionTimeout;
    private final long idleTimeoutNanos;
    private final Semaphore permits;
    private final ScheduledExecutorService evictionExecutor;

    @GuardedBy("this")
    private final Deque<IdleConnection> idleConnections = new ArrayDeque<>();
    @GuardedBy("this")
    private boolean closed;

    @Inject
    public PooledDuckLakeConnectionFactory(@ForDuckLakeMetastoreDriver ConnectionFactory delegate, DuckLakeConfig config)
    {
        this(delegate,
                config.getConnectionPoolMaxSize(),
                config.getConnectionPoolIdleTimeout(),
                config.getConnectionPoolAcquisitionTimeout());
    }

    @VisibleForTesting
    public PooledDuckLakeConnectionFactory(ConnectionFactory delegate, int maxSize, Duration idleTimeout, Duration acquisitionTimeout)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(maxSize >= 1, "maxSize must be at least 1");
        this.maxSize = maxSize;
        this.idleTimeoutNanos = requireNonNull(idleTimeout, "idleTimeout is null").roundTo(NANOSECONDS);
        this.acquisitionTimeout = requireNonNull(acquisitionTimeout, "acquisitionTimeout is null");
        this.permits = new Semaphore(maxSize, true);
        this.evictionExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("ducklake-metastore-connection-evictor-%s"));
        long period = max(idleTimeout.roundTo(MILLISECONDS) / 2, MINIMUM_EVICTION_PERIOD_MILLIS);
        evictionExecutor.scheduleWithFixedDelay(this::evictIdleConnections, period, period, MILLISECONDS);
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(acquisitionTimeout.roundTo(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Interrupted while waiting for a DuckLake metadata database connection", e);
        }
        if (!acquired) {
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Timed out after %s waiting for a DuckLake metadata database connection, all %s pooled connections are in use".formatted(acquisitionTimeout, maxSize));
        }
        boolean success = false;
        try {
            Connection connection = borrowConnection();
            success = true;
            return connection;
        }
        finally {
            if (!success) {
                permits.release();
            }
        }
    }

    /**
     * Returns a connection to the pool. JDBI calls this when the handle holding the connection is
     * closed; see {@link ConnectionFactory#getCleanableFor(Connection)}.
     */
    @Override
    public void closeConnection(Connection connection)
    {
        requireNonNull(connection, "connection is null");
        try {
            Connection pooled = reusable(connection) ? offer(connection) : connection;
            if (pooled != null) {
                closeQuietly(pooled);
            }
        }
        finally {
            permits.release();
        }
    }

    /**
     * Closes the pool. Bound as the connector shuts down, so that dropping a catalog does not leak
     * connections to the catalog database.
     */
    @PreDestroy
    public void close()
    {
        List<IdleConnection> connections;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            connections = ImmutableList.copyOf(idleConnections);
            idleConnections.clear();
        }
        evictionExecutor.shutdownNow();
        connections.forEach(idle -> closeQuietly(idle.connection()));
    }

    @VisibleForTesting
    public synchronized int idleConnectionCount()
    {
        return idleConnections.size();
    }

    private Connection borrowConnection()
            throws SQLException
    {
        while (true) {
            IdleConnection idle;
            synchronized (this) {
                if (closed) {
                    throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "DuckLake metadata database connection pool is closed");
                }
                idle = idleConnections.pollLast();
            }
            if (idle == null) {
                return delegate.openConnection();
            }
            if (isExpired(idle) || !reusable(idle.connection())) {
                closeQuietly(idle.connection());
                continue;
            }
            return idle.connection();
        }
    }

    /**
     * Adds the connection to the idle set, returning it back to the caller if the pool is already
     * closed and the connection has to be closed instead.
     */
    private synchronized Connection offer(Connection connection)
    {
        if (closed) {
            return connection;
        }
        idleConnections.addLast(new IdleConnection(connection, System.nanoTime()));
        return null;
    }

    private void evictIdleConnections()
    {
        ImmutableList.Builder<Connection> expired = ImmutableList.builder();
        synchronized (this) {
            while (!idleConnections.isEmpty() && isExpired(idleConnections.peekFirst())) {
                expired.add(idleConnections.pollFirst().connection());
            }
        }
        expired.build().forEach(PooledDuckLakeConnectionFactory::closeQuietly);
    }

    private boolean isExpired(IdleConnection idle)
    {
        return System.nanoTime() - idle.idleSinceNanos() >= idleTimeoutNanos;
    }

    /**
     * Restores the default connection state and verifies that the connection is still usable.
     */
    private static boolean reusable(Connection connection)
    {
        try {
            if (connection.isClosed()) {
                return false;
            }
            if (!connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
            return true;
        }
        catch (SQLException e) {
            log.debug(e, "Discarding unusable DuckLake metadata database connection");
            return false;
        }
    }

    private static void closeQuietly(Connection connection)
    {
        try {
            connection.close();
        }
        catch (SQLException e) {
            log.warn(e, "Failed to close DuckLake metadata database connection");
        }
    }

    private record IdleConnection(Connection connection, long idleSinceNanos)
    {
        private IdleConnection
        {
            requireNonNull(connection, "connection is null");
        }
    }
}
