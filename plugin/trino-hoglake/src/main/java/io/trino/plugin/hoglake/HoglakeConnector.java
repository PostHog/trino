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
package io.trino.plugin.hoglake;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Hoglake connector: metadata and commits through REST, Parquet through the filesystem.
 */
public class HoglakeConnector
        implements Connector
{
    private final HoglakeMetadata metadata;
    private final Map<ConnectorTransactionHandle, HoglakeMetadata> transactions = new ConcurrentHashMap<>();
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final LifeCycleManager lifeCycleManager;

    public HoglakeConnector(
            HoglakeMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            LifeCycleManager lifeCycleManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommit)
    {
        IsolationLevel.checkConnectorSupports(IsolationLevel.REPEATABLE_READ, isolationLevel);
        HoglakeTransactionHandle handle = new HoglakeTransactionHandle(UUID.randomUUID());
        transactions.put(handle, metadata.newTransaction(autoCommit));
        return handle;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return requireNonNull(transactions.get(transactionHandle), "Unknown transaction");
    }

    @Override
    public void commit(ConnectorTransactionHandle handle)
    {
        // Trino marks the catalog transaction finished before invoking commit,
        // so an exception will not be followed by connector rollback.
        HoglakeMetadata transaction = transactions.remove(handle);
        if (transaction != null) {
            transaction.commit();
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle handle)
    {
        HoglakeMetadata transaction = transactions.remove(handle);
        if (transaction != null) {
            transaction.rollback();
        }
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return Set.of(ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return List.of(new PropertyMetadata<>(
                        "partitioning",
                        "Partition transforms",
                        new ArrayType(VarcharType.VARCHAR),
                        List.class,
                        List.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        "sorted_by",
                        "Per-file sort fields",
                        new ArrayType(VarcharType.VARCHAR),
                        List.class,
                        List.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        "extra_properties",
                        "Custom metadata (replaced as a whole by SET PROPERTIES)",
                        new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()),
                        Map.class,
                        Map.of(),
                        false,
                        value -> {
                            Map<?, ?> properties = (Map<?, ?>) value;
                            if (properties.values().stream().anyMatch(Objects::isNull)) {
                                throw new TrinoException(StandardErrorCode.INVALID_TABLE_PROPERTY, "Custom property values cannot be null");
                            }
                            return Map.copyOf(properties);
                        },
                        value -> value));
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return false;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
