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
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Map;
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
        HoglakeTransactionHandle handle = new HoglakeTransactionHandle(UUID.randomUUID());
        transactions.put(handle, metadata.newTransaction());
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
        transactions.remove(handle);
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
    public java.util.List<io.trino.spi.session.PropertyMetadata<?>> getTableProperties()
    {
        return java.util.List.of(new io.trino.spi.session.PropertyMetadata<>(
                        "partitioning",
                        "Partition transforms",
                        new io.trino.spi.type.ArrayType(io.trino.spi.type.VarcharType.VARCHAR),
                        java.util.List.class,
                        java.util.List.of(),
                        false,
                        value -> (java.util.List<?>) value,
                        value -> value),
                new io.trino.spi.session.PropertyMetadata<>(
                        "sorted_by",
                        "Per-file sort fields",
                        new io.trino.spi.type.ArrayType(io.trino.spi.type.VarcharType.VARCHAR),
                        java.util.List.class,
                        java.util.List.of(),
                        false,
                        value -> (java.util.List<?>) value,
                        value -> value),
                new io.trino.spi.session.PropertyMetadata<>(
                        "extra_properties",
                        "Custom metadata (replaced as a whole by SET PROPERTIES)",
                        new io.trino.spi.type.MapType(io.trino.spi.type.VarcharType.VARCHAR, io.trino.spi.type.VarcharType.VARCHAR, new io.trino.spi.type.TypeOperators()),
                        java.util.Map.class,
                        java.util.Map.of(),
                        false,
                        value -> {
                            java.util.Map<?, ?> properties = (java.util.Map<?, ?>) value;
                            if (properties.values().stream().anyMatch(java.util.Objects::isNull)) {
                                throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY, "Custom property values cannot be null");
                            }
                            return java.util.Map.copyOf(properties);
                        },
                        value -> value));
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
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
