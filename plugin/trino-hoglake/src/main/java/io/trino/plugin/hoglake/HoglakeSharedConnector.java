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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorWritableTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Shared metadata discovery without filesystem or data execution authority.
 */
public final class HoglakeSharedConnector
        implements Connector
{
    private final HoglakeSharedHttpClient client;
    private final HoglakeSharedSessionVerifier verifier;
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;

    public HoglakeSharedConnector(HoglakeSharedHttpClient client, HoglakeSharedSessionVerifier verifier, LifeCycleManager lifeCycleManager)
    {
        this.client = requireNonNull(client, "client is null");
        this.verifier = requireNonNull(verifier, "verifier is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        ClassLoader classLoader = getClass().getClassLoader();
        splitManager = new ClassLoaderSafeConnectorSplitManager(new DenyingSplitManager(), classLoader);
        pageSourceProvider = new ClassLoaderSafeConnectorPageSourceProvider(new DenyingPageSourceProvider(), classLoader);
        pageSinkProvider = new ClassLoaderSafeConnectorPageSinkProvider(new DenyingPageSinkProvider(), classLoader);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return new HoglakeTransactionHandle(UUID.randomUUID());
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        verifier.verify(session);
        return new SharedMetadata();
    }

    @Override
    public void commit(ConnectorTransactionHandle handle) {}

    @Override
    public void rollback(ConnectorTransactionHandle handle) {}

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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public void shutdown()
    {
        try {
            client.close();
        }
        finally {
            lifeCycleManager.stop();
        }
    }

    private TrinoException denyExecution(ConnectorSession session)
    {
        verifier.verify(session);
        return new TrinoException(NOT_SUPPORTED, "Shared Hoglake supports metadata discovery only");
    }

    private final class DenyingSplitManager
            implements ConnectorSplitManager
    {
        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> dynamicFilterColumns,
                Constraint constraint)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
        {
            throw denyExecution(session);
        }
    }

    private final class DenyingPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                MemoryContext memoryContext)
        {
            throw denyExecution(session);
        }
    }

    private final class DenyingPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorInsertTableHandle insertTableHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorTableExecuteHandle tableExecuteHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorMergeSink createMergeSink(
                ConnectorTransactionHandle transactionHandle,
                ConnectorSession session,
                ConnectorMergeTableHandle mergeHandle,
                Optional<ConnectorTableCredentials> tableCredentials,
                ConnectorPageSinkId pageSinkId,
                MemoryContext memoryContext)
        {
            throw denyExecution(session);
        }
    }

    private final class SharedMetadata
            implements ConnectorMetadata
    {
        @Override
        public boolean schemaExists(ConnectorSession session, String schemaName)
        {
            return client.namespaceExists(session, schemaName);
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return client.listNamespaces(session).stream()
                    .map(HoglakeDtos.Namespace::name)
                    .collect(toImmutableList());
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
        {
            verifier.verify(session);
            List<String> schemas = schemaName.map(List::of).orElseGet(() -> listSchemaNames(session));
            return schemas.stream()
                    .flatMap(schema -> client.listTables(session, schema).stream()
                            .map(table -> new SchemaTableName(schema, table.name())))
                    .collect(toImmutableList());
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion)
        {
            throw denyExecution(session);
        }

        @Override
        public Iterator<RelationColumnsMetadata> streamRelationColumns(
                ConnectorSession session,
                Optional<String> schemaName,
                UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            verifier.verify(session);
            Set<SchemaTableName> tables = ImmutableSet.copyOf(listTables(session, schemaName));
            List<RelationColumnsMetadata> columns = relationFilter.apply(tables).stream()
                    .filter(tables::contains)
                    .flatMap(name -> client.table(session, name.getSchemaName(), name.getTableName()).stream()
                            .map(table -> RelationColumnsMetadata.forTable(name, columns(table))))
                    .collect(toImmutableList());
            return columns.iterator();
        }

        @Override
        public Iterator<RelationCommentMetadata> streamRelationComments(
                ConnectorSession session,
                Optional<String> schemaName,
                UnaryOperator<Set<SchemaTableName>> relationFilter)
        {
            verifier.verify(session);
            Set<SchemaTableName> tables = ImmutableSet.copyOf(listTables(session, schemaName));
            List<RelationCommentMetadata> comments = relationFilter.apply(tables).stream()
                    .filter(tables::contains)
                    .flatMap(name -> client.table(session, name.getSchemaName(), name.getTableName()).stream()
                            .map(table -> RelationCommentMetadata.forRelation(name, Optional.ofNullable(table.comment()))))
                    .collect(toImmutableList());
            return comments.iterator();
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            verifier.verify(session);
            if (prefix.getTable().isPresent()) {
                throw denyExecution(session);
            }
            ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
            streamRelationColumns(session, prefix.getSchema(), UnaryOperator.identity())
                    .forEachRemaining(table -> result.put(table.name(), table.tableColumns().orElseThrow()));
            return result.buildOrThrow();
        }

        @Override
        public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            verifier.verify(session);
            return listTableColumns(session, prefix).entrySet().stream()
                    .map(entry -> TableColumnsMetadata.forTable(entry.getKey(), entry.getValue()))
                    .iterator();
        }

        @Override
        public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
                ConnectorSession session,
                ConnectorAccessControl accessControl,
                ConnectorTableHandle tableHandle,
                String procedureName,
                Map<String, Object> executeProperties,
                RetryMode retryMode)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableExecuteHandle> getTableHandleForMaterializedViewExecute(
                ConnectorSession session,
                ConnectorAccessControl accessControl,
                ConnectorTableHandle tableHandle,
                String procedureName,
                Map<String, Object> executeProperties,
                RetryMode retryMode)
        {
            throw denyExecution(session);
        }

        @Override
        public Set<ColumnHandle> getColumnHandlesForTableExecute(ConnectorSession connectorSession, ConnectorTableHandle tableHandle, ConnectorTableExecuteHandle connectorTableExecuteHandle)
        {
            throw denyExecution(connectorSession);
        }

        @Override
        public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Map<String, Long> finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
        {
            throw denyExecution(session);
        }

        @Override
        public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
        {
            throw denyExecution(session);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(
                ConnectorSession session,
                ConnectorInsertTableHandle insertHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorInsertTableHandle beginRefreshMaterializedView(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                boolean hasForeignSourceTables,
                RetryMode retryMode,
                RefreshType refreshType)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                ConnectorInsertTableHandle insertHandle,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics,
                List<ConnectorTableHandle> sourceTableHandles,
                boolean hasForeignSourceTables,
                boolean hasSourceTableFunctions,
                boolean hasNonDeterministicFunctions)
        {
            throw denyExecution(session);
        }

        @Override
        public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishMerge(
                ConnectorSession session,
                ConnectorMergeTableHandle mergeTableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishMerge(
                ConnectorSession session,
                ConnectorMergeTableHandle mergeTableHandle,
                List<ConnectorTableHandle> sourceTableHandles,
                Collection<Slice> fragments,
                Collection<ComputedStatistics> computedStatistics,
                MemoryContext memoryContext)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle handle, Map<ColumnHandle, Constant> assignments)
        {
            throw denyExecution(session);
        }

        @Override
        public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
        {
            throw denyExecution(session);
        }

        @Override
        public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
        {
            throw denyExecution(session);
        }

        @Override
        public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
                ConnectorSession session,
                ConnectorTableHandle handle,
                List<AggregateFunction> aggregates,
                Map<String, ColumnHandle> assignments,
                List<List<ColumnHandle>> groupingSets)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
                ConnectorSession session,
                JoinType joinType,
                ConnectorTableHandle left,
                ConnectorTableHandle right,
                ConnectorExpression joinCondition,
                Map<String, ColumnHandle> leftAssignments,
                Map<String, ColumnHandle> rightAssignments,
                JoinStatistics statistics)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
                ConnectorSession session,
                ConnectorTableHandle handle,
                long topNCount,
                List<SortItem> sortItems,
                Map<String, ColumnHandle> assignments)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
        {
            throw denyExecution(session);
        }

        @Override
        public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorWritableTableHandle tableHandle)
        {
            throw denyExecution(session);
        }

        @Override
        public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession session, ConnectorTableFunctionHandle tableFunctionHandle)
        {
            throw denyExecution(session);
        }

        private static List<ColumnMetadata> columns(HoglakeDtos.Table table)
        {
            return table.columns().stream()
                    .map(SharedMetadata::column)
                    .collect(toImmutableList());
        }

        private static ColumnMetadata column(HoglakeDtos.Column column)
        {
            try {
                return ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(HoglakeTypes.toTrinoType(column))
                        .setNullable(column.isNullable())
                        .setComment(Optional.ofNullable(column.comment()))
                        .build();
            }
            catch (RuntimeException _) {
                throw new TrinoException(NOT_SUPPORTED, "Shared Hoglake column metadata is not supported");
            }
        }
    }
}
