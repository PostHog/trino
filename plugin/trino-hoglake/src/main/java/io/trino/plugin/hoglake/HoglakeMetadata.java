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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * Hoglake namespaces are Trino schemas, hoglake
 * tables are Trino tables, columns map through {@link HoglakeTypes}.
 *
 * <p>Query consistency: {@link #getTableHandle} resolves the catalog's
 * head snapshot once and pins it — snapshot id, table_uuid, and the
 * column list all ride the handle, and every later call in the query
 * (getColumnHandles, getTableMetadata, getSplits) serves from or scans
 * at that pin. Listing surfaces (SHOW SCHEMAS/TABLES, DESCRIBE via
 * streamTableColumns) read head; they are not part of any query's
 * consistency contract.
 */
public class HoglakeMetadata
        implements ConnectorMetadata
{
    private final HoglakeClient client;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Map<SchemaTableName, HoglakeDtos.ReplacementTarget> plannedTargets = new ConcurrentHashMap<>();
    private volatile Optional<String> creationOperation = Optional.empty();

    public HoglakeMetadata(HoglakeClient client)
    {
        this(client, null);
    }

    public HoglakeMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory)
    {
        this.client = requireNonNull(client, "client is null");
        this.fileSystemFactory = fileSystemFactory;
    }

    public HoglakeMetadata newTransaction()
    {
        return new HoglakeMetadata(client, fileSystemFactory);
    }

    public void rollback()
    {
        creationOperation.ifPresent(client::abortTableCreation);
        creationOperation = Optional.empty();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listNamespaces().stream()
                .map(HoglakeDtos.Namespace::name)
                .toList();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (!properties.isEmpty() || owner.getType() != PrincipalType.USER || !owner.getName().equals(session.getUser())) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake schema properties and custom owners are not supported");
        }
        checkSchemaEvolutionSupport();
        client.createNamespace(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping Hoglake schemas with CASCADE is not supported");
        }
        checkSchemaEvolutionSupport();
        HoglakeDtos.Namespace namespace = client.getNamespace(schemaName);
        if (namespace.namespaceId() == null) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Hoglake namespace identity is missing");
        }
        client.dropNamespace(schemaName, namespace.namespaceId());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        if (!(position instanceof ColumnPosition.Last) || !column.isNullable() || column.getComment().isPresent() || !column.getProperties().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake ADD COLUMN supports nullable columns at the end, without comments or properties");
        }
        Map<String, Object> params = Map.of();
        if (column.getType() instanceof DecimalType decimal) {
            params = Map.of("precision", decimal.getPrecision(), "scale", decimal.getScale());
        }
        HoglakeDtos.ColumnDefinition definition = new HoglakeDtos.ColumnDefinition(column.getName(), HoglakeTypes.toHoglakeType(column.getType()), params, true);
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "add_column", "column", definition));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "rename_column", "from", ((HoglakeColumnHandle) source).name(), "to", target));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "drop_column", "name", ((HoglakeColumnHandle) column).name()));
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        HoglakeColumnHandle source = (HoglakeColumnHandle) column;
        if (!HoglakeTypes.canPromote(source.type(), type)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported Hoglake column type change: %s to %s".formatted(source.type(), type));
        }
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "promote_column", "name", source.name(), "to", HoglakeTypes.toHoglakeType(type)));
    }

    private void alterColumns(HoglakeTableHandle handle, Map<String, Object> operation)
    {
        checkSchemaEvolutionSupport();
        client.alterColumns(handle.schemaName(), handle.tableName(), handle.tableUuid(), handle.snapshotId(), operation);
    }

    private void checkSchemaEvolutionSupport()
    {
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("guarded-schema-evolution-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support guarded-schema-evolution-v1");
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Table versioning (time travel) not yet supported by the hoglake connector");
        }
        // Pin the query's snapshot: resolve head once, then fetch the
        // table AT that snapshot (not at whatever head is by the time the
        // second request lands) so the handle is consistent-at-N.
        long snapshot = client.getCatalog().headSnapshotId();
        Optional<HoglakeDtos.Table> plannedTable = client.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshot);
        plannedTargets.putIfAbsent(tableName, new HoglakeDtos.ReplacementTarget(plannedTable.map(HoglakeDtos.Table::tableUuid).orElse(null), snapshot));
        return plannedTable
                .map(table -> (ConnectorTableHandle) new HoglakeTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        snapshot,
                        table.tableUuid(),
                        table.columns().stream().map(HoglakeMetadata::toColumnHandle).toList()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        return new ConnectorTableMetadata(
                handle.schemaTableName(),
                handle.columns().stream().map(HoglakeColumnHandle::columnMetadata).toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        if (schemaName.isPresent()) {
            // Nonexistent schema is the user's error, surfaced typed.
            for (HoglakeDtos.TableSummary summary : client.listTables(schemaName.get())) {
                tables.add(new SchemaTableName(schemaName.get(), summary.name()));
            }
            return tables;
        }
        for (String schema : listSchemaNames(session)) {
            try {
                for (HoglakeDtos.TableSummary summary : client.listTables(schema)) {
                    tables.add(new SchemaTableName(schema, summary.name()));
                }
            }
            catch (SchemaNotFoundException e) {
                // Namespace dropped between the listing and this fetch; skip.
            }
        }
        return tables;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (HoglakeColumnHandle column : handle.columns()) {
            handles.put(column.name(), column);
        }
        return handles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((HoglakeColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        List<SchemaTableName> names = prefix.getTable()
                .map(table -> List.of(new SchemaTableName(prefix.getSchema().orElseThrow(), table)))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        List<TableColumnsMetadata> result = new ArrayList<>();
        for (SchemaTableName name : names) {
            client.getTable(name.getSchemaName(), name.getTableName())
                    .ifPresent(table -> result.add(
                            TableColumnsMetadata.forTable(name, columnMetadata(table))));
        }
        return result.iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        TupleDomain<HoglakeColumnHandle> predicate = handle.constraint()
                .intersect(constraint.getSummary().transformKeys(HoglakeColumnHandle.class::cast));
        if (predicate.equals(handle.constraint())) {
            return Optional.empty();
        }
        // Statistics only exclude row groups; the engine must still evaluate every filter.
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(predicate), constraint.getSummary(), constraint.getExpression(), false));
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> properties, Type type)
    {
        if (type instanceof VarcharType) {
            return Optional.of(VarcharType.VARCHAR);
        }
        if (type instanceof TimeType time && time.getPrecision() <= 6) {
            return Optional.of(TimeType.TIME_MICROS);
        }
        if (type instanceof TimestampType timestamp && timestamp.getPrecision() <= 6) {
            return Optional.of(TimestampType.TIMESTAMP_MICROS);
        }
        if (type instanceof TimestampWithTimeZoneType timestamp && timestamp.getPrecision() <= 6) {
            return Optional.of(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS);
        }
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName name = tableMetadata.getTable();
        if (saveMode != SaveMode.REPLACE && client.getTable(name.getSchemaName(), name.getTableName()).isPresent()) {
            if (saveMode == SaveMode.IGNORE) {
                return;
            }
            throw new TrinoException(StandardErrorCode.TABLE_ALREADY_EXISTS, "Table already exists: " + name);
        }
        try {
            ConnectorOutputTableHandle handle = beginCreateTable(session, tableMetadata, Optional.empty(), RetryMode.NO_RETRIES, saveMode == SaveMode.REPLACE);
            finishCreateTable(session, handle, List.of(), List.of());
        }
        catch (TrinoException e) {
            if (saveMode == SaveMode.IGNORE && e.getErrorCode().equals(StandardErrorCode.TABLE_ALREADY_EXISTS.toErrorCode())) {
                return;
            }
            throw e;
        }
    }

    private static List<HoglakeDtos.ColumnDefinition> columnDefinitions(ConnectorTableMetadata metadata)
    {
        if (metadata.getComment().isPresent() || !metadata.getProperties().isEmpty() || metadata.getColumns().stream().anyMatch(column -> column.getComment().isPresent())) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake table properties and comments are not supported");
        }
        return metadata.getColumns().stream()
                .map(column -> {
                    Map<String, Object> params = Map.of();
                    if (column.getType() instanceof DecimalType decimal) {
                        params = Map.of("precision", decimal.getPrecision(), "scale", decimal.getScale());
                    }
                    return new HoglakeDtos.ColumnDefinition(column.getName(), HoglakeTypes.toHoglakeType(column.getType()), params, column.isNullable());
                })
                .toList();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata metadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        checkRetryMode(retryMode);
        if (layout.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Custom layouts are not supported");
        }
        List<HoglakeDtos.ColumnDefinition> definitions = columnDefinitions(metadata);
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("atomic-table-creation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-table-creation-v1");
        }
        HoglakeDtos.ReplacementTarget replacement = null;
        if (replace) {
            if (!catalog.capabilities().contains("atomic-table-replacement-v1")) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-table-replacement-v1");
            }
            replacement = plannedTargets.computeIfAbsent(metadata.getTable(), name -> new HoglakeDtos.ReplacementTarget(
                    client.getTable(name.getSchemaName(), name.getTableName(), catalog.headSnapshotId()).map(HoglakeDtos.Table::tableUuid).orElse(null),
                    catalog.headSnapshotId()));
        }
        String operation = UUID.randomUUID().toString();
        // Record before sending: a lost preparation response may still have created the operation.
        creationOperation = Optional.of(operation);
        HoglakeDtos.TableCreation prepared = client.prepareTableCreation(operation, metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), definitions, replacement);
        if (!"prepared".equals(prepared.state()) || !operation.equals(prepared.operationId()) || prepared.tableUuid() == null || prepared.columns() == null || prepared.writePath() == null) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake preparation response for operation " + operation);
        }
        List<HoglakeColumnHandle> columns = prepared.columns().stream().map(HoglakeMetadata::toColumnHandle).toList();
        return new HoglakeWriteHandle(metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), prepared.tableUuid(), catalog.headSnapshotId(), prepared.writePath(), columns, columns, Optional.of(operation));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkLifecycleSupport();
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        client.dropTable(handle.schemaName(), handle.tableName(), handle.tableUuid());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        if (!handle.schemaName().equals(newTableName.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Moving Hoglake tables between schemas is not supported");
        }
        checkLifecycleSupport();
        client.renameTable(handle.schemaName(), handle.tableName(), newTableName.getTableName(), handle.tableUuid());
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkLifecycleSupport();
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        client.truncateTable(handle.schemaName(), handle.tableName(), handle.tableUuid());
    }

    private void checkLifecycleSupport()
    {
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("guarded-table-lifecycle-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support guarded-table-lifecycle-v1");
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        checkRetryMode(retryMode);
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        HoglakeDtos.Table table = client.getTable(handle.schemaName(), handle.tableName(), handle.snapshotId()).orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
        if (table.partitionSpec() != null && table.partitionSpec().get("fields") instanceof List<?> fields && !fields.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Writing partitioned Hoglake tables is not supported");
        }
        handle.columns().forEach(column -> HoglakeTypes.toHoglakeType(column.type()));
        List<HoglakeColumnHandle> inputs = columns.stream().map(HoglakeColumnHandle.class::cast).toList();
        for (HoglakeColumnHandle column : handle.columns()) {
            if (!column.nullable() && !inputs.contains(column)) {
                throw new TrinoException(StandardErrorCode.CONSTRAINT_VIOLATION, "Missing required column: " + column.name());
            }
        }
        HoglakeDtos.Catalog catalog = client.getCatalog();
        Optional<String> operation = Optional.empty();
        if (catalog.capabilities() != null && catalog.capabilities().contains("idempotent-append-v1")) {
            operation = Optional.of(UUID.randomUUID().toString());
        }
        return new HoglakeWriteHandle(handle.schemaName(), handle.tableName(), handle.tableUuid(), handle.snapshotId(), catalog.dataPath(), handle.columns(), inputs, Optional.empty(), operation);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle handle, Collection<Slice> fragments, Collection<ComputedStatistics> statistics)
    {
        HoglakeWriteHandle writeHandle = (HoglakeWriteHandle) handle;
        String operation = writeHandle.creationOperation().orElseThrow();
        List<HoglakeDtos.FileRegistration> files = decodeFragments(fragments);
        HoglakeDtos.TableCreation result;
        try {
            result = client.publishTableCreation(operation, files);
        }
        catch (TrinoException failure) {
            if (isDefiniteRejection(failure)) {
                throw failure;
            }
            // Status uses the same catalog lock as publication. Never infer failure from
            // absence of the requested table, which could have been renamed or dropped.
            try {
                result = client.getTableCreation(operation);
                if ("prepared".equals(result.state())) {
                    result = client.publishTableCreation(operation, files);
                }
            }
            catch (RuntimeException recoveryFailure) {
                failure.addSuppressed(recoveryFailure);
                throw new TrinoException(HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE, "Hoglake publication outcome is unknown; inspect operation " + operation, failure);
            }
        }
        if (!operation.equals(result.operationId()) || !writeHandle.tableUuid().equals(result.tableUuid())) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Mismatched Hoglake creation receipt for operation " + operation);
        }
        if ("committed".equals(result.state()) && result.snapshotId() != null && result.snapshotId() > 0) {
            creationOperation = Optional.empty();
            return Optional.empty();
        }
        if ("rejected".equals(result.state()) && "target_exists".equals(result.reason())) {
            throw new TrinoException(StandardErrorCode.TABLE_ALREADY_EXISTS, "Table already exists: " + writeHandle.table());
        }
        if ("rejected".equals(result.state()) || "aborted".equals(result.state())) {
            throw new TrinoException(StandardErrorCode.TRANSACTION_CONFLICT, "Hoglake table creation " + result.state() + ": " + result.reason());
        }
        throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake publication receipt; inspect operation " + operation);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle handle, List<ConnectorTableHandle> sources, Collection<Slice> fragments, Collection<ComputedStatistics> statistics)
    {
        finishWrite((HoglakeWriteHandle) handle, fragments);
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle handle)
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle handle)
    {
        return HoglakeColumnHandle.ROW_ID;
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        checkRetryMode(retryMode);
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        HoglakeDtos.Table table = client.getTable(handle.schemaName(), handle.tableName(), handle.snapshotId()).orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
        Optional<String> insertFailure = Optional.empty();
        if (table.partitionSpec() != null && table.partitionSpec().get("fields") instanceof List<?> fields && !fields.isEmpty()) {
            insertFailure = Optional.of("Writing partitioned Hoglake tables is not supported");
        }
        if (table.sortSpec() != null && table.sortSpec().get("fields") instanceof List<?> fields && !fields.isEmpty()) {
            insertFailure = Optional.of("Writing sorted Hoglake tables is not supported");
        }
        // INSERT-only MERGE has no update cases, so also enforce this in the worker sink.
        if (!updateCaseColumns.isEmpty() && insertFailure.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, insertFailure.orElseThrow());
        }
        handle.columns().forEach(column -> HoglakeTypes.toHoglakeType(column.type()));
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("idempotent-mutation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support idempotent-mutation-v1 required for DELETE, UPDATE and MERGE");
        }
        return new HoglakeDeleteHandle(handle, catalog.dataPath(), UUID.randomUUID().toString(), insertFailure);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle handle,
            List<ConnectorTableHandle> sources,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> statistics)
    {
        return finishMerge(session, handle, sources, fragments, statistics, MemoryContext.NO_LIMIT);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle handle,
            List<ConnectorTableHandle> sources,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> statistics,
            MemoryContext memoryContext)
    {
        HoglakeDeleteHandle delete = (HoglakeDeleteHandle) handle;
        new HoglakeDeletePublisher(client, fileSystemFactory.create(session)).publish(delete, fragments, memoryContext);
        return Optional.empty();
    }

    private void finishWrite(HoglakeWriteHandle handle, Collection<Slice> fragments)
    {
        List<HoglakeDtos.FileRegistration> files = decodeFragments(fragments).stream()
                .sorted(comparing(HoglakeDtos.FileRegistration::path)
                        .thenComparingLong(HoglakeDtos.FileRegistration::recordCount)
                        .thenComparingLong(HoglakeDtos.FileRegistration::fileSizeBytes)
                        .thenComparingLong(HoglakeDtos.FileRegistration::footerSize))
                .toList();
        if (!files.isEmpty()) {
            client.commit(new HoglakeDtos.Commit(handle.snapshot(), List.of(new HoglakeDtos.Append(handle.namespace(), handle.table(), handle.tableUuid(), files)), handle.insertOperation().orElse(null)));
        }
    }

    private static List<HoglakeDtos.FileRegistration> decodeFragments(Collection<Slice> fragments)
    {
        List<HoglakeDtos.FileRegistration> files = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (Slice fragment : fragments) {
            try {
                files.add(mapper.readValue(fragment.getBytes(), HoglakeDtos.FileRegistration.class));
            }
            catch (IOException e) {
                throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake writer fragment", e);
            }
        }
        return files;
    }

    private static boolean isDefiniteRejection(TrinoException failure)
    {
        return failure.getErrorCode().equals(StandardErrorCode.TRANSACTION_CONFLICT.toErrorCode()) ||
                failure.getErrorCode().equals(StandardErrorCode.INVALID_ARGUMENTS.toErrorCode()) ||
                failure.getErrorCode().equals(HoglakeErrorCode.HOGLAKE_SNAPSHOT_EXPIRED.toErrorCode()) ||
                failure.getErrorCode().equals(HoglakeErrorCode.HOGLAKE_CATALOG_NOT_FOUND.toErrorCode());
    }

    private static void checkRetryMode(RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake writes do not support query retries");
        }
    }

    // ---- helpers -----------------------------------------------------------

    static HoglakeColumnHandle toColumnHandle(HoglakeDtos.Column column)
    {
        return new HoglakeColumnHandle(
                column.name(),
                column.fieldId(),
                HoglakeTypes.toTrinoType(column.type(), column.typeParams()),
                column.isNullable());
    }

    private static List<ColumnMetadata> columnMetadata(HoglakeDtos.Table table)
    {
        return table.columns().stream()
                .map(column -> toColumnHandle(column).columnMetadata())
                .toList();
    }
}
