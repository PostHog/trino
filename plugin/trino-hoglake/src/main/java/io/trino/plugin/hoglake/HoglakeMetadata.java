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
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
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
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
    private volatile Optional<String> creationOperation = Optional.empty();

    public HoglakeMetadata(HoglakeClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    public HoglakeMetadata newTransaction()
    {
        return new HoglakeMetadata(client);
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
        return client.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshot)
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
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "Replacing Hoglake tables is not supported");
        }
        SchemaTableName name = tableMetadata.getTable();
        if (client.getTable(name.getSchemaName(), name.getTableName()).isPresent()) {
            if (saveMode == SaveMode.IGNORE) {
                return;
            }
            throw new TrinoException(StandardErrorCode.TABLE_ALREADY_EXISTS, "Table already exists: " + name);
        }
        try {
            ConnectorOutputTableHandle handle = beginCreateTable(session, tableMetadata, Optional.empty(), RetryMode.NO_RETRIES, false);
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
        if (replace || layout.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Replacing tables and custom layouts are not supported");
        }
        List<HoglakeDtos.ColumnDefinition> definitions = columnDefinitions(metadata);
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("atomic-table-creation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-table-creation-v1");
        }
        String operation = UUID.randomUUID().toString();
        // Record before sending: a lost preparation response may still have created the operation.
        creationOperation = Optional.of(operation);
        HoglakeDtos.TableCreation prepared = client.prepareTableCreation(operation, metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), definitions);
        if (!"prepared".equals(prepared.state()) || !operation.equals(prepared.operationId()) || prepared.tableUuid() == null || prepared.columns() == null || prepared.writePath() == null) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake preparation response for operation " + operation);
        }
        List<HoglakeColumnHandle> columns = prepared.columns().stream().map(HoglakeMetadata::toColumnHandle).toList();
        return new HoglakeWriteHandle(metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), prepared.tableUuid(), catalog.headSnapshotId(), prepared.writePath(), columns, columns, Optional.of(operation));
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
        return new HoglakeWriteHandle(handle.schemaName(), handle.tableName(), handle.tableUuid(), handle.snapshotId(), client.getCatalog().dataPath(), handle.columns(), inputs, Optional.empty());
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

    private void finishWrite(HoglakeWriteHandle handle, Collection<Slice> fragments)
    {
        List<HoglakeDtos.FileRegistration> files = decodeFragments(fragments);
        if (!files.isEmpty()) {
            client.commit(new HoglakeDtos.Commit(handle.snapshot(), List.of(new HoglakeDtos.Append(handle.namespace(), handle.table(), handle.tableUuid(), files))));
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
