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
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_LOGIN_TOKEN_REFUSED;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
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
    private static final Logger log = Logger.get(HoglakeMetadata.class);

    private final HoglakeClient client;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Optional<HoglakePosthogSchema> posthogSchema;
    private final Map<SchemaTableName, HoglakeDtos.ReplacementTarget> plannedTargets = new ConcurrentHashMap<>();
    private volatile Optional<String> creationOperation = Optional.empty();
    private HoglakeTransactionState transaction;
    // The mount's read snapshot, pinned once per explicit transaction
    private Long mountSnapshot;

    public HoglakeMetadata(HoglakeClient client)
    {
        this(client, null);
    }

    public HoglakeMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory)
    {
        this(client, fileSystemFactory, Optional.empty());
    }

    public HoglakeMetadata(HoglakeClient client, TrinoFileSystemFactory fileSystemFactory, Optional<HoglakePosthogSchema> posthogSchema)
    {
        this.client = requireNonNull(client, "client is null");
        this.fileSystemFactory = fileSystemFactory;
        this.posthogSchema = requireNonNull(posthogSchema, "posthogSchema is null");
    }

    public HoglakeMetadata newTransaction()
    {
        return new HoglakeMetadata(client, fileSystemFactory, posthogSchema);
    }

    public HoglakeMetadata newTransaction(boolean autoCommit)
    {
        HoglakeMetadata metadata = newTransaction();
        if (!autoCommit) {
            metadata.transaction = new HoglakeTransactionState(client);
        }
        return metadata;
    }

    public void commit()
    {
        if (transaction != null) {
            transaction.commit();
        }
    }

    private void checkAutocommitDdl()
    {
        if (transaction != null) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake DDL is not supported in explicit transactions; use an autocommit statement");
        }
    }

    public void rollback()
    {
        if (transaction != null) {
            transaction.rollback();
        }
        creationOperation.ifPresent(client::abortTableCreation);
        creationOperation = Optional.empty();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> names = orgSchemaNames();
        if (posthogSchema.isEmpty() || !posthogSchemaListed(session)) {
            return names;
        }
        return ImmutableList.<String>builder().addAll(names).add(HoglakePosthogSchema.SCHEMA_NAME).build();
    }

    /**
     * The org catalog's namespaces, without any spelling of the posthog schema's name
     * when that schema is configured: the mount always takes the name.
     */
    private List<String> orgSchemaNames()
    {
        return client.listNamespaces().stream()
                .map(HoglakeDtos.Namespace::name)
                .filter(name -> !isPosthogSchema(name))
                .toList();
    }

    /**
     * Whether listings show the posthog schema to this session's login. It is not
     * shown when the server has no mount for this login (404), when the login may
     * not read the schema, or when the token service does not issue per-login
     * tokens to this catalog: those are steady states. It stays shown when the
     * mount or the token service cannot be asked, so an outage does not make the
     * schema come and go.
     */
    private boolean posthogSchemaListed(ConnectorSession session)
    {
        try {
            return posthogClient(session).findCatalog().isPresent();
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(PERMISSION_DENIED.toErrorCode()) || e.getErrorCode().equals(HOGLAKE_LOGIN_TOKEN_REFUSED.toErrorCode())) {
                return false;
            }
            log.debug("Listing the posthog schema of Hoglake catalog '%s' without asking its mount: %s", client.catalogName(), e.getErrorCode().getName());
            return true;
        }
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        if (posthogSchema.isEmpty()) {
            return ConnectorMetadata.super.schemaExists(session, schemaName);
        }
        if (!schemaName.equals(schemaName.toLowerCase(ENGLISH))) {
            // As the engine's default: Trino schemas are lowercase
            return false;
        }
        if (isPosthogSchema(schemaName)) {
            // A failure other than 404 is reported, not taken as absence.
            return posthogClient(session).findCatalog().isPresent();
        }
        return orgSchemaNames().contains(schemaName);
    }

    private boolean isPosthogSchema(String schemaName)
    {
        return posthogSchema.isPresent() && HoglakePosthogSchema.isPosthogSchema(schemaName);
    }

    /**
     * The mount's client for this session's login. Never kept: each call makes its
     * own from the session it was given.
     */
    private HoglakeClient posthogClient(ConnectorSession session)
    {
        return posthogSchema.orElseThrow().client(session);
    }

    /**
     * Refuses a write to the posthog schema before anything is checked, uploaded or
     * sent. Only PostHog adds data to it, on the server side.
     */
    private void checkWritable(String schemaName)
    {
        if (isPosthogSchema(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "The posthog schema is read-only");
        }
    }

    private void checkWritable(ConnectorTableHandle tableHandle)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        if (handle.mount().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "The posthog schema is read-only");
        }
        checkWritable(handle.schemaName());
    }

    /**
     * The client that serves a handle: the mount's for a posthog schema table, which
     * must name this catalog's mount, and the org catalog's otherwise. The mount's
     * client is the one of the calling session's login, whoever resolved the handle.
     */
    HoglakeClient clientFor(ConnectorSession session, HoglakeTableHandle handle)
    {
        return clientFor(client, posthogSchema, session, handle);
    }

    static HoglakeClient clientFor(HoglakeClient client, Optional<HoglakePosthogSchema> posthogSchema, ConnectorSession session, HoglakeTableHandle handle)
    {
        if (handle.mount().isEmpty()) {
            return client;
        }
        HoglakePosthogSchema schema = posthogSchema.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "The posthog schema is not configured for this catalog"));
        if (!schema.mountCatalog().equals(handle.mount().get().catalog())) {
            throw new TrinoException(NOT_SUPPORTED, "The table handle names another catalog's posthog schema");
        }
        return schema.client(session);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkWritable(schemaName);
        if (!properties.isEmpty() || owner.getType() != PrincipalType.USER || !owner.getPrincipalName().equals(session.getUser())) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake schema properties and custom owners are not supported");
        }
        checkSchemaEvolutionSupport();
        client.createNamespace(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        checkWritable(schemaName);
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping Hoglake schemas with CASCADE is not supported");
        }
        checkSchemaEvolutionSupport();
        HoglakeDtos.Namespace namespace = client.getNamespace(schemaName);
        if (namespace.namespaceId() == null) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Hoglake namespace identity is missing");
        }
        client.dropNamespace(schemaName, namespace.namespaceId(), operationMetadata(session, "DROP SCHEMA"));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        checkWritable(tableHandle);
        if (!(position instanceof ColumnPosition.Last) || !column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake ADD COLUMN supports nullable columns at the end");
        }
        HoglakeDtos.ColumnDefinition definition = columnDefinition(column);
        checkWriteSchemaSupport(client.getCatalog(), List.of(column.getType()));
        if (column.getComment().isPresent()) {
            checkMetadataSupport();
        }
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", column.getComment().isPresent() ? "add_column_with_metadata" : "add_column", "column", definition));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        checkWritable(tableHandle);
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "rename_column", "from", ((HoglakeColumnHandle) source).name(), "to", target));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        checkWritable(tableHandle);
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "drop_column", "name", ((HoglakeColumnHandle) column).name()));
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        checkWritable(tableHandle);
        HoglakeColumnHandle source = (HoglakeColumnHandle) column;
        if (!source.hoglakeType().equals(HoglakeTypes.toHoglakeType(source.type())) || !HoglakeTypes.canPromote(source.type(), type)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported Hoglake column type change: %s to %s".formatted(source.type(), type));
        }
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "promote_column", "name", source.name(), "to", HoglakeTypes.toHoglakeType(type)));
    }

    private void checkMetadataSupport()
    {
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("versioned-table-metadata-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support versioned-table-metadata-v1");
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        checkWritable(tableHandle);
        checkMetadataSupport();
        Map<String, Object> operation = new HashMap<>();
        operation.put("op", "set_table_comment");
        operation.put("comment", comment.orElse(null));
        alterColumns((HoglakeTableHandle) tableHandle, operation);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        checkWritable(tableHandle);
        checkMetadataSupport();
        Map<String, Object> operation = new HashMap<>();
        operation.put("op", "set_column_comment");
        operation.put("name", ((HoglakeColumnHandle) column).name());
        operation.put("comment", comment.orElse(null));
        alterColumns((HoglakeTableHandle) tableHandle, operation);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        checkWritable(tableHandle);
        if (!properties.keySet().equals(Set.of("extra_properties"))) {
            throw new TrinoException(NOT_SUPPORTED, "Only extra_properties can be altered; partitioning and sorted_by are creation properties");
        }
        checkMetadataSupport();
        alterColumns((HoglakeTableHandle) tableHandle, Map.of("op", "set_properties", "properties", properties.get("extra_properties").orElse(Map.of())));
    }

    private void alterColumns(HoglakeTableHandle handle, Map<String, Object> operation)
    {
        checkSchemaEvolutionSupport();
        client.alterColumns(handle.schemaName(), handle.tableName(), handle.tableUuid(), handle.snapshotId(), operation);
    }

    private void checkSchemaEvolutionSupport()
    {
        checkAutocommitDdl();
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
        if (isPosthogSchema(tableName.getSchemaName())) {
            return posthogTableHandle(session, tableName.getTableName());
        }
        // Pin the query's snapshot: resolve head once, then fetch the
        // table AT that snapshot (not at whatever head is by the time the
        // second request lands) so the handle is consistent-at-N.
        long snapshot = transaction == null ? client.getCatalog().headSnapshotId() : transaction.snapshot();
        Optional<HoglakeDtos.Table> plannedTable = client.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshot);
        plannedTargets.putIfAbsent(tableName, new HoglakeDtos.ReplacementTarget(plannedTable.map(HoglakeDtos.Table::tableUuid).orElse(null), snapshot));
        return plannedTable
                .map(table -> new HoglakeTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        snapshot,
                        table.tableUuid(),
                        table.columns().stream().map(HoglakeMetadata::toColumnHandle).toList()))
                .map(handle -> (ConnectorTableHandle) (transaction == null ? handle : transaction.overlay(handle)))
                .orElse(null);
    }

    /**
     * A table of the posthog schema, pinned at the mount's head snapshot (once per
     * explicit transaction), found in whichever of the mount's namespaces holds it.
     * The mount lists one namespace today; a name in two of them is refused rather
     * than guessed. A staged-write overlay never applies: the schema is never written.
     */
    private HoglakeTableHandle posthogTableHandle(ConnectorSession session, String tableName)
    {
        // The connector exposes no system tables, and their names are outside the
        // server's identifier grammar
        if (tableName.contains("$")) {
            return null;
        }
        HoglakeClient mount = posthogClient(session);
        Optional<HoglakeDtos.Catalog> catalog = mount.findCatalog();
        if (catalog.isEmpty()) {
            return null;
        }
        long snapshot = mountSnapshot(catalog.get());
        List<HoglakeTableHandle> found = new ArrayList<>();
        for (HoglakeDtos.Namespace namespace : mount.listNamespaces()) {
            mount.getTable(namespace.name(), tableName, snapshot).ifPresent(table -> found.add(new HoglakeTableHandle(
                    HoglakePosthogSchema.SCHEMA_NAME,
                    tableName,
                    snapshot,
                    table.tableUuid(),
                    table.columns().stream().map(HoglakeMetadata::toColumnHandle).toList())
                    .withMount(new HoglakeTableHandle.MountTable(mount.catalogName(), namespace.name()))));
        }
        if (found.size() > 1) {
            throw new TrinoException(NOT_SUPPORTED, "Table '%s' is ambiguous in the posthog schema".formatted(tableName));
        }
        return found.isEmpty() ? null : found.getFirst();
    }

    private synchronized long mountSnapshot(HoglakeDtos.Catalog catalog)
    {
        if (transaction == null) {
            return catalog.headSnapshotId();
        }
        if (mountSnapshot == null) {
            mountSnapshot = catalog.headSnapshotId();
        }
        return mountSnapshot;
    }

    /**
     * The posthog schema's tables: those of every namespace the mount lists, each name
     * once.
     */
    private List<SchemaTableName> posthogTables(ConnectorSession session)
    {
        HoglakeClient mount = posthogClient(session);
        Set<String> names = new LinkedHashSet<>();
        for (HoglakeDtos.Namespace namespace : mount.listNamespaces()) {
            try {
                mount.listTables(namespace.name()).forEach(summary -> names.add(summary.name()));
            }
            catch (SchemaNotFoundException e) {
                // Namespace gone between the listing and this fetch; skip.
            }
        }
        return names.stream()
                .map(name -> new SchemaTableName(HoglakePosthogSchema.SCHEMA_NAME, name))
                .toList();
    }

    /**
     * A posthog schema table at the mount's head, for listing surfaces; empty when no
     * namespace, or more than one, holds it.
     */
    private Optional<HoglakeDtos.Table> posthogTableAtHead(ConnectorSession session, String tableName)
    {
        HoglakeClient mount = posthogClient(session);
        List<HoglakeDtos.Table> found = new ArrayList<>();
        for (HoglakeDtos.Namespace namespace : mount.listNamespaces()) {
            mount.getTable(namespace.name(), tableName).ifPresent(found::add);
        }
        return found.size() == 1 ? Optional.of(found.getFirst()) : Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        HoglakeDtos.Table definition = clientFor(session, handle).getTable(handle.namespace(), handle.tableName(), handle.snapshotId())
                .orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
        return new ConnectorTableMetadata(
                handle.schemaTableName(),
                handle.columns().stream().map(HoglakeColumnHandle::columnMetadata).toList(),
                Map.of(
                        "partitioning", HoglakePartitioning.expressions(HoglakePartitioning.read(definition.partitionSpec()), handle.columns()),
                        "sorted_by", HoglakeSorting.expressions(HoglakeSorting.read(definition.sortSpec()), handle.columns()),
                        "extra_properties", definition.properties()),
                Optional.ofNullable(definition.comment()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        if (schemaName.isPresent() && isPosthogSchema(schemaName.get())) {
            // Failures are the user's to see, typed: the schema was named.
            return posthogTables(session);
        }
        if (schemaName.isPresent()) {
            // Nonexistent schema is the user's error, surfaced typed.
            for (HoglakeDtos.TableSummary summary : client.listTables(schemaName.get())) {
                tables.add(new SchemaTableName(schemaName.get(), summary.name()));
            }
            return tables;
        }
        if (posthogSchema.isPresent()) {
            tables.addAll(posthogTablesForListing(session));
        }
        for (String schema : posthogSchema.isPresent() ? orgSchemaNames() : listSchemaNames(session)) {
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

    /**
     * The posthog schema's tables for a listing of the whole catalog, which must not
     * depend on the mount: none when the mount is missing or cannot be asked, or when
     * this login gets no token for it.
     */
    private List<SchemaTableName> posthogTablesForListing(ConnectorSession session)
    {
        try {
            return posthogTables(session);
        }
        catch (SchemaNotFoundException e) {
            return List.of();
        }
        catch (TrinoException e) {
            log.debug("Listing Hoglake catalog '%s' without the posthog schema's tables: %s", client.catalogName(), e.getErrorCode().getName());
            return List.of();
        }
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
            Optional<HoglakeDtos.Table> table;
            if (isPosthogSchema(name.getSchemaName())) {
                table = posthogTableForListing(session, name.getTableName(), prefix.getSchema().isEmpty());
            }
            else {
                table = client.getTable(name.getSchemaName(), name.getTableName());
            }
            table.ifPresent(definition -> result.add(TableColumnsMetadata.forTable(name, columnMetadata(definition))));
        }
        return result.iterator();
    }

    private Optional<HoglakeDtos.Table> posthogTableForListing(ConnectorSession session, String tableName, boolean wholeCatalog)
    {
        try {
            return posthogTableAtHead(session, tableName);
        }
        catch (SchemaNotFoundException e) {
            return Optional.empty();
        }
        catch (TrinoException e) {
            if (!wholeCatalog) {
                throw e;
            }
            log.debug("Listing Hoglake catalog '%s' without the posthog schema's columns: %s", client.catalogName(), e.getErrorCode().getName());
            return Optional.empty();
        }
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

    /**
     * A scan that projects no column, such as {@code SELECT count(*) FROM t},
     * becomes count-only: its rows are counted from the catalog without
     * reading any file, so split planning gives it one whole-file split per
     * file instead of byte ranges. The engine's column pruning offers the
     * empty projection once the scan's outputs are unreferenced. Subscripts on
     * VARIANT columns are pushed down by {@link HoglakeVariantProjections}; any
     * other projection is left to the engine.
     */
    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        if (!projections.isEmpty()) {
            return HoglakeVariantProjections.applyProjection(handle, projections, assignments);
        }
        if (handle.countOnly()) {
            return Optional.empty();
        }
        return Optional.of(new ProjectionApplicationResult<>(handle.withCountOnly(), List.of(), List.of(), false));
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> properties, Type type)
    {
        if (type instanceof ArrayType array) {
            return Optional.of(new ArrayType(getSupportedType(session, properties, array.getElementType()).orElse(array.getElementType())));
        }
        if (type instanceof MapType map) {
            return Optional.of(new MapType(
                    getSupportedType(session, properties, map.getKeyType()).orElse(map.getKeyType()),
                    getSupportedType(session, properties, map.getValueType()).orElse(map.getValueType()),
                    new TypeOperators()));
        }
        if (type instanceof RowType row) {
            return Optional.of(RowType.from(row.getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), getSupportedType(session, properties, field.getType()).orElse(field.getType())))
                    .toList()));
        }
        if (type instanceof VarcharType) {
            return Optional.of(VarcharType.VARCHAR);
        }
        if (type instanceof TimeType time && time.getPrecision() <= 6) {
            return Optional.of(TimeType.TIME_MICROS);
        }
        if (type instanceof TimestampType timestamp && timestamp.getPrecision() <= 9) {
            return Optional.of(timestamp.getPrecision() <= 6 ? TimestampType.TIMESTAMP_MICROS : TimestampType.TIMESTAMP_NANOS);
        }
        if (type instanceof TimestampWithTimeZoneType timestamp && timestamp.getPrecision() <= 6) {
            return Optional.of(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS);
        }
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        checkWritable(tableMetadata.getTable().getSchemaName());
        checkAutocommitDdl();
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
        if (metadata.getProperties().keySet().stream().anyMatch(key -> !Set.of("partitioning", "sorted_by", "extra_properties").contains(key)) ||
                metadata.getColumns().stream().anyMatch(column -> !Set.of(HoglakeVariantShredding.PROPERTY).containsAll(column.getProperties().keySet()))) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported Hoglake table or column properties");
        }
        return metadata.getColumns().stream()
                .map(HoglakeMetadata::columnDefinition)
                .toList();
    }

    /**
     * The catalog definition of a new column, with the layout its {@code shredding}
     * property declares.
     */
    private static HoglakeDtos.ColumnDefinition columnDefinition(ColumnMetadata column)
    {
        HoglakeDtos.ColumnDefinition definition = HoglakeTypes.columnDefinition(column.getName(), column.getType(), column.isNullable())
                .withComment(column.getComment().orElse(null));
        return HoglakeVariantShredding.typeParams(column)
                .map(definition::withTypeParams)
                .orElse(definition);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata metadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        checkWritable(metadata.getTable().getSchemaName());
        checkAutocommitDdl();
        if (layout.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Custom layouts are not supported");
        }
        List<HoglakeDtos.ColumnDefinition> definitions = columnDefinitions(metadata);
        HoglakeDtos.Catalog catalog = client.getCatalog();
        checkRetryMode(retryMode, catalog);
        checkWriteSchemaSupport(catalog, metadata.getColumns().stream().map(ColumnMetadata::getType).toList());
        if (catalog.capabilities() == null || !catalog.capabilities().contains("atomic-table-creation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-table-creation-v1");
        }
        HoglakeDtos.ReplacementTarget replacement = null;
        HoglakeDtos.OperationMetadata operationMetadata = null;
        if (replace) {
            if (!catalog.capabilities().contains("atomic-table-replacement-v1")) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-table-replacement-v1");
            }
            replacement = plannedTargets.computeIfAbsent(metadata.getTable(), name -> new HoglakeDtos.ReplacementTarget(
                    client.getTable(name.getSchemaName(), name.getTableName(), catalog.headSnapshotId()).map(HoglakeDtos.Table::tableUuid).orElse(null),
                    catalog.headSnapshotId()));
            // The server requires it to replace an existing table, and records it only then
            if (replacement.expectedTableUuid() != null) {
                operationMetadata = operationMetadata(session, "CREATE OR REPLACE TABLE");
            }
        }
        @SuppressWarnings("unchecked")
        List<String> partitioning = (List<String>) metadata.getProperties().getOrDefault("partitioning", List.of());
        List<HoglakeDtos.PartitionField> partitionFields = HoglakePartitioning.parse(partitioning, HoglakePartitioning.initialColumns(definitions));
        if (!partitionFields.isEmpty() && !catalog.capabilities().contains("atomic-partitioned-table-creation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-partitioned-table-creation-v1");
        }
        @SuppressWarnings("unchecked")
        List<String> sorting = (List<String>) metadata.getProperties().getOrDefault("sorted_by", List.of());
        List<HoglakeDtos.SortField> sortFields = HoglakeSorting.parse(sorting, HoglakePartitioning.initialColumns(definitions));
        if (!sortFields.isEmpty() && !catalog.capabilities().contains("atomic-sorted-table-creation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support atomic-sorted-table-creation-v1");
        }
        @SuppressWarnings("unchecked")
        Map<String, String> extraProperties = (Map<String, String>) metadata.getProperties().getOrDefault("extra_properties", Map.of());
        if (metadata.getComment().isPresent() || !extraProperties.isEmpty() || definitions.stream().anyMatch(HoglakeDtos.ColumnDefinition::hasComments)) {
            checkMetadataSupport();
        }
        String operation = UUID.randomUUID().toString();
        // Record before sending: a lost preparation response may still have created the operation.
        creationOperation = Optional.of(operation);
        HoglakeDtos.TableCreation prepared = client.prepareTableCreation(operation, metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), definitions, replacement, partitionFields, sortFields, metadata.getComment().orElse(null), extraProperties, operationMetadata);
        if (!"prepared".equals(prepared.state()) || !operation.equals(prepared.operationId()) || prepared.tableUuid() == null || prepared.columns() == null || prepared.writePath() == null) {
            throw new TrinoException(HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE, "Invalid Hoglake preparation response for operation " + operation);
        }
        List<HoglakeColumnHandle> columns = prepared.columns().stream().map(HoglakeMetadata::toColumnHandle).toList();
        return new HoglakeWriteHandle(metadata.getTable().getSchemaName(), metadata.getTable().getTableName(), prepared.tableUuid(), catalog.headSnapshotId(), prepared.writePath(), columns, columns, Optional.of(operation), Optional.empty(), partitionFields, sortFields, catalog.capabilities().contains("claimed-uploads-v1"));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkWritable(tableHandle);
        checkLifecycleSupport();
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        client.dropTable(handle.schemaName(), handle.tableName(), handle.tableUuid(), operationMetadata(session, "DROP TABLE"));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkWritable(tableHandle);
        checkWritable(newTableName.getSchemaName());
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
        checkWritable(tableHandle);
        checkLifecycleSupport();
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        client.truncateTable(handle.schemaName(), handle.tableName(), handle.tableUuid(), operationMetadata(session, "TRUNCATE TABLE"));
    }

    /**
     * The server records who dropped a schema, or dropped, truncated or replaced a table,
     * and why, and refuses those operations without both. The author is the session user,
     * and the message names the statement and the query, but not the query text.
     */
    private static HoglakeDtos.OperationMetadata operationMetadata(ConnectorSession session, String statement)
    {
        if (session.getUser().isBlank()) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake requires a nonblank user for " + statement);
        }
        return new HoglakeDtos.OperationMetadata(session.getUser(), "Trino %s, query %s".formatted(statement, session.getQueryId()));
    }

    private void checkLifecycleSupport()
    {
        checkAutocommitDdl();
        HoglakeDtos.Catalog catalog = client.getCatalog();
        if (catalog.capabilities() == null || !catalog.capabilities().contains("guarded-table-lifecycle-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support guarded-table-lifecycle-v1");
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        checkWritable(tableHandle);
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        HoglakeDtos.Table table = client.getTable(handle.schemaName(), handle.tableName(), handle.snapshotId()).orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
        List<HoglakeDtos.PartitionField> partitionFields = HoglakePartitioning.read(table.partitionSpec());
        HoglakePartitioning.validate(partitionFields, handle.columns());
        List<HoglakeDtos.SortField> sortFields = HoglakeSorting.read(table.sortSpec());
        HoglakeSorting.validate(sortFields, handle.columns());
        handle.columns().forEach(column -> HoglakeTypes.toHoglakeType(column.type()));
        HoglakeVariantShredding.checkWritable(handle.columns());
        List<HoglakeColumnHandle> inputs = columns.stream().map(HoglakeColumnHandle.class::cast).toList();
        for (HoglakeColumnHandle column : handle.columns()) {
            if (!column.nullable() && !inputs.contains(column)) {
                throw new TrinoException(StandardErrorCode.CONSTRAINT_VIOLATION, "Missing required column: " + column.name());
            }
        }
        HoglakeDtos.Catalog catalog = client.getCatalog();
        checkRetryMode(retryMode, catalog);
        if (transaction != null) {
            transaction.checkWriteSupport(catalog);
        }
        checkWriteSchemaSupport(catalog, handle.columns().stream().map(HoglakeColumnHandle::type).toList());
        Optional<String> operation = Optional.empty();
        if (catalog.capabilities() != null && catalog.capabilities().contains("idempotent-append-v1")) {
            operation = Optional.of(transaction == null ? UUID.randomUUID().toString() : transaction.operation());
        }
        return new HoglakeWriteHandle(handle.schemaName(), handle.tableName(), handle.tableUuid(), handle.snapshotId(), tableWritePath(catalog.dataPath(), handle.schemaName(), handle.tableName()), handle.columns(), inputs, Optional.empty(), operation, partitionFields, sortFields, catalog.capabilities() != null && catalog.capabilities().contains("claimed-uploads-v1"));
    }

    /**
     * {@code add_files} needs the permission to insert into the table, and to delete from it
     * when it replaces data, as DuckLake's does.
     */
    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> properties,
            RetryMode retryMode)
    {
        if (!procedureName.equalsIgnoreCase(HoglakeAddFilesProcedure.NAME)) {
            return Optional.empty();
        }
        if (transaction != null) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake add_files is not supported in explicit transactions; use an autocommit statement");
        }
        HoglakeTableHandle table = (HoglakeTableHandle) tableHandle;
        accessControl.checkCanInsertIntoTable(null, table.schemaTableName(), Optional.empty());
        if (HoglakeAddFiles.replaces(properties)) {
            accessControl.checkCanDeleteFromTable(null, table.schemaTableName(), Optional.empty());
        }
        return Optional.of(HoglakeAddFiles.plan(client, table, properties));
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        if (!(tableExecuteHandle instanceof HoglakeAddFilesHandle handle)) {
            throw new IllegalArgumentException("Unknown table procedure handle: " + tableExecuteHandle);
        }
        if (fileSystemFactory == null) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake add_files requires a filesystem");
        }
        return HoglakeAddFiles.execute(session, client, fileSystemFactory.create(session), handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle handle, Collection<Slice> fragments, Collection<ComputedStatistics> statistics)
    {
        HoglakeWriteHandle writeHandle = (HoglakeWriteHandle) handle;
        // Uploads happened already; the barrier against them is in beginCreateTable
        checkWritable(writeHandle.namespace());
        String operation = writeHandle.creationOperation().orElseThrow();
        List<HoglakeDtos.FileRegistration> files = decodeFragments(fragments);
        HoglakeDtos.TableCreation result;
        try {
            result = client.publishTableCreation(operation, files, writeHandle.claimUploads());
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
                    result = client.publishTableCreation(operation, files, writeHandle.claimUploads());
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
        checkWritable(((HoglakeWriteHandle) handle).namespace());
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
        checkWritable(tableHandle);
        HoglakeTableHandle handle = (HoglakeTableHandle) tableHandle;
        HoglakeDtos.Table table = client.getTable(handle.schemaName(), handle.tableName(), handle.snapshotId()).orElseThrow(() -> new TableNotFoundException(handle.schemaTableName()));
        Optional<String> insertFailure = Optional.empty();
        List<HoglakeDtos.PartitionField> partitionFields = HoglakePartitioning.read(table.partitionSpec());
        try {
            HoglakePartitioning.validate(partitionFields, handle.columns());
        }
        catch (TrinoException e) {
            insertFailure = Optional.of(e.getMessage());
        }
        List<HoglakeDtos.SortField> sortFields = HoglakeSorting.read(table.sortSpec());
        try {
            HoglakeSorting.validate(sortFields, handle.columns());
        }
        catch (TrinoException e) {
            insertFailure = Optional.of(e.getMessage());
        }
        // INSERT-only MERGE has no update cases, so also enforce this in the worker sink.
        if (!updateCaseColumns.isEmpty() && insertFailure.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, insertFailure.orElseThrow());
        }
        if (!updateCaseColumns.isEmpty()) {
            // The worker sink checks the shredded layouts when an INSERT-only MERGE writes rows
            HoglakeVariantShredding.checkWritable(handle.columns());
        }
        handle.columns().forEach(column -> HoglakeTypes.toHoglakeType(column.type()));
        HoglakeDtos.Catalog catalog = client.getCatalog();
        checkRetryMode(retryMode, catalog);
        if (transaction != null) {
            transaction.checkWriteSupport(catalog);
        }
        checkWriteSchemaSupport(catalog, handle.columns().stream().map(HoglakeColumnHandle::type).toList());
        if (catalog.capabilities() == null || !catalog.capabilities().contains("idempotent-mutation-v1")) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support idempotent-mutation-v1 required for DELETE, UPDATE and MERGE");
        }
        return new HoglakeDeleteHandle(handle, tableWritePath(catalog.dataPath(), handle.schemaName(), handle.tableName()), transaction == null ? UUID.randomUUID().toString() : transaction.operation(), insertFailure, insertFailure.isPresent() ? List.of() : partitionFields, insertFailure.isPresent() ? List.of() : sortFields, catalog.capabilities().contains("claimed-uploads-v1"));
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
        checkWritable(delete.table());
        HoglakeDeletePublisher publisher = new HoglakeDeletePublisher(client, fileSystemFactory.create(session));
        if (transaction == null) {
            publisher.publish(delete, fragments, memoryContext);
        }
        else {
            publisher.publish(delete, fragments, memoryContext, transaction::stage);
        }
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
            HoglakeDtos.Commit request = new HoglakeDtos.Commit(handle.snapshot(), List.of(new HoglakeDtos.Append(handle.namespace(), handle.table(), handle.tableUuid(), files)), handle.insertOperation().orElse(null));
            if (transaction != null) {
                transaction.stage(request);
            }
            else if (handle.claimUploads()) {
                client.commitClaimed(request, false);
            }
            else {
                client.commit(request);
            }
        }
    }

    /**
     * The directory INSERT, UPDATE, DELETE and MERGE write a table's new data files and
     * deletion vectors under, and claim uploads under: {@code <data_path>/data/<namespace>/<table>/}.
     * <p>
     * A table in a contained namespace may register files only under
     * {@code <data_path>/data/<namespace>/}, spelled with the namespace name the commit names,
     * and Hoglake refuses anything else with {@code containment_violation}. The catalog root
     * itself is outside every namespace's prefix. The same directory is legal for an uncontained
     * namespace, and it is where Hoglake's compaction writes the table's files.
     * CREATE TABLE AS keeps the {@code write_path} Hoglake prepares for the new table.
     */
    static String tableWritePath(String catalogDataPath, String namespace, String table)
    {
        return (catalogDataPath.endsWith("/") ? catalogDataPath : catalogDataPath + "/") + "data/" + namespace + "/" + table + "/";
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

    private static void checkWriteSchemaSupport(HoglakeDtos.Catalog catalog, List<Type> types)
    {
        if (types.stream().anyMatch(HoglakeTypes::requiresRecursiveWriteSchema) &&
                (catalog.capabilities() == null || !catalog.capabilities().contains("recursive-write-schema-v1"))) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support recursive-write-schema-v1");
        }
    }

    private static void checkRetryMode(RetryMode retryMode, HoglakeDtos.Catalog catalog)
    {
        if (retryMode == RetryMode.NO_RETRIES) {
            return;
        }
        List<String> capabilities = catalog.capabilities();
        if (capabilities == null || !capabilities.containsAll(List.of("claimed-uploads-v1", "idempotent-append-v1", "atomic-table-creation-v1"))) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake write retries require claimed-uploads-v1, idempotent-append-v1 and atomic-table-creation-v1");
        }
        // Trino retains the planned write handle across TASK and QUERY execution retries.
        // Each attempt uploads fresh objects; only the engine's winning fragments are
        // published using that handle's stable operation ID. Losing uploads are fenced
        // and reclaimed through the server's upload ledger.
    }

    // ---- helpers -----------------------------------------------------------

    static HoglakeColumnHandle toColumnHandle(HoglakeDtos.Column column)
    {
        return new HoglakeColumnHandle(
                column.name(),
                column.fieldId(),
                HoglakeTypes.toTrinoType(column),
                column.isNullable(),
                column.children().stream().map(HoglakeMetadata::toColumnHandle).toList(),
                column.type(),
                column.comment(),
                List.of(),
                HoglakeVariantShredding.declaration(column));
    }

    private static List<ColumnMetadata> columnMetadata(HoglakeDtos.Table table)
    {
        return table.columns().stream()
                .map(column -> toColumnHandle(column).columnMetadata())
                .toList();
    }
}
