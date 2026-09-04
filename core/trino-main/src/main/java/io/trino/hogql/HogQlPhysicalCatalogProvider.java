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
package io.trino.hogql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.hogql.HogQlPhysicalCatalog.Column;
import io.trino.hogql.HogQlPhysicalCatalog.Table;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.hogql.HogQlPhysicalCatalog.identifier;
import static io.trino.metadata.MetadataListing.handleListingException;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static jakarta.ws.rs.core.Response.Status.REQUEST_ENTITY_TOO_LARGE;
import static java.util.Objects.requireNonNull;

public class HogQlPhysicalCatalogProvider
{
    static final int MAX_TABLES = 10_000;
    static final int MAX_COLUMNS = 100_000;
    static final int MAX_COLUMNS_PER_TABLE = 10_000;
    static final int MAX_VALUE_CHARACTERS = 8 * 1024 * 1024;

    private final Metadata metadata;
    private final AccessControl accessControl;
    private final TransactionManager transactionManager;
    private final CatalogManager catalogManager;

    @Inject
    public HogQlPhysicalCatalogProvider(
            Metadata metadata,
            AccessControl accessControl,
            TransactionManager transactionManager,
            CatalogManager catalogManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    public HogQlPhysicalCatalog load(Session session, String catalogName)
    {
        requireNonNull(session, "session is null");
        CatalogName requestedCatalog = new CatalogName(requireNonNull(catalogName, "catalogName is null"));
        CatalogHandle expectedHandle = currentCatalogHandle(requestedCatalog);

        TransactionId transactionId = transactionManager.beginTransaction(READ_UNCOMMITTED, true, true);
        boolean success = false;
        try {
            Session transactionSession = session.beginTransactionId(transactionId, transactionManager, accessControl);
            metadata.beginQuery(transactionSession);
            try {
                HogQlPhysicalCatalog result = load(transactionSession, catalogName, requestedCatalog, expectedHandle);
                success = true;
                return result;
            }
            finally {
                metadata.cleanupQuery(transactionSession);
            }
        }
        finally {
            if (transactionManager.transactionExists(transactionId)) {
                if (success) {
                    getFutureValue(transactionManager.asyncCommit(transactionId));
                }
                else {
                    getFutureValue(transactionManager.asyncAbort(transactionId));
                }
            }
        }
    }

    private HogQlPhysicalCatalog load(
            Session transactionSession,
            String catalogName,
            CatalogName requestedCatalog,
            CatalogHandle expectedHandle)
    {
        CatalogHandle transactionHandle = metadata.getCatalogHandle(transactionSession, catalogName)
                .orElseThrow(NotFoundException::new);
        verifyCatalogUnchanged(requestedCatalog, expectedHandle, transactionHandle);
        verifyCatalogVisible(transactionSession, catalogName);

        Map<SchemaTableName, List<PhysicalColumn>> tableColumns = listPhysicalTableColumns(transactionSession, catalogName);
        HogQlPhysicalCatalog result = createCatalog(catalogName, expectedHandle, tableColumns);
        verifyCatalogUnchanged(requestedCatalog, expectedHandle, currentCatalogHandle(requestedCatalog));
        return result;
    }

    private void verifyCatalogVisible(Session session, String catalogName)
    {
        Set<String> visibleCatalogs = accessControl.filterCatalogs(session.toSecurityContext(), Set.of(catalogName));
        if (!visibleCatalogs.contains(catalogName)) {
            throw new NotFoundException();
        }
    }

    private CatalogHandle currentCatalogHandle(CatalogName catalogName)
    {
        return catalogManager.getCatalog(catalogName)
                .filter(catalog -> !catalog.isFailed())
                .map(Catalog::getCatalogHandle)
                .orElseThrow(NotFoundException::new);
    }

    private static void verifyCatalogUnchanged(CatalogName catalogName, CatalogHandle expected, CatalogHandle actual)
    {
        if (!expected.equals(actual)) {
            throw new WebApplicationException("Catalog '%s' changed while its physical inventory was read".formatted(catalogName), CONFLICT);
        }
    }

    private Map<SchemaTableName, List<PhysicalColumn>> listPhysicalTableColumns(Session session, String catalogName)
    {
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName);
        AtomicInteger filteredCount = new AtomicInteger();
        List<TableColumnsMetadata> tables;
        try {
            tables = metadata.listTableColumns(
                    session,
                    prefix,
                    relationNames -> {
                        Set<SchemaTableName> filtered = accessControl.filterTables(session.toSecurityContext(), catalogName, relationNames);
                        filteredCount.addAndGet(filtered.size());
                        return filtered;
                    });
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "table columns", catalogName);
        }
        checkState(filteredCount.get() >= tables.size(), "relation filter was not applied to every returned relation");

        Map<SchemaTableName, List<ColumnMetadata>> columnsByTable = tables.stream()
                .filter(table -> !table.getTable().getSchemaName().equals("information_schema"))
                .collect(toImmutableMap(
                        TableColumnsMetadata::getTable,
                        table -> table.getColumns().orElseThrow(() -> new WebApplicationException(
                                "Redirected tables are not supported by the physical catalog endpoint",
                                CONFLICT))));
        Map<SchemaTableName, Set<String>> allowedColumns = accessControl.filterColumns(
                session.toSecurityContext(),
                catalogName,
                columnsByTable.entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().stream()
                                        .map(ColumnMetadata::getName)
                                        .collect(toImmutableSet()))));

        Map<SchemaTableName, List<PhysicalColumn>> result = new HashMap<>();
        columnsByTable.forEach((table, columns) -> {
            Set<String> visible = allowedColumns.getOrDefault(table, Set.of());
            ImmutableList.Builder<PhysicalColumn> physicalColumns = ImmutableList.builder();
            for (int index = 0; index < columns.size(); index++) {
                ColumnMetadata column = columns.get(index);
                if (visible.contains(column.getName())) {
                    physicalColumns.add(new PhysicalColumn(column, index + 1));
                }
            }
            result.put(table, physicalColumns.build());
        });
        return Map.copyOf(result);
    }

    private static HogQlPhysicalCatalog createCatalog(
            String catalogName,
            CatalogHandle catalogHandle,
            Map<SchemaTableName, List<PhysicalColumn>> tableColumns)
    {
        if (tableColumns.size() > MAX_TABLES) {
            throw tooLarge();
        }

        ImmutableList.Builder<Table> tables = ImmutableList.builderWithExpectedSize(tableColumns.size());
        int totalColumns = 0;
        int valueCharacters = catalogName.length() + catalogHandle.getVersion().toString().length();

        List<Map.Entry<SchemaTableName, List<PhysicalColumn>>> sortedTables = tableColumns.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(SchemaTableName::getSchemaName)
                        .thenComparing(SchemaTableName::getTableName)))
                .toList();
        for (Map.Entry<SchemaTableName, List<PhysicalColumn>> entry : sortedTables) {
            SchemaTableName tableName = entry.getKey();
            List<PhysicalColumn> columnMetadata = entry.getValue();
            if (columnMetadata.size() > MAX_COLUMNS_PER_TABLE || totalColumns + columnMetadata.size() > MAX_COLUMNS) {
                throw tooLarge();
            }

            ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(columnMetadata.size());
            for (int index = 0; index < columnMetadata.size(); index++) {
                PhysicalColumn physicalColumn = columnMetadata.get(index);
                ColumnMetadata column = physicalColumn.metadata();
                String typeSignature = column.getType().getTypeId().getId();
                valueCharacters += column.getName().length() + typeSignature.length();
                columns.add(new Column(
                        identifier(column.getName()),
                        physicalColumn.ordinal(),
                        typeSignature,
                        column.isNullable(),
                        column.isHidden(),
                        !column.isHidden()));
            }
            valueCharacters += tableName.getSchemaName().length() + tableName.getTableName().length();
            if (valueCharacters > MAX_VALUE_CHARACTERS) {
                throw tooLarge();
            }
            totalColumns += columnMetadata.size();
            tables.add(new Table(identifier(tableName.getSchemaName()), identifier(tableName.getTableName()), columns.build()));
        }

        return new HogQlPhysicalCatalog(
                HogQlPhysicalCatalog.PROTOCOL_VERSION,
                HogQlPhysicalCatalog.SCHEMA_VERSION,
                identifier(catalogName),
                catalogHandle.getVersion().toString(),
                tables.build());
    }

    private static WebApplicationException tooLarge()
    {
        return new WebApplicationException("Physical catalog inventory exceeds the compatibility endpoint limit", REQUEST_ENTITY_TOO_LARGE);
    }

    private record PhysicalColumn(ColumnMetadata metadata, int ordinal) {}
}
