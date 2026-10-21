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
package io.trino.plugin.posthogexternal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.TableInfo.ExtendedRelationType.TABLE;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DELTA_STORAGE_FORMAT;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Hive metastore view of the schemas and table registrations PostHog stores for one catalog.
 * Every table is external, so no connector ever deletes its files on drop.
 * Delta tables carry no columns or cached schema: the connector reads the schema from the transaction log.
 */
public class PostHogExternalMetastore
        implements HiveMetastore
{
    // Delta and Hive need at least one column on a table; Delta ignores it and reads the transaction log
    private static final List<Column> PLACEHOLDER_COLUMNS = ImmutableList.of(new Column("col", HiveType.HIVE_STRING, Optional.empty(), Map.of()));
    private static final Map<String, HiveStorageFormat> FILE_FORMATS = ImmutableMap.of(
            "parquet", HiveStorageFormat.PARQUET,
            "csv", HiveStorageFormat.CSV,
            "json", HiveStorageFormat.OPENX_JSON);

    private final PostHogMetastoreClient client;
    private final Optional<String> user;

    public PostHogExternalMetastore(PostHogMetastoreClient client, Optional<String> user)
    {
        this.client = requireNonNull(client, "client is null");
        this.user = requireNonNull(user, "user is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        if (!client.schemaExists(user, databaseName)) {
            return Optional.empty();
        }
        return Optional.of(Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build());
    }

    @Override
    public List<String> getAllDatabases()
    {
        return client.listSchemas(user);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return client.getTable(user, databaseName, tableName)
                .map(table -> toHiveTable(databaseName, table));
    }

    static Table toHiveTable(String schema, ExternalTable table)
    {
        String format = table.format().toLowerCase(Locale.ROOT);
        if (format.equals(ExternalTable.DELTA)) {
            Table.Builder builder = Table.builder()
                    .setDatabaseName(schema)
                    .setTableName(table.name())
                    .setOwner(Optional.empty())
                    .setTableType(EXTERNAL_TABLE.name())
                    .setDataColumns(PLACEHOLDER_COLUMNS)
                    .setParameters(ImmutableMap.of(
                            SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER,
                            "EXTERNAL", "TRUE"));
            builder.getStorageBuilder()
                    .setStorageFormat(DELTA_STORAGE_FORMAT)
                    .setSerdeParameters(ImmutableMap.of(PATH_PROPERTY, table.location()))
                    .setLocation(table.location());
            return builder.build();
        }
        HiveStorageFormat fileFormat = FILE_FORMATS.get(format);
        if (fileFormat == null) {
            throw new TrinoException(NOT_SUPPORTED, "Table %s.%s has unsupported format '%s'".formatted(schema, table.name(), table.format()));
        }
        Table.Builder builder = Table.builder()
                .setDatabaseName(schema)
                .setTableName(table.name())
                .setOwner(Optional.empty())
                .setTableType(EXTERNAL_TABLE.name())
                .setDataColumns(table.columns().stream()
                        .map(column -> new Column(column.name(), HiveType.valueOf(column.type()), Optional.empty(), Map.of()))
                        .collect(toImmutableList()))
                .setParameters(ImmutableMap.<String, String>builder()
                        .putAll(table.properties())
                        .put("EXTERNAL", "TRUE")
                        .buildKeepingLast());
        builder.getStorageBuilder()
                .setStorageFormat(fileFormat.toStorageFormat())
                .setLocation(table.location());
        return builder.build();
    }

    static ExternalTable toExternalTable(Table table)
    {
        boolean delta = DELTA_LAKE_PROVIDER.equalsIgnoreCase(table.getParameters().get(SPARK_TABLE_PROVIDER_KEY));
        if (!delta || !table.getTableType().equals(EXTERNAL_TABLE.name())) {
            throw new TrinoException(NOT_SUPPORTED, "Only existing Delta tables can be added to this catalog, with system.register_table");
        }
        String location = Optional.ofNullable(table.getStorage().getSerdeParameters().get(PATH_PROPERTY))
                .orElseGet(() -> table.getStorage().getLocation())
                .replaceAll("/+$", "");
        // Delta's cached schema and version parameters are left out on purpose: the transaction log is the source of truth
        return new ExternalTable(table.getTableName(), ExternalTable.DELTA, location, ImmutableList.of(), ImmutableMap.of());
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return client.listTables(user, databaseName).stream()
                .map(name -> new TableInfo(new SchemaTableName(databaseName, name), TABLE))
                .collect(toImmutableList());
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return client.listTables(user, databaseName).stream()
                .filter(name -> getTable(databaseName, name)
                        .map(table -> parameterValues.contains(table.getParameters().get(parameterKey)))
                        .orElse(false))
                .collect(toImmutableList());
    }

    @Override
    public void createDatabase(Database database)
    {
        if (database.getLocation().isPresent()) {
            throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Schemas in this catalog cannot have a location");
        }
        client.createSchema(user, database.getDatabaseName());
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        // the schema has no location, so there is never data to delete
        client.dropSchema(user, databaseName);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        client.createTable(user, table.getDatabaseName(), toExternalTable(table));
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        // removes the registration only; the files belong to the tenant
        client.dropTable(user, databaseName, tableName);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return ImmutableList.of();
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return ImmutableList.of();
    }

    private static TrinoException readOnly()
    {
        return new TrinoException(NOT_SUPPORTED, "Tables in this catalog are read-only; only registering and unregistering them is supported");
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        throw readOnly();
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        throw readOnly();
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw readOnly();
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw readOnly();
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        throw readOnly();
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw readOnly();
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw readOnly();
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw readOnly();
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw readOnly();
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw readOnly();
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw readOnly();
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw readOnly();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw readOnly();
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw readOnly();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw readOnly();
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw readOnly();
    }

    @Override
    public void dropRole(String role)
    {
        throw readOnly();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw readOnly();
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw readOnly();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw readOnly();
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw readOnly();
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw readOnly();
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw readOnly();
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw readOnly();
    }
}
