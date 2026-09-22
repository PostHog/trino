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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A hoglake table, resolved once at analysis time and pinned: schema =
 * hoglake namespace, table = table name, plus the snapshot the query
 * runs at, the incarnation it bound to, and the resolved columns.
 *
 * <p>The pin is what closes the head race: getColumnHandles and
 * getTableMetadata serve from this handle without re-fetching head, and
 * getSplits scans at {@code snapshotId} — so a concurrent commit (or
 * DROP + CREATE, which changes {@code tableUuid}) cannot rebind the
 * query to a different table state between analysis and execution. One
 * query, one consistent snapshot.
 *
 * <p>The constraint is used only for scan pruning; the engine retains residual filters.
 */
public record HoglakeTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("snapshotId") long snapshotId,
        @JsonProperty("tableUuid") String tableUuid,
        @JsonProperty("columns") List<HoglakeColumnHandle> columns,
        @JsonProperty("constraint") TupleDomain<HoglakeColumnHandle> constraint,
        @JsonProperty("stagedFiles") List<HoglakeDtos.ScanFile> stagedFiles,
        @JsonProperty("stagedDeletes") List<HoglakeDtos.DeleteFile> stagedDeletes)
        implements ConnectorTableHandle
{
    @JsonCreator
    public HoglakeTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableUuid, "tableUuid is null");
        requireNonNull(constraint, "constraint is null");
        columns = List.copyOf(requireNonNull(columns, "columns is null"));
        stagedFiles = stagedFiles == null ? List.of() : List.copyOf(stagedFiles);
        stagedDeletes = stagedDeletes == null ? List.of() : List.copyOf(stagedDeletes);
    }

    public HoglakeTableHandle(String schemaName, String tableName, long snapshotId, String tableUuid, List<HoglakeColumnHandle> columns, TupleDomain<HoglakeColumnHandle> constraint)
    {
        this(schemaName, tableName, snapshotId, tableUuid, columns, constraint, List.of(), List.of());
    }

    public HoglakeTableHandle(String schemaName, String tableName, long snapshotId, String tableUuid, List<HoglakeColumnHandle> columns)
    {
        this(schemaName, tableName, snapshotId, tableUuid, columns, TupleDomain.all());
    }

    public HoglakeTableHandle withConstraint(TupleDomain<HoglakeColumnHandle> constraint)
    {
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes);
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + "@" + snapshotId;
    }
}
