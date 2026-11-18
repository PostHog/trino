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
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

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
 *
 * <p>{@code countOnly} marks a scan that projects no column at all, such as
 * {@code SELECT count(*) FROM t}: its rows are counted from the catalog and
 * no file is read, so planning needs one split per file rather than byte
 * ranges.
 *
 * <p>{@code recordCount} is the table's row count exactly at {@code snapshotId}, as
 * the catalog reported it when the handle was resolved, for the optimizer's
 * statistics. It is empty when the catalog reported none for that snapshot.
 *
 * <p>{@code remote} marks a table of a remote schema, which another Hoglake serves
 * through this catalog's server. Its {@code snapshotId} is a snapshot of the
 * catalog that holds the table, never of this catalog. Such a table is read-only:
 * it has no staged changes.
 */
public record HoglakeTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("snapshotId") long snapshotId,
        @JsonProperty("tableUuid") String tableUuid,
        @JsonProperty("columns") List<HoglakeColumnHandle> columns,
        @JsonProperty("constraint") TupleDomain<HoglakeColumnHandle> constraint,
        @JsonProperty("stagedFiles") List<HoglakeDtos.ScanFile> stagedFiles,
        @JsonProperty("stagedDeletes") List<HoglakeDtos.DeleteFile> stagedDeletes,
        @JsonProperty("countOnly") boolean countOnly,
        @JsonProperty("recordCount") OptionalLong recordCount,
        @JsonProperty("remote") boolean remote)
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
        recordCount = requireNonNullElse(recordCount, OptionalLong.empty());
        if (remote && (!stagedFiles.isEmpty() || !stagedDeletes.isEmpty())) {
            throw new IllegalArgumentException("A remote table cannot have staged changes");
        }
    }

    public HoglakeTableHandle(
            String schemaName,
            String tableName,
            long snapshotId,
            String tableUuid,
            List<HoglakeColumnHandle> columns,
            TupleDomain<HoglakeColumnHandle> constraint,
            List<HoglakeDtos.ScanFile> stagedFiles,
            List<HoglakeDtos.DeleteFile> stagedDeletes,
            boolean countOnly)
    {
        this(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, countOnly, OptionalLong.empty(), false);
    }

    public HoglakeTableHandle(
            String schemaName,
            String tableName,
            long snapshotId,
            String tableUuid,
            List<HoglakeColumnHandle> columns,
            TupleDomain<HoglakeColumnHandle> constraint,
            List<HoglakeDtos.ScanFile> stagedFiles,
            List<HoglakeDtos.DeleteFile> stagedDeletes)
    {
        this(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, false);
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
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, countOnly, recordCount, remote);
    }

    public HoglakeTableHandle withCountOnly()
    {
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, true, recordCount, remote);
    }

    public HoglakeTableHandle withStaged(List<HoglakeDtos.ScanFile> stagedFiles, List<HoglakeDtos.DeleteFile> stagedDeletes)
    {
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, countOnly, recordCount, remote);
    }

    public HoglakeTableHandle withRecordCount(OptionalLong recordCount)
    {
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, countOnly, recordCount, remote);
    }

    public HoglakeTableHandle withRemote()
    {
        return new HoglakeTableHandle(schemaName, tableName, snapshotId, tableUuid, columns, constraint, stagedFiles, stagedDeletes, countOnly, recordCount, true);
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        String table = schemaName + "." + tableName + "@" + snapshotId;
        if (remote) {
            table += " remote";
        }
        if (countOnly) {
            return table + " countOnly";
        }
        return table;
    }
}
