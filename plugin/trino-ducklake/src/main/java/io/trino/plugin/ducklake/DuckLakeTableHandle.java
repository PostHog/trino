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
package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * @param rowCount the number of rows of the table in the snapshot, present once a {@code count(*)}
 *         over the table has been pushed into the catalog. The scan then reads no file and produces a
 *         single row holding this count under {@link DuckLakeColumnHandle#ROW_COUNT_COLUMN}.
 */
public record DuckLakeTableHandle(
        String schemaName,
        String tableName,
        long tableId,
        long snapshotId,
        String tableLocation,
        TupleDomain<DuckLakeColumnHandle> enforcedConstraint,
        TupleDomain<DuckLakeColumnHandle> unenforcedConstraint,
        OptionalLong rowCount)
        implements ConnectorTableHandle
{
    public DuckLakeTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
        requireNonNull(rowCount, "rowCount is null");
    }

    public DuckLakeTableHandle withRowCount(long rowCount)
    {
        return new DuckLakeTableHandle(
                schemaName,
                tableName,
                tableId,
                snapshotId,
                tableLocation,
                enforcedConstraint,
                unenforcedConstraint,
                OptionalLong.of(rowCount));
    }

    @JsonIgnore
    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        if (rowCount.isPresent()) {
            return schemaName + "." + tableName + "@" + snapshotId + " rows=" + rowCount.orElseThrow();
        }
        return schemaName + "." + tableName + "@" + snapshotId;
    }
}
