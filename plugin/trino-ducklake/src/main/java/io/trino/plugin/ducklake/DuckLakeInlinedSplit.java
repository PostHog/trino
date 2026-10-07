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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedColumn;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * The rows of a table that a DuckDB writer stored inline in the catalog database, in one of its
 * inlined data tables, rather than in a data file. They are read from the catalog database at the
 * snapshot the scan is pinned to.
 *
 * @param tableId the DuckLake table the rows belong to
 * @param snapshotId the snapshot the rows are read in
 * @param watermarkSnapshotId the newest snapshot of the catalog when the inlined data tables were
 *         listed; a flush committed after it fails the read, because it may have moved rows of
 *         this split into data files the scan does not read
 * @param inlinedTableName the inlined data table holding the rows
 * @param columns the columns of the inlined data table and the DuckLake columns they hold; a column
 *         of the table that is not among them did not exist when the rows were written, and reads as
 *         {@code NULL}
 */
public record DuckLakeInlinedSplit(
        String schemaName,
        String tableName,
        long tableId,
        long snapshotId,
        long watermarkSnapshotId,
        String inlinedTableName,
        List<DuckLakeInlinedColumn> columns)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeInlinedSplit.class));
    private static final int COLUMN_INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeInlinedColumn.class));

    public DuckLakeInlinedSplit
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(inlinedTableName, "inlinedTableName is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(schemaName)
                + estimatedSizeOf(tableName)
                + estimatedSizeOf(inlinedTableName)
                + estimatedSizeOf(columns, column -> COLUMN_INSTANCE_SIZE + estimatedSizeOf(column.name()) + estimatedSizeOf(column.postgresType()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(schemaName + "." + tableName)
                .addValue(inlinedTableName)
                .add("snapshot", snapshotId)
                .toString();
    }
}
