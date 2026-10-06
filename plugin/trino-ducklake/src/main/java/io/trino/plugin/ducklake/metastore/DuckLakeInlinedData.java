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
package io.trino.plugin.ducklake.metastore;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The rows of a table that are stored inline in the catalog database and visible in a snapshot.
 *
 * @param watermarkSnapshotId the newest snapshot of the catalog when the inlined data tables were
 *         listed. DuckDB moves inlined rows into Parquet files by deleting them from the catalog
 *         database, so whoever reads the rows later has to make sure no flush of the table was
 *         committed after this snapshot; otherwise the rows it finds are not the rows that were
 *         listed.
 * @param tables the inlined data tables that hold at least one row visible in the snapshot
 */
public record DuckLakeInlinedData(long watermarkSnapshotId, List<DuckLakeInlinedDataTable> tables)
{
    public DuckLakeInlinedData
    {
        tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
    }

    public static DuckLakeInlinedData none(long watermarkSnapshotId)
    {
        return new DuckLakeInlinedData(watermarkSnapshotId, ImmutableList.of());
    }
}
