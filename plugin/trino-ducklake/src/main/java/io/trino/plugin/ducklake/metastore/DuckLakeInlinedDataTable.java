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
 * An inlined data table that holds rows visible in a snapshot.
 *
 * @param tableName the name of the table in the catalog database, such as
 *         {@code ducklake_inlined_data_12_3}
 * @param schemaVersion the schema version of the DuckLake table the rows were written under
 * @param columns the data columns of the table, excluding the {@code row_id},
 *         {@code begin_snapshot} and {@code end_snapshot} columns every inlined data table has
 */
public record DuckLakeInlinedDataTable(String tableName, long schemaVersion, List<DuckLakeInlinedColumn> columns)
{
    public DuckLakeInlinedDataTable
    {
        requireNonNull(tableName, "tableName is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }
}
