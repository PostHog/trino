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

import static java.util.Objects.requireNonNull;

/**
 * A column of an inlined data table, the table in the catalog database that holds the rows a
 * DuckDB writer inlined while the DuckLake table had one schema version.
 *
 * @param columnId the DuckLake column the values belong to. An inlined data table names its
 *         columns as the table named them at its schema version, so the identifier is what
 *         carries the values across later renames.
 * @param name the name of the column in the inlined data table
 * @param postgresType the type of that column in the catalog database, as its {@code udt_name}
 *         spells it ({@code int4}, {@code bytea}, {@code varchar}, ...). DuckDB stores some types
 *         as text or bytes there, so this, not the DuckLake type, says how a value is stored.
 */
public record DuckLakeInlinedColumn(long columnId, String name, String postgresType)
{
    public DuckLakeInlinedColumn
    {
        requireNonNull(name, "name is null");
        requireNonNull(postgresType, "postgresType is null");
    }
}
