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
 * A row of {@code ducklake_view}: a query stored under a name, in the SQL dialect of whichever
 * engine defined it.
 */
public record DuckLakeViewEntry(
        long viewId,
        long schemaId,
        String schemaName,
        String viewName,
        String dialect,
        String sql,
        String columnAliases)
{
    public DuckLakeViewEntry
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(dialect, "dialect is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(columnAliases, "columnAliases is null");
    }
}
