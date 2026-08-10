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

public record DuckLakeTableEntry(
        long tableId,
        long schemaId,
        String schemaName,
        String tableName,
        String path,
        boolean pathIsRelative,
        String schemaPath,
        boolean schemaPathIsRelative)
{
    public DuckLakeTableEntry
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(path, "path is null");
        requireNonNull(schemaPath, "schemaPath is null");
    }
}
