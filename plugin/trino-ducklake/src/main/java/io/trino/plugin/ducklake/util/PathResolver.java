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
package io.trino.plugin.ducklake.util;

import io.trino.plugin.ducklake.metastore.DuckLakeTableEntry;

import static java.util.Objects.requireNonNull;

/**
 * Resolves DuckLake relative paths. In DuckLake, a data file path is relative to the table path,
 * the table path is relative to the schema path, and the schema path is relative to the base data
 * path of the lake. Any of them may also be stored as an absolute path.
 */
public final class PathResolver
{
    private PathResolver() {}

    public static String resolve(String basePath, String path, boolean pathIsRelative)
    {
        requireNonNull(basePath, "basePath is null");
        requireNonNull(path, "path is null");
        if (!pathIsRelative) {
            return path;
        }
        if (path.isEmpty()) {
            return basePath;
        }
        if (basePath.isEmpty() || basePath.endsWith("/")) {
            return basePath + path;
        }
        return basePath + "/" + path;
    }

    public static String tableLocation(String dataPath, DuckLakeTableEntry table)
    {
        String schemaLocation = resolve(dataPath, table.schemaPath(), table.schemaPathIsRelative());
        return resolve(schemaLocation, table.path(), table.pathIsRelative());
    }
}
