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

import com.google.common.collect.ImmutableMap;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.Map;
import java.util.Optional;

/**
 * The names a DuckLake data file gives the columns it stores.
 * <p>
 * A DuckLake file records the column identifier of each of its columns as the Parquet field id,
 * which is what makes a column identifiable after it has been renamed: the catalog knows the
 * column by its new name while the file still stores the old one. Reading by field id is what
 * lets both agree, and is how DuckDB reads these files.
 * <p>
 * Only top-level columns are resolved. A field id on a nested field identifies it just as well,
 * but renaming one is a separate operation this connector does not offer, so a struct is read
 * by the names its fields have in the file.
 */
public final class DuckLakeFieldIds
{
    private DuckLakeFieldIds() {}

    /**
     * Maps the column identifier of every top-level column of the file to the name the file gives
     * it. Empty when the file records no identifiers at all, which is the case for one written
     * before DuckLake wrote them and for one registered from outside DuckLake; such a file is read
     * by name, or through a name mapping when the catalog carries one for it.
     */
    public static Map<Integer, String> columnNamesByFieldId(MessageType fileSchema)
    {
        ImmutableMap.Builder<Integer, String> names = ImmutableMap.builder();
        for (Type field : fileSchema.getFields()) {
            Type.ID id = field.getId();
            if (id != null) {
                names.put(id.intValue(), field.getName());
            }
        }
        // a valid file gives each identifier once; keeping the last rather than throwing means a
        // malformed one reads a column instead of failing the query
        return names.buildKeepingLast();
    }

    /**
     * The name the given file gives the column with this identifier, or empty when the file does
     * not store that column.
     */
    public static Optional<String> columnName(Map<Integer, String> namesByFieldId, long columnId)
    {
        if (columnId < Integer.MIN_VALUE || columnId > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        return Optional.ofNullable(namesByFieldId.get((int) columnId));
    }
}
