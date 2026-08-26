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

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_column} as it is written. Unlike {@link DuckLakeColumnEntry}, which
 * describes a column read at a snapshot, this carries everything needed to insert the row,
 * including the position among its siblings and the parent it belongs to for nested types.
 */
public record DuckLakeColumnRow(
        long columnId,
        long columnOrder,
        String columnName,
        String columnType,
        OptionalLong parentColumn,
        boolean nullsAllowed,
        Optional<String> initialDefault,
        Optional<String> defaultValue)
{
    public DuckLakeColumnRow
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
        requireNonNull(parentColumn, "parentColumn is null");
        requireNonNull(initialDefault, "initialDefault is null");
        requireNonNull(defaultValue, "defaultValue is null");
    }

    public DuckLakeColumnRow withName(String newName)
    {
        return new DuckLakeColumnRow(columnId, columnOrder, newName, columnType, parentColumn, nullsAllowed, initialDefault, defaultValue);
    }

    public DuckLakeColumnRow withType(String newType)
    {
        return new DuckLakeColumnRow(columnId, columnOrder, columnName, newType, parentColumn, nullsAllowed, initialDefault, defaultValue);
    }

    public DuckLakeColumnRow withNullsAllowed(boolean newNullsAllowed)
    {
        return new DuckLakeColumnRow(columnId, columnOrder, columnName, columnType, parentColumn, newNullsAllowed, initialDefault, defaultValue);
    }
}
