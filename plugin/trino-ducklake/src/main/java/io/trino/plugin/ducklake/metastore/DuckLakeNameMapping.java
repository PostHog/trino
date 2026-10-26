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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;

/**
 * Maps the DuckLake fields of a table to the Parquet columns of a data file that was written
 * outside of DuckLake (for example registered with {@code ducklake_add_data_files}), so its
 * Parquet columns do not carry DuckLake field ids. Fields without an entry are not stored in
 * the file and read as NULL.
 *
 * @param columns all field entries of the mapping, keyed by the DuckLake field id
 *         ({@code ducklake_name_mapping.target_field_id}, which is a {@code ducklake_column.column_id})
 * @param nestedReadTypes the types the top-level columns with nested field entries are read as,
 *         keyed by column id: the column type with each nested field named as the file names it.
 *         They depend on the table schema, so the coordinator resolves them; a column missing here
 *         cannot be read through the mapping.
 */
public record DuckLakeNameMapping(Map<Long, DuckLakeNameMappingEntry> columns, Map<Long, Type> nestedReadTypes)
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeNameMapping.class));

    public DuckLakeNameMapping
    {
        columns = ImmutableMap.copyOf(columns);
        nestedReadTypes = ImmutableMap.copyOf(nestedReadTypes);
    }

    public DuckLakeNameMapping(Map<Long, DuckLakeNameMappingEntry> columns)
    {
        this(columns, ImmutableMap.of());
    }

    public Optional<DuckLakeNameMappingEntry> entry(long fieldId)
    {
        return Optional.ofNullable(columns.get(fieldId));
    }

    public Optional<Type> nestedReadType(long columnId)
    {
        return Optional.ofNullable(nestedReadTypes.get(columnId));
    }

    public boolean hasNestedFields()
    {
        return columns.values().stream().anyMatch(DuckLakeNameMappingEntry::hasNestedFields);
    }

    public DuckLakeNameMapping withNestedReadTypes(Map<Long, Type> nestedReadTypes)
    {
        return new DuckLakeNameMapping(columns, nestedReadTypes);
    }

    public long retainedSizeInBytes()
    {
        // the types are shared with the rest of the engine, so only the map is counted
        return INSTANCE_SIZE
                + estimatedSizeOf(columns, SizeOf::sizeOf, DuckLakeNameMappingEntry::retainedSizeInBytes)
                + estimatedSizeOf(nestedReadTypes, SizeOf::sizeOf, _ -> 0L);
    }
}
