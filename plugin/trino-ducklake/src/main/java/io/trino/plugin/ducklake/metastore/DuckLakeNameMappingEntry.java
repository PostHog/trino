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

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A single top-level entry of a {@link DuckLakeNameMapping}: the Parquet column that supplies
 * the values of one DuckLake field.
 *
 * @param sourceName name of the Parquet column in the data file, or the Hive partition key when
 *         {@code hivePartition} is true
 * @param hivePartition true when the values are stored in the file path as a Hive partition
 *         ({@code key=value}) instead of the Parquet data
 * @param hasNestedFields true when the mapping contains entries for fields nested inside this
 *         column, meaning the nested Parquet names may differ from the catalog names
 */
public record DuckLakeNameMappingEntry(String sourceName, boolean hivePartition, boolean hasNestedFields)
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeNameMappingEntry.class));

    public DuckLakeNameMappingEntry
    {
        requireNonNull(sourceName, "sourceName is null");
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(sourceName);
    }
}
