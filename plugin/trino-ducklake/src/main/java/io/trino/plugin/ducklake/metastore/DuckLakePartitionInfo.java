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

/**
 * The partitioning scheme of a table visible at a snapshot. Data files record the scheme they
 * were written with in {@code ducklake_data_file.partition_id}; the partition values of a file
 * are only meaningful when interpreted with the matching scheme.
 */
public record DuckLakePartitionInfo(
        long partitionId,
        List<DuckLakePartitionColumn> columns)
{
    public DuckLakePartitionInfo
    {
        columns = ImmutableList.copyOf(columns);
    }
}
