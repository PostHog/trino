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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The deletions of a table visible in a snapshot, read together so that they describe one state of
 * the catalog: DuckDB moves the deletions it recorded inline into delete files by deleting them from
 * the catalog database, and a reader that saw the delete files of one state and the inline
 * deletions of another would lose some.
 *
 * @param deleteFiles the visible delete files
 * @param inlinedDeletions the positions deleted inline, by data file identifier, each array sorted
 *         and free of duplicates
 */
public record DuckLakeDeletions(List<DuckLakeDeleteFileEntry> deleteFiles, Map<Long, long[]> inlinedDeletions)
{
    public DuckLakeDeletions
    {
        deleteFiles = ImmutableList.copyOf(requireNonNull(deleteFiles, "deleteFiles is null"));
        inlinedDeletions = ImmutableMap.copyOf(requireNonNull(inlinedDeletions, "inlinedDeletions is null"));
    }
}
