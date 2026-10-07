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

/**
 * Whether a table holds anything stored inline in the catalog database in a snapshot, which is
 * what the connector cannot modify.
 *
 * @param hasRows whether an inlined data table of the table holds a row visible in the snapshot
 * @param hasFileDeletions whether rows of a visible data file were deleted by recording their
 *         positions inline, in {@code ducklake_inlined_delete_<table id>}, rather than in a delete
 *         file
 */
public record DuckLakeInlinedSummary(boolean hasRows, boolean hasFileDeletions)
{
    public static final DuckLakeInlinedSummary NONE = new DuckLakeInlinedSummary(false, false);

    public boolean isEmpty()
    {
        return !hasRows && !hasFileDeletions;
    }
}
