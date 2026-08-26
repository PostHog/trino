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
 * The number of rows a table has in a snapshot according to the catalog, without reading any
 * data file.
 *
 * @param rowCount the record counts of the visible data files, less the rows their delete files
 *         remove
 * @param exact whether scanning the table would return exactly {@code rowCount} rows. It would not
 *         when the table holds a file the connector refuses to read, because the scan fails instead of
 *         returning rows, or a partial data file holding rows written after the snapshot, because only
 *         some of its rows are visible and the catalog does not record how many
 */
public record DuckLakeRowCount(long rowCount, boolean exact) {}
