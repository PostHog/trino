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
package io.trino.plugin.hoglake;

import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Split planning: GET /scan at the handle's pinned snapshot, one split
 * per data file. A 410 here is the typed "snapshot expired during
 * query" failure; a 404 is the table vanishing beneath the query
 * (TABLE_NOT_FOUND) — never a generic internal error.
 *
 * <p>DV refusal happens HERE, at planning, where the scan's
 * data-file/DV pairing is first visible: a query touching any DV-bearing
 * file fails before a single split is handed to the engine, so no
 * partial results can reach a streaming client first (v1 has no DV
 * application). The page source keeps a defense-in-depth guard.
 */
public class HoglakeSplitManager
        implements ConnectorSplitManager
{
    private final HoglakeClient client;

    public HoglakeSplitManager(HoglakeClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        List<HoglakeDtos.ScanFile> scan =
                client.scan(handle.schemaName(), handle.tableName(), handle.snapshotId());
        ensureNoRowLevelDeletes(scan);
        return new FixedSplitSource(toSplits(scan));
    }

    /**
     * v1 refusal, at planning: a scan containing any live deletion
     * vector cannot be read correctly without applying the DVs, so the
     * query fails loudly before any split exists — instead of silently
     * returning deleted rows, and instead of streaming partial results
     * from clean splits before a worker hits the DV-bearing one.
     */
    static void ensureNoRowLevelDeletes(List<HoglakeDtos.ScanFile> scan)
    {
        for (HoglakeDtos.ScanFile file : scan) {
            if (file.deleteFile() != null) {
                throw new TrinoException(NOT_SUPPORTED,
                        "table has row-level deletes; DV application not yet implemented in the hoglake connector"
                                + " (data file " + file.dataFile().path() + " has deletion vector "
                                + file.deleteFile().path() + " covering " + file.deleteFile().deleteCount() + " rows)");
            }
        }
    }

    /**
     * Pure split construction, unit-testable without a server.
     */
    static List<HoglakeSplit> toSplits(List<HoglakeDtos.ScanFile> scan)
    {
        return scan.stream()
                .map(file -> new HoglakeSplit(
                        file.dataFile().path(),
                        file.dataFile().fileSizeBytes(),
                        file.dataFile().recordCount(),
                        Optional.ofNullable(file.deleteFile()).map(HoglakeDtos.DeleteFile::path),
                        file.deleteFile() == null ? 0 : file.deleteFile().deleteCount()))
                .toList();
    }
}
