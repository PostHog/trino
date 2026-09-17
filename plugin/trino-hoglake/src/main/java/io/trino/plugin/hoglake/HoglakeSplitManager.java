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

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static java.util.Objects.requireNonNull;

/**
 * Split planning: GET /scan at the handle's pinned snapshot, one split per
 * data file, each carrying the deletion vector that scan paired with the
 * file. A 410 here is the typed "snapshot expired during query" failure; a
 * 404 is the table vanishing beneath the query (TABLE_NOT_FOUND) — never a
 * generic internal error.
 *
 * <p>The vector's bytes are not fetched at planning: a split describes a
 * worker's job, and the page source reads the vector through the
 * connector's filesystem for the split it is actually running. What is
 * checked here is that the scan's pairing is internally consistent, so an
 * inconsistent scan is rejected before any split reaches the engine rather
 * than being papered over per worker.
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
        if (handle.constraint().isNone() || constraint.getSummary().isNone()) {
            return new FixedSplitSource(List.of());
        }
        List<HoglakeDtos.ScanFile> scan =
                client.scan(handle.schemaName(), handle.tableName(), handle.snapshotId());
        return new FixedSplitSource(toSplits(scan));
    }

    /**
     * Pure split construction, unit-testable without a server.
     */
    static List<HoglakeSplit> toSplits(List<HoglakeDtos.ScanFile> scan)
    {
        return scan.stream()
                .map(HoglakeSplitManager::toSplit)
                .toList();
    }

    private static HoglakeSplit toSplit(HoglakeDtos.ScanFile file)
    {
        HoglakeDtos.DataFile dataFile = file.dataFile();
        if (dataFile == null) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "hoglake scan returned an entry without a data file");
        }
        HoglakeDtos.DeleteFile deleteFile = file.deleteFile();
        if (deleteFile == null) {
            return new HoglakeSplit(
                    dataFile.path(),
                    dataFile.fileSizeBytes(),
                    dataFile.recordCount(),
                    Optional.empty(),
                    0);
        }
        if (deleteFile.path() == null) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake scan paired data file %s with a deletion vector that has no path".formatted(dataFile.path()));
        }
        // The scan pairs a vector with the file it deletes from; a mismatch
        // means the pairing cannot be trusted at all, so the query fails
        // here instead of a worker deleting rows from the wrong file.
        if (deleteFile.dataFileId() != dataFile.dataFileId()) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake scan paired deletion vector %s (data_file_id %d) with data file %s (data_file_id %d)"
                            .formatted(deleteFile.path(), deleteFile.dataFileId(), dataFile.path(), dataFile.dataFileId()));
        }
        return new HoglakeSplit(
                dataFile.path(),
                dataFile.fileSizeBytes(),
                dataFile.recordCount(),
                Optional.of(deleteFile.path()),
                deleteFile.deleteCount(),
                Optional.ofNullable(deleteFile.fileFormat()));
    }
}
