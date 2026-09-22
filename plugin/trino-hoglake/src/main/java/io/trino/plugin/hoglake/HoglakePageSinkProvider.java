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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MemoryContext;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HoglakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String trinoVersion;

    public HoglakePageSinkProvider(TrinoFileSystemFactory fileSystemFactory, String trinoVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorMergeTableHandle handle,
            Optional<ConnectorTableCredentials> credentials,
            ConnectorPageSinkId sinkId,
            MemoryContext memoryContext)
    {
        HoglakeDeleteHandle merge = (HoglakeDeleteHandle) handle;
        HoglakeTableHandle table = merge.table();
        HoglakeWriteHandle write = new HoglakeWriteHandle(table.schemaName(), table.tableName(), table.tableUuid(), table.snapshotId(), merge.dataPath(), table.columns(), table.columns(), Optional.empty());
        return new HoglakeMergeSink(new HoglakePageSink(fileSystemFactory.create(session), write, trinoVersion), memoryContext);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle handle, Optional<ConnectorTableCredentials> credentials, ConnectorPageSinkId sinkId)
    {
        return new HoglakePageSink(fileSystemFactory.create(session), (HoglakeWriteHandle) handle, trinoVersion);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle handle, Optional<ConnectorTableCredentials> credentials, ConnectorPageSinkId sinkId)
    {
        return new HoglakePageSink(fileSystemFactory.create(session), (HoglakeWriteHandle) handle, trinoVersion);
    }
}
