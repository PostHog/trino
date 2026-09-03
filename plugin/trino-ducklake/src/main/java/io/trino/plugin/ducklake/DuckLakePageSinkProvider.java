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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.spi.PageIndexerFactory;
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

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.ducklake.DuckLakeSessionProperties.getTargetMaxFileSize;
import static java.util.Objects.requireNonNull;

public class DuckLakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DuckLakeWriterFactory writerFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec;
    private final int maxOpenPartitions;

    @Inject
    public DuckLakePageSinkProvider(
            TrinoFileSystemFactory fileSystemFactory,
            DuckLakeWriterFactory writerFactory,
            PageIndexerFactory pageIndexerFactory,
            JsonCodec<DuckLakeDataFile> dataFileCodec,
            JsonCodec<DuckLakeMergeFragment> mergeFragmentCodec,
            DuckLakeConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.mergeFragmentCodec = requireNonNull(mergeFragmentCodec, "mergeFragmentCodec is null");
        this.maxOpenPartitions = config.getMaxOpenPartitions();
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        if (tableHandle instanceof DuckLakeReplaceTarget target) {
            return createPageSink(session, target.tableLocation(), target.columns(), target.partitionFields());
        }
        return createPageSink(session, (DuckLakeWriteTarget) tableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorInsertTableHandle tableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(session, (DuckLakeWriteTarget) tableHandle);
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        DuckLakeWriteTarget target = ((DuckLakeMergeTableHandle) mergeHandle).writeTarget();
        return new DuckLakeMergeSink(
                createPageSink(session, target),
                target.columns().size(),
                dataFileCodec,
                mergeFragmentCodec);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, DuckLakeWriteTarget target)
    {
        return createPageSink(
                session,
                target.tableLocation(),
                target.columns(),
                target.partitioning().map(DuckLakePartitioning::fields).orElseGet(ImmutableList::of));
    }

    private ConnectorPageSink createPageSink(
            ConnectorSession session,
            String tableLocation,
            List<DuckLakeWriteColumn> columns,
            List<DuckLakePartitioning.Field> partitionFields)
    {
        return new DuckLakePageSink(
                session,
                fileSystemFactory.create(session),
                writerFactory,
                pageIndexerFactory,
                DuckLakeParquetSchema.create(columns),
                partitionFields.isEmpty() ? Optional.empty() : Optional.of(new DuckLakeWritePartitioner(partitionFields, columns)),
                tableLocation,
                getTargetMaxFileSize(session).toBytes(),
                maxOpenPartitions,
                dataFileCodec);
    }
}
