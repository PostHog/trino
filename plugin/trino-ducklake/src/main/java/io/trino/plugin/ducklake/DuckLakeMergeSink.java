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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * Applies a row-level change to a DuckLake table.
 * <p>
 * DuckLake has no in-place update: a changed row is a row removed from the file it lives in and a
 * new row written elsewhere. This sink therefore splits the rows the engine hands it, forwarding
 * everything to be written into the ordinary data file sink, and collecting the positions of
 * everything to be removed so that the statement can record them when it commits.
 * <p>
 * The removals are only collected here, not written: at most one delete file may apply to a data
 * file, so the positions this sink saw have to be combined with the ones already recorded, which
 * only the node finishing the statement can see in full.
 */
public class DuckLakeMergeSink
        implements ConnectorMergeSink
{
    private final ConnectorPageSink insertSink;
    private final JsonCodec<DuckLakeDataFile> dataFileCodec;
    private final JsonCodec<DuckLakeMergeFragment> fragmentCodec;
    private final int dataColumnCount;
    private final Long2ObjectMap<LongArrayList> deletedPositionsByDataFile = new Long2ObjectOpenHashMap<>();

    public DuckLakeMergeSink(
            ConnectorPageSink insertSink,
            int dataColumnCount,
            JsonCodec<DuckLakeDataFile> dataFileCodec,
            JsonCodec<DuckLakeMergeFragment> fragmentCodec)
    {
        this.insertSink = requireNonNull(insertSink, "insertSink is null");
        this.dataColumnCount = dataColumnCount;
        this.dataFileCodec = requireNonNull(dataFileCodec, "dataFileCodec is null");
        this.fragmentCodec = requireNonNull(fragmentCodec, "fragmentCodec is null");
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int operationChannel = page.getChannelCount() - 3;
        int rowIdChannel = page.getChannelCount() - 1;
        Block operations = page.getBlock(operationChannel);
        int positionCount = page.getPositionCount();

        int[] insertedPositions = new int[positionCount];
        int insertedCount = 0;
        int[] removedPositions = new int[positionCount];
        int removedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            switch (TINYINT.getByte(operations, position)) {
                case INSERT_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER -> {
                    insertedPositions[insertedCount] = position;
                    insertedCount++;
                }
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> {
                    removedPositions[removedCount] = position;
                    removedCount++;
                }
                default -> throw new IllegalArgumentException("Unexpected merge operation: " + TINYINT.getByte(operations, position));
            }
        }

        if (insertedCount > 0) {
            Page dataColumns = page.getColumns(dataColumnChannels());
            insertSink.appendPage(dataColumns.getPositions(insertedPositions, 0, insertedCount));
        }
        if (removedCount > 0) {
            recordRemovedRows(page.getBlock(rowIdChannel), removedPositions, removedCount);
        }
    }

    private void recordRemovedRows(Block rowIdBlock, int[] positions, int count)
    {
        List<Block> fields = getRowFieldsFromBlock(rowIdBlock);
        Block dataFileIds = fields.get(DuckLakeMergeRowId.DATA_FILE_ID_CHANNEL);
        Block rowPositions = fields.get(DuckLakeMergeRowId.FILE_ROW_POSITION_CHANNEL);
        for (int index = 0; index < count; index++) {
            int position = positions[index];
            long dataFileId = BIGINT.getLong(dataFileIds, position);
            deletedPositionsByDataFile
                    .computeIfAbsent(dataFileId, _ -> new LongArrayList())
                    .add(BIGINT.getLong(rowPositions, position));
        }
    }

    private int[] dataColumnChannels()
    {
        int[] channels = new int[dataColumnCount];
        Arrays.setAll(channels, channel -> channel);
        return channels;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return insertSink.finish().thenApply(insertedFiles -> {
            ImmutableList.Builder<Slice> fragments = ImmutableList.builder();
            for (Slice insertedFile : insertedFiles) {
                fragments.add(toFragment(DuckLakeMergeFragment.inserted(dataFileCodec.fromJson(insertedFile.getBytes()))));
            }
            deletedPositionsByDataFile.forEach((dataFileId, positions) ->
                    fragments.add(toFragment(DuckLakeMergeFragment.deleted(dataFileId, positions.toLongArray()))));
            return fragments.build();
        });
    }

    private Slice toFragment(DuckLakeMergeFragment fragment)
    {
        return wrappedBuffer(fragmentCodec.toJsonBytes(fragment));
    }

    @Override
    public void abort()
    {
        insertSink.abort();
    }
}
