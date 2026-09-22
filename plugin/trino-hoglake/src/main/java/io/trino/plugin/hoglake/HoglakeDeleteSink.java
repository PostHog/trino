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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.MemoryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;

final class HoglakeDeleteSink
        implements ConnectorMergeSink
{
    private final MemoryContext memoryContext;

    HoglakeDeleteSink(MemoryContext memoryContext)
    {
        this.memoryContext = memoryContext;
    }

    private final Map<Long, HoglakeDeleteBitmap> positions = new TreeMap<>();

    @Override
    public void storeMergedRows(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            byte operation = TINYINT.getByte(page.getBlock(page.getChannelCount() - 3), position);
            if (operation != DELETE_OPERATION_NUMBER && operation != UPDATE_DELETE_OPERATION_NUMBER) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake supports DELETE only");
            }
            SqlRow row = (SqlRow) HoglakeColumnHandle.ROW_ID.type().getObject(page.getBlock(page.getChannelCount() - 1), position);
            long fileId = BIGINT.getLong(row.getRawFieldBlock(0), row.getRawIndex());
            long filePosition = BIGINT.getLong(row.getRawFieldBlock(1), row.getRawIndex());
            positions.computeIfAbsent(fileId, _ -> new HoglakeDeleteBitmap()).add(filePosition);
        }
        long retained = positions.values().stream().mapToLong(HoglakeDeleteBitmap::retainedBytes).sum();
        HoglakeDeleteBitmap.checkSize(retained);
        memoryContext.setBytes(retained);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> fragments = new ArrayList<>();
        long fragmentBytes = 0;
        long positionBytes = positions.values().stream().mapToLong(HoglakeDeleteBitmap::retainedBytes).sum();
        try {
            var entries = positions.entrySet().iterator();
            while (entries.hasNext()) {
                var entry = entries.next();
                memoryContext.setBytes(positionBytes + fragmentBytes + entry.getValue().encodingWorkingBytes());
                byte[] vector = entry.getValue().encode("");
                Slice fragment = Slices.allocate(Long.BYTES + vector.length);
                fragment.setLong(0, entry.getKey());
                fragment.setBytes(Long.BYTES, vector);
                fragments.add(fragment);
                fragmentBytes += fragment.getRetainedSize() + 2L * Long.BYTES;
                positionBytes -= entry.getValue().retainedBytes();
                entries.remove();
                memoryContext.setBytes(positionBytes + fragmentBytes);
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to encode DELETE positions", e);
        }
        // MergeWriterOperator owns the reservation until its output has been
        // consumed and the operator closes its memory context.
        memoryContext.setBytes(fragmentBytes);
        return CompletableFuture.completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        positions.clear();
        memoryContext.setBytes(0);
    }
}
