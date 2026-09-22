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
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.MemoryContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Routes Trino's split UPDATE actions to the existing immutable-file writers.
 */
final class HoglakeMergeSink
        implements ConnectorMergeSink
{
    // Data file IDs are positive; the sentinel distinguishes append registrations.
    static final long APPEND_FRAGMENT = -1;

    private final HoglakePageSink inserts;
    private final HoglakeDeleteSink deletes;
    private final MemoryContext memoryContext;
    private long deleteBytes;
    private long positionBytes;

    HoglakeMergeSink(HoglakePageSink inserts, MemoryContext memoryContext)
    {
        this.inserts = inserts;
        this.memoryContext = memoryContext;
        this.deletes = new HoglakeDeleteSink(bytes -> {
            deleteBytes = bytes;
            updateMemory();
        });
    }

    private void updateMemory()
    {
        memoryContext.setBytes(deleteBytes + positionBytes + inserts.getMemoryUsage());
    }

    @Override
    public void storeMergedRows(Page page)
    {
        positionBytes = 2L * Integer.BYTES * page.getPositionCount();
        updateMemory();
        int[] insertPositions = new int[page.getPositionCount()];
        int[] deletePositions = new int[page.getPositionCount()];
        int insertCount = 0;
        int deleteCount = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            switch (TINYINT.getByte(page.getBlock(page.getChannelCount() - 3), position)) {
                case INSERT_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER -> insertPositions[insertCount++] = position;
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> deletePositions[deleteCount++] = position;
                default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Hoglake merge operation");
            }
        }
        deletes.storeMergedRows(page.getPositions(deletePositions, 0, deleteCount));
        inserts.appendPage(page.getPositions(insertPositions, 0, insertCount));
        positionBytes = 0;
        updateMemory();
    }

    @Override
    public long getCompletedBytes()
    {
        return inserts.getCompletedBytes();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return deletes.finish().thenCombine(inserts.finish(), (deleteFragments, insertFragments) -> {
            Collection<Slice> fragments = new ArrayList<>(deleteFragments);
            long retained = deleteBytes + insertFragments.stream().mapToLong(fragment -> fragment.getRetainedSize() + 2L * Long.BYTES).sum();
            for (Slice insert : insertFragments) {
                long reservation = retained + insert.getRetainedSize() + 3L * Long.BYTES;
                HoglakeDeleteBitmap.checkSize(reservation);
                memoryContext.setBytes(reservation);
                Slice fragment = Slices.allocate(Long.BYTES + insert.length());
                fragment.setLong(0, APPEND_FRAGMENT);
                fragment.setBytes(Long.BYTES, insert);
                fragments.add(fragment);
                retained += fragment.getRetainedSize() + 2L * Long.BYTES;
                HoglakeDeleteBitmap.checkSize(retained);
                memoryContext.setBytes(retained);
            }
            return fragments;
        });
    }

    @Override
    public void abort()
    {
        try {
            inserts.abort();
        }
        finally {
            positionBytes = 0;
            deletes.abort();
            memoryContext.setBytes(0);
        }
    }
}
