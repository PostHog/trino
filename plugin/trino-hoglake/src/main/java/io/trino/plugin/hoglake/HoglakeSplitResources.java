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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.connector.MemoryContext;

import static java.util.Objects.requireNonNull;

/**
 * Everything one split allocates, owned and accounted for in one place.
 *
 * <p>A split-scoped owner is established before any work starts, and every
 * allocation on the split's behalf is charged into its one aggregation: the
 * deletion vector's input bytes while they are read, the budget reserved for
 * decoding, the retained bitmap, and the Parquet reader's buffers. Because
 * there is one aggregation, there is one total — the engine's context receives
 * it and nothing else — so no call site has to decide whether a charge was
 * already made or which context the reader is using.
 *
 * <p>Ownership is equally unconditional: this owner exists from before loading
 * begins until the page source is returned, so a failure at any point closes
 * it and releases everything acquired so far. Once the page source is built it
 * adopts the owner, which is the only handoff in the lifecycle.
 *
 * <p>Not thread safe: a split's construction and its reader run on one thread,
 * except that {@link #close()} may be called from whichever thread is
 * cancelling or completing the split.
 */
final class HoglakeSplitResources
        implements AutoCloseable
{
    private final MemoryContext engineMemory;
    private final AggregatedMemoryContext allocation;

    private boolean closed;

    HoglakeSplitResources(MemoryContext engineMemory)
    {
        this.engineMemory = requireNonNull(engineMemory, "engineMemory is null");
        this.allocation = AggregatedMemoryContext.newAggregatedMemoryContext(bytes -> {
            if (!closed) {
                this.engineMemory.setBytes(bytes);
            }
        });
    }

    /**
     * The single aggregation the Parquet reader charges into and the page
     * source reports.
     */
    AggregatedMemoryContext allocation()
    {
        return allocation;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        // Suppress aggregate callbacks during teardown. A rejected reservation
        // can leave the shared handler's provisional total ahead of the
        // aggregate; replaying that total while freeing can fail again. The
        // one final zero below is the authoritative release for this owner.
        closed = true;
        try {
            allocation.close();
        }
        finally {
            // Also clear a reservation whose engine callback threw before the
            // aggregate could record it.
            engineMemory.setBytes(0);
        }
    }
}
