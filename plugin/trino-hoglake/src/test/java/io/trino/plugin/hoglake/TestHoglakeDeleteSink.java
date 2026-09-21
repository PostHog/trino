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

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeDeleteSink
{
    @Test
    void testEncodedFragmentsRemainAccountedUntilCleanup()
            throws Exception
    {
        AtomicLong current = new AtomicLong();
        AtomicLong peak = new AtomicLong();
        HoglakeDeleteSink sink = new HoglakeDeleteSink(bytes -> {
            current.set(bytes);
            peak.accumulateAndGet(bytes, Math::max);
        });
        sink.storeMergedRows(deletePage());
        var fragments = sink.finish().get();
        assertThat(fragments).hasSize(1);
        assertThat(current.get()).isGreaterThanOrEqualTo(fragments.iterator().next().getRetainedSize());
        assertThat(peak.get()).isGreaterThan(current.get());
        sink.abort();
        assertThat(current.get()).isZero();
    }

    @Test
    void testEncodingReservationFailsBeforeReturningFragments()
    {
        AtomicLong current = new AtomicLong();
        HoglakeDeleteSink sink = new HoglakeDeleteSink(bytes -> {
            if (bytes > 4096) {
                throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "synthetic encoding limit");
            }
            current.set(bytes);
        });
        sink.storeMergedRows(deletePage());
        assertThatThrownBy(sink::finish).isInstanceOf(TrinoException.class).hasMessageContaining("synthetic encoding limit");
        sink.abort();
        assertThat(current.get()).isZero();
    }

    private static Page deletePage()
    {
        var operation = TINYINT.createFixedSizeBlockBuilder(1);
        TINYINT.writeLong(operation, DELETE_OPERATION_NUMBER);
        var fileIds = new LongArrayBlock(2, Optional.empty(), new long[] {1, 1});
        var positions = new LongArrayBlock(2, Optional.empty(), new long[] {3, 7});
        return new Page(
                RunLengthEncodedBlock.create(operation.build(), 2),
                positions,
                RowBlock.fromNotNullSuppressedFieldBlocks(2, Optional.empty(), new Block[] {fileIds, positions}));
    }
}
