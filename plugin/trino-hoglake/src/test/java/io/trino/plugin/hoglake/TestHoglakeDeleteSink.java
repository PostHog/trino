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

import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.operator.MergeWriterOperator;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeDeleteSink
{
    @Test
    void testOperatorCancellationAbortsParquetWriter()
            throws Exception
    {
        try (var executor = newCachedThreadPool();
                var scheduledExecutor = newScheduledThreadPool(1)) {
            var context = createTaskContext(executor, scheduledExecutor, testSessionBuilder().build())
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext()
                    .addOperatorContext(0, new PlanNodeId("test"), "test");
            var memory = context.newLocalUserMemoryContext("test");
            var columns = List.of(new HoglakeColumnHandle("id", 1, BIGINT, true));
            var write = new HoglakeWriteHandle("ns", "target", "synthetic", 1, "memory:///warehouse/", columns, columns, Optional.empty());
            var fileSystem = new MemoryFileSystem();
            var writer = new HoglakePageSink(fileSystem, write, "test");
            var sink = new HoglakeMergeSink(writer, memory::setBytes);
            var operator = new MergeWriterOperator(context, sink, Function.identity(), memory);
            operator.addInput(new Page(
                    RunLengthEncodedBlock.create(BIGINT, 42L, 1),
                    RunLengthEncodedBlock.create(TINYINT, (long) INSERT_OPERATION_NUMBER, 1),
                    RunLengthEncodedBlock.create(BIGINT, 0L, 1),
                    RunLengthEncodedBlock.create(HoglakeColumnHandle.ROW_ID.type(), null, 1),
                    RunLengthEncodedBlock.create(TINYINT, 0L, 1)));
            assertThat(writer.getMemoryUsage()).isPositive();
            operator.close();
            assertThatThrownBy(writer::finish).hasMessage("Sink is aborted");
            assertThat(fileSystem.listFiles(Location.of("memory:///warehouse/")).hasNext()).isFalse();
            assertThat(context.getOperatorMemoryContext().getUserMemory()).isZero();
        }
    }

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

    @Test
    void testMergeAccountsWriterAndBothFragmentKinds()
            throws Exception
    {
        AtomicLong current = new AtomicLong();
        var columns = List.of(new HoglakeColumnHandle("id", 1, BIGINT, true));
        var write = new HoglakeWriteHandle("ns", "target", "synthetic", 1, "memory:///warehouse/", columns, columns, Optional.empty());
        var sink = new HoglakeMergeSink(new HoglakePageSink(new MemoryFileSystem(), write, "test"), current::set);
        var insert = new Page(
                RunLengthEncodedBlock.create(BIGINT, 42L, 2),
                RunLengthEncodedBlock.create(TINYINT, (long) INSERT_OPERATION_NUMBER, 2),
                RunLengthEncodedBlock.create(BIGINT, 0L, 2),
                RunLengthEncodedBlock.create(HoglakeColumnHandle.ROW_ID.type(), null, 2));
        sink.storeMergedRows(insert);
        assertThat(current.get()).isPositive();
        sink.storeMergedRows(deletePage());
        var fragments = sink.finish().get();
        assertThat(fragments).hasSize(2);
        assertThat(current.get()).isGreaterThanOrEqualTo(fragments.stream().mapToLong(fragment -> fragment.getRetainedSize()).sum());
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
