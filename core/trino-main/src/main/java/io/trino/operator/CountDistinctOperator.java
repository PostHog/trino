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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * Fuses a final, single-key distinct aggregation and the global count immediately above it.
 * Input must already have the partitioning required by the distinct aggregation. The count is
 * local to this driver; the existing final count aggregation sums these counts across drivers.
 * Full keys and the engine's equality operators are retained, so hash collisions are exact.
 */
public final class CountDistinctOperator
        implements Operator
{
    public static final class CountDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Type type;
        private final int channel;
        private final boolean countNull;
        private final FlatHashStrategyCompiler hashStrategyCompiler;
        private boolean closed;

        public CountDistinctOperatorFactory(int operatorId, PlanNodeId planNodeId, Type type, int channel, boolean countNull, FlatHashStrategyCompiler hashStrategyCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.type = requireNonNull(type, "type is null");
            this.channel = channel;
            this.countNull = countNull;
            this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new CountDistinctOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId, CountDistinctOperator.class.getSimpleName()),
                    type,
                    channel,
                    countNull,
                    hashStrategyCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CountDistinctOperatorFactory(operatorId, planNodeId, type, channel, countNull, hashStrategyCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext memoryContext;
    private final int channel;
    private final boolean countNull;
    private GroupByHash groupByHash;
    private Work<?> unfinishedWork;
    private boolean sawNull;
    private boolean finishing;
    private boolean finished;

    private CountDistinctOperator(OperatorContext operatorContext, Type type, int channel, boolean countNull, FlatHashStrategyCompiler hashStrategyCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        memoryContext = operatorContext.localUserMemoryContext();
        this.channel = channel;
        this.countNull = countNull;
        groupByHash = createGroupByHash(operatorContext.getSession(), ImmutableList.of(type), false, 10_000, hashStrategyCompiler, this::updateMemory);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator does not need input");
        Block block = page.getBlock(channel);
        if (!countNull && !sawNull && block.mayHaveNull()) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    sawNull = true;
                    break;
                }
            }
        }
        unfinishedWork = groupByHash.addPage(new Page(block));
        processUnfinishedWork();
        updateMemory();
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getOutput()
    {
        if (finished || (unfinishedWork != null && !processUnfinishedWork()) || !finishing) {
            return null;
        }
        long count = groupByHash.getGroupCount() - (sawNull ? 1 : 0);
        BlockBuilder output = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(output, count);
        close();
        return new Page(output.build());
    }

    private boolean processUnfinishedWork()
    {
        if (!unfinishedWork.process()) {
            return false;
        }
        unfinishedWork = null;
        updateMemory();
        return true;
    }

    private boolean updateMemory()
    {
        memoryContext.setBytes(groupByHash.getEstimatedSize());
        return operatorContext.isWaitingForMemory().isDone();
    }

    @Override
    public void close()
    {
        finishing = true;
        finished = true;
        unfinishedWork = null;
        groupByHash = null;
        memoryContext.setBytes(0);
    }

    @VisibleForTesting
    int getCapacity()
    {
        return groupByHash.getCapacity();
    }
}
