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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.createPages;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestCountDistinctOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("count-distinct-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("count-distinct-scheduled-%s"));
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators()));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    void testEmptyAndNulls()
    {
        for (Type type : ImmutableList.of(VARCHAR, BIGINT)) {
            assertCount(type, ImmutableList.of(), false, 0);
            assertCount(type, ImmutableList.of(), true, 0);
            List<Page> nulls = rowPagesBuilder(type).row((Object) null).pageBreak().row((Object) null).build();
            assertCount(type, nulls, false, 0);
            assertCount(type, nulls, true, 1);
        }
    }

    @Test
    void testDuplicatesAcrossPages()
    {
        List<Page> pages = rowPagesBuilder(VARCHAR)
                .row("a").row("b").row((Object) null).pageBreak()
                .row("a").row("c").row((Object) null).build();
        assertCount(VARCHAR, pages, false, 3);
        assertCount(VARCHAR, pages, true, 4);
        List<Page> bigintPages = rowPagesBuilder(BIGINT)
                .row(1L).row((Object) null).pageBreak()
                .row(1L).row(2L).row((Object) null).build();
        assertCount(BIGINT, bigintPages, false, 2);
        assertCount(BIGINT, bigintPages, true, 3);
        assertCount(BIGINT, rowPagesBuilder(BIGINT).addSequencePage(20000, 0).addSequencePage(20000, 10000).build(), false, 30000);
    }

    @Test
    void testOutputAndMemoryRelease()
            throws Exception
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        OperatorFactory factory = new CountDistinctOperator.CountDistinctOperatorFactory(0, new PlanNodeId("test"), VARCHAR, 1, false, hashStrategyCompiler);
        try (Operator operator = factory.createOperator(driverContext)) {
            // Count channel 1, leaving an unrelated channel out of the hash table.
            operator.addInput(rowPagesBuilder(BIGINT, VARCHAR).row(1L, "a").row(2L, "a").row(3L, null).build().getFirst());
            assertThat(operator.getOutput()).isNull();
            assertThat(operator.getOperatorContext().localUserMemoryContext().getBytes()).isPositive();
            operator.finish();
            assertThat(operator.needsInput()).isFalse();
            assertThat(BIGINT.getLong(operator.getOutput().getBlock(0), 0)).isEqualTo(1);
            assertThat(operator.isFinished()).isTrue();
            assertThat(operator.getOutput()).isNull();
            assertThat(operator.getOperatorContext().localUserMemoryContext().getBytes()).isZero();
        }
    }

    @Test
    void testCloseReleasesMemory()
            throws Exception
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        Operator operator = factory(BIGINT, false).createOperator(driverContext);
        operator.addInput(rowPagesBuilder(BIGINT).addSequencePage(20_000, 0).build().getFirst());
        assertThat(operator.getOperatorContext().localUserMemoryContext().getBytes()).isPositive();
        operator.close();
        assertThat(operator.getOperatorContext().localUserMemoryContext().getBytes()).isZero();
        assertThat(operator.isFinished()).isTrue();
        assertThat(operator.needsInput()).isFalse();
        assertThat(operator.getOutput()).isNull();
        operator.close();
    }

    @Test
    void testMemoryReservationYield()
            throws Exception
    {
        for (Type type : ImmutableList.of(VARCHAR, BIGINT)) {
            List<Page> pages = createPages(type, 6_000, 600);
            var result = finishOperatorWithYieldingGroupByHash(
                    pages,
                    type,
                    factory(type, false),
                    operator -> ((CountDistinctOperator) operator).getCapacity(),
                    450_000);
            assertThat(result.yieldCount()).isGreaterThanOrEqualTo(5);
            assertThat(result.output()).hasSize(1);
            assertThat(BIGINT.getLong(result.output().getFirst().getBlock(0), 0)).isEqualTo(3_600_000);
        }
    }

    private void assertCount(Type type, List<Page> pages, boolean countNull, long expected)
    {
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        assertOperatorEquals(factory(type, countNull), driverContext, pages, resultBuilder(TEST_SESSION, BIGINT).row(expected).build());
    }

    private OperatorFactory factory(Type type, boolean countNull)
    {
        return new CountDistinctOperator.CountDistinctOperatorFactory(0, new PlanNodeId("test"), type, 0, countNull, hashStrategyCompiler);
    }
}
