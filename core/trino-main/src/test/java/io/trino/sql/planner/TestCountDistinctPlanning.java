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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.FUSE_COUNT_DISTINCT;
import static io.trino.SystemSessionProperties.SPILL_ENABLED;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.LocalExecutionPlanner.canFuseCountDistinct;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCountDistinctPlanning
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final Session ENABLED = Session.builder(TEST_SESSION)
            .setSystemProperty(FUSE_COUNT_DISTINCT, "true")
            .build();

    @Test
    void testFinalAndSingleDistinct()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, ENABLED);
        Symbol key = p.symbol("key", BIGINT);
        for (AggregationNode.Step distinctStep : ImmutableList.of(FINAL, SINGLE)) {
            AggregationNode distinct = p.aggregation(b -> b.singleGroupingSet(key).step(distinctStep).source(p.values(key)));
            for (AggregationNode.Step countStep : ImmutableList.of(PARTIAL, SINGLE)) {
                AggregationNode count = p.aggregation(b -> b.globalGrouping().step(countStep)
                        .addAggregation(p.symbol("count"), aggregate("count", ImmutableList.of(key.toSymbolReference()), ImmutableList.of(BIGINT)))
                        .source(distinct));
                assertThat(canFuseCountDistinct(ENABLED, count)).isTrue();
                assertThat(canFuseCountDistinct(TEST_SESSION, count)).isFalse();
                assertThat(canFuseCountDistinct(Session.builder(ENABLED).setSystemProperty(SPILL_ENABLED, "true").build(), count)).isFalse();
                assertThat(canFuseCountDistinct(ENABLED, count(p, distinct))).isTrue();
            }
        }
    }

    @Test
    void testPartialAndStreamingDistinctAreNotFused()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, ENABLED);
        Symbol key = p.symbol("key", BIGINT);
        for (AggregationNode.Step step : ImmutableList.of(PARTIAL, INTERMEDIATE)) {
            // Partial aggregation may flush and emit the same key more than once.
            AggregationNode distinct = p.aggregation(b -> b.singleGroupingSet(key).step(step).source(p.values(key)));
            assertThat(canFuseCountDistinct(ENABLED, count(p, distinct))).isFalse();
        }
        AggregationNode streaming = p.aggregation(b -> b.singleGroupingSet(key).preGroupedSymbols(key).step(FINAL).source(p.values(key)));
        assertThat(canFuseCountDistinct(ENABLED, count(p, streaming))).isFalse();
    }

    @Test
    void testUnsupportedShapesAreNotFused()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, ENABLED);
        Symbol key = p.symbol("key", BIGINT);
        Symbol other = p.symbol("other", BIGINT);
        AggregationNode distinct = p.aggregation(b -> b.singleGroupingSet(key).step(FINAL).source(p.values(key)));
        AggregationNode count = count(p, distinct);
        Aggregation plainCount = count.getAggregations().values().iterator().next();
        Symbol flag = p.symbol("flag", BOOLEAN);
        AggregationNode booleanDistinct = p.aggregation(b -> b.singleGroupingSet(flag).step(FINAL).source(p.values(flag)));
        Aggregation booleanCount = aggregate("count", ImmutableList.of(flag.toSymbolReference()), ImmutableList.of(BOOLEAN));
        for (Aggregation aggregate : ImmutableList.of(
                new Aggregation(booleanCount.getResolvedFunction(), booleanCount.getArguments(), true, Optional.empty(), Optional.empty(), Optional.empty()),
                new Aggregation(booleanCount.getResolvedFunction(), booleanCount.getArguments(), false, Optional.of(flag), Optional.empty(), Optional.empty()),
                new Aggregation(booleanCount.getResolvedFunction(), booleanCount.getArguments(), false, Optional.empty(), Optional.empty(), Optional.of(flag)),
                new Aggregation(booleanCount.getResolvedFunction(), booleanCount.getArguments(), false, Optional.empty(), Optional.of(new OrderingScheme(ImmutableList.of(flag), ImmutableMap.of(flag, ASC_NULLS_LAST))), Optional.empty()))) {
            AggregationNode unsupported = p.aggregation(b -> b.globalGrouping().step(SINGLE)
                    .addAggregation(p.symbol("count"), aggregate).source(booleanDistinct));
            assertThat(canFuseCountDistinct(ENABLED, unsupported)).isFalse();
        }
        AggregationNode multipleKeys = p.aggregation(b -> b.singleGroupingSet(key, other).step(FINAL).source(p.values(key, other)));
        assertThat(canFuseCountDistinct(ENABLED, count(p, multipleKeys))).isFalse();
        AggregationNode grouped = p.aggregation(b -> b.singleGroupingSet(key).step(PARTIAL)
                .addAggregation(p.symbol("count"), plainCount).source(distinct));
        assertThat(canFuseCountDistinct(ENABLED, grouped)).isFalse();
        AggregationNode mixed = p.aggregation(b -> b.globalGrouping().step(PARTIAL)
                .addAggregation(p.symbol("count"), plainCount)
                .addAggregation(p.symbol("sum"), aggregate("sum", ImmutableList.of(key.toSymbolReference()), ImmutableList.of(BIGINT)))
                .source(distinct));
        assertThat(canFuseCountDistinct(ENABLED, mixed)).isFalse();
        AggregationNode finalCount = p.aggregation(b -> b.globalGrouping().step(FINAL)
                .addAggregation(p.symbol("count"), aggregate("count", ImmutableList.of(key.toSymbolReference()), ImmutableList.of(BIGINT)))
                .source(distinct));
        assertThat(canFuseCountDistinct(ENABLED, finalCount)).isFalse();
    }

    private static Aggregation aggregate(String name, List<Expression> arguments, List<Type> types)
    {
        return new Aggregation(FUNCTIONS.resolveFunction(name, fromTypes(types)), arguments, false, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static AggregationNode count(PlanBuilder p, AggregationNode distinct)
    {
        return p.aggregation(b -> b.globalGrouping().step(PARTIAL)
                .addAggregation(p.symbol("count"), aggregate("count", ImmutableList.of(), ImmutableList.of()))
                .source(distinct));
    }
}
