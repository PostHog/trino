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

import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.sql.SqlFormatter;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlPivot
        extends BasePlanTest
{
    private final HogQlCompiler compiler = new HogQlCompiler();

    @Test
    public void testLowersToStockFilteredAggregations()
    {
        String hogql = "SELECT custkey, filled_total, open_total FROM orders " +
                "PIVOT (sum(totalprice) AS total FOR orderstatus IN ('F' AS filled, 'O' AS open) GROUP BY custkey)";

        Plan plan = plan(SqlFormatter.formatSql(compiler.compile(hogql)), CREATED);

        List<AggregationNode> aggregations = PlanNodeSearcher.searchFrom(plan.getRoot())
                .whereIsInstanceOfAny(AggregationNode.class)
                .findAll().stream()
                .map(AggregationNode.class::cast)
                .toList();
        assertThat(aggregations).singleElement().satisfies(aggregation -> {
            assertThat(aggregation.getGroupingKeys()).hasSize(1);
            assertThat(aggregation.getAggregations()).hasSize(2);
            assertThat(aggregation.getAggregations().values())
                    .allSatisfy(value -> assertThat(value.getFilter()).isPresent());
        });
    }
}
