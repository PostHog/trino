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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCountDistinctAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("optimizer.fuse-count-distinct", "true"))
                .build();
    }

    @Test
    public void testFusedOperator()
    {
        assertQueryStats(
                getSession(),
                "SELECT count(DISTINCT name) FROM nation",
                stats -> assertThat(stats.getOperatorSummaries())
                        .extracting(OperatorStats::getOperatorType)
                        .contains("CountDistinctOperator"),
                result -> assertThat(result.getOnlyValue()).isEqualTo(25L));
    }

    @Test
    public void testDistinctTypeSemantics()
    {
        assertQuery("SELECT count(DISTINCT x) FROM (VALUES true, false, true, NULL) t(x)", "VALUES 2");
        assertQuery("SELECT count(DISTINCT x) FROM (VALUES nan(), nan(), infinity(), -infinity(), DOUBLE '0', DOUBLE '-0', NULL) t(x)", "VALUES 4");
        assertQuery("SELECT count(DISTINCT x) FROM (VALUES ARRAY[1, NULL], ARRAY[1, NULL], ARRAY[2, NULL], NULL) t(x)", "VALUES 2");
        assertQuery("SELECT count(DISTINCT x) FROM (VALUES ROW(ROW(1, CAST(NULL AS INTEGER))), ROW(ROW(1, NULL)), ROW(ROW(2, NULL)), ROW(NULL)) t(x)", "VALUES 2");
        assertQuery("SELECT count(DISTINCT repeat(comment, 100)) FROM nation", "VALUES 25");
    }

    @Test
    public void testFilterEvaluatedBeforeDistinct()
    {
        // Every order has the same key. Moving the volatile filter after deduplication
        // would incorrectly return zero about half the time.
        for (int iteration = 0; iteration < 10; iteration++) {
            assertQuery("SELECT count(DISTINCT x) FROM (SELECT BIGINT '1' x FROM orders) WHERE random() < 0.5", "VALUES 1");
        }
    }
}
