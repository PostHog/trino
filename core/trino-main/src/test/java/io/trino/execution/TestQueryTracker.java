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
package io.trino.execution;

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryTracker.TrackedQuery;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryTracker
{
    private static final Instant OLD = Instant.parse("2020-01-01T00:00:00Z");

    @Test
    public void testYoungHeadDoesNotHideOlderCompletion()
    {
        var retention = new CountingRetention();
        try (var executor = newSingleThreadScheduledExecutor()) {
            var tracker = new QueryTracker<TestingQuery>(config(1), executor, retention);
            TestingQuery young = add(tracker, "young", Instant.now());
            TestingQuery old = add(tracker, "old", OLD);

            tracker.removeExpiredQueries();

            assertThat(tracker.hasQuery(young.getQueryId())).isTrue();
            assertThat(tracker.hasQuery(old.getQueryId())).isFalse();
            assertThat(retention.checkedQueries).containsExactly(old.getQueryId());
            assertThat(tracker.getExpiredQueriesCount()).isEqualTo(1);
            assertThat(tracker.getPrunedQueriesCount()).isEqualTo(1);
            assertThat(young.isInfoPruned()).isTrue();

            tracker.removeExpiredQueries();
            assertThat(tracker.getPrunedQueriesCount()).isEqualTo(1);
        }
    }

    @Test
    public void testProtectedHeadDoesNotHideIdleCompletion()
    {
        var retention = new CountingRetention();
        try (var executor = newSingleThreadScheduledExecutor()) {
            var tracker = new QueryTracker<TestingQuery>(config(1), executor, retention);
            TestingQuery protectedQuery = add(tracker, "protected", OLD);
            TestingQuery idle = add(tracker, "idle", OLD);

            try (var request = retention.beginRequest(protectedQuery.getQueryId(), false).orElseThrow()) {
                tracker.removeExpiredQueries();
                assertThat(tracker.hasQuery(protectedQuery.getQueryId())).isTrue();
                assertThat(tracker.hasQuery(idle.getQueryId())).isFalse();
                assertThat(retention.checkedQueries).containsExactly(protectedQuery.getQueryId(), idle.getQueryId());
                assertThat(tracker.getExpiredQueriesCount()).isEqualTo(1);
                assertThat(tracker.getPrunedQueriesCount()).isEqualTo(1);
            }

            tracker.removeExpiredQueries();
            assertThat(tracker.getExpiredQueriesCount()).isZero();
            assertThat(tracker.getPrunedQueriesCount()).isZero();
            assertThat(retention.entryCount()).isZero();
        }
    }

    @Test
    public void testIdleSweepPrunesOnceAndKeepsNewestFullInfo()
    {
        var retention = new CountingRetention();
        try (var executor = newSingleThreadScheduledExecutor()) {
            var tracker = new QueryTracker<TestingQuery>(config(2), executor, retention);
            TestingQuery first = add(tracker, "first", Instant.now());
            TestingQuery second = add(tracker, "second", Instant.now());
            TestingQuery third = add(tracker, "third", Instant.now());
            TestingQuery fourth = add(tracker, "fourth", Instant.now());

            tracker.removeExpiredQueries();
            tracker.pruneExpiredQueries();
            assertThat(first.pruneCalls).isEqualTo(1);
            assertThat(second.pruneCalls).isEqualTo(1);
            assertThat(third.pruneCalls).isZero();
            assertThat(fourth.pruneCalls).isZero();
            assertThat(retention.checkedQueries).isEmpty();
            assertThat(tracker.getExpiredQueriesCount()).isEqualTo(4);
            assertThat(tracker.getPrunedQueriesCount()).isEqualTo(2);

            tracker.removeExpiredQueries();
            assertThat(first.pruneCalls).isEqualTo(1);
            assertThat(second.pruneCalls).isEqualTo(1);
            assertThat(tracker.getPrunedQueriesCount()).isEqualTo(2);
        }
    }

    @Test
    public void testDisabledPolicyKeepsSeparatePruningPass()
    {
        var retention = new QueryResultRetention(Optional.empty(), _ -> false, Instant::now);
        try (var executor = newSingleThreadScheduledExecutor()) {
            var tracker = new QueryTracker<TestingQuery>(config(1), executor, retention);
            TestingQuery first = add(tracker, "first", Instant.now());
            TestingQuery second = add(tracker, "second", Instant.now());

            tracker.removeExpiredQueries();
            assertThat(first.pruneCalls).isZero();
            tracker.pruneExpiredQueries();
            assertThat(first.pruneCalls).isEqualTo(1);
            assertThat(second.pruneCalls).isZero();
            assertThat(tracker.getExpiredQueriesCount()).isEqualTo(2);
            assertThat(tracker.getPrunedQueriesCount()).isEqualTo(1);
        }
    }

    @Test
    public void testPruningFailureDoesNotBlockLaterExpiry()
    {
        var retention = new CountingRetention();
        try (var executor = newSingleThreadScheduledExecutor()) {
            var tracker = new QueryTracker<TestingQuery>(config(1), executor, retention);
            TestingQuery protectedQuery = add(tracker, "protected", OLD);
            protectedQuery.failPruning = true;
            TestingQuery idle = add(tracker, "idle", OLD);

            try (var request = retention.beginRequest(protectedQuery.getQueryId(), false).orElseThrow()) {
                tracker.removeExpiredQueries();
                assertThat(protectedQuery.pruneCalls).isEqualTo(1);
                assertThat(tracker.hasQuery(protectedQuery.getQueryId())).isTrue();
                assertThat(tracker.hasQuery(idle.getQueryId())).isFalse();
                assertThat(tracker.getPrunedQueriesCount()).isZero();
            }
        }
    }

    private static QueryManagerConfig config(int maxHistory)
    {
        return new QueryManagerConfig()
                .setMinQueryExpireAge(new Duration(10, MINUTES))
                .setMaxQueryHistory(maxHistory);
    }

    private static TestingQuery add(QueryTracker<TestingQuery> tracker, String id, Instant end)
    {
        TestingQuery query = new TestingQuery(new QueryId(id), end);
        assertThat(tracker.addQuery(query)).isTrue();
        tracker.expireQuery(query.getQueryId());
        return query;
    }

    private static class CountingRetention
            extends QueryResultRetention
    {
        private final List<QueryId> checkedQueries = new ArrayList<>();

        public CountingRetention()
        {
            super(Optional.of(new Duration(10, SECONDS)), _ -> false, Instant::now);
        }

        @Override
        public boolean tryExpire(TrackedQuery query, Instant minimumEndTime, BooleanSupplier removal)
        {
            checkedQueries.add(query.getQueryId());
            return super.tryExpire(query, minimumEndTime, removal);
        }
    }

    private static class TestingQuery
            implements TrackedQuery
    {
        private final QueryId queryId;
        private final Instant end;
        private final Session session = testSessionBuilder().build();
        private int pruneCalls;
        private boolean failPruning;

        public TestingQuery(QueryId queryId, Instant end)
        {
            this.queryId = queryId;
            this.end = end;
        }

        @Override
        public QueryId getQueryId()
        {
            return queryId;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public Session getSession()
        {
            return session;
        }

        @Override
        public Instant getCreateTime()
        {
            return end;
        }

        @Override
        public Optional<Instant> getExecutionStartTime()
        {
            return Optional.of(end);
        }

        @Override
        public Optional<Duration> getPlanningTime()
        {
            return Optional.empty();
        }

        @Override
        public Instant getLastHeartbeat()
        {
            return end;
        }

        @Override
        public Optional<Instant> getEndTime()
        {
            return Optional.of(end);
        }

        @Override
        public void fail(Throwable cause)
        {
            throw new AssertionError(cause);
        }

        @Override
        public void pruneInfo()
        {
            pruneCalls++;
            if (failPruning) {
                throw new IllegalStateException("Pruning failed");
            }
        }

        @Override
        public boolean isInfoPruned()
        {
            return pruneCalls > 0 && !failPruning;
        }
    }
}
