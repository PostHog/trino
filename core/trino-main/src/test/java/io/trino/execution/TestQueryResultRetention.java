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
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryResultRetention
{
    private static final Instant START = Instant.parse("2020-01-01T00:00:00Z");
    private final AtomicReference<Instant> now = new AtomicReference<>(START);
    private final Set<TransactionId> transactions = new HashSet<>();
    private final QueryResultRetention retention = new QueryResultRetention(Optional.of(new Duration(10, SECONDS)), transactions::contains, now::get);
    private final TestingQuery query = new TestingQuery();

    @Test
    public void testDisabledPolicyDoesNotRetainEntries()
    {
        QueryResultRetention disabled = new QueryResultRetention(Optional.empty(), _ -> true, now::get);
        assertThat(disabled.register(query.getQueryId(), () -> true)).isTrue();
        try (var request = disabled.beginRequest(query.getQueryId(), false).orElseThrow()) {
            request.accepted();
        }
        assertThat(disabled.entryCount()).isZero();
        assertThat(disabled.tryExpire(query, START.plusSeconds(100), () -> {
            throw new AssertionError("Disabled retention must not remove queries");
        })).isFalse();
    }

    @Test
    public void testSlidingWindowAndTrackerCleanup()
    {
        register();
        now.set(START.plusSeconds(9));
        try (var request = retention.beginRequest(query.getQueryId(), false).orElseThrow()) {
            request.accepted();
        }
        now.set(START.plusSeconds(11));
        assertThat(expire()).isFalse();
        now.set(START.plusSeconds(20));
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isZero();
    }

    @Test
    public void testCompletionStartsWindowAfterEarlyRequest()
    {
        register();
        try (var request = retention.beginRequest(query.getQueryId(), false).orElseThrow()) {
            request.accepted();
        }
        query.end = START.plusSeconds(100);
        now.set(START.plusSeconds(101));
        assertThat(expire()).isFalse();
        now.set(START.plusSeconds(111));
        assertThat(expire()).isTrue();
    }

    @Test
    public void testRunningQueryAndMinimumAgeRemainProtected()
    {
        register();
        now.set(START.plusSeconds(100));
        query.done = false;
        assertThat(expire()).isFalse();
        query.done = true;
        assertThat(retention.tryExpire(query, START.minusSeconds(1), () -> true)).isFalse();
        assertThat(expire()).isTrue();
    }

    @Test
    public void testResponseSerializationAndIdempotentCompletion()
    {
        register();
        var request = retention.beginRequest(query.getQueryId(), false).orElseThrow();
        request.accepted();
        now.set(START.plusSeconds(100));
        assertThat(expire()).isFalse();
        request.close();
        request.close();
        assertThat(expire()).isFalse();
        now.set(START.plusSeconds(111));
        assertThat(expire()).isTrue();
        request.accepted();
        request.close();
        assertThat(retention.entryCount()).isZero();
    }

    @Test
    public void testRejectedRequestDoesNotRenewWindow()
    {
        register();
        now.set(START.plusSeconds(9));
        retention.beginRequest(query.getQueryId(), false).orElseThrow().close();
        now.set(START.plusSeconds(11));
        assertThat(expire()).isTrue();
    }

    @Test
    public void testExpiredFenceCoversBothTrackersAndLateRegistration()
    {
        register();
        register();
        now.set(START.plusSeconds(11));
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isEqualTo(1);
        assertThat(retention.beginRequest(query.getQueryId(), false)).isEmpty();
        assertThat(retention.register(query.getQueryId(), () -> true)).isFalse();
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isZero();
        assertThat(retention.beginRequest(query.getQueryId(), false)).isEmpty();
    }

    @Test
    public void testQueuedRequestCanRegisterBothTrackers()
    {
        var request = retention.beginRequest(query.getQueryId(), true).orElseThrow();
        register();
        register();
        now.set(START.plusSeconds(100));
        assertThat(expire()).isFalse();
        request.accepted();
        request.close();
        now.set(START.plusSeconds(111));
        assertThat(expire()).isTrue();
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isZero();
    }

    @Test
    public void testUnsubmittedAndDuplicateRegistrationDoNotLeak()
    {
        retention.beginRequest(query.getQueryId(), true).orElseThrow().close();
        assertThat(retention.entryCount()).isZero();
        assertThat(retention.register(query.getQueryId(), () -> false)).isFalse();
        assertThat(retention.entryCount()).isZero();
        register();
        assertThat(retention.register(query.getQueryId(), () -> false)).isFalse();
        now.set(START.plusSeconds(11));
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isZero();
    }

    @Test
    public void testOpenTransactionRetainsResultsUntilRemoved()
    {
        TransactionId transaction = TransactionId.create();
        transactions.add(transaction);
        query.session = testSessionBuilder().setTransactionId(transaction).build();
        register();
        now.set(START.plusSeconds(100));
        assertThat(expire()).isFalse();
        transactions.remove(transaction);
        assertThat(expire()).isTrue();
    }

    @Test
    public void testStartTransactionRetainsResultsWithoutSessionTransaction()
    {
        TransactionId transaction = TransactionId.create();
        transactions.add(transaction);
        query.startedTransaction = Optional.of(transaction);
        assertThat(query.getSession().getTransactionId()).isEmpty();
        register();
        now.set(START.plusSeconds(100));
        assertThat(expire()).isFalse();
        transactions.remove(transaction);
        assertThat(expire()).isTrue();
    }

    @Test
    public void testExpiryWinningConcurrentRequestCannotResurrectResults()
            throws Exception
    {
        register();
        register();
        now.set(START.plusSeconds(11));
        CountDownLatch removalStarted = new CountDownLatch(1);
        CountDownLatch finishRemoval = new CountDownLatch(1);
        CountDownLatch requestStarted = new CountDownLatch(1);
        try (var executor = newVirtualThreadPerTaskExecutor()) {
            var expiration = executor.submit(() -> retention.tryExpire(query, now.get(), () -> {
                removalStarted.countDown();
                await(finishRemoval);
                return true;
            }));
            try {
                await(removalStarted);
                var request = executor.submit(() -> {
                    requestStarted.countDown();
                    return retention.beginRequest(query.getQueryId(), false);
                });
                await(requestStarted);
                finishRemoval.countDown();
                assertThat(expiration.get(10, SECONDS)).isTrue();
                assertThat(request.get(10, SECONDS)).isEmpty();
            }
            finally {
                finishRemoval.countDown();
            }
        }
        assertThat(expire()).isTrue();
        assertThat(retention.entryCount()).isZero();
    }

    @Test
    public void testRequestWinningConcurrentExpiryProtectsResponse()
            throws Exception
    {
        register();
        now.set(START.plusSeconds(11));
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch finishRequest = new CountDownLatch(1);
        try (var executor = newVirtualThreadPerTaskExecutor()) {
            var request = executor.submit(() -> {
                try (var access = retention.beginRequest(query.getQueryId(), false).orElseThrow()) {
                    requestStarted.countDown();
                    await(finishRequest);
                    access.accepted();
                }
            });
            try {
                await(requestStarted);
                assertThat(expire()).isFalse();
            }
            finally {
                finishRequest.countDown();
            }
            request.get(10, SECONDS);
        }
        assertThat(expire()).isFalse();
        now.set(START.plusSeconds(22));
        assertThat(expire()).isTrue();
    }

    @Test
    public void testNonPositiveTimeoutIsRejected()
    {
        assertThatThrownBy(() -> new QueryManagerConfig().setCompletedResultIdleTimeout(new Duration(0, SECONDS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueryManagerConfig().setCompletedResultIdleTimeout(new Duration(-1, SECONDS)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void await(CountDownLatch latch)
    {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void register()
    {
        assertThat(retention.register(query.getQueryId(), () -> true)).isTrue();
    }

    private boolean expire()
    {
        return retention.tryExpire(query, now.get(), () -> true);
    }

    private static class TestingQuery
            implements TrackedQuery
    {
        private Session session = testSessionBuilder().build();
        private Instant end = START;
        private boolean done = true;
        private Optional<TransactionId> startedTransaction = Optional.empty();

        @Override
        public QueryId getQueryId()
        {
            return new QueryId("test_query");
        }

        @Override
        public boolean isDone()
        {
            return done;
        }

        @Override
        public Session getSession()
        {
            return session;
        }

        @Override
        public Instant getCreateTime()
        {
            return START;
        }

        @Override
        public Optional<Instant> getExecutionStartTime()
        {
            return Optional.of(START);
        }

        @Override
        public Optional<Duration> getPlanningTime()
        {
            return Optional.empty();
        }

        @Override
        public Instant getLastHeartbeat()
        {
            return START;
        }

        @Override
        public Optional<Instant> getEndTime()
        {
            return Optional.of(end);
        }

        @Override
        public Optional<TransactionId> getStartedTransactionId()
        {
            return startedTransaction;
        }

        @Override
        public void fail(Throwable cause)
        {
            throw new AssertionError(cause);
        }

        @Override
        public void pruneInfo() {}

        @Override
        public boolean isInfoPruned()
        {
            return false;
        }
    }
}
