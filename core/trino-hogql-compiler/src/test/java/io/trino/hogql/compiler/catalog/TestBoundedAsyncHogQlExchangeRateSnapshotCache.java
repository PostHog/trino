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
package io.trino.hogql.compiler.catalog;

import io.trino.hogql.compiler.catalog.BoundedAsyncHogQlExchangeRateSnapshotCache.SnapshotLoader;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot.ExchangeRate;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBoundedAsyncHogQlExchangeRateSnapshotCache
{
    private static final Duration REFRESH_AFTER = Duration.ofNanos(10);
    private static final Duration EXPIRE_AFTER = Duration.ofNanos(20);
    private static final Duration FAILURE_BACKOFF = Duration.ofNanos(100);

    @Test
    public void testLatestAndExactReadsShareLoadsAndRemainGenerationPinned()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlExchangeRateSnapshotCache cache = cache(3, ticker, loader);

        CompletableFuture<HogQlExchangeRateSnapshot> initialLoad = loader.expect(OptionalLong.empty());
        CompletionStage<HogQlExchangeRateSnapshot> first = cache.prewarm();
        CompletionStage<HogQlExchangeRateSnapshot> shared = cache.prewarm();
        assertThat(loader.loadCount(OptionalLong.empty())).isEqualTo(1);
        initialLoad.complete(snapshot(1));
        assertThat(first.toCompletableFuture()).isCompletedWithValue(snapshot(1));
        assertThat(shared.toCompletableFuture()).isCompletedWithValue(snapshot(1));

        assertThat(cache.currentSnapshot(OptionalLong.of(1))).contains(snapshot(1));
        assertThat(loader.loadCount(OptionalLong.of(1))).isZero();

        CompletableFuture<HogQlExchangeRateSnapshot> latestRefresh = loader.expect(OptionalLong.empty());
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot()).contains(snapshot(1));
        latestRefresh.complete(snapshot(2));

        assertThat(cache.currentSnapshot()).contains(snapshot(2));
        assertThat(cache.currentSnapshot(OptionalLong.of(1))).contains(snapshot(1));
    }

    @Test
    public void testRejectsGenerationRegressionAndChangedSameGeneration()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlExchangeRateSnapshotCache cache = cache(3, ticker, loader);
        completePrewarm(cache, loader, OptionalLong.empty(), snapshot(2));

        CompletableFuture<HogQlExchangeRateSnapshot> regressedLoad = loader.expect(OptionalLong.empty());
        CompletionStage<HogQlExchangeRateSnapshot> regressed = cache.prewarm();
        regressedLoad.complete(snapshot(1));
        assertFailure(regressed, Failure.GENERATION_MISMATCH);
        assertThat(cache.currentSnapshot()).contains(snapshot(2));

        CompletableFuture<HogQlExchangeRateSnapshot> changedLoad = loader.expect(OptionalLong.empty());
        CompletionStage<HogQlExchangeRateSnapshot> changed = cache.prewarm();
        changedLoad.complete(snapshot(2, "9100000000"));
        assertFailure(changed, Failure.GENERATION_MISMATCH);
        assertThat(cache.currentSnapshot()).contains(snapshot(2));
    }

    @Test
    public void testExactGenerationEvictionCancelsLoadAndNeverFallsBackToLatest()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlExchangeRateSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, OptionalLong.empty(), snapshot(1));

        CompletableFuture<HogQlExchangeRateSnapshot> generationSevenLoad = loader.expect(OptionalLong.of(7));
        CompletionStage<HogQlExchangeRateSnapshot> generationSeven = cache.prewarm(OptionalLong.of(7));
        CompletableFuture<HogQlExchangeRateSnapshot> generationEightLoad = loader.expect(OptionalLong.of(8));
        CompletionStage<HogQlExchangeRateSnapshot> generationEight = cache.prewarm(OptionalLong.of(8));

        assertThat(generationSevenLoad).isCancelled();
        assertThat(generationSeven.toCompletableFuture()).isCompletedExceptionally();
        assertThat(cache.currentSnapshot()).contains(snapshot(1));
        generationEightLoad.complete(snapshot(8));
        assertThat(generationEight.toCompletableFuture()).isCompletedWithValue(snapshot(8));

        cache.invalidate();
        CompletableFuture<HogQlExchangeRateSnapshot> latestLoad = loader.expect(OptionalLong.empty());
        assertThat(cache.currentSnapshot()).isEmpty();
        latestLoad.complete(snapshot(9));
        CompletableFuture<HogQlExchangeRateSnapshot> missingExact = loader.expect(OptionalLong.of(7));
        assertThat(cache.currentSnapshot(OptionalLong.of(7))).isEmpty();
        missingExact.completeExceptionally(new IllegalStateException("metadata unavailable"));
        assertThat(cache.currentSnapshot(OptionalLong.of(7))).isEmpty();
        assertThat(cache.currentSnapshot()).contains(snapshot(9));
    }

    @Test
    public void testRefreshFailureServesLastGoodOnlyUntilExpiryAndBacksOff()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlExchangeRateSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, OptionalLong.empty(), snapshot(1));

        CompletableFuture<HogQlExchangeRateSnapshot> failedLoad = loader.expect(OptionalLong.empty());
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot()).contains(snapshot(1));
        failedLoad.completeExceptionally(new IllegalStateException("metadata unavailable"));

        ticker.set(EXPIRE_AFTER.toNanos());
        assertThat(cache.currentSnapshot()).isEmpty();
        assertThat(loader.loadCount(OptionalLong.empty())).isEqualTo(2);

        CompletableFuture<HogQlExchangeRateSnapshot> recoveredLoad = loader.expect(OptionalLong.empty());
        ticker.set(REFRESH_AFTER.plus(FAILURE_BACKOFF).toNanos());
        assertThat(cache.currentSnapshot()).isEmpty();
        recoveredLoad.complete(snapshot(2));
        assertThat(cache.currentSnapshot()).contains(snapshot(2));
    }

    private static BoundedAsyncHogQlExchangeRateSnapshotCache cache(int maximumEntries, AtomicLong ticker, SnapshotLoader loader)
    {
        return new BoundedAsyncHogQlExchangeRateSnapshotCache(
                maximumEntries,
                REFRESH_AFTER,
                EXPIRE_AFTER,
                FAILURE_BACKOFF,
                ticker::get,
                Runnable::run,
                loader);
    }

    private static void completePrewarm(
            BoundedAsyncHogQlExchangeRateSnapshotCache cache,
            ControlledLoader loader,
            OptionalLong expectedGeneration,
            HogQlExchangeRateSnapshot snapshot)
    {
        CompletableFuture<HogQlExchangeRateSnapshot> load = loader.expect(expectedGeneration);
        CompletionStage<HogQlExchangeRateSnapshot> refresh = cache.prewarm(expectedGeneration);
        load.complete(snapshot);
        assertThat(refresh.toCompletableFuture()).isCompletedWithValue(snapshot);
    }

    private static void assertFailure(CompletionStage<HogQlExchangeRateSnapshot> refresh, Failure failure)
    {
        assertThatThrownBy(refresh.toCompletableFuture()::join)
                .cause()
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> assertThat(exception.failure()).isEqualTo(failure));
    }

    private static HogQlExchangeRateSnapshot snapshot(long generation)
    {
        return snapshot(generation, "9049000000");
    }

    private static HogQlExchangeRateSnapshot snapshot(long generation, String eurRate)
    {
        return new HogQlExchangeRateSnapshot(
                1,
                1,
                generation,
                "USD",
                10,
                List.of(
                        new ExchangeRate("EUR", "2024-01-01", eurRate),
                        new ExchangeRate("USD", "1970-01-01", "10000000000")));
    }

    private static final class ControlledLoader
            implements SnapshotLoader
    {
        private final Map<OptionalLong, ArrayDeque<CompletableFuture<HogQlExchangeRateSnapshot>>> expected = new HashMap<>();
        private final Map<OptionalLong, Integer> loadCounts = new HashMap<>();

        public CompletableFuture<HogQlExchangeRateSnapshot> expect(OptionalLong generation)
        {
            CompletableFuture<HogQlExchangeRateSnapshot> future = new CompletableFuture<>();
            expected.computeIfAbsent(generation, _ -> new ArrayDeque<>()).add(future);
            return future;
        }

        public int loadCount(OptionalLong generation)
        {
            return loadCounts.getOrDefault(generation, 0);
        }

        @Override
        public CompletionStage<HogQlExchangeRateSnapshot> load(OptionalLong expectedGeneration)
        {
            loadCounts.merge(expectedGeneration, 1, Integer::sum);
            ArrayDeque<CompletableFuture<HogQlExchangeRateSnapshot>> queue = expected.get(expectedGeneration);
            if (queue == null || queue.isEmpty()) {
                throw new AssertionError("unexpected exchange-rate load: " + expectedGeneration);
            }
            return queue.remove();
        }
    }
}
