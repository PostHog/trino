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

import io.trino.hogql.compiler.catalog.BoundedAsyncHogQlSemanticCatalogSnapshotCache.SnapshotLoader;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException.Failure;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBoundedAsyncHogQlSemanticCatalogSnapshotCache
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = catalog("ducklake");
    private static final Duration REFRESH_AFTER = Duration.ofNanos(10);
    private static final Duration EXPIRE_AFTER = Duration.ofNanos(20);
    private static final Duration FAILURE_BACKOFF = Duration.ofNanos(100);

    @Test
    public void testOutOfOrderRefreshCannotPublishAfterInvalidation()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        CompletableFuture<HogQlSemanticCatalogSnapshot> oldLoad = loader.expect(CATALOG);
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);

        CompletionStage<HogQlSemanticCatalogSnapshot> invalidatedRefresh = cache.prewarm(CATALOG);
        cache.invalidate(CATALOG);

        CompletableFuture<HogQlSemanticCatalogSnapshot> newLoad = loader.expect(CATALOG);
        CompletionStage<HogQlSemanticCatalogSnapshot> currentRefresh = cache.prewarm(CATALOG);
        newLoad.complete(snapshot(CATALOG, 2));
        oldLoad.complete(snapshot(CATALOG, 1));

        assertThat(currentRefresh.toCompletableFuture()).isCompletedWithValueMatching(snapshot -> snapshot.generation() == 2);
        assertThat(invalidatedRefresh.toCompletableFuture()).isCompletedExceptionally();
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
    }

    @Test
    public void testRejectsInvalidRefreshWithoutReplacingLastKnownGood()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 2));

        CompletableFuture<HogQlSemanticCatalogSnapshot> olderLoad = loader.expect(CATALOG);
        CompletionStage<HogQlSemanticCatalogSnapshot> olderRefresh = cache.prewarm(CATALOG);
        olderLoad.complete(snapshot(CATALOG, 1));

        assertRefreshFailure(olderRefresh, Failure.GENERATION_MISMATCH);
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);

        PhysicalIdentifier otherCatalog = catalog("other");
        CompletableFuture<HogQlSemanticCatalogSnapshot> mismatchedLoad = loader.expect(CATALOG);
        CompletionStage<HogQlSemanticCatalogSnapshot> mismatchedRefresh = cache.prewarm(CATALOG);
        mismatchedLoad.complete(snapshot(otherCatalog, 3));

        assertRefreshFailure(mismatchedRefresh, Failure.CATALOG_MISMATCH);
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
    }

    @Test
    public void testServesStaleSnapshotWhileRefreshingAndFailsClosedAfterExpiry()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));

        ticker.set(REFRESH_AFTER.toNanos() - 1);
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        assertThat(loader.loadCount(CATALOG)).isEqualTo(1);

        CompletableFuture<HogQlSemanticCatalogSnapshot> refresh = loader.expect(CATALOG);
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        assertThat(loader.loadCount(CATALOG)).isEqualTo(2);

        ticker.set(EXPIRE_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).isEmpty();
        assertThat(loader.loadCount(CATALOG)).isEqualTo(2);

        refresh.complete(snapshot(CATALOG, 2));
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
    }

    @Test
    public void testRefreshAgeSurvivesTickerWraparound()
    {
        AtomicLong ticker = new AtomicLong(Long.MAX_VALUE - 5);
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));

        loader.expect(CATALOG);
        ticker.set(Long.MIN_VALUE + 4);
        assertThat(cache.currentSnapshot(CATALOG)).isPresent();
        assertThat(loader.loadCount(CATALOG)).isEqualTo(2);
    }

    @Test
    public void testRefreshFailurePreservesSnapshotOnlyUntilExpiryAndBacksOff()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));

        CompletableFuture<HogQlSemanticCatalogSnapshot> failedLoad = loader.expect(CATALOG);
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).isPresent();
        failedLoad.completeExceptionally(new IllegalStateException("metadata unavailable"));

        ticker.set(EXPIRE_AFTER.toNanos() - 1);
        assertThat(cache.currentSnapshot(CATALOG)).isPresent();
        assertThat(loader.loadCount(CATALOG)).isEqualTo(2);

        ticker.set(EXPIRE_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).isEmpty();
        assertThat(loader.loadCount(CATALOG)).isEqualTo(2);

        CompletableFuture<HogQlSemanticCatalogSnapshot> recoveredLoad = loader.expect(CATALOG);
        ticker.set(REFRESH_AFTER.plus(FAILURE_BACKOFF).toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).isEmpty();
        assertThat(loader.loadCount(CATALOG)).isEqualTo(3);
        recoveredLoad.complete(snapshot(CATALOG, 2));
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
    }

    @Test
    public void testCatalogEntriesAreIsolatedAndLeastRecentlyUsedEntryIsEvicted()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        PhysicalIdentifier alpha = catalog("alpha");
        PhysicalIdentifier beta = catalog("beta");
        PhysicalIdentifier gamma = catalog("gamma");

        completePrewarm(cache, loader, snapshot(alpha, 1));
        completePrewarm(cache, loader, snapshot(beta, 2));
        assertThat(cache.currentSnapshot(alpha)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        completePrewarm(cache, loader, snapshot(gamma, 3));

        assertThat(cache.currentSnapshot(alpha)).isPresent();
        assertThat(cache.currentSnapshot(gamma)).isPresent();
        loader.expect(beta);
        assertThat(cache.currentSnapshot(beta)).isEmpty();
        assertThat(loader.loadCount(beta)).isEqualTo(2);
    }

    @Test
    public void testConcurrentRequestsShareOneRefresh()
            throws Exception
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        CompletableFuture<HogQlSemanticCatalogSnapshot> load = loader.expect(CATALOG);
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        int workers = 4;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService callers = Executors.newFixedThreadPool(workers)) {
            List<CompletableFuture<CompletionStage<HogQlSemanticCatalogSnapshot>>> requests = java.util.stream.IntStream.range(0, workers)
                    .mapToObj(_ -> CompletableFuture.supplyAsync(() -> {
                        ready.countDown();
                        await(start);
                        return cache.prewarm(CATALOG);
                    }, callers))
                    .toList();
            assertThat(ready.await(10, SECONDS)).isTrue();
            start.countDown();
            List<CompletionStage<HogQlSemanticCatalogSnapshot>> refreshes = requests.stream()
                    .map(CompletableFuture::join)
                    .toList();

            assertThat(loader.loadCount(CATALOG)).isEqualTo(1);
            assertThat(refreshes).allMatch(refresh -> !refresh.toCompletableFuture().isDone());

            load.complete(snapshot(CATALOG, 1));
            assertThat(refreshes).allSatisfy(refresh -> assertThat(refresh.toCompletableFuture())
                    .isCompletedWithValueMatching(snapshot -> snapshot.generation() == 1));
        }
    }

    private static BoundedAsyncHogQlSemanticCatalogSnapshotCache cache(int maximumEntries, AtomicLong ticker, SnapshotLoader loader)
    {
        return new BoundedAsyncHogQlSemanticCatalogSnapshotCache(
                maximumEntries,
                REFRESH_AFTER,
                EXPIRE_AFTER,
                FAILURE_BACKOFF,
                ticker::get,
                Runnable::run,
                loader);
    }

    private static void completePrewarm(
            BoundedAsyncHogQlSemanticCatalogSnapshotCache cache,
            ControlledLoader loader,
            HogQlSemanticCatalogSnapshot snapshot)
    {
        CompletableFuture<HogQlSemanticCatalogSnapshot> load = loader.expect(snapshot.catalog());
        CompletionStage<HogQlSemanticCatalogSnapshot> refresh = cache.prewarm(snapshot.catalog());
        load.complete(snapshot);
        assertThat(refresh.toCompletableFuture()).isCompletedWithValue(snapshot);
    }

    private static void assertRefreshFailure(CompletionStage<HogQlSemanticCatalogSnapshot> refresh, Failure failure)
    {
        assertThatThrownBy(refresh.toCompletableFuture()::join)
                .cause()
                .isInstanceOfSatisfying(HogQlSemanticCatalogException.class, exception -> assertThat(exception.failure()).isEqualTo(failure));
    }

    private static void await(CountDownLatch latch)
    {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static PhysicalIdentifier catalog(String name)
    {
        return new PhysicalIdentifier(name, false);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(PhysicalIdentifier catalog, long generation)
    {
        return new HogQlSemanticCatalogSnapshot(
                2,
                LANGUAGE_VERSION,
                catalog,
                generation,
                List.of(new LogicalTableDefinition(
                        "events",
                        new PhysicalQualifiedName(catalog, catalog("default"), catalog("events")),
                        List.of(new LogicalFieldDefinition(
                                "id",
                                catalog("id"),
                                "varchar",
                                LogicalType.STRING,
                                false,
                                true)),
                        List.of(),
                        List.of())));
    }

    private static final class ControlledLoader
            implements SnapshotLoader
    {
        private final Map<PhysicalIdentifier, ArrayDeque<CompletableFuture<HogQlSemanticCatalogSnapshot>>> loads = new HashMap<>();
        private final Map<PhysicalIdentifier, AtomicInteger> loadCounts = new HashMap<>();

        public CompletableFuture<HogQlSemanticCatalogSnapshot> expect(PhysicalIdentifier catalog)
        {
            CompletableFuture<HogQlSemanticCatalogSnapshot> load = new CompletableFuture<>();
            loads.computeIfAbsent(catalog, _ -> new ArrayDeque<>()).add(load);
            return load;
        }

        @Override
        public CompletionStage<HogQlSemanticCatalogSnapshot> load(PhysicalIdentifier catalog)
        {
            loadCounts.computeIfAbsent(catalog, _ -> new AtomicInteger()).incrementAndGet();
            return Optional.ofNullable(loads.get(catalog))
                    .map(ArrayDeque::poll)
                    .orElseThrow(() -> new IllegalStateException("unexpected load for catalog " + catalog.value()));
        }

        public int loadCount(PhysicalIdentifier catalog)
        {
            return Optional.ofNullable(loadCounts.get(catalog))
                    .map(AtomicInteger::get)
                    .orElse(0);
        }
    }
}
