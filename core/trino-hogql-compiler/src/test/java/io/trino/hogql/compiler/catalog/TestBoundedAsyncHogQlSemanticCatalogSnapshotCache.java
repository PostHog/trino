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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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

        CompletableFuture<HogQlSemanticCatalogSnapshot> mutatedLoad = loader.expect(CATALOG);
        CompletionStage<HogQlSemanticCatalogSnapshot> mutatedRefresh = cache.prewarm(CATALOG);
        mutatedLoad.complete(snapshot(CATALOG, 2, "changed_id"));

        assertRefreshFailure(mutatedRefresh, Failure.GENERATION_MISMATCH);
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(snapshot -> snapshot.logicalTables().getFirst().fields().getFirst().name()).isEqualTo("id");
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

    @Test
    public void testPinnedSnapshotRemainsStableAcrossRefreshPublication()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));
        HogQlSemanticCatalogSnapshotProvider provider = HogQlSemanticCatalogSnapshotProvider.fromCache(cache);

        CompletableFuture<HogQlSemanticCatalogSnapshot> load = loader.expect(CATALOG);
        ticker.set(REFRESH_AFTER.toNanos());
        PinnedSnapshot pinned = provider.pin(new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.empty()));
        load.complete(snapshot(CATALOG, 2));

        assertThat(pinned.generation()).isEqualTo(1);
        assertThat(pinned.snapshot().generation()).isEqualTo(1);
        assertThat(provider.pin(new PinRequest(CATALOG, LANGUAGE_VERSION, OptionalLong.of(2))).generation()).isEqualTo(2);
    }

    @Test
    public void testColdExactGenerationLoadsPinnedEntry()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        CompletableFuture<HogQlSemanticCatalogSnapshot> exactLoad = loader.expect(CATALOG, OptionalLong.of(7));
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(3, ticker, loader);

        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(7))).isEmpty();
        assertThat(loader.loadCount(CATALOG, OptionalLong.of(7))).isEqualTo(1);
        assertThat(loader.loadCount(CATALOG)).isZero();

        exactLoad.complete(snapshot(CATALOG, 7));
        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(7)))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(7L);
    }

    @Test
    public void testLatestCanAdvancePastCachedHistoricalGeneration()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(3, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));

        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(1)))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(1L);
        assertThat(loader.loadCount(CATALOG, OptionalLong.of(1))).isZero();

        CompletableFuture<HogQlSemanticCatalogSnapshot> latestRefresh = loader.expect(CATALOG);
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).isPresent();
        latestRefresh.complete(snapshot(CATALOG, 2));

        assertThat(cache.currentSnapshot(CATALOG))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(2L);
        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(1)))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(1L);
    }

    @Test
    public void testExactGenerationChurnPreservesLatestAndCancelsEvictedLoad()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        completePrewarm(cache, loader, snapshot(CATALOG, 1));

        CompletableFuture<HogQlSemanticCatalogSnapshot> generationSevenLoad = loader.expect(CATALOG, OptionalLong.of(7));
        CompletionStage<HogQlSemanticCatalogSnapshot> generationSeven = cache.prewarm(CATALOG, OptionalLong.of(7));

        CompletableFuture<HogQlSemanticCatalogSnapshot> generationEightLoad = loader.expect(CATALOG, OptionalLong.of(8));
        CompletionStage<HogQlSemanticCatalogSnapshot> generationEight = cache.prewarm(CATALOG, OptionalLong.of(8));

        assertThat(generationSevenLoad).isCancelled();
        assertThat(generationSeven.toCompletableFuture()).isCompletedExceptionally();
        assertThat(cache.currentSnapshot(CATALOG))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(1L);

        generationEightLoad.complete(snapshot(CATALOG, 8));
        assertThat(generationEight.toCompletableFuture()).isCompletedWithValueMatching(snapshot -> snapshot.generation() == 8);
    }

    @Test
    public void testExactGenerationOutageFailsClosedWithoutLatestFallback()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(3, ticker, loader);

        CompletableFuture<HogQlSemanticCatalogSnapshot> exactLoad = loader.expect(CATALOG, OptionalLong.of(7));
        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(7))).isEmpty();
        exactLoad.complete(snapshot(CATALOG, 7));

        ticker.set(11);
        completePrewarm(cache, loader, snapshot(CATALOG, 8));

        CompletableFuture<HogQlSemanticCatalogSnapshot> failedReload = loader.expect(CATALOG, OptionalLong.of(7));
        ticker.set(EXPIRE_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(7))).isEmpty();
        failedReload.completeExceptionally(new IllegalStateException("metadata unavailable"));

        assertThat(cache.currentSnapshot(CATALOG, OptionalLong.of(7))).isEmpty();
        assertThat(loader.loadCount(CATALOG, OptionalLong.of(7))).isEqualTo(2);
        assertThat(cache.currentSnapshot(CATALOG))
                .get()
                .extracting(HogQlSemanticCatalogSnapshot::generation)
                .isEqualTo(8L);
    }

    @Test
    public void testOrdinaryAndDelimitedCatalogIdentifiersHaveIsolatedEntries()
    {
        AtomicLong ticker = new AtomicLong();
        ControlledLoader loader = new ControlledLoader();
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(2, ticker, loader);
        PhysicalIdentifier ordinary = new PhysicalIdentifier("sales", false);
        PhysicalIdentifier delimited = new PhysicalIdentifier("sales", true);

        completePrewarm(cache, loader, snapshot(ordinary, 1));
        completePrewarm(cache, loader, snapshot(delimited, 2));

        assertThat(cache.currentSnapshot(ordinary)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        assertThat(cache.currentSnapshot(delimited)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
        cache.invalidate(ordinary);
        assertThat(cache.currentSnapshot(delimited)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(2L);
    }

    @Test
    public void testMalformedSchemaV2RefreshRetainsLastKnownGoodSnapshot()
    {
        AtomicLong ticker = new AtomicLong();
        ArrayDeque<CompletableFuture<byte[]>> payloads = new ArrayDeque<>();
        HogQlSemanticCatalogSnapshotLoader jsonLoader = HogQlSemanticCatalogSnapshotLoader.fromJsonTransport(
                _ -> payloads.remove(),
                new HogQlSemanticCatalogSnapshotJsonDecoder());
        BoundedAsyncHogQlSemanticCatalogSnapshotCache cache = cache(
                2,
                ticker,
                (catalog, expectedGeneration) -> jsonLoader.load(expectedGeneration.isPresent()
                        ? LoadRequest.pinned(catalog, LANGUAGE_VERSION, expectedGeneration.orElseThrow())
                        : LoadRequest.latest(catalog, LANGUAGE_VERSION)));

        CompletableFuture<byte[]> initialPayload = new CompletableFuture<>();
        payloads.add(initialPayload);
        CompletionStage<HogQlSemanticCatalogSnapshot> initialRefresh = cache.prewarm(CATALOG);
        initialPayload.complete(snapshotJson(1).getBytes(StandardCharsets.UTF_8));
        assertThat(initialRefresh.toCompletableFuture()).isCompletedWithValueMatching(snapshot -> snapshot.generation() == 1);

        CompletableFuture<byte[]> malformedPayload = new CompletableFuture<>();
        payloads.add(malformedPayload);
        ticker.set(REFRESH_AFTER.toNanos());
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        CompletionStage<HogQlSemanticCatalogSnapshot> malformedRefresh = cache.prewarm(CATALOG);
        malformedPayload.complete(snapshotJson(2)
                .replace("\"logicalTables\": []", "\"logicalTables\": {}")
                .getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(malformedRefresh.toCompletableFuture()::join)
                .cause()
                .isInstanceOf(HogQlSemanticCatalogSnapshotJsonDecoder.DecodeException.class);
        assertThat(cache.currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
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
        return snapshot(catalog, generation, "id");
    }

    private static HogQlSemanticCatalogSnapshot snapshot(PhysicalIdentifier catalog, long generation, String fieldName)
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
                                fieldName,
                                catalog(fieldName),
                                "varchar",
                                LogicalType.STRING,
                                false,
                                true)),
                        List.of(),
                        List.of())));
    }

    private static String snapshotJson(long generation)
    {
        return """
               {
                 "protocolVersion": 1,
                 "schemaVersion": 2,
                 "languageVersion": "1.0.0",
                 "catalog": {"value": "ducklake", "delimited": false},
                 "generation": %s,
                 "logicalTables": [],
                 "expressionFields": [],
                 "virtualTables": [],
                 "savedQueries": [],
                 "materializedViews": [],
                 "functions": [],
                 "modifierDefaults": []
               }
               """.formatted(generation);
    }

    private static final class ControlledLoader
            implements SnapshotLoader
    {
        private final Map<LoadKey, ArrayDeque<CompletableFuture<HogQlSemanticCatalogSnapshot>>> loads = new HashMap<>();
        private final Map<LoadKey, AtomicInteger> loadCounts = new HashMap<>();

        public CompletableFuture<HogQlSemanticCatalogSnapshot> expect(PhysicalIdentifier catalog)
        {
            return expect(catalog, OptionalLong.empty());
        }

        public CompletableFuture<HogQlSemanticCatalogSnapshot> expect(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
        {
            CompletableFuture<HogQlSemanticCatalogSnapshot> load = new CompletableFuture<>();
            loads.computeIfAbsent(new LoadKey(catalog, expectedGeneration), _ -> new ArrayDeque<>()).add(load);
            return load;
        }

        @Override
        public CompletionStage<HogQlSemanticCatalogSnapshot> load(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
        {
            LoadKey key = new LoadKey(catalog, expectedGeneration);
            loadCounts.computeIfAbsent(key, _ -> new AtomicInteger()).incrementAndGet();
            return Optional.ofNullable(loads.get(key))
                    .map(ArrayDeque::poll)
                    .orElseThrow(() -> new IllegalStateException("unexpected load for catalog " + catalog.value() + " and generation " + expectedGeneration));
        }

        public int loadCount(PhysicalIdentifier catalog)
        {
            return loadCount(catalog, OptionalLong.empty());
        }

        public int loadCount(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
        {
            return Optional.ofNullable(loadCounts.get(new LoadKey(catalog, expectedGeneration)))
                    .map(AtomicInteger::get)
                    .orElse(0);
        }
    }

    private record LoadKey(PhysicalIdentifier catalog, OptionalLong expectedGeneration) {}
}
