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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException.Failure;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class BoundedAsyncHogQlSemanticCatalogSnapshotCache
        implements HogQlSemanticCatalogSnapshotCache
{
    private final Object lock = new Object();
    private final int maximumEntries;
    private final long refreshAfterNanos;
    private final long expireAfterNanos;
    private final long failureBackoffNanos;
    private final LongSupplier ticker;
    private final Executor executor;
    private final SnapshotLoader loader;
    private final LinkedHashMap<CacheKey, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private final LinkedHashMap<GenerationKey, HogQlSemanticCatalogSnapshot> observedGenerations = new LinkedHashMap<>(16, 0.75f, true);

    public BoundedAsyncHogQlSemanticCatalogSnapshotCache(
            int maximumEntries,
            Duration refreshAfter,
            Duration expireAfter,
            Duration failureBackoff,
            LongSupplier ticker,
            Executor executor,
            SnapshotLoader loader)
    {
        if (maximumEntries <= 0) {
            throw new IllegalArgumentException("maximumEntries must be positive");
        }
        this.maximumEntries = maximumEntries;
        this.refreshAfterNanos = nonNegativeNanos(refreshAfter, "refreshAfter");
        this.expireAfterNanos = positiveNanos(expireAfter, "expireAfter");
        this.failureBackoffNanos = nonNegativeNanos(failureBackoff, "failureBackoff");
        if (refreshAfterNanos > expireAfterNanos) {
            throw new IllegalArgumentException("refreshAfter must not exceed expireAfter");
        }
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.loader = requireNonNull(loader, "loader is null");
    }

    @Override
    public Optional<HogQlSemanticCatalogSnapshot> currentSnapshot(PhysicalIdentifier catalog)
    {
        return currentSnapshot(catalog, OptionalLong.empty());
    }

    @Override
    public Optional<HogQlSemanticCatalogSnapshot> currentSnapshot(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(expectedGeneration, "expectedGeneration is null");
        CacheKey cacheKey = new CacheKey(catalog, expectedGeneration);
        RefreshTask refreshTask;
        Optional<HogQlSemanticCatalogSnapshot> result;
        List<RefreshCancellation> evictedRefreshes = new ArrayList<>(1);
        long now = ticker.getAsLong();
        synchronized (lock) {
            Entry entry = entries.get(cacheKey);
            if (entry == null) {
                entry = inheritedExactEntry(cacheKey, now).orElseGet(Entry::new);
                entries.put(cacheKey, entry);
            }

            HogQlSemanticCatalogSnapshot snapshot = entry.snapshot;
            long age = snapshot == null ? Long.MAX_VALUE : elapsedNanos(entry.loadedAtNanos, now);
            result = snapshot != null && age < expireAfterNanos ? Optional.of(snapshot) : Optional.empty();
            boolean refreshRequired = snapshot == null ||
                    (expectedGeneration.isEmpty() && age >= refreshAfterNanos) ||
                    (expectedGeneration.isPresent() && age >= expireAfterNanos);
            refreshTask = refreshRequired ? prepareRefresh(cacheKey, entry, now, false) : null;
            evictEntries(evictedRefreshes);
            if (!entries.containsKey(cacheKey)) {
                refreshTask = null;
            }
        }
        cancelRefreshes(evictedRefreshes, "HogQL semantic catalog cache entry was evicted");
        dispatch(refreshTask);
        return result;
    }

    public CompletionStage<HogQlSemanticCatalogSnapshot> prewarm(PhysicalIdentifier catalog)
    {
        return prewarm(catalog, OptionalLong.empty());
    }

    public CompletionStage<HogQlSemanticCatalogSnapshot> prewarm(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(expectedGeneration, "expectedGeneration is null");
        CacheKey cacheKey = new CacheKey(catalog, expectedGeneration);
        RefreshTask refreshTask;
        CompletableFuture<HogQlSemanticCatalogSnapshot> future;
        List<RefreshCancellation> evictedRefreshes = new ArrayList<>(1);
        long now = ticker.getAsLong();
        synchronized (lock) {
            Entry entry = entries.get(cacheKey);
            if (entry == null) {
                entry = inheritedExactEntry(cacheKey, now).orElseGet(Entry::new);
                entries.put(cacheKey, entry);
            }
            boolean unexpired = entry.snapshot != null && elapsedNanos(entry.loadedAtNanos, now) < expireAfterNanos;
            if (unexpired && expectedGeneration.isPresent()) {
                refreshTask = null;
                future = CompletableFuture.completedFuture(entry.snapshot);
            }
            else {
                refreshTask = prepareRefresh(cacheKey, entry, now, true);
                future = refreshTask == null ? entry.refresh : refreshTask.result();
            }
            evictEntries(evictedRefreshes);
            if (!entries.containsKey(cacheKey)) {
                refreshTask = null;
            }
        }
        cancelRefreshes(evictedRefreshes, "HogQL semantic catalog cache entry was evicted");
        dispatch(refreshTask);
        return future.minimalCompletionStage();
    }

    public void invalidate(PhysicalIdentifier catalog)
    {
        requireNonNull(catalog, "catalog is null");
        List<RefreshCancellation> refreshes = new ArrayList<>();
        synchronized (lock) {
            var iterator = entries.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<CacheKey, Entry> cacheEntry = iterator.next();
                if (cacheEntry.getKey().catalog().equals(catalog)) {
                    iterator.remove();
                    if (cacheEntry.getValue().refresh != null) {
                        refreshes.add(new RefreshCancellation(cacheEntry.getValue().refresh, cacheEntry.getValue().upstream));
                        cacheEntry.getValue().refresh = null;
                        cacheEntry.getValue().upstream = null;
                    }
                }
            }
        }
        cancelRefreshes(refreshes, "HogQL semantic catalog cache entry was invalidated");
    }

    private Optional<Entry> inheritedExactEntry(CacheKey cacheKey, long now)
    {
        if (cacheKey.expectedGeneration().isEmpty()) {
            return Optional.empty();
        }
        Entry latest = entries.get(new CacheKey(cacheKey.catalog(), OptionalLong.empty()));
        if (latest == null || latest.snapshot == null || latest.snapshot.generation() != cacheKey.expectedGeneration().orElseThrow()) {
            return Optional.empty();
        }
        if (elapsedNanos(latest.loadedAtNanos, now) >= expireAfterNanos) {
            return Optional.empty();
        }
        Entry inherited = new Entry();
        inherited.snapshot = latest.snapshot;
        inherited.loadedAtNanos = latest.loadedAtNanos;
        return Optional.of(inherited);
    }

    private RefreshTask prepareRefresh(CacheKey cacheKey, Entry entry, long now, boolean force)
    {
        if (entry.refresh != null) {
            return null;
        }
        if (!force && entry.refreshBackoffActive && elapsedNanos(entry.lastRefreshFailureAtNanos, now) < failureBackoffNanos) {
            return null;
        }
        CompletableFuture<HogQlSemanticCatalogSnapshot> result = new CompletableFuture<>();
        entry.refresh = result;
        return new RefreshTask(cacheKey, entry, result);
    }

    private void dispatch(RefreshTask refreshTask)
    {
        if (refreshTask == null) {
            return;
        }
        try {
            executor.execute(() -> load(refreshTask));
        }
        catch (RuntimeException e) {
            completeFailure(refreshTask, e);
        }
    }

    private void load(RefreshTask refreshTask)
    {
        CompletionStage<HogQlSemanticCatalogSnapshot> loaded;
        try {
            loaded = requireNonNull(loader.load(
                    refreshTask.cacheKey().catalog(),
                    refreshTask.cacheKey().expectedGeneration()), "snapshot loader returned null");
        }
        catch (RuntimeException failure) {
            completeFailure(refreshTask, failure);
            return;
        }
        catch (Error failure) {
            completeFailure(refreshTask, failure);
            throw failure;
        }
        CompletableFuture<HogQlSemanticCatalogSnapshot> upstream = loaded.toCompletableFuture();
        boolean owned;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.cacheKey());
            owned = entry == refreshTask.entry() && entry.refresh == refreshTask.result();
            if (owned) {
                entry.upstream = upstream;
            }
        }
        if (!owned) {
            upstream.cancel(true);
            return;
        }
        try {
            loaded.whenComplete((snapshot, failure) -> {
                if (failure != null) {
                    completeFailure(refreshTask, failure);
                    return;
                }
                if (snapshot == null) {
                    completeFailure(refreshTask, new NullPointerException("snapshot loader completed with null"));
                    return;
                }
                completeSuccess(refreshTask, snapshot);
            });
        }
        catch (RuntimeException failure) {
            completeFailure(refreshTask, failure);
        }
        catch (Error failure) {
            completeFailure(refreshTask, failure);
            throw failure;
        }
    }

    private void completeSuccess(RefreshTask refreshTask, HogQlSemanticCatalogSnapshot snapshot)
    {
        Throwable rejection = validateLoadedSnapshot(refreshTask, snapshot);
        if (rejection != null) {
            completeFailure(refreshTask, rejection);
            return;
        }

        boolean published = false;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.cacheKey());
            if (entry == refreshTask.entry() && entry.refresh == refreshTask.result()) {
                HogQlSemanticCatalogSnapshot observed = observedGenerations.get(new GenerationKey(snapshot.catalog(), snapshot.generation()));
                if (observed != null && !observed.equals(snapshot)) {
                    entry.refresh = null;
                    entry.upstream = null;
                    entry.lastRefreshFailureAtNanos = ticker.getAsLong();
                    entry.refreshBackoffActive = true;
                    rejection = new HogQlSemanticCatalogException(
                            Failure.GENERATION_MISMATCH,
                            "HogQL semantic catalog generation content changed");
                }
                else if (refreshTask.cacheKey().expectedGeneration().isEmpty() && entry.snapshot != null && snapshot.generation() < entry.snapshot.generation()) {
                    entry.refresh = null;
                    entry.upstream = null;
                    entry.lastRefreshFailureAtNanos = ticker.getAsLong();
                    entry.refreshBackoffActive = true;
                    rejection = new HogQlSemanticCatalogException(
                            Failure.GENERATION_MISMATCH,
                            format("HogQL semantic catalog generation regressed from %s to %s", entry.snapshot.generation(), snapshot.generation()));
                }
                else {
                    observedGenerations.put(new GenerationKey(snapshot.catalog(), snapshot.generation()), snapshot);
                    while (observedGenerations.size() > maximumEntries) {
                        observedGenerations.remove(observedGenerations.entrySet().iterator().next().getKey());
                    }
                    entry.snapshot = snapshot;
                    entry.loadedAtNanos = ticker.getAsLong();
                    entry.refreshBackoffActive = false;
                    entry.refresh = null;
                    entry.upstream = null;
                    published = true;
                }
            }
        }
        if (published) {
            refreshTask.result().complete(snapshot);
        }
        else if (rejection != null) {
            refreshTask.result().completeExceptionally(rejection);
        }
        else {
            refreshTask.result().completeExceptionally(new CancellationException("HogQL semantic catalog refresh no longer owns its cache entry"));
        }
    }

    private Throwable validateLoadedSnapshot(RefreshTask refreshTask, HogQlSemanticCatalogSnapshot snapshot)
    {
        if (!snapshot.catalog().equals(refreshTask.cacheKey().catalog())) {
            return new HogQlSemanticCatalogException(
                    Failure.CATALOG_MISMATCH,
                    "HogQL semantic catalog snapshot does not match the refreshed catalog");
        }
        if (refreshTask.cacheKey().expectedGeneration().isPresent() &&
                snapshot.generation() != refreshTask.cacheKey().expectedGeneration().orElseThrow()) {
            return new HogQlSemanticCatalogException(
                    Failure.GENERATION_MISMATCH,
                    "HogQL semantic catalog snapshot generation does not match the refreshed generation");
        }
        return null;
    }

    private void completeFailure(RefreshTask refreshTask, Throwable failure)
    {
        requireNonNull(failure, "failure is null");
        boolean owned;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.cacheKey());
            owned = entry == refreshTask.entry() && entry.refresh == refreshTask.result();
            if (owned) {
                entry.refresh = null;
                entry.upstream = null;
                entry.lastRefreshFailureAtNanos = ticker.getAsLong();
                entry.refreshBackoffActive = true;
            }
        }
        if (owned) {
            refreshTask.result().completeExceptionally(failure);
        }
        else {
            refreshTask.result().completeExceptionally(new CancellationException("HogQL semantic catalog refresh no longer owns its cache entry"));
        }
    }

    private void evictEntries(List<RefreshCancellation> evictedRefreshes)
    {
        while (entries.size() > maximumEntries) {
            Map.Entry<CacheKey, Entry> victim = entries.entrySet().stream()
                    .filter(entry -> entry.getKey().expectedGeneration().isPresent())
                    .findFirst()
                    .orElseGet(() -> entries.entrySet().iterator().next());
            entries.remove(victim.getKey());
            if (victim.getValue().refresh != null) {
                evictedRefreshes.add(new RefreshCancellation(victim.getValue().refresh, victim.getValue().upstream));
                victim.getValue().refresh = null;
                victim.getValue().upstream = null;
            }
        }
    }

    private static void cancelRefreshes(List<RefreshCancellation> refreshes, String message)
    {
        refreshes.forEach(refresh -> cancelRefresh(refresh, message));
    }

    private static void cancelRefresh(RefreshCancellation refresh, String message)
    {
        if (refresh.upstream() != null) {
            refresh.upstream().cancel(true);
        }
        refresh.result().completeExceptionally(new CancellationException(message));
    }

    private static long elapsedNanos(long start, long end)
    {
        return Math.max(0, end - start);
    }

    private static long nonNegativeNanos(Duration duration, String name)
    {
        requireNonNull(duration, name + " is null");
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return duration.toNanos();
    }

    private static long positiveNanos(Duration duration, String name)
    {
        long nanos = nonNegativeNanos(duration, name);
        if (nanos == 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return nanos;
    }

    @FunctionalInterface
    public interface SnapshotLoader
    {
        CompletionStage<HogQlSemanticCatalogSnapshot> load(PhysicalIdentifier catalog, OptionalLong expectedGeneration);
    }

    private static final class Entry
    {
        private HogQlSemanticCatalogSnapshot snapshot;
        private long loadedAtNanos;
        private long lastRefreshFailureAtNanos;
        private boolean refreshBackoffActive;
        private CompletableFuture<HogQlSemanticCatalogSnapshot> refresh;
        private CompletableFuture<HogQlSemanticCatalogSnapshot> upstream;
    }

    private record RefreshTask(
            CacheKey cacheKey,
            Entry entry,
            CompletableFuture<HogQlSemanticCatalogSnapshot> result) {}

    private record RefreshCancellation(
            CompletableFuture<HogQlSemanticCatalogSnapshot> result,
            CompletableFuture<HogQlSemanticCatalogSnapshot> upstream) {}

    private record CacheKey(PhysicalIdentifier catalog, OptionalLong expectedGeneration)
    {
        private CacheKey
        {
            catalog = requireNonNull(catalog, "catalog is null");
            expectedGeneration = requireNonNull(expectedGeneration, "expectedGeneration is null");
        }
    }

    private record GenerationKey(PhysicalIdentifier catalog, long generation)
    {
        private GenerationKey
        {
            catalog = requireNonNull(catalog, "catalog is null");
        }
    }
}
