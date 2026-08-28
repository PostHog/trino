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

import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;

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

public final class BoundedAsyncHogQlExchangeRateSnapshotCache
        implements HogQlExchangeRateSnapshotCache
{
    private final Object lock = new Object();
    private final int maximumEntries;
    private final long refreshAfterNanos;
    private final long expireAfterNanos;
    private final long failureBackoffNanos;
    private final LongSupplier ticker;
    private final Executor executor;
    private final SnapshotLoader loader;
    private final LinkedHashMap<OptionalLong, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private final LinkedHashMap<Long, HogQlExchangeRateSnapshot> observedGenerations = new LinkedHashMap<>(16, 0.75f, true);

    public BoundedAsyncHogQlExchangeRateSnapshotCache(
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
        refreshAfterNanos = nonNegativeNanos(refreshAfter, "refreshAfter");
        expireAfterNanos = positiveNanos(expireAfter, "expireAfter");
        failureBackoffNanos = nonNegativeNanos(failureBackoff, "failureBackoff");
        if (refreshAfterNanos > expireAfterNanos) {
            throw new IllegalArgumentException("refreshAfter must not exceed expireAfter");
        }
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.loader = requireNonNull(loader, "loader is null");
    }

    @Override
    public Optional<HogQlExchangeRateSnapshot> currentSnapshot(OptionalLong expectedGeneration)
    {
        expectedGeneration = HogQlExchangeRateSnapshotCache.validateExpectedGeneration(expectedGeneration);
        RefreshTask refreshTask;
        Optional<HogQlExchangeRateSnapshot> result;
        List<RefreshCancellation> evictedRefreshes = new ArrayList<>(1);
        long now = ticker.getAsLong();
        synchronized (lock) {
            Entry entry = entries.get(expectedGeneration);
            if (entry == null) {
                entry = inheritedExactEntry(expectedGeneration, now).orElseGet(Entry::new);
                entries.put(expectedGeneration, entry);
            }
            HogQlExchangeRateSnapshot snapshot = entry.snapshot;
            long age = snapshot == null ? Long.MAX_VALUE : elapsedNanos(entry.loadedAtNanos, now);
            result = snapshot != null && age < expireAfterNanos ? Optional.of(snapshot) : Optional.empty();
            boolean refreshRequired = snapshot == null ||
                    (expectedGeneration.isEmpty() && age >= refreshAfterNanos) ||
                    (expectedGeneration.isPresent() && age >= expireAfterNanos);
            refreshTask = refreshRequired ? prepareRefresh(expectedGeneration, entry, now, false) : null;
            evictEntries(evictedRefreshes);
            if (!entries.containsKey(expectedGeneration)) {
                refreshTask = null;
            }
        }
        cancelRefreshes(evictedRefreshes, "HogQL exchange-rate cache entry was evicted");
        dispatch(refreshTask);
        return result;
    }

    public CompletionStage<HogQlExchangeRateSnapshot> prewarm(OptionalLong expectedGeneration)
    {
        expectedGeneration = HogQlExchangeRateSnapshotCache.validateExpectedGeneration(expectedGeneration);
        RefreshTask refreshTask;
        CompletableFuture<HogQlExchangeRateSnapshot> future;
        List<RefreshCancellation> evictedRefreshes = new ArrayList<>(1);
        long now = ticker.getAsLong();
        synchronized (lock) {
            Entry entry = entries.get(expectedGeneration);
            if (entry == null) {
                entry = inheritedExactEntry(expectedGeneration, now).orElseGet(Entry::new);
                entries.put(expectedGeneration, entry);
            }
            boolean unexpired = entry.snapshot != null && elapsedNanos(entry.loadedAtNanos, now) < expireAfterNanos;
            if (unexpired && expectedGeneration.isPresent()) {
                refreshTask = null;
                future = CompletableFuture.completedFuture(entry.snapshot);
            }
            else {
                refreshTask = prepareRefresh(expectedGeneration, entry, now, true);
                future = refreshTask == null ? entry.refresh : refreshTask.result();
            }
            evictEntries(evictedRefreshes);
            if (!entries.containsKey(expectedGeneration)) {
                refreshTask = null;
            }
        }
        cancelRefreshes(evictedRefreshes, "HogQL exchange-rate cache entry was evicted");
        dispatch(refreshTask);
        return future.minimalCompletionStage();
    }

    public CompletionStage<HogQlExchangeRateSnapshot> prewarm()
    {
        return prewarm(OptionalLong.empty());
    }

    public void invalidate()
    {
        List<RefreshCancellation> refreshes = new ArrayList<>();
        synchronized (lock) {
            for (Entry entry : entries.values()) {
                if (entry.refresh != null) {
                    refreshes.add(new RefreshCancellation(entry.refresh, entry.upstream));
                    entry.refresh = null;
                    entry.upstream = null;
                }
            }
            entries.clear();
        }
        cancelRefreshes(refreshes, "HogQL exchange-rate cache was invalidated");
    }

    private Optional<Entry> inheritedExactEntry(OptionalLong expectedGeneration, long now)
    {
        if (expectedGeneration.isEmpty()) {
            return Optional.empty();
        }
        Entry latest = entries.get(OptionalLong.empty());
        if (latest == null || latest.snapshot == null || latest.snapshot.generation() != expectedGeneration.orElseThrow()) {
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

    private RefreshTask prepareRefresh(OptionalLong expectedGeneration, Entry entry, long now, boolean force)
    {
        if (entry.refresh != null) {
            return null;
        }
        if (!force && entry.refreshBackoffActive && elapsedNanos(entry.lastRefreshFailureAtNanos, now) < failureBackoffNanos) {
            return null;
        }
        CompletableFuture<HogQlExchangeRateSnapshot> result = new CompletableFuture<>();
        entry.refresh = result;
        return new RefreshTask(expectedGeneration, entry, result);
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
        CompletionStage<HogQlExchangeRateSnapshot> loaded;
        try {
            loaded = requireNonNull(loader.load(refreshTask.expectedGeneration()), "snapshot loader returned null");
        }
        catch (RuntimeException failure) {
            completeFailure(refreshTask, failure);
            return;
        }
        catch (Error failure) {
            completeFailure(refreshTask, failure);
            throw failure;
        }
        CompletableFuture<HogQlExchangeRateSnapshot> upstream = loaded.toCompletableFuture();
        boolean owned;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.expectedGeneration());
            owned = entry == refreshTask.entry() && entry.refresh == refreshTask.result();
            if (owned) {
                entry.upstream = upstream;
            }
        }
        if (!owned) {
            upstream.cancel(true);
            return;
        }
        loaded.whenComplete((snapshot, failure) -> {
            if (failure != null) {
                completeFailure(refreshTask, failure);
            }
            else if (snapshot == null) {
                completeFailure(refreshTask, new NullPointerException("snapshot loader completed with null"));
            }
            else {
                completeSuccess(refreshTask, snapshot);
            }
        });
    }

    private void completeSuccess(RefreshTask refreshTask, HogQlExchangeRateSnapshot snapshot)
    {
        Throwable rejection = validateLoadedSnapshot(refreshTask, snapshot);
        if (rejection != null) {
            completeFailure(refreshTask, rejection);
            return;
        }

        boolean published = false;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.expectedGeneration());
            if (entry == refreshTask.entry() && entry.refresh == refreshTask.result()) {
                HogQlExchangeRateSnapshot observed = observedGenerations.get(snapshot.generation());
                if (observed != null && !observed.equals(snapshot)) {
                    rejection = reject(entry, "HogQL exchange-rate generation content changed");
                }
                else if (refreshTask.expectedGeneration().isEmpty() && entry.snapshot != null && snapshot.generation() < entry.snapshot.generation()) {
                    rejection = reject(entry, format("HogQL exchange-rate generation regressed from %s to %s", entry.snapshot.generation(), snapshot.generation()));
                }
                else {
                    observedGenerations.put(snapshot.generation(), snapshot);
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
            refreshTask.result().completeExceptionally(new CancellationException("HogQL exchange-rate refresh no longer owns its cache entry"));
        }
    }

    private Throwable reject(Entry entry, String message)
    {
        entry.refresh = null;
        entry.upstream = null;
        entry.lastRefreshFailureAtNanos = ticker.getAsLong();
        entry.refreshBackoffActive = true;
        return new HogQlExchangeRateException(Failure.GENERATION_MISMATCH, message);
    }

    private static Throwable validateLoadedSnapshot(RefreshTask refreshTask, HogQlExchangeRateSnapshot snapshot)
    {
        if (refreshTask.expectedGeneration().isPresent() && snapshot.generation() != refreshTask.expectedGeneration().orElseThrow()) {
            return new HogQlExchangeRateException(Failure.GENERATION_MISMATCH, "HogQL exchange-rate snapshot generation does not match the refreshed generation");
        }
        return null;
    }

    private void completeFailure(RefreshTask refreshTask, Throwable failure)
    {
        requireNonNull(failure, "failure is null");
        boolean owned;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.expectedGeneration());
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
            refreshTask.result().completeExceptionally(new CancellationException("HogQL exchange-rate refresh no longer owns its cache entry"));
        }
    }

    private void evictEntries(List<RefreshCancellation> evictedRefreshes)
    {
        while (entries.size() > maximumEntries) {
            Map.Entry<OptionalLong, Entry> victim = entries.entrySet().stream()
                    .filter(entry -> entry.getKey().isPresent())
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
        refreshes.forEach(refresh -> {
            if (refresh.upstream() != null) {
                refresh.upstream().cancel(true);
            }
            refresh.result().completeExceptionally(new CancellationException(message));
        });
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
        CompletionStage<HogQlExchangeRateSnapshot> load(OptionalLong expectedGeneration);
    }

    private static final class Entry
    {
        private HogQlExchangeRateSnapshot snapshot;
        private long loadedAtNanos;
        private long lastRefreshFailureAtNanos;
        private boolean refreshBackoffActive;
        private CompletableFuture<HogQlExchangeRateSnapshot> refresh;
        private CompletableFuture<HogQlExchangeRateSnapshot> upstream;
    }

    private record RefreshTask(
            OptionalLong expectedGeneration,
            Entry entry,
            CompletableFuture<HogQlExchangeRateSnapshot> result) {}

    private record RefreshCancellation(
            CompletableFuture<HogQlExchangeRateSnapshot> result,
            CompletableFuture<HogQlExchangeRateSnapshot> upstream) {}
}
