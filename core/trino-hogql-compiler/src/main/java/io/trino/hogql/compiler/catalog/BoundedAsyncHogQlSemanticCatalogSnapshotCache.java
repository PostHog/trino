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
    private final LinkedHashMap<PhysicalIdentifier, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);

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
        requireNonNull(catalog, "catalog is null");
        RefreshTask refreshTask;
        Optional<HogQlSemanticCatalogSnapshot> result;
        List<CompletableFuture<HogQlSemanticCatalogSnapshot>> evictedRefreshes = new ArrayList<>(1);
        long now = ticker.getAsLong();
        synchronized (lock) {
            Entry entry = entries.get(catalog);
            if (entry == null) {
                entry = new Entry();
                entries.put(catalog, entry);
                evictEntries(evictedRefreshes);
            }

            HogQlSemanticCatalogSnapshot snapshot = entry.snapshot;
            long age = snapshot == null ? Long.MAX_VALUE : elapsedNanos(entry.loadedAtNanos, now);
            result = snapshot != null && age < expireAfterNanos ? Optional.of(snapshot) : Optional.empty();
            refreshTask = (snapshot == null || age >= refreshAfterNanos) ? prepareRefresh(catalog, entry, now, false) : null;
        }
        cancelRefreshes(evictedRefreshes, "HogQL semantic catalog cache entry was evicted");
        dispatch(refreshTask);
        return result;
    }

    public CompletionStage<HogQlSemanticCatalogSnapshot> prewarm(PhysicalIdentifier catalog)
    {
        requireNonNull(catalog, "catalog is null");
        RefreshTask refreshTask;
        CompletableFuture<HogQlSemanticCatalogSnapshot> future;
        List<CompletableFuture<HogQlSemanticCatalogSnapshot>> evictedRefreshes = new ArrayList<>(1);
        synchronized (lock) {
            Entry entry = entries.get(catalog);
            if (entry == null) {
                entry = new Entry();
                entries.put(catalog, entry);
                evictEntries(evictedRefreshes);
            }
            refreshTask = prepareRefresh(catalog, entry, ticker.getAsLong(), true);
            future = refreshTask == null ? entry.refresh : refreshTask.result();
        }
        cancelRefreshes(evictedRefreshes, "HogQL semantic catalog cache entry was evicted");
        dispatch(refreshTask);
        return future.minimalCompletionStage();
    }

    public void invalidate(PhysicalIdentifier catalog)
    {
        requireNonNull(catalog, "catalog is null");
        CompletableFuture<HogQlSemanticCatalogSnapshot> refresh = null;
        synchronized (lock) {
            Entry removed = entries.remove(catalog);
            if (removed != null) {
                refresh = removed.refresh;
                removed.refresh = null;
            }
        }
        cancelRefresh(refresh, "HogQL semantic catalog cache entry was invalidated");
    }

    private RefreshTask prepareRefresh(PhysicalIdentifier catalog, Entry entry, long now, boolean force)
    {
        if (entry.refresh != null) {
            return null;
        }
        if (!force && entry.refreshBackoffActive && elapsedNanos(entry.lastRefreshFailureAtNanos, now) < failureBackoffNanos) {
            return null;
        }
        CompletableFuture<HogQlSemanticCatalogSnapshot> result = new CompletableFuture<>();
        entry.refresh = result;
        return new RefreshTask(catalog, entry, result);
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
            loaded = requireNonNull(loader.load(refreshTask.catalog()), "snapshot loader returned null");
        }
        catch (RuntimeException failure) {
            completeFailure(refreshTask, failure);
            return;
        }
        catch (Error failure) {
            completeFailure(refreshTask, failure);
            throw failure;
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
            Entry entry = entries.get(refreshTask.catalog());
            if (entry == refreshTask.entry() && entry.refresh == refreshTask.result()) {
                if (entry.snapshot != null && snapshot.generation() < entry.snapshot.generation()) {
                    entry.refresh = null;
                    entry.lastRefreshFailureAtNanos = ticker.getAsLong();
                    entry.refreshBackoffActive = true;
                    rejection = new HogQlSemanticCatalogException(
                            Failure.GENERATION_MISMATCH,
                            format("HogQL semantic catalog generation regressed from %s to %s", entry.snapshot.generation(), snapshot.generation()));
                }
                else {
                    entry.snapshot = snapshot;
                    entry.loadedAtNanos = ticker.getAsLong();
                    entry.refreshBackoffActive = false;
                    entry.refresh = null;
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
        if (!snapshot.catalog().equals(refreshTask.catalog())) {
            return new HogQlSemanticCatalogException(
                    Failure.CATALOG_MISMATCH,
                    "HogQL semantic catalog snapshot does not match the refreshed catalog");
        }
        return null;
    }

    private void completeFailure(RefreshTask refreshTask, Throwable failure)
    {
        requireNonNull(failure, "failure is null");
        boolean owned;
        synchronized (lock) {
            Entry entry = entries.get(refreshTask.catalog());
            owned = entry == refreshTask.entry() && entry.refresh == refreshTask.result();
            if (owned) {
                entry.refresh = null;
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

    private void evictEntries(List<CompletableFuture<HogQlSemanticCatalogSnapshot>> evictedRefreshes)
    {
        while (entries.size() > maximumEntries) {
            Map.Entry<PhysicalIdentifier, Entry> eldest = entries.entrySet().iterator().next();
            entries.remove(eldest.getKey());
            if (eldest.getValue().refresh != null) {
                evictedRefreshes.add(eldest.getValue().refresh);
                eldest.getValue().refresh = null;
            }
        }
    }

    private static void cancelRefreshes(List<CompletableFuture<HogQlSemanticCatalogSnapshot>> refreshes, String message)
    {
        refreshes.forEach(refresh -> cancelRefresh(refresh, message));
    }

    private static void cancelRefresh(CompletableFuture<HogQlSemanticCatalogSnapshot> refresh, String message)
    {
        if (refresh != null) {
            refresh.completeExceptionally(new CancellationException(message));
        }
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
        CompletionStage<HogQlSemanticCatalogSnapshot> load(PhysicalIdentifier catalog);
    }

    private static final class Entry
    {
        private HogQlSemanticCatalogSnapshot snapshot;
        private long loadedAtNanos;
        private long lastRefreshFailureAtNanos;
        private boolean refreshBackoffActive;
        private CompletableFuture<HogQlSemanticCatalogSnapshot> refresh;
    }

    private record RefreshTask(
            PhysicalIdentifier catalog,
            Entry entry,
            CompletableFuture<HogQlSemanticCatalogSnapshot> result) {}
}
