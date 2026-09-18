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
package io.trino.connector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.RevisionedCatalogStore;
import io.trino.spi.catalog.RevisionedCatalogStore.CatalogSnapshot;
import io.trino.spi.catalog.RevisionedCatalogStore.IncompleteSnapshotException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.connector.CatalogSyncFailure.CATALOGS_NOT_APPLIED;
import static io.trino.connector.CatalogSyncFailure.NOTHING_PUBLISHED;
import static io.trino.connector.CatalogSyncFailure.NOT_INITIALIZED;
import static io.trino.connector.CatalogSyncFailure.REVISION_REGRESSED;
import static io.trino.connector.CatalogSyncFailure.SNAPSHOT_INCOMPLETE;
import static io.trino.connector.CatalogSyncFailure.STORE_NOT_REVISIONED;
import static io.trino.connector.CatalogSyncFailure.STORE_UNREACHABLE;
import static io.trino.connector.CatalogSyncFailure.SYNCHRONIZATION_DISABLED;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Keeps the catalogs of this coordinator in step with the catalogs an external writer publishes in
 * the catalog store, without restarting and without writing anything back to the store.
 *
 * <p>The published revision is polled on a jittered interval; a full snapshot is only fetched when
 * the revision changed. Reconciliation runs on a single thread, so at most one snapshot is applied
 * at a time and a connector that takes long to initialize can never cause overlapping attempts.
 *
 * <p>Anything that fails - an unreachable store, an incomplete snapshot, a connector that does not
 * start - leaves the last known good local state in place and is reported through
 * {@link #catalogSyncState()}. Only a snapshot whose catalogs were all applied advances the applied
 * revision, so a coordinator never claims readiness for a revision it did not fully install.
 */
@ThreadSafe
public class CatalogSynchronizer
{
    private static final Logger log = Logger.get(CatalogSynchronizer.class);

    private final CatalogStoreManager catalogStoreManager;
    private final CoordinatorDynamicCatalogManager catalogManager;
    private final boolean enabled;
    private final long pollIntervalMillis;
    private final long minRetryDelayMillis;
    private final long maxRetryDelayMillis;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("catalog-sync"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicReference<CatalogSyncState> state = new AtomicReference<>(CatalogSyncState.initial());

    private int consecutiveFailures;

    @Inject
    public CatalogSynchronizer(
            CatalogStoreManager catalogStoreManager,
            CoordinatorDynamicCatalogManager catalogManager,
            CatalogSyncConfig config)
    {
        this.catalogStoreManager = requireNonNull(catalogStoreManager, "catalogStoreManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.enabled = config.isEnabled();
        this.pollIntervalMillis = config.getPollInterval().toMillis();
        this.minRetryDelayMillis = config.getMinRetryDelay().toMillis();
        this.maxRetryDelayMillis = max(config.getMaxRetryDelay().toMillis(), minRetryDelayMillis);
    }

    @PostConstruct
    public void start()
    {
        if (started.getAndSet(true)) {
            return;
        }
        if (!enabled) {
            // Reading a managed store without following it is a valid way to inspect a cell, and it
            // is also what a half-configured serving coordinator looks like. It is not failed here,
            // because failing startup would take a coordinator down over a diagnostic setting; it
            // is reported as loudly as it can be instead, and readiness stays false.
            recordFailure(SYNCHRONIZATION_DISABLED);
            scheduleTask(this::warnWhenStoreIsManaged, 0);
            return;
        }
        log.info("Following published catalogs, polling every %s", Duration.succinctDuration(pollIntervalMillis, MILLISECONDS));
        schedule(pollIntervalMillis);
    }

    @PreDestroy
    public void stop()
    {
        stopped.set(true);
        executor.shutdownNow();
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public CatalogSyncState catalogSyncState()
    {
        return state.get();
    }

    private void schedule(long delayMillis)
    {
        scheduleTask(this::runOnce, delayMillis);
    }

    private void scheduleTask(Runnable task, long delayMillis)
    {
        if (stopped.get()) {
            return;
        }
        try {
            executor.schedule(task, delayMillis, MILLISECONDS);
        }
        catch (RuntimeException e) {
            if (!stopped.get()) {
                log.error(e, "Could not schedule the next catalog synchronization");
            }
        }
    }

    /**
     * A coordinator whose catalogs are published by someone else, but which was not told to follow
     * them, only ever serves what it loaded at startup. Nothing can repair that at runtime, so it
     * is stated once, clearly, at startup.
     */
    private void warnWhenStoreIsManaged()
    {
        if (catalogStoreManager.revisionedCatalogStore().isPresent()) {
            log.error("The catalog store publishes catalog revisions, but catalog.sync.enabled is false: " +
                    "this coordinator will not follow catalogs published after it started, and reports itself as not ready");
        }
    }

    private void runOnce()
    {
        long delayMillis;
        try {
            delayMillis = synchronizeCatalogs() ? nextPollDelay() : nextRetryDelay();
        }
        catch (Throwable e) {
            // Never let the loop die: the next attempt has to keep the coordinator's state converging
            log.error(e, "Unexpected error while synchronizing catalogs");
            delayMillis = nextRetryDelay();
        }
        schedule(delayMillis);
    }

    /**
     * @return true if the published state is fully applied
     */
    @VisibleForTesting
    boolean synchronizeCatalogs()
    {
        Optional<RevisionedCatalogStore> store = catalogStoreManager.revisionedCatalogStore();
        if (store.isEmpty()) {
            recordFailure(STORE_NOT_REVISIONED);
            return false;
        }
        if (!catalogManager.isInitialized()) {
            // Reconciling before the initial load would fight with it over the same catalogs
            recordFailure(NOT_INITIALIZED);
            return false;
        }

        CatalogSyncState current = state.get();
        OptionalLong published;
        try {
            published = store.get().currentRevision();
        }
        catch (RuntimeException e) {
            recordFailure(STORE_UNREACHABLE, e);
            return false;
        }

        if (published.isEmpty()) {
            // A store nothing has published to is not an empty desired state. It is also what a
            // restored, emptied or wrongly addressed store looks like, so nothing is removed here
            recordFailure(NOTHING_PUBLISHED);
            return false;
        }
        long revision = published.orElseThrow();
        if (hasRegressed(current, revision)) {
            return false;
        }

        if (current.appliedRevision().stream().anyMatch(applied -> applied == revision) && current.failedCatalogs() == 0) {
            recordSuccess(revision, revision, 0);
            return true;
        }

        CatalogSnapshot snapshot;
        try {
            snapshot = store.get().fetchSnapshot();
        }
        catch (RuntimeException e) {
            recordFailure(snapshotFailure(e), e);
            return false;
        }

        // The snapshot is read after the revision was polled, so it is checked again: the store can
        // have been replaced by an older one in between
        if (hasRegressed(current, snapshot.revision())) {
            return false;
        }

        int failedCatalogs = applySnapshot(snapshot);
        if (failedCatalogs > 0) {
            recordPartialApplication(snapshot.revision(), failedCatalogs);
            return false;
        }
        recordSuccess(snapshot.revision(), snapshot.revision(), 0);
        return true;
    }

    /**
     * A published revision below the one already applied here means the shared state went
     * backwards - an older dump, a lagging replica, a different cell answering. Applying it would
     * remove catalogs that are working. The coordinator freezes on its last good state instead and
     * says so, which also stops it from claiming readiness.
     */
    private boolean hasRegressed(CatalogSyncState current, long revision)
    {
        if (current.appliedRevision().stream().noneMatch(applied -> revision < applied)) {
            return false;
        }
        log.error(
                "Published catalog revision %s is older than the applied revision %s; keeping the catalogs this coordinator has",
                revision,
                current.appliedRevision().orElseThrow());
        recordFailure(REVISION_REGRESSED);
        return true;
    }

    private static CatalogSyncFailure snapshotFailure(RuntimeException failure)
    {
        // The store distinguishes damaged contents from a store it could not reach; either way the
        // reason itself stays in the log and only the category is reported
        if (failure instanceof IncompleteSnapshotException) {
            return SNAPSHOT_INCOMPLETE;
        }
        return STORE_UNREACHABLE;
    }

    /**
     * @return the number of catalogs of this snapshot that could not be applied
     */
    private int applySnapshot(CatalogSnapshot snapshot)
    {
        int failedCatalogs = 0;
        for (CatalogProperties catalog : snapshot.catalogs()) {
            try {
                if (catalogManager.applyPublishedCatalog(catalog)) {
                    log.info("Applied catalog %s of revision %s", catalog.name(), snapshot.revision());
                }
            }
            catch (Throwable e) {
                failedCatalogs++;
                log.error(e, "Could not apply catalog %s of revision %s", catalog.name(), snapshot.revision());
            }
        }

        if (failedCatalogs > 0) {
            // Removing while part of the revision could not be installed would widen the damage of a
            // snapshot that turns out to be wrong; the removals happen once the additions succeed
            return failedCatalogs;
        }

        Set<CatalogName> publishedNames = snapshot.catalogs().stream()
                .map(CatalogProperties::name)
                .collect(toImmutableSet());
        // Only catalogs that came from the store are removed, and only because the snapshot, which was
        // verified to be complete, no longer contains them
        for (CatalogName catalogName : ImmutableSet.copyOf(catalogManager.storeCatalogNames())) {
            if (publishedNames.contains(catalogName)) {
                continue;
            }
            try {
                if (catalogManager.removePublishedCatalog(catalogName)) {
                    log.info("Removed catalog %s, which revision %s no longer publishes", catalogName, snapshot.revision());
                }
            }
            catch (Throwable e) {
                failedCatalogs++;
                log.error(e, "Could not remove catalog %s of revision %s", catalogName, snapshot.revision());
            }
        }
        return failedCatalogs;
    }

    private void recordSuccess(long observedRevision, long appliedRevision, int failedCatalogs)
    {
        consecutiveFailures = 0;
        state.set(new CatalogSyncState(
                OptionalLong.of(observedRevision),
                OptionalLong.of(appliedRevision),
                failedCatalogs,
                OptionalLong.of(System.currentTimeMillis()),
                Optional.empty()));
    }

    private void recordPartialApplication(long observedRevision, int failedCatalogs)
    {
        consecutiveFailures++;
        CatalogSyncState current = state.get();
        state.set(new CatalogSyncState(
                OptionalLong.of(observedRevision),
                current.appliedRevision(),
                failedCatalogs,
                current.lastSuccessMillis(),
                Optional.of(CATALOGS_NOT_APPLIED)));
    }

    private void recordFailure(CatalogSyncFailure failure, Throwable cause)
    {
        // The cause can name the store, its URL or a property value, so it is logged and not reported
        log.warn(cause, "Catalog synchronization failed with %s; keeping the catalogs this coordinator already has", failure);
        recordFailure(failure);
    }

    private void recordFailure(CatalogSyncFailure failure)
    {
        consecutiveFailures++;
        CatalogSyncState current = state.get();
        state.set(new CatalogSyncState(
                current.observedRevision(),
                current.appliedRevision(),
                current.failedCatalogs(),
                current.lastSuccessMillis(),
                Optional.of(failure)));
    }

    private long nextPollDelay()
    {
        // Spread the polls of the coordinators of a cluster instead of having them arrive together
        long jitter = pollIntervalMillis / 2;
        return ThreadLocalRandom.current().nextLong(pollIntervalMillis - jitter, pollIntervalMillis + jitter + 1);
    }

    private long nextRetryDelay()
    {
        int doublings = min(max(consecutiveFailures - 1, 0), 20);
        long scaled = minRetryDelayMillis << doublings;
        long cap = maxRetryDelayMillis;
        if (scaled > 0) {
            cap = min(scaled, maxRetryDelayMillis);
        }
        return ThreadLocalRandom.current().nextLong(minRetryDelayMillis, cap + 1);
    }

    /**
     * What this coordinator knows about the published catalogs and how much of it is in effect
     * here. {@code appliedRevision} is only set once every catalog of that revision was installed.
     */
    public record CatalogSyncState(
            OptionalLong observedRevision,
            OptionalLong appliedRevision,
            int failedCatalogs,
            OptionalLong lastSuccessMillis,
            Optional<CatalogSyncFailure> lastFailure)
    {
        public CatalogSyncState
        {
            requireNonNull(observedRevision, "observedRevision is null");
            requireNonNull(appliedRevision, "appliedRevision is null");
            requireNonNull(lastSuccessMillis, "lastSuccessMillis is null");
            requireNonNull(lastFailure, "lastFailure is null");
        }

        public static CatalogSyncState initial()
        {
            return new CatalogSyncState(OptionalLong.empty(), OptionalLong.empty(), 0, OptionalLong.empty(), Optional.empty());
        }

        public boolean isUpToDate()
        {
            return observedRevision.stream()
                    .anyMatch(observed -> appliedRevision.stream().anyMatch(applied -> applied == observed))
                    && failedCatalogs == 0
                    && lastFailure.isEmpty();
        }
    }
}
