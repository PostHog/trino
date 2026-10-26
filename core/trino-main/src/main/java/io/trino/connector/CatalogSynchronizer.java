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
import com.google.common.collect.ImmutableList;
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
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static io.trino.server.FailureSummary.summarize;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

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
 * {@link #catalogSyncState()}. A store that cannot be read, or a snapshot that cannot be trusted,
 * never advances the applied revision.
 *
 * <p>A catalog whose connector does not start is isolated instead: when only some catalogs of a
 * snapshot fail, the rest of the snapshot, including its removals, takes effect and the applied
 * revision advances. The catalogs that failed are reported by name and keep their last working
 * version here, if they had one, and the coordinator does not claim readiness until they are
 * installed. One broken definition therefore cannot hold back every other catalog. When every
 * catalog of a snapshot fails, the cause is the snapshot or this process rather than one
 * definition, so the revision is not applied and nothing is removed.
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

    public Optional<String> storeIdentity()
    {
        return catalogStoreManager.revisionedCatalogStore().flatMap(RevisionedCatalogStore::storeIdentity);
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
            log.error("Unexpected error while synchronizing catalogs (%s)", summarize(e));
            log.debug(e, "Unexpected error while synchronizing catalogs");
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
            recordSuccess(revision, revision);
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

        SnapshotApplication application = applySnapshot(snapshot);
        if (!application.revisionApplied()) {
            recordPartialApplication(snapshot.revision(), current.appliedRevision(), application.failedCatalogs());
            return false;
        }
        if (!application.failedCatalogs().isEmpty()) {
            recordPartialApplication(snapshot.revision(), OptionalLong.of(snapshot.revision()), application.failedCatalogs());
            return false;
        }
        recordSuccess(snapshot.revision(), snapshot.revision());
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
     * Installs every catalog of the snapshot that can be installed, and removes the catalogs the
     * snapshot no longer publishes unless no catalog of it could be installed.
     */
    private SnapshotApplication applySnapshot(CatalogSnapshot snapshot)
    {
        List<FailedCatalog> failedCatalogs = new ArrayList<>();
        for (CatalogProperties catalog : snapshot.catalogs()) {
            try {
                if (catalogManager.applyPublishedCatalog(catalog)) {
                    log.info("Applied catalog %s of revision %s", catalog.name(), snapshot.revision());
                }
            }
            catch (Throwable e) {
                failedCatalogs.add(new FailedCatalog(catalog.name(), catalog.version(), catalog.connectorName()));
                // A connector reports what it could not reach, which can include a resolved credential
                log.error("Could not apply catalog %s of revision %s (%s)", catalog.name(), snapshot.revision(), summarize(e));
                log.debug(e, "Could not apply catalog %s of revision %s", catalog.name(), snapshot.revision());
            }
        }
        List<FailedCatalog> sortedFailedCatalogs = failedCatalogs.stream()
                .sorted(comparing(failedCatalog -> failedCatalog.name().toString()))
                .collect(toImmutableList());

        if (!snapshot.catalogs().isEmpty() && failedCatalogs.size() == snapshot.catalogs().size()) {
            // Not one catalog could be installed. That is a snapshot or an environment that is wrong,
            // not one bad definition, and removing catalogs on its account would widen the damage
            log.error(
                    "None of the %s catalogs of revision %s could be applied; keeping the catalogs this coordinator has",
                    failedCatalogs.size(),
                    snapshot.revision());
            return new SnapshotApplication(false, sortedFailedCatalogs);
        }
        if (!failedCatalogs.isEmpty()) {
            log.warn(
                    "Applying revision %s without %s catalogs that could not be applied: %s",
                    snapshot.revision(),
                    failedCatalogs.size(),
                    sortedFailedCatalogs.stream().map(failedCatalog -> failedCatalog.name().toString()).collect(joining(", ")));
        }

        int failedRemovals = 0;
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
                failedRemovals++;
                log.error("Could not remove catalog %s of revision %s (%s)", catalogName, snapshot.revision(), summarize(e));
                log.debug(e, "Could not remove catalog %s of revision %s", catalogName, snapshot.revision());
            }
        }
        // A catalog that is still reachable although the revision removed it is not isolated the
        // way a catalog that failed to start is, so the revision is not applied while one remains
        return new SnapshotApplication(failedRemovals == 0, sortedFailedCatalogs);
    }

    private void recordSuccess(long observedRevision, long appliedRevision)
    {
        consecutiveFailures = 0;
        state.set(new CatalogSyncState(
                OptionalLong.of(observedRevision),
                OptionalLong.of(appliedRevision),
                ImmutableList.of(),
                OptionalLong.of(System.currentTimeMillis()),
                Optional.empty()));
    }

    /**
     * Some or all catalogs of the observed revision could not be installed. The coordinator keeps
     * retrying them, and it is not up to date until they are installed, whether or not the rest of
     * the revision was applied.
     */
    private void recordPartialApplication(long observedRevision, OptionalLong appliedRevision, List<FailedCatalog> failedCatalogs)
    {
        consecutiveFailures++;
        CatalogSyncState current = state.get();
        state.set(new CatalogSyncState(
                OptionalLong.of(observedRevision),
                appliedRevision,
                failedCatalogs,
                current.lastSuccessMillis(),
                Optional.of(CATALOGS_NOT_APPLIED)));
    }

    private void recordFailure(CatalogSyncFailure failure, Throwable cause)
    {
        // The cause can name the store, its connection URL or a property value. It is not reported
        // through readiness, and the ordinary log line carries only the types it is made of; the
        // message and the stack trace are available at debug level
        log.warn("Catalog synchronization failed with %s (%s); keeping the catalogs this coordinator already has", failure, summarize(cause));
        log.debug(cause, "Catalog synchronization failed with %s", failure);
        recordFailure(failure);
    }

    private void recordFailure(CatalogSyncFailure failure)
    {
        consecutiveFailures++;
        CatalogSyncState current = state.get();
        state.set(new CatalogSyncState(
                current.observedRevision(),
                current.appliedRevision(),
                current.failedCatalogInstances(),
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

    private record SnapshotApplication(boolean revisionApplied, List<FailedCatalog> failedCatalogs)
    {
        private SnapshotApplication
        {
            failedCatalogs = ImmutableList.copyOf(requireNonNull(failedCatalogs, "failedCatalogs is null"));
        }
    }

    /**
     * A published catalog definition that could not be installed here. If an earlier version of the
     * catalog was installed, that version stays active.
     */
    public record FailedCatalog(CatalogName name, CatalogVersion version, ConnectorName connectorName)
    {
        public FailedCatalog
        {
            requireNonNull(name, "name is null");
            requireNonNull(version, "version is null");
            requireNonNull(connectorName, "connectorName is null");
        }
    }

    /**
     * What this coordinator knows about the published catalogs and how much of it is in effect
     * here. {@code appliedRevision} is set once a revision was installed apart from the catalogs in
     * {@code failedCatalogInstances}, which are ordered by name. It is not set for a revision none of
     * whose catalogs could be installed.
     */
    public record CatalogSyncState(
            OptionalLong observedRevision,
            OptionalLong appliedRevision,
            List<FailedCatalog> failedCatalogInstances,
            OptionalLong lastSuccessMillis,
            Optional<CatalogSyncFailure> lastFailure)
    {
        public CatalogSyncState
        {
            requireNonNull(observedRevision, "observedRevision is null");
            requireNonNull(appliedRevision, "appliedRevision is null");
            failedCatalogInstances = ImmutableList.copyOf(requireNonNull(failedCatalogInstances, "failedCatalogInstances is null"));
            requireNonNull(lastSuccessMillis, "lastSuccessMillis is null");
            requireNonNull(lastFailure, "lastFailure is null");
        }

        public static CatalogSyncState initial()
        {
            return new CatalogSyncState(OptionalLong.empty(), OptionalLong.empty(), ImmutableList.of(), OptionalLong.empty(), Optional.empty());
        }

        public int failedCatalogs()
        {
            return failedCatalogInstances.size();
        }

        public boolean isUpToDate()
        {
            return observedRevision.stream()
                    .anyMatch(observed -> appliedRevision.stream().anyMatch(applied -> applied == observed))
                    && failedCatalogInstances.isEmpty()
                    && lastFailure.isEmpty();
        }
    }
}
