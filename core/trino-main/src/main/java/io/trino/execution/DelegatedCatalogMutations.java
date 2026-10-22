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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogStoreManager;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.DelegatedCatalogStore;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Shared by {@link CreateCatalogTask} and {@link DropCatalogTask} for a catalog store that hands
 * catalog mutations to an external writer. The writer accepting a request does not make the change
 * visible here: it arrives later through catalog synchronization. This waits for that, polling the
 * local catalog state without blocking a query thread and without holding any catalog lock, and
 * gives up with a retryable error once the store's visibility timeout has passed.
 */
public class DelegatedCatalogMutations
{
    private static final Logger log = Logger.get(DelegatedCatalogMutations.class);

    @VisibleForTesting
    static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    private final Optional<CatalogStoreManager> catalogStoreManager;
    private final CatalogManager catalogManager;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final Duration pollInterval;
    // Each poll can call access control, which may be remote, so waits do not share one thread
    private final ScheduledExecutorService executor = newScheduledThreadPool(4, daemonThreadsNamed("delegated-catalog-wait-%s"));

    @Inject
    public DelegatedCatalogMutations(Optional<CatalogStoreManager> catalogStoreManager, CatalogManager catalogManager, AccessControl accessControl, GroupProvider groupProvider)
    {
        this(catalogStoreManager, catalogManager, accessControl, groupProvider, POLL_INTERVAL);
    }

    @VisibleForTesting
    DelegatedCatalogMutations(Optional<CatalogStoreManager> catalogStoreManager, CatalogManager catalogManager, AccessControl accessControl, GroupProvider groupProvider, Duration pollInterval)
    {
        this.catalogStoreManager = requireNonNull(catalogStoreManager, "catalogStoreManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.pollInterval = requireNonNull(pollInterval, "pollInterval is null");
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    /**
     * The configured store if catalog mutations are delegated, evaluated per statement because the
     * store is configured after the server has been wired.
     */
    public Optional<DelegatedCatalogStore> delegatedCatalogStore()
    {
        return catalogStoreManager.flatMap(CatalogStoreManager::delegatedCatalogStore);
    }

    /**
     * Whether the catalog is active on this coordinator: present by name and not failed.
     */
    public boolean isActive(CatalogName catalogName)
    {
        return catalogManager.getCatalog(catalogName)
                .filter(catalog -> !catalog.isFailed())
                .isPresent();
    }

    /**
     * Whether the catalog is active on this coordinator and the user of the session may see it.
     */
    public boolean isAvailable(Session session, CatalogName catalogName)
    {
        return isActive(catalogName) &&
                accessControl.filterCatalogs(currentSecurityContext(session), Set.of(catalogName.toString())).contains(catalogName.toString());
    }

    /**
     * The session's security context with the user's groups as the group provider reports them
     * now. A session's groups are resolved when its query starts, and a control plane may grant a
     * new catalog through a group that only exists once the catalog does; checked with the groups
     * the waiting statement started with, that grant would never become visible to it.
     */
    private SecurityContext currentSecurityContext(Session session)
    {
        Identity identity = session.getIdentity();
        Identity current = Identity.from(identity)
                .withGroups(groupProvider.getGroups(identity.getUser()))
                .build();
        return new SecurityContext(session.getRequiredTransactionId(), current, session.getQueryId(), session.getStart());
    }

    /**
     * Completes once the catalog is active on this coordinator and the user of the statement is
     * allowed to see it, or fails with {@code CATALOG_NOT_AVAILABLE} after the timeout. When the
     * store changed nothing (IF NOT EXISTS matched an existing catalog) and that catalog is present
     * here but failed to load, waiting cannot help, so that fails at once.
     */
    public ListenableFuture<Void> awaitCreated(QueryStateMachine stateMachine, CatalogName catalogName, boolean changed, Duration timeout)
    {
        Session session = stateMachine.getSession();
        if (!changed && catalogManager.getCatalog(catalogName).map(Catalog::isFailed).orElse(false)) {
            return immediateFailedFuture(new TrinoException(CATALOG_NOT_AVAILABLE, "Catalog '%s' exists but failed to load on this coordinator".formatted(catalogName)));
        }
        return await(
                stateMachine,
                () -> isAvailable(session, catalogName),
                timeout,
                () -> {
                    if (catalogManager.getCatalog(catalogName).map(Catalog::isFailed).orElse(false)) {
                        return new TrinoException(CATALOG_NOT_AVAILABLE, "Catalog '%s' was accepted but failed to load on this coordinator".formatted(catalogName));
                    }
                    return new TrinoException(CATALOG_NOT_AVAILABLE, "Catalog '%s' was accepted but is not yet available on this coordinator; retry the statement shortly".formatted(catalogName));
                });
    }

    /**
     * Completes once the catalog is no longer present on this coordinator, or fails with
     * {@code CATALOG_NOT_AVAILABLE} after the timeout.
     */
    public ListenableFuture<Void> awaitDropped(QueryStateMachine stateMachine, CatalogName catalogName, Duration timeout)
    {
        return await(
                stateMachine,
                () -> catalogManager.getCatalog(catalogName).isEmpty(),
                timeout,
                () -> new TrinoException(CATALOG_NOT_AVAILABLE, "Catalog '%s' was accepted for removal but is still present on this coordinator; retry the statement shortly".formatted(catalogName)));
    }

    /**
     * A check that fails, such as an access control call that times out, is a check that does not
     * hold yet: the next poll asks again, and the deadline still bounds the wait.
     */
    private static boolean holds(BooleanSupplier condition)
    {
        try {
            return condition.getAsBoolean();
        }
        catch (RuntimeException e) {
            log.debug(e, "Delegated catalog check failed, polling again");
            return false;
        }
    }

    private ListenableFuture<Void> await(QueryStateMachine stateMachine, BooleanSupplier condition, Duration timeout, Supplier<TrinoException> timeoutFailure)
    {
        if (holds(condition)) {
            return immediateVoidFuture();
        }

        long deadline = System.nanoTime() + timeout.toNanos();
        SettableFuture<Void> result = SettableFuture.create();
        // A query that is cancelled or fails for another reason stops the polling
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                result.cancel(false);
            }
        });

        Runnable check = new Runnable()
        {
            @Override
            public void run()
            {
                if (result.isDone()) {
                    return;
                }
                try {
                    if (holds(condition)) {
                        result.set(null);
                        return;
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        result.setException(timeoutFailure.get());
                        return;
                    }
                    executor.schedule(this, Math.min(pollInterval.toNanos(), remaining), NANOSECONDS);
                }
                catch (Throwable e) {
                    result.setException(e);
                }
            }
        };
        try {
            executor.schedule(check, Math.min(pollInterval.toNanos(), Math.max(timeout.toNanos(), 0)), NANOSECONDS);
        }
        catch (RuntimeException e) {
            result.setException(e);
        }
        return result;
    }
}
