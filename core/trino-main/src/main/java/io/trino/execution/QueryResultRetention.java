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

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.execution.QueryTracker.TrackedQuery;
import io.trino.spi.QueryId;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Coordinates completed-history eviction with capability-authorized result requests.
 * Both query trackers share entries. Each entry exists only while a tracker or request holds it.
 */
public class QueryResultRetention
{
    private final Optional<Duration> idleTimeout;
    private final Predicate<TransactionId> transactionExists;
    private final Supplier<Instant> clock;
    private final ConcurrentMap<QueryId, Entry> entries = new ConcurrentHashMap<>();

    @Inject
    public QueryResultRetention(QueryManagerConfig config, TransactionManager transactionManager)
    {
        this(config.getCompletedResultIdleTimeout(), transactionManager::transactionExists, Instant::now);
    }

    QueryResultRetention(Optional<Duration> idleTimeout, Predicate<TransactionId> transactionExists, Supplier<Instant> clock)
    {
        this.idleTimeout = requireNonNull(idleTimeout, "idleTimeout is null");
        this.transactionExists = requireNonNull(transactionExists, "transactionExists is null");
        this.clock = requireNonNull(clock, "clock is null");
    }

    public boolean isEnabled()
    {
        return idleTimeout.isPresent();
    }

    public boolean register(QueryId queryId, BooleanSupplier registration)
    {
        if (!isEnabled()) {
            return registration.getAsBoolean();
        }
        AtomicBoolean registered = new AtomicBoolean();
        entries.compute(queryId, (_, existing) -> {
            Entry entry = existing == null ? new Entry() : existing;
            if (!entry.expired && registration.getAsBoolean()) {
                entry.trackers++;
                registered.set(true);
            }
            return entry.isUnused() ? null : entry;
        });
        return registered.get();
    }

    public Optional<Request> beginRequest(QueryId queryId, boolean allowUnregistered)
    {
        if (!isEnabled()) {
            return Optional.of(new Request(queryId, null));
        }
        AtomicReference<Entry> acquired = new AtomicReference<>();
        entries.compute(queryId, (_, existing) -> {
            if (existing == null && !allowUnregistered) {
                return null;
            }
            Entry entry = existing == null ? new Entry() : existing;
            if (!entry.expired) {
                entry.requests++;
                acquired.set(entry);
            }
            return entry;
        });
        if (acquired.get() == null) {
            return Optional.empty();
        }
        return Optional.of(new Request(queryId, acquired.get()));
    }

    public boolean tryExpire(TrackedQuery query, Instant minimumEndTime, BooleanSupplier removal)
    {
        AtomicBoolean removed = new AtomicBoolean();
        entries.computeIfPresent(query.getQueryId(), (_, entry) -> {
            if (!entry.expired) {
                Optional<Instant> endTime = query.getEndTime();
                if (!query.isDone() || endTime.isEmpty() || endTime.get().isAfter(minimumEndTime) || entry.requests != 0) {
                    return entry;
                }
                if (query.getSession().getTransactionId().filter(transactionExists).isPresent() ||
                        query.getStartedTransactionId().filter(transactionExists).isPresent()) {
                    return entry;
                }
                Instant lastAccess = endTime.get();
                if (entry.lastAccess != null && entry.lastAccess.isAfter(lastAccess)) {
                    lastAccess = entry.lastAccess;
                }
                if (lastAccess.plusMillis(idleTimeout.orElseThrow().toMillis()).isAfter(clock.get())) {
                    return entry;
                }
                entry.expired = true;
            }
            if (removal.getAsBoolean()) {
                entry.trackers--;
                removed.set(true);
            }
            return entry.isUnused() ? null : entry;
        });
        return removed.get();
    }

    int entryCount()
    {
        return entries.size();
    }

    public final class Request
            implements AutoCloseable
    {
        private final QueryId queryId;
        private final Entry entry;
        private boolean accepted;
        private boolean closed;

        private Request(QueryId queryId, Entry entry)
        {
            this.queryId = queryId;
            this.entry = entry;
        }

        public void accepted()
        {
            if (entry == null) {
                return;
            }
            entries.computeIfPresent(queryId, (_, current) -> {
                if (current == entry && !closed) {
                    accepted = true;
                    entry.lastAccess = clock.get();
                }
                return current;
            });
        }

        @Override
        public void close()
        {
            if (entry == null) {
                return;
            }
            entries.computeIfPresent(queryId, (_, current) -> {
                if (current == entry && !closed) {
                    closed = true;
                    entry.requests--;
                    if (accepted) {
                        entry.lastAccess = clock.get();
                    }
                }
                return current.isUnused() ? null : current;
            });
        }
    }

    private static final class Entry
    {
        // All entry fields are accessed inside the owning map's compute operation.
        private int trackers;
        private int requests;
        private boolean expired;
        private Instant lastAccess;

        public boolean isUnused()
        {
            return trackers == 0 && requests == 0;
        }
    }
}
