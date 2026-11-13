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
package io.trino.plugin.hoglake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Answer;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Issued;
import io.trino.plugin.hoglake.rest.HoglakeStorageSessionClient.Refusal;
import io.trino.plugin.hoglake.rest.HoglakeTokenSource;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.STORAGE_SESSION_ACCEPT_FLOOR;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.STORAGE_SESSION_TEAMS_PER_REQUEST;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The S3 sessions of the posthog schema that this coordinator holds, for one
 * catalog: one session for each Trino login and team, asked from the
 * credential service when a split of that team is handed out and no session
 * with enough lifetime is held.
 *
 * <p>A session is kept under the login it was issued for, and found only by
 * that login's key: {@link #forLogin} is the only way to a session, and the
 * login it is given is the only input to the lookup and to the token that
 * signs a request. Queries of one login share its sessions; two logins never do.
 *
 * <p>A session is handed out only while at least
 * {@link #HAND_OUT_FLOOR ten minutes} of it remain. The service returns the
 * same session until it has less than that by its own clock, so an answer is
 * accepted with nine minutes, and a login and team that were just asked are
 * not asked again for {@link #REASK_GUARD five seconds}: a clock that runs
 * ahead of the service's then costs one request each five seconds, not a loop.
 *
 * <p>All logins of the catalog share one request budget at the service, which
 * charges it for every request. So the requests of one catalog are paced below
 * that budget, the asks of one login that arrive together go out as one
 * request of several teams, one login and team is asked by one request at a
 * time however many queries wait for it, and a refusal is remembered. When the
 * service asks to wait, or does not answer, the request is sent again for up
 * to {@link #MAX_WAIT a minute} before the query fails.
 */
public final class HoglakeStorageSessions
        implements Closeable
{
    static final Duration HAND_OUT_FLOOR = Duration.ofMinutes(10);
    static final Duration REASK_GUARD = Duration.ofSeconds(5);
    static final Duration MAX_WAIT = Duration.ofSeconds(60);
    static final Duration REFUSAL_RETENTION = Duration.ofSeconds(60);
    static final Duration MERGE_WINDOW = Duration.ofMillis(25);
    static final int MAX_ENTRIES = 10_000;
    // As long as a session lives
    static final Duration ENTRY_LIFETIME = Duration.ofMinutes(15);
    // Below the service's 10 a second with a burst of 20 for all logins of the catalog
    static final int REQUESTS_PER_SECOND = 8;
    static final int REQUEST_BURST = 16;
    static final int MAX_REQUESTS_IN_FLIGHT = 3;
    private static final Duration UNREACHABLE_RETRY = Duration.ofSeconds(1);
    // A request in flight ends within its own time; this only bounds a wait on a lost one
    private static final Duration LOST_REQUEST_MARGIN = Duration.ofSeconds(30);

    /**
     * How a wait passes. A test passes it by moving its clock.
     */
    public interface Waiter
    {
        void waitUntil(long epochMillis)
                throws InterruptedException;
    }

    private final String mountCatalog;
    private final HoglakeStorageSessionClient client;
    private final HoglakeLoginTokenSource loginTokens;
    private final HoglakeStorageSessionStats stats;
    private final Clock clock;
    private final Waiter waiter;
    private final Duration mergeWindow;
    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("hoglake-storage-sessions-%d")
            .build());

    private final Cache<Key, State> entries;
    // A refusal that does not depend on the login or the team
    private final AtomicReference<Refused> refusedForAll = new AtomicReference<>();
    // No request is sent before this time: the service asked to wait, or did not answer
    private final AtomicLong waitUntilMillis = new AtomicLong();
    private final RequestBudget budget = new RequestBudget();
    private final Semaphore requestsInFlight = new Semaphore(MAX_REQUESTS_IN_FLIGHT);

    private final Object lock = new Object();
    // Guarded by lock: the one ask in progress for a login and team, and the asks of a login not yet sent
    private final Map<Key, Flight> flights = new HashMap<>();
    private final Map<String, List<Flight>> queued = new HashMap<>();

    public HoglakeStorageSessions(String mountCatalog, HoglakeStorageSessionClient client, HoglakeLoginTokenSource loginTokens, HoglakeStorageSessionStats stats)
    {
        this(mountCatalog, client, loginTokens, stats, Clock.systemUTC(), HoglakeStorageSessions::sleepUntil, MERGE_WINDOW);
    }

    @VisibleForTesting
    HoglakeStorageSessions(
            String mountCatalog,
            HoglakeStorageSessionClient client,
            HoglakeLoginTokenSource loginTokens,
            HoglakeStorageSessionStats stats,
            Clock clock,
            Waiter waiter,
            Duration mergeWindow)
    {
        this.mountCatalog = requireNonNull(mountCatalog, "mountCatalog is null");
        this.client = requireNonNull(client, "client is null");
        this.loginTokens = requireNonNull(loginTokens, "loginTokens is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.waiter = requireNonNull(waiter, "waiter is null");
        this.mergeWindow = requireNonNull(mergeWindow, "mergeWindow is null");
        this.entries = EvictableCacheBuilder.newBuilder()
                .maximumSize(MAX_ENTRIES)
                .expireAfterWrite(ENTRY_LIFETIME)
                .ticker(new Ticker()
                {
                    @Override
                    public long read()
                    {
                        return MILLISECONDS.toNanos(clock.millis());
                    }
                })
                .build();
    }

    /**
     * The sessions of one login. The login is fixed here, and nothing else
     * selects a session or the token a request is signed with.
     */
    public LoginSessions forLogin(String login)
    {
        return new LoginSessions(login);
    }

    /**
     * Where the connector waits for sessions, so that no thread of the engine does.
     */
    public ExecutorService executor()
    {
        return executor;
    }

    static void sleepUntil(long epochMillis)
            throws InterruptedException
    {
        long wait = epochMillis - System.currentTimeMillis();
        if (wait > 0) {
            Thread.sleep(wait);
        }
    }

    @VisibleForTesting
    long cachedEntries()
    {
        entries.cleanUp();
        return entries.size();
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }

    @Override
    public String toString()
    {
        return "HoglakeStorageSessions[redacted]";
    }

    public final class LoginSessions
    {
        private final String login;

        private LoginSessions(String login)
        {
            this.login = requireNonNull(login, "login is null");
        }

        /**
         * A session for each of {@code teams}, each with at least the hand-out
         * floor left now. Waits for the credential service when a session
         * must be asked for, and fails when one team is refused: the caller
         * gets every session or none.
         */
        public Map<Long, HoglakeStorageSession> sessions(Set<Long> teams)
        {
            long deadline = clock.millis() + MAX_WAIT.toMillis();
            Map<Long, Held> held = new HashMap<>();
            while (true) {
                long now = clock.millis();
                List<Key> missing = new ArrayList<>();
                for (long team : teams) {
                    // What a request of this call returned is used as the cache's own entry is
                    Held current = held.get(team);
                    if (current != null && usable(current, now)) {
                        continue;
                    }
                    held.remove(team);
                    Key key = new Key(login, mountCatalog, team);
                    Optional<State> answer = answered(key, now);
                    if (answer.isEmpty()) {
                        missing.add(key);
                        continue;
                    }
                    switch (answer.get()) {
                        case Held cached -> {
                            held.put(team, cached);
                            stats.sessionReused();
                        }
                        case Refused refused -> throw refused.failure();
                    }
                }
                if (missing.isEmpty()) {
                    ImmutableMap.Builder<Long, HoglakeStorageSession> sessions = ImmutableMap.builder();
                    held.forEach((team, session) -> sessions.put(team, session.session()));
                    return sessions.buildOrThrow();
                }
                Refused all = refusedForAll.get();
                if (all != null && now < all.untilMillis()) {
                    throw all.failure();
                }
                if (now >= deadline) {
                    throw Refusal.BUSY.failure();
                }
                for (Flight flight : join(login, missing, now)) {
                    switch (await(flight)) {
                        case Held issued -> held.put(flight.key().team(), issued);
                        case Refused refused -> throw refused.failure();
                    }
                }
            }
        }

        @Override
        public String toString()
        {
            return "LoginSessions[redacted]";
        }
    }

    /**
     * What is held for a key and needs no request now: a session that can be
     * handed out, or a refusal that is still remembered.
     */
    private Optional<State> answered(Key key, long now)
    {
        State state = entries.getIfPresent(key);
        if (state instanceof Held held && usable(held, now)) {
            return Optional.of(held);
        }
        if (state instanceof Refused refused && now < refused.untilMillis()) {
            return Optional.of(refused);
        }
        return Optional.empty();
    }

    private boolean usable(Held held, long now)
    {
        long left = held.session().expirationEpochMillis() - now;
        if (left >= HAND_OUT_FLOOR.toMillis()) {
            return true;
        }
        // Just asked: the service would return this session again
        return left >= STORAGE_SESSION_ACCEPT_FLOOR.toMillis() && now - held.askedAtMillis() < REASK_GUARD.toMillis();
    }

    /**
     * The ask in progress for each key that still needs one: the one that is
     * already there, or a new one that goes out with the login's other asks
     * of this moment.
     */
    private List<Flight> join(String login, List<Key> keys, long now)
    {
        List<Flight> joined = new ArrayList<>();
        synchronized (lock) {
            boolean send = false;
            for (Key key : keys) {
                Flight flight = flights.get(key);
                if (flight == null) {
                    // An ask stores its answer before it ends. So with no ask in progress, an answer
                    // that came while this caller looked is in the cache now, and is not asked for again.
                    if (answered(key, now).isPresent()) {
                        continue;
                    }
                    flight = new Flight(key, new CompletableFuture<>());
                    flights.put(key, flight);
                    List<Flight> waiting = queued.get(login);
                    if (waiting == null) {
                        waiting = new ArrayList<>();
                        queued.put(login, waiting);
                        send = true;
                    }
                    waiting.add(flight);
                }
                joined.add(flight);
            }
            if (send) {
                try {
                    executor.execute(() -> send(login));
                }
                catch (RejectedExecutionException e) {
                    // The catalog is closing
                    finish(queued.remove(login), refused(Refusal.UNREACHABLE, 0));
                }
            }
        }
        return joined;
    }

    private State await(Flight flight)
    {
        try {
            return flight.future().get(MAX_WAIT.plus(LOST_REQUEST_MARGIN).toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return refused(Refusal.UNREACHABLE, 0);
        }
        catch (ExecutionException | TimeoutException e) {
            return refused(Refusal.BUSY, 0);
        }
    }

    /**
     * Sends what a login asked for in one merge window: the teams ascending,
     * as many requests as the teams need, each for the whole wait.
     */
    private void send(String login)
    {
        boolean interrupted = false;
        try {
            waiter.waitUntil(clock.millis() + mergeWindow.toMillis());
        }
        catch (InterruptedException e) {
            interrupted = true;
        }
        List<Flight> asked;
        synchronized (lock) {
            asked = queued.remove(login);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            finish(asked, refused(Refusal.UNREACHABLE, 0));
            return;
        }
        asked.sort(Comparator.comparingLong(flight -> flight.key().team()));
        long deadline = clock.millis() + MAX_WAIT.toMillis();
        List<List<Flight>> requests = Lists.partition(asked, STORAGE_SESSION_TEAMS_PER_REQUEST);
        for (List<Flight> asks : requests.subList(1, requests.size())) {
            try {
                executor.execute(() -> resolve(login, asks, deadline));
            }
            catch (RejectedExecutionException e) {
                finish(asks, refused(Refusal.UNREACHABLE, 0));
            }
        }
        resolve(login, requests.getFirst(), deadline);
    }

    /**
     * Completes every ask of {@code asks}, whatever happens.
     */
    private void resolve(String login, List<Flight> asks, long deadline)
    {
        try {
            fetch(loginTokens.forLogin(login), asks, deadline);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(asks, refused(Refusal.UNREACHABLE, 0));
        }
        catch (TrinoException e) {
            // The login has no token: the token source's own error, which names nothing
            finish(asks, new Refused(e.getErrorCode(), e.getMessage(), 0));
        }
        catch (RuntimeException e) {
            // Not attached and not logged: what failed may hold a session
            finish(asks, new Refused(GENERIC_INTERNAL_ERROR.toErrorCode(), "Failed to obtain storage sessions for the posthog schema", 0));
        }
    }

    private void fetch(HoglakeTokenSource tokens, List<Flight> asks, long deadline)
            throws InterruptedException
    {
        Set<Long> teams = asks.stream().map(flight -> flight.key().team()).collect(toImmutableSet());
        Refusal lastWait = Refusal.BUSY;
        while (true) {
            Refused all = refusedForAll.get();
            if (all != null && clock.millis() < all.untilMillis()) {
                finish(asks, all);
                return;
            }
            if (!awaitTurn(deadline)) {
                finish(asks, refused(lastWait, 0));
                return;
            }
            Answer answer = sendRequest(tokens, teams, deadline);
            long now = clock.millis();
            switch (answer) {
                case Issued issued -> {
                    stats.sessionsIssued(asks.size());
                    for (Flight flight : asks) {
                        Held held = new Held(issued.sessions().get(flight.key().team()), now);
                        store(flight.key(), held);
                        finish(ImmutableSet.of(flight), held);
                    }
                    return;
                }
                case HoglakeStorageSessionClient.Refused refusal -> {
                    switch (refusal.refusal()) {
                        case BUSY -> {
                            stats.requestWaited();
                            lastWait = Refusal.BUSY;
                            waitUntilMillis.accumulateAndGet(now + refusal.retryAfter().toMillis(), Math::max);
                        }
                        case UNREACHABLE -> {
                            stats.requestWaited();
                            lastWait = Refusal.UNREACHABLE;
                            waitUntilMillis.accumulateAndGet(now + UNREACHABLE_RETRY.toMillis(), Math::max);
                        }
                        case TEAM_REFUSED -> {
                            stats.requestRefused();
                            if (asks.size() > 1) {
                                // The service does not say which team it refused, and the teams of
                                // one request may be those of several queries: each is asked alone,
                                // so only the refused ones fail and are remembered as refused
                                for (Flight flight : asks) {
                                    fetch(tokens, ImmutableList.of(flight), deadline);
                                }
                                return;
                            }
                            remember(asks, Refusal.TEAM_REFUSED, now + REFUSAL_RETENTION.toMillis());
                            return;
                        }
                        case TOKEN_REFUSED -> {
                            stats.requestRefused();
                            remember(asks, Refusal.TOKEN_REFUSED, now + REFUSAL_RETENTION.toMillis());
                            return;
                        }
                        case INVALID_RESPONSE -> {
                            stats.requestRefused();
                            // As for a session: the same login and team are not asked again at once
                            remember(asks, Refusal.INVALID_RESPONSE, now + REASK_GUARD.toMillis());
                            return;
                        }
                        case NOT_OFFERED, REQUEST_REFUSED -> {
                            stats.requestRefused();
                            Refused refused = refused(refusal.refusal(), now + REFUSAL_RETENTION.toMillis());
                            refusedForAll.set(refused);
                            finish(asks, refused);
                            return;
                        }
                    }
                }
            }
        }
    }

    /**
     * Waits until a request may be sent: after the time the service asked to
     * wait until, and within the catalog's own request budget.
     *
     * @return false when that is not before the deadline
     */
    private boolean awaitTurn(long deadline)
            throws InterruptedException
    {
        while (true) {
            long now = clock.millis();
            if (now >= deadline) {
                return false;
            }
            long wait = waitUntilMillis.get() - now;
            if (wait <= 0) {
                wait = budget.take(now);
                if (wait == 0) {
                    return true;
                }
            }
            waiter.waitUntil(now + Math.min(wait, deadline - now));
        }
    }

    private Answer sendRequest(HoglakeTokenSource tokens, Set<Long> teams, long deadline)
            throws InterruptedException
    {
        long remaining = deadline - clock.millis();
        if (remaining <= 0 || !requestsInFlight.tryAcquire(remaining, MILLISECONDS)) {
            return new HoglakeStorageSessionClient.Refused(Refusal.BUSY, Duration.ZERO);
        }
        try {
            stats.requestSent();
            return client.request(tokens, teams, Duration.ofMillis(remaining));
        }
        finally {
            requestsInFlight.release();
        }
    }

    private void remember(List<Flight> asks, Refusal refusal, long untilMillis)
    {
        Refused refused = refused(refusal, untilMillis);
        for (Flight flight : asks) {
            // In place of a session the login held for the team
            store(flight.key(), refused);
        }
        finish(asks, refused);
    }

    private void store(Key key, State state)
    {
        // A cache entry cannot be replaced; this is the only writer of the key while its ask is in progress
        entries.invalidate(key);
        uncheckedCacheGet(entries, key, () -> state);
    }

    /**
     * Ends the asks: a later caller starts a new one, and the waiting ones get the result.
     */
    private void finish(Iterable<Flight> asks, State state)
    {
        synchronized (lock) {
            for (Flight flight : asks) {
                flights.remove(flight.key(), flight);
            }
        }
        for (Flight flight : asks) {
            flight.future().complete(state);
        }
    }

    private static Refused refused(Refusal refusal, long untilMillis)
    {
        TrinoException failure = refusal.failure();
        return new Refused(failure.getErrorCode(), failure.getMessage(), untilMillis);
    }

    /**
     * What a session is kept under. The mount is the same for every key of one
     * instance; it is part of the key so that a second mount could never share
     * an entry with the first.
     */
    private record Key(String login, String mount, long team)
    {
        private Key
        {
            requireNonNull(login, "login is null");
            requireNonNull(mount, "mount is null");
        }

        @Override
        public String toString()
        {
            return "Key[redacted]";
        }
    }

    private record Flight(Key key, CompletableFuture<State> future) {}

    private sealed interface State
            permits Held, Refused {}

    /**
     * @param askedAtMillis when the request that returned the session was answered
     */
    private record Held(HoglakeStorageSession session, long askedAtMillis)
            implements State {}

    /**
     * A refusal with the only text a user sees for it, remembered until {@code untilMillis}.
     */
    private record Refused(ErrorCode errorCode, String message, long untilMillis)
            implements State
    {
        public TrinoException failure()
        {
            return new TrinoException(() -> errorCode, message);
        }
    }

    /**
     * The catalog's own request budget: a bucket of {@link #REQUEST_BURST}
     * requests that fills at {@link #REQUESTS_PER_SECOND} a second.
     */
    private static final class RequestBudget
    {
        private double requests = REQUEST_BURST;
        private boolean used;
        private long filledAtMillis;

        /**
         * Takes one request, or returns how many milliseconds to wait for one.
         */
        public synchronized long take(long nowMillis)
        {
            if (used && nowMillis > filledAtMillis) {
                requests = Math.min(REQUEST_BURST, requests + (nowMillis - filledAtMillis) * REQUESTS_PER_SECOND / 1000.0);
            }
            if (!used || nowMillis > filledAtMillis) {
                filledAtMillis = nowMillis;
            }
            used = true;
            if (requests >= 1) {
                requests -= 1;
                return 0;
            }
            return Math.max(1, (long) Math.ceil((1 - requests) * 1000 / REQUESTS_PER_SECOND));
        }
    }
}
