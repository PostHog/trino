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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.plugin.hoglake.HoglakeStorageSessions.LoginSessions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_CATALOG_UNAVAILABLE;
import static io.trino.plugin.hoglake.HoglakePosthogSchema.SPLITS_PER_BATCH;
import static java.util.Objects.requireNonNull;

/**
 * The splits of a posthog schema table, handed out with the S3 session of
 * each split's team attached.
 *
 * <p>The file list is planned whole, as for any table. The sessions are not:
 * a batch of at most {@value HoglakePosthogSchema#SPLITS_PER_BATCH} splits
 * gets its sessions when the engine asks for it, so a session is asked for
 * only when its team's splits are about to run and is at most one batch old
 * when the engine has it. The splits are in team order, so a team's session
 * is in use only while that team's own splits are handed out.
 *
 * <p>The source belongs to the login that planned the scan: it holds that
 * login's sessions and can reach no other's. The wait for the credential
 * service is on the connector's executor, never on the thread that asks.
 *
 * <p>Before a batch is handed out, every split in it that reads storage is
 * checked against its team's prefix. One file outside its prefix, or one team
 * without a session, fails the query: no split of that batch is handed out and
 * no file is skipped.
 */
final class HoglakeStorageSessionSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HoglakeStorageSessionSplitSource.class);

    static final String SESSION_SPLITS = "storageSessionSplits";
    static final String SESSION_TEAMS = "storageSessionTeams";
    static final String SESSION_WAIT_MILLIS = "storageSessionWaitMillis";
    static final String SESSION_SECONDS_LEFT_AT_HAND_OUT = "storageSessionSecondsLeftAtHandOut";

    /**
     * A split and the team whose prefix holds its file.
     *
     * @param readsStorage false for a split that is answered from catalog counts alone: it needs no session
     */
    record TeamSplit(long teamId, HoglakeSplit split, boolean readsStorage)
    {
        TeamSplit
        {
            requireNonNull(split, "split is null");
        }
    }

    private final List<TeamSplit> splits;
    private final LoginSessions sessions;
    private final Executor executor;
    private final Clock clock;

    private volatile int offset;
    private volatile boolean closed;
    private volatile Metrics metrics = Metrics.EMPTY;

    HoglakeStorageSessionSplitSource(List<TeamSplit> splits, LoginSessions sessions, Executor executor, Clock clock)
    {
        // Stable: the splits of one team, and the ranges of one file, keep their order
        this.splits = requireNonNull(splits, "splits is null").stream()
                .sorted(Comparator.comparingLong(TeamSplit::teamId))
                .collect(toImmutableList());
        this.sessions = requireNonNull(sessions, "sessions is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.clock = requireNonNull(clock, "clock is null");
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        int from = offset;
        if (closed || from >= splits.size()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }
        int to = Math.min(splits.size(), from + Math.min(maxSize, SPLITS_PER_BATCH));
        List<TeamSplit> batch = splits.subList(from, to);
        // Only the teams of this batch: a session asked for ahead of its splits ages unused
        Set<Long> teams = new TreeSet<>();
        for (TeamSplit split : batch) {
            if (split.readsStorage()) {
                teams.add(split.teamId());
            }
        }
        if (teams.isEmpty()) {
            offset = to;
            return CompletableFuture.completedFuture(batch.stream().map(TeamSplit::split).collect(toImmutableList()));
        }
        CompletableFuture<List<ConnectorSplit>> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    List<ConnectorSplit> attached = attach(batch, teams);
                    offset = to;
                    future.complete(attached);
                }
                catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        }
        catch (RejectedExecutionException e) {
            // The catalog is closing
            future.completeExceptionally(new TrinoException(HOGLAKE_CATALOG_UNAVAILABLE, "The catalog is shutting down"));
        }
        return future;
    }

    private List<ConnectorSplit> attach(List<TeamSplit> batch, Set<Long> teams)
    {
        long started = clock.millis();
        Map<Long, HoglakeStorageSession> issued = sessions.sessions(teams);
        long now = clock.millis();
        ImmutableList.Builder<ConnectorSplit> attached = ImmutableList.builder();
        long sessionSplits = 0;
        long leastLeftMillis = Long.MAX_VALUE;
        for (TeamSplit teamSplit : batch) {
            HoglakeSplit split = teamSplit.split();
            if (!teamSplit.readsStorage()) {
                attached.add(split);
                continue;
            }
            HoglakeStorageSession session = requireNonNull(issued.get(teamSplit.teamId()), "no session for a team that was asked for");
            try {
                // The team is the scan's, the prefix is the credential service's; the worker checks again
                session.checkCovers(split);
            }
            catch (TrinoException e) {
                log.debug("A file of the posthog schema is outside the prefix of its team: team %s, data file %s", teamSplit.teamId(), split.dataFileId());
                throw e;
            }
            attached.add(split.withSession(session));
            sessionSplits++;
            leastLeftMillis = Math.min(leastLeftMillis, session.expirationEpochMillis() - now);
        }
        record(sessionSplits, teams.size(), now - started, leastLeftMillis);
        return attached.build();
    }

    private void record(long sessionSplits, long teams, long waitMillis, long leastLeftMillis)
    {
        Metrics batch = new Metrics(ImmutableMap.<String, Metric<?>>of(
                SESSION_SPLITS, new LongCount(sessionSplits),
                SESSION_TEAMS, new LongCount(teams),
                SESSION_WAIT_MILLIS, new LongCount(waitMillis),
                // One value for each batch: the session of the batch with the least time left
                SESSION_SECONDS_LEFT_AT_HAND_OUT, TDigestHistogram.fromValue(leastLeftMillis / 1000.0)));
        metrics = metrics.mergeWith(batch);
    }

    @Override
    public void close()
    {
        // A request in progress is not cancelled: other queries of the login may wait for it
        closed = true;
    }

    @Override
    public boolean isFinished()
    {
        return closed || offset >= splits.size();
    }

    @Override
    public Metrics getMetrics()
    {
        return metrics;
    }
}
