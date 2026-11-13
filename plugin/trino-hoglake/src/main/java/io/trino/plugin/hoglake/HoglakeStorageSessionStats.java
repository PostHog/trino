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

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters of one catalog's S3 sessions for the posthog schema, since the
 * node started. A coordinator counts what it asked the credential service
 * for; every node counts the session splits it started to read.
 */
public final class HoglakeStorageSessionStats
{
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong sessionsIssued = new AtomicLong();
    private final AtomicLong sessionsReused = new AtomicLong();
    private final AtomicLong refusals = new AtomicLong();
    private final AtomicLong waits = new AtomicLong();
    private final AtomicLong splitsStarted = new AtomicLong();
    private final AtomicLong leastMillisLeftAtStart = new AtomicLong(Long.MAX_VALUE);

    public void requestSent()
    {
        requests.incrementAndGet();
    }

    public void sessionsIssued(int count)
    {
        sessionsIssued.addAndGet(count);
    }

    public void sessionReused()
    {
        sessionsReused.incrementAndGet();
    }

    public void requestRefused()
    {
        refusals.incrementAndGet();
    }

    public void requestWaited()
    {
        waits.incrementAndGet();
    }

    /**
     * A worker starts the storage calls of a split whose session has this long left.
     */
    public void splitStarted(long millisLeft)
    {
        splitsStarted.incrementAndGet();
        leastMillisLeftAtStart.accumulateAndGet(millisLeft, Math::min);
    }

    /**
     * Requests sent to the credential service.
     */
    @Managed
    public long getRequests()
    {
        return requests.get();
    }

    @Managed
    public long getSessionsIssued()
    {
        return sessionsIssued.get();
    }

    /**
     * Sessions handed out from this node's cache, without a request.
     */
    @Managed
    public long getSessionsReused()
    {
        return sessionsReused.get();
    }

    /**
     * Requests that were answered with a refusal that is not a request to wait.
     */
    @Managed
    public long getRefusals()
    {
        return refusals.get();
    }

    /**
     * Requests that the service asked to send again later, or that it did not answer.
     */
    @Managed
    public long getWaits()
    {
        return waits.get();
    }

    /**
     * Splits of the posthog schema whose storage calls this node started with a session.
     */
    @Managed
    public long getSplitsStarted()
    {
        return splitsStarted.get();
    }

    /**
     * The least time a session had left when this node started a split with
     * it, or -1 when it has started none. A value near zero means that splits
     * wait for most of their session's lifetime before they are read.
     */
    @Managed
    public long getLeastMillisLeftAtStart()
    {
        long least = leastMillisLeftAtStart.get();
        if (least == Long.MAX_VALUE) {
            return -1;
        }
        return least;
    }
}
