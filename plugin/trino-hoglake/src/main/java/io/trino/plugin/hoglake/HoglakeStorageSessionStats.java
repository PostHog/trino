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
 * Counters of what one catalog's coordinator asked the credential service
 * for, to obtain the S3 sessions of the posthog schema, since the node started.
 */
public final class HoglakeStorageSessionStats
{
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong sessionsIssued = new AtomicLong();
    private final AtomicLong sessionsReused = new AtomicLong();
    private final AtomicLong refusals = new AtomicLong();
    private final AtomicLong waits = new AtomicLong();

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
}
