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
package io.trino.plugin.hoglake.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that moves only when a test moves it.
 */
public final class ManualClock
        extends Clock
{
    private final AtomicLong millis;

    public ManualClock(Instant start)
    {
        millis = new AtomicLong(start.toEpochMilli());
    }

    public void set(long epochMillis)
    {
        millis.set(epochMillis);
    }

    public void advance(Duration duration)
    {
        millis.addAndGet(duration.toMillis());
    }

    /**
     * Moves the clock forward to a time, or leaves it when it is already past it:
     * what a wait until that time does to the time, however many wait at once.
     */
    public void advanceTo(long epochMillis)
    {
        millis.accumulateAndGet(epochMillis, Math::max);
    }

    @Override
    public long millis()
    {
        return millis.get();
    }

    @Override
    public Instant instant()
    {
        return Instant.ofEpochMilli(millis());
    }

    @Override
    public ZoneId getZone()
    {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone)
    {
        throw new UnsupportedOperationException();
    }
}
