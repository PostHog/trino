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
package io.trino.hogql;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.trino.hogql.HogQlCompilationEvent.Phase;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class HogQlCompilationStats
        implements HogQlCompilationObserver
{
    private final CounterStat completedCompilations = new CounterStat();
    private final CounterStat successfulCompilations = new CounterStat();
    private final CounterStat userErrorFailures = new CounterStat();
    private final CounterStat internalErrorFailures = new CounterStat();
    private final CounterStat externalErrorFailures = new CounterStat();
    private final CounterStat insufficientResourcesFailures = new CounterStat();
    private final TimeStat totalTime = new TimeStat(NANOSECONDS);
    private final TimeStat compilationTime = new TimeStat(NANOSECONDS);
    private final TimeStat parseTime = new TimeStat(NANOSECONDS);
    private final TimeStat bindTime = new TimeStat(NANOSECONDS);
    private final TimeStat lowerTime = new TimeStat(NANOSECONDS);
    private final TimeStat parameterBindingTime = new TimeStat(NANOSECONDS);
    private final AtomicLong lastCatalogGeneration = new AtomicLong(-1);

    @Override
    public void compilationCompleted(HogQlCompilationEvent event)
    {
        completedCompilations.update(1);
        totalTime.addNanos(event.totalNanos());
        event.dimensions().catalogGeneration().ifPresent(lastCatalogGeneration::set);
        event.phaseNanos().forEach((phase, duration) -> phaseTime(phase).addNanos(duration));
        switch (event.outcome()) {
            case SUCCESS -> successfulCompilations.update(1);
            case USER_ERROR -> userErrorFailures.update(1);
            case INTERNAL_ERROR -> internalErrorFailures.update(1);
            case EXTERNAL_ERROR -> externalErrorFailures.update(1);
            case INSUFFICIENT_RESOURCES -> insufficientResourcesFailures.update(1);
        }
    }

    private TimeStat phaseTime(Phase phase)
    {
        return switch (phase) {
            case COMPILATION -> compilationTime;
            case PARSE -> parseTime;
            case BIND -> bindTime;
            case LOWER -> lowerTime;
            case PARAMETER_BINDING -> parameterBindingTime;
        };
    }

    @Managed
    @Nested
    public CounterStat getCompletedCompilations()
    {
        return completedCompilations;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulCompilations()
    {
        return successfulCompilations;
    }

    @Managed
    @Nested
    public CounterStat getUserErrorFailures()
    {
        return userErrorFailures;
    }

    @Managed
    @Nested
    public CounterStat getInternalErrorFailures()
    {
        return internalErrorFailures;
    }

    @Managed
    @Nested
    public CounterStat getExternalErrorFailures()
    {
        return externalErrorFailures;
    }

    @Managed
    @Nested
    public CounterStat getInsufficientResourcesFailures()
    {
        return insufficientResourcesFailures;
    }

    @Managed
    public long getLastCatalogGeneration()
    {
        return lastCatalogGeneration.get();
    }

    @Managed
    @Nested
    public TimeStat getTotalTime()
    {
        return totalTime;
    }

    @Managed
    @Nested
    public TimeStat getCompilationTime()
    {
        return compilationTime;
    }

    @Managed
    @Nested
    public TimeStat getParseTime()
    {
        return parseTime;
    }

    @Managed
    @Nested
    public TimeStat getBindTime()
    {
        return bindTime;
    }

    @Managed
    @Nested
    public TimeStat getLowerTime()
    {
        return lowerTime;
    }

    @Managed
    @Nested
    public TimeStat getParameterBindingTime()
    {
        return parameterBindingTime;
    }
}
