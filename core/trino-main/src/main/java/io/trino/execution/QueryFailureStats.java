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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class QueryFailureStats
{
    private static final Logger log = Logger.get(QueryFailureStats.class);

    private final MBeanExporter exporter;
    private final int maxFailureSeries;
    @GuardedBy("this")
    private final Map<FailureKey, FailureCounter> counters = new HashMap<>();
    @GuardedBy("this")
    private final List<MBeanExport> exports = new ArrayList<>();
    @GuardedBy("this")
    private int failureSeries;
    @GuardedBy("this")
    private boolean destroyed;

    @Inject
    public QueryFailureStats(MBeanExporter exporter, QueryManagerConfig config)
    {
        this(exporter, requireNonNull(config, "config is null").getMaxInfrastructureFailureSeries());
    }

    QueryFailureStats(MBeanExporter exporter, int maxFailureSeries)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        checkArgument(maxFailureSeries > 0, "maxFailureSeries must be positive");
        this.maxFailureSeries = maxFailureSeries;
    }

    public synchronized void recordFailure(String user, ErrorCode errorCode)
    {
        requireNonNull(user, "user is null");
        requireNonNull(errorCode, "errorCode is null");
        if (destroyed || errorCode.getType() == ErrorType.USER_ERROR) {
            return;
        }

        FailureKey key = new FailureKey(user, errorCode.getName(), errorCode.getType(), false);
        FailureCounter counter = counters.get(key);
        if (counter == null) {
            if (failureSeries >= maxFailureSeries) {
                key = new FailureKey("__other__", "__other__", errorCode.getType(), true);
            }
            else {
                failureSeries++;
            }
            counter = counters.computeIfAbsent(key, this::exportCounter);
        }
        counter.increment();
    }

    private FailureCounter exportCounter(FailureKey key)
    {
        var counter = new FailureCounter();
        try {
            exports.add(exporter.exportWithGeneratedName(counter, QueryFailureStats.class, ImmutableMap.of(
                    "name", "QueryFailureStats",
                    "user", key.user(),
                    "errorCode", key.errorCode(),
                    "errorType", key.errorType().name(),
                    "overflow", Boolean.toString(key.overflow()))));
        }
        catch (JmxException e) {
            log.warn(e, "Could not export a query failure counter");
        }
        return counter;
    }

    @PreDestroy
    public synchronized void destroy()
    {
        destroyed = true;
        for (MBeanExport export : exports) {
            try {
                export.unexport();
            }
            catch (JmxException e) {
                log.warn(e, "Could not unexport a query failure counter");
            }
        }
        exports.clear();
        counters.clear();
    }

    private record FailureKey(String user, String errorCode, ErrorType errorType, boolean overflow) {}

    public static final class FailureCounter
    {
        private final AtomicLong totalCount = new AtomicLong();

        public void increment()
        {
            totalCount.incrementAndGet();
        }

        @Managed
        public long getTotalCount()
        {
            return totalCount.get();
        }
    }
}
