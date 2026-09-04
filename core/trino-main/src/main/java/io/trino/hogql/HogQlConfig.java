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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HogQlConfig
{
    private boolean enabled;
    private int compilationThreads = 2;
    private int compilationQueueCapacity = 32;
    private Duration compilationTimeout = new Duration(10, SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("hogql.enabled")
    @ConfigDescription("Enable the native HogQL query submission endpoint")
    public HogQlConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @Min(1)
    public int getCompilationThreads()
    {
        return compilationThreads;
    }

    @Config("hogql.compilation-threads")
    @ConfigDescription("Number of coordinator threads dedicated to HogQL compilation")
    public HogQlConfig setCompilationThreads(int compilationThreads)
    {
        this.compilationThreads = compilationThreads;
        return this;
    }

    @Min(0)
    public int getCompilationQueueCapacity()
    {
        return compilationQueueCapacity;
    }

    @Config("hogql.compilation-queue-capacity")
    @ConfigDescription("Maximum number of HogQL compilations waiting for a compiler thread")
    public HogQlConfig setCompilationQueueCapacity(int compilationQueueCapacity)
    {
        this.compilationQueueCapacity = compilationQueueCapacity;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getCompilationTimeout()
    {
        return compilationTimeout;
    }

    @Config("hogql.compilation-timeout")
    @ConfigDescription("Maximum wall time for one HogQL compilation, including queue wait")
    public HogQlConfig setCompilationTimeout(Duration compilationTimeout)
    {
        this.compilationTimeout = compilationTimeout;
        return this;
    }
}
