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

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestHogQlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HogQlConfig.class)
                .setEnabled(false)
                .setCompilationThreads(2)
                .setCompilationQueueCapacity(32)
                .setCompilationTimeout(new Duration(10, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        assertFullMapping(
                Map.of(
                        "hogql.enabled", "true",
                        "hogql.compilation-threads", "4",
                        "hogql.compilation-queue-capacity", "128",
                        "hogql.compilation-timeout", "3s"),
                new HogQlConfig()
                        .setEnabled(true)
                        .setCompilationThreads(4)
                        .setCompilationQueueCapacity(128)
                        .setCompilationTimeout(new Duration(3, SECONDS)));
    }
}
