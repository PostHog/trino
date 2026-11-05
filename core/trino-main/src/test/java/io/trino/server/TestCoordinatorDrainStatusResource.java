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
package io.trino.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCoordinatorDrainStatusResource
{
    @Test
    public void testIdleOnlyWithoutAnyWork()
    {
        CoordinatorDrainStatusResource resource = new CoordinatorDrainStatusResource("node", "abcde", () -> false, () -> false, () -> false, () -> false);

        CoordinatorDrainStatusResource.CoordinatorDrainStatus status = resource.drainStatus();
        assertThat(status.idle()).isTrue();
        assertThat(status.nodeId()).isEqualTo("node");
        assertThat(status.coordinatorId()).isEqualTo("abcde");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    public void testEveryRegistryPreventsIdle(int busyRegistry)
    {
        CoordinatorDrainStatusResource resource = new CoordinatorDrainStatusResource(
                "node",
                "abcde",
                () -> busyRegistry == 0,
                () -> busyRegistry == 1,
                () -> busyRegistry == 2,
                () -> busyRegistry == 3);

        assertThat(resource.drainStatus().idle()).isFalse();
    }

    @Test
    public void testHandoffToLaterRegistry()
    {
        // A submission that is created while the pending registry is read is seen by the next one.
        AtomicBoolean created = new AtomicBoolean();
        CoordinatorDrainStatusResource resource = new CoordinatorDrainStatusResource(
                "node",
                "abcde",
                () -> {
                    created.set(true);
                    return false;
                },
                created::get,
                () -> false,
                () -> false);

        assertThat(resource.drainStatus().idle()).isFalse();
    }

    @Test
    public void testRegistryFailureDoesNotProduceIdle()
    {
        CoordinatorDrainStatusResource resource = new CoordinatorDrainStatusResource(
                "node",
                "abcde",
                () -> false,
                () -> false,
                () -> {
                    throw new IllegalStateException("unavailable");
                },
                () -> false);

        assertThatThrownBy(resource::drainStatus).isInstanceOf(IllegalStateException.class);
    }
}
