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

import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryDrainStatusResource
{
    private static final QueryId QUERY_ID = new QueryId("20000101_000000_00000_abcde");

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    public void testEveryRegistryPreventsAbsenceProof(int presentRegistry)
    {
        QueryDrainStatusResource resource = new QueryDrainStatusResource(
                "node",
                "abcde",
                _ -> presentRegistry == 0,
                _ -> presentRegistry == 1,
                _ -> presentRegistry == 2,
                _ -> presentRegistry == 3);

        assertThat(resource.drainStatus(QUERY_ID).absent()).isFalse();
    }

    @Test
    public void testHandoffToLaterRegistry()
    {
        AtomicBoolean dispatched = new AtomicBoolean();
        QueryDrainStatusResource resource = new QueryDrainStatusResource(
                "node",
                "abcde",
                _ -> {
                    dispatched.set(true);
                    return false;
                },
                _ -> dispatched.get(),
                _ -> false,
                _ -> false);

        assertThat(resource.drainStatus(QUERY_ID).absent()).isFalse();
    }

    @Test
    public void testRegistryFailureDoesNotProduceProof()
    {
        QueryDrainStatusResource resource = new QueryDrainStatusResource(
                "node",
                "abcde",
                _ -> false,
                _ -> false,
                _ -> { throw new IllegalStateException("unavailable"); },
                _ -> false);

        assertThatThrownBy(() -> resource.drainStatus(QUERY_ID))
                .isInstanceOf(IllegalStateException.class);
    }
}
