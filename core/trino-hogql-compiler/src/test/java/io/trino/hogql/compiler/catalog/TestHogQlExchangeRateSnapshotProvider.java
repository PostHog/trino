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
package io.trino.hogql.compiler.catalog;

import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot.ExchangeRate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlExchangeRateSnapshotProvider
{
    @Test
    public void testPinsOneExactGeneration()
    {
        AtomicReference<OptionalLong> requestedGeneration = new AtomicReference<>();
        HogQlExchangeRateSnapshotCache cache = expectedGeneration -> {
            requestedGeneration.set(expectedGeneration);
            return Optional.of(snapshot(7));
        };
        HogQlExchangeRateSnapshotProvider provider = HogQlExchangeRateSnapshotProvider.fromCache(cache);

        HogQlExchangeRateSnapshotProvider.PinnedSnapshot pinned = provider.pin(OptionalLong.of(7));

        assertThat(requestedGeneration).hasValue(OptionalLong.of(7));
        assertThat(pinned.generation()).isEqualTo(7);
        assertThat(pinned.snapshot()).isEqualTo(snapshot(7));
    }

    @Test
    public void testFailsClosedForUnavailableAndMismatchedGeneration()
    {
        HogQlExchangeRateSnapshotProvider unavailable = HogQlExchangeRateSnapshotProvider.fromCache(_ -> Optional.empty());
        assertThatThrownBy(() -> unavailable.pin(OptionalLong.empty()))
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> assertThat(exception.failure()).isEqualTo(Failure.UNAVAILABLE));

        HogQlExchangeRateSnapshotProvider mismatched = HogQlExchangeRateSnapshotProvider.fromCache(_ -> Optional.of(snapshot(2)));
        assertThatThrownBy(() -> mismatched.pin(OptionalLong.of(1)))
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> assertThat(exception.failure()).isEqualTo(Failure.GENERATION_MISMATCH));
    }

    private static HogQlExchangeRateSnapshot snapshot(long generation)
    {
        return new HogQlExchangeRateSnapshot(
                1,
                1,
                generation,
                "USD",
                10,
                List.of(new ExchangeRate("USD", "1970-01-01", "10000000000")));
    }
}
