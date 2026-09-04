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

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface HogQlExchangeRateSnapshotCache
{
    Optional<HogQlExchangeRateSnapshot> currentSnapshot(OptionalLong expectedGeneration);

    default Optional<HogQlExchangeRateSnapshot> currentSnapshot()
    {
        return currentSnapshot(OptionalLong.empty());
    }

    default Optional<HogQlExchangeRateSnapshot> currentSnapshot(long expectedGeneration)
    {
        return currentSnapshot(OptionalLong.of(expectedGeneration));
    }

    static OptionalLong validateExpectedGeneration(OptionalLong expectedGeneration)
    {
        requireNonNull(expectedGeneration, "expectedGeneration is null");
        if (expectedGeneration.isPresent() && expectedGeneration.orElseThrow() <= 0) {
            throw new IllegalArgumentException("expected exchange-rate generation must be positive");
        }
        return expectedGeneration;
    }
}
