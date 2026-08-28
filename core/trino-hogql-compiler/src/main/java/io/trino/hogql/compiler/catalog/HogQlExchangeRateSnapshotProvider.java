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

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface HogQlExchangeRateSnapshotProvider
{
    PinnedSnapshot pin(OptionalLong expectedGeneration);

    static HogQlExchangeRateSnapshotProvider fromCache(HogQlExchangeRateSnapshotCache cache)
    {
        requireNonNull(cache, "cache is null");
        return expectedGeneration -> {
            HogQlExchangeRateSnapshotCache.validateExpectedGeneration(expectedGeneration);
            HogQlExchangeRateSnapshot snapshot = cache.currentSnapshot(expectedGeneration)
                    .orElseThrow(() -> new HogQlExchangeRateException(Failure.UNAVAILABLE, "HogQL exchange-rate snapshot is unavailable"));
            if (expectedGeneration.isPresent() && snapshot.generation() != expectedGeneration.orElseThrow()) {
                throw new HogQlExchangeRateException(Failure.GENERATION_MISMATCH, "HogQL exchange-rate snapshot generation does not match the request");
            }
            return new PinnedSnapshot(snapshot);
        };
    }

    record PinnedSnapshot(HogQlExchangeRateSnapshot snapshot)
    {
        public PinnedSnapshot
        {
            snapshot = requireNonNull(snapshot, "snapshot is null");
        }

        public long generation()
        {
            return snapshot.generation();
        }
    }
}
