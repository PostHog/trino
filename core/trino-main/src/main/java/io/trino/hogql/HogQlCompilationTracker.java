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

import io.trino.hogql.HogQlCompilationEvent.Dimensions;
import io.trino.hogql.HogQlCompilationEvent.Outcome;
import io.trino.hogql.HogQlCompilationEvent.Phase;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static io.trino.hogql.HogQlCompilationEvent.Outcome.EXTERNAL_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.INSUFFICIENT_RESOURCES;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.INTERNAL_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.SUCCESS;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.USER_ERROR;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public final class HogQlCompilationTracker
{
    private final HogQlCompilationObserver observer;
    private Dimensions dimensions;
    private final LongSupplier ticker;
    private final long startedNanos;
    private final EnumMap<Phase, Long> phaseNanos = new EnumMap<>(Phase.class);

    private Phase failedPhase;
    private boolean completed;

    public HogQlCompilationTracker(HogQlCompilationObserver observer, Dimensions dimensions)
    {
        this(observer, dimensions, System::nanoTime);
    }

    HogQlCompilationTracker(HogQlCompilationObserver observer, Dimensions dimensions, LongSupplier ticker)
    {
        this.observer = requireNonNull(observer, "observer is null");
        this.dimensions = requireNonNull(dimensions, "dimensions is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        startedNanos = ticker.getAsLong();
    }

    public <T> T observe(Phase phase, Supplier<T> operation)
    {
        requireNonNull(phase, "phase is null");
        requireNonNull(operation, "operation is null");
        if (completed) {
            throw new IllegalStateException("HogQL compilation observation is complete");
        }
        long phaseStartedNanos = ticker.getAsLong();
        try {
            return operation.get();
        }
        catch (RuntimeException | Error failure) {
            failedPhase = phase;
            throw failure;
        }
        finally {
            phaseNanos.merge(phase, elapsedNanos(phaseStartedNanos, ticker.getAsLong()), Long::sum);
        }
    }

    public void succeeded()
    {
        complete(SUCCESS, Optional.empty());
    }

    public void catalogGeneration(OptionalLong catalogGeneration)
    {
        if (completed) {
            throw new IllegalStateException("HogQL compilation observation is complete");
        }
        dimensions = dimensions.withCatalogGeneration(requireNonNull(catalogGeneration, "catalogGeneration is null"));
    }

    public void failed(Throwable failure)
    {
        requireNonNull(failure, "failure is null");
        complete(outcome(failure), Optional.ofNullable(failedPhase));
    }

    private void complete(Outcome outcome, Optional<Phase> failedPhase)
    {
        if (completed) {
            throw new IllegalStateException("HogQL compilation observation is complete");
        }
        completed = true;
        observer.compilationCompleted(new HogQlCompilationEvent(
                dimensions,
                outcome,
                failedPhase,
                elapsedNanos(startedNanos, ticker.getAsLong()),
                Map.copyOf(phaseNanos)));
    }

    private static Outcome outcome(Throwable failure)
    {
        if (!(failure instanceof TrinoException trinoException)) {
            return INTERNAL_ERROR;
        }
        ErrorType errorType = trinoException.getErrorCode().getType();
        return switch (errorType) {
            case USER_ERROR -> USER_ERROR;
            case INTERNAL_ERROR -> INTERNAL_ERROR;
            case EXTERNAL -> EXTERNAL_ERROR;
            case INSUFFICIENT_RESOURCES -> INSUFFICIENT_RESOURCES;
        };
    }

    private static long elapsedNanos(long start, long end)
    {
        return max(0, end - start);
    }
}
