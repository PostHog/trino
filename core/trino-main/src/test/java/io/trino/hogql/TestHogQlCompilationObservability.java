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
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.trino.hogql.HogQlCompilationEvent.Outcome.EXTERNAL_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.INTERNAL_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.SUCCESS;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.USER_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Phase.BIND;
import static io.trino.hogql.HogQlCompilationEvent.Phase.LOWER;
import static io.trino.hogql.HogQlCompilationEvent.Phase.PARSE;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_CATALOG_NOT_READY;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_SYNTAX_ERROR;
import static io.trino.hogql.parser.HogQlLanguageContract.current;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchRuntimeException;

public class TestHogQlCompilationObservability
{
    private static final String QUERY_SECRET = "literal-query-secret";
    private static final String VALUE_SECRET = "typed-value-secret";
    private static final String NAME_SECRET = "semantic-name-secret";

    @Test
    public void testEventContainsOnlyRedactedDimensionsAndPhaseTimings()
    {
        AtomicLong ticker = new AtomicLong();
        List<HogQlCompilationEvent> events = new ArrayList<>();
        HogQlCompilationTracker tracker = new HogQlCompilationTracker(events::add, Dimensions.fromEnvelope(envelope()), ticker::get);

        assertThat(tracker.observe(PARSE, () -> {
            ticker.addAndGet(11);
            return "parsed";
        })).isEqualTo("parsed");
        tracker.observe(BIND, () -> {
            ticker.addAndGet(13);
            return null;
        });
        ticker.addAndGet(17);
        tracker.succeeded();

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.dimensions()).isEqualTo(new Dimensions(
                    HogQlCompileEnvelope.PROTOCOL_VERSION,
                    current().languageVersion(),
                    1,
                    1,
                    1,
                    1,
                    OptionalLong.of(41)));
            assertThat(event.outcome()).isEqualTo(SUCCESS);
            assertThat(event.failedPhase()).isEmpty();
            assertThat(event.totalNanos()).isEqualTo(41);
            assertThat(event.phaseNanos()).containsExactlyInAnyOrderEntriesOf(Map.of(PARSE, 11L, BIND, 13L));
            assertThat(event.toString()).doesNotContain(QUERY_SECRET, VALUE_SECRET, NAME_SECRET);
        });
    }

    @ParameterizedTest
    @MethodSource("failures")
    public void testFailureOutcomeAndPhaseAreStable(Supplier<RuntimeException> failureSupplier, Outcome expectedOutcome)
    {
        List<HogQlCompilationEvent> events = new ArrayList<>();
        HogQlCompilationTracker tracker = new HogQlCompilationTracker(events::add, Dimensions.fromEnvelope(envelope()), () -> 0);

        RuntimeException failure = catchRuntimeException(() -> tracker.observe(LOWER, () -> {
            throw failureSupplier.get();
        }));
        tracker.failed(failure);

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.outcome()).isEqualTo(expectedOutcome);
            assertThat(event.failedPhase()).contains(LOWER);
            assertThat(event.toString()).doesNotContain(QUERY_SECRET, VALUE_SECRET, NAME_SECRET);
        });
    }

    @Test
    public void testStatsRecordOutcomesAndPhaseDurations()
    {
        HogQlCompilationStats stats = new HogQlCompilationStats();
        Dimensions dimensions = Dimensions.fromEnvelope(envelope());

        stats.compilationCompleted(new HogQlCompilationEvent(dimensions, SUCCESS, Optional.empty(), 100, Map.of(PARSE, 10L, BIND, 20L, LOWER, 30L)));
        stats.compilationCompleted(new HogQlCompilationEvent(dimensions, USER_ERROR, Optional.of(PARSE), 50, Map.of(PARSE, 40L)));

        assertThat(stats.getCompletedCompilations().getTotalCount()).isEqualTo(2);
        assertThat(stats.getSuccessfulCompilations().getTotalCount()).isEqualTo(1);
        assertThat(stats.getUserErrorFailures().getTotalCount()).isEqualTo(1);
        assertThat(stats.getInternalErrorFailures().getTotalCount()).isZero();
        assertThat(stats.getTotalTime().getAllTime().getCount()).isEqualTo(2);
        assertThat(stats.getParseTime().getAllTime().getCount()).isEqualTo(2);
        assertThat(stats.getBindTime().getAllTime().getCount()).isEqualTo(1);
        assertThat(stats.getLowerTime().getAllTime().getCount()).isEqualTo(1);
    }

    private static Stream<Arguments> failures()
    {
        return Stream.of(
                Arguments.of((Supplier<RuntimeException>) () -> new TrinoException(HOGQL_SYNTAX_ERROR, "synthetic syntax failure"), USER_ERROR),
                Arguments.of((Supplier<RuntimeException>) () -> new TrinoException(HOGQL_CATALOG_NOT_READY, "synthetic catalog failure"), EXTERNAL_ERROR),
                Arguments.of((Supplier<RuntimeException>) () -> new IllegalStateException("synthetic internal failure"), INTERNAL_ERROR));
    }

    private static HogQlCompileEnvelope envelope()
    {
        HogQlTypedValue typedValue = new HogQlTypedValue("varchar", new StringValue(VALUE_SECRET));
        return new HogQlCompileEnvelope(
                "SELECT '" + QUERY_SECRET + "'",
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                current().languageVersion(),
                Map.of(NAME_SECRET + "-parameter", typedValue),
                Map.of(NAME_SECRET + "-variable", typedValue),
                Map.of(NAME_SECRET + "-filter", typedValue),
                Map.of(NAME_SECRET + "-modifier", typedValue),
                OptionalLong.of(41));
    }
}
