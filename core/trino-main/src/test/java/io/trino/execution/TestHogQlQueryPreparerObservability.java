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

import io.trino.hogql.HogQlCompilationEvent;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlTypedValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static io.trino.execution.QuerySubmission.hogQl;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.SUCCESS;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.USER_ERROR;
import static io.trino.hogql.HogQlCompilationEvent.Phase.COMPILATION;
import static io.trino.hogql.HogQlCompilationEvent.Phase.PARAMETER_BINDING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlQueryPreparerObservability
{
    private static final String SECRET = "sensitive-observability-value";

    @Test
    public void testHogQlPreparationEmitsOneRedactedEventAndTrinoDoesNot()
    {
        List<HogQlCompilationEvent> events = new ArrayList<>();
        QueryPreparer queryPreparer = new QueryPreparer(new SqlParser(), new HogQlCompiler(), events::add);

        queryPreparer.prepareQuery(testSessionBuilder().build(), hogQl(envelope("SELECT {input}", Map.of("input", typedValue()))));
        queryPreparer.prepareQuery(testSessionBuilder().build(), "SELECT '" + SECRET + "'");

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.outcome()).isEqualTo(SUCCESS);
            assertThat(event.phaseNanos()).containsKeys(COMPILATION, PARAMETER_BINDING);
            assertThat(event.toString()).doesNotContain(SECRET);
        });
    }

    @Test
    public void testCompilerFailureRecordsRedactedOutcomeAndPhase()
    {
        List<HogQlCompilationEvent> events = new ArrayList<>();
        QueryPreparer queryPreparer = new QueryPreparer(new SqlParser(), new HogQlCompiler(), events::add);

        assertThatThrownBy(() -> queryPreparer.prepareQuery(
                testSessionBuilder().build(),
                hogQl(envelope("SELECT '" + SECRET, Map.of()))))
                .isInstanceOf(RuntimeException.class);

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.outcome()).isEqualTo(USER_ERROR);
            assertThat(event.failedPhase()).contains(COMPILATION);
            assertThat(event.toString()).doesNotContain(SECRET);
        });
    }

    private static HogQlCompileEnvelope envelope(String query, Map<String, HogQlTypedValue> parameters)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                io.trino.hogql.parser.HogQlLanguageContract.current().languageVersion(),
                parameters,
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.empty());
    }

    private static HogQlTypedValue typedValue()
    {
        return new HogQlTypedValue("varchar", new StringValue(SECRET));
    }
}
