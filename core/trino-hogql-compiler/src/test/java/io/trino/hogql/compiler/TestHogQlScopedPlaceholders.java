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
package io.trino.hogql.compiler;

import io.trino.hogql.compiler.HogQlTypedValue.NumberValue;
import io.trino.hogql.compiler.HogQlTypedValue.StringValue;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlScopedPlaceholders
{
    private final HogQlCompiler compiler = new HogQlCompiler();

    @Test
    public void testBindsVariablesAndFiltersWithoutInterpolation()
    {
        HogQlCompilationResult result = compiler.compile(envelope(
                "SELECT {variables.organization_id}, {filters.date_from}, {variables.organization_id}",
                Map.of("organization_id", new HogQlTypedValue("bigint", new NumberValue("42"))),
                Map.of("date_from", new HogQlTypedValue("varchar", new StringValue("2026-01-01")))));

        assertThat(result.parameterNames()).containsExactly(
                "variables.organization_id",
                "filters.date_from",
                "variables.organization_id");
        assertThat(result.statement().toString()).doesNotContain("42", "2026-01-01");
    }

    @Test
    public void testReportsMissingScopedBinding()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile(envelope("SELECT {variables.organization_id}", Map.of(), Map.of())));

        assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_BINDING_ERROR.toErrorCode());
        assertThat(exception).hasMessageContaining("Missing HogQL parameter bindings: variables.organization_id");
    }

    @Test
    public void testLeavesOtherDottedPlaceholderExpressionsUnsupported()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile(envelope("SELECT {payload.organization_id}", Map.of(), Map.of())));

        assertThat(exception.getErrorCode()).isEqualTo(HogQlErrorCode.HOGQL_SYNTAX_ERROR.toErrorCode());
        assertThat(exception).hasMessageContaining("non-name placeholder");
    }

    private static HogQlCompileEnvelope envelope(
            String query,
            Map<String, HogQlTypedValue> variables,
            Map<String, HogQlTypedValue> filters)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                variables,
                filters,
                Map.of(),
                OptionalLong.empty());
    }
}
