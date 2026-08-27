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
package io.trino.hogql.parser;

import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FrameBoundType;
import io.trino.hogql.parser.tree.HogQlQuery.FrameType;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.NullTreatment;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.SortDirection;
import io.trino.hogql.parser.tree.HogQlQuery.WindowReference;
import io.trino.hogql.parser.tree.HogQlQuery.WindowSpecification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlWindowParser
{
    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testBuildsInlineWindowSpecificationAndFrame()
    {
        String hogql = "SELECT sum({value}) FILTER (WHERE {filter}) OVER " +
                "(PARTITION BY {partition}, team_id ORDER BY {sort} DESC NULLS LAST " +
                "ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)";

        HogQlQuery query = parser.parseStatement(hogql);
        FunctionCall function = (FunctionCall) ((ExpressionProjection) query.projections().getFirst()).expression();

        assertThat(function.name().value()).isEqualTo("sum");
        assertThat(function.arguments()).singleElement().isInstanceOf(Placeholder.class);
        assertThat(function.filter()).get().isInstanceOf(Placeholder.class);
        WindowSpecification window = (WindowSpecification) function.window().orElseThrow();
        assertThat(window.partitionBy()).hasSize(2);
        assertThat(window.orderBy()).singleElement().satisfies(sortItem -> {
            assertThat(sortItem.expression()).isInstanceOf(Placeholder.class);
            assertThat(sortItem.direction()).isEqualTo(SortDirection.DESCENDING);
        });
        assertThat(window.span().startOffset()).isEqualTo(hogql.indexOf("PARTITION"));
        assertThat(window.span().endOffset()).isEqualTo(hogql.length() - 1);
        assertThat(window.frame()).get().satisfies(frame -> {
            assertThat(frame.type()).isEqualTo(FrameType.ROWS);
            assertThat(frame.start().type()).isEqualTo(FrameBoundType.PRECEDING);
            assertThat(frame.start().value()).get().isInstanceOf(Placeholder.class);
            assertThat(frame.end()).get().extracting(HogQlQuery.FrameBound::type).isEqualTo(FrameBoundType.CURRENT_ROW);
        });
    }

    @Test
    public void testBuildsNamedWindowReferenceAndDefinitions()
    {
        HogQlQuery query = parser.parseStatement(
                "SELECT row_number() OVER recent " +
                        "WINDOW recent AS (PARTITION BY team_id ORDER BY timestamp), " +
                        "trailing AS (ORDER BY timestamp RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)");

        FunctionCall function = (FunctionCall) ((ExpressionProjection) query.projections().getFirst()).expression();
        assertThat(function.window()).get().isInstanceOf(WindowReference.class);
        WindowReference reference = (WindowReference) function.window().orElseThrow();
        assertThat(reference.name().value()).isEqualTo("recent");
        assertThat(query.windows()).hasSize(2);
        assertThat(query.windows()).extracting(definition -> definition.name().value())
                .containsExactly("recent", "trailing");
        assertThat(query.windows().getFirst().specification().partitionBy()).hasSize(1);
        assertThat(query.windows().get(1).specification().frame()).get().satisfies(frame -> {
            assertThat(frame.type()).isEqualTo(FrameType.RANGE);
            assertThat(frame.start().type()).isEqualTo(FrameBoundType.CURRENT_ROW);
            assertThat(frame.end()).get().extracting(HogQlQuery.FrameBound::type).isEqualTo(FrameBoundType.UNBOUNDED_FOLLOWING);
        });
    }

    @Test
    public void testBuildsCanonicalWindowNullTreatment()
    {
        HogQlQuery query = parser.parseStatement("SELECT first_value(value) OVER (ORDER BY timestamp) IGNORE NULLS");

        FunctionCall function = (FunctionCall) ((ExpressionProjection) query.projections().getFirst()).expression();
        assertThat(function.nullTreatment()).contains(NullTreatment.IGNORE);
        assertThat(function.window()).get().isInstanceOf(WindowSpecification.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT quantile(0.9)(value) OVER ()",
            "SELECT count()(DISTINCT value) OVER named WINDOW named AS ()",
    })
    public void testRejectsParametricWindowFunctions(String hogql)
    {
        assertThatThrownBy(() -> parser.parseStatement(hogql))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("parametric window function");
    }

    @Test
    public void testRejectsNullTreatmentOutsideWindowFunction()
    {
        assertThatThrownBy(() -> parser.parseStatement("SELECT first_value(value) IGNORE NULLS"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("IGNORE NULLS outside window function");
    }
}
