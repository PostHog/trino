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
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlTemplateStringParser
{
    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testDesugarsInterpolatedTemplateToTypedConcatenation()
    {
        HogQlQuery query = parser.parseStatement("SELECT f'{year}-{month}-{day}'");
        FunctionCall concat = (FunctionCall) ((ExpressionProjection) query.projections().getFirst()).expression();

        assertThat(concat.name().value()).isEqualTo("concat");
        assertThat(concat.arguments()).hasSize(5);
        assertThat(concat.arguments().get(0)).isInstanceOfSatisfying(
                FunctionCall.class,
                function -> assertThat(function.name().value()).isEqualTo("toString"));
        assertThat(concat.arguments().get(1)).isInstanceOfSatisfying(
                Literal.class,
                literal -> assertThat(literal.value()).isEqualTo("-"));
    }

    @Test
    public void testKeepsLiteralOnlyTemplateAsLiteral()
    {
        HogQlQuery query = parser.parseStatement("SELECT f'hello\\nworld'");
        Literal literal = (Literal) ((ExpressionProjection) query.projections().getFirst()).expression();

        assertThat(literal.value()).isEqualTo("hello\nworld");
    }

    @Test
    public void testKeepsEmptyTemplateAsEmptyLiteral()
    {
        HogQlQuery query = parser.parseStatement("SELECT f''");
        Literal literal = (Literal) ((ExpressionProjection) query.projections().getFirst()).expression();

        assertThat(literal.value()).isEmpty();
    }
}
