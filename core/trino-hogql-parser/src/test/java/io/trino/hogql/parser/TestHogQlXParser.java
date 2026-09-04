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
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlXParser
{
    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testLowersNestedTagToHxTuple()
    {
        HogQlQuery query = parser.parseStatement(
                "SELECT <a href='https://example.com' target>{event}<strong>Bold!</strong></a> FROM events");
        TupleExpression tag = (TupleExpression) ((ExpressionProjection) query.projections().getFirst()).expression();

        assertThat(tag.values()).hasSize(8);
        assertThat(tag.values().get(0)).isEqualTo(new Literal(HogQlQuery.LiteralKind.STRING, "__hx_tag", tag.values().get(0).span()));
        assertThat(tag.values().get(1)).isInstanceOfSatisfying(
                Literal.class,
                literal -> assertThat(literal.value()).isEqualTo("a"));
        assertThat(tag.values().get(7)).isInstanceOfSatisfying(
                TupleExpression.class,
                children -> assertThat(children.values()).hasSize(2));
    }

    @Test
    public void testRejectsMismatchedClosingTag()
    {
        assertThatThrownBy(() -> parser.parseStatement("SELECT <a>text</strong>"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("mismatched HogQLX closing tag");
    }
}
