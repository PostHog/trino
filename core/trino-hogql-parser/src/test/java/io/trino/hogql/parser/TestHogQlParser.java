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
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlParser
{
    private final HogQlParser parser = new HogQlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT",
            "SELECT count(*)",
            "SELECT * FROM first JOIN second",
            "DROP TABLE events",
            "SELECT 1; SELECT 2",
            "SELECT * FROM",
            "SELECT {value}",
            "SELECT 1 GROUP BY 1",
    })
    public void testRejectsSyntaxWithoutAnAstMapping(String hogql)
    {
        assertThatThrownBy(() -> parser.parseStatement(hogql))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageStartingWith("line ");
    }

    @Test
    public void testUsesCanonicalGrammarForExpressionsAndWhereClause()
    {
        HogQlQuery query = parser.parseStatement("SELECT event + 1 * 2 FROM events WHERE event >= 3 AND NOT false");

        assertThat(query.projections()).hasSize(1);
        assertThat(query.from()).isPresent();
        assertThat(query.where()).isPresent();
    }

    @Test
    public void testSourceSpansUseUnicodeCodePointOffsetsAndEndPositions()
    {
        HogQlQuery query = parser.parseStatement("SELECT '😀',\n event");
        ExpressionProjection projection = (ExpressionProjection) query.projections().get(1);
        ColumnReference reference = (ColumnReference) projection.expression();

        assertThat(query.span()).isEqualTo(new HogQlQuery.SourceSpan(0, 18, 1, 1, 2, 7));
        assertThat(reference.span()).isEqualTo(new HogQlQuery.SourceSpan(13, 18, 2, 2, 2, 7));
    }
}
