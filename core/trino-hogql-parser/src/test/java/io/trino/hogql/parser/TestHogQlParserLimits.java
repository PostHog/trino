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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlParserLimits
{
    private static final int LARGE_VALID_QUERY_TOKEN_COUNT = 160_000;

    @Test
    public void testRejectsTokenBomb()
    {
        HogQlParser parser = new HogQlParser(new HogQlParserLimits(5, 100, 1_000));

        assertThatThrownBy(() -> parser.parseSyntax("SELECT one, two, three"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("token limit exceeded");
    }

    @Test
    public void testRejectsDeeplyNestedExpression()
    {
        HogQlParser parser = new HogQlParser(new HogQlParserLimits(1_000, 32, 10_000));
        String query = "SELECT " + "(".repeat(64) + "1" + ")".repeat(64);

        assertThatThrownBy(() -> parser.parseSyntax(query))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("parse depth limit exceeded");
    }

    @Test
    public void testRejectsParseTreeNodeBomb()
    {
        HogQlParser parser = new HogQlParser(new HogQlParserLimits(1_000, 100, 25));

        assertThatThrownBy(() -> parser.parseSyntax("SELECT one, two, three, four"))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("parse tree node limit exceeded");
    }

    @Test
    public void testAcceptsInputWithinInjectedLimits()
    {
        HogQlParser parser = new HogQlParser(new HogQlParserLimits(100, 100, 1_000));

        assertThatCode(() -> parser.parseSyntax("SELECT event FROM events WHERE event = 'signup'"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testDefaultLimitsAcceptLargeValidQuery()
    {
        assertThatCode(() -> new HogQlParser().parseSyntax(queryWithTokenCount(LARGE_VALID_QUERY_TOKEN_COUNT)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testDefaultTokenLimitRemainsBounded()
    {
        int tokenCount = HogQlParserLimits.defaults().maxTokens() + 1;

        assertThatThrownBy(() -> new HogQlParser().parseSyntax(queryWithTokenCount(tokenCount)))
                .isInstanceOf(HogQlParsingException.class)
                .hasMessageContaining("token limit exceeded");
    }

    private static String queryWithTokenCount(int tokenCount)
    {
        if (tokenCount < 3) {
            throw new IllegalArgumentException("tokenCount must be at least 3");
        }
        return "SELECT" + " /**/".repeat(tokenCount - 3) + " 1";
    }
}
