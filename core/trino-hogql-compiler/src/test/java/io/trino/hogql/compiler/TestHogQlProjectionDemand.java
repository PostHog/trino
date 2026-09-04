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

import io.trino.hogql.parser.HogQlParser;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlProjectionDemand
{
    private final HogQlParser parser = new HogQlParser();

    @Test
    public void testJoinUsingDemandsTheSharedDerivedColumn()
    {
        HogQlQuery query = parser.parseStatement("SELECT sub.event FROM (SELECT * FROM events) sub JOIN persons USING (name)");

        HogQlProjectionDemand.RequiredOutputs outputs = HogQlProjectionDemand.collect(query)
                .forAlias(new Identifier("sub", false, query.span()));

        assertThat(outputs.includes("event")).isTrue();
        assertThat(outputs.includes("name")).isTrue();
    }
}
