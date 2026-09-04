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

import io.trino.sql.SqlFormatter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHogQlTemplateStringCompiler
{
    @Test
    public void testLowersTemplateStringToStockTrinoAst()
    {
        String sql = SqlFormatter.formatSql(new HogQlCompiler().compile(
                "SELECT f'{year}-{month}-{day}' FROM calendar"));

        assertThat(sql)
                .contains("concat(")
                .contains("CAST(year AS varchar)")
                .contains("CAST(month AS varchar)")
                .contains("CAST(day AS varchar)");
    }
}
