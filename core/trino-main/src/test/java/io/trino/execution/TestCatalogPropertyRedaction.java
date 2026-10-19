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

import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;

import static io.trino.execution.CatalogPropertyRedaction.REDACTED_STATEMENT;
import static io.trino.execution.CatalogPropertyRedaction.definesCatalogProperties;
import static io.trino.execution.CatalogPropertyRedaction.mayDefineCatalogProperties;
import static io.trino.execution.CatalogPropertyRedaction.redactedSql;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCatalogPropertyRedaction
{
    private static final SqlParser PARSER = new SqlParser();

    @Test
    void testLexicalCheck()
    {
        assertThat(mayDefineCatalogProperties("CREATE CATALOG c USING x")).isTrue();
        assertThat(mayDefineCatalogProperties("  create\n\tcatalog c")).isTrue();
        assertThat(mayDefineCatalogProperties("/* a */ -- b\nCREATE/* c */CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE -- comment\n CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("EXECUTE IMMEDIATE 'CREATE CATALOG c USING x'")).isTrue();
        assertThat(mayDefineCatalogProperties("PREPARE p FROM CREATE CATALOG c USING x")).isTrue();
        assertThat(mayDefineCatalogProperties("EXPLAIN CREATE CATALOG c USING x")).isTrue();

        assertThat(mayDefineCatalogProperties("SELECT 1")).isFalse();
        assertThat(mayDefineCatalogProperties("CREATE TABLE catalog (x int)")).isTrue();
        assertThat(mayDefineCatalogProperties("SELECT * FROM create_catalog")).isFalse();
        assertThat(mayDefineCatalogProperties("SELECT xcreate catalogs")).isFalse();
        assertThat(mayDefineCatalogProperties("DROP CATALOG c")).isFalse();
        assertThat(mayDefineCatalogProperties("CATALOG then CREATE")).isFalse();
        assertThat(mayDefineCatalogProperties("SELECT '--' ; CREATE/*x*/CATALOG c USING x")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE /* unterminated CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE x CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE\u00e9CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE/**/ /*a*/--b\n\tCATALOG")).isTrue();
    }

    @Test
    @Timeout(10)
    void testLexicalCheckIsLinear()
    {
        // Any client can send text to this check, so crafted text must not make it backtrack:
        // a pattern alternating over comments took exponential time on exactly this shape
        String comments = "CREATE/*" + "*//*".repeat(200_000);
        assertThat(mayDefineCatalogProperties(comments)).isFalse();
        assertThat(mayDefineCatalogProperties("CREATE ".repeat(200_000))).isFalse();
        assertThat(mayDefineCatalogProperties(comments + "*/CATALOG")).isTrue();
        assertThat(mayDefineCatalogProperties("CREATE ".repeat(200_000) + "x")).isFalse();
        assertThat(mayDefineCatalogProperties("CREATE --\n".repeat(200_000) + "CATALOG")).isTrue();
    }

    @Test
    void testCreateCatalog()
    {
        assertThat(redact("CREATE CATALOG IF NOT EXISTS c USING postgresql COMMENT 'kept' WITH (\"connection-url\" = 'jdbc:postgresql://h/db', \"connection-password\" = 'secret')"))
                .hasValue(
                        """
                        CREATE CATALOG IF NOT EXISTS c USING postgresql
                        COMMENT 'kept'
                        WITH (
                           "connection-url" = '***',
                           "connection-password" = '***'
                        )""");
        // A statement without properties is recorded exactly as written
        assertThat(redact("CREATE CATALOG canceled_catalog USING tpch")).hasValue("CREATE CATALOG canceled_catalog USING tpch");
        // Any expression is masked, not only literals
        assertThat(redact("CREATE CATALOG c USING x WITH (a = concat('se', 'cret'), b = ?)").orElseThrow())
                .doesNotContain("cret")
                .doesNotContain("?");
    }

    @Test
    void testWrappedCreateCatalog()
    {
        assertThat(redact("EXPLAIN CREATE CATALOG c USING x WITH (p = 'secret')").orElseThrow())
                .startsWith("EXPLAIN")
                .contains("p = '***'")
                .doesNotContain("secret");
        assertThat(redact("PREPARE s FROM CREATE CATALOG c USING x WITH (p = 'secret')").orElseThrow())
                .startsWith("PREPARE s FROM")
                .doesNotContain("secret");
        assertThat(redact("EXECUTE IMMEDIATE 'CREATE CATALOG c USING x WITH (p = ?)' USING 'secret'").orElseThrow())
                .startsWith("EXECUTE IMMEDIATE")
                .contains("''***''")
                .contains("USING '***'")
                .doesNotContain("secret");
        // An inner statement that cannot be parsed is replaced as a whole
        assertThat(redact("EXECUTE IMMEDIATE 'CREATE CATALOG c USING x WITH (p = ''secret'' ''broken'')'").orElseThrow())
                .contains(REDACTED_STATEMENT)
                .doesNotContain("secret");
    }

    @Test
    void testOtherStatementsAreNotRedacted()
    {
        assertThat(redact("SELECT 'CREATE CATALOG c USING x WITH (p = ''secret'')'")).isEmpty();
        assertThat(redact("EXECUTE IMMEDIATE 'SELECT 1'")).isEmpty();
        assertThat(redact("EXPLAIN SELECT 1")).isEmpty();
        assertThat(redact("DROP CATALOG c")).isEmpty();
    }

    @Test
    void testDefinesCatalogProperties()
    {
        assertThat(definesCatalogProperties("CREATE CATALOG c USING x", PARSER)).isTrue();
        assertThat(definesCatalogProperties("CREATE CATALOG c USING x WITH (p = 'a' 'b')", PARSER)).isTrue();
        assertThat(definesCatalogProperties("SELECT 'create catalog'", PARSER)).isFalse();
        assertThat(definesCatalogProperties("SELECT 1", PARSER)).isFalse();
    }

    private static Optional<String> redact(String sql)
    {
        return redactedSql(PARSER.createStatement(sql), PARSER);
    }
}
