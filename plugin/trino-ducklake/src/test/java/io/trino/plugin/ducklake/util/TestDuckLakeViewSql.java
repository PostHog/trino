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
package io.trino.plugin.ducklake.util;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.ducklake.util.DuckLakeViewSql.withCatalogPlaceholder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckLakeViewSql
{
    @Test
    void testCatalogQualifiedNameIsReplaced()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM lake.main.t", "lake"))
                .isEqualTo("SELECT a FROM {DUCKLAKE_CATALOG}.main.t");
    }

    @Test
    void testEveryOccurrenceIsReplaced()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM lake.main.t JOIN lake.main.u ON TRUE", "lake"))
                .isEqualTo("SELECT a FROM {DUCKLAKE_CATALOG}.main.t JOIN {DUCKLAKE_CATALOG}.main.u ON TRUE");
    }

    @Test
    void testNameOfAnotherCatalogIsLeftAlone()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM other.main.t", "lake"))
                .isEqualTo("SELECT a FROM other.main.t");
    }

    @Test
    void testUnqualifiedQueryIsUnchanged()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM t", "lake"))
                .isEqualTo("SELECT a FROM t");
        assertThat(withCatalogPlaceholder("SELECT a FROM main.t", "lake"))
                .isEqualTo("SELECT a FROM main.t");
    }

    @Test
    void testColumnQualifiedByTheCatalogIsReplaced()
    {
        // a four-part column reference names the catalog just as a table reference does
        assertThat(withCatalogPlaceholder("SELECT lake.main.t.a FROM lake.main.t", "lake"))
                .isEqualTo("SELECT {DUCKLAKE_CATALOG}.main.t.a FROM {DUCKLAKE_CATALOG}.main.t");
    }

    @Test
    void testAliasSharingTheCatalogNameIsLeftAlone()
    {
        // lake.a here is a column of the alias, not a catalog-qualified name, and rewriting it
        // would point the reference at a catalog
        assertThat(withCatalogPlaceholder("SELECT lake.a FROM main.t AS lake", "lake"))
                .isEqualTo("SELECT lake.a FROM main.t AS lake");
    }

    @Test
    void testStringLiteralNamingTheCatalogIsLeftAlone()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM lake.main.t WHERE p = 'lake.main.t'", "lake"))
                .isEqualTo("SELECT a FROM {DUCKLAKE_CATALOG}.main.t WHERE p = 'lake.main.t'");
    }

    @Test
    void testQuotedCatalogNameIsReplaced()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM \"my lake\".main.t", "my lake"))
                .isEqualTo("SELECT a FROM {DUCKLAKE_CATALOG}.main.t");
    }

    @Test
    void testQuotedIdentifierNamingSomethingElseIsLeftAlone()
    {
        assertThat(withCatalogPlaceholder("SELECT \"lake.main.t\" FROM lake.main.t", "lake"))
                .isEqualTo("SELECT \"lake.main.t\" FROM {DUCKLAKE_CATALOG}.main.t");
    }

    @Test
    void testUnquotedNameMatchesRegardlessOfCase()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM LAKE.main.t", "lake"))
                .isEqualTo("SELECT a FROM {DUCKLAKE_CATALOG}.main.t");
    }

    @Test
    void testNamePrefixedByTheCatalogIsLeftAlone()
    {
        assertThat(withCatalogPlaceholder("SELECT a FROM lakehouse.main.t", "lake"))
                .isEqualTo("SELECT a FROM lakehouse.main.t");
    }
}
