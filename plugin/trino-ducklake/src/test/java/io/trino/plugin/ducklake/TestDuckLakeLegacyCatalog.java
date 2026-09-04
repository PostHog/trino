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
package io.trino.plugin.ducklake;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Verifies reads from older DuckLake catalogs that do not have the
 * {@code ducklake_inlined_data_tables} table or the {@code ducklake_name_mapping.is_partition}
 * column.
 */
final class TestDuckLakeLegacyCatalog
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDuckLakeCatalog catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            String externalFile = catalog.externalParquetFile("legacy_mapped");
            catalog.executeInDuckDb(
                    "CREATE TABLE legacy_table (id INTEGER, v VARCHAR)",
                    "INSERT INTO legacy_table VALUES (1, 'one'), (2, 'two')",
                    "CREATE TABLE legacy_mapped (id INTEGER, v VARCHAR)",
                    "COPY (SELECT * FROM (VALUES ('three', 3), ('four', 4)) t(v, id)) TO '%s' (FORMAT parquet)".formatted(externalFile),
                    "SELECT * FROM ducklake_add_data_files('lake', 'legacy_mapped', '%s', schema => 'main')".formatted(externalFile));
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }
        makeCatalogLegacy(catalog);
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    private static void makeCatalogLegacy(TestingDuckLakeCatalog catalog)
            throws SQLException
    {
        catalog.executeInMetastore(
                "DROP TABLE IF EXISTS public.ducklake_inlined_data_tables",
                "ALTER TABLE public.ducklake_name_mapping DROP COLUMN IF EXISTS is_partition");
    }

    @Test
    void testReadsWithoutInlinedDataTablesRegistry()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("legacy_table");
        assertQuery("SELECT id, v FROM legacy_table", "VALUES (1, 'one'), (2, 'two')");
        assertQuery("SELECT count(*) FROM legacy_table", "VALUES 2");
    }

    @Test
    void testNameMappingWithoutIsPartitionColumn()
    {
        assertQuery("SELECT id, v FROM legacy_mapped", "VALUES (3, 'three'), (4, 'four')");
        assertQuery("SELECT count(*) FROM legacy_mapped WHERE id > 3", "VALUES 1");
    }
}
