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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Verifies reads from older DuckLake catalogs that do not have the
 * {@code ducklake_inlined_data_tables} table.
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
            catalog.executeInDuckDb(
                    "CREATE TABLE legacy_table (id INTEGER, v VARCHAR)",
                    "INSERT INTO legacy_table VALUES (1, 'one'), (2, 'two')");
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }
        dropInlinedDataTablesRegistry(catalog);
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    private static void dropInlinedDataTablesRegistry(TestingDuckLakeCatalog catalog)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS public.ducklake_inlined_data_tables");
        }
    }

    @Test
    void testReadsWithoutInlinedDataTablesRegistry()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("legacy_table");
        assertQuery("SELECT id, v FROM legacy_table", "VALUES (1, 'one'), (2, 'two')");
        assertQuery("SELECT count(*) FROM legacy_table", "VALUES 2");
    }
}
