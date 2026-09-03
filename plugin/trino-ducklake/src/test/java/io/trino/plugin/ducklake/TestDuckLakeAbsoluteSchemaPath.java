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

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Verifies that a schema whose catalog row holds an absolute path keeps its tables there.
 * <p>
 * DuckLake stores each path either relative to the one above it or as an absolute path of its own.
 * This connector only ever writes relative ones, so the fixture writes an absolute one by hand.
 * That needs a catalog of its own, because the absolute path names a Trino file system that DuckDB
 * cannot resolve, and DuckDB checks what it reads when it attaches.
 */
final class TestDuckLakeAbsoluteSchemaPath
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            // the catalog has to exist before Trino can attach to it, and only DuckDB creates one
            catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
        }
        catch (SQLException e) {
            abort("Failed to create a DuckLake catalog with DuckDB (extension download requires network access): " + e);
        }
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    @Test
    void testTablesOfSuchASchemaAreWrittenAndReadThere()
            throws SQLException
    {
        String schema = "abs_schema_" + randomNameSuffix();
        String directory = "elsewhere_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schema);
        catalog.executeInMetastore("UPDATE public.ducklake_schema SET path = 'local:///%s/', path_is_relative = false WHERE schema_name = '%s'"
                .formatted(directory, schema));

        assertUpdate("CREATE TABLE %s.t AS SELECT 1 AS a".formatted(schema), 1);
        assertThat(catalog.dataPath().resolve(directory).resolve("t")).isDirectory();
        assertQuery("SELECT a FROM %s.t".formatted(schema), "VALUES 1");
        assertUpdate("INSERT INTO %s.t VALUES 2".formatted(schema), 1);
        assertQuery("SELECT a FROM %s.t ORDER BY a".formatted(schema), "VALUES 1, 2");
    }
}
