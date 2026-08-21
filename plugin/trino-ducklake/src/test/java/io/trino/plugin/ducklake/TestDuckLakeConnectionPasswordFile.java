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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * The catalog database password is taken from a file instead of a catalog property, so that a
 * catalog created with {@code CREATE CATALOG} does not carry the credential in the statement
 * that Trino logs and shows in the Web UI.
 */
final class TestDuckLakeConnectionPasswordFile
        extends AbstractTestQueryFramework
{
    private Path passwordFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDuckLakeCatalog catalog = closeAfterClass(new TestingDuckLakeCatalog());
        try {
            catalog.executeInDuckDb("CREATE TABLE region AS SELECT * FROM (VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA')) t(regionkey, name)");
        }
        catch (SQLException e) {
            abort("Failed to create DuckLake fixtures with DuckDB (extension download requires network access): " + e);
        }

        passwordFile = Files.createTempFile("ducklake-connection-password", "");
        passwordFile.toFile().deleteOnExit();
        writePassword(TestingDuckLakeCatalog.PASSWORD);

        return DuckLakeQueryRunner.builder(catalog)
                .removeConnectorProperty("ducklake.metadata.connection-password")
                .addConnectorProperty("ducklake.metadata.connection-password-file", passwordFile.toString())
                // never hand out a pooled connection to a later statement, so that every statement
                // authenticates with the password the file holds at that moment
                .addConnectorProperty("ducklake.metadata.connection-pool.idle-timeout", "1ms")
                .build();
    }

    @Test
    void testPasswordFileIsReadOnEveryConnection()
            throws IOException
    {
        assertQuery("SELECT name FROM region WHERE regionkey = 1", "VALUES 'AMERICA'");

        // rotating the credential takes effect without restarting Trino or recreating the catalog
        writePassword("rotated-to-a-wrong-password");
        assertQueryFails("SELECT name FROM region WHERE regionkey = 1", ".*password authentication failed.*");

        writePassword(TestingDuckLakeCatalog.PASSWORD);
        assertQuery("SELECT name FROM region WHERE regionkey = 1", "VALUES 'AMERICA'");
    }

    /**
     * Writes the password the way a Kubernetes secret or {@code echo} would, with a trailing
     * newline that is not part of the password.
     */
    private void writePassword(String password)
            throws IOException
    {
        Files.writeString(passwordFile, password + "\n", UTF_8);
    }
}
