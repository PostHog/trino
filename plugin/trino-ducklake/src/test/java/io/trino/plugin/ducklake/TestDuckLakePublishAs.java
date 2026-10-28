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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_CONFLICT;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * {@code publish_as} gives a table the name of another one, replacing that one if it exists, in a
 * single snapshot. A pipeline builds a table under a hidden name and publishes it under the name
 * readers query, which never resolves to nothing in between.
 */
@Execution(SAME_THREAD)
final class TestDuckLakePublishAs
        extends AbstractTestQueryFramework
{
    private TestingDuckLakeCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalog = closeAfterClass(new TestingDuckLakeCatalog());
        catalog.executeInDuckDb("CREATE TABLE _bootstrap (x INTEGER)", "DROP TABLE _bootstrap");
        // unlike add_files, the procedure needs no catalog property: it only renames and drops
        return DuckLakeQueryRunner.builder(catalog).build();
    }

    @Test
    void testReplacesTheTargetInOneSnapshot()
            throws Exception
    {
        String target = "live_" + randomNameSuffix();
        String staging = "staging_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + target + " (id INTEGER, note VARCHAR)");
        assertUpdate("INSERT INTO " + target + " VALUES (1, 'old'), (2, 'old')", 2);
        // the table taking the name has columns of its own
        assertUpdate("CREATE TABLE " + staging + " (id BIGINT, amount DOUBLE)");
        assertUpdate("INSERT INTO " + staging + " VALUES (10, 1.5), (20, 2.5)", 2);
        try {
            long replacedTableId = tableRow(target).tableId();
            TableRow published = tableRow(staging);
            List<Path> replacedFiles = currentDataFiles(target);
            assertThat(replacedFiles).isNotEmpty();
            long before = latestSnapshot();

            assertUpdate(publishAs(staging, target), "VALUES ('replaced_tables', 1)");

            long snapshot = latestSnapshot();
            assertThat(snapshot).isEqualTo(before + 1);
            // the published table keeps its identity, location and rows under the new name
            assertThat(tableRow(target)).isEqualTo(published);
            assertThat(getQueryRunner().tableExists(getSession(), staging)).isFalse();
            assertQuery("SELECT * FROM " + target, "VALUES (10, 1.5), (20, 2.5)");
            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = '" + target + "'",
                    "VALUES ('id', 'bigint'), ('amount', 'double')");
            assertThat(catalog.rows("SELECT id::VARCHAR, amount::VARCHAR FROM %s ORDER BY id".formatted(target))).containsExactly("10", "1.5", "20", "2.5");

            // DuckDB records a DROP TABLE and an ALTER TABLE ... RENAME TO committed together the same way
            assertThat(catalog.scalar("SELECT changes::VARCHAR FROM ducklake_snapshots('lake') WHERE snapshot_id = " + snapshot))
                    .isEqualTo("{tables_created=[main.%s], tables_dropped=[%s]}".formatted(target, replacedTableId));

            // a reader of the previous snapshot still reads both tables as they were
            assertThat(catalog.rows("SELECT id::VARCHAR, note FROM %s AT (VERSION => %s) ORDER BY id".formatted(target, before))).containsExactly("1", "old", "2", "old");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM %s AT (VERSION => %s) ORDER BY id".formatted(staging, before))).containsExactly("10", "20");

            // the replaced table is retired as DROP TABLE retires it: its files end with the snapshot and stay in place
            assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s AND end_snapshot = %s".formatted(replacedTableId, snapshot)))
                    .isEqualTo(replacedFiles.size());
            assertThat(metastoreLong("SELECT count(*) FROM ducklake_data_file WHERE table_id = %s AND end_snapshot IS NULL".formatted(replacedTableId))).isZero();
            assertThat(replacedFiles).allMatch(Files::isRegularFile);

            // DuckDB keeps writing to the published table
            catalog.executeInDuckDb("INSERT INTO %s VALUES (30, 3.5)".formatted(target));
            assertQuery("SELECT * FROM " + target, "VALUES (10, 1.5), (20, 2.5), (30, 3.5)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + target);
            assertUpdate("DROP TABLE IF EXISTS " + staging);
        }
    }

    @Test
    void testReaderOfThePreviousSnapshotKeepsReadingTheReplacedTable()
    {
        String target = "live_" + randomNameSuffix();
        String staging = "staging_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + target + " (id INTEGER)");
        assertUpdate("INSERT INTO " + target + " VALUES 1, 2", 2);
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("INSERT INTO " + staging + " VALUES 3", 1);
        try {
            inTransaction(session -> {
                // the transaction reads the snapshot current when it first reads the catalog
                assertQuery(session, "SELECT * FROM " + target, "VALUES 1, 2");
                assertUpdate(publishAs(staging, target), "VALUES ('replaced_tables', 1)");
                assertQuery(session, "SELECT * FROM " + target, "VALUES 1, 2");
                assertQuery(session, "SELECT * FROM " + staging, "VALUES 3");
            });
            assertQuery("SELECT * FROM " + target, "VALUES 3");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + target);
            assertUpdate("DROP TABLE IF EXISTS " + staging);
        }
    }

    @Test
    void testPublishesUnderAnUnusedNameInTheSchemaOfTheTable()
            throws Exception
    {
        String schema = "imports_" + randomNameSuffix();
        String target = "events_" + randomNameSuffix();
        String staging = "staging_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schema);
        assertUpdate("CREATE TABLE %s.%s (id INTEGER)".formatted(schema, staging));
        assertUpdate("INSERT INTO %s.%s VALUES 7".formatted(schema, staging), 1);
        try {
            TableRow published = tableRow(staging);
            long before = latestSnapshot();

            assertUpdate(publishAs(schema + "." + staging, target), "VALUES ('replaced_tables', 0)");

            long snapshot = latestSnapshot();
            assertThat(snapshot).isEqualTo(before + 1);
            assertThat(tableRow(target)).isEqualTo(published);
            assertQuery("SELECT * FROM %s.%s".formatted(schema, target), "VALUES 7");
            // the name is resolved in the schema of the published table, not in the schema of the session
            assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'".formatted(schema), "VALUES '%s'".formatted(target));
            assertThat(getQueryRunner().tableExists(getSession(), target)).isFalse();
            assertThat(catalog.rows("SELECT id::VARCHAR FROM %s.%s".formatted(schema, target))).containsExactly("7");
            assertThat(catalog.scalar("SELECT changes::VARCHAR FROM ducklake_snapshots('lake') WHERE snapshot_id = " + snapshot))
                    .isEqualTo("{tables_created=[%s.%s]}".formatted(schema, target));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS %s.%s".formatted(schema, target));
            assertUpdate("DROP TABLE IF EXISTS %s.%s".formatted(schema, staging));
            assertUpdate("DROP SCHEMA " + schema);
        }
    }

    @Test
    void testRejectsTargetsItCannotPublishAs()
            throws Exception
    {
        String staging = "staging_" + randomNameSuffix();
        String view = "view_" + randomNameSuffix();
        String duckDbView = "duckdb_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("INSERT INTO " + staging + " VALUES 1", 1);
        assertUpdate("CREATE VIEW " + view + " SECURITY INVOKER AS SELECT 2 AS id");
        // a view in DuckDB's dialect, which Trino does not read, still holds the name
        catalog.executeInDuckDb("CREATE VIEW %s AS SELECT 3 AS id".formatted(duckDbView));
        try {
            long before = latestSnapshot();
            assertQueryFails("ALTER TABLE " + staging + " EXECUTE publish_as()", "target must be specified");
            assertQueryFails(publishAs(staging, ""), "target must be a table name, but is empty");
            assertQueryFails(publishAs(staging, "  "), "target must be a table name, but is empty");
            assertQueryFails(publishAs(staging, "main.other"), "target must be an unqualified table name, which is published in the schema of the table: main.other");
            assertQueryFails(publishAs(staging, "ducklake.main.other"), "target must be an unqualified table name, which is published in the schema of the table: ducklake.main.other");
            assertQueryFails(publishAs(staging, staging), "target must name another table than the one published: " + staging);
            assertQueryFails(publishAs(staging, staging.toUpperCase(ENGLISH)), "target must name another table than the one published: " + staging);
            assertQueryFails(publishAs(staging, view), "target is a view, which publish_as cannot replace: main." + view);
            assertQueryFails(publishAs(staging, duckDbView), "target is a view, which publish_as cannot replace: main." + duckDbView);

            assertThat(latestSnapshot()).isEqualTo(before);
            assertQuery("SELECT * FROM " + staging, "VALUES 1");
            assertQuery("SELECT * FROM " + view, "VALUES 2");
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + duckDbView)).containsExactly("3");
        }
        finally {
            catalog.executeInDuckDb("DROP VIEW " + duckDbView);
            assertUpdate("DROP VIEW " + view);
            assertUpdate("DROP TABLE " + staging);
        }
    }

    @Test
    void testRequiresRenamingTheTableAndDroppingTheTarget()
    {
        String staging = "staging_" + randomNameSuffix();
        String target = "live_" + randomNameSuffix();
        String unused = "unused_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("CREATE TABLE " + target + " (id INTEGER)");
        try {
            String user = getSession().getUser();
            assertAccessDenied(
                    publishAs(staging, target),
                    "Cannot rename table from ducklake.main.%s to ducklake.main.%s".formatted(staging, target),
                    privilege(user, staging, RENAME_TABLE));
            assertAccessDenied(
                    publishAs(staging, target),
                    "Cannot drop table ducklake.main." + target,
                    privilege(user, target, DROP_TABLE));
            assertThat(getQueryRunner().tableExists(getSession(), staging)).isTrue();
            assertThat(getQueryRunner().tableExists(getSession(), target)).isTrue();

            // a name no table holds needs no permission to drop one
            assertAccessAllowed(publishAs(staging, unused), privilege(user, unused, DROP_TABLE));
            assertThat(getQueryRunner().tableExists(getSession(), unused)).isTrue();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + unused);
            assertUpdate("DROP TABLE IF EXISTS " + staging);
            assertUpdate("DROP TABLE " + target);
        }
    }

    @Test
    void testChecksExactlyTheRenameAndTheDrop()
    {
        String staging = "staging_" + randomNameSuffix();
        String target = "live_" + randomNameSuffix();
        String unused = "unused_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("CREATE TABLE " + target + " (id INTEGER)");
        try {
            RecordingAccessControl accessControl = new RecordingAccessControl();
            planPublishAs(newMetadata(), accessControl, staging, target);
            assertThat(accessControl.checks()).containsExactly(
                    "rename main.%s to main.%s".formatted(staging, target),
                    "drop main." + target);

            accessControl = new RecordingAccessControl();
            planPublishAs(newMetadata(), accessControl, staging, unused);
            assertThat(accessControl.checks()).containsExactly("rename main.%s to main.%s".formatted(staging, unused));
        }
        finally {
            assertUpdate("DROP TABLE " + staging);
            assertUpdate("DROP TABLE " + target);
        }
    }

    @Test
    void testFailsWhenTheTableIsReplacedOrDroppedConcurrently()
            throws Exception
    {
        String staging = "staging_" + randomNameSuffix();
        String target = "live_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("INSERT INTO " + staging + " VALUES 1", 1);
        try {
            DuckLakeMetadata metadata = newMetadata();
            ConnectorTableExecuteHandle publish = planPublishAs(metadata, staging, target);
            catalog.executeInDuckDb(
                    "DROP TABLE " + staging,
                    "CREATE TABLE %s (id INTEGER)".formatted(staging),
                    "INSERT INTO %s VALUES (2)".formatted(staging));
            long snapshot = latestSnapshot();
            assertFailure(() -> metadata.executeTableExecute(SESSION, publish), DUCKLAKE_COMMIT_CONFLICT, "Table main.%s was replaced while publishing it".formatted(staging));
            assertThat(latestSnapshot()).isEqualTo(snapshot);
            assertQuery("SELECT * FROM " + staging, "VALUES 2");
            assertThat(getQueryRunner().tableExists(getSession(), target)).isFalse();

            DuckLakeMetadata dropped = newMetadata();
            ConnectorTableExecuteHandle publishDropped = planPublishAs(dropped, staging, target);
            catalog.executeInDuckDb("DROP TABLE " + staging);
            snapshot = latestSnapshot();
            assertFailure(() -> dropped.executeTableExecute(SESSION, publishDropped), TABLE_NOT_FOUND, "Table main.%s was dropped or renamed while publishing it".formatted(staging));
            assertThat(latestSnapshot()).isEqualTo(snapshot);
            assertThat(getQueryRunner().tableExists(getSession(), target)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + staging);
            assertUpdate("DROP TABLE IF EXISTS " + target);
        }
    }

    @Test
    void testFailsWhenTheTargetChangesConcurrently()
            throws Exception
    {
        String staging = "staging_" + randomNameSuffix();
        String other = "other_" + randomNameSuffix();
        String target = "live_" + randomNameSuffix();
        String viewTarget = "view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("INSERT INTO " + staging + " VALUES 1", 1);
        assertUpdate("CREATE TABLE " + other + " (id INTEGER)");
        assertUpdate("INSERT INTO " + other + " VALUES 2", 1);
        try {
            // a table took the unused name after the statement was planned, so nothing checked
            // that the caller may drop it, nor did the caller expect to replace anything
            DuckLakeMetadata metadata = newMetadata();
            ConnectorTableExecuteHandle publish = planPublishAs(metadata, staging, target);
            catalog.executeInDuckDb("CREATE TABLE %s (id INTEGER)".formatted(target), "INSERT INTO %s VALUES (3)".formatted(target));
            assertFailure(() -> metadata.executeTableExecute(SESSION, publish), DUCKLAKE_COMMIT_CONFLICT, "Table main.%s was created, dropped or replaced while publishing as it".formatted(target));
            assertQuery("SELECT * FROM " + target, "VALUES 3");
            assertQuery("SELECT * FROM " + staging, "VALUES 1");

            // so did a view
            DuckLakeMetadata viewMetadata = newMetadata();
            ConnectorTableExecuteHandle publishOverView = planPublishAs(viewMetadata, staging, viewTarget);
            catalog.executeInDuckDb("CREATE VIEW %s AS SELECT 4 AS id".formatted(viewTarget));
            assertFailure(() -> viewMetadata.executeTableExecute(SESSION, publishOverView), DUCKLAKE_COMMIT_CONFLICT, "View main.%s was created while publishing as it".formatted(viewTarget));
            assertThat(catalog.rows("SELECT id::VARCHAR FROM " + viewTarget)).containsExactly("4");

            // two publications racing for one name: the second finds another table holding it
            DuckLakeMetadata first = newMetadata();
            ConnectorTableExecuteHandle publishStaging = planPublishAs(first, staging, target);
            DuckLakeMetadata second = newMetadata();
            ConnectorTableExecuteHandle publishOther = planPublishAs(second, other, target);
            assertThat(first.executeTableExecute(SESSION, publishStaging)).isEqualTo(ImmutableMap.of("replaced_tables", 1L));
            assertFailure(() -> second.executeTableExecute(SESSION, publishOther), DUCKLAKE_COMMIT_CONFLICT, "Table main.%s was created, dropped or replaced while publishing as it".formatted(target));
            assertQuery("SELECT * FROM " + target, "VALUES 1");
            assertQuery("SELECT * FROM " + other, "VALUES 2");

            // the table to replace is gone
            DuckLakeMetadata third = newMetadata();
            ConnectorTableExecuteHandle publishOtherAgain = planPublishAs(third, other, target);
            catalog.executeInDuckDb("DROP TABLE " + target);
            assertFailure(() -> third.executeTableExecute(SESSION, publishOtherAgain), DUCKLAKE_COMMIT_CONFLICT, "Table main.%s was created, dropped or replaced while publishing as it".formatted(target));
            assertQuery("SELECT * FROM " + other, "VALUES 2");
            assertThat(getQueryRunner().tableExists(getSession(), target)).isFalse();
        }
        finally {
            catalog.executeInDuckDb("DROP VIEW IF EXISTS " + viewTarget);
            assertUpdate("DROP TABLE IF EXISTS " + staging);
            assertUpdate("DROP TABLE IF EXISTS " + other);
            assertUpdate("DROP TABLE IF EXISTS " + target);
        }
    }

    @Test
    void testLandsOnConcurrentChangesItDoesNotDependOn()
            throws Exception
    {
        String staging = "staging_" + randomNameSuffix();
        String target = "live_" + randomNameSuffix();
        String unrelated = "unrelated_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + staging + " (id INTEGER)");
        assertUpdate("INSERT INTO " + staging + " VALUES 1", 1);
        assertUpdate("CREATE TABLE " + target + " (id INTEGER)");
        assertUpdate("INSERT INTO " + target + " VALUES 10", 1);
        assertUpdate("CREATE TABLE " + unrelated + " (id INTEGER)");
        try {
            DuckLakeMetadata metadata = newMetadata();
            ConnectorTableExecuteHandle publish = planPublishAs(metadata, staging, target);
            long planned = latestSnapshot();
            catalog.executeInDuckDb(
                    "INSERT INTO %s VALUES (2)".formatted(staging),
                    "INSERT INTO %s VALUES (3)".formatted(unrelated),
                    "INSERT INTO %s VALUES (11)".formatted(target));

            assertThat(metadata.executeTableExecute(SESSION, publish)).isEqualTo(ImmutableMap.of("replaced_tables", 1L));

            assertThat(latestSnapshot()).isEqualTo(planned + 4);
            // rows added to the published table meanwhile are published with it
            assertQuery("SELECT * FROM " + target, "VALUES 1, 2");
            assertQuery("SELECT * FROM " + unrelated, "VALUES 3");
            // and, as with DROP TABLE, rows added to the replaced table meanwhile go with it
            assertThat(catalog.rows("SELECT id::VARCHAR FROM %s AT (VERSION => %s) ORDER BY id".formatted(target, planned + 3))).containsExactly("10", "11");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + staging);
            assertUpdate("DROP TABLE IF EXISTS " + target);
            assertUpdate("DROP TABLE " + unrelated);
        }
    }

    private static String publishAs(String table, String target)
    {
        return "ALTER TABLE %s EXECUTE publish_as(target => '%s')".formatted(table, target.replace("'", "''"));
    }

    private DuckLakeMetadata newMetadata()
    {
        DuckLakeConnector connector = (DuckLakeConnector) getQueryRunner().getCoordinator().getConnector("ducklake");
        return connector.getInjector().getInstance(DuckLakeMetadataFactory.class).create(SESSION.getIdentity());
    }

    private static ConnectorTableExecuteHandle planPublishAs(DuckLakeMetadata metadata, String table, String target)
    {
        return planPublishAs(metadata, new AllowAllAccessControl(), table, target);
    }

    /**
     * Plans the statement the way the engine does, which pins the metadata to the snapshot current
     * now. Running it later commits against whatever other writers committed in between.
     */
    private static ConnectorTableExecuteHandle planPublishAs(DuckLakeMetadata metadata, ConnectorAccessControl accessControl, String table, String target)
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("main", table), Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();
        return metadata.getTableHandleForExecute(SESSION, accessControl, tableHandle, "PUBLISH_AS", ImmutableMap.of("target", target), NO_RETRIES)
                .orElseThrow();
    }

    private static void assertFailure(ThrowingCallable statement, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(statement)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()))
                .hasMessage(message);
    }

    private long latestSnapshot()
            throws SQLException
    {
        return metastoreLong("SELECT max(snapshot_id) FROM ducklake_snapshot");
    }

    private TableRow tableRow(String tableName)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT table_id, table_uuid::varchar, path FROM ducklake_table WHERE table_name = '%s' AND end_snapshot IS NULL".formatted(tableName))) {
            assertThat(resultSet.next()).isTrue();
            TableRow row = new TableRow(resultSet.getLong("table_id"), resultSet.getString("table_uuid"), resultSet.getString("path"));
            assertThat(resultSet.next()).isFalse();
            return row;
        }
    }

    /**
     * The data files of a table of the {@code main} schema, as they lie in the data directory.
     */
    private List<Path> currentDataFiles(String tableName)
            throws SQLException
    {
        @Language("SQL") String sql =
                """
                SELECT s.path || t.path || f.path
                FROM ducklake_data_file f
                JOIN ducklake_table t ON t.table_id = f.table_id AND t.end_snapshot IS NULL
                JOIN ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL
                WHERE t.table_name = '%s' AND f.end_snapshot IS NULL AND f.path_is_relative AND t.path_is_relative AND s.path_is_relative
                """.formatted(tableName);
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ImmutableList.Builder<Path> files = ImmutableList.builder();
            while (resultSet.next()) {
                files.add(catalog.dataPath().resolve(resultSet.getString(1)));
            }
            return files.build();
        }
    }

    private long metastoreLong(@Language("SQL") String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(catalog.jdbcUrl(), TestingDuckLakeCatalog.USER, TestingDuckLakeCatalog.PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next()).isTrue();
            long value = resultSet.getLong(1);
            assertThat(resultSet.next()).isFalse();
            return value;
        }
    }

    private record TableRow(long tableId, String tableUuid, String path) {}

    /**
     * Records the checks of the statements that write to tables, allowing all of them.
     */
    private static final class RecordingAccessControl
            extends AllowAllAccessControl
    {
        private final List<String> checks = new ArrayList<>();

        public List<String> checks()
        {
            return ImmutableList.copyOf(checks);
        }

        @Override
        public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
        {
            checks.add("create " + tableName);
        }

        @Override
        public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
        {
            checks.add("drop " + tableName);
        }

        @Override
        public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
        {
            checks.add("rename %s to %s".formatted(tableName, newTableName));
        }

        @Override
        public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName, Optional<String> branch)
        {
            checks.add("insert " + tableName);
        }

        @Override
        public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName, Optional<String> branch)
        {
            checks.add("delete " + tableName);
        }

        @Override
        public void checkCanTruncateTable(ConnectorSecurityContext context, SchemaTableName tableName)
        {
            checks.add("truncate " + tableName);
        }
    }
}
