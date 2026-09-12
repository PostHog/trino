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
import io.trino.plugin.base.util.Closables;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckLakeWorkerPasswordFile
        extends AbstractTestQueryFramework
{
    private Path workerPasswordFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDuckLakeCatalog catalog = closeAfterClass(new TestingDuckLakeCatalog());
        catalog.executeInDuckDb("CREATE TABLE worker_password_file (value BIGINT)");

        Path directory = Files.createTempDirectory("ducklake-worker-password");
        Path coordinatorPasswordFile = directory.resolve("coordinator-password");
        workerPasswordFile = directory.resolve("worker-password");
        closeAfterClass(() -> {
            Files.deleteIfExists(coordinatorPasswordFile);
            Files.deleteIfExists(directory);
        });
        Files.writeString(coordinatorPasswordFile, TestingDuckLakeCatalog.PASSWORD + "\n", UTF_8);

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("ducklake")
                        .setSchema("main")
                        .build())
                .setWorkerCount(1)
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .build();
        try {
            queryRunner.installPlugin(new WorkerPasswordFilePlugin(workerPasswordFile));
            queryRunner.createCatalog("ducklake", "ducklake", ImmutableMap.<String, String>builder()
                    .put("ducklake.metadata.connection-url", catalog.jdbcUrl())
                    .put("ducklake.metadata.connection-user", TestingDuckLakeCatalog.USER)
                    .put("ducklake.metadata.connection-password-file", coordinatorPasswordFile.toString())
                    .put("ducklake.data-path", "local:///")
                    .put("fs.local.enabled", "true")
                    .put("local.location", catalog.dataPath().toString())
                    .buildOrThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            Closables.closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testWorkersReadAndWriteWithoutMetadataPasswordFile()
            throws Exception
    {
        assertThat(workerPasswordFile).doesNotExist();
        assertUpdate("INSERT INTO worker_password_file VALUES (7), (11)", 2);
        assertQuery("SELECT value FROM worker_password_file", "VALUES (7), (11)");

        // A replacement worker first loads the existing catalog after the tenant is already usable.
        // The coordinator cannot execute tasks, and removing the original worker ensures that the
        // following writes and reads execute on the replacement with its password file still absent.
        getDistributedQueryRunner().addServers(1);
        getDistributedQueryRunner().removeWorker();
        assertUpdate("INSERT INTO worker_password_file VALUES (13)", 1);
        assertQuery("SELECT value FROM worker_password_file", "VALUES (7), (11), (13)");
        assertThat(workerPasswordFile).doesNotExist();
    }

    private static final class WorkerPasswordFilePlugin
            extends DuckLakePlugin
    {
        private final Path workerPasswordFile;

        public WorkerPasswordFilePlugin(Path workerPasswordFile)
        {
            this.workerPasswordFile = workerPasswordFile;
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new DuckLakeConnectorFactory()
            {
                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    if (context.getCurrentNode().isCoordinator()) {
                        return super.create(catalogName, config, context);
                    }

                    // All in-process test nodes share a filesystem. Substituting an absent path
                    // models a worker's independent Secret mount while the coordinator's file exists.
                    Map<String, String> workerConfig = new HashMap<>(config);
                    workerConfig.put("ducklake.metadata.connection-password-file", workerPasswordFile.toString());
                    return super.create(catalogName, workerConfig, context);
                }
            });
        }
    }
}
