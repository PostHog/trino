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

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Starts a Trino server serving a DuckLake catalog, for driving the connector by hand against a
 * catalog database and data files that already exist.
 * <p>
 * Point it at a running catalog database with {@code -Dducklake.connection-url}, the base location
 * of the data files with {@code -Dducklake.data-location}, and connect a client to the URL it
 * prints. Unlike the tests, it keeps running until interrupted and does not create the catalog.
 */
public final class DuckLakeDevServer
{
    private static final Logger log = Logger.get(DuckLakeDevServer.class);

    private DuckLakeDevServer() {}

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        String connectionUrl = System.getProperty("ducklake.connection-url", "jdbc:postgresql://127.0.0.1:15440/lake");
        String connectionUser = System.getProperty("ducklake.connection-user", "lake");
        String connectionPassword = System.getProperty("ducklake.connection-password", "lake");
        String dataLocation = System.getProperty("ducklake.data-location");
        if (dataLocation == null) {
            throw new IllegalArgumentException("Set -Dducklake.data-location to the directory holding the DuckLake data files");
        }
        String port = System.getProperty("ducklake.http-port", "8080");

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("ducklake")
                                .setSchema("main")
                                .build())
                .addCoordinatorProperty("http-server.http.port", port)
                .build();
        queryRunner.installPlugin(new DuckLakePlugin());
        queryRunner.createCatalog("ducklake", "ducklake", Map.of(
                "ducklake.metadata.connection-url", connectionUrl,
                "ducklake.metadata.connection-user", connectionUser,
                "ducklake.metadata.connection-password", connectionPassword,
                "ducklake.data-path", "local:///",
                "fs.local.enabled", "true",
                "local.location", dataLocation));

        log.info("======== SERVER STARTED: %s ========", queryRunner.getCoordinator().getBaseUrl());
    }
}
