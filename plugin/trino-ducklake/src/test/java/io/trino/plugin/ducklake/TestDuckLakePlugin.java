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

import com.google.common.collect.ImmutableMap;
import io.trino.node.InternalNode;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.NodeVersion.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakePlugin
{
    @TempDir
    private Path temporaryDirectory;

    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = getOnlyElement(new DuckLakePlugin().getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("ducklake.metadata.connection-url", "jdbc:postgresql://example.net:5432/lake")
                                .put("ducklake.data-path", "s3://bucket/prefix/")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testWorkerDoesNotRequireMetadataPasswordFile()
    {
        Path missingPasswordFile = temporaryDirectory.resolve("not-projected-on-worker");
        Connector connector = createConnector(false, ImmutableMap.of(
                "ducklake.metadata.connection-password-file", missingPasswordFile.toString()));
        try {
            assertThat(connector.getPageSourceProvider()).isNotNull();
            assertThat(connector.getPageSinkProvider()).isNotNull();
            assertThat(missingPasswordFile).doesNotExist();
        }
        finally {
            connector.shutdown();
        }
    }

    @Test
    void testCoordinatorRequiresMetadataPasswordFile()
    {
        Path missingPasswordFile = temporaryDirectory.resolve("not-projected-on-coordinator");
        assertThatThrownBy(() -> createConnector(true, ImmutableMap.of(
                "ducklake.metadata.connection-password-file", missingPasswordFile.toString())))
                .hasMessageContaining("Invalid configuration property ducklake.metadata.connection-password-file: file does not exist: " + missingPasswordFile);
    }

    @Test
    void testCoordinatorAcceptsMetadataPasswordFile()
            throws IOException
    {
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("password"), "secret\n");
        createConnector(true, ImmutableMap.of(
                "ducklake.metadata.connection-password-file", passwordFile.toString()))
                .shutdown();
    }

    @Test
    void testPasswordAndPasswordFileAreMutuallyExclusive()
            throws IOException
    {
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("password"), "secret\n");
        for (boolean coordinator : new boolean[] {true, false}) {
            assertThatThrownBy(() -> createConnector(coordinator, ImmutableMap.of(
                    "ducklake.metadata.connection-password", "secret",
                    "ducklake.metadata.connection-password-file", passwordFile.toString())))
                    .hasMessageContaining("ducklake.metadata.connection-password and ducklake.metadata.connection-password-file cannot both be set");
        }
    }

    private static Connector createConnector(boolean coordinator, Map<String, String> properties)
    {
        ConnectorFactory factory = getOnlyElement(new DuckLakePlugin().getConnectorFactories());
        return factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("ducklake.metadata.connection-url", "jdbc:postgresql://example.net:5432/lake")
                        .put("ducklake.data-path", "s3://bucket/prefix/")
                        .put("bootstrap.quiet", "true")
                        .putAll(properties)
                        .buildOrThrow(),
                new TestingConnectorContext(TestingNodeManager.builder()
                        .localNode(new InternalNode("test", URI.create("http://localhost:8080"), UNKNOWN, coordinator))
                        .build()));
    }
}
