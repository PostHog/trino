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
package io.trino.plugin.posthogexternal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.SchemaAlreadyExistsException;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_EXTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPostHogMetastoreClient
{
    private static final Optional<String> USER = Optional.of("alice");
    private static final ExternalTable INVOICES = new ExternalTable("invoices", "delta", "s3://bucket/delta/invoices", ImmutableList.of(), ImmutableMap.of());

    private Path tokenFile;
    private FakePostHogMetastore server;
    private PostHogMetastoreClient client;

    @BeforeEach
    void setUp()
            throws IOException
    {
        tokenFile = Files.createTempFile("posthog-metastore-token", "");
        Files.writeString(tokenFile, "token-one\n");
        server = new FakePostHogMetastore("cat_1", "token-one");
        client = new PostHogMetastoreClient(server.uri(), tokenFile, "cat_1");
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        server.close();
        Files.deleteIfExists(tokenFile);
    }

    @Test
    void testSchemaAndTableLifecycle()
    {
        assertThat(client.listSchemas(USER)).isEmpty();
        assertThat(client.schemaExists(USER, "billing")).isFalse();
        client.createSchema(USER, "billing");
        assertThat(client.schemaExists(USER, "billing")).isTrue();
        assertThat(client.listSchemas(USER)).containsExactly("billing");

        client.createTable(USER, "billing", INVOICES);
        assertThat(client.listTables(USER, "billing")).containsExactly("invoices");
        assertThat(client.getTable(USER, "billing", "invoices")).contains(INVOICES);
        assertThat(client.getTable(USER, "billing", "missing")).isEmpty();

        client.dropTable(USER, "billing", "invoices");
        assertThat(client.listTables(USER, "billing")).isEmpty();
        client.dropSchema(USER, "billing");
        assertThat(client.listSchemas(USER)).isEmpty();

        assertThat(server.requests()).allSatisfy(request -> assertThat(request.user()).isEqualTo(USER));
    }

    @Test
    void testErrorsMapToTrinoExceptions()
    {
        assertThatThrownBy(() -> client.dropSchema(USER, "billing")).isInstanceOf(SchemaNotFoundException.class);
        assertThatThrownBy(() -> client.createTable(USER, "billing", INVOICES)).isInstanceOf(SchemaNotFoundException.class);
        client.createSchema(USER, "billing");
        assertThatThrownBy(() -> client.createSchema(USER, "billing")).isInstanceOf(SchemaAlreadyExistsException.class);
        assertThatThrownBy(() -> client.dropTable(USER, "billing", "invoices")).isInstanceOf(TableNotFoundException.class);
        client.createTable(USER, "billing", INVOICES);
        assertThatThrownBy(() -> client.createTable(USER, "billing", INVOICES)).isInstanceOf(TableAlreadyExistsException.class);
        assertThatThrownBy(() -> client.dropSchema(USER, "billing"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(SCHEMA_NOT_EMPTY.toErrorCode()));
        assertThatThrownBy(() -> client.createTable(USER, "billing", new ExternalTable("bad", "delta", "s3://bucket/x/", ImmutableList.of(), ImmutableMap.of())))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode());
                    assertThat(e).hasMessageContaining("(invalid)");
                });
    }

    @Test
    void testNamesOutsideTheContractNeverReachTheServer()
    {
        assertThatThrownBy(() -> client.createSchema(USER, "../other"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode()));
        assertThat(client.schemaExists(USER, "Billing")).isFalse();
        assertThat(client.getTable(USER, "billing", "a/b")).isEmpty();
        assertThat(server.requests()).isEmpty();
    }

    @Test
    void testServerFailureNeverLeaksTheToken()
    {
        server.failNextRequests(500, "boom with token-one inside");
        assertThatThrownBy(() -> client.listSchemas(USER))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(GENERIC_EXTERNAL_ERROR.toErrorCode());
                    assertThat(e).hasMessageContaining("HTTP 500").hasMessageNotContaining("token-one");
                });
    }

    @Test
    void testTokenFileRotationIsPickedUpWithoutRestart()
            throws IOException
    {
        client.createSchema(USER, "billing");

        server.setToken("token-two");
        assertThatThrownBy(() -> client.listSchemas(USER))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
                    assertThat(e).hasMessageContaining("HTTP 401").hasMessageNotContaining("token-one");
                });

        Files.writeString(tokenFile, "token-two");
        assertThat(client.listSchemas(USER)).containsExactly("billing");
    }

    @Test
    void testMissingTokenFileFailsClearly()
            throws IOException
    {
        Files.delete(tokenFile);
        assertThatThrownBy(() -> client.listSchemas(USER))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e).hasMessageContaining("token file"));
        assertThat(server.requests()).isEmpty();
    }
}
