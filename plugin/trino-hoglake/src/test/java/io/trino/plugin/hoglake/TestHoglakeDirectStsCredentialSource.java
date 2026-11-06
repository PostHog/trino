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
package io.trino.plugin.hoglake;

import com.sun.net.httpserver.HttpServer;
import io.trino.plugin.hoglake.HoglakeTeamCredentialSource.TeamCredentials;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE;
import static io.trino.plugin.hoglake.TestHoglakeTeamCredentials.assertErrorCode;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The smoke's {@code direct-sts} source against an in-process STS that
 * answers AssumeRole in the AWS query protocol.
 */
final class TestHoglakeDirectStsCredentialSource
{
    private static final ConnectorSession SESSION = ConnectorTestFixtures.session();
    private static final SchemaTableName EVENTS = new SchemaTableName("ingest", "events");
    // Placeholders: the source takes the account from GetCallerIdentity and the bucket from the catalog's data_path.
    private static final String DATA_PATH = "s3://example-bucket/millrace/millrace-dev/";

    // AssumeRole requests only; GetCallerIdentity calls are counted apart.
    private final List<Map<String, String>> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger callerIdentityRequests = new AtomicInteger();
    private final AtomicInteger dataPathReads = new AtomicInteger();
    private volatile Duration lifetime = Duration.ofHours(1);
    private volatile String deniedTeam = "";
    private HttpServer server;
    private StsClient sts;

    @BeforeEach
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                Map<String, String> form = new HashMap<>();
                for (String pair : new String(exchange.getRequestBody().readAllBytes(), UTF_8).split("&", -1)) {
                    String[] parts = pair.split("=", 2);
                    form.put(URLDecoder.decode(parts[0], UTF_8), URLDecoder.decode(parts[1], UTF_8));
                }
                exchange.getResponseHeaders().set("Content-Type", "text/xml");
                byte[] body;
                if (form.get("Action").equals("GetCallerIdentity")) {
                    callerIdentityRequests.incrementAndGet();
                    body = ("<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><GetCallerIdentityResult>" +
                            "<Arn>arn:aws:sts::111122223333:assumed-role/AWSReservedSSO_developers_0/someone</Arn><UserId>AROA:someone</UserId>" +
                            "<Account>111122223333</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>")
                            .getBytes(UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                    return;
                }
                requests.add(form);
                String team = form.get("Tags.member.1.Value");
                if (team.equals(deniedTeam)) {
                    body = "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>denied %s</Message></Error><RequestId>r</RequestId></ErrorResponse>"
                            .formatted(form.get("Policy")).getBytes(UTF_8);
                    exchange.sendResponseHeaders(403, body.length);
                }
                else {
                    body =
                            """
                            <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials>
                            <AccessKeyId>ASIA%1$s-%2$d</AccessKeyId><SecretAccessKey>secret-%1$s</SecretAccessKey><SessionToken>token-%1$s</SessionToken>
                            <Expiration>%3$s</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></AssumeRoleResponse>
                            """.formatted(team, requests.size(), Instant.now().plus(lifetime)).getBytes(UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                }
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        sts = StsClient.builder()
                .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("coordinator-key", "coordinator-secret")))
                .build();
    }

    @AfterEach
    void tearDown()
    {
        sts.close();
        server.stop(0);
    }

    @Test
    void testAssumeRoleRequest()
    {
        HoglakeDirectStsCredentialSource source = source("org/0188 x", true);
        TeamCredentials vended = source.vend(SESSION, EVENTS, Set.of(42L));
        assertThat(vended.region()).isEqualTo("us-east-1");
        assertThat(vended.bucket()).isEqualTo("example-bucket");
        assertThat(vended.credentials().get(42L).prefix()).isEqualTo("millrace/millrace-dev/data/ingest/events/team_id=42/");
        assertThat(vended.credentials().get(42L).accessKeyId()).isEqualTo("ASIA42-1");

        Map<String, String> request = requests.getFirst();
        assertThat(request).containsEntry("Action", "AssumeRole")
                .containsEntry("RoleArn", "arn:aws:iam::111122223333:role/hoglake-events-reader-dev")
                .containsEntry("DurationSeconds", "3600")
                .containsEntry("Tags.member.1.Key", "hoglake-team-id")
                .containsEntry("Tags.member.1.Value", "42")
                .containsEntry("Tags.member.2.Key", "hoglake-org-id")
                .containsEntry("Tags.member.2.Value", "org/0188 x")
                .containsEntry("Policy", "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\"," +
                        "\"Resource\":\"arn:aws:s3:::example-bucket/millrace/millrace-dev/data/ingest/events/team_id=42/*\"}]}")
                .doesNotContainKey("SourceIdentity");
        assertThat(request.get("RoleSessionName")).matches("hoglake-42-org-0188-x-[0-9a-f]{8}");
        assertThat(HoglakeDirectStsCredentialSource.sessionName(9_223_372_036_854_775_807L, "o".repeat(80)))
                .hasSize(64)
                .matches("[\\w+=,.@-]+");
    }

    @Test
    void testCacheServesOnlyWhileThirtyMinutesRemain()
    {
        HoglakeDirectStsCredentialSource source = source("org", true);
        TeamCredentials first = source.vend(SESSION, EVENTS, Set.of(4L, 42L));
        assertThat(requests).hasSize(2);
        // Inside the lifetime: no call.
        assertThat(source.vend(SESSION, EVENTS, Set.of(4L, 42L)).credentials()).isEqualTo(first.credentials());
        assertThat(source.vend(SESSION, EVENTS, Set.of(4L)).credentials().get(4L)).isEqualTo(first.credentials().get(4L));
        assertThat(requests).hasSize(2);

        // An entry with less than 30 minutes left is minted again, on every vend.
        lifetime = Duration.ofMinutes(29);
        source.vend(SESSION, EVENTS, Set.of(7L));
        source.vend(SESSION, EVENTS, Set.of(7L));
        assertThat(requests).hasSize(4);
        lifetime = Duration.ofMinutes(31);
        TeamCredentials renewed = source.vend(SESSION, EVENTS, Set.of(7L));
        assertThat(source.vend(SESSION, EVENTS, Set.of(7L))).isEqualTo(renewed);
        assertThat(requests).hasSize(5);
        // The account and the data path are resolved once.
        assertThat(callerIdentityRequests).hasValue(1);
        assertThat(dataPathReads).hasValue(1);
    }

    @Test
    void testOneFailingTeamFailsTheVend()
    {
        deniedTeam = "42";
        HoglakeDirectStsCredentialSource source = source("org", true);
        assertErrorCode(() -> source.vend(SESSION, EVENTS, Set.of(4L, 42L)), HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE);
        assertThatThrownBy(() -> source.vend(SESSION, EVENTS, Set.of(42L)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("AssumeRole for team 42 failed: AccessDenied")
                .hasNoCause();
    }

    @Test
    void testVendOnWorkerThrows()
    {
        HoglakeDirectStsCredentialSource source = source("org", false);
        assertThatThrownBy(() -> source.vend(SESSION, EVENTS, Set.of(4L)))
                .hasMessageContaining("coordinator only");
        assertThat(requests).isEmpty();
        assertThat(callerIdentityRequests).hasValue(0);
    }

    /**
     * Only the millrace-dev catalog's ingest.events: another catalog fails
     * at startup, and another table fails before any STS call.
     */
    @Test
    void testServesOnlyTheDevEventsTable()
    {
        assertThatThrownBy(() -> new HoglakeDirectStsCredentialSource("millrace-prod-us", "org", node(true), () -> DATA_PATH, sts))
                .hasMessageContaining("serves only the millrace-dev catalog, not millrace-prod-us");
        HoglakeDirectStsCredentialSource source = source("org", true);
        SchemaTableName eventsV2 = new SchemaTableName("ingest", "events_v2");
        assertThatThrownBy(() -> source.vend(SESSION, eventsV2, Set.of(4L)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("hoglake.s3-credentials.source=direct-sts reads only millrace-dev ingest.events, not ingest.events_v2");
        assertThatThrownBy(() -> source.teamOf(eventsV2, "s3://example-bucket/millrace/millrace-dev/data/ingest/events_v2/team_id=4/a.parquet"))
                .hasMessageContaining("reads only millrace-dev ingest.events, not ingest.events_v2");
        assertThat(source.teamOf(EVENTS, DATA_PATH + "data/ingest/events/team_id=4/a.parquet")).isEqualTo(4);
        assertThat(requests).isEmpty();
        assertThat(callerIdentityRequests).hasValue(0);
    }

    private HoglakeDirectStsCredentialSource source(String orgId, boolean coordinator)
    {
        return new HoglakeDirectStsCredentialSource("millrace-dev", orgId, node(coordinator), () -> {
            dataPathReads.incrementAndGet();
            return DATA_PATH;
        }, sts);
    }

    private static Node node(boolean coordinator)
    {
        return new Node()
        {
            @Override
            public String getHost()
            {
                return "localhost";
            }

            @Override
            public HostAddress getHostAndPort()
            {
                return HostAddress.fromParts("localhost", 8080);
            }

            @Override
            public String getNodeIdentifier()
            {
                return "node";
            }

            @Override
            public String getVersion()
            {
                return "test";
            }

            @Override
            public boolean isCoordinator()
            {
                return coordinator;
            }
        };
    }
}
