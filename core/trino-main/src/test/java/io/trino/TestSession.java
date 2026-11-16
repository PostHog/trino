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
package io.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.QueryId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSession
{
    private static final String CALLER_JWT_CREDENTIAL = "internal$hoglake_session_jwt";
    private static final String FIRST_CALLER_JWT = "synthetic-header-a.synthetic-payload-a.synthetic-signature-a";
    private static final String SECOND_CALLER_JWT = "synthetic-header-b.synthetic-payload-b.synthetic-signature-b";
    private static final String FIRST_ORDINARY_CREDENTIAL = "synthetic-storage-secret-a";
    private static final String SECOND_ORDINARY_CREDENTIAL = "synthetic-storage-secret-b";

    @Test
    public void testSetCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of("first_property", "some_value")));
    }

    @Test
    public void testBuildWithCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();
        session = Session.builder(session)
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of("first_property", "some_value")));
    }

    @Test
    public void testAddSecondCatalogProperty()
    {
        Session session = Session.builder(testSessionBuilder().build())
                .setCatalogSessionProperty("some_catalog", "first_property", "some_value")
                .build();
        session = Session.builder(session)
                .setCatalogSessionProperty("some_catalog", "second_property", "another_value")
                .build();

        assertThat(session.getCatalogProperties())
                .isEqualTo(Map.of("some_catalog", Map.of(
                        "first_property", "some_value",
                        "second_property", "another_value")));
    }

    @Test
    public void testCallerCredentialExcludedFromSessionRepresentation()
    {
        JsonCodec<SessionRepresentation> codec = new JsonCodecFactory(
                new JsonMapperProvider()
                        .withJsonSerializers(ImmutableMap.of(Span.class, new SpanSerializer(OpenTelemetry.noop())))
                        .withJsonDeserializers(ImmutableMap.of(Span.class, new SpanDeserializer(OpenTelemetry.noop())))
                        .get())
                .jsonCodec(SessionRepresentation.class);
        var sessionPropertyManager = new SessionPropertyManager();

        for (Session session : callerCredentialSessions()) {
            SessionRepresentation representation = session.toSessionRepresentation();
            String json = codec.toJson(representation);
            assertThat(json).doesNotContain(
                    CALLER_JWT_CREDENTIAL,
                    FIRST_CALLER_JWT,
                    SECOND_CALLER_JWT,
                    FIRST_ORDINARY_CREDENTIAL,
                    SECOND_ORDINARY_CREDENTIAL);
            assertThat(ImmutableList.of(
                    session.toString(),
                    session.getIdentity().toString(),
                    session.getIdentity().toConnectorIdentity().toString()))
                    .allSatisfy(value -> assertThat(value).doesNotContain(
                            FIRST_CALLER_JWT,
                            SECOND_CALLER_JWT,
                            FIRST_ORDINARY_CREDENTIAL,
                            SECOND_ORDINARY_CREDENTIAL));

            SessionRepresentation restoredRepresentation = codec.fromJson(json);
            assertThat(restoredRepresentation.getUser()).isEqualTo(session.getUser());
            assertThat(restoredRepresentation.getGroups()).isEqualTo(session.getIdentity().getGroups());
            assertThat(restoredRepresentation.toIdentity().getExtraCredentials()).isEmpty();
            Session restoredWithoutCredentials = restoredRepresentation.toSession(sessionPropertyManager);
            assertThat(restoredWithoutCredentials.getIdentity().getExtraCredentials()).isEmpty();
            assertThat(restoredWithoutCredentials.getOriginalIdentity().getExtraCredentials()).isEmpty();
        }
    }

    @Test
    public void testCallerCredentialWorkerRestorationAndConnectorIdentity()
    {
        var sessionPropertyManager = new SessionPropertyManager();
        var catalogHandle = createRootCatalogHandle(new CatalogName("hoglake"), new CatalogVersion("test"));

        for (Session session : callerCredentialSessions()) {
            Map<String, String> credentials = session.getIdentity().getExtraCredentials();
            Session workerSession = session.toSessionRepresentation()
                    .toSession(sessionPropertyManager, credentials, Optional.empty());

            assertThat(workerSession.getUser()).isEqualTo(session.getUser());
            assertThat(workerSession.getIdentity().getPrincipal()).isEqualTo(session.getIdentity().getPrincipal());
            assertThat(workerSession.getIdentity().getGroups()).isEqualTo(session.getIdentity().getGroups());
            assertThat(workerSession.getIdentity().getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(credentials);
            assertThat(workerSession.getOriginalIdentity().getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(credentials);
            assertThat(session.getOriginalIdentity().getExtraCredentials()).isEmpty();

            for (Session connectorSessionSource : ImmutableList.of(session, workerSession)) {
                assertThat(connectorSessionSource.getIdentity().toConnectorIdentity().getExtraCredentials())
                        .containsExactlyInAnyOrderEntriesOf(credentials);
                assertThat(connectorSessionSource.getIdentity().toConnectorIdentity("hoglake").getExtraCredentials())
                        .containsExactlyInAnyOrderEntriesOf(credentials);
                assertThat(connectorSessionSource.toConnectorSession().getIdentity().getExtraCredentials())
                        .containsExactlyInAnyOrderEntriesOf(credentials);
                assertThat(connectorSessionSource.toConnectorSession(catalogHandle).getIdentity().getExtraCredentials())
                        .containsExactlyInAnyOrderEntriesOf(credentials);
            }
        }
    }

    @Test
    public void testCreateViewSession()
    {
        Optional<String> catalog = Optional.of("test_catalog");
        Optional<String> schema = Optional.of("test_schema");
        QueryId queryId = new QueryId("test_query_id");
        TransactionId transactionId = TransactionId.create();
        Identity identity = new Identity.Builder("test_user").build();
        Identity originalIdentity = new Identity.Builder("test_original_user").build();
        Optional<String> source = Optional.of("test_source");
        TimeZoneKey timeZoneKey = TimeZoneKey.UTC_KEY;
        Locale locale = Locale.ENGLISH;
        Optional<String> remoteUserAddress = Optional.of("1.1.1.1");
        Optional<String> userAgent = Optional.of("test_agent");
        Optional<String> clientInfo = Optional.of("test_client_info");
        Optional<String> traceToken = Optional.of("test_trace_token");
        Instant start = Instant.ofEpochMilli(2L);

        Session originalSession = Session.builder(testSessionBuilder().build())
                .setQueryId(queryId)
                .setTransactionId(transactionId)
                .setOriginalIdentity(originalIdentity)
                .setSource(source)
                .setTimeZoneKey(timeZoneKey)
                .setLocale(locale)
                .setRemoteUserAddress(remoteUserAddress)
                .setUserAgent(userAgent)
                .setClientInfo(clientInfo)
                .setTraceToken(traceToken)
                .setStart(start)
                .build();

        Session viewSession = originalSession.createViewSession(catalog, schema, identity, ImmutableList.of());

        assertThat(viewSession).isNotNull();
        assertThat(viewSession.getQueryId()).isEqualTo(queryId);
        assertThat(viewSession.getTransactionId()).isEqualTo(Optional.of(transactionId));
        assertThat(viewSession.getOriginalIdentity()).isEqualTo(originalIdentity);
        assertThat(viewSession.getSource()).isEqualTo(source);
        assertThat(viewSession.getTimeZoneKey()).isEqualTo(timeZoneKey);
        assertThat(viewSession.getLocale()).isEqualTo(locale);
        assertThat(viewSession.getRemoteUserAddress()).isEqualTo(remoteUserAddress);
        assertThat(viewSession.getUserAgent()).isEqualTo(userAgent);
        assertThat(viewSession.getClientInfo()).isEqualTo(clientInfo);
        assertThat(viewSession.getTraceToken()).isEqualTo(traceToken);
        assertThat(viewSession.getStart()).isEqualTo(start);
    }

    private static List<Session> callerCredentialSessions()
    {
        Session firstCaller = callerCredentialSession("example.alice", ImmutableMap.of(
                CALLER_JWT_CREDENTIAL, FIRST_CALLER_JWT,
                "storage_token", FIRST_ORDINARY_CREDENTIAL));
        Session secondCaller = callerCredentialSession("example.bob", ImmutableMap.of(
                CALLER_JWT_CREDENTIAL, SECOND_CALLER_JWT,
                "storage_token", SECOND_ORDINARY_CREDENTIAL));
        Session credentialFreeCaller = callerCredentialSession("example.carol", ImmutableMap.of());
        return ImmutableList.of(firstCaller, credentialFreeCaller, secondCaller, credentialFreeCaller, firstCaller);
    }

    private static Session callerCredentialSession(String user, Map<String, String> credentials)
    {
        Identity identity = Identity.forUser(user)
                .withPrincipal(new BasicPrincipal(user))
                .withGroups(ImmutableSet.of("org_example", "tier_free"))
                .withExtraCredentials(credentials)
                .build();
        return testSessionBuilder()
                .setIdentity(identity)
                .setOriginalIdentity(Identity.from(identity).withExtraCredentials(ImmutableMap.of()).build())
                .build();
    }
}
