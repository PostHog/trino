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
package io.trino.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.jaxrs.testing.GuavaMultivaluedMap;
import io.trino.client.ProtocolHeaders;
import io.trino.security.AllowAllAccessControl;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.security.UserMapping;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.client.ProtocolHeaders.createProtocolHeaders;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHttpRequestSessionContextFactory
{
    private static final String HOGLAKE_SESSION_JWT = "internal$hoglake_session_jwt";
    private static final String CALLER_TOKEN = "synthetic.HeAdEr_Caller.Caller-Signature_01";

    @Test
    public void testSessionContext()
    {
        assertSessionContext(TRINO_HEADERS);
        assertSessionContext(createProtocolHeaders("taco"));
    }

    private static void assertSessionContext(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(protocolHeaders.requestUser(), "testUser")
                .put(protocolHeaders.requestSource(), "testSource")
                .put(protocolHeaders.requestCatalog(), "testCatalog")
                .put(protocolHeaders.requestSchema(), "testSchema")
                .put(protocolHeaders.requestPath(), "testPath")
                .put(protocolHeaders.requestLanguage(), "zh-TW")
                .put(protocolHeaders.requestTimeZone(), "Asia/Taipei")
                .put(protocolHeaders.requestClientInfo(), "client-info")
                .put(protocolHeaders.requestSession(), QUERY_MAX_MEMORY + "=1GB")
                .put(protocolHeaders.requestSession(), "catalog.some_session_property=1GB")
                .put(protocolHeaders.requestSession(), "catalog.with.a.dot.some_session_property=1GB")
                .put(protocolHeaders.requestSession(), JOIN_DISTRIBUTION_TYPE + "=partitioned," + MAX_HASH_PARTITION_COUNT + " = 43")
                .put(protocolHeaders.requestSession(), "some_session_property=some value with %2C comma")
                .put(protocolHeaders.requestPreparedStatement(), "query1=select * from foo,query2=select * from bar")
                .put(protocolHeaders.requestRole(), "system=ROLE{system-role}")
                .put(protocolHeaders.requestRole(), "foo_connector=ALL")
                .put(protocolHeaders.requestRole(), "bar_connector=NONE")
                .put(protocolHeaders.requestRole(), "foobar_connector=ROLE{catalog-role}")
                .put(protocolHeaders.requestExtraCredential(), "test.token.foo=bar")
                .put(protocolHeaders.requestExtraCredential(), "test.token.abc=xyz")
                .build());

        SessionContext context = sessionContextFactory(protocolHeaders).createSessionContext(
                headers,
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getSource().orElse(null)).isEqualTo("testSource");
        assertThat(context.getCatalog().orElse(null)).isEqualTo("testcatalog"); // lowercased
        assertThat(context.getSchema().orElse(null)).isEqualTo("testschema"); // lowercased
        assertThat(context.getPath().orElse(null)).isEqualTo("testPath");
        assertThat(context.getIdentity()).isEqualTo(Identity.forUser("testUser")
                .withGroups(ImmutableSet.of("testUser"))
                .withConnectorRoles(ImmutableMap.of(
                        "foo_connector", new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                        "bar_connector", new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                        "foobar_connector", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("catalog-role"))))
                .withEnabledRoles(ImmutableSet.of("system-role"))
                .build());
        assertThat(context.getClientInfo().orElse(null)).isEqualTo("client-info");
        assertThat(context.getLanguage().orElse(null)).isEqualTo("zh-TW");
        assertThat(context.getTimeZoneId().orElse(null)).isEqualTo("Asia/Taipei");
        assertThat(context.getCatalogSessionProperties()).isEqualTo(ImmutableMap.of(
                "catalog", ImmutableMap.of("some_session_property", "1GB"),
                "catalog.with.a.dot", ImmutableMap.of("some_session_property", "1GB")));
        assertThat(context.getSystemProperties()).isEqualTo(ImmutableMap.of(
                QUERY_MAX_MEMORY, "1GB",
                JOIN_DISTRIBUTION_TYPE, "partitioned",
                MAX_HASH_PARTITION_COUNT, "43",
                "some_session_property", "some value with , comma"));
        assertThat(context.getPreparedStatements()).isEqualTo(ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertThat(context.getSelectedRole()).isEqualTo(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("system-role")));
        assertThat(context.getIdentity().getExtraCredentials()).isEqualTo(ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
    }

    @Test
    public void testMappedUser()
    {
        assertMappedUser(TRINO_HEADERS);
        assertMappedUser(createProtocolHeaders("taco"));
    }

    private static void assertMappedUser(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> userHeaders = new GuavaMultivaluedMap<>(ImmutableListMultimap.of(protocolHeaders.requestUser(), "testUser"));
        MultivaluedMap<String, String> emptyHeaders = new MultivaluedHashMap<>();

        SessionContext context = sessionContextFactory(protocolHeaders).createSessionContext(
                userHeaders,
                Optional.of("testRemote"),
                Optional.empty());
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("testUser")
                        .withGroups(ImmutableSet.of("testUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        context = sessionContextFactory(protocolHeaders).createSessionContext(
                emptyHeaders,
                Optional.of("testRemote"),
                Optional.of(Identity.forUser("mappedUser").withGroups(ImmutableSet.of("test")).build()));
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("mappedUser")
                        .withGroups(ImmutableSet.of("test", "mappedUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        context = sessionContextFactory(protocolHeaders).createSessionContext(
                userHeaders,
                Optional.of("testRemote"),
                Optional.of(Identity.ofUser("mappedUser")));
        assertThat(context.getIdentity())
                .isEqualTo(Identity.forUser("testUser")
                        .withGroups(ImmutableSet.of("testUser"))
                        .withEnabledRoles(ImmutableSet.of("system-role"))
                        .build());

        assertInvalidSession(protocolHeaders, emptyHeaders)
                .matches(e -> ((WebApplicationException) e).getResponse().getStatus() == 400);
    }

    @Test
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        assertPreparedStatementsHeaderDoesNotParse(TRINO_HEADERS);
        assertPreparedStatementsHeaderDoesNotParse(createProtocolHeaders("taco"));
    }

    private static void assertPreparedStatementsHeaderDoesNotParse(ProtocolHeaders protocolHeaders)
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(protocolHeaders.requestUser(), "testUser")
                .put(protocolHeaders.requestSource(), "testSource")
                .put(protocolHeaders.requestCatalog(), "testCatalog")
                .put(protocolHeaders.requestSchema(), "testSchema")
                .put(protocolHeaders.requestPath(), "testPath")
                .put(protocolHeaders.requestLanguage(), "zh-TW")
                .put(protocolHeaders.requestTimeZone(), "Asia/Taipei")
                .put(protocolHeaders.requestClientInfo(), "null")
                .put(protocolHeaders.requestPreparedStatement(), "query1=abcdefg")
                .build());

        assertInvalidSession(protocolHeaders, headers)
                .hasMessageMatching("Invalid " + protocolHeaders.requestPreparedStatement() + " header: line 1:1: mismatched input 'abcdefg'. Expecting: .*");
    }

    @Test
    public void testInternalExtraCredentialName()
    {
        MultivaluedMap<String, String> headers = new GuavaMultivaluedMap<>(ImmutableListMultimap.<String, String>builder()
                .put(TRINO_HEADERS.requestUser(), "testUser")
                .put(TRINO_HEADERS.requestExtraCredential(), "internal$abc=xyz")
                .build());

        assertInvalidSession(TRINO_HEADERS, headers)
                .hasMessage("Invalid extra credential name: internal$abc");
    }

    @Test
    public void testProtectedCredentialUserBinding()
            throws Exception
    {
        Identity authenticatedIdentity = UserMapping.createUserMapping(Optional.of("login-(.*)"), Optional.empty())
                .mapIdentity(Identity.forUser("login-mappedUser")
                        .withPrincipal(new BasicPrincipal("login-mappedUser"))
                        .withExtraCredentials(ImmutableMap.of(
                                HOGLAKE_SESSION_JWT, CALLER_TOKEN,
                                "ordinary", "authenticated-value",
                                "overridden", "authenticated-value",
                                "internal$other", "unrelated-value"))
                        .build());
        for (ProtocolHeaders protocolHeaders : List.of(TRINO_HEADERS, createProtocolHeaders("taco"))) {
            HttpRequestSessionContextFactory factory = sessionContextFactory(protocolHeaders);
            for (String user : List.of("", "mappedUser", "otherUser")) {
                for (String originalUser : List.of("", "mappedUser", "otherUser")) {
                    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
                    if (!user.isEmpty()) {
                        headers.putSingle(protocolHeaders.requestUser(), user);
                    }
                    if (!originalUser.isEmpty()) {
                        headers.putSingle(protocolHeaders.requestOriginalUser(), originalUser);
                    }
                    headers.add(protocolHeaders.requestExtraCredential(), "overridden=client-value");
                    headers.add(protocolHeaders.requestExtraCredential(), "client-only=client-value");
                    ImmutableMap.Builder<String, String> expectedCredentials = ImmutableMap.<String, String>builder()
                            .put("ordinary", "authenticated-value")
                            .put("overridden", "client-value")
                            .put("internal$other", "unrelated-value")
                            .put("client-only", "client-value");
                    if (!user.equals("otherUser") && !originalUser.equals("otherUser")) {
                        expectedCredentials.put(HOGLAKE_SESSION_JWT, CALLER_TOKEN);
                    }
                    Map<String, String> expected = expectedCredentials.buildOrThrow();

                    SessionContext context = factory.createSessionContext(headers, Optional.empty(), Optional.of(authenticatedIdentity));
                    assertThat(context.getIdentity().getExtraCredentials()).isEqualTo(expected);
                    assertThat(factory.extractAuthorizedIdentity(Optional.of(authenticatedIdentity), headers).getExtraCredentials()).isEqualTo(expected);
                    assertThat(context.getIdentity().getPrincipal()).contains(new BasicPrincipal("login-mappedUser"));
                    if (originalUser.isEmpty()) {
                        assertThat(context.getOriginalIdentity().getExtraCredentials()).isEqualTo(expected);
                    }
                    else {
                        assertThat(context.getOriginalIdentity().getExtraCredentials()).isEmpty();
                    }
                    assertThat(authenticatedIdentity.getExtraCredentials())
                            .containsEntry(HOGLAKE_SESSION_JWT, CALLER_TOKEN)
                            .containsEntry("overridden", "authenticated-value");
                }
            }
        }
    }

    @Test
    public void testInterleavedProtectedCredentials()
    {
        for (ProtocolHeaders protocolHeaders : List.of(TRINO_HEADERS, createProtocolHeaders("taco"))) {
            HttpRequestSessionContextFactory factory = sessionContextFactory(protocolHeaders);
            Identity firstCaller = Identity.forUser("firstCaller")
                    .withExtraCredentials(ImmutableMap.of(HOGLAKE_SESSION_JWT, CALLER_TOKEN))
                    .build();
            Identity secondCaller = Identity.forUser("secondCaller")
                    .withExtraCredentials(ImmutableMap.of(HOGLAKE_SESSION_JWT, "synthetic.header.second-signature"))
                    .build();
            for (Identity identity : List.of(firstCaller, Identity.ofUser("legacyCaller"), secondCaller, firstCaller)) {
                MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
                headers.putSingle(protocolHeaders.requestUser(), identity.getUser());
                SessionContext context = factory.createSessionContext(headers, Optional.empty(), Optional.of(identity));
                assertThat(context.getIdentity().getExtraCredentials()).isEqualTo(identity.getExtraCredentials());
            }
            MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
            headers.putSingle(protocolHeaders.requestUser(), "unauthenticatedCaller");
            headers.putSingle(protocolHeaders.requestExtraCredential(), "ordinary=client-value");
            assertThat(factory.createSessionContext(headers, Optional.empty(), Optional.empty()).getIdentity().getExtraCredentials())
                    .isEqualTo(ImmutableMap.of("ordinary", "client-value"));
            assertThat(factory.extractAuthorizedIdentity(Optional.empty(), headers).getExtraCredentials())
                    .isEqualTo(ImmutableMap.of("ordinary", "client-value"));
        }
    }

    @Test
    public void testProtectedCredentialInjection()
    {
        for (ProtocolHeaders protocolHeaders : List.of(TRINO_HEADERS, createProtocolHeaders("taco"))) {
            HttpRequestSessionContextFactory factory = sessionContextFactory(protocolHeaders);
            MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
            headers.putSingle(protocolHeaders.requestUser(), "mappedUser");
            headers.putSingle(protocolHeaders.requestExtraCredential(), HOGLAKE_SESSION_JWT + "=synthetic.header.injected-signature");
            Identity authenticatedIdentity = Identity.forUser("mappedUser")
                    .withExtraCredentials(ImmutableMap.of(HOGLAKE_SESSION_JWT, CALLER_TOKEN))
                    .build();
            for (Optional<Identity> identity : List.of(Optional.<Identity>empty(), Optional.of(authenticatedIdentity))) {
                assertThatThrownBy(() -> factory.createSessionContext(headers, Optional.empty(), identity))
                        .isInstanceOf(WebApplicationException.class)
                        .hasMessage("Invalid extra credential name: " + HOGLAKE_SESSION_JWT)
                        .hasMessageNotContaining(CALLER_TOKEN)
                        .hasMessageNotContaining("synthetic.header.injected-signature");
                assertThatThrownBy(() -> factory.extractAuthorizedIdentity(identity, headers))
                        .isInstanceOf(WebApplicationException.class)
                        .hasMessage("Invalid extra credential name: " + HOGLAKE_SESSION_JWT)
                        .hasMessageNotContaining(CALLER_TOKEN)
                        .hasMessageNotContaining("synthetic.header.injected-signature");
            }
        }
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertInvalidSession(ProtocolHeaders protocolHeaders, MultivaluedMap<String, String> headers)
    {
        return assertThatThrownBy(
                () -> sessionContextFactory(protocolHeaders)
                        .createSessionContext(headers, Optional.of("testRemote"), Optional.empty()))
                .isInstanceOf(WebApplicationException.class);
    }

    private static HttpRequestSessionContextFactory sessionContextFactory(ProtocolHeaders headers)
    {
        return new HttpRequestSessionContextFactory(
                new PreparedStatementEncoder(new ProtocolConfig()),
                createTestingMetadataManager(),
                ImmutableSet::of,
                new AllowAllAccessControl(),
                new ProtocolConfig()
                        .setAlternateHeaderName(headers.getProtocolName()),
                QueryDataEncoder.EncoderSelector.noEncoder());
    }
}
