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
package io.trino.server.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.ProtocolConfig;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.AuthenticatedPasswordPrincipal;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import jakarta.ws.rs.core.MultivaluedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.DUCKLAKE;
import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.HOGLAKE;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPasswordAuthenticator
{
    private static final String USER = "example-db.alice";
    private static final String PASSWORD = "synthetic-caller-password";
    private static final String TOKEN_KEY = "internal$hoglake_session_jwt";
    private static final String TOKEN = "synthetic.exact.jwt";

    @TempDir
    Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBothPasswordSpiPathsAcquireAfterProof(boolean principalOnly)
            throws Exception
    {
        var proofs = new AtomicInteger();
        var acquisitions = new AtomicInteger();
        Principal principal = AuthenticatedPasswordPrincipal.tenant(USER, HOGLAKE);
        io.trino.spi.security.PasswordAuthenticator plugin = new io.trino.spi.security.PasswordAuthenticator()
        {
            @Override
            public Principal createAuthenticatedPrincipal(String user, String password)
            {
                assertThat(principalOnly).isTrue();
                verifyProof(user, password);
                return principal;
            }

            @Override
            public Optional<Identity> createAuthenticatedIdentity(String user, String password)
            {
                if (principalOnly) {
                    return Optional.empty();
                }
                verifyProof(user, password);
                return Optional.of(Identity.forUser(USER).withPrincipal(principal).build());
            }

            private void verifyProof(String user, String password)
            {
                assertThat(user).isEqualTo(USER);
                assertThat(password).isEqualTo(PASSWORD);
                proofs.incrementAndGet();
            }
        };
        PasswordAuthenticator authenticator = authenticator(new PasswordAuthenticatorConfig(), true, (user, password) -> {
            assertThat(proofs.get()).isEqualTo(1);
            assertThat(user).isEqualTo(USER);
            assertThat(password).isEqualTo(PASSWORD);
            acquisitions.incrementAndGet();
            return TOKEN;
        }, plugin);

        Identity identity = authenticate(authenticator, USER);
        assertThat(proofs.get()).isEqualTo(1);
        assertThat(acquisitions.get()).isEqualTo(1);
        assertThat(identity.getUser()).isEqualTo(USER);
        assertThat(identity.getPrincipal()).contains(new BasicPrincipal(USER));
        assertThat(identity.getExtraCredentials()).containsExactlyEntriesOf(Map.of(TOKEN_KEY, TOKEN));
        assertThat(identity.toString()).doesNotContain(PASSWORD, TOKEN);
    }

    @Test
    void testQualifiedVerifiedLoginDiffersFromMappedIdentity()
            throws Exception
    {
        String verifiedUser = "example-db.alice@allowed";
        var config = new PasswordAuthenticatorConfig()
                .setUserMappingPattern("(.*)@allowed")
                .setHostQualifiedUserDomains(List.of("tenants.example.com"));
        var headers = new MultivaluedHashMap<String, String>();
        headers.putSingle("X-Trino-User", "alice@allowed");
        headers.putSingle("X-Trino-Original-User", "alice@allowed");
        var acquisitions = new AtomicInteger();
        var authenticator = authenticator(config, true, (user, password) -> {
            assertThat(user).isEqualTo(verifiedUser);
            assertThat(password).isEqualTo(PASSWORD);
            assertThat(headers.getFirst("X-Trino-User")).isEqualTo("alice@allowed");
            assertThat(headers.getFirst("X-Trino-Original-User")).isEqualTo("alice@allowed");
            acquisitions.incrementAndGet();
            return TOKEN;
        }, identityPlugin(Identity.forUser(verifiedUser)
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant(verifiedUser, HOGLAKE))
                .withGroups(ImmutableSet.of("org_example_db", "tier_scale"))
                .withEnabledRoles(ImmutableSet.of("reader"))
                .withConnectorRoles(Map.of("catalog", new SelectedRole(ROLE, Optional.of("catalog_reader"))))
                .withExtraCredentials(Map.of("ordinary", "synthetic-ordinary", TOKEN_KEY, "stale-token"))
                .build()));

        Identity identity = authenticator.authenticate(credentials("alice@allowed"), Optional.of("example-db.tenants.example.com"), headers);
        assertThat(acquisitions.get()).isEqualTo(1);
        assertThat(identity.getUser()).isEqualTo(USER);
        assertThat(identity.getPrincipal()).contains(new BasicPrincipal(verifiedUser));
        assertThat(identity.getGroups()).containsExactlyInAnyOrder("org_example_db", "tier_scale");
        assertThat(identity.getEnabledRoles()).containsExactly("reader");
        assertThat(identity.getCatalogRoles()).containsExactlyEntriesOf(Map.of("catalog", new SelectedRole(ROLE, Optional.of("catalog_reader"))));
        assertThat(identity.getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(Map.of("ordinary", "synthetic-ordinary", TOKEN_KEY, TOKEN));
        assertThat(headers.getFirst("X-Trino-User")).isEqualTo(USER);
        assertThat(headers.getFirst("X-Trino-Original-User")).isEqualTo(USER);
        assertThat(identity.toConnectorIdentity().getPrincipal()).contains(new BasicPrincipal(verifiedUser));
    }

    @Test
    void testOnlyMatchingAlternateHeadersFollowSuccessfulMapping()
            throws Exception
    {
        var config = new PasswordAuthenticatorConfig().setUserMappingPattern("(.*)@allowed");
        var manager = manager(config, (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE));
        var authenticator = new PasswordAuthenticator(manager, config, new ProtocolConfig().setAlternateHeaderName("Alternate"), true, (_, _) -> TOKEN);
        var headers = new MultivaluedHashMap<String, String>();
        headers.putSingle("X-Alternate-User", "example-db.alice@allowed");
        headers.putSingle("X-Alternate-Original-User", "example-db.someone-else");

        Identity identity = authenticator.authenticate(credentials("example-db.alice@allowed"), Optional.empty(), headers);
        assertThat(identity.getUser()).isEqualTo(USER);
        assertThat(headers.getFirst("X-Alternate-User")).isEqualTo(USER);
        assertThat(headers.getFirst("X-Alternate-Original-User")).isEqualTo("example-db.someone-else");
    }

    @ParameterizedTest
    @ValueSource(strings = {"example-db.root", "example-db.svc_0123456789abcdef01234567"})
    void testRootAndServiceUseTheVerifiedQualifiedLogin(String login)
            throws Exception
    {
        var authenticator = authenticator(new PasswordAuthenticatorConfig(), true, (user, password) -> {
            assertThat(user).isEqualTo(login);
            assertThat(password).isEqualTo(PASSWORD);
            return TOKEN;
        }, (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE));

        assertThat(authenticate(authenticator, login).getExtraCredentials()).containsEntry(TOKEN_KEY, TOKEN);
    }

    @Test
    void testExplicitDuckLakeMakesNoIssuerRequest()
            throws Exception
    {
        var authenticator = authenticator(new PasswordAuthenticatorConfig(), true, noAcquisition(),
                identityPlugin(Identity.forUser(USER)
                        .withPrincipal(AuthenticatedPasswordPrincipal.tenant(USER, DUCKLAKE))
                        .withGroups(ImmutableSet.of("org_example_db", "tier_scale"))
                        .withExtraCredentials(Map.of("ordinary", "synthetic-ordinary", TOKEN_KEY, "stale-token"))
                        .build()));

        Identity identity = authenticate(authenticator, USER);
        assertThat(identity.getPrincipal()).contains(new BasicPrincipal(USER));
        assertThat(identity.getGroups()).containsExactlyInAnyOrder("org_example_db", "tier_scale");
        assertThat(identity.getExtraCredentials()).containsExactlyEntriesOf(Map.of("ordinary", "synthetic-ordinary"));
    }

    @Test
    void testTypedOperatorMakesNoIssuerRequest()
            throws Exception
    {
        var authenticator = authenticator(new PasswordAuthenticatorConfig(), true, noAcquisition(),
                identityPlugin(Identity.forUser("__trino_ops")
                        .withPrincipal(AuthenticatedPasswordPrincipal.operator("__trino_ops"))
                        .withGroups(ImmutableSet.of("existing_ops_group"))
                        .withExtraCredentials(Map.of(TOKEN_KEY, "stale-token"))
                        .build()));

        Identity identity = authenticate(authenticator, "__trino_ops");
        assertThat(identity.getPrincipal()).contains(new BasicPrincipal("__trino_ops"));
        assertThat(identity.getGroups()).containsExactly("existing_ops_group");
        assertThat(identity.getExtraCredentials()).isEmpty();
    }

    @Test
    void testUnknownEvidenceCannotFallBack()
    {
        for (Identity identity : List.of(
                Identity.forUser(USER).withPrincipal(new BasicPrincipal(USER)).build(),
                Identity.ofUser(USER),
                Identity.forUser("__trino_ops").withPrincipal(new BasicPrincipal("__trino_ops")).build())) {
            assertTerminal(identity, new PasswordAuthenticatorConfig(), identity.getUser());
        }
    }

    @Test
    void testEvidenceMustMatchTheVerifiedCredential()
    {
        assertTerminal(Identity.forUser(USER)
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant("other-db.alice", HOGLAKE))
                .build(), new PasswordAuthenticatorConfig(), USER);
        assertTerminal(Identity.forUser("other-db.alice")
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant(USER, HOGLAKE))
                .build(), new PasswordAuthenticatorConfig(), USER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"other-db.alice", "alice", "__trino_ops", "example-db.", "Example-db.alice"})
    void testTenantMappingRequiresTheExactDatabasePrefix(String mappedUser)
            throws Exception
    {
        assertTerminal(Identity.forUser(USER)
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant(USER, HOGLAKE))
                .build(), mappedTo(mappedUser), USER);
    }

    @Test
    void testDuckLakeMappingAlsoRequiresTheDatabaseBinding()
            throws Exception
    {
        assertTerminal(Identity.forUser(USER)
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant(USER, DUCKLAKE))
                .build(), mappedTo("other-db.alice"), USER);
    }

    @Test
    void testUnqualifiedTenantCannotAcquire()
    {
        assertTerminal(Identity.forUser("alice")
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant("alice", HOGLAKE))
                .build(), new PasswordAuthenticatorConfig(), "alice");
    }

    @Test
    void testOperatorExemptionRequiresBothExactNames()
            throws Exception
    {
        assertTerminal(Identity.forUser(USER)
                .withPrincipal(AuthenticatedPasswordPrincipal.operator(USER))
                .build(), mappedTo("__trino_ops"), USER);
        assertTerminal(Identity.forUser("__trino_ops")
                .withPrincipal(AuthenticatedPasswordPrincipal.operator("__trino_ops"))
                .build(), mappedTo(USER), "__trino_ops");
        assertTerminal(Identity.forUser("__trino_ops")
                .withPrincipal(AuthenticatedPasswordPrincipal.tenant("__trino_ops", DUCKLAKE))
                .build(), new PasswordAuthenticatorConfig(), "__trino_ops");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMappingRuleRefusalIsTerminalOnlyWhenEnabled(boolean enabled)
            throws Exception
    {
        var laterProofs = new AtomicInteger();
        var config = new PasswordAuthenticatorConfig().setUserMappingPattern("(.*)@allowed");
        var authenticator = authenticator(
                config,
                enabled,
                noAcquisition(),
                (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE),
                (_, _) -> {
                    laterProofs.incrementAndGet();
                    return new BasicPrincipal("legacy-user@allowed");
                });
        if (enabled) {
            assertThatThrownBy(() -> authenticate(authenticator, USER))
                    .isExactlyInstanceOf(TerminalAuthenticationException.class)
                    .hasMessage("Managed authentication failed")
                    .hasNoCause();
            assertThat(laterProofs.get()).isZero();
        }
        else {
            assertThat(authenticate(authenticator, USER).getUser()).isEqualTo("legacy-user");
            assertThat(laterProofs.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"other-db.alice", "alice", "__trino_ops"})
    void testDisabledAcquisitionPreservesLegacyMapping(String mappedUser)
            throws Exception
    {
        var authenticator = authenticator(mappedTo(mappedUser), false, noAcquisition(),
                identityPlugin(Identity.forUser(USER)
                        .withPrincipal(AuthenticatedPasswordPrincipal.tenant(USER, HOGLAKE))
                        .withGroups(ImmutableSet.of("ordinary_group"))
                        .withExtraCredentials(Map.of("ordinary", "synthetic-ordinary"))
                        .build()));

        Identity identity = authenticate(authenticator, USER);
        assertThat(identity.getUser()).isEqualTo(mappedUser);
        assertThat(identity.getPrincipal()).contains(new BasicPrincipal(USER));
        assertThat(identity.getGroups()).containsExactly("ordinary_group");
        assertThat(identity.getExtraCredentials()).containsExactlyEntriesOf(Map.of("ordinary", "synthetic-ordinary"));
    }

    @Test
    void testDisabledAcquisitionPreservesPlainPrincipal()
            throws Exception
    {
        Principal principal = () -> USER;
        var authenticator = authenticator(
                new PasswordAuthenticatorConfig(),
                false,
                noAcquisition(),
                identityPlugin(Identity.forUser(USER).withPrincipal(principal).build()));

        assertThat(authenticate(authenticator, USER).getPrincipal()).containsSame(principal);
    }

    @Test
    void testDisabledAcquisitionPreservesCredentialsForEveryEvidence()
            throws Exception
    {
        for (Principal principal : List.of(
                AuthenticatedPasswordPrincipal.tenant(USER, HOGLAKE),
                AuthenticatedPasswordPrincipal.tenant(USER, DUCKLAKE),
                AuthenticatedPasswordPrincipal.operator("__trino_ops"),
                new BasicPrincipal(USER))) {
            var authenticator = authenticator(new PasswordAuthenticatorConfig(), false, noAcquisition(),
                    identityPlugin(Identity.forUser(principal.getName())
                            .withPrincipal(principal)
                            .withExtraCredentials(Map.of("ordinary", "synthetic-ordinary", TOKEN_KEY, "existing-token"))
                            .build()));
            Identity identity = authenticate(authenticator, principal.getName());
            assertThat(identity.getPrincipal()).contains(new BasicPrincipal(principal.getName()));
            assertThat(identity.getExtraCredentials()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "ordinary", "synthetic-ordinary",
                    TOKEN_KEY, "existing-token"));
        }
    }

    @Test
    void testPasswordFailuresRemainRecoverableBeforeSuccessfulProof()
            throws Exception
    {
        var secondProofs = new AtomicInteger();
        var authenticator = authenticator(
                new PasswordAuthenticatorConfig(),
                true,
                (_, _) -> TOKEN,
                (_, _) -> {
                    throw new AccessDeniedException("Invalid credentials");
                },
                (user, _) -> {
                    secondProofs.incrementAndGet();
                    return AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE);
                });

        assertThat(authenticate(authenticator, USER).getExtraCredentials()).containsEntry(TOKEN_KEY, TOKEN);
        assertThat(secondProofs.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid_grant", "timeout", "disabled", "malformed", "signing", "throttled"})
    void testIssuerFailuresAreSanitizedWithoutPasswordFallback(String failure)
    {
        var laterProofs = new AtomicInteger();
        var authenticator = authenticator(new PasswordAuthenticatorConfig(), true, (_, _) -> {
            throw new IllegalStateException(failure + PASSWORD + TOKEN);
        }, (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE), (_, _) -> {
            laterProofs.incrementAndGet();
            return new BasicPrincipal(USER);
        });
        var headers = new MultivaluedHashMap<String, String>();
        headers.putSingle("X-Trino-User", USER);
        headers.putSingle("X-Trino-Original-User", USER);

        assertThatThrownBy(() -> authenticator.authenticate(credentials(USER), Optional.empty(), headers))
                .isExactlyInstanceOf(TerminalAuthenticationException.class)
                .hasMessage("Managed authentication failed")
                .hasNoCause();
        assertThat(laterProofs.get()).isZero();
        assertThat(headers.getFirst("X-Trino-User")).isEqualTo(USER);
        assertThat(headers.getFirst("X-Trino-Original-User")).isEqualTo(USER);
    }

    @Test
    void testMissingAcquiredTokenIsTerminal()
    {
        for (BiFunction<String, String, String> acquirer : List.<BiFunction<String, String, String>>of((_, _) -> "", (_, _) -> null)) {
            var authenticator = authenticator(
                    new PasswordAuthenticatorConfig(),
                    true,
                    acquirer,
                    (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE));
            assertThatThrownBy(() -> authenticate(authenticator, USER))
                    .isExactlyInstanceOf(TerminalAuthenticationException.class)
                    .hasNoCause();
        }
    }

    @Test
    void testInterleavedCallersKeepExactIndependentTokens()
            throws Exception
    {
        var acquisitions = new AtomicInteger();
        var authenticator = authenticator(
                new PasswordAuthenticatorConfig(),
                true,
                (user, _) -> "synthetic." + user + "." + acquisitions.incrementAndGet(),
                (user, _) -> AuthenticatedPasswordPrincipal.tenant(user, HOGLAKE));

        Identity alice = authenticate(authenticator, USER);
        Identity bob = authenticate(authenticator, "example-db.bob");
        Identity aliceAgain = authenticate(authenticator, USER);
        assertThat(alice.getExtraCredentials()).containsExactlyEntriesOf(Map.of(TOKEN_KEY, "synthetic.example-db.alice.1"));
        assertThat(bob.getExtraCredentials()).containsExactlyEntriesOf(Map.of(TOKEN_KEY, "synthetic.example-db.bob.2"));
        assertThat(aliceAgain.getExtraCredentials()).containsExactlyEntriesOf(Map.of(TOKEN_KEY, "synthetic.example-db.alice.3"));
        assertThat(alice.getPrincipal()).contains(new BasicPrincipal(USER));
        assertThat(bob.getPrincipal()).contains(new BasicPrincipal("example-db.bob"));
        assertThat(acquisitions.get()).isEqualTo(3);

        for (Identity identity : List.of(alice, bob, aliceAgain)) {
            Session session = Session.builder(testSessionBuilder().build())
                    .setIdentity(identity)
                    .setOriginalIdentity(Identity.from(identity).withExtraCredentials(ImmutableMap.of()).build())
                    .build();
            Session workerSession = session.toSessionRepresentation()
                    .toSession(new SessionPropertyManager(), identity.getExtraCredentials(), Optional.empty());
            assertThat(workerSession.getIdentity().getExtraCredentials()).containsExactlyEntriesOf(identity.getExtraCredentials());
            assertThat(workerSession.getIdentity().toConnectorIdentity().getExtraCredentials()).containsExactlyEntriesOf(identity.getExtraCredentials());
            assertThat(workerSession.getIdentity().getPrincipal()).contains(new BasicPrincipal(identity.getPrincipal().orElseThrow().getName()));
            assertThat(workerSession.toString()).doesNotContain(PASSWORD, identity.getExtraCredentials().get(TOKEN_KEY));
        }
    }

    private void assertTerminal(Identity identity, PasswordAuthenticatorConfig config, String login)
    {
        var laterProofs = new AtomicInteger();
        var authenticator = authenticator(config, true, noAcquisition(), identityPlugin(identity), (_, _) -> {
            laterProofs.incrementAndGet();
            return new BasicPrincipal(login);
        });
        assertThatThrownBy(() -> authenticate(authenticator, login))
                .isExactlyInstanceOf(TerminalAuthenticationException.class)
                .hasMessage("Managed authentication failed")
                .hasNoCause();
        assertThat(laterProofs.get()).isZero();
    }

    private PasswordAuthenticatorConfig mappedTo(String user)
            throws IOException
    {
        Path mapping = Files.writeString(
                temporaryDirectory.resolve("mapping.json"),
                "{\"rules\":[{\"pattern\":\"^.*$\",\"user\":\"" + user + "\"}]}");
        return new PasswordAuthenticatorConfig().setUserMappingFile(mapping.toFile());
    }

    private static PasswordAuthenticator authenticator(
            PasswordAuthenticatorConfig config,
            boolean enabled,
            BiFunction<String, String, String> acquirer,
            io.trino.spi.security.PasswordAuthenticator... plugins)
    {
        return new PasswordAuthenticator(manager(config, plugins), config, new ProtocolConfig(), enabled, acquirer);
    }

    private static PasswordAuthenticatorManager manager(PasswordAuthenticatorConfig config, io.trino.spi.security.PasswordAuthenticator... plugins)
    {
        var manager = new PasswordAuthenticatorManager(config, new SecretsResolver(ImmutableMap.of()));
        manager.setAuthenticators(plugins);
        return manager;
    }

    private static Identity authenticate(PasswordAuthenticator authenticator, String user)
            throws AuthenticationException
    {
        return authenticator.authenticate(credentials(user), Optional.empty(), new MultivaluedHashMap<>());
    }

    private static BasicAuthCredentials credentials(String user)
    {
        return new BasicAuthCredentials(user, Optional.of(PASSWORD));
    }

    private static BiFunction<String, String, String> noAcquisition()
    {
        return (_, _) -> {
            throw new AssertionError("The caller must not acquire a token");
        };
    }

    private static io.trino.spi.security.PasswordAuthenticator identityPlugin(Identity identity)
    {
        return new io.trino.spi.security.PasswordAuthenticator()
        {
            @Override
            public Principal createAuthenticatedPrincipal(String user, String password)
            {
                throw new AssertionError("The principal fallback must not run");
            }

            @Override
            public Optional<Identity> createAuthenticatedIdentity(String user, String password)
            {
                return Optional.of(identity);
            }
        };
    }
}
