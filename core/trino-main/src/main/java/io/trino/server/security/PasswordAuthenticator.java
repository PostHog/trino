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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;
import io.trino.server.ProtocolConfig;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.AuthenticatedPasswordPrincipal;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedMap;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static io.trino.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticator
        implements Authenticator
{
    private static final String HOGLAKE_SESSION_JWT = "internal$hoglake_session_jwt";
    private static final String OPERATOR_USER = "__trino_ops";

    private final PasswordAuthenticatorManager authenticatorManager;
    private final UserMapping userMapping;
    private final HostQualifiedUsers hostQualifiedUsers;
    private final Optional<String> alternateHeaderName;
    private final boolean callerTokensEnabled;
    private final BiFunction<String, String, String> callerTokenAcquirer;

    @Inject
    public PasswordAuthenticator(
            PasswordAuthenticatorManager authenticatorManager,
            PasswordAuthenticatorConfig config,
            ProtocolConfig protocolConfig,
            Optional<TrinoCallerTokenClient> callerTokenClient)
    {
        this(authenticatorManager,
                config,
                protocolConfig,
                callerTokenClient.isPresent(),
                (user, password) -> callerTokenClient.orElseThrow().acquire(user, password));
    }

    @VisibleForTesting
    PasswordAuthenticator(
            PasswordAuthenticatorManager authenticatorManager,
            PasswordAuthenticatorConfig config,
            ProtocolConfig protocolConfig,
            boolean callerTokensEnabled,
            BiFunction<String, String, String> callerTokenAcquirer)
    {
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        this.hostQualifiedUsers = config.createHostQualifiedUsers();
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
        this.callerTokensEnabled = callerTokensEnabled;
        this.callerTokenAcquirer = requireNonNull(callerTokenAcquirer, "callerTokenAcquirer is null");
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        BasicAuthCredentials basicAuthCredentials = extractBasicAuthCredentials(request)
                .orElseThrow(() -> needAuthentication(null));
        return authenticate(basicAuthCredentials, requestHost(request), request.getHeaders());
    }

    @VisibleForTesting
    Identity authenticate(BasicAuthCredentials basicAuthCredentials, Optional<String> host, MultivaluedMap<String, String> headers)
            throws AuthenticationException
    {
        // The typed user stays in basicAuthCredentials: rewriteUserHeaderToMappedUser compares the
        // client's X-Trino-User against it, and replaces it with the qualified identity on a match.
        String user = hostQualifiedUsers.qualify(basicAuthCredentials.getUser(), host);
        String password = basicAuthCredentials.getPassword()
                .orElseThrow(() -> new AuthenticationException("Malformed credentials: password is empty"));

        AuthenticationException exception = null;
        Identity originalIdentity = null;
        Identity mappedIdentity = null;
        for (io.trino.spi.security.PasswordAuthenticator authenticator : authenticatorManager.getAuthenticators()) {
            boolean passwordVerified = false;
            try {
                Optional<Identity> authenticatedIdentity = authenticator.createAuthenticatedIdentity(user, password);
                if (authenticatedIdentity.isPresent()) {
                    originalIdentity = authenticatedIdentity.get();
                }
                else {
                    Principal principal = authenticator.createAuthenticatedPrincipal(user, password);
                    originalIdentity = Identity.forUser(principal.toString())
                            .withPrincipal(principal)
                            .build();
                }
                passwordVerified = true;
                mappedIdentity = userMapping.mapIdentity(originalIdentity);
                break;
            }
            catch (UserMappingException | AccessDeniedException e) {
                if (callerTokensEnabled && passwordVerified) {
                    throw new TerminalAuthenticationException();
                }
                if (exception == null) {
                    exception = needAuthentication(e.getMessage());
                }
                else {
                    exception.addSuppressed(needAuthentication(e.getMessage()));
                }
            }
            catch (RuntimeException e) {
                throw new RuntimeException("Authentication error", e);
            }
        }

        if (mappedIdentity == null) {
            verify(exception != null, "exception not set");
            throw exception;
        }

        // Selection follows the successful password boundary, outside its recoverable failures.
        Identity identity = acquireCallerToken(user, password, requireNonNull(originalIdentity, "originalIdentity is null"), mappedIdentity);
        return rewriteUserHeaderToMappedUser(identity, basicAuthCredentials, headers);
    }

    private Identity acquireCallerToken(String qualifiedUser, String password, Identity originalIdentity, Identity mappedIdentity)
            throws TerminalAuthenticationException
    {
        Principal principal = originalIdentity.getPrincipal().orElse(null);
        if (!(principal instanceof AuthenticatedPasswordPrincipal evidence)) {
            if (callerTokensEnabled) {
                throw new TerminalAuthenticationException();
            }
            return mappedIdentity;
        }

        Identity identity = Identity.from(mappedIdentity)
                .withPrincipal(new BasicPrincipal(evidence.getName()))
                .build();
        if (!callerTokensEnabled) {
            return identity;
        }

        Map<String, String> credentials = new HashMap<>(identity.getExtraCredentials());
        credentials.remove(HOGLAKE_SESSION_JWT);
        identity = Identity.from(identity).withExtraCredentials(credentials).build();

        if (!qualifiedUser.equals(evidence.getName()) || !qualifiedUser.equals(originalIdentity.getUser())) {
            throw new TerminalAuthenticationException();
        }
        if (evidence.isOperator()) {
            if (!OPERATOR_USER.equals(evidence.getName()) || !OPERATOR_USER.equals(mappedIdentity.getUser()) || evidence.backend().isPresent()) {
                throw new TerminalAuthenticationException();
            }
            return identity;
        }

        Optional<String> originalDatabase = qualifiedDatabase(evidence.getName());
        if (OPERATOR_USER.equals(evidence.getName()) || evidence.backend().isEmpty() ||
                originalDatabase.isEmpty() || !originalDatabase.equals(qualifiedDatabase(mappedIdentity.getUser()))) {
            throw new TerminalAuthenticationException();
        }
        switch (evidence.backend().orElseThrow()) {
            case DUCKLAKE -> {
                return identity;
            }
            case HOGLAKE -> {
                String token;
                try {
                    token = callerTokenAcquirer.apply(evidence.getName(), password);
                    if (token == null || token.isEmpty()) {
                        throw new TerminalAuthenticationException();
                    }
                }
                catch (RuntimeException _) {
                    // Issuer failures can contain caller evidence or response bodies.
                    throw new TerminalAuthenticationException();
                }
                return Identity.from(identity)
                        .withAdditionalExtraCredentials(Map.of(HOGLAKE_SESSION_JWT, token))
                        .build();
            }
        }
        throw new TerminalAuthenticationException();
    }

    private static Optional<String> qualifiedDatabase(String user)
    {
        int separator = user.indexOf('.');
        if (separator <= 0 || separator == user.length() - 1) {
            return Optional.empty();
        }
        return Optional.of(user.substring(0, separator));
    }

    /**
     * When the user in the basic authentication header matches the x-trino-user header, we assume that the client does
     * not want to force the runtime user name, and only wanted to communicate the authentication user.
     */
    private Identity rewriteUserHeaderToMappedUser(Identity mappedIdentity, BasicAuthCredentials basicAuthCredentials, MultivaluedMap<String, String> headers)
    {
        ProtocolHeaders protocolHeaders;
        try {
            protocolHeaders = detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException _) {
            // this shouldn't fail here, but ignore and it will be handled elsewhere
            return mappedIdentity;
        }
        // Current clients send the login in both the original-user and the user header. Rewrite every
        // header that repeats the typed login, not only the preferred one: when the mapped identity differs
        // from the typed login (a host-qualified user), a user header left as typed reads as a request to
        // impersonate the typed name, which the access control then denies. A header is only ever replaced
        // with the identity this request just authenticated, so the rewrite grants nothing.
        String typedUser = basicAuthCredentials.getUser();
        for (String userHeader : List.of(protocolHeaders.requestOriginalUser(), protocolHeaders.requestUser())) {
            if (typedUser.equals(headers.getFirst(userHeader))) {
                headers.putSingle(userHeader, mappedIdentity.getUser());
            }
        }
        return mappedIdentity;
    }

    /**
     * The host the client addressed. This is the request URI host, so it honors forwarded headers
     * exactly when the HTTP server is configured to process them.
     */
    private static Optional<String> requestHost(ContainerRequestContext request)
    {
        return Optional.ofNullable(request.getUriInfo().getRequestUri().getHost());
    }

    private static AuthenticationException needAuthentication(String message)
    {
        return new AuthenticationException(message, BasicAuthCredentials.AUTHENTICATE_HEADER);
    }
}
