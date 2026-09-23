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

import com.google.inject.Inject;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;
import io.trino.server.ProtocolConfig;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedMap;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static io.trino.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticator
        implements Authenticator
{
    private final PasswordAuthenticatorManager authenticatorManager;
    private final UserMapping userMapping;
    private final HostQualifiedUsers hostQualifiedUsers;
    private final Optional<String> alternateHeaderName;

    @Inject
    public PasswordAuthenticator(PasswordAuthenticatorManager authenticatorManager, PasswordAuthenticatorConfig config, ProtocolConfig protocolConfig)
    {
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        this.hostQualifiedUsers = config.createHostQualifiedUsers();
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        BasicAuthCredentials basicAuthCredentials = extractBasicAuthCredentials(request)
                .orElseThrow(() -> needAuthentication(null));
        // The typed user stays in basicAuthCredentials: rewriteUserHeaderToMappedUser compares the
        // client's X-Trino-User against it, and replaces it with the qualified identity on a match.
        String user = hostQualifiedUsers.qualify(basicAuthCredentials.getUser(), requestHost(request));
        String password = basicAuthCredentials.getPassword()
                .orElseThrow(() -> new AuthenticationException("Malformed credentials: password is empty"));

        AuthenticationException exception = null;
        for (io.trino.spi.security.PasswordAuthenticator authenticator : authenticatorManager.getAuthenticators()) {
            try {
                Optional<Identity> authenticatedIdentity = authenticator.createAuthenticatedIdentity(user, password);
                if (authenticatedIdentity.isPresent()) {
                    Identity mappedIdentity = userMapping.mapIdentity(authenticatedIdentity.get());
                    return rewriteUserHeaderToMappedUser(mappedIdentity, basicAuthCredentials, request.getHeaders());
                }
                Principal principal = authenticator.createAuthenticatedPrincipal(user, password);
                Identity mappedIdentity = userMapping.mapIdentity(Identity.forUser(principal.toString())
                        .withPrincipal(principal)
                        .build());
                return rewriteUserHeaderToMappedUser(mappedIdentity, basicAuthCredentials, request.getHeaders());
            }
            catch (UserMappingException | AccessDeniedException e) {
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

        verify(exception != null, "exception not set");
        throw exception;
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
