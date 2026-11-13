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

import io.airlift.units.DataSize;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeLoginTokenSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.Principal;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

/**
 * The read-only schema {@value #SCHEMA_NAME}: the org's events mount, the Hoglake
 * catalog {@code <orgID>-events} on the same server as the org catalog
 * {@code <orgID>}.
 *
 * <p>The mount is read with a token of the Trino login that asks, never with the
 * org catalog's token. {@link #client(ConnectorSession)} is the only way to a
 * client that can send a request, and it binds that client to the session's login.
 *
 * <p>With it configured, the name {@value #SCHEMA_NAME} always means the mount: a
 * namespace of that name in the org's own catalog is never listed, read or written,
 * whether or not the mount exists or answers. Falling back to the org's namespace
 * would let a mount outage put user-writable data under the name of PostHog's data.
 */
public final class HoglakePosthogSchema
        implements Closeable
{
    public static final String SCHEMA_NAME = "posthog";

    /**
     * What the subject of a per-login token request is: the name of a Trino login.
     */
    public static final String LOGIN_SUBJECT_TOKEN_TYPE = "urn:posthog:params:oauth:token-type:trino-login";

    /**
     * The audience of the per-login token that asks for the S3 sessions the
     * schema's files are read with. It is a second token of the login: the token
     * for the mount is not accepted where sessions are issued, and this one is
     * not accepted by the mount.
     */
    public static final String STORAGE_SESSION_AUDIENCE = "hogtower-vend";

    /**
     * The most teams one request for S3 sessions may name.
     */
    public static final int STORAGE_SESSION_TEAMS_PER_REQUEST = 10;

    /**
     * The least lifetime an issued S3 session must have left to be accepted: the
     * issuer's floor of ten minutes, less one minute for a difference between the
     * issuer's clock and this node's.
     */
    public static final Duration STORAGE_SESSION_ACCEPT_FLOOR = Duration.ofMinutes(9);

    public static final Duration STORAGE_SESSION_REQUEST_TIMEOUT = Duration.ofSeconds(20);

    /**
     * The most splits of the schema handed to the engine at once, the least the
     * engine takes: a split's session is then at most one batch old when the
     * engine receives it.
     */
    public static final int SPLITS_PER_BATCH = 100;

    /**
     * The largest target split size for the schema's tables. A split waits on a
     * worker behind the splits before it, on the lifetime of its session; smaller
     * splits make that queue shorter in bytes.
     */
    public static final DataSize MAX_SPLIT_SIZE = DataSize.of(64, MEGABYTE);

    // hoglake MountNames.PATTERN without the suffix: a canonical lowercase UUID.
    private static final Pattern ORG_ID = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static final String MOUNT_SUFFIX = "-events";

    private final String mountCatalog;
    // Sends nothing by itself: it has no token until it is bound to a login
    private final HoglakeClient mountClient;
    private final HoglakeLoginTokenSource loginTokens;

    public HoglakePosthogSchema(String orgCatalog, HoglakeClient mountClient, HoglakeLoginTokenSource loginTokens)
    {
        if (!isOrgCatalog(orgCatalog)) {
            throw new IllegalArgumentException("The posthog schema requires an org catalog named by a lowercase UUID");
        }
        this.mountCatalog = mountCatalog(orgCatalog);
        this.mountClient = requireNonNull(mountClient, "mountClient is null");
        this.loginTokens = requireNonNull(loginTokens, "loginTokens is null");
    }

    public static boolean isOrgCatalog(String catalog)
    {
        return catalog != null && ORG_ID.matcher(catalog).matches();
    }

    public static String mountCatalog(String orgCatalog)
    {
        return orgCatalog + MOUNT_SUFFIX;
    }

    /**
     * The scope a per-login token request names, so that the issuer returns a token
     * for the mount and not for the org catalog. This request field is a proposal
     * that the issuer has not agreed to yet; this method is the only place that
     * knows its form.
     */
    public static String mountScope(String mountCatalog)
    {
        return "hoglake:" + mountCatalog + ":read";
    }

    /**
     * The token source of the requests for S3 sessions: the same token exchange as
     * for the mount, with the audience of the service that issues the sessions and
     * no scope. The issuer decides what the token may ask for.
     */
    public static HoglakeLoginTokenSource storageSessionTokens(URI tokenUri, String clientId, String clientSecret, HttpClient httpClient)
    {
        return new HoglakeLoginTokenSource.TokenExchange(tokenUri, clientId, clientSecret, STORAGE_SESSION_AUDIENCE, LOGIN_SUBJECT_TOKEN_TYPE, Optional.empty(), httpClient);
    }

    /**
     * Whether a Trino schema name addresses this schema. Hoglake namespace names are
     * compared without case, so no spelling of an org namespace can stand beside it.
     */
    public static boolean isPosthogSchema(String schemaName)
    {
        return schemaName != null && schemaName.toLowerCase(Locale.ROOT).equals(SCHEMA_NAME);
    }

    public String mountCatalog()
    {
        return mountCatalog;
    }

    /**
     * The mount's client for the login of {@code session}. It is made for each call
     * and must not be kept: the next call may come from another login.
     */
    public HoglakeClient client(ConnectorSession session)
    {
        return mountClient.withLogin(loginTokens.forLogin(login(session)));
    }

    /**
     * The login a token is requested for, and whose S3 sessions a split source
     * hands out: the session user, accepted only when it is the login that
     * authenticated. Nothing else of the session is read.
     *
     * <p>The session user alone is not enough. An impersonated session has another
     * user than its principal, and the body of a view that runs as its owner is
     * analyzed with the owner as the user and no principal. Both are refused, so
     * neither can read the schema as a login that did not authenticate.
     */
    static String login(ConnectorSession session)
    {
        ConnectorIdentity identity = session.getIdentity();
        Optional<Principal> principal = identity.getPrincipal();
        if (principal.isEmpty() || !identity.getUser().equals(principal.get().getName())) {
            throw new TrinoException(PERMISSION_DENIED, "Access Denied: The posthog schema can be read only as the login that authenticated");
        }
        return identity.getUser();
    }

    @Override
    public void close()
    {
        mountClient.close();
    }
}
