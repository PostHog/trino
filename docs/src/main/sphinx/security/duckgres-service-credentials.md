# Duckgres service credentials

The `duckgres-service-credential` password authenticator validates short-lived
organization service grants with a Duckgres control plane. Persistent user
passwords remain on a separate authenticator.

## Configuration

Configure the coordinator to load both authenticators:

```properties
http-server.authentication.type=PASSWORD
password-authenticator.config-files=etc/service-credentials.properties,etc/password-authenticator.properties
```

In `etc/service-credentials.properties`:

```properties
password-authenticator.name=duckgres-service-credential
duckgres-service-credential.endpoint=https://auth.example.com/auth/trino/service-credentials
duckgres-service-credential.token-file=/etc/trino/secrets/service-auth-token
```

The token is a dedicated credential for this coordinator's cell and validation
endpoint. Do not share it with another cell, a provisioning service, or a tenant.
The control plane maps each token to one immutable configured cell ID and verifies
that the organization currently belongs to that cell. No cell ID is sent in the
authentication request body.

Mount only this cell's token file on its coordinators. The control plane receives
a separate JSON configuration mapping cell IDs to current and previous tokens;
never mount that complete map on a coordinator. This plugin reads its plain-text
file for every request and sends the first nonempty line as the bearer token.
To rotate without an outage, add the new token to the cell's control-plane entry
and restart the control-plane replicas first, then update this cell's coordinator
file to put the new token first, and finally remove the previous token from both.

| Property | Default | Description |
| --- | --- | --- |
| `duckgres-service-credential.endpoint` | Required | Full authentication endpoint URL. User information, query parameters, and fragments are forbidden. |
| `duckgres-service-credential.token-file` | Required | Mounted file containing the dedicated bearer token. |
| `duckgres-service-credential.allow-insecure-http` | `false` | Permit HTTP on an explicitly trusted private route. Use network policy to restrict that route. |
| `duckgres-service-credential.http-client.connect-timeout` | `2s` | Connection timeout. |
| `duckgres-service-credential.http-client.request-timeout` | `5s` | Whole authentication request timeout. |
| `duckgres.service-credential.auth-metadata-enabled` | `false` | Request negotiated backend metadata and V2 readiness. |

HTTPS uses normal certificate verification. Redirects are not followed. The HTTP
client does not inherit environment proxy settings. Authentication responses are
limited to 64 KiB. Credentials and upstream error bodies are never included in
authentication exceptions.

Configure tenant host qualification so a client logging in as `svc_<24 hex digits>`
on its tenant host authenticates as `<database-name>.svc_<24 hex digits>`. The
control plane must verify the grant belongs to that exact organization and is live,
not revoked, and enabled for Trino. It returns the exact qualified identity, future
`expires_at` timestamp, and the organization's `org_` and `tier_` groups. No other
groups or mismatched identity are accepted.

In **every** file authenticator loaded alongside this plugin, also set:

```properties
file.reserved-user-regex=(?:[^.]+[.])?svc_[0-9a-f]{24}
```

This prevents a denied or expired service grant from falling through to a stale
persistent password entry. Other authenticators must likewise reject this exact
service-grant pattern. Persistent logins such as `svc_reporter` remain supported. The control plane must not project service grants into password
or group files.

## Coordinator readiness

The authenticator reports its loaded configuration through the security revisions
in `/v1/catalog/sync`. Each report makes an authenticated `GET` to the same
configured validation endpoint, using the current token file. The control plane
must authenticate the bearer token using the same cell binding as credential
validation and return `{"cell_id":"registered:cell-a"}`. This check does not mint
or validate a tenant grant.

The revision is `service-auth-v1:sha256:` followed by the lowercase SHA-256 hex
digest of the UTF-8 string
`duckgres-service-credential-v1\n<endpoint>\n<cell_id>`, where the endpoint is the
configured URI in ASCII form. It binds the reachable validation endpoint to the
authenticated cell without exposing the token or a token fingerprint. Token
rotation within the same cell does not change the revision, but every report
still validates the current token. An unavailable endpoint, rejected token,
redirect, or malformed response makes the component unavailable rather than
returning a cached revision. The same HTTP timeouts and response size limit used
for credential validation apply.

The pool controller must compare this revision separately from persistent
file-password, group, and access-control revisions. Deploy the authenticated
readiness endpoint and controller support before enabling this reporting version
on coordinators. A successful readiness report establishes callback configuration
and cell authentication; it does not establish that a particular service grant
is valid or authorized to query data.

## Expiry and availability

There is no Trino authentication-result cache. Every HTTP request, including query
polling and cancellation, revalidates the grant. Control plane unavailability fails
closed. Clients must renew before expiry by sending `rotate_secret: false` to the
control plane's service-credential refresh endpoint. Renewal preserves both the
grant identity and secret because gateways can bind query ownership to the complete
credential. Require `secret_rotated: false` in the response and retain the in-memory
secret; an older control plane might ignore the request field and rotate it.

Default pgwire refresh rotates the secret and is not supported for an active Trino
query. Changing either the grant identity or secret mid-query can prevent polling
and cancellation. Expired or revoked grants cannot be renewed; fail the client
operation and use an authorized operator connection if the query needs cancellation.

Deploy the control plane validation endpoint and token first, then enable this
plugin and the file reservation, and only then switch internal callers. External
users continue using their existing persistent credentials.

## Negotiated authentication metadata

The default-off implementation remains under review.
Its contract authorizes no deployment or per-login traffic.
The `duckgres.service-credential.auth-metadata-enabled` property defaults to `false`.
It does not enable {doc}`caller-token acquisition <caller-tokens>`.
Existing connection properties retain their `duckgres-service-credential.` prefix.

With metadata enabled, the authenticator sends this header on the existing callback:

```text
Accept: application/vnd.posthog.trufflehog-auth.v2+json
```

The request JSON and dedicated cell authentication remain unchanged.
Non-negotiated requests retain the current V1 response.
The server requires separate publication support before it returns V2.
Its V2 response uses the negotiated content type.

A successful V2 POST adds `backend` to the existing response.
Its recognized values are `hoglake` and `ducklake`.
The control plane derives backend from the proven warehouse/cell assignment.
It does not accept backend from the request.
Ordinary group validation remains unchanged.
The callback never supplies operator evidence.

Missing, unknown, malformed, or conflicting backend metadata supplies UNKNOWN.
Enabled managed acquisition treats UNKNOWN as terminal.
UNKNOWN never selects the legacy DuckLake path.
An issuer refusal never repairs missing classification.
The typed Principal carries classification locally until core consumes it.
It adds no classification field to worker messages, plans, or RPC.

### V2 readiness

A successful negotiated GET returns `cell_id` and integer `auth_metadata_version: 2`.
The authenticator requires the V2 content type.
It reports a V2 fingerprint only after that capability check succeeds.
A V1 response never proves V2 capability.

The fingerprint is `service-auth-v2:sha256:` followed by lowercase SHA-256 hex.
Its UTF-8 input is:

```text
duckgres-service-credential-v2\n<endpoint>\n<cell-id>\nauth_metadata_version=2
```

The input has no trailing newline.
The endpoint uses the configured URI's ASCII representation.
The cell comes from the authenticated readiness response.
The controller computes the same expected value.
It must require V2 when the configured reader requires V2 metadata.
V1 responses and V1 fingerprint computation remain unchanged.

File publication, callback publication, reader modes, receipt enforcement, and acquisition remain independent.
Reader support must precede approved publication.
The existing admission barrier remains mandatory.
No setting here authorizes a rollout or backend migration.
