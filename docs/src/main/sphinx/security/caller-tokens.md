# Managed caller tokens

The Hogtower classification contract has review approval.
The default-off implementation remains under review.
Runtime acceptance remains pending.
This documentation authorizes no deployment, per-login traffic, or backend migration.

This feature covers SQL Basic authentication.
Web UI form sessions do not acquire caller JWTs through this path.
Shared-catalog activation requires a separate review of those sessions.

Each selected Basic-authentication request obtains a fresh token, unless a reuse period is set.
A SQL client authenticates once for each statement it submits.
Include that request rate in the bcrypt capacity review.
A later request does not refresh an existing worker session.

## Configuration

Configure acquisition on the coordinator in `etc/config.properties`.
The properties use the `http-server.authentication.password.caller-token.` prefix.

| Property | Default | Description |
| --- | --- | --- |
| `http-server.authentication.password.caller-token.enabled` | `false` | Acquire a token after managed password authentication. |
| `http-server.authentication.password.caller-token.issuer-uri` | Unset | Exact configured endpoint for the caller grant. Required only when acquisition is enabled. |
| `http-server.authentication.password.caller-token.cell-token-file` | Unset | Readable regular file containing the dedicated cell bearer credential. Required only when acquisition is enabled. |
| `http-server.authentication.password.caller-token.timeout` | `5s` | Request timeout. Inclusive bounds: `1ms` through `30s`. |
| `http-server.authentication.password.caller-token.max-response-size` | `64KiB` | Response limit. Inclusive bounds: `1B` through `1MiB`. |
| `http-server.authentication.password.caller-token.allow-insecure-http-for-development` | `false` | Permit HTTP only for explicit development or test use. |
| `http-server.authentication.password.caller-token.reuse-period` | `0s` | Time a token is reused for the same login and password. `0s` acquires a token for every request. Inclusive bounds: `0s` through `5m`. |

The issuer URI must be absolute.
It requires a host without user information, query parameters, or a fragment.
HTTPS is the default requirement.
The development exception never permits a production HTTP fallback.
The client follows no redirects.

The client reads the cell file for every acquisition.
It limits the file to `16KiB`.
The file contains the dedicated bearer credential, not a mapping for other cells.
The client limits the encoded grant form to `16KiB`.
That limit matches the issuer.

The client uses the existing internal issuer endpoint.
The form contains only `grant_type`, `username`, and `password`.
The password supplies request-scoped caller evidence.

The dedicated client defaults to HTTP/1.
It sends `Accept-Encoding: identity`.
It disables request logging by default.
It never replays password evidence.
It adds no password-retention state.
Without a reuse period, it keeps no token.

The response requires `access_token`, `token_type`, and `expires_in` in one JSON object.
The client rejects unknown fields, duplicate keys, and trailing data.
It requires a Bearer token plus a positive integer `expires_in`.
It treats the token as opaque.
It never parses JWT claims.
It preserves the exact token bytes.

## Token reuse

With a reuse period, the coordinator keeps each granted token in memory and returns it to later requests that present the same login and password.
Reuse follows the same successful password authentication and user mapping as acquisition.
A request that fails either never reaches a kept token.

The client finds a kept token by the verified login and an HMAC-SHA256 of the login and password.
The key has 32 random bytes, is made at startup, and never leaves the client.
An entry holds the token and the time its reuse ends.
It holds no password.

A token is reused until its age reaches the reuse period.
Reuse also ends at half of the `expires_in` value the issuer returned.
The client reads that value from the response and still does not parse the token.
Concurrent requests for the same login and password share one grant request.
Only a successful grant is kept.
The client keeps at most 10,000 tokens and shares them with no other node.

The issuer checks the login when it grants the token, not for each reusing request.
A change at the issuer reaches new statements at most one reuse period later.
A reused token has less time left, so a statement can count on the token lifetime less the reuse period.
A changed password is refused by password authentication once the coordinator has the change.
Until the password file is reloaded, the old password can still start statements on a kept token.
Without reuse, the issuer refuses it in that window.

## Authenticated selection

Classification comes from the successful password authenticator.
The {doc}`file reader <password-file>` uses credentials and metadata from one captured password store.
The {doc}`service authenticator <duckgres-service-credentials>` uses negotiated callback metadata.
A narrow typed Principal supplies local evidence.
A plain Principal supplies UNKNOWN.

| Evidence | Acquisition behavior |
| --- | --- |
| Explicit Hoglake | Require a successful caller grant. |
| Explicit DuckLake | Preserve the legacy path with zero issuer calls. |
| Authenticated ops evidence | Preserve the operational exemption without a tenant JWT. |
| UNKNOWN or conflicting evidence | Terminate authentication without fallback. |

Backend selection never comes from a client header, hostname, catalog, or issuer refusal.
Existing hostname qualification selects a credential namespace only.
The password still establishes access.
Root follows the ordinary tenant rules.
Service scope ceilings remain unchanged.

Acquisition follows successful authentication and trusted user mapping.
The mapped tenant retains the exact qualified database prefix.
An unqualified mapped tenant fails that binding.
A cross-warehouse mapping fails.
The issuer receives the verified qualified login rather than its mapped alias.
These checks do not change mapping while acquisition remains disabled.

The ops exemption requires the exact authenticated operational name plus trusted operator evidence.
The mapped name must also remain `__trino_ops`.
A mapped tenant alias never establishes the exemption.
Existing OPA authorization remains unchanged.

Selected-path issuance failures terminate authentication.
Neither authentication loop tries another authenticator.
Timeouts, disabled issuance, malformed responses, and `invalid_grant` never select legacy fallback.
Failures expose no passwords, hashes, tokens, or issuer response bodies.

## Session boundary and activation

Core consumes classification locally.
It replaces the typed Principal with its ordinary principal name.
Classification adds no backend groups or worker, plan, or RPC fields.
Core attaches only the acquired JWT under `internal$hoglake_session_jwt`.
That credential belongs to the mapped authenticated identity.

The existing transport rejects client injection of `internal$` credentials.
It removes this JWT when the effective or original user differs from the authenticated identity.
It preserves ordinary credentials.
Credential transport does not establish connector readiness.
This implementation adds no AWS session, refresh, or connector activation.

The file reader, service reader, and acquisition setting independently default to `false`.
Hogtower separately controls file publication, callback publication, and V2 receipt enforcement.
None of those capabilities enables another.
Old binaries reject metadata artifacts.
Reader-first publication requires the existing admission barrier.
V1 readiness never substitutes for V2 capability.

Contract approval does not enable the issuer grant.
The H1 failure-bucket gate remains mandatory.
The H2 bcrypt-budget gate remains mandatory.
Every enabled acquisition path requires password-retention evidence.
A file-cache repair does not remediate LDAP or Salesforce caches.

Existing username-only issuance requires a reviewed stop-and-drain plan before verified-origin reliance.
The drain covers the maximum remaining token lifetime plus verifier skew.
This implementation authorizes no legacy shutdown.
Shared-catalog consumption remains M2c work.
The AWS session profile remains M4 work.
