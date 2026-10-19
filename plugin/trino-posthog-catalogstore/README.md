# PostHog catalog store

## Delegated catalog requests

A read-only (`catalog-store.read-only=true`) coordinator rejects `CREATE CATALOG`
and `DROP CATALOG`, because an external writer publishes its catalogs. With
`catalog-store.delegate.endpoint` set, it forwards those statements instead, on
behalf of the user who ran them, to the control plane that publishes the
catalogs. The statement returns once the published change has arrived through
catalog synchronization and the user can see the new catalog.

| Property | Default | Description |
| --- | --- | --- |
| `catalog-store.delegate.endpoint` | (unset) | Base URL; requests go to `{endpoint}/create` and `{endpoint}/drop`. Unset keeps mutations rejected. |
| `catalog-store.delegate.token-file` | (required with an endpoint) | File holding the bearer token, re-read on every request. |
| `catalog-store.delegate.allow-insecure-http` | `false` | Allow `http` on a trusted private route; otherwise `https` is required. |
| `catalog-store.delegate.request-timeout` | `15s` | Budget for one request to the control plane. |
| `catalog-store.delegate.visibility-timeout` | `3m` | How long an accepted request waits for the change to arrive on this coordinator. |

The writable store refuses delegation at startup. Redirects are not followed,
responses are bounded to 64 KiB, and request bodies never appear in errors or
logs. The query text Trino records for every `CREATE CATALOG` masks all
property values as `'***'`.

## Shared catalog-store tests

`TestPostHogCatalogStoreDurability` exercises real coordinators against PostgreSQL.
It covers startup reuse, absent live synchronization, stale drops, conflicting
properties, and failed catalogs that remain visible by name. These characterize
the store's boundaries; they do not make concurrent catalog writers safe.

The cancellation tests hold an isolated PostgreSQL table lock while a real
coordinator executes `CREATE CATALOG` or `DROP CATALOG`. They verify that the
client can receive `FAILED` with `USER_CANCELED` before the blocked write completes.
Releasing the lock then changes the persisted catalog. A failed or cancelled DDL
response is therefore not proof that all catalog writes have stopped. An external
rollout controller must retain an ambiguous mutation until recovery establishes
that no delayed writer can remain.

The default database fixture uses Testcontainers. Without Docker, an explicitly
configured disposable local PostgreSQL server can be used:

```sh
./mvnw -pl plugin/trino-posthog-catalogstore test \
  -DbranchScopedLocalRepo.enabled=true \
  -Dtest=TestPostHogCatalogStoreDurability,TestTestingCatalogStoreDatabase \
  -Dcatalogstore.test.jdbc-url=jdbc:postgresql://localhost:5432/catalogs \
  -Dcatalogstore.test.jdbc-user=test
```

Build the matching reactor dependencies first, as described in
[DEVELOPMENT.md](../../.github/DEVELOPMENT.md). Use branch-scoped local artifacts
when testing multiple worktrees.

The optional test password property is `catalogstore.test.jdbc-password` and
defaults to the synthetic value `test`. Use a disposable local test database,
never production credentials or a production tunnel. The fixture accepts only
loopback URLs without options or embedded credentials. It creates a random schema
per fixture and drops only that schema during cleanup. Testcontainers remains
the default when `catalogstore.test.jdbc-url` is absent.
