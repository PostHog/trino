# PostHog catalog store

## Catalog identity authorization

`catalog.required-identity-group` binds a catalog instance to a canonical owner.
The engine removes this property before creating the connector.
An empty binding is invalid.

The reserved format is `external_identity_<name-hash>_<owner-hash>`.
Each hash contains 32 lowercase hexadecimal characters.
The name hash contains the first 16 bytes of SHA-256 over the UTF-8 catalog name.
The owner hash identifies the canonical owner.
The publisher must include the canonical owner ID in that hash.
The engine rejects malformed or mismatched reserved bindings.

The identity must hold the required group for the actual instance retained by its transaction.
The current authorization policy must also permit the operation.
Replacing the visible name does not authorize another owner's retained instance.
A newly bound name also rejects a retained unbound instance.
A trusted identity's valid name claim rejects any unbound instance with that name.
This check also applies after removal or ordinary name reuse.

Credential rotation under the same canonical owner keeps the same identity group.

Unbound catalogs retain their existing behavior without a matching name claim.
An unrelated external name claim does not block ordinary authorization.
Client headers or extra credentials cannot supply trusted name claims.
The group provider supplies the identity groups.
Revoke the name claim together with its generation grant in one group snapshot.

The existing `/v1/catalog/sync` response reports `catalogIdentityVersion: 1`.
It reports each active instance's name, content version, connector, binding, and store-publication status.
Failed placeholders do not represent active instances.
`catalogStoreIdentity` reports the managed store's configured cell identity.
These fields contain no connector credentials.
The synchronization revision alone does not prove that a particular instance matches a desired binding.

Deploy this support to every coordinator before publishing bound catalogs.
Include draining coordinators that can receive statements in existing transactions.
Require matching live receipts before granting a newly bound name.
Validate future admissions against the current bound definitions.
A failed application must not authorize a process-only catalog with the same name.
The engine refuses publication over an actual process-only catalog.

Delete existing external definitions with the old publisher before its cutover.
Keep stored secrets and source credentials.
Recreate the definitions after deployment.
Old prefixed catalogs are not automatically renamed or removed.

## Catalogs that cannot start

A published catalog can fail to start on a coordinator.
For example, its properties can refer to a secret that does not exist.
The coordinator then applies the other catalogs of the revision, including removals.
The applied revision advances, and `failedCatalogInstances` names each catalog that failed.
Each entry has the `name`, `version` and `connector` of the published definition.
The list is sorted by name, and `failedCatalogs` is its size.

A catalog that failed keeps its last working version on that coordinator, if it had one.
That version stays in `catalogInstances`.
The new version that failed is in `failedCatalogInstances`.
While the list is not empty, `ready` is false and `lastFailure` is `CATALOGS_NOT_APPLIED`.
The coordinator retries the failed catalogs until they start or are no longer published.

If every catalog of a revision fails, the coordinator does not apply the revision.
It keeps its catalogs and removes nothing.
This failure points to the revision or to the coordinator, not to one definition.
If a removal fails, the coordinator does not apply the revision either.

The applied revision alone does not prove that a specific catalog definition is active.
A controller must compare `catalogInstances` and `failedCatalogInstances` with the definition it published.

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
