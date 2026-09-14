# Shared catalog-store tests

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
