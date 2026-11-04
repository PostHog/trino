# PostHog external tables

`connector.name=posthog_external` reads Delta Lake tables in place from a
tenant's own bucket. It is the Delta Lake connector with two changes:

- The metastore is a REST service in the PostHog control plane. Schemas and
  table registrations are stored there. Nothing is written to the bucket.
- The data is read-only. The catalog allows only `CREATE SCHEMA`,
  `DROP SCHEMA`, `DROP TABLE`, `system.register_table` and
  `system.unregister_table`. Tables are external, so `DROP TABLE` and
  `unregister_table` remove only the registration and never delete files.

Tenants do not write the catalog file. They run `CREATE CATALOG`, the
coordinator forwards it to the control plane (see the catalog store's
[delegated catalog requests](../trino-posthog-catalogstore/README.md#delegated-catalog-requests)),
and the control plane publishes the catalog with the properties below.

```sql
CREATE CATALOG lake USING posthog_external WITH (
  "s3.aws-access-key" = '...',
  "s3.aws-secret-key" = '...',
  "s3.region" = 'eu-central-1'
);
CREATE SCHEMA lake.billing;
CALL lake.system.register_table(
  schema_name => 'billing',
  table_name => 'invoices',
  table_location => 's3://their-bucket/delta/invoices');
SELECT * FROM lake.billing.invoices;
CALL lake.system.unregister_table(schema_name => 'billing', table_name => 'invoices');
```

Schema and table names are free choices. Only `table_location` ties a
registration to data.

## Catalog properties

| Property | Description |
| --- | --- |
| `posthog-external.metastore-uri` | Base URL of the metastore service. |
| `posthog-external.metastore-token-file` | File holding the bearer token, re-read on every request. |
| `posthog-external.catalog-id` | Opaque id of this catalog in the metastore, sent on every call. |

Every other property goes to the Delta Lake connector unchanged, for example
`fs.native-s3.enabled`, `s3.region`, `s3.endpoint` and the S3 keys. The
connector always sets `delta.security=system` and refuses another value. It
defaults `delta.register-table-procedure.enabled` to `true` and both metastore
cache TTLs to `0s`, so a registration made on one coordinator is visible on
the next query everywhere. An unknown `posthog-external.*` property fails the
catalog.

## Metastore API

Base: `{metastore-uri}/trino/external-metastore/v1/catalogs/{catalog-id}`,
with `Authorization: Bearer <token>` and `X-Trino-User: <session user>`.

| Method | Path | Result |
| --- | --- | --- |
| `GET` | `/schemas` | `{"schemas": [...]}` |
| `GET`, `PUT`, `DELETE` | `/schemas/{schema}` | `DELETE` returns 409 `not_empty` while tables remain |
| `GET` | `/schemas/{schema}/tables` | `{"tables": [...]}` |
| `GET`, `PUT`, `DELETE` | `/schemas/{schema}/tables/{table}` | Table body: `name`, `format`, `location`, `columns`, `properties` |

Errors are `{"error": "<code>", "message": "<text>"}`, with codes
`not_found`, `already_exists`, `not_empty`, `invalid`, `limit_exceeded` and
`unauthorized`. `FakePostHogMetastore` in the tests implements the same API.

## Run it locally end to end

This runs the published image against the real control-plane metastore,
MinIO, and the Delta test table from `trino-delta-lake`. Every key is a
throwaway MinIO user.

1. Build the image with `core/docker/build.sh`
   (see [core/docker/README.md](../../core/docker/README.md)).
2. Start the metastore. The control plane repository has a harness in
   `hack/e2e-external-metastore` that serves the catalog-request and metastore
   routes on a throwaway Postgres database, seeds one ready warehouse whose
   root login is `e2edb`, and writes the cell token to its `-state` directory:

   ```sh
   go run ./hack/e2e-external-metastore -dsn "$DSN" -state "$STATE"
   ```

3. Start MinIO with two buckets and two read-only users. `tenant-bucket` holds
   the Delta table, `other-bucket` is empty:

   ```sh
   docker run -d --name e2e-minio -p 19900:9000 -p 19901:9001 \
     -e MINIO_ROOT_USER=e2eroot -e MINIO_ROOT_PASSWORD=e2eroot-throwaway-pw \
     minio/minio server /data --console-address :9001
   docker exec e2e-minio mc alias set root http://127.0.0.1:9000 e2eroot e2eroot-throwaway-pw
   docker exec e2e-minio mc mb root/tenant-bucket root/other-bucket
   docker cp plugin/trino-delta-lake/src/test/resources/databricks73/person e2e-minio:/tmp/person
   docker exec e2e-minio mc cp -r /tmp/person/ root/tenant-bucket/delta/person/
   ```

   Create a `GetBucketLocation`/`ListBucket`/`GetObject` policy per bucket and
   attach them to `e2ereader` (tenant-bucket) and `e2eoutsider` (other-bucket).

4. Create the catalogs through the control plane, as the coordinator would
   forward `CREATE CATALOG`. `user` is the Trino login that ran the statement:

   ```sh
   curl -H "Authorization: Bearer $(cat "$STATE/token")" -H 'Content-Type: application/json' \
     localhost:18080/trino/catalog-requests/v1/create -d '{
       "user": "e2edb", "catalog": "lake", "connector": "posthog_external",
       "properties": {"s3.aws-access-key": "e2ereader", "s3.aws-secret-key": "...",
                      "s3.region": "us-east-1", "s3.path-style-access": "true"}}'
   ```

   Repeat with `lake_denied` and the `e2eoutsider` keys.

5. Write the published properties to static catalog files. Locally there is
   no catalog store, so the control plane's `TestDumpPostHogExternalCatalogProperties`
   (build tag `e2e`) renders them with the real keys in place of the secret
   references, `s3.endpoint=http://host.docker.internal:19900`, and an empty
   `catalog.required-identity-group`, which a static catalog file rejects.

6. Start Trino with those files and the token:

   ```sh
   docker run -d --name e2e-trino -p 18090:8080 --add-host host.docker.internal:host-gateway \
     -e CATALOG_MANAGEMENT=static \
     -v "$CATALOGS/lake.properties:/etc/trino/catalog/lake.properties:ro" \
     -v "$CATALOGS/lake_denied.properties:/etc/trino/catalog/lake_denied.properties:ro" \
     -v "$STATE_TOKEN_DIR:/etc/trino/service-auth:ro" trino-posthog-local:<version>
   docker exec -it e2e-trino trino --user e2edb
   ```

What to check:

- `register_table` on `lake` and `SELECT count(*)` returns 12 rows.
- `INSERT`, `CREATE TABLE` and `CREATE TABLE AS` are refused.
- `unregister_table` leaves every file in the bucket.
- The same registration on `lake_denied` fails with S3 403.
- A wrong token in the token file fails every call with 401.
- The registrations are rows in the control plane's metastore tables.
