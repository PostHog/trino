# Hoglake connector

The Hoglake connector provides read-only access to a Hoglake catalog. It obtains
metadata and file lists from the Hoglake REST API and reads Parquet files directly
from S3-compatible object storage. Hoglake namespaces appear as Trino schemas.

## Configuration

Create `etc/catalog/hoglake.properties`:

```properties
connector.name=hoglake
hoglake.uri=http://localhost:8080
hoglake.catalog=lake
hoglake.s3.region=us-east-1
```

`hoglake.uri` is the REST base URI, without a `/v1` suffix. The endpoint must be
reachable from Trino. Every worker needs access to the object storage containing
the catalog's files.

| Property | Description | Default |
| --- | --- | --- |
| `hoglake.uri` | Hoglake REST base URI; required. | None |
| `hoglake.catalog` | Hoglake catalog to expose. | `hoglake` |
| `hoglake.client.request-timeout` | Positive request timeout, with `ms`, `s`, `m`, `h`, or `d` suffix. | `2m` |
| `hoglake.s3.endpoint` | Optional S3-compatible endpoint. | AWS endpoint resolution |
| `hoglake.s3.region` | S3 region. | `us-east-1` |
| `hoglake.s3.access-key` | Optional S3 access key. | AWS default credential provider chain |
| `hoglake.s3.secret-key` | Optional S3 secret key. | AWS default credential provider chain |
| `hoglake.s3.path-style` | Enable S3 path-style access. | `false` |

Use Trino's [secrets support](/security/secrets) for explicit credentials. Workers use the
configured credentials or the AWS default credential provider chain; the connector
does not obtain temporary credentials from Hoglake.

## Types

| Hoglake | Trino |
| --- | --- |
| `boolean` | `BOOLEAN` |
| `int`, `long` | `INTEGER`, `BIGINT` |
| `float`, `double` | `REAL`, `DOUBLE` |
| `string`, `binary` | `VARCHAR`, `VARBINARY` |
| `date`, `time` | `DATE`, `TIME(6)` |
| `timestamp`, `timestamptz` | `TIMESTAMP(6)`, `TIMESTAMP(6) WITH TIME ZONE` |
| `uuid` | `UUID` |
| `decimal(p,s)` | `DECIMAL(p,s)` |

## Read consistency and limitations

Each table handle pins the catalog snapshot and resolved columns during planning.
File planning reads that snapshot. If it expires during a query, the query fails
with `HOGLAKE_SNAPSHOT_EXPIRED`; retry the query to plan against a retained snapshot.

Parquet columns bind by field ID. Fields without IDs fall back to column names.
Columns absent from older files produce nulls.

The connector does not support writes, DDL, time-travel SQL, nested types, or
predicate pushdown. It creates one split per file. Queries encountering deletion
vectors fail during split planning because applying row-level deletes is not yet
supported.

## Development

The connector builds and ships with this Trino fork, using the same SPI, Parquet,
and filesystem versions as the engine. Do not install a plugin built for a different
Trino version.

Build from the repository root:

```shell
./mvnw -pl plugin/trino-hoglake -am package -DskipTests
```

The plugin ZIP is produced under `plugin/trino-hoglake/target/` and is included in
the server distribution as `plugin/hoglake`. Standalone connector regression tests
live in this module. Tests that start the Hoglake server belong with that server
and should consume the matching Trino distribution.
