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
s3.region=us-east-1
```

`hoglake.uri` is the REST base URI, without a `/v1` suffix. The endpoint must be
reachable from Trino. Every worker needs access to the object storage containing
the catalog's files.

| Property | Description | Default |
| --- | --- | --- |
| `hoglake.uri` | Hoglake REST base URI; required. | None |
| `hoglake.catalog` | Hoglake catalog to expose. | `hoglake` |
| `hoglake.client.request-timeout` | Positive request timeout, with `ms`, `s`, `m`, `h`, or `d` suffix. | `2m` |
| `fs.s3.enabled` | Enable the native S3 filesystem. | `true` |
| `s3.endpoint` | Optional S3-compatible endpoint. | AWS endpoint resolution |
| `s3.region` | S3 region. | `us-east-1` |
| `s3.aws-access-key` | Optional S3 access key. | AWS default credential provider chain |
| `s3.aws-secret-key` | Optional S3 secret key. | AWS default credential provider chain |
| `s3.path-style-access` | Enable S3 path-style access. | `false` |
| `fs.cache.enabled` | Enable filesystem data caching. | `false` |

The connector uses Trino's shared [S3 filesystem configuration](/object-storage/file-system-s3).
Use Trino's [secrets support](/security/secrets) for explicit credentials. Workers use the
configured credentials or the AWS default credential provider chain; the connector
does not obtain temporary credentials from Hoglake.

Existing catalogs can continue using these aliases:

| Existing property | Standard property |
| --- | --- |
| `hoglake.s3.endpoint` | `s3.endpoint` |
| `hoglake.s3.region` | `s3.region` |
| `hoglake.s3.access-key` | `s3.aws-access-key` |
| `hoglake.s3.secret-key` | `s3.aws-secret-key` |
| `hoglake.s3.path-style` | `s3.path-style-access` |

Equivalent settings under both names are accepted; conflicting settings fail catalog
initialization. Blank legacy endpoint and credential values retain their default
behavior. Unrecognized properties and invalid configuration now fail catalog
initialization, including properties that older versions silently ignored.

## Filesystem caching

Set `fs.cache.enabled=true` in the catalog to cache Parquet reads through Trino's
shared [filesystem cache](/object-storage/file-system-cache). Omitted or `false`
leaves data caching disabled.

Every node, including the coordinator, must load a cache manager capable of caching
table data. Configure the `alluxio` manager through `cache-manager.config-files`
in each node's `etc/config.properties`, following the filesystem cache documentation.
Cache directories must exist and be writable on every node, with capacity limits
configured. Directory, capacity, page-size, and TTL properties belong in the cache
manager properties file, not the Hoglake catalog. The default `memory` manager alone
is insufficient; enabling data caching without a suitable manager fails catalog
initialization.

The engine manages cache resources and scopes entries by Trino catalog and cache
usage. Hoglake uses the default file keys: location, last-modified time, and length.
Warm reads can still issue S3 HEAD requests to determine file identity. These keys
are not Hoglake snapshot identifiers or per-user cache partitions. Published data
files should remain immutable; changing content while retaining the same location,
modification time, and length cannot be detected by this key scheme.

This caches filesystem data only, not Hoglake REST metadata. Split scheduling is
unchanged: there is no preference for workers already holding a file in cache.
Per-catalog cache metrics and tracing are provided by the shared cache infrastructure.

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

## Row-level deletes

Hoglake pairs a data file with its live deletion vector at the snapshot being
read. When a file has one, the connector reads the vector and drops the rows it
marks deleted, so deleted rows contribute to no result, aggregate, filter, or
join. The vector a query applies is the one the scan reported for the query's
pinned snapshot, never a newer vector fetched during execution.

Deletion vectors are puffin containers holding a single uncompressed
`deletion-vector-v1` blob, the encoding Hoglake records as `puffin-dv`. The
positions in the vector are file-relative row numbers, so the mask stays correct
across projected columns, page and batch boundaries, and pruned row groups.
Vectors are read through the same filesystem configuration, authentication, and
caching as Parquet data.

The encoding mixes byte order the way Iceberg's `deletion-vector-v1` does: the
blob's declared length and checksum are big-endian, while the roaring bitmap's
own fields are little-endian. Hoglake's server reader, its writer, and its DuckDB
client all use this layout, and the connector reads it.

An unfiltered `count(*)` still answers from catalog metadata: a file's visible
rows are its record count minus its deletion vector's delete count. The vector
is read and validated in that path too, so a count cannot silently ignore
deletes. Counts with a filter, and every other aggregation, read the data and
apply the vector.

A deletion vector that is missing, corrupt, truncated, unsupported, or
inconsistent with the catalog fails the query; it is never treated as "no
deleted rows". Validated conditions include the container and blob structure,
the blob checksum, the blob's declared length, the vector's bitmap encoding, the
blob's `referenced-data-file` when the writer set one, that the catalog's delete
count equals the vector's cardinality, and that every deleted position lies
inside the Parquet file's own row count. Deletion vectors larger than 256 MB,
compressed puffin footers, and bucket counts larger than the vector that declares
them can hold are refused.

## Read consistency and limitations

Each table handle pins the catalog snapshot and resolved columns during planning.
File planning reads that snapshot. If it expires during a query, the query fails
with `HOGLAKE_SNAPSHOT_EXPIRED`; retry the query to plan against a retained snapshot.

Parquet columns bind by field ID. Fields without IDs fall back to column names.
Columns absent from older files produce nulls.

Query predicates are used to prune Parquet row groups using compatible statistics.
Trino retains the residual filters to evaluate matching rows. Missing or unusable
statistics do not exclude data. UUID bounds are not used because their ordering
differs from Parquet's binary ordering.

The connector creates one split per file. Catalog file pruning is not available:
the Hoglake scan API exposes statistics state but no per-file column bounds.
The connector does not support writes, DDL, time-travel SQL, or nested types.
Only Hoglake's `puffin-dv` deletion-vector format is read; any other format
fails the query. Equality deletes and any other row-level delete representation
are not read.

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
