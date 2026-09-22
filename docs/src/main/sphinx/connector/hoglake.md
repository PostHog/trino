# Hoglake connector

The Hoglake connector provides read and append access to a Hoglake catalog. It obtains
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
inside the row-count bound. Scans use the Parquet footer's physical row count;
metadata-only counts use the catalog's record count and do not independently
read the Parquet footer.

Deletion vectors larger than 256 MiB, compressed puffin footers, and bucket
counts larger than the vector that declares them can hold are refused. Footer
JSON is limited to 64 KiB and 16 levels of nesting. The streaming footer parser
has a separate 1 MiB memory reservation.

The connector checks bitmap headers and payload lengths before deserialization
and reserves the input, decoded objects, backing arrays, and temporary decoder
arrays against query memory before allocating them. There is no fixed ratio
between serialized and decoded size. Retained bitmap memory remains charged
until the split closes; a metadata-only count releases it before returning.

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
The connector does not support time-travel SQL or nested types.
Only Hoglake's `puffin-dv` deletion-vector format is read; any other format
fails the query. Equality deletes and any other row-level delete representation
are not read. SQL `DELETE`, `UPDATE` and `MERGE` write compatible vectors.

## Writing tables

The connector supports `CREATE TABLE`, `INSERT INTO`, and `CREATE TABLE AS SELECT`
(CTAS) in an existing Hoglake namespace:

```sql
CREATE TABLE hoglake.analytics.measurements (id bigint, value double, label varchar);
INSERT INTO hoglake.analytics.measurements VALUES (1, 12.5, 'sample');
CREATE TABLE hoglake.analytics.measurement_copy AS
SELECT * FROM hoglake.analytics.measurements;
```

Writes use the catalog's `data_path` and the connector's filesystem credentials.
Bucket roots such as `s3://example-bucket` and paths with or without a trailing
slash are supported.
These credentials need permission to create objects and remove aborted uploads.
No separate write configuration is required. Files use Snappy-compressed Parquet
with catalog field IDs, a target size of 128 MiB, and deferred catalog statistics.
The Hoglake hydrator can populate these statistics later; rows are readable immediately.

The scalar types listed above are writable. Bounded `VARCHAR` becomes unbounded
`VARCHAR`, and temporal precisions below six become precision six. Higher temporal
precisions, `CHAR`, `SMALLINT`, `TINYINT`, and nested types are rejected on creation.
Omitted nullable columns receive nulls. `NOT NULL` constraints are enforced.

An insert registers all its files in one catalog commit. Its table UUID and read
snapshot guard against concurrent table replacement or schema changes. Concurrent
appends are allowed. Empty inserts do not create files or commits.

CREATE and CTAS require a server advertising `atomic-table-creation-v1`.
The connector prepares an unpublished operation, writes its files, then publishes
the table and all initial files in one atomic catalog transaction. The target must
still be absent at publication. No temporary table or rename is used.

Preparation returns stable field IDs and an operation-specific write path. A lost
publication response is resolved through the durable operation receipt, with one
identical retry if the operation is still prepared. If recovery remains unavailable,
the error includes the operation ID for inspection. Rollback aborts the operation;
it never drops a table. A committed operation remains committed even after later
renames or drops. The server retains terminal receipts and expires unpublished
operations after 24 hours, so longer-running creations must be retried as new queries.
There is no fallback to the old staging-table protocol on older servers.

Writes are limited to single-statement transactions and unpartitioned tables.
DELETE, UPDATE and MERGE also reject sorted tables. Query/task retries, comments
and custom table properties are not supported. INSERT's existing writer treats
sort specifications as advisory and does not apply them.

Servers advertising `idempotent-append-v1` support recovery of one INSERT
operation. The connector creates one operation ID in `beginInsert` and sends it as
`idempotency_key` with a deterministic commit payload through `/commit/prepared`,
which requires the key and prevents older replicas from silently ignoring it.
After a timeout, connection loss, or invalid success response, it checks `/commit/receipts/{operation}` and
allows at most two identical commit retries, followed by a final receipt check.
Recovery uses exponential backoff with jitter (100–200 ms, 200–400 ms, then
400–800 ms) and honors `Retry-After` on HTTP 429 and 5xx responses. After the
initial request, all recovery waits and requests share one
`hoglake.client.request-timeout` budget (two minutes by default). A hint that
exceeds the remaining budget ends recovery without sending another request.
A missing receipt does not
mean the original request stopped. If recovery remains unresolved, the error
reports an unknown outcome and the operation ID. The receipt remains valid after
later writes, rename, drop, or snapshot expiry; server receipts do not expire.

Deploy the supporting server before the connector. Servers without the capability
retain single-attempt INSERT behavior. Receipt lookup requires the same deployment
access boundary as the catalog API; the standalone server does not implement
application authentication. Recovery covers publication of one operation only:
manual SQL reruns create a new operation and can duplicate rows. Query and task
retries remain unsupported. Files handed to the coordinator are not
deleted on an ambiguous commit failure, to avoid removing committed data. Failed
writes can therefore leave unregistered objects for operator cleanup.

## Table lifecycle

`DROP TABLE`, same-schema `ALTER TABLE ... RENAME TO ...`, and `TRUNCATE TABLE`
require the server capability `guarded-table-lifecycle-v1`. Deploy the supporting
Hoglake server before the connector. Cross-schema moves are unsupported.

These operations send the table UUID captured during planning. A reused name
cannot redirect a stale statement to a replacement table. Rename preserves the
UUID, schema, files and retained history. Truncate atomically clears live data
files and deletion vectors while preserving the UUID, schema, partition/sort
specifications, properties and row-id allocation. DROP and TRUNCATE do not
physically remove storage objects; normal retention and cleanup still apply.

INSERT and TRUNCATE serialize at catalog publication. Inserts committed before
truncate are cleared; inserts planned before truncate and committed afterward
conflict. Fresh inserts can proceed. Truncate does not use drop/recreate.

Lifecycle requests have no durable operation receipt and are not automatically
retried by the connector. A timeout, malformed response or server failure may
leave the outcome unknown. Inspect table identity and snapshot history before
issuing a new statement: retrying TRUNCATE can erase intervening inserts.
Append receipt recovery, when supported by the connector, does not make
lifecycle requests idempotent. A server replay of an already committed INSERT
returns its original receipt after rename, truncate or drop, without adding data.

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
live in this module. `TestHoglakeLiveWrites` is an opt-in integration test against
an isolated Hoglake server and S3-compatible bucket. Enable it with
`-Dhoglake.test.uri=<local-server-uri>` and
`-Dhoglake.test.s3-endpoint=<local-object-store-uri>`. It uses synthetic test
credentials and creates a unique test catalog. The default bucket is
`trino-write-test`; override its URI with `hoglake.test.data-path`. Set
`hoglake.test.root-path` to a separate empty bucket URI without a trailing slash
to also run the bucket-root regression. `TestHoglakeLiveWriteFailures` uses the
same server settings to verify ambiguous responses and concurrent DDL against
the real catalog through a fault-injecting local proxy.
The normal test run uses synthetic HTTP fixtures and does not require Docker.

### Atomic table replacement

`CREATE OR REPLACE TABLE` and `CREATE OR REPLACE TABLE AS SELECT` require the
server capability `atomic-table-replacement-v1`. Deploy the server first.
Replacement supports the same definitions and types as ordinary creation,
including CTAS reading the old target. The old table stays visible until one
atomic publication replaces its definition and data with a new table UUID.

The connector guards the target identity and snapshot observed during planning.
Concurrent target changes, including INSERT, rename, drop, truncate, replacement,
and compaction, reject publication. Absent targets use normal creation semantics.
Failed preparation, writes, or publication do not remove the old table. Lost
publication responses use the existing durable creation receipt; unresolved
outcomes do not authorize deleting uploaded files.

Existing retained snapshots remain readable through Hoglake's snapshot API;
Trino time-travel syntax remains unsupported. Changefeed windows crossing a
replacement require full-snapshot reconciliation against the new incarnation.

## Schema evolution

`CREATE SCHEMA`, empty `DROP SCHEMA`, and top-level `ALTER TABLE ... ADD COLUMN`,
`RENAME COLUMN`, and `DROP COLUMN` require `guarded-schema-evolution-v1`.
Deploy the supporting Hoglake server to all replicas before the connector.
Schema properties, custom owners, and `DROP SCHEMA ... CASCADE` are unsupported.
A namespace containing tables or views cannot be dropped. Deletion sends the
namespace identity returned by the server, so concurrent name reuse conflicts.

ADD COLUMN appends a nullable column using the existing writable scalar types.
Column positions, defaults, comments, properties, and nested-field changes are
unsupported. Rename preserves the field ID. Drop retires the field ID; a later
column with the same name receives a new ID. Existing Parquet files remain
readable by field ID, with nulls for columns absent from the file. ADD and RENAME
are refused while live files lack field IDs or have pending or failed hydration; hydrate, rewrite,
or retire those files first. This prevents name binding from exposing a dropped
column's old values as a newly added column. The last column and partition/sort
source columns cannot be dropped.

Column changes carry the planned table UUID and read snapshot. Concurrent DDL,
name reuse, or replacement causes a conflict; unrelated table changes and ordinary
appends do not invalidate the DDL basis. Expired conflict history is rejected.
INSERT plans made before evolution conflict at publication; plan a new statement
against the evolved schema. A prepared replacement also conflicts if column
alteration commits first; if replacement commits first, the old UUID rejects the
alteration. These operations are serialized by the existing catalog commit lock.

SQL column type changes (`ALTER COLUMN ... SET DATA TYPE`) are explicitly
unsupported, including widening and same-type requests. The server's existing
REST promotion policy remains unchanged: signed integer widening through `long`,
unsigned widening through `uint32`, and `float` to `double`, preserving field IDs.
This slice adds no type promotions or writable types. Reading externally promoted
files remains subject to the connector's existing type and Parquet reader support.

Schema mutations have no durable receipt and are sent once. A lost response may
leave the outcome unknown; inspect the catalog before issuing another statement.
INSERT receipt recovery does not make schema mutations safe to replay.

### Row-level DELETE

`DELETE FROM table WHERE predicate` evaluates the predicate in Trino. Selected rows
carry the immutable catalog data-file ID and original Parquet file position, including
row-group offsets after pruning. Existing deleted positions are preserved. Workers
return compressed position sets, which the coordinator unions per file and publishes
as one atomic snapshot. Counts report newly deleted rows, including zero for repeated
or no-match deletes. Data files and prior deletion vectors remain available to retained
snapshots; DELETE does not change the table UUID.

### UPDATE and MERGE

`UPDATE` writes replacement rows and deletes the original physical positions.
`MERGE` supports matched UPDATE/DELETE and unmatched INSERT actions. Trino rejects
multiple source rows matching the same target row. Upserts provide SQL MERGE
semantics; they do not enforce unique keys. Unchanged columns, existing deletions
and historical snapshots are preserved. Affected-row counts count each update
once. Insert-only, delete-only, mixed and zero-row executions are supported.

Deploy the Hoglake server with `idempotent-mutation-v1` to **all replicas first**.
The connector requires that capability for DELETE, UPDATE and MERGE and uses
`/commit/mutations/prepared`. It publishes all appended files and vectors in one
snapshot, requiring a read snapshot, table UUID and operation ID. Empty delete
groups guard insert-only and zero-row statements. An older replica cannot silently
accept this contract. Any intervening target-table change conflicts, including
INSERT, DELETE, compaction, schema changes, truncate, drop/name reuse and replacement.
Unrelated tables may change. Rerun conflicting SQL from a fresh snapshot.
Partitioned and sorted mutations and additional writable types remain unsupported.

Publication recovery uses the same bounded receipt lookup and identical-request replay
as INSERT, with a full-payload mixed-mutation server contract. A missing receipt is not proof that
publication failed. After submission, the connector retains appended files and uploaded vectors even if
recovery or cancellation leaves the outcome unknown; the error reports the operation ID.
Before submission, failures leave the table unchanged. Worker abort cleans files
until fragment handoff; coordinator failure cleans newly uploaded vectors. Files
already handed off can remain orphaned. There is no orphan cleanup mechanism in
this connector.

Mutation sinks limit compressed position sets to 64 MiB per worker sink. The coordinator
separately limits aggregate fragment payloads and vector-construction working memory
to 64 MiB each. Retained fragments, decoded vectors, and encoding workspace are also
charged to query memory; either limit can reject a statement before publication.
Final vector construction runs on the coordinator;
query/task retries and multi-statement write transactions remain unsupported.
