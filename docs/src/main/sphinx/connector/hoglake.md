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
The connector does not support time-travel SQL.
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

The scalar types listed above, `TINYINT`, `SMALLINT`, `TIMESTAMP(9)`, native
`VARIANT`, and recursive `ARRAY`, `MAP`, and named `ROW` types are writable.
New type support requires `recursive-write-schema-v1`: deploy the server to all
replicas before updating the connector. Bounded `VARCHAR` becomes unbounded;
`TIME` and zoned timestamps normalize to precision six, and unzoned timestamps
normalize to six or nine without rounding. Precision above nine for unzoned
timestamps, precision above six for time/zoned timestamps, and `CHAR` are rejected.
Nanosecond timestamps must fit signed int64 nanoseconds (approximately 1677–2262);
out-of-range values fail before file registration.

Every nested catalog node retains its field ID and nullability. Map keys are
required. Omitted nullable columns receive nulls; required nested values are
checked only when their parent exists. Reads bind nested row fields by ID after
external renames, and missing fields produce nulls. A historical row with none
of its fields remaining is refused because its null-versus-present state cannot
be recovered by this reader. SQL nested field evolution remains separate from
whole-column type changes.

Existing Hoglake columns also support these lossless SQL mappings:

| Hoglake type | SQL type | Write constraint |
| --- | --- | --- |
| `uint8` | `SMALLINT` | 0 through 255 |
| `uint16` | `INTEGER` | 0 through 65535 |
| `uint32` | `BIGINT` | 0 through 4294967295 |
| `uint64` | `DECIMAL(20,0)` | 0 through 18446744073709551615 |
| `json` | `VARCHAR` | Valid JSON text, preserved without reserialization |
| `timestamp_s` | `TIMESTAMP(6)` | Whole seconds |
| `timestamp_ms` | `TIMESTAMP(6)` | Whole milliseconds |

SQL creation uses the canonical signed/string/microsecond mappings; it does not
infer unsigned or JSON catalog types from values. Unsigned 64-bit data requires
block conversion between decimal values and physical unsigned INT64, including
inside containers; this adds CPU and allocation cost. Unsigned predicates remain
residuals. Statistics remain field-ID keyed and are populated by the existing
server hydrator. Native VARIANT writes are unshredded.

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

INSERT, UPDATE and MERGE
support partitioned and sorted tables. DELETE and delete-only MERGE support both layouts. TASK and QUERY execution retries require `claimed-uploads-v1`, `idempotent-append-v1`, and `atomic-table-creation-v1`. The writer applies the live sort specification to each new file.

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
Column positions, defaults, column properties, and nested-field changes are
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

SQL column type changes (`ALTER COLUMN ... SET DATA TYPE`) use the same
UUID, snapshot, and capability guards. The supported compatibility matrix is:

| Existing SQL type | Target SQL type |
| --- | --- |
| `TINYINT` | `SMALLINT`, `INTEGER`, `BIGINT` |
| `SMALLINT` | `INTEGER`, `BIGINT` |
| `INTEGER` | `BIGINT` |
| `REAL` | `DOUBLE` |

All other changes, including narrowing, decimal precision or scale changes,
temporal precision changes, same-type requests, and container changes, are
rejected. Promotions preserve field IDs and nullability. Historical Parquet
files retain their physical types and are widened by the reader; new files use
the promoted type. Predicates on widened historical physical columns remain
residuals to avoid interpreting Bloom filters with the wrong physical width;
these files can require more scanning. The server re-encodes statistics in the
same transaction.
Unsigned catalog types are not promoted through their signed SQL aliases.
Nested field evolution is a separate operation and is not enabled by this matrix.

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
Replacement rows honor the live partition and sort specifications.

Publication recovery uses the same bounded receipt lookup and identical-request replay
as INSERT, with a full-payload mixed-mutation server contract. A missing receipt is not proof that
publication failed. After submission, the connector retains appended files and uploaded vectors even if
recovery or cancellation leaves the outcome unknown; the error reports the operation ID.
Before submission, failures leave the table unchanged. Worker abort cleans files
until fragment handoff; coordinator failure cleans newly uploaded vectors. Files
already handed off are preserved until publication or explicit upload reclamation
(see upload cleanup below). Legacy servers do not track these orphaned files.

Mutation sinks limit compressed position sets to 64 MiB per worker sink. The coordinator
separately limits aggregate fragment payloads and vector-construction working memory
to 64 MiB each. Retained fragments, decoded vectors, and encoding workspace are also
charged to query memory; either limit can reject a statement before publication.
Final vector construction runs on the coordinator;
explicit DML transactions stage the resulting vectors until commit.

### Partitioned writes

Use `WITH (partitioning = ARRAY['region', 'bucket(id, 16)', 'day(created_at)'])`
on CREATE TABLE or CTAS. Identity may also be written `identity(region)`.
Supported transforms are identity, bucket, year, month, day and hour. Sources may
be scalar columns or struct leaves using dotted paths; arrays, maps and VARIANT
cannot be sources. Truncate and hour on DATE are refused because the server's
cross-client transform contract does not define them. Bucket accepts the server's
explicit allowlist; unsigned 32/64-bit, alternate timestamp precisions, booleans,
floating-point and JSON sources are not bucketable.

Partitioned CREATE/CTAS and CREATE OR REPLACE require server capability
`atomic-partitioned-table-creation-v1`. Preparation uses a dedicated endpoint so an
old replica cannot silently ignore the spec. Publish installs the spec and all
initial files in one snapshot. Existing partitioned tables need no new server
contract for INSERT, UPDATE or MERGE. Updated rows route to their new partitions
and publish atomically with deletion of their old positions. Concurrent target
DDL conflicts with the write's pinned snapshot.

Partition strings match the Python writer: null remains JSON null, temporal
transforms floor before the epoch, timestamp identity retains six fractional
digits when nonzero, and nanosecond timestamp identity uses epoch nanoseconds.
Identity DATE and microsecond timestamps require years 1–9999 for cross-client
string compatibility. Timestamps with time zone use UTC. Partition expressions
use simple, case-sensitive column paths; quoted or punctuation-bearing names are
not supported in these expressions.

The writer groups each incoming page by partition and keeps one file open. This
bounds writer memory independently of partition cardinality. Interleaved
partitions can create smaller files; grouping input by partition keys improves
file sizes. Partitioning does not add scan pruning in this change.

### Sorted writes

Use `WITH (sorted_by = ARRAY['event_time DESC NULLS LAST', 'id ASC NULLS FIRST'])`
on CREATE TABLE or CTAS. The default is `ASC NULLS LAST`. Scalar columns and
struct-leaf paths use the same simple, case-sensitive names as partitioning.
Sort fields must be distinct, orderable scalar sources; repeated children and
VARIANT are refused. Combine `sorted_by` and `partitioning` on the same table.

Sorted creation requires server capability `atomic-sorted-table-creation-v1` and
uses a dedicated preparation endpoint that older replicas refuse. Sort and
partition specs publish atomically with initial data, including replacement.
INSERT, UPDATE and MERGE apply existing native sort specs without a new server
capability. Comparison follows Hoglake ordering on logical values, including
unsigned values exposed as wider signed or decimal types. Floating-point sort keys
order negative zero before positive zero and NaN after positive infinity, matching
Hoglake compaction; original floating-point values remain unchanged.

Sorting applies within each output file, not across files or existing data.
The writer buffers one partition at a time, flushing at an estimated 32 MiB
including auxiliary keys and sort-position overhead (plus an incoming page).
Each sorted batch closes its files so later batches cannot break file ordering.
This costs CPU and memory and may produce smaller files than unsorted writes.

### Comments and custom properties

Table and column comments are persisted with snapshot history, including CREATE,
CTAS/replacement, `COMMENT ON`, and `ADD COLUMN ... COMMENT`. Custom annotations
use `WITH (extra_properties = MAP(ARRAY['owner.team'], ARRAY['analytics']))`.
`ALTER TABLE t SET PROPERTIES extra_properties = ...` replaces that map;
`extra_properties = DEFAULT` clears it. Keys are lowercase ASCII, at most 128
characters; `hoglake.`/`trino.` prefixes and storage-setting names are reserved.
At most 100 entries, values at most 4096 UTF-16 code units; comments at most 16384.
NUL is rejected. These annotations do not configure storage. Requires the server's
`versioned-table-metadata-v1` capability and additive V11 migration. Upgrade all server replicas before metadata
use: old DDL writers cannot preserve new versioned metadata. Old replicas
refuse metadata operations, and existing metadata-free operations remain compatible.


### Abandoned upload cleanup

Servers advertising `claimed-uploads-v1` provide durable ownership before each
Parquet or deletion-vector upload. Trino renews 24-hour leases during writes and
settles ownership atomically with publication. Unknown commit responses never
permit worker deletion. An operator can explicitly schedule expired claims with
`POST /v1/catalogs/{catalog}/uploads/schedule-expired`; the existing cleanup drain
checks retained references before deletion. No new background job is enabled.
Unclaimed older or foreign files are outside this cleanup mechanism. Permanent
fences prevent late commits and allow later sweeps to reclaim late-finishing PUTs.
This costs a claim request and durable row per file. Upgrade all server replicas
and apply V12 before use; old servers continue legacy writes without claim cleanup.
Writers idle past 24 hours can be fenced by an explicit reclamation and must retry
with new paths.

### Execution retries

With the required capabilities above, both `retry_policy = 'TASK'` and
`retry_policy = 'QUERY'` support CTAS, replacement, INSERT, UPDATE, DELETE and
MERGE. Trino retains the planned statement handle and operation ID while retrying
execution. Each worker attempt writes fresh claimed object paths, and Trino
selects the successful fragments. Only those fragments enter the atomic commit;
losing attempts remain invisible and can be reclaimed through the upload ledger.
Commit response recovery continues to use the same durable receipt.

Retries retain the original snapshot and table identity. They do not rebase a
conflicting mutation or revive an expired snapshot. Trino's retry policy still
determines which failures are recoverable: a coordinator process loss is not a
QUERY execution retry. Resubmitting SQL creates a new operation and can apply the
write again. Inspect the durable receipt when publication has an unknown outcome.
All server replicas must support claimed uploads before enabling write retries;
there is no fallback to unclaimed publication.

### Multi-statement DML transactions

Servers advertising `atomic-dml-transactions-v1` and `claimed-uploads-v1` support
explicit transactions containing INSERT, UPDATE, DELETE and MERGE on existing
tables. The first table lookup pins one catalog snapshot for the transaction.
Subsequent statements read that snapshot plus their own staged inserts and
vectors, including updates or deletes of rows inserted earlier in the transaction.
Other sessions see none of those changes until commit. Commit publishes all
written tables in one catalog snapshot; rollback publishes nothing.

The connector provides repeatable reads with snapshot isolation. Concurrent data,
schema, compaction or identity changes to any written table reject the entire
commit. Read-only tables are not validated at commit, so write skew is possible;
SERIALIZABLE is rejected. READ COMMITTED and READ UNCOMMITTED receive the stronger
repeatable-read behavior. Metadata listing surfaces still list the current head.
Trino enforces the single write-catalog boundary; there is no distributed commit
across catalogs. CREATE/CTAS/replacement, ALTER, COMMENT, DROP, RENAME, TRUNCATE and
schema DDL require autocommit and are rejected inside explicit transactions before
catalog mutation.

Staging metadata is held on the coordinator, bounded to 64 MiB and 10000 staged
uploads per transaction (including superseded vectors). Files remain in object
storage under leased upload claims. Staged files have private negative IDs only
inside the connector; permanent positive file and row IDs are allocated at commit.
The transaction endpoint resolves vectors for staged files by the path of exactly
one same-table append in that commit. No physical rewrite is required to delete
newly inserted rows, and ordinary numeric file references retain their snapshot
checks. All other consumers read the usual committed files and vectors.

Commit uses one stable operation ID and durable receipt for the complete payload.
A lost response is recovered with that receipt; an unresolved outcome reports the
operation ID and preserves every object. A coordinator loss before publication
leaves no partial catalog changes, but its in-memory transaction cannot be resumed.
A loss during publication requires checking the receipt; SQL resubmission is a
new transaction. Rollback abandons known staged objects only before publication.
Losing attempts and superseded vectors remain eligible for explicit upload cleanup.
No permanent snapshot pin is created: retention can expire the read snapshot and
cause the transaction to fail. Long idle transactions are also subject to upload
lease expiry. Upgrade every server replica before enabling these writes.
