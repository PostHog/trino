# DuckLake connector

```{raw} html
<img src="../_static/img/duckdb.png" class="connector-logo">
```

The DuckLake connector allows querying tables in a
[DuckLake](https://ducklake.select/) catalog. DuckLake stores table metadata in
a SQL database and table data as Parquet files on object storage. The connector
reads the metadata directly from the catalog database (PostgreSQL) and scans
the Parquet data files with Trino's native Parquet reader, so queries run
distributed across the cluster without going through DuckDB.

The connector is read-only. Tables are written by DuckDB (or other DuckLake
writers) and queried from Trino. Rows removed with `DELETE` in DuckDB are
recorded in positional delete files, which the connector applies when reading.

## Requirements

To connect to DuckLake, you need:

- A DuckLake catalog stored in a PostgreSQL database, specification version
  1.0 or older.
- Network access from the Trino coordinator and workers to the catalog
  database.
- Access to the object storage (or file system) location holding the Parquet
  data files.

## General configuration

To configure the DuckLake connector, create a catalog properties file
`etc/catalog/example.properties` with the following content, replacing the
connection properties as appropriate for your setup:

```text
connector.name=ducklake
ducklake.metadata.connection-url=jdbc:postgresql://example.net:5432/ducklake
ducklake.metadata.connection-user=admin
ducklake.metadata.connection-password=secret
ducklake.data-path=s3://example-bucket/
fs.native-s3.enabled=true
s3.region=us-east-1
```

The following configuration properties are available:

:::{list-table}
:widths: 40, 45, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `ducklake.metadata.connection-url`
  - The JDBC URL of the PostgreSQL database holding the DuckLake catalog
    (`ducklake_*` tables).
  -
* - `ducklake.metadata.connection-user`
  - User name for the catalog database.
  -
* - `ducklake.metadata.connection-password`
  - Password for the catalog database.
  -
* - `ducklake.metadata.connection-password-file`
  - Path to a file holding the password for the catalog database. The file is read again for
    every connection, so a rotated password takes effect without restarting Trino. Cannot be
    combined with `ducklake.metadata.connection-password`. Prefer this property for a catalog
    created with [](/sql/create-catalog), because the password then does not appear in the
    statement that Trino records in the query log and shows in the Web UI.
  -
* - `ducklake.metadata.connection-pool.max-size`
  - Maximum number of connections the catalog keeps open to the catalog database. Keep this
    small when a cluster hosts many DuckLake catalogs, because the limit applies per catalog.
  - `10`
* - `ducklake.metadata.connection-pool.idle-timeout`
  - Close pooled connections that have been idle for longer than this. The pool keeps no
    minimum number of idle connections, so a catalog that is not queried ends up holding no
    connection to the catalog database.
  - `1m`
* - `ducklake.metadata.connection-pool.acquisition-timeout`
  - Fail a query that waits longer than this for a connection from the pool, instead of letting
    it wait indefinitely.
  - `30s`
* - `ducklake.metadata.schema`
  - Schema in the catalog database holding the `ducklake_*` metadata tables.
  - `public`
* - `ducklake.data-path`
  - Base location of the DuckLake data files. Relative data file paths in the
    catalog resolve against this location. Must match the `DATA_PATH` the
    writer used when attaching the catalog.
  -
* - `ducklake.file-statistics-pruning.enabled`
  - Prune data files using the per-file column statistics recorded in the
    catalog.
  - `true`
* - `ducklake.max-split-size`
  - Target size of a split. A data file larger than this is read as several
    byte ranges in parallel. Also configurable per query with the
    `max_split_size` [catalog session property](/sql/set-session).
  - `64MB`
* - `ducklake.commit.max-retries`
  - How often a commit that lost the race for the next snapshot is attempted again against the
    newer state. Raise this on a catalog with many concurrent writers. See
    [](ducklake-concurrent-writers).
  - `10`
* - `ducklake.commit.retry-backoff`
  - How long to wait before attempting a commit again. The wait is doubled after each attempt,
    up to 32 times this value.
  - `20ms`
:::

The connector supports reading from S3, Azure Storage, Google Cloud Storage,
and HDFS using the same [file system configuration](/object-storage) as other
object storage connectors, such as `fs.native-s3.enabled=true` and the `s3.*`
properties.

(ducklake-concurrent-writers)=
### Concurrent writers

A DuckLake catalog orders every change on one chain of snapshots, so a commit
has to claim the snapshot following the newest one. When another writer, such
as DuckDB or a second Trino cluster, claims it first, the connector applies the
DuckLake conflict rules and either lands the commit on the newer snapshot or
fails the query. It never rewrites the data files it already wrote, and a
failed attempt leaves nothing behind in the catalog.

The commit lands on the newer snapshot when the other writer changed something
this statement does not depend on. Two writers inserting into the same table is
the common case and always succeeds, as does any pair of statements writing to
different tables. The connector attempts the commit again up to
`ducklake.commit.max-retries` times, waiting `ducklake.commit.retry-backoff`
before the first attempt and doubling the wait after each one.

The query fails with the `DUCKLAKE_COMMIT_CONFLICT` error code when the other
writer changed the table this statement writes to, in a way that invalidates
the result:

* Inserting into a table another writer altered, dropped, or deleted from.
* Deleting from a table another writer altered, dropped, inserted into, or
  compacted.
* Altering a table another writer altered or dropped.
* Dropping a table another writer dropped.
* Rewriting a data file or a delete file another writer replaced.

Statements naming what they create, such as `CREATE TABLE` and `CREATE SCHEMA`,
resolve the name against the newer catalog instead, and report the ordinary
"already exists" or "not found" error if the other writer took it.

These are the rules DuckDB applies to the same catalog, so a statement is
accepted or rejected here exactly as it would be there. A client driving the
statement can match `DUCKLAKE_COMMIT_CONFLICT` to tell a lost race apart from a
broken catalog, and run the statement again once it has been replanned.

A statement reads one snapshot throughout, so a table it only reads from can
change under it without failing the commit. The result is then computed from
the snapshot the statement started at, which is what reading a snapshot means,
rather than from the newest one.

A query fails with `DUCKLAKE_UNSUPPORTED_CHANGE_TYPE` when another writer
recorded a kind of change this connector does not know. The connector cannot
decide whether committing on top of that snapshot is safe, so it refuses rather
than risk dropping the other writer's work. Upgrade the connector to a version
that understands the DuckLake version the other writer uses.

## Type mapping

The connector maps DuckLake column types to Trino types as follows:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - DuckLake type
  - Trino type
* - `boolean`
  - `BOOLEAN`
* - `int8`, `int16`, `int32`, `int64`
  - `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`
* - `uint8`, `uint16`, `uint32`
  - `SMALLINT`, `INTEGER`, `BIGINT` (widened)
* - `uint64`
  - `DECIMAL(20, 0)`
* - `int128`
  - `DECIMAL(38, 0)`
* - `float32`, `float64`
  - `REAL`, `DOUBLE`
* - `decimal(p, s)`
  - `DECIMAL(p, s)`
* - `varchar`, `json`
  - `VARCHAR`
* - `blob`
  - `VARBINARY`
* - `uuid`
  - `UUID`
* - `date`
  - `DATE`
* - `time`
  - `TIME(6)`
* - `timestamp`, `timestamp_s`, `timestamp_ms`, `timestamp_ns`
  - `TIMESTAMP(6)`, `TIMESTAMP(0)`, `TIMESTAMP(3)`, `TIMESTAMP(9)`
* - `timestamptz`
  - `TIMESTAMP(6) WITH TIME ZONE`
* - `list`, `struct`, `map`
  - `ARRAY`, `ROW`, `MAP`
:::

Queries on columns of the unsupported types `uint128`, `timetz`, and
`interval` fail.

## Name mapping

Data files that were written outside of DuckLake, for example files registered
with the DuckDB `ducklake_add_data_files` function, do not carry DuckLake field
identifiers in their Parquet metadata. For such files the catalog records a name
mapping in `ducklake_name_mapping` that assigns a Parquet column name to each
DuckLake column, and the connector reads the columns of these files under the
mapped names. A column that the mapping does not cover is not stored in the file
and reads as `NULL`. Predicates are pushed into the Parquet reader under the
mapped names, so file and row group pruning remain correct.

## Performance

The connector skips data files that cannot match query predicates:

- Partition values recorded in the catalog are compared against predicates on
  partition columns, for the `identity`, `year`, `month`, `day`, and `hour`
  partition transforms.
- The per-file minimum, maximum, and null count column statistics in the
  catalog are compared against predicates on all columns. The
  `ducklake.file-statistics-pruning.enabled` catalog property or the
  `file_statistics_pruning_enabled` [catalog session
  property](/sql/set-session) disable this behavior.

A data file larger than `ducklake.max-split-size` is divided into splits that
each cover a byte range of the file, so the row groups of a single large file
are read by many workers in parallel instead of by a single thread. Lower the
value with the `ducklake.max-split-size` catalog property or the
`max_split_size` [catalog session property](/sql/set-session) to increase the
parallelism of queries over few, large files.

`SELECT count(*)` over a whole table is answered from the record counts in the
catalog, without listing the data files of the table or reading any of them. A
count that the catalog cannot answer on its own, such as one restricted by a
predicate on a partition column, still lists the files but reads none of them,
because a scan that reads no column is served from the record count of each
file.

The connector reads the Parquet footer of a data file in a single request, using
the footer size recorded in the catalog. Files written with many columns or many
row groups have footers larger than the length a reader guesses at, and every
split of such a file would otherwise pay for a second request.

The connector also derives table statistics (row count, null fractions, and
value ranges) from the catalog for use by the [cost-based
optimizer](/optimizer/cost-based-optimizations).

## Limitations

- The connector is read-only; `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and DDL
  statements are not supported.
- Each query reads at the latest catalog snapshot committed when the query
  starts; time travel with `FOR VERSION AS OF` is not yet supported.
- Queries on a column whose name mapping reads the values from a Hive partition
  in the file path (`ducklake_name_mapping.is_partition`), or maps the fields
  nested inside the column, fail. Other columns of such a table can be read.
- Encrypted data files and inlined data are not supported.
