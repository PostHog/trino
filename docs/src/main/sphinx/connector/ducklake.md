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
:::

The connector supports reading from S3, Azure Storage, Google Cloud Storage,
and HDFS using the same [file system configuration](/object-storage) as other
object storage connectors, such as `fs.native-s3.enabled=true` and the `s3.*`
properties.

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

The connector also derives table statistics (row count, null fractions, and
value ranges) from the catalog for use by the [cost-based
optimizer](/optimizer/cost-based-optimizations).

## Limitations

- The connector is read-only; `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and DDL
  statements are not supported.
- Each query reads at the latest catalog snapshot committed when the query
  starts; time travel with `FOR VERSION AS OF` is not yet supported.
- Column mappings (`ducklake_name_mapping`) created by column renames are not
  applied; Parquet columns are resolved by name.
- Encrypted data files and inlined data are not supported.
