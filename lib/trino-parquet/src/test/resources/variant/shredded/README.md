# Shredded VARIANT fixtures

Parquet files with shredded VARIANT columns, for testing readers of the
[Parquet VARIANT shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
`TestShreddedVariantFixtures` checks that the files are complete and that their
footers can be read. It does not decode variant values.

## `parquet-testing/`

Copied without changes from the `shredded_variant` directory of
[apache/parquet-testing](https://github.com/apache/parquet-testing):

- Repository commit: `56653c437c8092f704a092d0d1d4e600124cd49f` (2026-09-15)
- Last commit that changed `shredded_variant`:
  `cf1eed4dfb45794be0f35c42d1d323fd617d763f` (apache/parquet-testing#117)
- License: Apache License, Version 2.0. `LICENSE.txt` is a copy of the license
  file at the root of the source repository.

`README.md` in that directory is the upstream description of `cases.json` and of
the `.variant.bin` encoding. The cases come from Iceberg's `TestVariantReaders`.

`cases.json` has 138 entries:

- 128 single-row cases (`variant_file`, `variant`)
- 3 multi-row cases (`variant_files`, `variants`): 45, 83, and 126. Row 0 of
  case 83 is SQL NULL, so its variant file is `null`.
- 6 error cases (`error_message`): 40, 42, 87, 127, 128, and 137
- Case 3, which has no files

Seven single-row cases (41, 43, 84, 125, 131, 132, and 138) have `-INVALID` file
names and a `notes` entry. These files do not follow the specification. A reader
can reject them or read the shredded value.

To update, replace the directory contents with a newer `shredded_variant`
directory, update the commits above, and update the expected case numbers in
`TestShreddedVariantFixtures`.

## `duckdb/`

Written by DuckDB v1.5.5 (build `d8cdaa33fd`) with `generate.sql`. To generate
them again, run this command in that directory:

```text
duckdb < generate.sql
```

The script fails with any other DuckDB version. Its output is deterministic.
DuckDB shreds VARIANT columns when it writes Parquet, and it infers the
shredding schema from the data. The script does not set the `SHREDDING` option.

Each `<name>.duckdb.jsonl` file is DuckDB's own read-back of `<name>.parquet`
(`v::JSON`), not the script input. JSON does not keep the variant type of a
number, and it shows SQL NULL and variant null in the same way.

| File                        | Shredded `typed_value` of `v`                                                                                       | Covers                                                                                                         |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `objects.parquet`           | object: `address` (object: `zip`, `city`), `tags` (list of string), `extra`, `score` (double), `active` (boolean), `age` (int64), `name` | Keys with different value types in different rows, non-object rows, null and missing fields, a nested object |
| `nulls.parquet`             | object: `b` (string), `a` (object: `c` (int32))                                                                     | SQL NULL and variant null, `{}` rows, null fields, missing fields, `[]` in an object column                    |
| `arrays.parquet`            | list of int64                                                                                                       | Integer, string, mixed, nested, and empty arrays, an array of objects, a non-array row, SQL NULL              |
| `case-variant-keys.parquet` | object: `props` (object: `Plan`), `$os`, `$browser`                                                                  | Keys that differ only by case, at the top level and in a nested object                                        |
| `wide-object.parquet`       | object with 70 int64 fields `k00` to `k69`                                                                          | A metadata dictionary of 70 keys in descending order                                                          |
| `wide-object-unshredded.parquet` | list of int64                                                                                                  | The 70-key object of `wide-object.parquet` in row 3, stored unshredded in `value` with its DuckDB metadata    |

All files have one row group. The `id` column is an optional INT32.

### DuckDB v1.5.5 behavior

These facts were checked by reading the files with pyarrow 25.0.1. Some of them
do not follow the specification, and a reader of DuckDB-written files must
handle them.

1. The `v` group has the VARIANT logical type annotation, and `metadata` is its
   first child.
2. Shredded object field groups and list `element` groups are `optional`. The
   specification requires `required` groups.
3. DuckDB does not write SQL NULL as a null `v` group. A SQL NULL row has
   `value` set to variant null (`0x00`), so it is the same as a variant null
   row. See rows 1 and 2 of `nulls.parquet` and row 8 of `arrays.parquet`.
4. The `sorted_strings` bit of the metadata header is always 1, including for
   dictionaries that are not sorted (`objects.parquet`, `wide-object.parquet`)
   and dictionaries with duplicate strings (row 6 of `arrays.parquet` has
   `["k", "k"]`). In `wide-object-unshredded.parquet`, the unsorted dictionary
   belongs to an object stored in `value`, so a reader that copies `value`
   keeps it. The specification allows the bit only when the strings are
   sorted and unique. A reader that binary-searches a dictionary with this bit
   set does not find most keys of `wide-object.parquet`. For example,
   `io.trino.spi.variant.Metadata.id` binary-searches dictionaries of 64 or more
   strings, and finds 1 of the 70 keys of that dictionary. DuckDB PR
   duckdb/duckdb#23496 proposed a fix and was closed without merging.
5. Integers that come from JSON are written to `value` as INT64, also where
   `typed_value` is INT64 (`age` in rows 1, 3, and 8 of `objects.parquet`, the
   integer elements of rows 1 and 3 of `arrays.parquet`, all fields in
   `wide-object.parquet`). Typed
   BIGINT values are written to `typed_value` (row 9 of `objects.parquet`, rows 9
   and 10 of `arrays.parquet`). An INTEGER is written to `value` as INT32 when
   `typed_value` is INT64 (row 10 of `objects.parquet`).
6. An empty object is written as a partially shredded object: `value` is an
   empty object, and `typed_value` is not null with all of its fields missing
   (rows 3 and 7 of `nulls.parquet`). This is valid. Compare with error case
   128 in `parquet-testing/`, where `value` is variant null.
7. In these files, every object key is in `typed_value`. A row has a top-level
   `value` only if it is not an object or it is an empty object.
8. If keys differ only by case, DuckDB shreds the first spelling that it finds.
   It drops the values of all other spellings from both `value` and
   `typed_value`, in all rows, at all levels. The metadata of a row still
   contains the dropped spelling. In `case-variant-keys.parquet` these input
   values are not in the file: `props.plan` = `"pro"` (row 1), `props.plan` =
   `"team"` (row 2), `$Browser` = `"Safari"` and `props.PLAN` = `"enterprise"`
   (row 3), and `$Browser` = `"Edge2"` (row 4). See duckdb/duckdb#24297. Because
   DuckDB never writes two shredded fields whose names differ only by case,
   this file does not contain such sibling fields.
9. The order of the fields in `typed_value` is not the order of the metadata
   dictionary and is not alphabetical.
