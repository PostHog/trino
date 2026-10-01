-- Generates the DuckDB-written shredded VARIANT fixtures in this directory.
-- See README.md for the DuckDB version, the physical layout of each file, and
-- the known DuckDB deviations from the Parquet VARIANT specification.
--
-- Usage (from this directory):
--     duckdb < generate.sql
--
-- DuckDB shreds VARIANT columns on COPY ... (FORMAT parquet) and infers the
-- shredding schema from the data. This script does not pass the SHREDDING
-- option, so the files show the inferred layout that other DuckDB-based
-- writers produce.

SELECT CASE WHEN version() <> 'v1.5.5'
    THEN error('These fixtures are pinned to DuckDB v1.5.5, found ' || version())
END;

SET threads = 1;

-- Object shredding with fields whose value type differs between rows.
-- Rows 1-8 come from JSON, so integers are INT64 variants. Rows 9-10 come
-- from typed SQL values.
COPY (
    SELECT id, v FROM (VALUES
        (1, '{"name": "alice", "age": 30, "active": true, "score": 1.5, "tags": ["a", "b"], "address": {"city": "Paris", "zip": "75001"}}'::JSON::VARIANT),
        (2, '{"name": "bob", "age": "thirty-one", "active": false, "score": 2, "address": {"city": "Berlin"}}'::JSON::VARIANT),
        (3, '{"name": 42, "age": 25, "extra": "only-here"}'::JSON::VARIANT),
        (4, '{"name": "carol", "age": null}'::JSON::VARIANT),
        (5, '"a plain string"'::JSON::VARIANT),
        (6, '123'::JSON::VARIANT),
        (7, '{"address": "not an object"}'::JSON::VARIANT),
        (8, '{"name": "dave", "age": 40, "active": true, "address": {"city": "Rome", "zip": 100}}'::JSON::VARIANT),
        (9, {'name': 'erin', 'age': 50::BIGINT}::VARIANT),
        (10, {'name': 'frank', 'age': 60::INTEGER, 'score': 3.25::DOUBLE}::VARIANT)
    ) t(id, v)
    ORDER BY id
) TO 'objects.parquet' (FORMAT parquet);

-- SQL NULL, variant null, empty objects, null fields, and missing fields.
COPY (
    SELECT id, v FROM (VALUES
        (1, NULL::VARIANT),
        (2, 'null'::JSON::VARIANT),
        (3, '{}'::JSON::VARIANT),
        (4, '{"a": null}'::JSON::VARIANT),
        (5, '{"a": 1, "b": "x"}'::JSON::VARIANT),
        (6, '{"b": "y"}'::JSON::VARIANT),
        (7, '{"a": {}}'::JSON::VARIANT),
        (8, '{"a": {"c": null}}'::JSON::VARIANT),
        (9, '[]'::JSON::VARIANT),
        (10, '{"a": [null]}'::JSON::VARIANT)
    ) t(id, v)
    ORDER BY id
) TO 'nulls.parquet' (FORMAT parquet);

-- Arrays: the inferred top-level typed_value is a LIST.
COPY (
    SELECT id, v FROM (VALUES
        (1, '[1, 2, 3]'::JSON::VARIANT),
        (2, '["x", "y"]'::JSON::VARIANT),
        (3, '[1, "two", null, 4.5]'::JSON::VARIANT),
        (4, '[]'::JSON::VARIANT),
        (5, '[[1, 2], [3]]'::JSON::VARIANT),
        (6, '[{"k": 1}, {"k": 2}]'::JSON::VARIANT),
        (7, '{"not": "an array"}'::JSON::VARIANT),
        (8, NULL::VARIANT),
        (9, [4, 5, NULL]::BIGINT[]::VARIANT),
        (10, [6]::BIGINT[]::VARIANT)
    ) t(id, v)
    ORDER BY id
) TO 'arrays.parquet' (FORMAT parquet);

-- Object keys that differ only by case, at the top level and in a nested
-- object. DuckDB v1.5.5 shreds the first spelling it sees and drops the values
-- of every other spelling (see README.md).
COPY (
    SELECT id, v FROM (VALUES
        (1, '{"$browser": "Chrome", "$os": "Mac", "props": {"Plan": "free", "plan": "pro"}}'::JSON::VARIANT),
        (2, '{"$browser": "Firefox", "$os": "Linux", "props": {"plan": "team"}}'::JSON::VARIANT),
        (3, '{"$Browser": "Safari", "$os": "iOS", "props": {"PLAN": "enterprise"}}'::JSON::VARIANT),
        (4, '{"$browser": "Edge", "$Browser": "Edge2"}'::JSON::VARIANT)
    ) t(id, v)
    ORDER BY id
) TO 'case-variant-keys.parquet' (FORMAT parquet);

-- One object with 70 keys inserted in descending order (k69, k68, ..., k00).
-- The metadata dictionary keeps insertion order but sets sorted_strings.
COPY (
    SELECT
        1 AS id,
        ('{' || string_agg('"k' || lpad(i::VARCHAR, 2, '0') || '": ' || i, ', ' ORDER BY i DESC) || '}')::JSON::VARIANT AS v
    FROM range(70) r(i)
) TO 'wide-object.parquet' (FORMAT parquet);

-- The same 70-key object in a column that is shredded as a list. The object is
-- not shredded, so it stays in `value` with the metadata dictionary above.
COPY (
    SELECT id, v FROM (
        SELECT 1 AS id, '[1, 2]'::JSON::VARIANT AS v
        UNION ALL
        SELECT 2, '[3]'::JSON::VARIANT
        UNION ALL
        SELECT 3, ('{' || string_agg('"k' || lpad(i::VARCHAR, 2, '0') || '": ' || i, ', ' ORDER BY i DESC) || '}')::JSON::VARIANT
        FROM range(70) r(i)
    )
    ORDER BY id
) TO 'wide-object-unshredded.parquet' (FORMAT parquet);

-- DuckDB's own read-back of each file, for comparison by future readers.
COPY (SELECT id, v::JSON AS v FROM 'objects.parquet' ORDER BY id) TO 'objects.duckdb.jsonl' (FORMAT json);
COPY (SELECT id, v::JSON AS v FROM 'nulls.parquet' ORDER BY id) TO 'nulls.duckdb.jsonl' (FORMAT json);
COPY (SELECT id, v::JSON AS v FROM 'arrays.parquet' ORDER BY id) TO 'arrays.duckdb.jsonl' (FORMAT json);
COPY (SELECT id, v::JSON AS v FROM 'case-variant-keys.parquet' ORDER BY id) TO 'case-variant-keys.duckdb.jsonl' (FORMAT json);
COPY (SELECT id, v::JSON AS v FROM 'wide-object.parquet' ORDER BY id) TO 'wide-object.duckdb.jsonl' (FORMAT json);
COPY (SELECT id, v::JSON AS v FROM 'wide-object-unshredded.parquet' ORDER BY id) TO 'wide-object-unshredded.duckdb.jsonl' (FORMAT json);
