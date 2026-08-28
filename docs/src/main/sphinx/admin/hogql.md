# Native HogQL queries

The PostHog Trino distribution can accept read-only HogQL through
`POST /v1/hogql`. The coordinator parses and compiles the query to the standard
Trino SQL AST before analysis and planning. Workers, connectors, access control,
and the optimizer receive standard Trino plans.

Set the following coordinator property to enable the endpoint:

```properties
hogql.enabled=true
```

Compilation runs on a dedicated fixed-size executor. Configure
`hogql.compilation-threads`, `hogql.compilation-queue-capacity`, and
`hogql.compilation-timeout`; saturation and timeout fail with retryable
insufficient-resource errors without affecting standard SQL submission.

The executable profile supports read-only queries over logical tables declared
by the pinned manifest, including ordered star expansion, properties, lazy
relationships, and declarative actions. Physical tables remain available
through normal Trino names. Unsupported functions and semantic definitions
fail before Trino analysis.

The endpoint accepts a JSON request envelope. Identity, catalog, schema, time
zone, source, tags, and allowed session properties continue to use the standard
Trino request headers.

```json
{
  "query": "SELECT event FROM events WHERE person_id = {person_id}",
  "protocolVersion": 1,
  "languageVersion": "1.0.0",
  "parameters": {
    "person_id": {"type": "uuid", "value": "00000000-0000-0000-0000-000000000000"}
  },
  "variables": {},
  "filters": {},
  "modifiers": {}
}
```

Responses use the standard Trino query results protocol, including result
paging and cancellation. The coordinator rejects request bodies larger than 2
MiB and more than 1,000 total entries across `parameters`, `variables`,
`filters`, and `modifiers`.

## Modifiers

Modifiers are defined by the pinned semantic catalog manifest. An explicit
modifier causes the coordinator to pin a manifest even when the query does not
otherwise use semantic catalog definitions. Unknown modifiers and values whose
type does not exactly match the declared type are rejected before execution.

Each declared modifier has one behavior:

| Behavior | Result |
| --- | --- |
| `TRINO_SESSION_PROPERTY` | Applies the explicit value, or the manifest default when omitted, as a query-scoped Trino session property. Normal session-property validation and access control still apply. |
| `COMPILER` | Uses the compiler's default behavior when omitted. Explicit values are rejected until that modifier has a compiler handler. |
| `SAFE_NOOP` | Accepts a type-valid explicit value or default and does not change the query. |
| `UNSUPPORTED` | Does nothing when omitted and rejects explicit use. |

Modifier values stay outside the generated statement AST. The coordinator
decodes them through the typed-value boundary and carries the resulting session
overrides separately during query preparation.

To return a native Trino plan instead of executing the query, add an `explain`
object:

```json
{
  "query": "SELECT event FROM events",
  "protocolVersion": 1,
  "languageVersion": "1.0.0",
  "explain": {"type": "DISTRIBUTED", "format": "TEXT"}
}
```

The supported plan types are `LOGICAL`, `DISTRIBUTED`, `VALIDATE`, and `IO`.
The supported formats are `TEXT`, `GRAPHVIZ`, and `JSON`. Execution timeouts
continue to use Trino session properties such as `query_max_execution_time`.

## Compilation capacity

HogQL compilation runs on coordinator threads that are separate from standard
query dispatch work. The following properties set the maximum concurrent and
queued compilation work:

| Property | Default | Description |
| --- | --- | --- |
| `hogql.compilation-threads` | `2` | Number of coordinator threads dedicated to HogQL compilation. The value must be at least `1`. |
| `hogql.compilation-queue-capacity` | `32` | Maximum compilations waiting for a compiler thread. Set this to `0` to reject requests instead of queuing them when every compiler thread is busy. |

When the workers and queue are full, the coordinator fails the HogQL query with
`HOGQL_COMPILATION_QUEUE_FULL`. This is an insufficient-resources error, so the
caller can retry the query. Standard Trino SQL does not use this executor or
queue.

## Semantic catalog manifests

Logical tables, fields, quoted physical names, and star expansion use an
immutable semantic catalog manifest published by Duckgres. Configure its base
URI on the coordinator:

```properties
hogql.semantic-catalog.uri=https://duckgres.example
```

The coordinator refreshes manifests asynchronously and compiles each query
against one pinned generation. A query does not wait for a remote metadata
request. A missing, expired, malformed, or mismatched manifest fails the query
with a catalog readiness or compatibility error.

The compatibility request path is
`/v1/hogql/compatibility/semantic-catalog`. The coordinator identifies the
selected Trino catalog, HogQL language version, and optional requested
generation. Manifests contain typed identifiers and definitions, not SQL text.

Qualified stars on relations without a manifest schema compile to standard
Trino `relation.*`. `EXCLUDE` needs the manifest's ordered, star-visible field
list, and fails with a source-located compatibility error when that schema is
unavailable. Duckgres should publish physical tables as physical-derived
semantic definitions from the physical catalog inventory when those tables
need HogQL star exclusion.

Set `hogql.semantic-catalog.authentication-token-file` to a file containing a
Duckgres read-only or admin token. Trino reads the file for every manifest
request so token rotation does not require a coordinator restart. Surrounding
whitespace is stripped; blank tokens, oversized tokens, and embedded CR or LF
are rejected. The token is sent as `X-Duckgres-Internal-Secret` and is never
included in error messages.

Clients can set `catalogGeneration` in the request envelope to require an exact
immutable generation. Exact-generation cache entries never fall back to the
latest manifest. A missing, expired, or mismatched generation fails closed.

## Function profile

The original MVP function set remains supported:

| Family | HogQL names |
| --- | --- |
| Numeric, conditional, and strings | `abs`, `coalesce`, `if`, `lower`, `upper`, `length`, `concat`, `replace` |
| Collections | `map`, `arraySort`, `arrayDistinct`, `arrayFlatten`, `arrayStringConcat` |
| Date and time | `dateAdd`, `dateDiff`, `dateTrunc` |
| Aggregates | `count`, `sum`, `min`, `max`, `avg`, `any`, `argMin`, `argMax`, `array_agg` |
| Windows | `first_value`, `rank`, `row_number` |

The Team 2 materialized-view compatibility extension adds the exact scalar,
aggregate, JSON, collection, date/time, regular-expression, conversion, and
window rewrites exercised by its frozen query corpus. The in-tree
`HogQlV0FunctionRegistry` is the source of truth for accepted names, arities,
determinism, and lowering strategy. Catalog function declarations do not
silently broaden this fail-closed profile.

Declarative actions resolve by name or ID from the pinned manifest and lower
to optimizer-visible predicates or relation membership. Cohorts, saved
queries, explicit modifiers without an enabled behavior, HogQLX,
PIVOT/UNPIVOT execution, unsupported ClickHouse clauses, and complete
type/function parity remain outside this endpoint profile. Unsupported
constructs return typed compatibility errors; they do not fall through as
Trino functions.

`any`, `argMin`, and `argMax` are nondeterministic when more than one input can
satisfy the selection rule. Differential validation uses unique selection keys
or invariant-based comparisons for those calls.

## Physical catalog inventory

An authenticated client can read connector-facing table and column metadata
from `GET /v1/hogql/compatibility/physical-catalog?catalog=<catalog>&protocolVersion=1`.
The endpoint is available only on coordinators when `hogql.enabled=true`.

The versioned response contains `protocolVersion` and `schemaVersion` before
the structured catalog, schema, table, and column identifiers. Each column
includes its one-based ordinal, exact Trino type signature, nullability, hidden
state, and star visibility. Hidden columns are reported when the caller can see
them, but remain excluded from star expansion. The ordinal is the connector's
original column position. Column visibility filtering can therefore leave gaps
in the returned ordinals.

The `catalogHandleVersion` identifies the active Trino catalog registration.
It detects a catalog replacement during the request, but it is not the
connector's metadata snapshot or a semantic catalog generation. Duckgres
assigns its own monotonically increasing generation when it publishes the
inventory.

The endpoint applies the same catalog, table, and column visibility filters as
Trino metadata listings. It reads one connector transaction, so DuckLake
metadata is pinned to one snapshot. The request fails if the catalog is dropped
or replaced while the inventory is read. Results are limited to 10,000 tables,
10,000 columns per table, 100,000 columns in total, and 8 million characters of
identifier and type text.

Use normal Trino HTTP authentication and request headers. The endpoint does not
accept a separate credential or an existing transaction ID.

## Semantic catalog properties

| Property | Default | Description |
| --- | --- | --- |
| `hogql.semantic-catalog.maximum-entries` | `100` | Maximum catalog snapshots retained by one coordinator. |
| `hogql.semantic-catalog.authentication-token-file` | none | Required when the semantic catalog URI is set. File containing a rotating Duckgres read-only or admin token. |
| `hogql.semantic-catalog.refresh-after` | `1m` | Age at which a cached manifest is refreshed in the background. |
| `hogql.semantic-catalog.expire-after` | `5m` | Age after which a cached manifest cannot be used. |
| `hogql.semantic-catalog.failure-backoff` | `10s` | Minimum delay before retrying a failed refresh. |
| `hogql.semantic-catalog.loader-threads` | `4` | Number of background manifest loader threads. |
| `hogql.semantic-catalog.loader-queue-capacity` | `64` | Maximum queued background manifest loads. |
| `hogql.semantic-catalog.request-timeout` | `10s` | Timeout for a background compatibility request. |
| `hogql.semantic-catalog.maximum-response-size` | `8MB` | Maximum accepted manifest response size. |

Keep `refresh-after` below `expire-after`. Size the entry count, loader threads,
and queue capacity for the number of catalogs assigned to one coordinator.

## Image identity and rollback

Release images use tags of the form
`<trino>-ducklake.<release>-hogql.<release>`. Inspect the immutable digest and
the `io.posthog.trino.*` OCI labels to verify the source/DuckLake revision,
compiler build, server and CLI artifact digests, language version, and semantic
catalog protocol/schema.

To roll back, first stop routing clients to `/v1/hogql`. Set
`hogql.enabled=false` and roll the coordinators; standard `/v1/statement`
queries remain on the unchanged Trino path. Restore the previous immutable
image digest if needed. Semantic catalog content is immutable, so restore prior
content by publishing it as a new higher generation rather than mutating an
existing generation.
