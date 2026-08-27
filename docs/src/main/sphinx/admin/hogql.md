# Native HogQL queries

The PostHog Trino distribution can accept read-only HogQL through
`POST /v1/hogql`. The coordinator parses and compiles the query to the standard
Trino SQL AST before analysis and planning. Workers, connectors, access control,
and the optimizer receive standard Trino plans.

Set the following coordinator property to enable the endpoint:

```properties
hogql.enabled=true
```

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

The Trino transport does not currently send an application authentication
credential. Duckgres requires `X-Duckgres-Internal-Secret` by default, so the
production connection remains incomplete until the coordinator credential
mechanism is configured.

## Semantic catalog properties

| Property | Default | Description |
| --- | --- | --- |
| `hogql.semantic-catalog.maximum-entries` | `100` | Maximum catalog snapshots retained by one coordinator. |
| `hogql.semantic-catalog.refresh-after` | `1m` | Age at which a cached manifest is refreshed in the background. |
| `hogql.semantic-catalog.expire-after` | `5m` | Age after which a cached manifest cannot be used. |
| `hogql.semantic-catalog.failure-backoff` | `10s` | Minimum delay before retrying a failed refresh. |
| `hogql.semantic-catalog.loader-threads` | `4` | Number of background manifest loader threads. |
| `hogql.semantic-catalog.loader-queue-capacity` | `64` | Maximum queued background manifest loads. |
| `hogql.semantic-catalog.request-timeout` | `10s` | Timeout for a background compatibility request. |
| `hogql.semantic-catalog.maximum-response-size` | `8MB` | Maximum accepted manifest response size. |

Keep `refresh-after` below `expire-after`. Size the entry count, loader threads,
and queue capacity for the number of catalogs assigned to one coordinator.
