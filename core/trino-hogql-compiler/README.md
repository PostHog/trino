# Native HogQL compiler

This module compiles HogQL directly to Trino's public SQL AST. It deliberately
does not render an intermediate SQL string or add HogQL nodes to Trino's
analyzer and planner.

The implementation is split into two modules:

- `trino-hogql-parser` owns the HogQL grammar and a private, source-located
  parser AST. It has no dependency on Trino's SQL tree.
- `trino-hogql-compiler` is the only translation boundary. It converts every
  parser node into an `io.trino.sql.tree` node.

The coordinator submits the resulting statement through the standard Trino
analysis, planning, scheduling, paging, and cancellation paths. The original
HogQL text remains the query text used by limits, history, and resource-group
selection.

## M0 contract

M0 accepts a deliberately small grammar:

```text
SELECT <projection> [, ...] [FROM <qualified physical table>]
```

Projections can be `*`, qualified column references, integers, strings,
booleans, or `NULL`. Identifiers can be unquoted, double quoted, or backquoted.
The parser rejects unsupported clauses and multiple statements instead of
silently interpreting them as SQL.

Set `hogql.enabled=true` on the coordinator to register `POST /v1/hogql`. The
request body is raw HogQL text and the response uses Trino's existing statement
protocol. Follow-up result and cancellation URIs remain under `/v1/statement`.
The endpoint is absent when the property is false, which is the default.

Development is based on the fork's `ducklake-connector` integration branch. At
the start of this milestone its head was `posthog-484-ducklake.9`, commit
`3065d56e4d5e256962c8297b4299909db8301c0a`.

Charts `main` still provides the deployment compatibility baseline:
`posthog-484-ducklake.6` at commit
`3bb054e5b80bc6ad740b26f6748b7b3f5548ff32`, published as
`ghcr.io/posthog/trino:484-ducklake.6`. Its linux/arm64 digest is
`sha256:85d2b37dfb7c966a1bfe7b0470efaa6a0116c86cb6a05d14a6bdc8c02f7a6787`.

## Fork maintenance

The `ducklake-connector` branch's common ancestor with `trinodb/trino` is
`67f588f0c81b21b425e1a43b05d70f9cf8798d6c`. The first nine PostHog commits
produce the Charts `main` `.6` baseline:

1. `a002cc56980` adds the DuckLake connector and PostHog image workflow.
2. `4cd0573a57f` adds column-name mapping.
3. `500cdf40aa3` fixes Parquet millisecond timestamp reads.
4. `c03df89358a` reads fully visible partial files.
5. `6ef914e4149` splits large data files.
6. `09690b4fc8b` pools metadata connections and supports password files.
7. `e6c3fbd9d83` adds the PostgreSQL catalog store.
8. `0bab82c7520` counts rows from catalog metadata.
9. `3bb054e5b80` reads Parquet footers using the recorded size.

The rolling integration branch continues through `.7`-`.9` with the DuckLake
write path, Trino views and schema evolution, partitioned-table reads, field-ID
based reads, and missing-file-column handling. Merge commits and CI maintenance
also live on this line; inspect the exact range from the upstream common
ancestor rather than assuming the release-tag history is linear.

Keep native HogQL changes as a separate ordered stack above those commits:
parser/compiler modules, coordinator submission seam, metadata provider,
optional functions, then packaging and tests. During an upstream update, fetch
`trinodb/trino`, identify the new common ancestor, rebase the DuckLake series,
then rebase the HogQL series onto `ducklake-connector`. Resolve AST API changes
in `TrinoAstFactory` and submission conflicts in `QueuedStatementResource`,
`DispatchManager`, and `QueryPreparer`; do not introduce HogQL branches into
later execution stages.
Run the unchanged SQL/coordinator suites, the complete DuckLake suite, and the
HogQL suite before creating a release tag.

`.github/workflows/docker-publish-posthog.yml` publishes linux/arm64 images.
Pushing `posthog-<image-tag>` builds the complete distribution and publishes
`ghcr.io/posthog/trino:<image-tag>`; a manual workflow dispatch accepts the same
image-tag without the `posthog-` prefix. The workflow intentionally skips tests
and checks, so its source commit must already have passed the suites above.

## Validation

On a fresh checkout, install their reactor dependencies without running tests:

```shell
./mvnw -pl :trino-hogql-parser,:trino-hogql-compiler,:trino-main -am install \
    -DskipTests
```

Then run the parser, compiler, and coordinator tests together:

```shell
./mvnw -pl :trino-hogql-parser,:trino-hogql-compiler,:trino-main test \
    -Dtest=TestHogQlParser,TestHogQlCompiler,TestQueryPreparer,TestHogQlConfig,TestHogQlStatementResource \
    -Dsurefire.failIfNoSpecifiedTests=false
```

The compiler tests compare the shared SQL subset with Trino's SQL parser,
format and reparse the generated tree, verify source locations, and assert that
no private HogQL AST node crosses the compiler boundary.

Measure the standard SQL preparation path with and without the compiler
available using `BenchmarkQueryPreparer`. On JDK 25, enable annotation
processing when generating the JMH benchmark index:

```shell
./mvnw -pl :trino-main clean test-compile exec:exec \
    -Dmaven.compiler.proc=full \
    -Dexec.executable=java \
    -Dexec.classpathScope=test \
    -Dexec.args='-cp %classpath io.trino.execution.BenchmarkQueryPreparer'
```

On 2026-08-26, one linux/arm64 Temurin 25 fork measured standard SQL
preparation at `3.792 ± 0.096 us/op` with the HogQL compiler available and
`3.708 ± 0.066 us/op` without it. The confidence intervals overlap; this M0
sample does not show a distinguishable unused-path regression. Retain the
benchmark for repeated measurements on release hardware.
