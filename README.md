<h1 align="center">TruffleHog</h1>

<p align="center">
  <b>PostHog's distributed SQL lakehouse query engine — a custom fork of <a href="https://trino.io/">Trino</a> built to power the <a href="https://posthog.com/docs/data-warehouse">PostHog Data Warehouse</a>.</b>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="Apache 2.0 License" /></a>
  <a href="https://posthog.com/docs/data-warehouse"><img src="https://img.shields.io/badge/PostHog-Data_Warehouse-F54E00" alt="PostHog Data Warehouse" /></a>
  <a href="https://trino.io/docs/current/"><img src="https://img.shields.io/badge/Upstream_Docs-Trino-DD00A1" alt="Upstream Trino Documentation" /></a>
</p>

---

## Overview

**TruffleHog** is [PostHog](https://posthog.com)'s custom fork of [Trino](https://github.com/trinodb/trino) and serves as the distributed SQL execution foundation of PostHog's multi-tenant Data Warehouse.

TruffleHog decouples compute from object storage so PostHog customers can query, join, and transform petabyte-scale lakehouse tables, external databases, and their own PostHog product analytics data through a single horizontally scalable SQL engine — while speaking the standard Trino client protocol so existing JDBC drivers, Python clients, and BI tools work out of the box.

## Role in the PostHog Data Warehouse Architecture

TruffleHog is one of three core services that make up PostHog's data warehouse stack alongside **`posthog/hogtower`** and **`posthog/hoglake`**:

```text
                       ┌──────────────────────────────────────────┐
                       │             posthog/hogtower             │
                       │    Control Plane, Tenancy & Identity     │
                       │  • Tenant warehouse & catalog lifecycle  │
                       │  • OAuth 2.0 / RFC 8693 token issuer     │
                       │  • Dynamic catalog & OPA policy rollout  │
                       └───────────┬──────────────────┬───────────┘
           Catalog sync, OPA &     │                  │  OAuth 2.0 / token-exchange
           delegated DDL           ▼                  ▼  verification
┌──────────────────────────────────────┐          ┌──────────────────────────────────────┐
│          PostHog/trufflehog          │  REST    │           posthog/hoglake            │
│      Distributed SQL Compute         ├─────────►│   Lakehouse Format & Catalog API     │
│  • Stateless coordinators & workers  │ metadata │  • Transactional metadata (Postgres) │
│  • HogLake, DuckLake, Iceberg &      │          │  • Snapshots, namespaces & schemas   │
│    federated external connectors     │          │  • Read-only org event mounts        │
│  • Shredded Parquet VARIANT engine   │          └──────────────────┬───────────────────┘
└──────────────────┬───────────────────┘                             │
                   │  Direct Parquet / VARIANT reads & writes        │  Tracks Parquet files
                   └────────────────────────┬────────────────────────┘
                                            ▼
                           ┌──────────────────────────────────┐
                           │   S3-Compatible Object Storage   │
                           │    (Parquet + Shredded VARIANT)  │
                           └──────────────────────────────────┘
```

- **[`posthog/hogtower`](https://github.com/PostHog/hogtower) — Control Plane, Tenancy & Identity:** Manages multi-tenant warehouse provisioning, organization-to-catalog mappings, OPA authorization policies, and identity. HogTower publishes dynamic catalog revisions to TruffleHog coordinators, handles delegated `CREATE CATALOG` / `DROP CATALOG` requests with cluster-wide visibility checks, and acts as the OAuth 2.0 / RFC 8693 token-exchange issuer for catalog and per-user access.
- **[`posthog/hoglake`](https://github.com/PostHog/hoglake) — Lakehouse Table Format & Metadata Catalog:** PostHog's open lakehouse table format and REST catalog service designed for petabyte-scale warehouse workloads and high snapshot churn. HogLake stores transactional metadata, schema evolution, and snapshots in PostgreSQL while table data lives as Parquet files in S3-compatible object storage. It also exposes each organization's PostHog event stream (`<org-id>-events`) as a read-only `posthog` schema mount.
- **`PostHog/trufflehog` (this repository) — Distributed SQL Query & Compute Engine:** Executes interactive queries, cross-source joins, and batch materializations across `HogLake`, `DuckLake`, `Iceberg`, and federated external data sources (PostgreSQL, MySQL, Snowflake, BigQuery, S3, and more), reading and writing Parquet files directly against object storage.

## How TruffleHog Differs from ClickHouse at PostHog

ClickHouse and TruffleHog solve complementary problems at PostHog:

| Dimension | ClickHouse at PostHog | TruffleHog (`PostHog/trufflehog`) |
| --- | --- | --- |
| **Primary workload** | Real-time product analytics, session replay metadata, and high-throughput streaming event ingestion | Multi-tenant Data Warehouse queries, ad-hoc SQL joins, federated external sources, and batch transformations / ETL |
| **Storage & compute model** | Tightly coupled local storage and compute tuned for sub-second scans over PostHog's core tables | Stateless distributed compute decoupled from S3-compatible object storage |
| **Data formats & sources** | Native ClickHouse MergeTree tables | Open lakehouse formats (`HogLake`, `DuckLake`, `Iceberg`, Parquet with shredded `VARIANT`) plus federated external databases and SaaS sources |
| **Multi-tenancy & isolation** | Shared clusters keyed by team/project ID | Per-tenant dynamic catalogs, OAuth/OPA isolation, resource groups, and read-only `posthog` event mounts per login |
| **PostHog event access** | Primary store for live product analytics ingestion and queries | Reads organization event mounts via `HogLake` (`posthog` schema) so warehouse tables can join directly with PostHog events, backed by ClickHouse-compatible SQL functions (`trino-posthog-functions`) |

## How TruffleHog Differs from Upstream Trino

While TruffleHog preserves Trino's core SQL optimizer, execution engine, SPI, and wire protocol, it adds several capabilities built specifically for PostHog's warehouse:

- **Native HogLake Connector (`plugin/trino-hoglake`):** Full read/write integration with the `HogLake` REST catalog API, including OAuth 2.0 client-credentials authentication, RFC 8693 per-login token exchange for read-only `posthog` organization event mounts, worker-local Parquet footer caching, and split-planning optimizations.
- **Native DuckLake Connector (`plugin/trino-ducklake`):** Support for reading, writing, and merging DuckLake-managed lakehouse tables on object storage.
- **Shredded Parquet `VARIANT` Support (`core/trino-spi`, `lib/trino-parquet`):** End-to-end `VARIANT` data type support, Parquet `VARIANT` shredding on read and write, pushed-down subscript path assembly, and lazy residual decoding for semi-structured event and warehouse properties.
- **Multi-Tenant Dynamic Catalog Store (`plugin/trino-posthog-catalogstore`):** PostgreSQL-backed catalog synchronization across multiple coordinators, coordinator readiness/drain status endpoints, and control-plane-delegated `CREATE CATALOG` / `DROP CATALOG` execution with cluster-wide visibility waits.
- **ClickHouse & HogQL Function Compatibility (`plugin/trino-posthog-functions`):** Native implementations of ClickHouse hash (`cityHash64`, etc.), JSON extraction, and numeric functions relied upon by HogQL and PostHog warehouse queries.
- **Multi-Tenant Authentication & Authorization Extensions:** Service-credential authentication (`plugin/trino-password-authenticators`), live-reloading file authenticators and group providers, and OPA policy revision tracking (`plugin/trino-opa`).
- **Lean ARM64 Server Distribution (`core/trino-server-posthog`):** A trimmed server assembly and layered OCI container build packaging only the connectors required by PostHog's warehouse infrastructure.

## Upstream Trino Relationship

TruffleHog is built on the shoulders of [upstream Trino (`trinodb/trino`)](https://github.com/trinodb/trino).

- **Best-effort upstream sync:** We do our best to keep TruffleHog up to date with upstream Trino releases and improvements (query optimizer, vectorized execution, Parquet/filesystem libraries, security fixes, and connectors) on a **best-effort basis where it makes sense** for PostHog's workloads.
- **Compatibility by design:** To keep upstream merges clean and ensure seamless compatibility with the Trino ecosystem (JDBC, Python `trino` client, BI integrations, and standard plugins), internal Java package names (`io.trino.*`), Maven coordinates, and client wire protocol headers (`X-Trino-*`) intentionally remain unchanged.
- **Pragmatic divergence:** Where PostHog's multi-tenant control plane (`hogtower`), lakehouse format (`hoglake`), or operational model has different requirements from general-purpose upstream Trino, TruffleHog diverges intentionally.

---

## Development & Contributing

- **Agent & contributor safety rules:** Read [AGENTS.md](AGENTS.md) and [CLAUDE.md](CLAUDE.md) before making changes. **This repository is public — never include customer data, internal endpoints, account IDs, or secrets in code, tests, docs, commits, or PRs.**
- **Developer guide & code style:** See [.github/DEVELOPMENT.md](.github/DEVELOPMENT.md) for Java code style (`airstyle`), configuration conventions, testing, and Web UI build instructions.
- **Security:** See [.github/SECURITY.md](.github/SECURITY.md) to report security vulnerabilities.

## Build Requirements

- Linux or macOS (on Apple Silicon, Rosetta 2 may be required for legacy npm tooling)
- Java 25.0.1+, 64-bit (`JAVA_HOME` must point to JDK 25)
- Docker (required for container builds and Testcontainers-based integration tests)

## Building TruffleHog

TruffleHog is a standard Maven multi-module project. Ensure `JAVA_HOME` points to JDK 25 before invoking `./mvnw` or `mvnd`.

To build only the PostHog warehouse server assembly (`trino-server-posthog`) and CLI:

```bash
./mvnw clean install \
  -pl core/trino-server-posthog,client/trino-cli -am \
  -T 1C -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true
```

To build and install the entire repository (skipping tests, Javadoc, and static checks for speed):

```bash
./mvnw clean install -T 2C -nsu -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true
```

Because the full test suite is large, run tests scoped to the modules you change:

```bash
./mvnw test -Dair.check.skip-all=true -pl <module> -Dtest=<TestClass>
```

Before opening a pull request, format Java changes and run the static checks on modified modules (`mvnd` works in place of `./mvnw`):

```bash
./mvnw -pl <changed-modules> airstyle:format
./mvnw verify -DskipTests -P ci -pl <changed-modules>
```

## Running TruffleHog Locally

### Running in IntelliJ IDEA

1. Build the project once from the command line, then open the root `pom.xml` in [IntelliJ IDEA](https://www.jetbrains.com/idea/).
2. Under **File > Project Structure**, ensure both the Project SDK and Project language level are set to **JDK 25**.
3. For individual JUnit tests in IntelliJ, set the JUnit run configuration template VM options to:
   ```text
   -ea --add-modules=jdk.incubator.vector
   ```
4. To start a lightweight development server in the IDE, run a connector `*QueryRunner` (such as `TpchQueryRunner`) or launch the full development server:
   - **Main Class:** `io.trino.server.DevelopmentServer`
   - **VM Options:** `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true --sun-misc-unsafe-memory-access=allow --add-modules jdk.incubator.vector`
   - **Working directory:** `$MODULE_DIR$` (`testing/trino-server-dev`)
   - **Use classpath of module:** `trino-server-dev`

### Running the CLI

Start the interactive TruffleHog CLI against a local coordinator:

```bash
client/trino-cli/target/trino-cli-*-executable.jar
```

Run a test query:

```sql
SELECT * FROM system.runtime.nodes;
SELECT * FROM tpch.tiny.region;
```

## License

TruffleHog is licensed under the [Apache License, Version 2.0](LICENSE).
