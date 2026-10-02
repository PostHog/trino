# Monitoring with JMX

Trino exposes a large number of different metrics via the Java Management Extensions (JMX).

You have to enable JMX by setting the ports used by the RMI registry and server
in the {ref}`config.properties file <config-properties>`:

```text
jmx.rmiregistry.port=9080
jmx.rmiserver.port=9081
```

- `jmx.rmiregistry.port`:
  Specifies the port for the JMX RMI registry. JMX clients should connect to this port.
- `jmx.rmiserver.port`:
  Specifies the port for the JMX RMI server. Trino exports many metrics,
  that are useful for monitoring via JMX.

Additionally configure a Java system property in the
[jvm.config](jvm-config) with the RMI server port:

```properties
-Dcom.sun.management.jmxremote.rmi.port=9081
```

JConsole (supplied with the JDK), [VisualVM](https://visualvm.github.io/), and
many other tools can be used to access the metrics in a client application.
Many monitoring solutions support JMX. You can also use the
{doc}`/connector/jmx` and query the metrics using SQL.

Many of these JMX metrics are a complex metric object such as a `CounterStat`
that has a collection of related metrics. For example, `InputPositions` has
`InputPositions.TotalCount`, `InputPositions.OneMinute.Count`, and so on.

A small subset of the available metrics are described below.

## JVM

- Heap size: `java.lang:type=Memory:HeapMemoryUsage.used`
- Thread count: `java.lang:type=Threading:ThreadCount`

## Trino cluster and nodes

- Active nodes:
  `trino.failuredetector:name=HeartbeatFailureDetector:ActiveCount`
- Free memory (general pool):
  `trino.memory:type=ClusterMemoryPool:name=general:FreeDistributedBytes`
- Cumulative count (since Trino started) of queries that ran out of memory and were killed:
  `trino.memory:name=ClusterMemoryManager:QueriesKilledDueToOutOfMemory`

## Trino queries

- Active queries currently executing or queued: `trino.execution:name=QueryManager:RunningQueries`
- Queries started: `trino.execution:name=QueryManager:StartedQueries.FiveMinute.Count`
- Failed queries from last 5 min (all): `trino.execution:name=QueryManager:FailedQueries.FiveMinute.Count`
- Failed queries from last 5 min (internal): `trino.execution:name=QueryManager:InternalFailures.FiveMinute.Count`
- Failed queries from last 5 min (external): `trino.execution:name=QueryManager:ExternalFailures.FiveMinute.Count`
- Failed queries (user): `trino.execution:name=QueryManager:UserErrorFailures.FiveMinute.Count`
- Execution latency (P50): `trino.execution:name=QueryManager:ExecutionTime.FiveMinutes.P50`
- Input data rate (P90): `trino.execution:name=QueryManager:WallInputBytesRate.FiveMinutes.P90`
- Queries waiting for resources: `trino.execution:name=QueryManager:WaitingForResourcesQueries`
- Longest wait for resources (seconds): `trino.execution:name=QueryManager:WaitingForResourcesMaxAgeInSeconds`

### Infrastructure failures by user and error code

Coordinators also export cumulative infrastructure query failures as
`trino.execution:name=QueryFailureStats,user=<user>,errorCode=<error-code>,errorType=<error-type>,overflow=false:TotalCount`.
These counters increment at query completion for `INTERNAL_ERROR`, `EXTERNAL`,
and `INSUFFICIENT_RESOURCES` failures. The `user` property is the authorized
session user, as reported by the query API. It can differ from the authenticating
principal when impersonation is enabled. The counters contain no organization
UUID, query ID, SQL text, exception message, or file path.

Counters are created after the first failure for each combination and reset when
the coordinator restarts. Export at most 4096 distinct user/error-code/error-type
combinations per coordinator. Further combinations increment up to three
additional counters with `user=__other__`, `errorCode=__other__`, and
`overflow=true`, preserving the error type. Existing combinations continue to
increment their original counters. Overflow counters must remain in aggregate
failure alerts because their tenant and error-code attribution is unavailable.

The existing `QueryManager` failure counters remain unchanged. Because each
series starts at its first failure, rate calculations cannot reconstruct failures
that precede its first scrape. Retain the aggregate counters for total failure
coverage. JMX exporter rules can use the new MBean properties as labels without
polling the query API or executing SQL.

## Trino tasks

- Input data bytes: `trino.execution:name=SqlTaskManager:InputDataSize.FiveMinute.Count`
- Input rows: `trino.execution:name=SqlTaskManager:InputPositions.FiveMinute.Count`

## Connectors

Many connectors provide their own metrics. The metric names typically start with
`trino.plugin`.
