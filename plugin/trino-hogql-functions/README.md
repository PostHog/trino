# HogQL compatibility functions

This optional plugin supplies the `hogql_*` functions used by the HogQL Trino printer.
It contains mathematical, geographic, H3, bitmap, encoding, HTML-text, and IPv6 functions.
It also supplies bitmap aggregation functions.

The plugin is not included in the default server distribution. Installing it does not
update or enable a HogQL compiler. The compiler must emit the matching function names
and argument types.

## Build and test

Use the JDK specified by the root project. Build this plugin from the same revision as
the target Trino server. Trino does not guarantee SPI compatibility across versions.

From the repository root, run:

```bash
./mvnw -pl plugin/trino-hogql-functions -am install -DskipTests
./mvnw -pl plugin/trino-hogql-functions verify
./mvnw -pl plugin/trino-hogql-functions -am validate
```

The tests load the plugin into a local query runner. They check function registration,
representative SQL results, grouped and empty aggregations, invalid inputs, and H3 native
library loading. They do not establish complete ClickHouse equivalence.

## Install and remove

1. Build the plugin ZIP in `plugin/trino-hogql-functions/target/`.
2. Extract the ZIP into an empty plugin directory on every coordinator and worker.
   Put the JAR files directly under `plugin/hogql-functions/`, not inside another directory.
   Include the dependency JAR files. Do not copy only the main JAR.
3. Restart Trino and run these checks:

```sql
SELECT hogql_plugin_version();
SHOW FUNCTIONS LIKE 'hogql_%';
SELECT hogql_bitmap_to_array(hogql_bitmap_build(ARRAY[BIGINT '3', 1, 3]));
SELECT hogql_h3_is_valid(hogql_geo_to_h3(37.775, -122.418, 9));
```

The bitmap check returns `[1, 3]`. The H3 check returns `1`.
Test representative queries in a test cluster before installing the plugin elsewhere.
To remove the plugin, first stop queries that use its functions. Remove the plugin
directory from every node and restart Trino. Do not change a running node's plugin files.

## Compatibility limits

- Function names are prefixed with `hogql_`; the plugin does not replace built-in functions.
- The H3 dependency includes native code. The operating system and architecture must be
  supported by H3 Java 4.5.0. Its native library must load on every node.
  Set `--enable-native-access=ALL-UNNAMED` in the JVM configuration to permit native access
  explicitly. Otherwise, JDK 25 reports a warning when H3 loads its native library.
- `hogql_geo_to_h3` takes latitude, longitude, and resolution. Coordinate-returning H3
  functions return latitude before longitude. The compiler must account for source settings
  that change coordinate order.
- Bitmap values use this plugin's private `VARBINARY` format: sorted, distinct unsigned
  64-bit bit patterns stored as little-endian words. They are not ClickHouse serialized
  `AggregateFunction` states or Roaring bitmaps. Do not import ClickHouse bitmap bytes.
  Treat these states as intermediate query values, not a stable interchange format.
- Trino exposes bitmap members as signed `BIGINT` values. Values with the high bit set
  retain their bit pattern but display as negative numbers. Ordering uses unsigned comparison.
- Bitmap aggregation currently decodes and encodes the state for each input. Large groups
  can be expensive. This implementation is not intended for high-cardinality bitmap workloads.
- H3 neighborhood, child, and line outputs and geohash grids have an item limit of 1,000,000.
  The child and ring checks use conservative upper bounds. Requests can fail before reaching
  that many actual output items.
- Geographic distance inputs must be finite and within longitude and latitude bounds.
  IP parsing accepts literals only and performs no DNS lookup.
- Mathematical and geographic results use floating-point arithmetic. The HTML-text
  function uses the prototype's text-removal rules, not a full HTML parser.
  Binary string outputs require care because Trino `VARCHAR` expects UTF-8.
- This plugin does not add CityHash, general aggregate-state compatibility, or compiler
  rewrites for unsupported query syntax. A successful plugin check does not establish full
  HogQL compatibility.

Keep installation opt-in until query-level comparisons establish the required result
equivalence and resource usage for the intended workload.
