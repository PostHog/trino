/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.hogql.compiler;

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionRewrite;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlParser;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlFunctionResolver
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);

    private final HogQlParser parser = new HogQlParser();
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testMapsStockAndCompatibilityFunctionsToQualifiedTrinoNames()
    {
        HogQlQuery query = resolve(
                "SELECT hogUpper('one'), hogCompat('two')",
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false),
                function("hogCompat", FunctionKind.SCALAR, FunctionImplementation.UDF, List.of("ducklake", "compat", "hog_compat"), signature(1), false, false, false, false));

        assertThat(TrinoAstFactory.createStatement(query, Map.of())).isEqualTo(sqlParser.createStatement(
                "SELECT system.builtin.upper('one'), ducklake.compat.hog_compat('two')"));
    }

    @Test
    public void testCompilerPinsOnceAndResolvesUserFunctions()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlSemanticCatalogSnapshot snapshot = snapshot(List.of(
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false)));
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(snapshot);
        });

        HogQlCompilationResult result = new HogQlCompiler().compile(envelope("SELECT [hogUpper('one')][1]"), Optional.of(context));

        assertThat(pins).hasValue(1);
        assertThat(result.catalogGeneration()).hasValue(7);
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement("SELECT ARRAY[system.builtin.upper('one')][1]"));
    }

    @Test
    public void testV0CatalogFunctionsCannotExpandFrozenRegistry()
    {
        HogQlSemanticCatalogSnapshot snapshot = snapshot(List.of(
                function("manifestOnly", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("upper"), signature(1), false, false, false, false)));
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(snapshot));

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compileV0(envelope("SELECT manifestOnly('value')"), Optional.of(context)));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception).hasMessage("line 1:8: Unknown HogQL function: manifestOnly");
    }

    @Test
    public void testCompilerRewritesNullPredicatesAndPreservesPlaceholders()
    {
        HogQlSemanticCatalogSnapshot snapshot = snapshot(List.of(
                rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                rewriteFunction("hogIsNotNull", FunctionRewrite.IS_NOT_NULL)));
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(snapshot));

        HogQlCompilationResult result = new HogQlCompiler().compile(
                envelope(
                        "SELECT hogIsNull({first}), hogIsNotNull({second})",
                        Map.of(
                                "first", new HogQlTypedValue("varchar", new HogQlTypedValue.StringValue("one")),
                                "second", new HogQlTypedValue("varchar", new HogQlTypedValue.StringValue("two")))),
                Optional.of(context));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement("SELECT ? IS NULL, ? IS NOT NULL"));
        assertThat(result.parameterNames()).containsExactly("first", "second");
    }

    @Test
    public void testCompilerResolvesV0FunctionsWithoutCatalogContext()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT coalesce(NULL, 'fallback'), any(value), argMin(value, timestamp), " +
                        "arrayDistinct([1, 1]), dateDiff('day', start_time, end_time), rank() OVER (), " +
                        "nullIf(value, ''), ifNull(value, 'fallback'), trim(value), round(score, 2), " +
                        "now(), current_timestamp(), isNotNull(value), groupArray(value), uniq(value), " +
                        "toInt(value), toFloat(value), toString(value), toDate(timestamp), toDateTime(timestamp), " +
                        "toStartOfMonth(timestamp), toStartOfDay(timestamp), toStartOfHour(timestamp), toMonday(timestamp), " +
                        "countIf(active), sumIf(value, active), maxIf(value, active), uniqIf(value, active), " +
                        "uniqExact(value), groupUniqArray(value), multiIf(first, 'one', second, 'two', 'other') FROM metrics"));

        assertThat(result.catalogGeneration()).isEmpty();
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT coalesce(NULL, 'fallback'), arbitrary(value), min_by(value, timestamp), " +
                        "array_distinct(ARRAY[1, 1]), date_diff('day', start_time, end_time), rank() OVER (), " +
                        "nullif(value, ''), coalesce(value, 'fallback'), \"trim\"(value), round(score, 2), " +
                        "now(), now(), value IS NOT NULL, array_agg(value), approx_distinct(value), " +
                        "CAST(value AS bigint), CAST(value AS double), CAST(value AS varchar), CAST(timestamp AS date), CAST(timestamp AS timestamp(0)), " +
                        "date_trunc('month', timestamp), date_trunc('day', timestamp), date_trunc('hour', timestamp), date_trunc('week', timestamp), " +
                        "count(*) FILTER (WHERE active), sum(value) FILTER (WHERE active), max(value) FILTER (WHERE active), " +
                        "approx_distinct(value) FILTER (WHERE active), count(DISTINCT value), array_agg(DISTINCT value), " +
                        "CASE WHEN first THEN 'one' WHEN second THEN 'two' ELSE 'other' END FROM metrics"));
    }

    @Test
    public void testCompilerRejectsUnknownFunctionsWithoutCatalogContext()
    {
        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compile(envelope("SELECT ordinary(1)")));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception).hasMessage("line 1:8: Unknown HogQL function: ordinary");
    }

    @Test
    public void testCompilerLowersJsonExtractionFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT JSONExtractString(payload, 'name'), JSONExtractInt(payload, 'items', 0), " +
                        "JSONExtractFloat(payload, 'score'), JSONExtractRaw(payload, 'object'), " +
                        "JSONLength(payload), JSONLength(payload, 'items'), " +
                        "JSONExtract(payload_text, 'Map(String, Float64)'), " +
                        "JSONExtractKeysAndValues(payload_text, 'Float64'), JSONExtractKeysAndValuesRaw(payload_text) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT coalesce(json_extract_scalar(payload, '$[\"name\"]'), ''), " +
                        "coalesce(TRY_CAST(json_extract_scalar(payload, '$[\"items\"][0]') AS bigint), 0), " +
                        "coalesce(TRY_CAST(json_extract_scalar(payload, '$[\"score\"]') AS double), 0E0), " +
                        "coalesce(json_format(json_extract(payload, '$[\"object\"]')), ''), " +
                        "coalesce(json_size(payload, '$'), 0), coalesce(json_size(payload, '$[\"items\"]'), 0), " +
                        "coalesce(TRY_CAST(json_parse(payload_text) AS map(varchar, double)), CAST(map(ARRAY[], ARRAY[]) AS map(varchar, double))), " +
                        "map_entries(coalesce(TRY_CAST(json_parse(payload_text) AS map(varchar, double)), CAST(map(ARRAY[], ARRAY[]) AS map(varchar, double)))), " +
                        "map_entries(transform_values(coalesce(TRY_CAST(json_parse(payload_text) AS map(varchar, json)), " +
                        "CAST(map(ARRAY[], ARRAY[]) AS map(varchar, json))), (key, value) -> json_format(value))) FROM records"));

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compile(envelope("SELECT JSONExtractString(payload, dynamic_key)")));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception).hasMessageContaining("JSON path segments must be string or integer literals");
    }

    @Test
    public void testCompilerLowersDateTimeCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT today(), toIntervalDay(2), addDays(timestamp, 3), addMonths(timestamp, -2), " +
                        "dateAdd(timestamp, INTERVAL 4 DAY), fromUnixTimestamp(epoch), toUnixTimestamp(timestamp), " +
                        "toDateTime(timestamp, 'UTC'), formatDateTime(timestamp, '%Y-%m-%d'), " +
                        "toTimeZone(timestamp, 'UTC'), parseDateTimeBestEffort(text) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT CAST(now() AS date), 2 * INTERVAL '1' DAY, date_add('day', 3, timestamp), " +
                        "date_add('month', -2, timestamp), timestamp + 4 * INTERVAL '1' DAY, " +
                        "from_unixtime(epoch), CAST(to_unixtime(timestamp) AS bigint), " +
                        "with_timezone(CAST(timestamp AS timestamp(0)), 'UTC'), date_format(timestamp, '%Y-%m-%d'), " +
                        "at_timezone(timestamp, 'UTC'), TRY_CAST(text AS timestamp(3)) FROM records"));
    }

    @Test
    public void testCompilerLowersScalarCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT not(active), and(active, ready), greater(score, 10), like(name, 'pro%'), " +
                        "least(first, second), greatest(first, second), position(name, 'needle'), " +
                        "startsWith(name, 'prefix'), substring(name, 2, 4), log10(score) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT NOT active, active AND ready, score > 10, name LIKE 'pro%', " +
                        "least(first, second), greatest(first, second), strpos(name, 'needle'), " +
                        "starts_with(name, 'prefix'), substring(name, 2, 4), log10(score) FROM records"));
    }

    @Test
    public void testCompilerLowersRegexCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT extract(name, '([a-z]+)'), extract(name, '[a-z]+'), match(name, '^prefix'), " +
                        "replaceRegexpAll(name, '([a-z])', 'x_') FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT coalesce(regexp_extract(name, '([a-z]+)', 1), ''), " +
                        "coalesce(regexp_extract(name, '[a-z]+', 0), ''), regexp_like(name, '^prefix'), " +
                        "regexp_replace(name, '([a-z])', 'x_') FROM records"));
    }

    @Test
    public void testCompilerLowersExtendedRegexCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT extractAll(name, '([a-z]+)'), extractAll(name, '[a-z]+'), " +
                        "replaceRegexpOne(name, '([a-z])', 'x_') FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT regexp_extract_all(name, '([a-z]+)', 1), regexp_extract_all(name, '[a-z]+', 0), " +
                        "regexp_replace(name, '(?s)^(.*?)(([a-z]))', '$1x_') FROM records"));
    }

    @Test
    public void testCompilerLowersExtendedJsonCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT JSONExtractBool(payload, 'active'), JSONExtractUInt(payload, 'count'), " +
                        "JSONExtractArrayRaw(payload_text), JSONExtractArrayRaw(payload, 'items'), " +
                        "JSONExtractKeysAndValuesRaw(payload, 'object') FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT coalesce(TRY_CAST(json_extract_scalar(payload, '$[\"active\"]') AS boolean), false), " +
                        "coalesce(TRY_CAST(json_extract_scalar(payload, '$[\"count\"]') AS bigint), 0), " +
                        "transform(coalesce(TRY_CAST(json_parse(payload_text) AS array(json)), CAST(ARRAY[] AS array(json))), " +
                        "_hogql_json_item -> json_format(_hogql_json_item)), " +
                        "transform(coalesce(TRY_CAST(json_extract(payload, '$[\"items\"]') AS array(json)), CAST(ARRAY[] AS array(json))), " +
                        "_hogql_json_item -> json_format(_hogql_json_item)), " +
                        "map_entries(transform_values(coalesce(TRY_CAST(json_extract(payload, '$[\"object\"]') AS map(varchar, json)), " +
                        "CAST(map(ARRAY[], ARRAY[]) AS map(varchar, json))), (key, value) -> json_format(value))) FROM records"));
    }

    @Test
    public void testCompilerLowersAggregateCombinators()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT argMaxIf(value, timestamp, active), anyIf(value, active), minIf(value, active), " +
                        "avgIf(value, active), groupArrayIf(value, active), uniqExactIf(value, active), " +
                        "groupUniqArrayIf(value, active), countDistinct(value) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT max_by(value, timestamp) FILTER (WHERE active), arbitrary(value) FILTER (WHERE active), " +
                        "min(value) FILTER (WHERE active), avg(value) FILTER (WHERE active), " +
                        "array_agg(value) FILTER (WHERE active), count(DISTINCT value) FILTER (WHERE active), " +
                        "array_agg(DISTINCT value) FILTER (WHERE active), count(DISTINCT value) FROM records"));
    }

    @Test
    public void testCompilerLowersNumericConversions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT toFloatOrZero(text), toFloatOrDefault(text, 1), toDecimal(text, 4), intDiv(total, 1000), intDiv(-5, 2) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT coalesce(TRY_CAST(text AS double), 0E0), " +
                        "coalesce(TRY_CAST(text AS double), CAST(1 AS double)), " +
                        "TRY_CAST(text AS decimal(18, 4)), " +
                        "CAST(total AS bigint) / CAST(1000 AS bigint) - if(CAST(total AS bigint) % CAST(1000 AS bigint) <> 0 AND " +
                        "(CAST(total AS bigint) < 0 AND CAST(1000 AS bigint) > 0 OR CAST(total AS bigint) > 0 AND CAST(1000 AS bigint) < 0), 1, 0), " +
                        "CAST(-5 AS bigint) / CAST(2 AS bigint) - if(CAST(-5 AS bigint) % CAST(2 AS bigint) <> 0 AND " +
                        "(CAST(-5 AS bigint) < 0 AND CAST(2 AS bigint) > 0 OR CAST(-5 AS bigint) > 0 AND CAST(2 AS bigint) < 0), 1, 0) FROM records"));

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compile(envelope("SELECT toDecimal(value, scale)")));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception).hasMessageContaining("decimal scale must be an integer literal");
    }

    @Test
    public void testCompilerLowersArrayCompatibilityFunctions()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT arrayElement(items, -1), arrayFilter(x -> x > 0, items), arrayFirst(x -> x > 0, items), " +
                        "arrayMap(x -> x + 1, items), arraySum(items), range(3), range(0), tupleElement(item, 2), " +
                        "splitByChar(',', text), has(items, 3), has(items, NULL) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT element_at(items, -1), filter(items, x -> x > 0), element_at(filter(items, x -> x > 0), 1), " +
                        "transform(items, x -> x + 1), reduce(items, 0, (_hogql_sum, _hogql_item) -> _hogql_sum + _hogql_item, _hogql_sum -> _hogql_sum), " +
                        "if(3 <= 0, CAST(ARRAY[] AS array(bigint)), sequence(0, 3 - 1)), " +
                        "if(0 <= 0, CAST(ARRAY[] AS array(bigint)), sequence(0, 0 - 1)), item[2], split(text, ','), " +
                        "if(3 IS NULL, any_match(items, _hogql_item -> _hogql_item IS NULL), coalesce(contains(items, 3), false)), " +
                        "if(NULL IS NULL, any_match(items, _hogql_item -> _hogql_item IS NULL), coalesce(contains(items, NULL), false)) FROM records"));
    }

    @Test
    public void testCompilerLowersExpressionAndDateAliases()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT assumeNotNull(name), empty(name), notEmpty(name), equals(first, second), multiply(amount, 2), " +
                        "multiplyDecimal(amount, rate), divideDecimal(amount, rate), in(kind, ['a', 'b']), tuple(name, amount), " +
                        "subtractMonths(timestamp, 2), toIntervalMonth(3), toStartOfWeek(timestamp), " +
                        "splitByString('::', name), hasAny(first_array, second_array), parseDateTime(text, '%Y-%m-%d'), " +
                        "toLastDayOfMonth(timestamp), anyLast(name), lag(name, 2) OVER (), lagInFrame(name) OVER () FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT name, coalesce(length(CAST(name AS varchar)), 0) = 0, coalesce(length(CAST(name AS varchar)), 0) > 0, " +
                        "first = second, amount * 2, amount * rate, amount / rate, contains(ARRAY['a', 'b'], kind), ROW(name, amount), " +
                        "date_add('month', -(2), timestamp), 3 * INTERVAL '1' MONTH, " +
                        "date_add('day', -1, date_trunc('week', date_add('day', 1, timestamp))), split(name, '::'), " +
                        "arrays_overlap(first_array, second_array), date_parse(text, '%Y-%m-%d'), last_day_of_month(timestamp), " +
                        "arbitrary(name), lag(name, 2) OVER (), lag(name) OVER () FROM records"));
    }

    @Test
    public void testCompilerLowersRemainingScalarAliases()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope(
                "SELECT arraySlice(items, 2, 3), arrayEnumerate(items), pow(2, exponent), substringUTF8(name, 2, 3), " +
                        "arrayConcat(first_array, second_array), subtractYears(timestamp, 2), toIntOrZero(text), toUUID(uuid_text), " +
                        "toJSONString(payload), JSONHas(payload, 'key'), JSON_VALUE(payload, '$.key'), md5(name), roundBankers(score, 2), " +
                        "and(first, second, third), JSONExtractString(payload), medianIf(score, active) FROM records"));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT slice(items, 2, 3), " +
                        "if(cardinality(items) = 0, CAST(ARRAY[] AS array(bigint)), sequence(1, cardinality(items))), " +
                        "power(2, exponent), substring(name, 2, 3), concat(first_array, second_array), " +
                        "date_add('year', -(2), timestamp), coalesce(TRY_CAST(text AS bigint), 0), TRY_CAST(uuid_text AS uuid), " +
                        "json_format(CAST(payload AS json)), json_extract(payload, '$[\"key\"]') IS NOT NULL, " +
                        "json_extract_scalar(payload, '$.key'), md5(to_utf8(CAST(name AS varchar))), round(score, 2), " +
                        "(first AND second) AND third, coalesce(json_extract_scalar(payload, '$'), ''), " +
                        "approx_percentile(score, 5E-1) FILTER (WHERE active) FROM records"));
    }

    @Test
    public void testCompilerEnforcesV0FunctionArities()
    {
        assertThat(new HogQlCompiler().compile(envelope("SELECT coalesce('value')")).statement())
                .isEqualTo(sqlParser.createStatement("SELECT 'value'"));

        for (String query : List.of("SELECT if(true, 1)", "SELECT concat('value')")) {
            TrinoException exception = catchThrowableOfType(
                    TrinoException.class,
                    () -> new HogQlCompiler().compile(envelope(query)));

            assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
            assertThat(exception).hasMessageContaining("does not accept");
        }

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> new HogQlCompiler().compile(envelope("SELECT multiIf(first, 1, second, 2)")));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception).hasMessageContaining("requires condition/result pairs followed by a default value");
    }

    @ParameterizedTest
    @MethodSource("nestedFunctionQueries")
    public void testResolvesFunctionsThroughoutQueryTree(String query, String expected)
    {
        HogQlQuery resolved = resolve(
                query,
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false));

        assertThat(TrinoAstFactory.createStatement(resolved, Map.of())).isEqualTo(sqlParser.createStatement(expected));
    }

    private static Stream<Arguments> nestedFunctionQueries()
    {
        return Stream.of(
                Arguments.of(
                        "WITH cte AS (SELECT hogUpper('cte') AS value) " +
                                "SELECT hogUpper(value) FROM cte WHERE hogUpper(value) = 'x' " +
                                "GROUP BY hogUpper(value) HAVING hogUpper(value) = 'y' ORDER BY hogUpper(value)",
                        "WITH cte AS (SELECT system.builtin.upper('cte') AS value) " +
                                "SELECT system.builtin.upper(value) FROM cte WHERE system.builtin.upper(value) = 'x' " +
                                "GROUP BY system.builtin.upper(value) HAVING system.builtin.upper(value) = 'y' ORDER BY system.builtin.upper(value)"),
                Arguments.of(
                        "SELECT hogUpper('left') UNION ALL SELECT hogUpper('right')",
                        "SELECT system.builtin.upper('left') UNION ALL SELECT system.builtin.upper('right')"),
                Arguments.of(
                        "SELECT * FROM (VALUES (hogUpper('value')))",
                        "SELECT * FROM (VALUES (system.builtin.upper('value')))"),
                Arguments.of(
                        "SELECT [hogUpper('value')][hogUpper('1')]",
                        "SELECT ARRAY[system.builtin.upper('value')][system.builtin.upper('1')]"),
                Arguments.of(
                        "SELECT hogUpper('value').field",
                        "SELECT system.builtin.upper('value').field"));
    }

    @Test
    public void testAcceptsExactAndVariadicArities()
    {
        FunctionCapabilityDefinition exact = function("exact", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("exact"), signature(2), false, false, false, false);
        FunctionCapabilityDefinition variadic = function(
                "variadic",
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of("variadic"),
                new FunctionSignature(List.of("varchar", "varchar"), "varchar", true),
                false,
                false,
                false,
                false);

        resolve("SELECT exact('one', 'two'), variadic('one'), variadic('one', 'two', 'three')", exact, variadic);
    }

    @Test
    public void testMapsWindowFunctionsAndPreservesWindowSpecifications()
    {
        HogQlQuery query = resolve(
                "SELECT hogRank(value) OVER (PARTITION BY team_id ORDER BY timestamp ROWS CURRENT ROW)",
                function("hogRank", FunctionKind.WINDOW, FunctionImplementation.STOCK, List.of("analytics", "rank_value"), signature(1), false, false, false, true));

        assertThat(TrinoAstFactory.createStatement(query, Map.of())).isEqualTo(sqlParser.createStatement(
                "SELECT analytics.rank_value(value) OVER (PARTITION BY team_id ORDER BY timestamp ROWS CURRENT ROW)"));
    }

    @Test
    public void testRejectsCallsOutsideEveryDeclaredArity()
    {
        FunctionCapabilityDefinition function = function(
                "exact",
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of("exact"),
                List.of(signature(1), signature(3)),
                false,
                false,
                false,
                false);

        assertResolutionError("SELECT exact('one', 'two')", function, "HogQL function exact does not accept 2 arguments");
        assertResolutionError(
                "SELECT hogIsNull('one', 'two')",
                rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                "HogQL function hogIsNull does not accept 2 arguments");
    }

    @ParameterizedTest
    @MethodSource("unsupportedInvocationFeatures")
    public void testRejectsUnsupportedInvocationFeatures(
            String query,
            FunctionCapabilityDefinition function,
            String message)
    {
        assertUnsupportedError(query, function, message);
    }

    private static Stream<Arguments> unsupportedInvocationFeatures()
    {
        return Stream.of(
                Arguments.of(
                        "SELECT aggregate(DISTINCT value)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), false, false, false, true),
                        "HogQL function aggregate does not support DISTINCT"),
                Arguments.of(
                        "SELECT aggregate(value ORDER BY value)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), true, false, false, true),
                        "HogQL function aggregate does not support ORDER BY"),
                Arguments.of(
                        "SELECT aggregate(value) FILTER (WHERE true)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), true, true, false, true),
                        "HogQL function aggregate does not support FILTER"),
                Arguments.of(
                        "SELECT windowOnly(value)",
                        function("windowOnly", FunctionKind.WINDOW, FunctionImplementation.STOCK, List.of("window_only"), signature(1), false, false, false, true),
                        "HogQL window function windowOnly requires an OVER clause"),
                Arguments.of(
                        "SELECT aggregate(value) OVER ()",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), false, false, false, false),
                        "HogQL function aggregate does not support OVER"),
                Arguments.of(
                        "SELECT tableOnly(value)",
                        function("tableOnly", FunctionKind.TABLE, FunctionImplementation.STOCK, List.of("table_only"), signature(1), false, false, false, false),
                        "HogQL table function tableOnly cannot be used as an expression"),
                Arguments.of(
                        "SELECT hogIsNull(DISTINCT value)",
                        rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                        "HogQL function hogIsNull does not support DISTINCT"),
                Arguments.of(
                        "SELECT hogIsNull(value ORDER BY value)",
                        rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                        "HogQL function hogIsNull does not support ORDER BY"),
                Arguments.of(
                        "SELECT hogIsNull(value) FILTER (WHERE true)",
                        rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                        "HogQL function hogIsNull does not support FILTER"),
                Arguments.of(
                        "SELECT hogIsNull(value) OVER ()",
                        rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                        "HogQL function hogIsNull does not support OVER"),
                Arguments.of(
                        "SELECT hogIsNull(value) OVER () IGNORE NULLS",
                        rewriteFunction("hogIsNull", FunctionRewrite.IS_NULL),
                        "HogQL function hogIsNull does not support null treatment"));
    }

    @Test
    public void testRejectsUnknownFunctionsAtCallLocation()
    {
        assertResolutionError("SELECT missing('one')", "Unknown HogQL function: missing");
    }

    private HogQlQuery resolve(String query, FunctionCapabilityDefinition... functions)
    {
        return HogQlFunctionResolver.resolve(new PinnedSnapshot(snapshot(List.of(functions))), parser.parseStatement(query));
    }

    private void assertResolutionError(String query, FunctionCapabilityDefinition function, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query, function));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private void assertResolutionError(String query, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private void assertUnsupportedError(String query, FunctionCapabilityDefinition function, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query, function));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private static FunctionCapabilityDefinition function(
            String name,
            FunctionKind kind,
            FunctionImplementation implementation,
            List<String> trinoName,
            FunctionSignature signature,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow)
    {
        return function(name, kind, implementation, trinoName, List.of(signature), supportsDistinct, supportsOrderBy, supportsFilter, supportsWindow);
    }

    private static FunctionCapabilityDefinition function(
            String name,
            FunctionKind kind,
            FunctionImplementation implementation,
            List<String> trinoName,
            List<FunctionSignature> signatures,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow)
    {
        return new FunctionCapabilityDefinition(
                name,
                kind,
                implementation,
                trinoName.stream().map(value -> new PhysicalIdentifier(value, false)).toList(),
                signatures,
                true,
                supportsDistinct,
                supportsOrderBy,
                supportsFilter,
                supportsWindow);
    }

    private static FunctionSignature signature(int arity)
    {
        return new FunctionSignature(Stream.generate(() -> "varchar").limit(arity).toList(), "varchar", false);
    }

    private static FunctionCapabilityDefinition rewriteFunction(String name, FunctionRewrite rewrite)
    {
        return new FunctionCapabilityDefinition(
                name,
                FunctionKind.SCALAR,
                FunctionImplementation.REWRITE,
                List.of(),
                Optional.of(rewrite),
                List.of(new FunctionSignature(List.of("varchar"), "boolean", false)),
                true,
                false,
                false,
                false,
                false);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(List<FunctionCapabilityDefinition> functions)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                7,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                functions,
                List.of());
    }

    private static HogQlCompileEnvelope envelope(String query)
    {
        return envelope(query, Map.of());
    }

    private static HogQlCompileEnvelope envelope(String query, Map<String, HogQlTypedValue> parameters)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                parameters,
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.empty());
    }
}
