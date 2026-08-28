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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionRewrite;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class HogQlV0FunctionRegistry
{
    private static final Map<String, FunctionCapabilityDefinition> FUNCTIONS = functions(
            scalar("coalesce", "coalesce", variadicSignature(1)),
            scalar("if", "if", signature(3)),
            scalar("abs", "abs", signature(1)),
            scalar("lower", "lower", signature(1)),
            scalar("upper", "upper", signature(1)),
            scalar("length", "length", signature(1)),
            scalar("concat", "concat", variadicSignature(2)),
            scalar("replace", "replace", signature(3)),
            scalar("nullIf", "nullif", signature(2)),
            scalar("ifNull", "coalesce", signature(2)),
            scalar("trim", "trim", signature(1)),
            scalar("round", "round", signature(1), signature(2)),
            nondeterministicScalar("now", "now", signature(0)),
            nondeterministicScalar("current_timestamp", "now", signature(0)),
            rewrite("isNotNull", FunctionRewrite.IS_NOT_NULL, "boolean", signature(1)),
            rewrite("isNull", FunctionRewrite.IS_NULL, "boolean", signature(1)),
            rewrite("toInt", FunctionRewrite.CAST_BIGINT, "bigint", signature(1)),
            rewrite("toFloat", FunctionRewrite.CAST_DOUBLE, "double", signature(1)),
            rewrite("toFloatOrZero", FunctionRewrite.FLOAT_OR_ZERO, "double", signature(1)),
            rewrite("toFloatOrDefault", FunctionRewrite.FLOAT_OR_DEFAULT, "double", signature(2)),
            rewrite("toDecimal", FunctionRewrite.DECIMAL_CAST, "decimal", signature(2)),
            rewrite("intDiv", FunctionRewrite.INT_DIV, "bigint", signature(2)),
            rewrite("arrayElement", FunctionRewrite.ARRAY_ELEMENT, "any", signature(2)),
            rewrite("arrayFilter", FunctionRewrite.ARRAY_FILTER, "array(any)", signature(2)),
            rewrite("arrayFirst", FunctionRewrite.ARRAY_FIRST, "any", signature(2)),
            rewrite("arrayMap", FunctionRewrite.ARRAY_MAP, "array(any)", signature(2)),
            rewrite("arraySum", FunctionRewrite.ARRAY_SUM, "any", signature(1)),
            rewrite("arraySlice", FunctionRewrite.ARRAY_SLICE, "array(any)", signature(3)),
            rewrite("arrayEnumerate", FunctionRewrite.ARRAY_ENUMERATE, "array(bigint)", signature(1)),
            rewrite("range", FunctionRewrite.RANGE, "array(bigint)", signature(1), signature(2)),
            rewrite("tupleElement", FunctionRewrite.TUPLE_ELEMENT, "any", signature(2)),
            rewrite("splitByChar", FunctionRewrite.SPLIT_CHAR, "array(varchar)", signature(2)),
            rewrite("has", FunctionRewrite.HAS, "boolean", signature(2)),
            rewrite("assumeNotNull", FunctionRewrite.ASSUME_NOT_NULL, "any", signature(1)),
            rewrite("empty", FunctionRewrite.EMPTY, "boolean", signature(1)),
            rewrite("notEmpty", FunctionRewrite.NOT_EMPTY, "boolean", signature(1)),
            rewrite("equals", FunctionRewrite.EQUALS, "boolean", signature(2)),
            rewrite("plus", FunctionRewrite.PLUS, "any", signature(2)),
            rewrite("multiply", FunctionRewrite.MULTIPLY, "any", signature(2)),
            rewrite("multiplyDecimal", FunctionRewrite.MULTIPLY_DECIMAL, "decimal", signature(2)),
            rewrite("divideDecimal", FunctionRewrite.DIVIDE_DECIMAL, "decimal", signature(2)),
            rewrite("divide", FunctionRewrite.DIVIDE_DECIMAL, "any", signature(2)),
            rewrite("in", FunctionRewrite.IN_ARRAY, "boolean", signature(2)),
            rewrite("tuple", FunctionRewrite.TUPLE, "row", variadicSignature(1)),
            rewrite("subtractMonths", FunctionRewrite.SUBTRACT_MONTHS, "any", signature(2)),
            rewrite("subtractDays", FunctionRewrite.SUBTRACT_DAYS, "any", signature(2)),
            rewrite("toIntervalMonth", FunctionRewrite.INTERVAL_MONTH, "interval year to month", signature(1)),
            rewrite("toStartOfWeek", FunctionRewrite.START_WEEK, "timestamp", signature(1), signature(2)),
            rewrite("subtractYears", FunctionRewrite.SUBTRACT_YEARS, "any", signature(2)),
            rewrite("toIntOrZero", FunctionRewrite.INT_OR_ZERO, "bigint", signature(1)),
            rewrite("_toInt16", FunctionRewrite.CAST_SMALLINT, "smallint", signature(1)),
            rewrite("toUUID", FunctionRewrite.CAST_UUID, "uuid", signature(1)),
            rewrite("toJSONString", FunctionRewrite.TO_JSON_STRING, "varchar", signature(1)),
            rewrite("JSONHas", FunctionRewrite.JSON_HAS, "boolean", signature(2)),
            rewrite("JSONExtractKeys", FunctionRewrite.JSON_EXTRACT_KEYS, "array(varchar)", signature(1), variadicSignature(2)),
            rewrite("JSON_VALUE", FunctionRewrite.JSON_VALUE, "varchar", signature(2)),
            rewrite("getSurveyResponse", FunctionRewrite.SURVEY_RESPONSE, "varchar", signature(2)),
            rewrite("md5", FunctionRewrite.MD5, "varbinary", signature(1)),
            rewrite("date_part", FunctionRewrite.DATE_PART, "bigint", signature(2)),
            rewrite("minus", FunctionRewrite.MINUS, "any", signature(2)),
            rewrite("notEquals", FunctionRewrite.NOT_EQUALS, "boolean", signature(2)),
            rewrite("splitByString", FunctionRewrite.SPLIT_STRING, "array(varchar)", signature(2)),
            rewrite("toString", FunctionRewrite.CAST_VARCHAR, "varchar", signature(1)),
            rewrite("toDate", FunctionRewrite.CAST_DATE, "date", signature(1)),
            rewrite("toDateTime", FunctionRewrite.CAST_TIMESTAMP, "timestamp(0)", signature(1), signature(2)),
            rewrite("toStartOfMonth", FunctionRewrite.DATE_TRUNC_MONTH, "timestamp", signature(1)),
            rewrite("toStartOfDay", FunctionRewrite.DATE_TRUNC_DAY, "timestamp", signature(1)),
            rewrite("toStartOfHour", FunctionRewrite.DATE_TRUNC_HOUR, "timestamp", signature(1)),
            rewrite("toMonday", FunctionRewrite.DATE_TRUNC_WEEK, "timestamp", signature(1)),
            rewrite("multiIf", FunctionRewrite.MULTI_IF, "any", variadicSignature(3)),
            rewrite("JSONExtractString", FunctionRewrite.JSON_EXTRACT_STRING, "varchar", signature(1), variadicSignature(2)),
            rewrite("JSONExtractInt", FunctionRewrite.JSON_EXTRACT_INT, "bigint", variadicSignature(2)),
            rewrite("JSONExtractFloat", FunctionRewrite.JSON_EXTRACT_FLOAT, "double", variadicSignature(2)),
            rewrite("JSONExtractBool", FunctionRewrite.JSON_EXTRACT_BOOL, "boolean", variadicSignature(2)),
            rewrite("JSONExtractUInt", FunctionRewrite.JSON_EXTRACT_UINT, "bigint", variadicSignature(2)),
            rewrite("JSONExtractArrayRaw", FunctionRewrite.JSON_EXTRACT_ARRAY_RAW, "array(varchar)", signature(1), variadicSignature(2)),
            rewrite("JSONExtractRaw", FunctionRewrite.JSON_EXTRACT_RAW, "varchar", variadicSignature(2)),
            rewrite("JSONLength", FunctionRewrite.JSON_LENGTH, "bigint", signature(1), variadicSignature(2)),
            rewrite("JSONExtract", FunctionRewrite.JSON_EXTRACT_TYPED, "any", signature(2)),
            rewrite("JSONExtractKeysAndValues", FunctionRewrite.JSON_KEYS_AND_VALUES, "array(row(varchar,any))", signature(2)),
            rewrite("JSONExtractKeysAndValuesRaw", FunctionRewrite.JSON_KEYS_AND_VALUES_RAW, "array(row(varchar,varchar))", signature(1), variadicSignature(2)),
            rewrite("today", FunctionRewrite.TODAY, "date", signature(0)),
            rewrite("toIntervalDay", FunctionRewrite.INTERVAL_DAY, "interval day to second", signature(1)),
            rewrite("addDays", FunctionRewrite.ADD_DAYS, "any", signature(2)),
            rewrite("addMonths", FunctionRewrite.ADD_MONTHS, "any", signature(2)),
            rewrite("dateAdd", FunctionRewrite.DATE_ADD, "any", signature(2), signature(3)),
            rewrite("toUnixTimestamp", FunctionRewrite.TO_UNIX_TIMESTAMP, "bigint", signature(1)),
            rewrite("parseDateTimeBestEffort", FunctionRewrite.PARSE_TIMESTAMP, "timestamp(3)", signature(1)),
            rewrite("not", FunctionRewrite.NOT, "boolean", signature(1)),
            rewrite("and", FunctionRewrite.AND, "boolean", signature(2), variadicSignature(2)),
            rewrite("greater", FunctionRewrite.GREATER, "boolean", signature(2)),
            rewrite("greaterOrEquals", FunctionRewrite.GREATER_OR_EQUAL, "boolean", signature(2)),
            rewrite("lessOrEquals", FunctionRewrite.LESS_OR_EQUAL, "boolean", signature(2)),
            rewrite("like", FunctionRewrite.LIKE, "boolean", signature(2)),
            rewrite("extract", FunctionRewrite.REGEX_EXTRACT, "varchar", signature(2)),
            rewrite("replaceRegexpAll", FunctionRewrite.REGEX_REPLACE_ALL, "varchar", signature(3)),
            rewrite("replaceRegexpOne", FunctionRewrite.REGEX_REPLACE_ONE, "varchar", signature(3)),
            rewrite("extractAll", FunctionRewrite.REGEX_EXTRACT_ALL, "array(varchar)", signature(2)),
            scalar("fromUnixTimestamp", "from_unixtime", signature(1)),
            scalar("formatDateTime", "date_format", signature(2)),
            scalar("toTimeZone", "at_timezone", signature(2)),
            scalar("least", "least", variadicSignature(1)),
            scalar("greatest", "greatest", variadicSignature(1)),
            scalar("position", "strpos", signature(2)),
            scalar("startsWith", "starts_with", signature(2)),
            scalar("substring", "substring", signature(2), signature(3)),
            scalar("log10", "log10", signature(1)),
            scalar("exp", "exp", signature(1)),
            scalar("match", "regexp_like", signature(2)),
            scalar("floor", "floor", signature(1)),
            scalar("toDayOfMonth", "day", signature(1)),
            scalar("toDayOfWeek", "day_of_week", signature(1)),
            scalar("mapFromArrays", "map", signature(2)),
            scalar("mapUpdate", "map_concat", signature(2)),
            scalar("toMonth", "month", signature(1)),
            scalar("toYear", "year", signature(1)),
            scalar("ceil", "ceiling", signature(1)),
            scalar("pow", "power", signature(2)),
            scalar("substringUTF8", "substring", signature(2), signature(3)),
            scalar("arrayConcat", "concat", variadicSignature(2)),
            scalar("roundBankers", "round", signature(1), signature(2)),
            scalar("cityHash64", "cityhash64", signature(1)),
            scalar("formatReadableTimeDelta", "format_readable_time_delta", signature(1), signature(2), signature(3)),
            scalar("hasAny", "arrays_overlap", signature(2)),
            scalar("parseDateTime", "date_parse", signature(2)),
            scalar("toLastDayOfMonth", "last_day_of_month", signature(1)),
            rewrite("map", FunctionRewrite.MAP_CONSTRUCTOR, "map(any,any)", signature(0), variadicSignature(2)),
            scalar("dateDiff", "date_diff", signature(3)),
            scalar("date_diff", "date_diff", signature(3)),
            scalar("dateTrunc", "date_trunc", signature(2)),
            rewrite("arraySort", FunctionRewrite.ARRAY_SORT, "array(any)", signature(1), signature(2)),
            scalar("arrayMin", "array_min", signature(1)),
            scalar("arrayDistinct", "array_distinct", signature(1)),
            scalar("arrayFlatten", "flatten", signature(1)),
            scalar("arrayStringConcat", "array_join", signature(1), signature(2)),
            scalar("replaceAll", "replace", signature(3)),
            aggregate("count", "count", true, signature(0), signature(1)),
            aggregate("sum", "sum", true, signature(1)),
            aggregate("min", "min", true, signature(1)),
            aggregate("max", "max", true, signature(1)),
            aggregate("avg", "avg", true, signature(1)),
            aggregateWithOrderBy("array_agg", "array_agg", true, signature(1)),
            aggregateWithOrderBy("groupArray", "array_agg", false, signature(1)),
            aggregateRewrite("countIf", FunctionRewrite.COUNT_IF, "bigint", signature(1)),
            aggregateRewrite("sumIf", FunctionRewrite.SUM_IF, "any", signature(2)),
            aggregateRewrite("maxIf", FunctionRewrite.MAX_IF, "any", signature(2)),
            aggregateRewrite("uniqIf", FunctionRewrite.UNIQ_IF, "bigint", signature(2)),
            aggregateRewrite("uniqExact", FunctionRewrite.UNIQ_EXACT, "bigint", signature(1)),
            aggregateRewrite("groupUniqArray", FunctionRewrite.GROUP_UNIQ_ARRAY, "array(any)", signature(1)),
            aggregateRewrite("argMaxIf", FunctionRewrite.ARG_MAX_IF, "any", signature(3)),
            aggregateRewrite("argMinIf", FunctionRewrite.ARG_MIN_IF, "any", signature(3)),
            aggregateRewrite("anyIf", FunctionRewrite.ANY_IF, "any", signature(2)),
            aggregateRewrite("minIf", FunctionRewrite.MIN_IF, "any", signature(2)),
            aggregateRewrite("avgIf", FunctionRewrite.AVG_IF, "double", signature(2)),
            aggregateRewrite("groupArrayIf", FunctionRewrite.GROUP_ARRAY_IF, "array(any)", signature(2), signature(3)),
            aggregateRewrite("quantile", FunctionRewrite.QUANTILE, "any", signature(2)),
            aggregateRewrite("quantileExact", FunctionRewrite.QUANTILE_EXACT, "any", signature(2)),
            aggregateRewrite("quantileIf", FunctionRewrite.QUANTILE_IF, "any", signature(3)),
            aggregateRewrite("uniqExactIf", FunctionRewrite.UNIQ_EXACT_IF, "bigint", signature(2)),
            aggregateRewrite("groupUniqArrayIf", FunctionRewrite.GROUP_UNIQ_ARRAY_IF, "array(any)", signature(2)),
            aggregateRewrite("countDistinct", FunctionRewrite.COUNT_DISTINCT, "bigint", signature(1)),
            aggregateRewrite("medianIf", FunctionRewrite.MEDIAN_IF, "double", signature(2)),
            nondeterministicAggregate("uniq", "approx_distinct", false, signature(1)),
            nondeterministicAggregate("any", "arbitrary", false, signature(1)),
            nondeterministicAggregate("argMin", "min_by", false, signature(2)),
            nondeterministicAggregate("argMax", "max_by", false, signature(2)),
            nondeterministicAggregate("anyLast", "arbitrary", false, signature(1)),
            window("rank", "rank", signature(0)),
            window("first_value", "first_value", signature(1)),
            window("lag", "lag", signature(1), signature(2), signature(3)),
            window("lagInFrame", "lag", signature(1), signature(2), signature(3)),
            window("row_number", "row_number", signature(0)));

    private HogQlV0FunctionRegistry() {}

    public static Map<String, FunctionCapabilityDefinition> functions()
    {
        return FUNCTIONS;
    }

    private static Map<String, FunctionCapabilityDefinition> functions(FunctionCapabilityDefinition... functions)
    {
        Map<String, FunctionCapabilityDefinition> result = new LinkedHashMap<>();
        for (FunctionCapabilityDefinition function : functions) {
            FunctionCapabilityDefinition previous = result.put(function.name().toLowerCase(java.util.Locale.ENGLISH), function);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate HogQL v0 function");
            }
        }
        return Map.copyOf(result);
    }

    private static FunctionCapabilityDefinition scalar(String hogQlName, String trinoName, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.SCALAR, false, false, false, false, true, signatures);
    }

    private static FunctionCapabilityDefinition nondeterministicScalar(String hogQlName, String trinoName, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.SCALAR, false, false, false, false, false, signatures);
    }

    private static FunctionCapabilityDefinition rewrite(String hogQlName, FunctionRewrite rewrite, String returnType, FunctionSignature... signatures)
    {
        return new FunctionCapabilityDefinition(
                hogQlName,
                FunctionKind.SCALAR,
                FunctionImplementation.REWRITE,
                List.of(),
                java.util.Optional.of(rewrite),
                java.util.Arrays.stream(signatures)
                        .map(signature -> new FunctionSignature(signature.argumentTypes(), returnType, signature.variadic()))
                        .toList(),
                true,
                false,
                false,
                false,
                false);
    }

    private static FunctionCapabilityDefinition aggregate(String hogQlName, String trinoName, boolean supportsDistinct, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.AGGREGATE, supportsDistinct, false, true, true, true, signatures);
    }

    private static FunctionCapabilityDefinition aggregateRewrite(String hogQlName, FunctionRewrite rewrite, String returnType, FunctionSignature... signatures)
    {
        return new FunctionCapabilityDefinition(
                hogQlName,
                FunctionKind.AGGREGATE,
                FunctionImplementation.REWRITE,
                List.of(),
                java.util.Optional.of(rewrite),
                java.util.Arrays.stream(signatures)
                        .map(signature -> new FunctionSignature(signature.argumentTypes(), returnType, signature.variadic()))
                        .toList(),
                true,
                false,
                false,
                false,
                true);
    }

    private static FunctionCapabilityDefinition aggregateWithOrderBy(String hogQlName, String trinoName, boolean supportsDistinct, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.AGGREGATE, supportsDistinct, true, true, true, true, signatures);
    }

    private static FunctionCapabilityDefinition nondeterministicAggregate(String hogQlName, String trinoName, boolean supportsDistinct, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.AGGREGATE, supportsDistinct, false, true, true, false, signatures);
    }

    private static FunctionCapabilityDefinition window(String hogQlName, String trinoName, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.WINDOW, false, false, false, true, true, signatures);
    }

    private static FunctionCapabilityDefinition function(
            String hogQlName,
            String trinoName,
            FunctionKind kind,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow,
            boolean deterministic,
            FunctionSignature... signatures)
    {
        return new FunctionCapabilityDefinition(
                hogQlName,
                kind,
                FunctionImplementation.STOCK,
                List.of(new PhysicalIdentifier(trinoName, false)),
                List.of(signatures),
                deterministic,
                supportsDistinct,
                supportsOrderBy,
                supportsFilter,
                supportsWindow);
    }

    private static FunctionSignature signature(int arity)
    {
        return new FunctionSignature(java.util.stream.Stream.generate(() -> "any").limit(arity).toList(), "any", false);
    }

    private static FunctionSignature variadicSignature(int minimumArity)
    {
        return new FunctionSignature(java.util.stream.Stream.generate(() -> "any").limit(minimumArity + 1L).toList(), "any", true);
    }
}
