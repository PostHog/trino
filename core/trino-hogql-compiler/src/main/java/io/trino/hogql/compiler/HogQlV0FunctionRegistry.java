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
            rewrite("toInt", FunctionRewrite.CAST_BIGINT, "bigint", signature(1)),
            rewrite("toFloat", FunctionRewrite.CAST_DOUBLE, "double", signature(1)),
            rewrite("toString", FunctionRewrite.CAST_VARCHAR, "varchar", signature(1)),
            rewrite("toDate", FunctionRewrite.CAST_DATE, "date", signature(1)),
            rewrite("toDateTime", FunctionRewrite.CAST_TIMESTAMP, "timestamp(0)", signature(1)),
            rewrite("toStartOfMonth", FunctionRewrite.DATE_TRUNC_MONTH, "timestamp", signature(1)),
            rewrite("toStartOfDay", FunctionRewrite.DATE_TRUNC_DAY, "timestamp", signature(1)),
            rewrite("toStartOfHour", FunctionRewrite.DATE_TRUNC_HOUR, "timestamp", signature(1)),
            rewrite("toMonday", FunctionRewrite.DATE_TRUNC_WEEK, "timestamp", signature(1)),
            rewrite("multiIf", FunctionRewrite.MULTI_IF, "any", variadicSignature(3)),
            rewrite("JSONExtractString", FunctionRewrite.JSON_EXTRACT_STRING, "varchar", variadicSignature(2)),
            rewrite("JSONExtractInt", FunctionRewrite.JSON_EXTRACT_INT, "bigint", variadicSignature(2)),
            rewrite("JSONExtractFloat", FunctionRewrite.JSON_EXTRACT_FLOAT, "double", variadicSignature(2)),
            rewrite("JSONExtractRaw", FunctionRewrite.JSON_EXTRACT_RAW, "varchar", variadicSignature(2)),
            rewrite("JSONLength", FunctionRewrite.JSON_LENGTH, "bigint", signature(1), variadicSignature(2)),
            rewrite("JSONExtract", FunctionRewrite.JSON_EXTRACT_TYPED, "any", signature(2)),
            rewrite("JSONExtractKeysAndValues", FunctionRewrite.JSON_KEYS_AND_VALUES, "array(row(varchar,any))", signature(2)),
            rewrite("JSONExtractKeysAndValuesRaw", FunctionRewrite.JSON_KEYS_AND_VALUES_RAW, "array(row(varchar,varchar))", signature(1)),
            scalar("map", "map", signature(2)),
            scalar("dateDiff", "date_diff", signature(3)),
            scalar("dateAdd", "date_add", signature(3)),
            scalar("dateTrunc", "date_trunc", signature(2)),
            scalar("arraySort", "array_sort", signature(1)),
            scalar("arrayDistinct", "array_distinct", signature(1)),
            scalar("arrayFlatten", "flatten", signature(1)),
            scalar("arrayStringConcat", "array_join", signature(1), signature(2)),
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
            nondeterministicAggregate("uniq", "approx_distinct", false, signature(1)),
            nondeterministicAggregate("any", "arbitrary", false, signature(1)),
            nondeterministicAggregate("argMin", "min_by", false, signature(2)),
            nondeterministicAggregate("argMax", "max_by", false, signature(2)),
            window("rank", "rank", signature(0)),
            window("first_value", "first_value", signature(1)),
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
                false);
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
