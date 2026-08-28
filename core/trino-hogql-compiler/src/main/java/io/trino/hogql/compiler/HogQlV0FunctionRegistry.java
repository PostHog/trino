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

    private static FunctionCapabilityDefinition aggregate(String hogQlName, String trinoName, boolean supportsDistinct, FunctionSignature... signatures)
    {
        return function(hogQlName, trinoName, FunctionKind.AGGREGATE, supportsDistinct, false, true, true, true, signatures);
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
