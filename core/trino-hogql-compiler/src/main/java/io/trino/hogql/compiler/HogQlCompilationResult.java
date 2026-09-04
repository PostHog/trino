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

import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record HogQlCompilationResult(
        Statement statement,
        List<String> parameterNames,
        List<HogQlModifierBinding> modifierBindings,
        OptionalLong catalogGeneration,
        OptionalLong exchangeRateGeneration)
{
    public HogQlCompilationResult
    {
        statement = requireNonNull(statement, "statement is null");
        parameterNames = List.copyOf(requireNonNull(parameterNames, "parameterNames is null"));
        modifierBindings = List.copyOf(requireNonNull(modifierBindings, "modifierBindings is null"));
        catalogGeneration = requireNonNull(catalogGeneration, "catalogGeneration is null");
        exchangeRateGeneration = requireNonNull(exchangeRateGeneration, "exchangeRateGeneration is null");
    }

    public HogQlCompilationResult(Statement statement, List<String> parameterNames, OptionalLong catalogGeneration)
    {
        this(statement, parameterNames, List.of(), catalogGeneration, OptionalLong.empty());
    }

    public HogQlCompilationResult(Statement statement, List<String> parameterNames, List<HogQlModifierBinding> modifierBindings, OptionalLong catalogGeneration)
    {
        this(statement, parameterNames, modifierBindings, catalogGeneration, OptionalLong.empty());
    }
}
