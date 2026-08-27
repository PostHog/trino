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
package io.trino.hogql;

import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.hogql.HogQlCompilationEvent.Outcome.SUCCESS;
import static java.util.Objects.requireNonNull;

public record HogQlCompilationEvent(
        Dimensions dimensions,
        Outcome outcome,
        Optional<Phase> failedPhase,
        long totalNanos,
        Map<Phase, Long> phaseNanos)
{
    public HogQlCompilationEvent
    {
        dimensions = requireNonNull(dimensions, "dimensions is null");
        outcome = requireNonNull(outcome, "outcome is null");
        failedPhase = requireNonNull(failedPhase, "failedPhase is null");
        if (outcome == SUCCESS && failedPhase.isPresent()) {
            throw new IllegalArgumentException("successful compilation has a failed phase");
        }
        if (totalNanos < 0) {
            throw new IllegalArgumentException("totalNanos is negative");
        }
        phaseNanos = Map.copyOf(requireNonNull(phaseNanos, "phaseNanos is null"));
        if (phaseNanos.values().stream().anyMatch(duration -> duration == null || duration < 0)) {
            throw new IllegalArgumentException("phaseNanos contains an invalid duration");
        }
    }

    public enum Phase
    {
        COMPILATION,
        PARSE,
        BIND,
        LOWER,
        PARAMETER_BINDING,
    }

    public enum Outcome
    {
        SUCCESS,
        USER_ERROR,
        INTERNAL_ERROR,
        EXTERNAL_ERROR,
        INSUFFICIENT_RESOURCES,
    }

    public record Dimensions(
            int protocolVersion,
            HogQlLanguageVersion languageVersion,
            int parameterCount,
            int variableCount,
            int filterCount,
            int modifierCount,
            OptionalLong catalogGeneration)
    {
        public Dimensions
        {
            if (protocolVersion <= 0) {
                throw new IllegalArgumentException("protocolVersion must be positive");
            }
            languageVersion = requireNonNull(languageVersion, "languageVersion is null");
            if (parameterCount < 0 || variableCount < 0 || filterCount < 0 || modifierCount < 0) {
                throw new IllegalArgumentException("semantic field count is negative");
            }
            catalogGeneration = requireNonNull(catalogGeneration, "catalogGeneration is null");
            if (catalogGeneration.isPresent() && catalogGeneration.orElseThrow() <= 0) {
                throw new IllegalArgumentException("catalogGeneration must be positive");
            }
        }

        public static Dimensions fromEnvelope(HogQlCompileEnvelope envelope)
        {
            requireNonNull(envelope, "envelope is null");
            return new Dimensions(
                    envelope.protocolVersion(),
                    envelope.languageVersion(),
                    envelope.parameters().size(),
                    envelope.variables().size(),
                    envelope.filters().size(),
                    envelope.modifiers().size(),
                    envelope.catalogGeneration());
        }
    }
}
