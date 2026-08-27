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
package io.trino.execution;

import io.trino.hogql.compiler.HogQlCompileEnvelope;

import java.util.Optional;

import static io.trino.execution.QueryLanguage.HOGQL;
import static io.trino.execution.QueryLanguage.TRINO;
import static java.util.Objects.requireNonNull;

public record QuerySubmission(QueryLanguage language, String originalText, Optional<HogQlCompileEnvelope> hogQlEnvelope)
{
    public QuerySubmission
    {
        requireNonNull(language, "language is null");
        requireNonNull(originalText, "originalText is null");
        hogQlEnvelope = requireNonNull(hogQlEnvelope, "hogQlEnvelope is null");
        if (language == TRINO && hogQlEnvelope.isPresent()) {
            throw new IllegalArgumentException("Trino submission has a HogQL envelope");
        }
        if (language == HOGQL && (hogQlEnvelope.isEmpty() || !hogQlEnvelope.orElseThrow().query().equals(originalText))) {
            throw new IllegalArgumentException("HogQL submission envelope does not match its query");
        }
    }

    public static QuerySubmission trino(String originalText)
    {
        return new QuerySubmission(TRINO, originalText, Optional.empty());
    }

    public static QuerySubmission hogQl(HogQlCompileEnvelope envelope)
    {
        requireNonNull(envelope, "envelope is null");
        return new QuerySubmission(HOGQL, envelope.query(), Optional.of(envelope));
    }
}
