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
package io.trino.plugin.opa.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * What OPA answers when it is asked for the revision of the policy data it serves. OPA answers a
 * document request with {@code {"result": ...}}, and leaves {@code result} out entirely when the
 * document is undefined - which is what a bundle that does not define a revision looks like, and
 * is deliberately not turned into a revision here.
 */
public record OpaPolicyRevisionResult(Optional<String> result)
{
    @JsonCreator
    public OpaPolicyRevisionResult(@JsonProperty("result") Optional<String> result)
    {
        this.result = result == null ? Optional.empty() : result;
    }
}
