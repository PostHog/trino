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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record HogQlModifierBinding(String modifierName, Optional<List<PhysicalIdentifier>> sessionProperty, HogQlTypedValue value)
{
    public HogQlModifierBinding
    {
        modifierName = requireNonNull(modifierName, "modifierName is null");
        if (modifierName.isBlank()) {
            throw new IllegalArgumentException("modifier name is empty");
        }
        sessionProperty = requireNonNull(sessionProperty, "sessionProperty is null")
                .map(property -> List.copyOf(requireNonNull(property, "session property is null")));
        if (sessionProperty.isPresent() && sessionProperty.orElseThrow().isEmpty()) {
            throw new IllegalArgumentException("session property name is empty");
        }
        value = requireNonNull(value, "value is null");
    }
}
