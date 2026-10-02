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
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.parquet.variant.VariantPaths;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A step of a path into a VARIANT value: an object key, or, without a key,
 * the elements of an array.
 */
public record HoglakeVariantPathStep(@JsonProperty("key") Optional<String> key)
{
    public HoglakeVariantPathStep
    {
        requireNonNull(key, "key is null");
    }

    public static HoglakeVariantPathStep objectKey(String key)
    {
        return new HoglakeVariantPathStep(Optional.of(key));
    }

    public static HoglakeVariantPathStep arrayElement()
    {
        return new HoglakeVariantPathStep(Optional.empty());
    }

    public VariantPaths.Step toVariantPathStep()
    {
        return key.<VariantPaths.Step>map(VariantPaths.Key::new).orElseGet(VariantPaths.ArrayElement::new);
    }

    @Override
    public String toString()
    {
        return key.map(name -> "['" + name + "']").orElse("[*]");
    }
}
