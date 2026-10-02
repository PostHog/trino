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
package io.trino.parquet.variant;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/// The parts of a VARIANT value that a query reads, as a tree of object keys
/// and array elements. A node that the query reads as a whole has no children.
///
/// An array element step stands for every element of an array: a reader keeps
/// all elements, so that the array keeps its length.
public final class VariantPaths
{
    private static final VariantPaths WHOLE = new VariantPaths(true, ImmutableMap.of(), Optional.empty());

    private final boolean whole;
    private final Map<String, VariantPaths> keys;
    private final Optional<VariantPaths> elements;

    private VariantPaths(boolean whole, Map<String, VariantPaths> keys, Optional<VariantPaths> elements)
    {
        this.whole = whole;
        this.keys = ImmutableMap.copyOf(keys);
        this.elements = requireNonNull(elements, "elements is null");
    }

    /// The tree of the given paths. Each path has at least one step.
    public static VariantPaths of(List<List<Step>> paths)
    {
        checkArgument(!paths.isEmpty(), "paths is empty");
        Builder root = new Builder();
        for (List<Step> path : paths) {
            checkArgument(!path.isEmpty(), "path is empty");
            Builder node = root;
            for (Step step : path) {
                node = switch (step) {
                    case Key(String name) -> node.keys.computeIfAbsent(name, _ -> new Builder());
                    case ArrayElement _ -> {
                        if (node.elements == null) {
                            node.elements = new Builder();
                        }
                        yield node.elements;
                    }
                };
            }
            node.whole = true;
        }
        return root.build();
    }

    /// Whether the query reads this node as a whole, so it has no children.
    public boolean whole()
    {
        return whole;
    }

    /// The paths below each object key that the query reads.
    public Map<String, VariantPaths> keys()
    {
        return keys;
    }

    /// The paths below the elements of an array, if the query reads them.
    public Optional<VariantPaths> elements()
    {
        return elements;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof VariantPaths that &&
                whole == that.whole &&
                keys.equals(that.keys) &&
                elements.equals(that.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whole, keys, elements);
    }

    @Override
    public String toString()
    {
        if (whole) {
            return "*";
        }
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
        keys.forEach((name, paths) -> joiner.add(name + ": " + paths));
        elements.ifPresent(paths -> joiner.add("[]: " + paths));
        return joiner.toString();
    }

    public sealed interface Step
            permits ArrayElement,
                    Key {}

    /// The value of an object key.
    public record Key(String name)
            implements Step
    {
        public Key
        {
            requireNonNull(name, "name is null");
        }
    }

    /// The elements of an array.
    public record ArrayElement()
            implements Step {}

    private static final class Builder
    {
        private boolean whole;
        private final Map<String, Builder> keys = new HashMap<>();
        private Builder elements;

        private VariantPaths build()
        {
            // A node that the query reads as a whole needs none of its parts
            if (whole) {
                return WHOLE;
            }
            return new VariantPaths(
                    false,
                    keys.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build())),
                    Optional.ofNullable(elements).map(Builder::build));
        }
    }
}
