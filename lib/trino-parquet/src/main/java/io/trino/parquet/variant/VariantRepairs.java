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

import io.airlift.slice.Slice;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.VariantDecoder.valueSize;

/// Repairs two defects of some Variant writers: a metadata dictionary that sets
/// `sorted_strings` but is not sorted, and object fields that are not in field name
/// order. Lookups that trust either one miss keys. [ShreddedVariantAssembler] repairs
/// the values that it reads.
final class VariantRepairs
{
    private VariantRepairs() {}

    /// Returns `metadata` without the `sorted_strings` flag if its dictionary is not
    /// sorted and unique. Some writers set the flag for every dictionary, and lookups
    /// that trust it miss keys.
    static Metadata withVerifiedSortedFlag(Metadata metadata)
    {
        if (!metadata.isSorted() || isSortedAndUnique(metadata)) {
            return metadata;
        }
        Slice copy = metadata.toSlice().copy();
        copy.setByte(0, metadataHeader(false, metadataOffsetSize(copy.getByte(0))));
        return Metadata.from(copy);
    }

    private static boolean isSortedAndUnique(Metadata metadata)
    {
        for (int id = 1; id < metadata.dictionarySize(); id++) {
            if (metadata.get(id - 1).compareTo(metadata.get(id)) >= 0) {
                return false;
            }
        }
        return true;
    }

    /// Returns `variant` with the fields of each object in field name order, as the
    /// specification requires, or `variant` itself if it is in that order. Some writers
    /// write object fields in field id order.
    ///
    /// @throws IllegalArgumentException if an object has two fields with the same name, or a
    ///         value is truncated
    static Variant withSortedObjectFields(Variant variant)
    {
        return switch (variant.basicType()) {
            case PRIMITIVE, SHORT_STRING -> {
                // A value only reads its header when it is created
                checkArgument(valueSize(variant.data(), 0) <= variant.data().length(), "Shredded VARIANT value is truncated");
                yield variant;
            }
            case ARRAY -> {
                List<Variant> elements = variant.arrayElements().collect(toImmutableList());
                List<Variant> sortedElements = null;
                for (int index = 0; index < elements.size(); index++) {
                    Variant element = elements.get(index);
                    Variant sortedElement = withSortedObjectFields(element);
                    if (sortedElements == null && sortedElement != element) {
                        sortedElements = new ArrayList<>(elements.subList(0, index));
                    }
                    if (sortedElements != null) {
                        sortedElements.add(sortedElement);
                    }
                }
                if (sortedElements == null) {
                    yield variant;
                }
                yield Variant.ofArray(sortedElements);
            }
            case OBJECT -> {
                List<ObjectFieldIdValue> fields = variant.objectFields().collect(toImmutableList());
                List<Slice> names = new ArrayList<>(fields.size());
                List<Variant> values = new ArrayList<>(fields.size());
                boolean unchanged = true;
                for (ObjectFieldIdValue field : fields) {
                    Slice name = variant.metadata().get(field.fieldId());
                    Variant value = withSortedObjectFields(field.value());
                    unchanged &= value == field.value() && (names.isEmpty() || names.getLast().compareTo(name) < 0);
                    names.add(name);
                    values.add(value);
                }
                if (unchanged) {
                    yield variant;
                }
                Map<Slice, Variant> sortedFields = new HashMap<>();
                for (int index = 0; index < names.size(); index++) {
                    Slice name = names.get(index);
                    checkArgument(sortedFields.put(name, values.get(index)) == null, "Shredded VARIANT object has duplicate field %s", name.toStringUtf8());
                }
                yield Variant.ofObject(sortedFields);
            }
        };
    }
}
