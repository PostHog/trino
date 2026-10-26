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
import io.trino.spi.variant.Header;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.metadataVersion;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.util.Objects.requireNonNull;

/// Finds fields of encoded Variant objects by name. It trusts neither the order of the
/// fields of an object nor the `sorted_strings` flag of the dictionary, because some
/// writers, such as DuckDB 1.5.5, write fields in field id order and mark unsorted
/// dictionaries as sorted. A lookup reads each field id of the object once and compares
/// the names in the dictionary in place, so it neither decodes nor copies the object.
///
/// A lookup reports an object that has a field with a requested name twice, which the
/// specification does not allow. It does not check the other fields of the object.
final class VariantResidualLookup
{
    private Slice metadata;
    private int offsetSize;
    private int dictionarySize;
    private int offsetsStart;
    private int namesStart;

    /// Sets the metadata of the values that the next lookups read.
    ///
    /// @throws IllegalArgumentException if the metadata is invalid
    public void reset(Slice metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        checkArgument(metadata.length() >= 2, "VARIANT metadata is truncated");
        byte header = metadata.getByte(0);
        checkArgument(metadataVersion(header) == Header.VERSION, "Unsupported VARIANT metadata version: %s", metadataVersion(header));
        offsetSize = metadataOffsetSize(header);
        checkArgument(metadata.length() >= 1 + offsetSize, "VARIANT metadata is truncated");
        dictionarySize = readOffset(metadata, 1, offsetSize);
        offsetsStart = 1 + offsetSize;
        long namesStart = offsetsStart + (dictionarySize + 1L) * offsetSize;
        checkArgument(dictionarySize >= 0 && namesStart <= metadata.length(), "VARIANT metadata is truncated");
        this.namesStart = (int) namesStart;
    }

    /// Finds the fields of `names` in the object that starts at `offset` of `data`.
    /// Sets `starts[i]` to the start of the value of `names.get(i)` in `data`, or to -1 if
    /// the object has no such field.
    ///
    /// @throws IllegalArgumentException if the value is not an object, if the object has a
    ///         field with one of the names twice, or if it is invalid
    public void find(Slice data, int offset, List<Slice> names, int[] starts)
    {
        byte header = data.getByte(offset);
        checkArgument(getBasicType(header) == OBJECT, "VARIANT value is not an object");
        boolean large = objectIsLarge(header);
        int idSize = objectFieldIdSize(header);
        int fieldOffsetSize = objectFieldOffsetSize(header);
        int count = large ? data.getInt(offset + 1) : data.getByte(offset + 1) & 0xFF;
        int idsStart = offset + 1 + (large ? Integer.BYTES : 1);
        long fieldOffsetsStart = idsStart + (long) count * idSize;
        long valuesStart = fieldOffsetsStart + (count + 1L) * fieldOffsetSize;
        checkArgument(count >= 0 && valuesStart <= data.length(), "VARIANT object is truncated");

        Arrays.fill(starts, 0, names.size(), -1);
        for (int index = 0; index < count; index++) {
            int fieldId = readOffset(data, idsStart + index * idSize, idSize);
            checkArgument(fieldId >= 0 && fieldId < dictionarySize, "VARIANT object has field id %s, which is not in the dictionary", fieldId);
            int nameStart = readOffset(metadata, offsetsStart + fieldId * offsetSize, offsetSize);
            int nameLength = readOffset(metadata, offsetsStart + (fieldId + 1) * offsetSize, offsetSize) - nameStart;
            for (int name = 0; name < names.size(); name++) {
                Slice candidate = names.get(name);
                if (candidate.length() == nameLength && metadata.equals(namesStart + nameStart, nameLength, candidate, 0, nameLength)) {
                    checkArgument(starts[name] == -1, "VARIANT object has duplicate field %s", candidate.toStringUtf8());
                    long valueStart = valuesStart + readOffset(data, (int) fieldOffsetsStart + index * fieldOffsetSize, fieldOffsetSize);
                    // A four-byte offset can be negative
                    checkArgument(valueStart >= valuesStart && valueStart < data.length(), "VARIANT object has an invalid field offset");
                    starts[name] = (int) valueStart;
                }
            }
        }
    }
}
