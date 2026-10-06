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
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.variant.Header.BasicType.ARRAY;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.getPrimitiveType;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.objectFieldIdSize;
import static io.trino.spi.variant.Header.objectFieldOffsetSize;
import static io.trino.spi.variant.Header.objectIsLarge;
import static io.trino.spi.variant.VariantDecoder.valueSize;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.lang.Math.toIntExact;

/// Repairs two defects of some Variant writers: a metadata dictionary that sets
/// `sorted_strings` but is not sorted, and object fields that are not in field name
/// order. Lookups that trust either one miss keys. DuckDB 1.5.5 writes both.
/// [ShreddedVariantAssembler] repairs the values that it reads, [VariantShredder] the
/// values that it writes, and the Parquet reader the unshredded values that it reads.
public final class VariantRepairs
{
    private VariantRepairs() {}

    /// Whether the files of a writer can have the defects that [#repair(VariantBlock,ParquetDataSourceId)]
    /// repairs. A repair checks every object of every value, which costs several times
    /// more than a key lookup, so readers check only the files of these writers.
    ///
    /// @param createdBy the `created_by` field of the file footer
    public static boolean writesDefects(Optional<String> createdBy)
    {
        // DuckDB 1.5.5 writes both defects, and no release has fixed them
        return createdBy.map(writer -> writer.startsWith("DuckDB")).orElse(false);
    }

    /// Returns `variant` with the fields of each object in field name order, and with a
    /// `sorted_strings` flag only if its dictionary is sorted and unique, or `variant`
    /// itself if it has neither defect. A repaired object has a new dictionary.
    ///
    /// @throws IllegalArgumentException if an object has two fields with the same name, or a
    ///         value is truncated
    public static Variant repair(Variant variant)
    {
        boolean sortedAndUnique = isSortedAndUnique(variant.metadata());
        Variant sorted = withSortedObjectFields(variant, sortedAndUnique);
        Metadata metadata = sorted.metadata();
        if (sorted == variant) {
            if (!metadata.isSorted() || sortedAndUnique) {
                return variant;
            }
            return Variant.from(withoutSortedFlag(metadata), variant.data());
        }
        Metadata verified = withVerifiedSortedFlag(metadata);
        if (verified == metadata) {
            return sorted;
        }
        return Variant.from(verified, sorted.data());
    }

    /// Returns `variants` with each value repaired by [#repair(Variant)], or `variants`
    /// itself if no value needs it. Only the blocks of a block that needs repairs are
    /// copied.
    ///
    /// @throws ParquetCorruptionException if an object has two fields with the same name, or a
    ///         value is invalid
    public static VariantBlock repair(VariantBlock variants, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        try {
            return repair(variants);
        }
        catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            // Variant decoding reports invalid data with these exceptions
            throw new ParquetCorruptionException(e, dataSourceId, "Invalid VARIANT: %s", e.getMessage());
        }
        catch (RuntimeException e) {
            // The variant package also reports invalid data with its own package-private VerifyException
            if (e.getClass().getPackageName().equals(Variant.class.getPackageName())) {
                throw new ParquetCorruptionException(e, dataSourceId, "Invalid VARIANT: %s", e.getMessage());
            }
            throw e;
        }
    }

    private static VariantBlock repair(VariantBlock variants)
    {
        int positionCount = variants.getPositionCount();
        BlockBuilder metadataBuilder = null;
        BlockBuilder valueBuilder = null;
        for (int position = 0; position < positionCount; position++) {
            Variant repaired = null;
            if (!variants.isNull(position)) {
                Variant variant = variants.getVariant(position);
                repaired = repair(variant);
                if (repaired == variant) {
                    repaired = null;
                }
            }
            if (repaired != null && metadataBuilder == null) {
                // The first value that needs a repair: copy the values before it
                metadataBuilder = VARBINARY.createBlockBuilder(null, positionCount);
                valueBuilder = VARBINARY.createBlockBuilder(null, positionCount);
                for (int previous = 0; previous < position; previous++) {
                    appendUnchanged(variants, previous, metadataBuilder, valueBuilder);
                }
            }
            if (repaired != null) {
                VARBINARY.writeSlice(metadataBuilder, repaired.metadata().toSlice());
                VARBINARY.writeSlice(valueBuilder, repaired.data());
            }
            else if (metadataBuilder != null) {
                appendUnchanged(variants, position, metadataBuilder, valueBuilder);
            }
        }
        if (metadataBuilder == null) {
            return variants;
        }
        long[] valueIsValid = variants.getRawValueIsValid();
        if (valueIsValid != null && variants.getRawOffset() != 0) {
            long[] copy = Bitmap.allocateWords(positionCount, false);
            Bitmap.copyBits(valueIsValid, variants.getRawOffset(), copy, 0, positionCount);
            valueIsValid = copy;
        }
        return VariantBlock.create(positionCount, metadataBuilder.build(), valueBuilder.build(), Optional.ofNullable(valueIsValid));
    }

    private static void appendUnchanged(VariantBlock variants, int position, BlockBuilder metadataBuilder, BlockBuilder valueBuilder)
    {
        if (variants.isNull(position)) {
            metadataBuilder.appendNull();
            valueBuilder.appendNull();
            return;
        }
        int rawPosition = variants.getRawOffset() + position;
        VARBINARY.writeSlice(metadataBuilder, VARBINARY.getSlice(variants.getRawMetadata(), rawPosition));
        VARBINARY.writeSlice(valueBuilder, VARBINARY.getSlice(variants.getRawValues(), rawPosition));
    }

    /// Returns `metadata` without the `sorted_strings` flag if its dictionary is not
    /// sorted and unique. Some writers set the flag for every dictionary, and lookups
    /// that trust it miss keys.
    static Metadata withVerifiedSortedFlag(Metadata metadata)
    {
        if (!metadata.isSorted() || isSortedAndUnique(metadata)) {
            return metadata;
        }
        return withoutSortedFlag(metadata);
    }

    private static Metadata withoutSortedFlag(Metadata metadata)
    {
        Slice copy = metadata.toSlice().copy();
        copy.setByte(0, metadataHeader(false, metadataOffsetSize(copy.getByte(0))));
        return Metadata.from(copy);
    }

    private static boolean isSortedAndUnique(Metadata metadata)
    {
        // The names are compared in place, because this runs for every value
        Slice dictionary = metadata.toSlice();
        int size = metadata.dictionarySize();
        if (size < 2) {
            return true;
        }
        int offsetSize = metadataOffsetSize(dictionary.getByte(0));
        int offsetsStart = 1 + offsetSize;
        int namesStart = offsetsStart + (size + 1) * offsetSize;
        // Each name ends where the next one starts
        int start = readOffset(dictionary, offsetsStart, offsetSize);
        int end = readOffset(dictionary, offsetsStart + offsetSize, offsetSize);
        for (int id = 1; id < size; id++) {
            int nextEnd = readOffset(dictionary, offsetsStart + (id + 1) * offsetSize, offsetSize);
            if (dictionary.compareTo(namesStart + start, end - start, dictionary, namesStart + end, nextEnd - end) >= 0) {
                return false;
            }
            start = end;
            end = nextEnd;
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
        // In a sorted dictionary without duplicates, the field ids are in field name order
        return withSortedObjectFields(variant, isSortedAndUnique(variant.metadata()));
    }

    private static Variant withSortedObjectFields(Variant variant, boolean idsInNameOrder)
    {
        // Most values are in order, and checking a value costs much less than copying it
        if (hasSortedObjectFields(variant, idsInNameOrder)) {
            return variant;
        }
        return switch (variant.basicType()) {
            case PRIMITIVE, SHORT_STRING -> {
                // A value only reads its header when it is created
                checkArgument(valueSize(variant.data(), 0) <= variant.data().length(), "VARIANT value is truncated");
                yield variant;
            }
            case ARRAY -> {
                List<Variant> elements = variant.arrayElements().collect(toImmutableList());
                List<Variant> sortedElements = null;
                for (int index = 0; index < elements.size(); index++) {
                    Variant element = elements.get(index);
                    Variant sortedElement = withSortedObjectFields(element, idsInNameOrder);
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
                    Variant value = withSortedObjectFields(field.value(), idsInNameOrder);
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
                    checkArgument(sortedFields.put(name, values.get(index)) == null, "VARIANT object has duplicate field %s", name.toStringUtf8());
                }
                yield Variant.ofObject(sortedFields);
            }
        };
    }

    /// Whether [#withSortedObjectFields] would return `variant` itself, because each
    /// object has its fields in field name order without duplicates, and no primitive
    /// value is truncated. A value with another layout, for example an object whose values
    /// are not in field order, returns false, and [#withSortedObjectFields] decides. The
    /// check reads the encoding without recursion, so a deeply nested value does not
    /// overflow the stack, and it does not decode the values.
    ///
    /// @param idsInNameOrder whether the field ids of the dictionary are in field name order
    private static boolean hasSortedObjectFields(Variant variant, boolean idsInNameOrder)
    {
        Metadata metadata = variant.metadata();
        Slice data = variant.data();
        // The start and end of each container that is left to check
        int[] pending = new int[16];
        int pendingCount = 0;
        if (!isContainer(data.getByte(0))) {
            return isComplete(data, 0, data.length());
        }
        pending[pendingCount++] = 0;
        pending[pendingCount++] = data.length();
        while (pendingCount > 0) {
            int end = pending[--pendingCount];
            int start = pending[--pendingCount];
            byte header = data.getByte(start);
            boolean object = getBasicType(header) == OBJECT;
            boolean large = object ? objectIsLarge(header) : arrayIsLarge(header);
            int idSize = object ? objectFieldIdSize(header) : 0;
            int offsetSize = object ? objectFieldOffsetSize(header) : arrayFieldOffsetSize(header);
            int idsStart = start + 1 + (large ? Integer.BYTES : 1);
            if (idsStart > end) {
                return false;
            }
            int count = large ? data.getInt(start + 1) : data.getByte(start + 1) & 0xFF;
            long offsetsStart = idsStart + (long) count * idSize;
            long valuesStart = offsetsStart + (count + 1L) * offsetSize;
            if (count < 0 || valuesStart > end) {
                return false;
            }
            long valuesEnd = valuesStart + readOffset(data, toIntExact(offsetsStart) + count * offsetSize, offsetSize);
            if (valuesEnd > end) {
                return false;
            }
            int previousFieldId = -1;
            for (int index = 0; index < count; index++) {
                if (object) {
                    int fieldId = readOffset(data, idsStart + index * idSize, idSize);
                    if (fieldId < 0 || fieldId >= metadata.dictionarySize()) {
                        return false;
                    }
                    if (index > 0 && !(idsInNameOrder ? previousFieldId < fieldId : metadata.get(previousFieldId).compareTo(metadata.get(fieldId)) < 0)) {
                        return false;
                    }
                    previousFieldId = fieldId;
                }
                // The elements of an array, and here the values of an object, are in order, so each one ends where the next one starts
                long valueStart = valuesStart + readOffset(data, toIntExact(offsetsStart) + index * offsetSize, offsetSize);
                long valueEnd = valuesStart + readOffset(data, toIntExact(offsetsStart) + (index + 1) * offsetSize, offsetSize);
                if (valueStart < valuesStart || valueStart >= valueEnd || valueEnd > valuesEnd) {
                    return false;
                }
                if (!isContainer(data.getByte(toIntExact(valueStart)))) {
                    if (!isComplete(data, toIntExact(valueStart), toIntExact(valueEnd))) {
                        return false;
                    }
                    continue;
                }
                if (pendingCount + 2 > pending.length) {
                    pending = Arrays.copyOf(pending, pending.length * 2);
                }
                pending[pendingCount++] = toIntExact(valueStart);
                pending[pendingCount++] = toIntExact(valueEnd);
            }
        }
        return true;
    }

    private static boolean isContainer(byte header)
    {
        BasicType basicType = getBasicType(header);
        return basicType == OBJECT || basicType == ARRAY;
    }

    /// Whether the primitive value at `start` ends at or before `end`
    private static boolean isComplete(Slice data, int start, int end)
    {
        byte header = data.getByte(start);
        if (getBasicType(header) == PRIMITIVE) {
            PrimitiveType type = getPrimitiveType(header);
            // A string or binary value has a length after its header
            if ((type == PrimitiveType.STRING || type == PrimitiveType.BINARY) && end - start < 1 + Integer.BYTES) {
                return false;
            }
        }
        int size = valueSize(data, start);
        return size > 0 && size <= end - start;
    }
}
