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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.variant.VariantShreddingSchema.ArrayValue;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.parquet.variant.VariantShreddingSchema.TypedValue;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.VariantDecoder.valueSize;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal4;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Builds VARIANT values from the columns of a shredded VARIANT group.
///
/// The input is a block of [VariantShreddingSchema#physicalType()]. For each row, the
/// assembler merges `value` and `typed_value` as the
/// [specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
/// describes, and writes objects and arrays with new metadata. It reads every column
/// of the group, so it is the slow path for queries that read the whole variant.
///
/// The assembler reads some data that the specification does not allow, in the same way
/// as the Apache Iceberg reader:
///
/// - If a partially shredded object has a shredded field in `value` too, the shredded
///   field is used. The specification lets readers assume that this does not occur.
/// - A missing value at the top level or in an array is a variant null, and so is a
///   null `optional` list element group.
///
/// It also repairs two defects of some writers in the values that it copies from
/// `value`: a metadata dictionary that sets `sorted_strings` but is not sorted, and
/// object fields that are not in field name order. Lookups that trust either one miss
/// keys.
public final class ShreddedVariantAssembler
{
    private final VariantShreddingSchema schema;
    private final ParquetDataSourceId dataSourceId;

    public ShreddedVariantAssembler(VariantShreddingSchema schema, ParquetDataSourceId dataSourceId)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
    }

    /// Returns a VARIANT block with one value for each position of `block`.
    public Block assemble(Block block)
            throws ParquetCorruptionException
    {
        List<Block> fields = getRowFieldsFromBlock(block);
        Block metadataBlock = fields.get(0);
        BoundValue value = new BoundValue(
                fields.get(1),
                schema.value().typedValue().map(typedValue -> bindTypedValue(typedValue, fields.get(2))));

        int positionCount = block.getPositionCount();
        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            VARIANT.writeObject(builder, readVariant(value, metadataBlock, position));
        }
        return builder.build();
    }

    private Variant readVariant(BoundValue value, Block metadataBlock, int position)
            throws ParquetCorruptionException
    {
        if (metadataBlock.isNull(position)) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT metadata is null");
        }
        try {
            return readValue(value, position, new RowMetadata(VARBINARY.getSlice(metadataBlock, position))).orElse(Variant.NULL_VALUE);
        }
        catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            // Variant decoding reports invalid data with these exceptions
            throw new ParquetCorruptionException(e, dataSourceId, "Invalid shredded VARIANT: %s", e.getMessage());
        }
        catch (RuntimeException e) {
            // The variant package also reports invalid data with its own package-private VerifyException
            if (e.getClass().getPackageName().equals(Variant.class.getPackageName())) {
                throw new ParquetCorruptionException(e, dataSourceId, "Invalid shredded VARIANT: %s", e.getMessage());
            }
            throw e;
        }
    }

    /// Checks the dictionary offsets of encoded metadata. Unlike
    /// {@link Metadata#validateFully()}, this allows empty strings, which have equal
    /// offsets, and empty dictionaries with offsets of more than one byte.
    @VisibleForTesting
    static void validateDictionaryOffsets(Slice metadata)
    {
        int offsetSize = metadataOffsetSize(metadata.getByte(0));
        int dictionarySize = readOffset(metadata, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        int previous = 0;
        for (int index = 0; index <= dictionarySize; index++) {
            int offset = readOffset(metadata, offsetsStart + index * offsetSize, offsetSize);
            checkArgument(index != 0 || offset == 0, "First dictionary offset must be 0");
            checkArgument(offset >= previous, "Dictionary offsets must not decrease");
            previous = offset;
        }
        int dictionaryStart = offsetsStart + (dictionarySize + 1) * offsetSize;
        checkArgument(dictionaryStart + previous == metadata.length(), "Last dictionary offset must equal dictionary length");
    }

    /// Returns `metadata` without the `sorted_strings` flag if its dictionary is not
    /// sorted and unique. Some writers set the flag for every dictionary, and lookups
    /// that trust it miss keys.
    @VisibleForTesting
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
    @VisibleForTesting
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

    /// Returns the value of a group with `value` and `typed_value` columns, or empty
    /// if both are null.
    private Optional<Variant> readValue(BoundValue value, int position, RowMetadata metadata)
            throws ParquetCorruptionException
    {
        Optional<Variant> untypedValue = Optional.empty();
        if (!value.value().isNull(position)) {
            Slice data = VARBINARY.getSlice(value.value(), position);
            if (data.length() == 0) {
                throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT value is empty");
            }
            // Only objects and arrays refer to the metadata dictionary
            Metadata valueMetadata = EMPTY_METADATA;
            if (getBasicType(data.getByte(0)).isContainer()) {
                valueMetadata = metadata.metadata();
            }
            untypedValue = Optional.of(withSortedObjectFields(Variant.from(valueMetadata, data)));
        }

        if (value.typedValue().isEmpty() || value.typedValue().get().block().isNull(position)) {
            return untypedValue;
        }
        return Optional.of(switch (value.typedValue().get()) {
            case BoundPrimitive primitive -> {
                checkNoUntypedValue(untypedValue);
                yield readPrimitive(primitive.primitive(), primitive.block(), position);
            }
            case BoundObject object -> readObject(object, position, metadata, untypedValue);
            case BoundArray array -> {
                checkNoUntypedValue(untypedValue);
                yield readArray(array, position, metadata);
            }
        });
    }

    private void checkNoUntypedValue(Optional<Variant> untypedValue)
            throws ParquetCorruptionException
    {
        if (untypedValue.isPresent()) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT value that is not an object has both value and typed_value");
        }
    }

    private Variant readObject(BoundObject object, int position, RowMetadata metadata, Optional<Variant> untypedValue)
            throws ParquetCorruptionException
    {
        Map<Slice, Variant> fields = new HashMap<>();
        for (int field = 0; field < object.fields().size(); field++) {
            // An optional field group that is null is a missing field, like a group with null value and typed_value
            if (!object.fieldGroups().get(field).isNull(position)) {
                Optional<Variant> fieldValue = readValue(object.fields().get(field), position, metadata);
                if (fieldValue.isPresent()) {
                    fields.put(object.names().get(field), fieldValue.get());
                }
            }
        }

        if (untypedValue.isPresent()) {
            Variant partialObject = untypedValue.get();
            if (partialObject.basicType() != OBJECT) {
                throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT object has a value that is not an object");
            }
            for (ObjectFieldIdValue field : partialObject.objectFields().toList()) {
                Slice name = partialObject.metadata().get(field.fieldId());
                if (!object.nameSet().contains(name)) {
                    fields.put(name, field.value());
                }
            }
        }
        return Variant.ofObject(fields);
    }

    private Variant readArray(BoundArray array, int position, RowMetadata metadata)
            throws ParquetCorruptionException
    {
        int offset = array.array().getOffset(position);
        int length = array.array().getLength(position);
        List<Variant> elements = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            int elementPosition = offset + index;
            // A null optional element group is read like an element with null value and typed_value
            if (array.elementGroups().isNull(elementPosition)) {
                elements.add(Variant.NULL_VALUE);
            }
            else {
                elements.add(readValue(array.element(), elementPosition, metadata).orElse(Variant.NULL_VALUE));
            }
        }
        return Variant.ofArray(elements);
    }

    private static Variant readPrimitive(PrimitiveValue primitive, Block block, int position)
    {
        return switch (primitive.shreddedType()) {
            case BOOLEAN -> Variant.ofBoolean(BOOLEAN.getBoolean(block, position));
            case INT8 -> Variant.ofByte(TINYINT.getByte(block, position));
            case INT16 -> Variant.ofShort(SMALLINT.getShort(block, position));
            case INT32 -> Variant.ofInt(INTEGER.getInt(block, position));
            case INT64 -> Variant.ofLong(BIGINT.getLong(block, position));
            case FLOAT -> Variant.ofFloat(REAL.getFloat(block, position));
            case DOUBLE -> Variant.ofDouble(DOUBLE.getDouble(block, position));
            case DECIMAL4 -> {
                DecimalType type = (DecimalType) primitive.type();
                Slice data = Slices.allocate(ENCODED_DECIMAL4_SIZE);
                encodeDecimal4(toIntExact(type.getLong(block, position)), type.getScale(), data, 0);
                yield Variant.from(EMPTY_METADATA, data);
            }
            case DECIMAL8 -> {
                DecimalType type = (DecimalType) primitive.type();
                Slice data = Slices.allocate(ENCODED_DECIMAL8_SIZE);
                encodeDecimal8(type.getLong(block, position), type.getScale(), data, 0);
                yield Variant.from(EMPTY_METADATA, data);
            }
            case DECIMAL16 -> {
                DecimalType type = (DecimalType) primitive.type();
                Int128 unscaled;
                if (type.isShort()) {
                    unscaled = Int128.valueOf(type.getLong(block, position));
                }
                else {
                    unscaled = (Int128) type.getObject(block, position);
                }
                Slice data = Slices.allocate(ENCODED_DECIMAL16_SIZE);
                encodeDecimal16(unscaled, type.getScale(), data, 0);
                yield Variant.from(EMPTY_METADATA, data);
            }
            case DATE -> Variant.ofDate(DATE.getInt(block, position));
            case TIME_MICROS -> Variant.ofTimeMicrosNtz(TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
            case TIMESTAMP_MICROS -> Variant.ofTimestampMicrosNtz(TIMESTAMP_MICROS.getLong(block, position));
            case TIMESTAMP_NANOS -> {
                LongTimestamp timestamp = (LongTimestamp) TIMESTAMP_NANOS.getObject(block, position);
                yield Variant.ofTimestampNanosNtz(timestamp.getEpochMicros() * NANOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);
            }
            case TIMESTAMP_TZ_MICROS -> {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
                yield Variant.ofTimestampMicrosUtc(timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_MICROSECOND);
            }
            case TIMESTAMP_TZ_NANOS -> {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, position);
                yield Variant.ofTimestampNanosUtc(timestamp.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
            }
            case BINARY -> Variant.ofBinary(VARBINARY.getSlice(block, position));
            case STRING -> Variant.ofString(VARCHAR.getSlice(block, position));
            case UUID -> Variant.ofUuid(trinoUuidToJavaUuid(UUID.getSlice(block, position)));
        };
    }

    private static BoundTypedValue bindTypedValue(TypedValue typedValue, Block block)
    {
        return switch (typedValue) {
            case PrimitiveValue primitive -> new BoundPrimitive(block, primitive);
            case ObjectValue object -> {
                List<Block> fieldGroups = getRowFieldsFromBlock(block);
                ImmutableList.Builder<BoundValue> fields = ImmutableList.builder();
                for (int field = 0; field < object.fields().size(); field++) {
                    fields.add(bindValue(object.fields().get(field).value(), fieldGroups.get(field)));
                }
                List<Slice> names = object.fields().stream()
                        .map(field -> utf8Slice(field.name()))
                        .collect(toImmutableList());
                yield new BoundObject(block, names, ImmutableSet.copyOf(names), fieldGroups, fields.build());
            }
            case ArrayValue array -> {
                ColumnarArray columnarArray = toColumnarArray(block);
                Block elementGroups = columnarArray.getElementsBlock();
                yield new BoundArray(block, columnarArray, elementGroups, bindValue(array.element(), elementGroups));
            }
        };
    }

    private static BoundValue bindValue(ShreddedValue value, Block group)
    {
        List<Block> fields = getRowFieldsFromBlock(group);
        return new BoundValue(fields.get(0), value.typedValue().map(typedValue -> bindTypedValue(typedValue, fields.get(1))));
    }

    /// The metadata of one row, decoded the first time that a `value` column needs it.
    private static final class RowMetadata
    {
        private final Slice slice;
        private Metadata metadata;

        public RowMetadata(Slice slice)
        {
            this.slice = requireNonNull(slice, "slice is null");
        }

        public Metadata metadata()
        {
            if (metadata == null) {
                Metadata decoded = Metadata.from(slice);
                validateDictionaryOffsets(slice);
                metadata = withVerifiedSortedFlag(decoded);
            }
            return metadata;
        }
    }

    /// The blocks of a group with `value` and `typed_value` columns.
    private record BoundValue(Block value, Optional<BoundTypedValue> typedValue) {}

    private sealed interface BoundTypedValue
            permits BoundArray,
                    BoundObject,
                    BoundPrimitive
    {
        Block block();
    }

    private record BoundPrimitive(Block block, PrimitiveValue primitive)
            implements BoundTypedValue {}

    private record BoundObject(Block block, List<Slice> names, Set<Slice> nameSet, List<Block> fieldGroups, List<BoundValue> fields)
            implements BoundTypedValue {}

    private record BoundArray(Block block, ColumnarArray array, Block elementGroups, BoundValue element)
            implements BoundTypedValue {}
}
