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
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.VariantBlock;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.EMPTY_SLICE;
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
import static io.trino.spi.variant.Header.PrimitiveType.NULL;
import static io.trino.spi.variant.Header.arrayFieldOffsetSize;
import static io.trino.spi.variant.Header.arrayIsLarge;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.primitiveHeader;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA_SLICE;
import static io.trino.spi.variant.VariantDecoder.valueSize;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal4;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantEncoder.encodeDouble;
import static io.trino.spi.variant.VariantEncoder.encodeLong;
import static io.trino.spi.variant.VariantEncoder.encodeString;
import static io.trino.spi.variant.VariantEncoder.encodedStringSize;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparing;
import static java.util.Objects.checkFromToIndex;
import static java.util.Objects.requireNonNull;

/// Builds VARIANT values from the columns of a shredded VARIANT group.
///
/// The input is a block of [VariantShreddingSchema#physicalType()], or its columns as
/// [ShreddedVariantColumns]. For each row, the assembler merges `value` and `typed_value`
/// as the [specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
/// describes.
///
/// Without paths, the assembler builds each whole value, and writes objects and arrays with
/// new metadata. It reads every column of the group, so it is the slow path for queries
/// that read the whole variant.
///
/// With [VariantPaths], it builds only the parts of each value that the paths read, from a
/// schema [pruned][VariantShreddingSchema#prune(VariantPaths)] to those paths. For every
/// path, the result reads the same value or fails with the same error as the whole value:
///
/// - An object on a path has only the keys that the paths name, if the value has them. A
///   key that a shredded field holds is read from that field, and the other keys are found
///   in the partially shredded object in `value`, without decoding it.
/// - A value that is not an object keeps its type: a primitive keeps its encoding, and an
///   array keeps its length, with the elements that a path reads and variant nulls for the
///   others.
/// - A node that a path reads whole is copied whole, repaired as described below.
///
/// The top-level `metadata` and `value` columns are read only for the rows that need them:
/// a row whose `typed_value` is null, a key that is not shredded, or an object or array
/// that is copied from a `value` column. Values without such objects and arrays share
/// one metadata dictionary of the keys of the paths, and are encoded without new metadata.
///
/// The assembler reads some data that the specification does not allow, in the same way
/// as the Apache Iceberg reader:
///
/// - If a partially shredded object has a shredded field in `value` too, the shredded
///   field is used. The specification lets readers assume that this does not occur.
/// - A missing value at the top level or in an array is a variant null, and so is a
///   null `optional` list element group.
///
/// It also repairs two defects of some writers in the values that it copies whole from
/// `value`: a metadata dictionary that sets `sorted_strings` but is not sorted, and
/// object fields that are not in field name order. Lookups that trust either one miss
/// keys. The lookups of keys on paths trust neither.
///
/// An assembler reuses its buffers between rows, so it is not thread-safe: each reader
/// uses its own.
public final class ShreddedVariantAssembler
{
    private static final int MISSING = 0;
    private static final int WRITTEN = 1;
    // The value needs new metadata, so it is built with Variant objects
    private static final int NEEDS_METADATA = 2;
    private static final byte NULL_HEADER = primitiveHeader(NULL);

    private final VariantShreddingSchema schema;
    private final Optional<ValuePlan> prunedPlan;
    private final Slice prunedMetadata;
    private final boolean alwaysNewMetadata;
    private final boolean topLevelVariantNullIsSqlNull;
    private final ParquetDataSourceId dataSourceId;
    private final PrunedVariantWriter writer = new PrunedVariantWriter();
    private final VariantResidualLookup lookup = new VariantResidualLookup();
    private long rowsWithNewMetadata;

    public ShreddedVariantAssembler(VariantShreddingSchema schema, ParquetDataSourceId dataSourceId)
    {
        this(schema, Optional.empty(), false, dataSourceId);
    }

    /// @param schema the schema of the columns, pruned to `paths` if it has them
    /// @param paths the parts of the values to build, or empty to build whole values
    /// @param topLevelVariantNullIsSqlNull whether a top-level variant null is read as SQL
    ///         NULL, for writers that store SQL NULL as a variant null
    public ShreddedVariantAssembler(VariantShreddingSchema schema, Optional<VariantPaths> paths, boolean topLevelVariantNullIsSqlNull, ParquetDataSourceId dataSourceId)
    {
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(paths, "paths is null");
        this.topLevelVariantNullIsSqlNull = topLevelVariantNullIsSqlNull;
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        if (paths.isEmpty() || paths.get().whole()) {
            this.prunedPlan = Optional.empty();
            this.prunedMetadata = EMPTY_METADATA_SLICE;
            this.alwaysNewMetadata = false;
            return;
        }
        // The fields of the objects that the writer encodes have the ids of this dictionary
        SortedSet<String> keys = new TreeSet<>(comparing(Slices::utf8Slice));
        collectKeys(paths.get(), keys);
        List<Slice> names = keys.stream().map(Slices::utf8Slice).collect(toImmutableList());
        Map<String, Integer> ids = new HashMap<>();
        for (int id = 0; id < names.size(); id++) {
            ids.put(names.get(id).toStringUtf8(), id);
        }
        // Metadata does not allow an empty key, so the values of paths with one get new metadata
        this.alwaysNewMetadata = keys.contains("");
        this.prunedMetadata = alwaysNewMetadata ? EMPTY_METADATA_SLICE : Metadata.of(names).toSlice();
        this.prunedPlan = Optional.of(valuePlan(schema.value(), PathNode.of(paths.get(), ids)));
    }

    /// The number of rows of pruned values that were built with new metadata, because a
    /// path reads an object or an array whole, or, for paths with an empty key, which
    /// the shared dictionary cannot hold, every row.
    public long rowsWithNewMetadata()
    {
        return rowsWithNewMetadata;
    }

    /// Returns a VARIANT block with one value for each position of `block`.
    public Block assemble(Block block)
            throws ParquetCorruptionException
    {
        return assemble(block, 0, block.getPositionCount(), Long.MAX_VALUE);
    }

    /// Returns a VARIANT block for up to `maxPositions` positions of `block`, from
    /// position `start`. It stops after the first position at which the result reaches
    /// `maxSizeInBytes`, so it has at least one position if `maxPositions` is not zero.
    ///
    /// Assembly copies every value, so a block of dictionary-encoded columns can grow
    /// much larger than the reader sized it. Callers bound the result with
    /// `maxSizeInBytes` and assemble the rest of the block in later calls.
    public Block assemble(Block block, int start, int maxPositions, long maxSizeInBytes)
            throws ParquetCorruptionException
    {
        return assemble(ShreddedVariantColumns.of(block), start, maxPositions, maxSizeInBytes);
    }

    /// Like [#assemble(Block,int,int,long)], for the columns of a group.
    public Block assemble(ShreddedVariantColumns columns, int start, int maxPositions, long maxSizeInBytes)
            throws ParquetCorruptionException
    {
        checkFromToIndex(start, start + maxPositions, columns.positionCount());
        checkArgument(maxSizeInBytes > 0, "maxSizeInBytes must be positive: %s", maxSizeInBytes);
        BoundValue value = new BoundValue(
                new ValueColumn(columns::value),
                schema.value().typedValue().map(typedValue -> bindTypedValue(typedValue, columns.typedValue())));
        RowContext row = new RowContext(new ValueColumn(columns::metadata));
        if (prunedPlan.isPresent()) {
            return assemblePruned(columns, value, row, start, maxPositions, maxSizeInBytes);
        }

        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, maxPositions);
        for (int position = start; position < start + maxPositions && builder.getSizeInBytes() < maxSizeInBytes; position++) {
            if (columns.isNull(position)) {
                builder.appendNull();
                continue;
            }
            row.reset(position);
            Variant variant = readVariant(value, row);
            if (variant.isNull() && topLevelVariantNullIsSqlNull) {
                builder.appendNull();
                continue;
            }
            VARIANT.writeObject(builder, variant);
        }
        return builder.build();
    }

    private Variant readVariant(BoundValue value, RowContext row)
            throws ParquetCorruptionException
    {
        try {
            return readValue(value, row.position(), row.rowMetadata()).orElse(Variant.NULL_VALUE);
        }
        catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            throw invalidVariant(e);
        }
        catch (RuntimeException e) {
            throw invalidVariantOrRethrow(e);
        }
    }

    private Block assemblePruned(ShreddedVariantColumns columns, BoundValue value, RowContext row, int start, int maxPositions, long maxSizeInBytes)
            throws ParquetCorruptionException
    {
        ValuePlan plan = prunedPlan.orElseThrow();
        BlockBuilder metadataBuilder = VARBINARY.createBlockBuilder(null, maxPositions);
        BlockBuilder valueBuilder = VARBINARY.createBlockBuilder(null, maxPositions);
        long[] valueIsValid = null;
        int positionCount = 0;
        for (int position = start; position < start + maxPositions && metadataBuilder.getSizeInBytes() + valueBuilder.getSizeInBytes() < maxSizeInBytes; position++) {
            positionCount++;
            boolean isNull = columns.isNull(position);
            if (!isNull) {
                row.reset(position);
                isNull = !writePrunedRow(plan, value, row, metadataBuilder, valueBuilder);
            }
            if (isNull) {
                if (valueIsValid == null) {
                    valueIsValid = Bitmap.allocateWords(maxPositions, true);
                }
                Bitmap.clear(valueIsValid, 0, positionCount - 1);
                metadataBuilder.appendNull();
                valueBuilder.appendNull();
            }
        }
        return VariantBlock.create(positionCount, metadataBuilder.build(), valueBuilder.build(), Optional.ofNullable(valueIsValid));
    }

    /// Writes the pruned value of a row, or returns false if it is a SQL NULL.
    private boolean writePrunedRow(ValuePlan plan, BoundValue value, RowContext row, BlockBuilder metadataBuilder, BlockBuilder valueBuilder)
            throws ParquetCorruptionException
    {
        try {
            writer.reset();
            int result = NEEDS_METADATA;
            if (!alwaysNewMetadata) {
                result = writeValue(plan, value, row.position(), row, true);
            }
            if (result == NEEDS_METADATA) {
                rowsWithNewMetadata++;
                Variant variant = readPrunedValue(plan, value, row.position(), row, true).orElse(Variant.NULL_VALUE);
                if (variant.isNull() && topLevelVariantNullIsSqlNull) {
                    return false;
                }
                VARBINARY.writeSlice(metadataBuilder, variant.metadata().toSlice());
                VARBINARY.writeSlice(valueBuilder, variant.data());
                return true;
            }
            if (result == MISSING) {
                writer.write(Variant.NULL_VALUE.data(), 0, ENCODED_NULL_SIZE);
            }
            Slice data = writer.value();
            if (topLevelVariantNullIsSqlNull && data.length() == ENCODED_NULL_SIZE && data.getByte(0) == NULL_HEADER) {
                return false;
            }
            VARBINARY.writeSlice(metadataBuilder, prunedMetadata);
            VARBINARY.writeSlice(valueBuilder, data);
            return true;
        }
        catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            throw invalidVariant(e);
        }
        catch (RuntimeException e) {
            throw invalidVariantOrRethrow(e);
        }
    }

    private ParquetCorruptionException invalidVariant(RuntimeException e)
    {
        // Variant decoding reports invalid data with these exceptions
        return new ParquetCorruptionException(e, dataSourceId, "Invalid shredded VARIANT: %s", e.getMessage());
    }

    private ParquetCorruptionException invalidVariantOrRethrow(RuntimeException e)
    {
        // The variant package also reports invalid data with its own package-private VerifyException
        if (e.getClass().getPackageName().equals(Variant.class.getPackageName())) {
            return new ParquetCorruptionException(e, dataSourceId, "Invalid shredded VARIANT: %s", e.getMessage());
        }
        throw e;
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

    /// Returns the value of a group with `value` and `typed_value` columns, or empty
    /// if both are null.
    private Optional<Variant> readValue(BoundValue value, int position, RowMetadata metadata)
            throws ParquetCorruptionException
    {
        Optional<Variant> untypedValue = Optional.empty();
        Block valueBlock = value.value().block();
        if (!valueBlock.isNull(position)) {
            Slice data = valueData(valueBlock, position);
            // Only objects and arrays refer to the metadata dictionary
            Metadata valueMetadata = EMPTY_METADATA;
            if (getBasicType(data.getByte(0)).isContainer()) {
                valueMetadata = metadata.metadata();
            }
            untypedValue = Optional.of(VariantRepairs.withSortedObjectFields(Variant.from(valueMetadata, data)));
        }

        if (value.typedValue().isEmpty() || value.typedValue().get().block().isNull(position)) {
            return untypedValue;
        }
        return Optional.of(switch (value.typedValue().get()) {
            case BoundPrimitive primitive -> {
                checkNoUntypedValue(untypedValue.isPresent());
                yield readPrimitive(primitive.primitive(), primitive.block(), position);
            }
            case BoundObject object -> readObject(object, position, metadata, untypedValue);
            case BoundArray array -> {
                checkNoUntypedValue(untypedValue.isPresent());
                yield readArray(array, position, metadata);
            }
        });
    }

    private Slice valueData(Block valueBlock, int position)
            throws ParquetCorruptionException
    {
        Slice data = VARBINARY.getSlice(valueBlock, position);
        if (data.length() == 0) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT value is empty");
        }
        return data;
    }

    private void checkNoUntypedValue(boolean hasUntypedValue)
            throws ParquetCorruptionException
    {
        if (hasUntypedValue) {
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

    /// Writes the pruned value of a group with `value` and `typed_value` columns with
    /// the writer, and returns [#WRITTEN], [#MISSING] if both are null, or
    /// [#NEEDS_METADATA] if the value has an object or an array that a path reads whole.
    /// The top-level `value` column is read only if the value needs it.
    private int writeValue(ValuePlan plan, BoundValue value, int position, RowContext row, boolean topLevel)
            throws ParquetCorruptionException
    {
        if (value.typedValue().isPresent() && !value.typedValue().get().block().isNull(position)) {
            return switch (value.typedValue().get()) {
                case BoundPrimitive primitive -> {
                    // The top-level value column is read only when a row needs it
                    if (!topLevel) {
                        checkNoUntypedValue(!value.value().block().isNull(position));
                    }
                    writePrimitive(primitive.primitive(), primitive.block(), position);
                    yield WRITTEN;
                }
                case BoundObject object -> {
                    if (plan.path().whole()) {
                        yield NEEDS_METADATA;
                    }
                    yield writeObject((ObjectPlan) plan.typedValue().orElseThrow(), object, value, position, row, topLevel);
                }
                case BoundArray array -> {
                    if (plan.path().whole()) {
                        yield NEEDS_METADATA;
                    }
                    if (!topLevel) {
                        checkNoUntypedValue(!value.value().block().isNull(position));
                    }
                    yield writeArray((ArrayPlan) plan.typedValue().orElseThrow(), array, position, row);
                }
            };
        }
        Block valueBlock = value.value().block();
        if (valueBlock.isNull(position)) {
            return MISSING;
        }
        Slice data = valueData(valueBlock, position);
        return writeUntyped(plan.path(), data, 0, row);
    }

    private int writeObject(ObjectPlan plan, BoundObject object, BoundValue group, int position, RowContext row, boolean topLevel)
            throws ParquetCorruptionException
    {
        // The keys that no shredded field holds are found in the partially shredded object
        Slice partialObject = partialObject(plan, group, position, row, topLevel);
        int[] residualStarts = plan.residualStarts();

        int dataStart = writer.size();
        int mark = writer.beginContainer();
        for (KeyPlan key : plan.keys()) {
            int result;
            if (key.shreddedField() >= 0) {
                // An optional field group that is null is a missing field
                if (object.fieldGroups().get(key.shreddedField()).isNull(position)) {
                    continue;
                }
                writer.beginElement(key.node().id());
                result = writeValue(key.shredded(), object.fields().get(key.shreddedField()), position, row, false);
            }
            else {
                if (partialObject == null || residualStarts[key.residualIndex()] < 0) {
                    continue;
                }
                writer.beginElement(key.node().id());
                result = writeUntyped(key.node(), partialObject, residualStarts[key.residualIndex()], row);
            }
            if (result == NEEDS_METADATA) {
                return NEEDS_METADATA;
            }
            if (result == MISSING) {
                writer.cancelElement();
            }
        }
        writer.endObject(mark, dataStart);
        return WRITTEN;
    }

    /// Returns the partially shredded object of a typed object, or null if the group has
    /// none, and finds in it the keys of the paths that no shredded field holds. A nested
    /// `value` column is read with its `typed_value`, so it is always checked, as in a
    /// whole read. The top-level one is read only when a key needs it.
    private Slice partialObject(ObjectPlan plan, BoundValue group, int position, RowContext row, boolean topLevel)
            throws ParquetCorruptionException
    {
        if (plan.residualNames().isEmpty() && topLevel) {
            return null;
        }
        Block valueBlock = group.value().block();
        if (valueBlock.isNull(position)) {
            return null;
        }
        Slice partialObject = valueData(valueBlock, position);
        if (getBasicType(partialObject.getByte(0)) != OBJECT) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT object has a value that is not an object");
        }
        if (!plan.residualNames().isEmpty()) {
            row.lookup().find(partialObject, 0, plan.residualNames(), plan.residualStarts());
        }
        return partialObject;
    }

    private int writeArray(ArrayPlan plan, BoundArray array, int position, RowContext row)
            throws ParquetCorruptionException
    {
        int offset = array.array().getOffset(position);
        int length = array.array().getLength(position);
        int dataStart = writer.size();
        int mark = writer.beginContainer();
        for (int index = 0; index < length; index++) {
            int elementPosition = offset + index;
            writer.beginElement(0);
            int result = MISSING;
            // Without a path below the elements, each element is a variant null
            if (plan.element().isPresent() && !array.elementGroups().isNull(elementPosition)) {
                result = writeValue(plan.element().get(), array.element(), elementPosition, row, false);
            }
            if (result == NEEDS_METADATA) {
                return NEEDS_METADATA;
            }
            if (result == MISSING) {
                writer.write(Variant.NULL_VALUE.data(), 0, ENCODED_NULL_SIZE);
            }
        }
        writer.endArray(mark, dataStart);
        return WRITTEN;
    }

    /// Writes the pruned value that starts at `offset` of `data`, an encoded value of a
    /// `value` column.
    private int writeUntyped(PathNode path, Slice data, int offset, RowContext row)
            throws ParquetCorruptionException
    {
        byte header = data.getByte(offset);
        switch (getBasicType(header)) {
            case PRIMITIVE, SHORT_STRING -> {
                int size = valueSize(data, offset);
                checkArgument(size > 0 && size <= data.length() - offset, "VARIANT value is truncated");
                writer.write(data, offset, size);
                return WRITTEN;
            }
            case OBJECT -> {
                if (path.whole()) {
                    return NEEDS_METADATA;
                }
                int[] starts = path.keyStarts();
                row.lookup().find(data, offset, path.keyNames(), starts);
                int dataStart = writer.size();
                int mark = writer.beginContainer();
                for (int key = 0; key < path.keys().size(); key++) {
                    if (starts[key] >= 0) {
                        writer.beginElement(path.keys().get(key).id());
                        if (writeUntyped(path.keys().get(key), data, starts[key], row) == NEEDS_METADATA) {
                            return NEEDS_METADATA;
                        }
                    }
                }
                writer.endObject(mark, dataStart);
                return WRITTEN;
            }
            case ARRAY -> {
                if (path.whole()) {
                    return NEEDS_METADATA;
                }
                UntypedArray array = UntypedArray.decode(data, offset);
                int dataStart = writer.size();
                int mark = writer.beginContainer();
                for (int index = 0; index < array.count(); index++) {
                    writer.beginElement(0);
                    if (path.elements().isEmpty()) {
                        // Without a path below the elements, each element is a variant null
                        writer.write(Variant.NULL_VALUE.data(), 0, ENCODED_NULL_SIZE);
                    }
                    else if (writeUntyped(path.elements().get(), data, array.elementStart(index), row) == NEEDS_METADATA) {
                        return NEEDS_METADATA;
                    }
                }
                writer.endArray(mark, dataStart);
                return WRITTEN;
            }
        }
        throw new IllegalStateException("Unexpected basic type: " + getBasicType(header));
    }

    /// Writes a `typed_value` primitive with the encoding of [#readPrimitive].
    private void writePrimitive(PrimitiveValue primitive, Block block, int position)
    {
        switch (primitive.shreddedType()) {
            case STRING -> {
                Slice value = VARCHAR.getSlice(block, position);
                int length = encodedStringSize(value.length());
                encodeString(value, writer.reserve(length), writer.size());
                writer.advance(length);
            }
            case INT64 -> {
                encodeLong(BIGINT.getLong(block, position), writer.reserve(ENCODED_LONG_SIZE), writer.size());
                writer.advance(ENCODED_LONG_SIZE);
            }
            case DOUBLE -> {
                encodeDouble(DOUBLE.getDouble(block, position), writer.reserve(ENCODED_DOUBLE_SIZE), writer.size());
                writer.advance(ENCODED_DOUBLE_SIZE);
            }
            default -> {
                Slice data = readPrimitive(primitive, block, position).data();
                writer.write(data, 0, data.length());
            }
        }
    }

    /// Returns the pruned value of a group as a [Variant], with new metadata, or empty if
    /// both `value` and `typed_value` are null. It is used for the values in which a path
    /// reads an object or an array whole, and gives the same value as [#writeValue].
    private Optional<Variant> readPrunedValue(ValuePlan plan, BoundValue value, int position, RowContext row, boolean topLevel)
            throws ParquetCorruptionException
    {
        if (plan.path().whole()) {
            // The schema below a node that is read whole is not pruned
            return readValue(value, position, row.rowMetadata());
        }
        if (value.typedValue().isPresent() && !value.typedValue().get().block().isNull(position)) {
            return Optional.of(switch (value.typedValue().get()) {
                case BoundPrimitive primitive -> {
                    if (!topLevel) {
                        checkNoUntypedValue(!value.value().block().isNull(position));
                    }
                    yield readPrimitive(primitive.primitive(), primitive.block(), position);
                }
                case BoundObject object -> readPrunedObject((ObjectPlan) plan.typedValue().orElseThrow(), object, value, position, row, topLevel);
                case BoundArray array -> {
                    if (!topLevel) {
                        checkNoUntypedValue(!value.value().block().isNull(position));
                    }
                    ArrayPlan arrayPlan = (ArrayPlan) plan.typedValue().orElseThrow();
                    int offset = array.array().getOffset(position);
                    int length = array.array().getLength(position);
                    List<Variant> elements = new ArrayList<>(length);
                    for (int index = 0; index < length; index++) {
                        int elementPosition = offset + index;
                        Optional<Variant> element = Optional.empty();
                        if (arrayPlan.element().isPresent() && !array.elementGroups().isNull(elementPosition)) {
                            element = readPrunedValue(arrayPlan.element().get(), array.element(), elementPosition, row, false);
                        }
                        elements.add(element.orElse(Variant.NULL_VALUE));
                    }
                    yield Variant.ofArray(elements);
                }
            });
        }
        Block valueBlock = value.value().block();
        if (valueBlock.isNull(position)) {
            return Optional.empty();
        }
        return Optional.of(readPrunedUntyped(plan.path(), valueData(valueBlock, position), 0, row));
    }

    private Variant readPrunedObject(ObjectPlan plan, BoundObject object, BoundValue group, int position, RowContext row, boolean topLevel)
            throws ParquetCorruptionException
    {
        Slice partialObject = partialObject(plan, group, position, row, topLevel);
        int[] residualStarts = plan.residualStarts();
        Map<Slice, Variant> fields = new HashMap<>();
        for (KeyPlan key : plan.keys()) {
            if (key.shreddedField() >= 0) {
                if (!object.fieldGroups().get(key.shreddedField()).isNull(position)) {
                    readPrunedValue(key.shredded(), object.fields().get(key.shreddedField()), position, row, false)
                            .ifPresent(value -> fields.put(key.node().name(), value));
                }
            }
            else if (partialObject != null && residualStarts[key.residualIndex()] >= 0) {
                fields.put(key.node().name(), readPrunedUntyped(key.node(), partialObject, residualStarts[key.residualIndex()], row));
            }
        }
        return Variant.ofObject(fields);
    }

    private Variant readPrunedUntyped(PathNode path, Slice data, int offset, RowContext row)
            throws ParquetCorruptionException
    {
        byte header = data.getByte(offset);
        int size = valueSize(data, offset);
        checkArgument(size > 0 && size <= data.length() - offset, "VARIANT value is truncated");
        if (!getBasicType(header).isContainer()) {
            return Variant.from(EMPTY_METADATA, data.slice(offset, size));
        }
        if (path.whole()) {
            return VariantRepairs.withSortedObjectFields(Variant.from(row.rowMetadata().metadata(), data.slice(offset, size)));
        }
        if (getBasicType(header) == OBJECT) {
            int[] starts = path.keyStarts();
            row.lookup().find(data, offset, path.keyNames(), starts);
            Map<Slice, Variant> fields = new HashMap<>();
            for (int key = 0; key < path.keys().size(); key++) {
                if (starts[key] >= 0) {
                    fields.put(path.keys().get(key).name(), readPrunedUntyped(path.keys().get(key), data, starts[key], row));
                }
            }
            return Variant.ofObject(fields);
        }
        UntypedArray array = UntypedArray.decode(data, offset);
        List<Variant> elements = new ArrayList<>(array.count());
        for (int index = 0; index < array.count(); index++) {
            if (path.elements().isEmpty()) {
                elements.add(Variant.NULL_VALUE);
            }
            else {
                elements.add(readPrunedUntyped(path.elements().get(), data, array.elementStart(index), row));
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
        return new BoundValue(new ValueColumn(fields.get(0)), value.typedValue().map(typedValue -> bindTypedValue(typedValue, fields.get(1))));
    }

    private static void collectKeys(VariantPaths paths, Set<String> keys)
    {
        paths.keys().forEach((key, child) -> {
            keys.add(key);
            collectKeys(child, keys);
        });
        paths.elements().ifPresent(elements -> collectKeys(elements, keys));
    }

    private static ValuePlan valuePlan(ShreddedValue value, PathNode path)
    {
        if (path.whole()) {
            return new ValuePlan(path, Optional.empty());
        }
        return new ValuePlan(path, value.typedValue().map(typedValue -> switch (typedValue) {
            case PrimitiveValue _ -> new PrimitivePlan();
            case ObjectValue object -> {
                ImmutableList.Builder<KeyPlan> keys = ImmutableList.builder();
                ImmutableList.Builder<Slice> residualNames = ImmutableList.builder();
                int residualCount = 0;
                for (PathNode key : path.keys()) {
                    int field = -1;
                    for (int index = 0; index < object.fields().size(); index++) {
                        if (object.fields().get(index).name().equals(key.name().toStringUtf8())) {
                            field = index;
                        }
                    }
                    if (field >= 0) {
                        keys.add(new KeyPlan(key, field, valuePlan(object.fields().get(field).value(), key), -1));
                    }
                    else {
                        keys.add(new KeyPlan(key, -1, null, residualCount++));
                        residualNames.add(key.name());
                    }
                }
                yield new ObjectPlan(keys.build(), residualNames.build());
            }
            case ArrayValue array -> new ArrayPlan(path.elements().map(elements -> valuePlan(array.element(), elements)));
        }));
    }

    /// A node of [VariantPaths], with the keys below it in field name order, and the
    /// starts of their values that a lookup finds, which the assembler reuses for each row.
    private static final class PathNode
    {
        private final Slice name;
        private final int id;
        private final boolean whole;
        private final List<PathNode> keys;
        private final List<Slice> keyNames;
        private final int[] keyStarts;
        private final Optional<PathNode> elements;

        /// @param id the field id of the key of this node in the metadata of pruned values
        private PathNode(Slice name, int id, boolean whole, List<PathNode> keys, Optional<PathNode> elements)
        {
            this.name = requireNonNull(name, "name is null");
            this.id = id;
            this.whole = whole;
            this.keys = ImmutableList.copyOf(keys);
            this.keyNames = keys.stream().map(PathNode::name).collect(toImmutableList());
            this.keyStarts = new int[keys.size()];
            this.elements = requireNonNull(elements, "elements is null");
        }

        static PathNode of(VariantPaths paths, Map<String, Integer> ids)
        {
            return of(EMPTY_SLICE, -1, paths, ids);
        }

        private static PathNode of(Slice name, int id, VariantPaths paths, Map<String, Integer> ids)
        {
            List<PathNode> keys = paths.keys().entrySet().stream()
                    .map(entry -> of(utf8Slice(entry.getKey()), ids.get(entry.getKey()), entry.getValue(), ids))
                    .sorted(comparing(PathNode::name))
                    .collect(toImmutableList());
            return new PathNode(name, id, paths.whole(), keys, paths.elements().map(elements -> of(EMPTY_SLICE, -1, elements, ids)));
        }

        public Slice name()
        {
            return name;
        }

        public int id()
        {
            return id;
        }

        public boolean whole()
        {
            return whole;
        }

        public List<PathNode> keys()
        {
            return keys;
        }

        public List<Slice> keyNames()
        {
            return keyNames;
        }

        public int[] keyStarts()
        {
            return keyStarts;
        }

        public Optional<PathNode> elements()
        {
            return elements;
        }
    }

    /// How a group with `value` and `typed_value` columns is pruned to the paths of a node.
    /// A node that is read whole has no plan for `typed_value`, which is read whole.
    private record ValuePlan(PathNode path, Optional<TypedPlan> typedValue) {}

    private sealed interface TypedPlan
            permits ArrayPlan,
                    ObjectPlan,
                    PrimitivePlan {}

    private record PrimitivePlan()
            implements TypedPlan {}

    /// The keys of an object on the paths, in field name order, and the names of the keys
    /// that no shredded field holds, which are looked up in the partially shredded object,
    /// with the starts of their values, which the assembler reuses for each row.
    private static final class ObjectPlan
            implements TypedPlan
    {
        private final List<KeyPlan> keys;
        private final List<Slice> residualNames;
        private final int[] residualStarts;

        private ObjectPlan(List<KeyPlan> keys, List<Slice> residualNames)
        {
            this.keys = ImmutableList.copyOf(keys);
            this.residualNames = ImmutableList.copyOf(residualNames);
            this.residualStarts = new int[residualNames.size()];
        }

        public List<KeyPlan> keys()
        {
            return keys;
        }

        public List<Slice> residualNames()
        {
            return residualNames;
        }

        public int[] residualStarts()
        {
            return residualStarts;
        }
    }

    /// A key of an object on the paths, held by the shredded field with index
    /// `shreddedField`, or else by the partially shredded object.
    private record KeyPlan(PathNode node, int shreddedField, ValuePlan shredded, int residualIndex) {}

    /// An array, whose elements are read only if a path reads them.
    private record ArrayPlan(Optional<ValuePlan> element)
            implements TypedPlan {}

    /// The elements of an array in a `value` column.
    private record UntypedArray(Slice data, int count, int offsetSize, int offsetsStart, int valuesStart)
    {
        static UntypedArray decode(Slice data, int offset)
        {
            byte header = data.getByte(offset);
            boolean large = arrayIsLarge(header);
            int offsetSize = arrayFieldOffsetSize(header);
            int count = large ? data.getInt(offset + 1) : data.getByte(offset + 1) & 0xFF;
            int offsetsStart = offset + 1 + (large ? Integer.BYTES : 1);
            long valuesStart = offsetsStart + (count + 1L) * offsetSize;
            checkArgument(count >= 0 && valuesStart <= data.length(), "VARIANT array is truncated");
            return new UntypedArray(data, count, offsetSize, offsetsStart, (int) valuesStart);
        }

        int elementStart(int index)
        {
            long start = valuesStart + (long) readOffset(data, offsetsStart + index * offsetSize, offsetSize);
            // A four-byte offset can be negative
            checkArgument(start >= valuesStart && start < data.length(), "VARIANT array has an invalid element offset");
            return (int) start;
        }
    }

    /// A column that is read the first time that a row needs it.
    private static final class ValueColumn
    {
        private final Supplier<Block> loader;
        private Block block;

        public ValueColumn(Supplier<Block> loader)
        {
            this.loader = requireNonNull(loader, "loader is null");
        }

        public ValueColumn(Block block)
        {
            this.loader = () -> block;
            this.block = requireNonNull(block, "block is null");
        }

        public Block block()
        {
            if (block == null) {
                block = requireNonNull(loader.get(), "loader returned null");
            }
            return block;
        }
    }

    /// The row that is assembled, and its metadata, which is read and decoded the first
    /// time that a `value` column needs it.
    private final class RowContext
    {
        private final ValueColumn metadataColumn;
        private int position;
        private Slice metadata;
        private RowMetadata rowMetadata;
        private boolean lookupReady;

        public RowContext(ValueColumn metadataColumn)
        {
            this.metadataColumn = requireNonNull(metadataColumn, "metadataColumn is null");
        }

        public void reset(int position)
        {
            this.position = position;
            metadata = null;
            rowMetadata = null;
            lookupReady = false;
        }

        public int position()
        {
            return position;
        }

        public RowMetadata rowMetadata()
                throws ParquetCorruptionException
        {
            if (rowMetadata == null) {
                rowMetadata = new RowMetadata(metadata());
            }
            return rowMetadata;
        }

        public VariantResidualLookup lookup()
                throws ParquetCorruptionException
        {
            if (!lookupReady) {
                // Like the metadata of a whole read, which the lookup reads in place
                Slice metadata = metadata();
                validateDictionaryOffsets(metadata);
                lookup.reset(metadata);
                lookupReady = true;
            }
            return lookup;
        }

        private Slice metadata()
                throws ParquetCorruptionException
        {
            if (metadata == null) {
                Block block = metadataColumn.block();
                if (block.isNull(position)) {
                    throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT metadata is null");
                }
                metadata = VARBINARY.getSlice(block, position);
            }
            return metadata;
        }
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
                metadata = VariantRepairs.withVerifiedSortedFlag(decoded);
            }
            return metadata;
        }
    }

    /// The blocks of a group with `value` and `typed_value` columns.
    private record BoundValue(ValueColumn value, Optional<BoundTypedValue> typedValue) {}

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
