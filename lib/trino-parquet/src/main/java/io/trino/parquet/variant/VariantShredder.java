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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.variant.VariantShreddingSchema.ArrayValue;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectField;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedType;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.parquet.variant.VariantShreddingSchema.TypedValue;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.ARRAY;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.VariantEncoder.encodeObject;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/// Splits VARIANT values into the columns of a shredded VARIANT group, as the
/// [specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
/// describes. It is the reverse of [ShreddedVariantAssembler].
///
/// The output is a block of [VariantShreddingSchema#physicalType()]:
///
/// - A SQL NULL is a null row. A variant null is a `value` of variant null, so the two
///   stay different.
/// - A value goes to `typed_value` only if it has the Variant type of the column. The
///   exception is the exact numerics, which the Variant encoding defines as one
///   equivalence class, and whose width Trino does not show: an integer of any width goes
///   to an integer column that holds it, and a decimal of any width goes to a decimal
///   column of its scale whose precision holds it. They are read back with the width of
///   the column. A value of another type, a decimal of another scale, and a value that
///   the Trino type of the column cannot hold stay in `value`.
/// - An object goes to an object `typed_value`, with each shredded field found by its
///   exact name. The other fields, including keys that differ from a shredded field only
///   by case, stay in `value` as a partially shredded object.
/// - Every element of an array goes to the list. A null element is a `value` of
///   variant null.
///
/// The `metadata` column holds the dictionary of each value, which has the names of its
/// shredded fields too, and all `value` columns of a row use it. Before it splits a
/// value, the shredder repairs the defects that [ShreddedVariantAssembler] repairs in the
/// values that it reads, so the files that it writes do not have them. A repaired value
/// has a new dictionary, and a primitive value has an empty one.
public final class VariantShredder
{
    private final RowType physicalType;
    private final BoundValue value;

    public VariantShredder(VariantShreddingSchema schema)
    {
        requireNonNull(schema, "schema is null");
        this.physicalType = schema.physicalType();
        this.value = bindValue(schema.value());
    }

    /// Returns a block of [VariantShreddingSchema#physicalType()] with one row for each
    /// position of `variants`. A dictionary or run-length encoded block keeps its
    /// encoding, so each of its values is shredded once, and a large value that many
    /// positions share is not copied for each of them.
    ///
    /// @throws IllegalArgumentException if an object has two fields with the same name, or
    ///         a value is truncated
    public Block shred(Block variants)
    {
        // Compacting drops the entries that no position uses, and can return a run-length encoded block
        Block block = variants instanceof DictionaryBlock dictionary ? dictionary.compact() : variants;
        return switch (block) {
            case RunLengthEncodedBlock runLengthEncoded -> RunLengthEncodedBlock.create(shredValues(runLengthEncoded.getValue()), runLengthEncoded.getPositionCount());
            case DictionaryBlock dictionary -> dictionary.createProjection(shredValues(dictionary.getDictionary()));
            case ValueBlock values -> shredValues(values);
        };
    }

    private Block shredValues(Block variants)
    {
        int positionCount = variants.getPositionCount();
        RowBlockBuilder builder = (RowBlockBuilder) physicalType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (variants.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Variant variant = repair(VARIANT.getObject(variants, position));
            builder.buildEntry(fields -> {
                VARBINARY.writeSlice(fields.get(0), variant.metadata().toSlice());
                writeValue(variant, value, fields, 1);
            });
        }
        return builder.build();
    }

    private static Variant repair(Variant variant)
    {
        Variant sorted = VariantRepairs.withSortedObjectFields(variant);
        Metadata metadata = VariantRepairs.withVerifiedSortedFlag(sorted.metadata());
        if (metadata == sorted.metadata()) {
            return sorted;
        }
        return Variant.from(metadata, sorted.data());
    }

    /// Writes `variant` to the `value` column at `valueField` of a group, and to the
    /// `typed_value` column after it.
    private static void writeValue(Variant variant, BoundValue value, List<BlockBuilder> groupFields, int valueField)
    {
        BlockBuilder valueBuilder = groupFields.get(valueField);
        if (value.typedValue().isEmpty()) {
            VARBINARY.writeSlice(valueBuilder, variant.data());
            return;
        }

        BlockBuilder typedValueBuilder = groupFields.get(valueField + 1);
        switch (value.typedValue().get()) {
            case BoundPrimitive primitive -> {
                if (isShreddedPrimitive(variant, primitive.primitive())) {
                    valueBuilder.appendNull();
                    writePrimitive(variant, primitive.primitive(), typedValueBuilder);
                    return;
                }
            }
            case BoundObject object -> {
                if (variant.basicType() == OBJECT) {
                    writeObject(variant, object, valueBuilder, (RowBlockBuilder) typedValueBuilder);
                    return;
                }
            }
            case BoundArray array -> {
                if (variant.basicType() == ARRAY) {
                    valueBuilder.appendNull();
                    writeArray(variant, array, (ArrayBlockBuilder) typedValueBuilder);
                    return;
                }
            }
        }
        // The value does not have the shredded type
        VARBINARY.writeSlice(valueBuilder, variant.data());
        typedValueBuilder.appendNull();
    }

    private static void writeObject(Variant object, BoundObject shredded, BlockBuilder valueBuilder, RowBlockBuilder typedValueBuilder)
    {
        Variant[] shreddedFields = new Variant[shredded.fields().size()];
        List<ObjectFieldIdValue> otherFields = new ArrayList<>();
        Metadata metadata = object.metadata();
        object.objectFields().forEach(field -> {
            // The exact name, so that a key that differs from a shredded field only by case is not shredded
            Integer index = shredded.fieldIndexes().get(metadata.get(field.fieldId()));
            if (index == null) {
                otherFields.add(field);
            }
            else {
                shreddedFields[index] = field.value();
            }
        });

        if (otherFields.isEmpty()) {
            valueBuilder.appendNull();
        }
        else {
            // A partially shredded object. Its fields are in field name order, like the fields of the object.
            VARBINARY.writeSlice(valueBuilder, encodeFields(otherFields));
        }
        typedValueBuilder.buildEntry(fieldGroups -> {
            for (int field = 0; field < shreddedFields.length; field++) {
                Variant fieldValue = shreddedFields[field];
                BoundValue fieldSchema = shredded.fields().get(field);
                ((RowBlockBuilder) fieldGroups.get(field)).buildEntry(groupFields -> {
                    if (fieldValue == null) {
                        // A missing field has null value and typed_value columns
                        groupFields.forEach(BlockBuilder::appendNull);
                    }
                    else {
                        writeValue(fieldValue, fieldSchema, groupFields, 0);
                    }
                });
            }
        });
    }

    /// Encodes an object of fields of one object, which refer to the same metadata.
    private static Slice encodeFields(List<ObjectFieldIdValue> fields)
    {
        int maxFieldId = 0;
        int totalLength = 0;
        for (ObjectFieldIdValue field : fields) {
            maxFieldId = max(maxFieldId, field.fieldId());
            totalLength += field.value().data().length();
        }
        Slice output = Slices.allocate(encodedObjectSize(maxFieldId, fields.size(), totalLength));
        int written = encodeObject(fields.size(), index -> fields.get(index).fieldId(), index -> fields.get(index).value().data(), output, 0);
        verify(written == output.length(), "Encoded object size does not match the expected size");
        return output;
    }

    private static void writeArray(Variant array, BoundArray shredded, ArrayBlockBuilder typedValueBuilder)
    {
        typedValueBuilder.buildEntry(elementBuilder -> {
            RowBlockBuilder elementGroups = (RowBlockBuilder) elementBuilder;
            int length = array.getArrayLength();
            for (int index = 0; index < length; index++) {
                Variant element = array.getArrayElement(index);
                elementGroups.buildEntry(groupFields -> writeValue(element, shredded.element(), groupFields, 0));
            }
        });
    }

    private static boolean isShreddedPrimitive(Variant variant, PrimitiveValue primitive)
    {
        if (variant.basicType() == SHORT_STRING) {
            return primitive.shreddedType() == ShreddedType.STRING;
        }
        if (variant.basicType() != PRIMITIVE) {
            return false;
        }
        PrimitiveType type = variant.primitiveType();
        return switch (primitive.shreddedType()) {
            case BOOLEAN -> type == PrimitiveType.BOOLEAN_TRUE || type == PrimitiveType.BOOLEAN_FALSE;
            case INT8 -> isInteger(type) && fits(integerValue(variant), Byte.MIN_VALUE, Byte.MAX_VALUE);
            case INT16 -> isInteger(type) && fits(integerValue(variant), Short.MIN_VALUE, Short.MAX_VALUE);
            case INT32 -> isInteger(type) && fits(integerValue(variant), Integer.MIN_VALUE, Integer.MAX_VALUE);
            case INT64 -> isInteger(type);
            case FLOAT -> type == PrimitiveType.FLOAT;
            case DOUBLE -> type == PrimitiveType.DOUBLE;
            case DECIMAL4, DECIMAL8, DECIMAL16 -> isDecimal(type) && fitsDecimal(variant, (DecimalType) primitive.type());
            case DATE -> type == PrimitiveType.DATE;
            // TIME(6) holds only times of day
            case TIME_MICROS -> type == PrimitiveType.TIME_NTZ_MICROS && variant.getTimeMicros() >= 0 && variant.getTimeMicros() < MICROSECONDS_PER_DAY;
            case TIMESTAMP_MICROS -> type == PrimitiveType.TIMESTAMP_NTZ_MICROS;
            case TIMESTAMP_NANOS -> type == PrimitiveType.TIMESTAMP_NTZ_NANOS;
            case TIMESTAMP_TZ_MICROS -> type == PrimitiveType.TIMESTAMP_UTC_MICROS && fitsTimestampWithTimeZone(floorDiv(variant.getTimestampMicros(), MICROSECONDS_PER_MILLISECOND));
            // The Parquet writer multiplies the epoch milliseconds of a value by 1,000,000 before it adds the rest
            case TIMESTAMP_TZ_NANOS -> type == PrimitiveType.TIMESTAMP_UTC_NANOS && floorDiv(variant.getTimestampNanos(), NANOSECONDS_PER_MILLISECOND) >= Long.MIN_VALUE / NANOSECONDS_PER_MILLISECOND;
            case BINARY -> type == PrimitiveType.BINARY;
            case STRING -> type == PrimitiveType.STRING;
            case UUID -> type == PrimitiveType.UUID;
        };
    }

    private static boolean isInteger(PrimitiveType type)
    {
        return type == PrimitiveType.INT8 || type == PrimitiveType.INT16 || type == PrimitiveType.INT32 || type == PrimitiveType.INT64;
    }

    private static long integerValue(Variant variant)
    {
        PrimitiveType type = variant.primitiveType();
        if (type == PrimitiveType.INT8) {
            return variant.getByte();
        }
        if (type == PrimitiveType.INT16) {
            return variant.getShort();
        }
        if (type == PrimitiveType.INT32) {
            return variant.getInt();
        }
        return variant.getLong();
    }

    private static boolean fits(long value, long min, long max)
    {
        return value >= min && value <= max;
    }

    private static boolean isDecimal(PrimitiveType type)
    {
        return type == PrimitiveType.DECIMAL4 || type == PrimitiveType.DECIMAL8 || type == PrimitiveType.DECIMAL16;
    }

    /// Whether a decimal of any width has the scale of the column and fits its precision.
    /// A decimal of another scale stays in `value`, because the scale shows when the value
    /// is printed.
    private static boolean fitsDecimal(Variant variant, DecimalType type)
    {
        Slice data = variant.data();
        if (data.getByte(1) != type.getScale()) {
            return false;
        }
        if (variant.primitiveType() == PrimitiveType.DECIMAL4) {
            return fitsPrecision(data.getInt(2), type.getPrecision());
        }
        if (variant.primitiveType() == PrimitiveType.DECIMAL8) {
            return fitsPrecision(data.getLong(2), type.getPrecision());
        }
        Int128 unscaled = decimal16Unscaled(data);
        if (unscaled.getHigh() == unscaled.getLow() >> 63) {
            return fitsPrecision(unscaled.getLow(), type.getPrecision());
        }
        return type.getPrecision() > MAX_SHORT_PRECISION && !overflows(unscaled.toBigInteger(), type.getPrecision());
    }

    private static boolean fitsPrecision(long unscaled, int precision)
    {
        // Every long has fewer than 20 digits
        if (precision > MAX_SHORT_PRECISION) {
            return true;
        }
        long limit = longTenToNth(precision);
        return unscaled > -limit && unscaled < limit;
    }

    /// The unscaled value of a DECIMAL16, which is 16 little-endian bytes after the header and the scale
    private static Int128 decimal16Unscaled(Slice data)
    {
        return Int128.valueOf(data.getLong(10), data.getLong(2));
    }

    /// Whether TIMESTAMP WITH TIME ZONE holds the epoch milliseconds. It stores them in 52
    /// bits, with the time zone in the other 12 (see `DateTimeEncoding`).
    private static boolean fitsTimestampWithTimeZone(long epochMillis)
    {
        return epochMillis << 12 >> 12 == epochMillis;
    }

    private static void writePrimitive(Variant variant, PrimitiveValue primitive, BlockBuilder builder)
    {
        switch (primitive.shreddedType()) {
            case BOOLEAN -> BOOLEAN.writeBoolean(builder, variant.getBoolean());
            case INT8 -> TINYINT.writeLong(builder, integerValue(variant));
            case INT16 -> SMALLINT.writeLong(builder, integerValue(variant));
            case INT32 -> INTEGER.writeLong(builder, integerValue(variant));
            case INT64 -> BIGINT.writeLong(builder, integerValue(variant));
            case FLOAT -> REAL.writeFloat(builder, variant.getFloat());
            case DOUBLE -> DOUBLE.writeDouble(builder, variant.getDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> writeDecimal(variant, (DecimalType) primitive.type(), builder);
            case DATE -> DATE.writeLong(builder, variant.getDate());
            case TIME_MICROS -> TIME_MICROS.writeLong(builder, variant.getTimeMicros() * PICOSECONDS_PER_MICROSECOND);
            case TIMESTAMP_MICROS -> TIMESTAMP_MICROS.writeLong(builder, variant.getTimestampMicros());
            case TIMESTAMP_NANOS -> {
                long nanos = variant.getTimestampNanos();
                int picosOfMicro = floorMod(nanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                TIMESTAMP_NANOS.writeObject(builder, new LongTimestamp(floorDiv(nanos, NANOSECONDS_PER_MICROSECOND), picosOfMicro));
            }
            case TIMESTAMP_TZ_MICROS -> {
                long micros = variant.getTimestampMicros();
                int picosOfMilli = floorMod(micros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND;
                TIMESTAMP_TZ_MICROS.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(floorDiv(micros, MICROSECONDS_PER_MILLISECOND), picosOfMilli, UTC_KEY));
            }
            case TIMESTAMP_TZ_NANOS -> {
                long nanos = variant.getTimestampNanos();
                int picosOfMilli = floorMod(nanos, NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND;
                TIMESTAMP_TZ_NANOS.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(floorDiv(nanos, NANOSECONDS_PER_MILLISECOND), picosOfMilli, UTC_KEY));
            }
            case BINARY -> VARBINARY.writeSlice(builder, variant.getBinary());
            case STRING -> VARCHAR.writeSlice(builder, variant.getString());
            // Both store the 16 bytes in big-endian order
            case UUID -> UUID.writeSlice(builder, variant.getUuidSlice());
        }
    }

    /// Writes a decimal of any width to a column of its scale whose precision holds it
    private static void writeDecimal(Variant variant, DecimalType type, BlockBuilder builder)
    {
        Slice data = variant.data();
        PrimitiveType width = variant.primitiveType();
        if (width == PrimitiveType.DECIMAL16) {
            Int128 unscaled = decimal16Unscaled(data);
            if (type.isShort()) {
                type.writeLong(builder, unscaled.toLongExact());
            }
            else {
                type.writeObject(builder, unscaled);
            }
            return;
        }
        long unscaled = width == PrimitiveType.DECIMAL4 ? data.getInt(2) : data.getLong(2);
        if (type.isShort()) {
            type.writeLong(builder, unscaled);
        }
        else {
            type.writeObject(builder, Int128.valueOf(unscaled));
        }
    }

    private static BoundValue bindValue(ShreddedValue value)
    {
        return new BoundValue(value.typedValue().map(VariantShredder::bindTypedValue));
    }

    private static BoundTypedValue bindTypedValue(TypedValue typedValue)
    {
        return switch (typedValue) {
            case PrimitiveValue primitive -> new BoundPrimitive(primitive);
            case ObjectValue object -> {
                ImmutableMap.Builder<Slice, Integer> fieldIndexes = ImmutableMap.builder();
                for (int field = 0; field < object.fields().size(); field++) {
                    fieldIndexes.put(utf8Slice(object.fields().get(field).name()), field);
                }
                List<BoundValue> fields = object.fields().stream()
                        .map(ObjectField::value)
                        .map(VariantShredder::bindValue)
                        .collect(toImmutableList());
                yield new BoundObject(fields, fieldIndexes.buildOrThrow());
            }
            case ArrayValue array -> new BoundArray(bindValue(array.element()));
        };
    }

    /// The schema of a group with `value` and `typed_value` columns.
    private record BoundValue(Optional<BoundTypedValue> typedValue) {}

    private sealed interface BoundTypedValue
            permits BoundArray,
                    BoundObject,
                    BoundPrimitive {}

    private record BoundPrimitive(PrimitiveValue primitive)
            implements BoundTypedValue {}

    /// The shredded fields of an object, and the index of each field by its name.
    private record BoundObject(List<BoundValue> fields, Map<Slice, Integer> fieldIndexes)
            implements BoundTypedValue {}

    private record BoundArray(BoundValue element)
            implements BoundTypedValue {}
}
