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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Comparator.comparingInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/// The layout of a shredded VARIANT group in a Parquet file, as defined by the
/// [Parquet VARIANT shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
///
/// [#physicalType()] describes the group as a Trino row type, so that the
/// Parquet reader can read its columns. [ShreddedVariantAssembler] then builds
/// VARIANT values from that row.
///
/// The parser accepts these deviations from the specification, because the
/// meaning of the data does not change:
///
/// - Object field groups and list element groups that are `optional` instead
///   of `required`
/// - A `value` column that is not in the schema, which is read as null
///
/// A `typed_value` column of `TIMESTAMP(isAdjustedToUTC=true)` is read as
/// TIMESTAMP WITH TIME ZONE, which holds about 71,000 years before and after 1970.
/// A value outside that range fails the read, although a Variant can hold it.
///
/// To write shredded VARIANT values, create the schema with [#of(ShreddedValue)] and
/// add the group of [#toParquetType(String,Repetition)] to the Parquet schema. The
/// Parquet writer reads the schema back from that group with
/// [#fromWriterSchema(GroupType)], and writes the values with [VariantShredder].
public final class VariantShreddingSchema
{
    private final ShreddedValue value;

    private VariantShreddingSchema(ShreddedValue value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    /// Parses a shredded VARIANT group.
    ///
    /// @param variantGroup the group from the schema of
    ///         {@link io.trino.parquet.metadata.ParquetMetadata#getFileMetaData()}, which has
    ///         lowercase field names
    /// @param originalNames the original field names of the same group
    public static VariantShreddingSchema fromParquet(GroupType variantGroup, ParquetOriginalFieldNames originalNames, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        if (variantGroup.isRepetition(REPEATED)) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT group %s is repeated", originalNames);
        }
        return new VariantShreddingSchema(parseValue(variantGroup, originalNames, true, dataSourceId));
    }

    /// Returns a schema to write.
    ///
    /// @throws IllegalArgumentException if a Trino reader cannot read the schema: an
    ///         object has no fields, or a field with an empty name, or two fields whose names
    ///         are equal or differ only by case
    public static VariantShreddingSchema of(ShreddedValue value)
    {
        checkWritable(value, "$");
        return new VariantShreddingSchema(value);
    }

    /// Parses a shredded VARIANT group of a schema to write. The group must be the
    /// group that [#toParquetType(String,Repetition)] returns, with an optional field id.
    ///
    /// @throws IllegalArgumentException if the group does not have that layout
    public static VariantShreddingSchema fromWriterSchema(GroupType variantGroup)
    {
        VariantShreddingSchema schema = of(writerValue(variantGroup));
        GroupType expected = schema.toParquetType(variantGroup.getName(), variantGroup.getRepetition());
        if (variantGroup.getId() != null) {
            expected = expected.withId(variantGroup.getId().intValue());
        }
        checkArgument(variantGroup.equals(expected), "Shredded VARIANT group does not have the layout of the specification: %s, expected: %s", variantGroup, expected);
        return schema;
    }

    private static ShreddedValue writerValue(GroupType group)
    {
        if (!group.containsField("typed_value")) {
            return new ShreddedValue(Optional.empty());
        }
        org.apache.parquet.schema.Type typedValue = group.getType("typed_value");
        if (typedValue.isPrimitive()) {
            return new ShreddedValue(Optional.of(primitiveValue(typedValue.asPrimitiveType())
                    .orElseThrow(() -> new IllegalArgumentException("Unsupported shredded VARIANT value type: " + typedValue))));
        }
        GroupType typedGroup = typedValue.asGroupType();
        if (typedGroup.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            checkArgument(typedGroup.getFieldCount() == 1 && !typedGroup.getType(0).isPrimitive(), "Shredded VARIANT list does not have a repeated group: %s", typedGroup);
            GroupType repeated = typedGroup.getType(0).asGroupType();
            checkArgument(repeated.getFieldCount() == 1 && !repeated.getType(0).isPrimitive(), "Shredded VARIANT list does not have an element group: %s", typedGroup);
            return new ShreddedValue(Optional.of(new ArrayValue(writerValue(repeated.getType(0).asGroupType()))));
        }
        ImmutableList.Builder<ObjectField> fields = ImmutableList.builder();
        for (org.apache.parquet.schema.Type field : typedGroup.getFields()) {
            checkArgument(!field.isPrimitive(), "Shredded VARIANT object field is not a group: %s", field);
            fields.add(new ObjectField(field.getName(), writerValue(field.asGroupType())));
        }
        return new ShreddedValue(Optional.of(new ObjectValue(fields.build())));
    }

    private static void checkWritable(ShreddedValue value, String path)
    {
        if (value.typedValue().isEmpty()) {
            return;
        }
        switch (value.typedValue().get()) {
            // The type of a primitive value is checked when it is created
            case PrimitiveValue _ -> {}
            case ObjectValue object -> {
                checkArgument(!object.fields().isEmpty(), "Shredded VARIANT object %s has no fields", path);
                // The reader finds Parquet columns by lowercase name, so it cannot tell these fields apart
                Map<String, String> lowercaseNames = new HashMap<>();
                for (ObjectField field : object.fields()) {
                    String name = field.name();
                    checkArgument(!name.isEmpty(), "Shredded VARIANT object %s has a field with an empty name", path);
                    String previous = lowercaseNames.putIfAbsent(name.toLowerCase(ENGLISH), name);
                    checkArgument(previous == null || !previous.equals(name), "Shredded VARIANT object %s has duplicate field %s", path, name);
                    checkArgument(previous == null, "Shredded VARIANT object %s has fields that differ only by case: %s and %s", path, previous, name);
                    checkWritable(field.value(), path + "." + name);
                }
            }
            case ArrayValue array -> checkWritable(array.element(), path + "[*]");
        }
    }

    /// The `value` and `typed_value` columns of the VARIANT group.
    public ShreddedValue value()
    {
        return value;
    }

    /// Returns the schema with only the shredded columns that the paths read.
    ///
    /// A value read with the pruned schema gives the same result as the whole value
    /// for each of the paths, including errors such as a key of a value that is not an
    /// object: every `value` column on the way stays, so a value that is not an object
    /// or an array stays unchanged, and an array keeps all its elements. The value can
    /// have object fields that no path reads. Corrupt data in the columns that are not
    /// read is not detected, as with any column pruning.
    public VariantShreddingSchema prune(VariantPaths paths)
    {
        return new VariantShreddingSchema(pruneValue(value, paths));
    }

    private static ShreddedValue pruneValue(ShreddedValue value, VariantPaths paths)
    {
        if (paths.whole()) {
            return value;
        }
        return new ShreddedValue(value.typedValue().map(typedValue -> pruneTypedValue(typedValue, paths)));
    }

    private static TypedValue pruneTypedValue(TypedValue typedValue, VariantPaths paths)
    {
        return switch (typedValue) {
            case PrimitiveValue primitive -> primitive;
            case ObjectValue object -> {
                List<ObjectField> fields = object.fields().stream()
                        .filter(field -> paths.keys().containsKey(field.name()))
                        .map(field -> new ObjectField(field.name(), pruneValue(field.value(), paths.keys().get(field.name()))))
                        .collect(toImmutableList());
                if (fields.isEmpty()) {
                    // The reader needs a column of the group to tell whether the value is an object
                    ObjectField anchor = anchorField(object);
                    fields = ImmutableList.of(new ObjectField(anchor.name(), minimalValue(anchor.value())));
                }
                yield new ObjectValue(fields);
            }
            case ArrayValue array -> new ArrayValue(paths.elements()
                    .map(elements -> pruneValue(array.element(), elements))
                    .orElseGet(() -> minimalValue(array.element())));
        };
    }

    /// The fewest columns that still tell whether a value is null, missing, or of its
    /// shredded type.
    private static ShreddedValue minimalValue(ShreddedValue value)
    {
        return new ShreddedValue(value.typedValue().map(typedValue -> switch (typedValue) {
            case PrimitiveValue primitive -> primitive;
            case ObjectValue object -> {
                ObjectField anchor = anchorField(object);
                yield new ObjectValue(ImmutableList.of(new ObjectField(anchor.name(), minimalValue(anchor.value()))));
            }
            case ArrayValue array -> new ArrayValue(minimalValue(array.element()));
        }));
    }

    /// The field that an object keeps when no path reads its fields. A field with a
    /// fixed-width primitive type is the cheapest to read, and an object or an array
    /// field the most expensive.
    private static ObjectField anchorField(ObjectValue object)
    {
        return object.fields().stream()
                .min(comparingInt(field -> field.value().typedValue()
                        .map(typedValue -> switch (typedValue) {
                            case PrimitiveValue primitive -> primitive.type() instanceof FixedWidthType ? 0 : 1;
                            case ObjectValue _, ArrayValue _ -> 2;
                        })
                        .orElse(1)))
                .orElseThrow();
    }

    /// Returns the type that reads the group: a row of `metadata`, `value`, and,
    /// if the group has it, `typed_value`.
    public RowType physicalType()
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(field("metadata", VARBINARY));
        fields.addAll(valueFields(value));
        return RowType.from(fields.build());
    }

    private static List<RowType.Field> valueFields(ShreddedValue value)
    {
        // The value column is always read. It is null if the file does not have it.
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(field("value", VARBINARY));
        value.typedValue().ifPresent(typedValue -> fields.add(field("typed_value", typedValueType(typedValue))));
        return fields.build();
    }

    private static Type typedValueType(TypedValue typedValue)
    {
        return switch (typedValue) {
            case PrimitiveValue primitive -> primitive.type();
            case ObjectValue object -> RowType.from(object.fields().stream()
                    .map(field -> field(field.name().toLowerCase(ENGLISH), RowType.from(valueFields(field.value()))))
                    .collect(toImmutableList()));
            case ArrayValue array -> new ArrayType(RowType.from(valueFields(array.element())));
        };
    }

    /// Returns the Parquet group of a VARIANT with this schema, in the layout of the
    /// specification: a `required` `metadata` column, an `optional` `value` column in
    /// each group, `required` object field groups, and three-level lists with
    /// `required` element groups. Without a top-level `typed_value`, the group has the
    /// unshredded layout, in which `value` is `required`.
    public GroupType toParquetType(String name, Repetition repetition)
    {
        checkArgument(repetition != REPEATED, "VARIANT group is repeated: %s", name);
        Types.GroupBuilder<GroupType> group = Types.buildGroup(repetition)
                .as(variantType(Header.VERSION))
                .addField(Types.required(BINARY).named("metadata"));
        if (value.typedValue().isEmpty()) {
            return group.addField(Types.required(BINARY).named("value")).named(name);
        }
        return group.addFields(valueParquetFields(value)).named(name);
    }

    private static org.apache.parquet.schema.Type[] valueParquetFields(ShreddedValue value)
    {
        // A group always has a value column, which holds the values that typed_value cannot hold
        ImmutableList.Builder<org.apache.parquet.schema.Type> fields = ImmutableList.builder();
        fields.add(Types.optional(BINARY).named("value"));
        value.typedValue().ifPresent(typedValue -> fields.add(typedValueParquetType(typedValue)));
        return fields.build().toArray(new org.apache.parquet.schema.Type[0]);
    }

    private static org.apache.parquet.schema.Type typedValueParquetType(TypedValue typedValue)
    {
        return switch (typedValue) {
            case PrimitiveValue primitive -> primitiveParquetType(primitive).named("typed_value");
            case ObjectValue object -> {
                Types.GroupBuilder<GroupType> group = Types.buildGroup(OPTIONAL);
                for (ObjectField field : object.fields()) {
                    group.addField(Types.buildGroup(REQUIRED).addFields(valueParquetFields(field.value())).named(field.name()));
                }
                yield group.named("typed_value");
            }
            case ArrayValue array -> Types.buildGroup(OPTIONAL)
                    .as(listType())
                    .addField(Types.buildGroup(REPEATED)
                            .addField(Types.buildGroup(REQUIRED).addFields(valueParquetFields(array.element())).named("element"))
                            .named("list"))
                    .named("typed_value");
        };
    }

    private static Types.PrimitiveBuilder<PrimitiveType> primitiveParquetType(PrimitiveValue primitive)
    {
        return switch (primitive.shreddedType()) {
            case BOOLEAN -> Types.optional(PrimitiveTypeName.BOOLEAN);
            case INT8 -> Types.optional(INT32).as(intType(8, true));
            case INT16 -> Types.optional(INT32).as(intType(16, true));
            case INT32 -> Types.optional(INT32);
            case INT64 -> Types.optional(INT64);
            case FLOAT -> Types.optional(PrimitiveTypeName.FLOAT);
            case DOUBLE -> Types.optional(PrimitiveTypeName.DOUBLE);
            case DECIMAL4 -> Types.optional(INT32).as(decimalAnnotation(primitive));
            case DECIMAL8 -> Types.optional(INT64).as(decimalAnnotation(primitive));
            case DECIMAL16 -> Types.optional(FIXED_LEN_BYTE_ARRAY)
                    .length(decimalByteLength(((DecimalType) primitive.type()).getPrecision()))
                    .as(decimalAnnotation(primitive));
            case DATE -> Types.optional(INT32).as(dateType());
            case TIME_MICROS -> Types.optional(INT64).as(timeType(false, MICROS));
            case TIMESTAMP_MICROS -> Types.optional(INT64).as(timestampType(false, MICROS));
            case TIMESTAMP_NANOS -> Types.optional(INT64).as(timestampType(false, NANOS));
            case TIMESTAMP_TZ_MICROS -> Types.optional(INT64).as(timestampType(true, MICROS));
            case TIMESTAMP_TZ_NANOS -> Types.optional(INT64).as(timestampType(true, NANOS));
            case BINARY -> Types.optional(BINARY);
            case STRING -> Types.optional(BINARY).as(stringType());
            case UUID -> Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(uuidType());
        };
    }

    private static LogicalTypeAnnotation decimalAnnotation(PrimitiveValue primitive)
    {
        DecimalType decimal = (DecimalType) primitive.type();
        return decimalType(decimal.getScale(), decimal.getPrecision());
    }

    /// The fewest bytes of a two's complement number that hold every unscaled value of the precision
    private static int decimalByteLength(int precision)
    {
        int bits = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE).bitLength() + 1;
        return (bits + Byte.SIZE - 1) / Byte.SIZE;
    }

    private static ShreddedValue parseValue(GroupType group, ParquetOriginalFieldNames names, boolean variantGroup, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        checkMatchingNames(group, names);

        boolean hasMetadata = false;
        boolean hasValue = false;
        Optional<TypedValue> typedValue = Optional.empty();
        for (int index = 0; index < group.getFieldCount(); index++) {
            org.apache.parquet.schema.Type field = group.getType(index);
            ParquetOriginalFieldNames fieldNames = names.children().get(index);
            String name = fieldNames.name();
            if (variantGroup && name.equals("metadata") && !hasMetadata) {
                checkBinary(field, names, dataSourceId);
                hasMetadata = true;
            }
            else if (name.equals("value") && !hasValue) {
                checkBinary(field, names, dataSourceId);
                hasValue = true;
            }
            else if (name.equals("typed_value") && typedValue.isEmpty()) {
                typedValue = Optional.of(parseTypedValue(field, fieldNames, dataSourceId));
            }
            else {
                throw new ParquetCorruptionException(dataSourceId, "Unexpected field %s in shredded VARIANT group %s", name, names);
            }
        }

        if (variantGroup && !hasMetadata) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT group %s has no metadata field", names);
        }
        if (!hasValue && typedValue.isEmpty()) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT group %s has neither a value nor a typed_value field", names);
        }
        return new ShreddedValue(typedValue);
    }

    private static TypedValue parseTypedValue(org.apache.parquet.schema.Type field, ParquetOriginalFieldNames names, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        if (field.isRepetition(REPEATED)) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT field %s is repeated", names);
        }
        if (field.isPrimitive()) {
            return primitiveValue(field.asPrimitiveType())
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported shredded VARIANT value type: " + field));
        }

        GroupType group = field.asGroupType();
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        if (annotation instanceof ListLogicalTypeAnnotation) {
            return parseArray(group, names, dataSourceId);
        }
        if (annotation != null) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported shredded VARIANT value type: " + field);
        }
        return parseObject(group, names, dataSourceId);
    }

    private static ArrayValue parseArray(GroupType group, ParquetOriginalFieldNames names, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        checkMatchingNames(group, names);
        // A list has a repeated group with one element field
        if (group.getFieldCount() != 1 || group.getType(0).isPrimitive() || !group.getType(0).isRepetition(REPEATED)) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT list %s does not have a repeated group", names);
        }
        GroupType repeated = group.getType(0).asGroupType();
        ParquetOriginalFieldNames repeatedNames = names.children().getFirst();
        checkMatchingNames(repeated, repeatedNames);
        // ParquetTypeUtils.getArrayElementColumn reads a repeated group with these names as the element of a
        // two-level list, which the specification does not allow
        if (repeated.getLogicalTypeAnnotation() != null || repeated.getName().equals("array") || repeated.getName().equals(group.getName() + "_tuple")) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT list %s does not have the three-level list structure", names);
        }
        if (repeated.getFieldCount() != 1 || repeated.getType(0).isPrimitive() || repeated.getType(0).isRepetition(REPEATED)) {
            throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT list %s does not have an element group", names);
        }
        return new ArrayValue(parseValue(repeated.getType(0).asGroupType(), repeatedNames.children().getFirst(), false, dataSourceId));
    }

    private static ObjectValue parseObject(GroupType group, ParquetOriginalFieldNames names, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        checkMatchingNames(group, names);
        ImmutableList.Builder<ObjectField> fields = ImmutableList.builder();
        Set<String> fieldNames = new HashSet<>();
        Set<String> lowercaseFieldNames = new HashSet<>();
        for (int index = 0; index < group.getFieldCount(); index++) {
            org.apache.parquet.schema.Type field = group.getType(index);
            ParquetOriginalFieldNames fieldOriginalNames = names.children().get(index);
            String name = fieldOriginalNames.name();
            if (!fieldNames.add(name)) {
                throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT object %s has duplicate field %s", names, name);
            }
            // The reader finds Parquet columns by lowercase name, so it cannot tell these fields apart
            if (!lowercaseFieldNames.add(name.toLowerCase(ENGLISH))) {
                throw new TrinoException(NOT_SUPPORTED, "Shredded VARIANT object fields that differ only by case are not supported: %s in %s".formatted(name, names));
            }
            if (field.isPrimitive() || field.isRepetition(REPEATED)) {
                throw new ParquetCorruptionException(dataSourceId, "Shredded VARIANT object field %s in %s is not a group", name, names);
            }
            fields.add(new ObjectField(name, parseValue(field.asGroupType(), fieldOriginalNames, false, dataSourceId)));
        }
        return new ObjectValue(fields.build());
    }

    private static void checkBinary(org.apache.parquet.schema.Type field, ParquetOriginalFieldNames groupNames, ParquetDataSourceId dataSourceId)
            throws ParquetCorruptionException
    {
        if (!field.isPrimitive() || field.isRepetition(REPEATED) || field.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
            throw new ParquetCorruptionException(dataSourceId, "Field %s in shredded VARIANT group %s is not binary", field.getName(), groupNames);
        }
    }

    private static void checkMatchingNames(GroupType group, ParquetOriginalFieldNames names)
    {
        checkArgument(group.getFieldCount() == names.children().size(), "Original field names do not match Parquet group %s", group.getName());
        for (int index = 0; index < group.getFieldCount(); index++) {
            checkArgument(
                    names.children().get(index).name().toLowerCase(ENGLISH).equals(group.getType(index).getName()),
                    "Original field names do not match Parquet group %s",
                    group.getName());
        }
    }

    private static Optional<TypedValue> primitiveValue(PrimitiveType type)
    {
        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        return switch (type.getPrimitiveTypeName()) {
            case BOOLEAN -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.BOOLEAN, BOOLEAN);
                default -> Optional.empty();
            };
            case INT32 -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.INT32, INTEGER);
                case IntLogicalTypeAnnotation integer when integer.isSigned() && integer.getBitWidth() == 8 -> primitiveValue(ShreddedType.INT8, TINYINT);
                case IntLogicalTypeAnnotation integer when integer.isSigned() && integer.getBitWidth() == 16 -> primitiveValue(ShreddedType.INT16, SMALLINT);
                case IntLogicalTypeAnnotation integer when integer.isSigned() && integer.getBitWidth() == 32 -> primitiveValue(ShreddedType.INT32, INTEGER);
                case DateLogicalTypeAnnotation _ -> primitiveValue(ShreddedType.DATE, DATE);
                case DecimalLogicalTypeAnnotation decimal -> decimalValue(ShreddedType.DECIMAL4, decimal, 9);
                default -> Optional.empty();
            };
            case INT64 -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.INT64, BIGINT);
                case IntLogicalTypeAnnotation integer when integer.isSigned() && integer.getBitWidth() == 64 -> primitiveValue(ShreddedType.INT64, BIGINT);
                case DecimalLogicalTypeAnnotation decimal -> decimalValue(ShreddedType.DECIMAL8, decimal, MAX_SHORT_PRECISION);
                case TimeLogicalTypeAnnotation time when !time.isAdjustedToUTC() && time.getUnit() == MICROS -> primitiveValue(ShreddedType.TIME_MICROS, TIME_MICROS);
                case TimestampLogicalTypeAnnotation timestamp when timestamp.isAdjustedToUTC() && timestamp.getUnit() == MICROS -> primitiveValue(ShreddedType.TIMESTAMP_TZ_MICROS, TIMESTAMP_TZ_MICROS);
                case TimestampLogicalTypeAnnotation timestamp when timestamp.isAdjustedToUTC() && timestamp.getUnit() == NANOS -> primitiveValue(ShreddedType.TIMESTAMP_TZ_NANOS, TIMESTAMP_TZ_NANOS);
                case TimestampLogicalTypeAnnotation timestamp when !timestamp.isAdjustedToUTC() && timestamp.getUnit() == MICROS -> primitiveValue(ShreddedType.TIMESTAMP_MICROS, TIMESTAMP_MICROS);
                case TimestampLogicalTypeAnnotation timestamp when !timestamp.isAdjustedToUTC() && timestamp.getUnit() == NANOS -> primitiveValue(ShreddedType.TIMESTAMP_NANOS, TIMESTAMP_NANOS);
                default -> Optional.empty();
            };
            case FLOAT -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.FLOAT, REAL);
                default -> Optional.empty();
            };
            case DOUBLE -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.DOUBLE, DOUBLE);
                default -> Optional.empty();
            };
            case BINARY -> switch (annotation) {
                case null -> primitiveValue(ShreddedType.BINARY, VARBINARY);
                case StringLogicalTypeAnnotation _ -> primitiveValue(ShreddedType.STRING, VARCHAR);
                case DecimalLogicalTypeAnnotation decimal -> decimalValue(ShreddedType.DECIMAL16, decimal, MAX_PRECISION);
                default -> Optional.empty();
            };
            case FIXED_LEN_BYTE_ARRAY -> switch (annotation) {
                case UUIDLogicalTypeAnnotation _ when type.getTypeLength() == 16 -> primitiveValue(ShreddedType.UUID, UUID);
                case DecimalLogicalTypeAnnotation decimal -> decimalValue(ShreddedType.DECIMAL16, decimal, MAX_PRECISION);
                case null, default -> Optional.empty();
            };
            case INT96 -> Optional.empty();
        };
    }

    private static Optional<TypedValue> decimalValue(ShreddedType shreddedType, DecimalLogicalTypeAnnotation decimal, int maxPrecision)
    {
        if (decimal.getPrecision() > maxPrecision) {
            return Optional.empty();
        }
        return primitiveValue(shreddedType, createDecimalType(decimal.getPrecision(), decimal.getScale()));
    }

    private static Optional<TypedValue> primitiveValue(ShreddedType shreddedType, Type type)
    {
        return Optional.of(new PrimitiveValue(shreddedType, type));
    }

    /// The Variant types that a `typed_value` column can hold.
    public enum ShreddedType
    {
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        DECIMAL4,
        DECIMAL8,
        DECIMAL16,
        DATE,
        TIME_MICROS,
        TIMESTAMP_MICROS,
        TIMESTAMP_NANOS,
        TIMESTAMP_TZ_MICROS,
        TIMESTAMP_TZ_NANOS,
        BINARY,
        STRING,
        UUID,
    }

    /// A group with a `value` column, a `typed_value` column, or both. A missing
    /// `value` column is read as null.
    public record ShreddedValue(Optional<TypedValue> typedValue)
    {
        public ShreddedValue
        {
            requireNonNull(typedValue, "typedValue is null");
        }
    }

    public sealed interface TypedValue
            permits ArrayValue,
                    ObjectValue,
                    PrimitiveValue {}

    /// A `typed_value` column of a Variant primitive type, read as `type`.
    public record PrimitiveValue(ShreddedType shreddedType, Type type)
            implements TypedValue
    {
        public PrimitiveValue
        {
            requireNonNull(shreddedType, "shreddedType is null");
            requireNonNull(type, "type is null");
            checkArgument(holds(type, shreddedType), "Type %s does not hold shredded VARIANT values of type %s", type, shreddedType);
        }

        /// Returns a column of a type that is not a decimal.
        public static PrimitiveValue of(ShreddedType shreddedType)
        {
            return new PrimitiveValue(shreddedType, nonDecimalType(shreddedType));
        }

        /// Returns a column of DECIMAL4, DECIMAL8 or DECIMAL16 values with the precision and scale.
        public static PrimitiveValue decimal(ShreddedType shreddedType, int precision, int scale)
        {
            return new PrimitiveValue(shreddedType, createDecimalType(precision, scale));
        }

        private static boolean holds(Type type, ShreddedType shreddedType)
        {
            return switch (shreddedType) {
                case DECIMAL4 -> type instanceof DecimalType decimal && decimal.getPrecision() <= 9;
                case DECIMAL8 -> type instanceof DecimalType decimal && decimal.getPrecision() <= MAX_SHORT_PRECISION;
                case DECIMAL16 -> type instanceof DecimalType;
                case BOOLEAN, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, DATE, TIME_MICROS, TIMESTAMP_MICROS, TIMESTAMP_NANOS,
                     TIMESTAMP_TZ_MICROS, TIMESTAMP_TZ_NANOS, BINARY, STRING, UUID -> type.equals(nonDecimalType(shreddedType));
            };
        }

        private static Type nonDecimalType(ShreddedType shreddedType)
        {
            return switch (shreddedType) {
                case BOOLEAN -> BOOLEAN;
                case INT8 -> TINYINT;
                case INT16 -> SMALLINT;
                case INT32 -> INTEGER;
                case INT64 -> BIGINT;
                case FLOAT -> REAL;
                case DOUBLE -> DOUBLE;
                case DECIMAL4, DECIMAL8, DECIMAL16 -> throw new IllegalArgumentException("A decimal column needs a precision and a scale: " + shreddedType);
                case DATE -> DATE;
                case TIME_MICROS -> TIME_MICROS;
                case TIMESTAMP_MICROS -> TIMESTAMP_MICROS;
                case TIMESTAMP_NANOS -> TIMESTAMP_NANOS;
                case TIMESTAMP_TZ_MICROS -> TIMESTAMP_TZ_MICROS;
                case TIMESTAMP_TZ_NANOS -> TIMESTAMP_TZ_NANOS;
                case BINARY -> VARBINARY;
                case STRING -> VARCHAR;
                case UUID -> UUID;
            };
        }
    }

    /// A `typed_value` group with one group for each shredded object field.
    public record ObjectValue(List<ObjectField> fields)
            implements TypedValue
    {
        public ObjectValue
        {
            fields = ImmutableList.copyOf(fields);
        }
    }

    /// A shredded object field. The name is the variant key, in its original case.
    public record ObjectField(String name, ShreddedValue value)
    {
        public ObjectField
        {
            requireNonNull(name, "name is null");
            requireNonNull(value, "value is null");
        }
    }

    /// A `typed_value` list. Each element is a group with `value` and `typed_value` columns.
    public record ArrayValue(ShreddedValue element)
            implements TypedValue
    {
        public ArrayValue
        {
            requireNonNull(element, "element is null");
        }
    }
}
