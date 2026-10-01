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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
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

import java.util.HashSet;
import java.util.List;
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
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

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

    /// The `value` and `typed_value` columns of the VARIANT group.
    public ShreddedValue value()
    {
        return value;
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
