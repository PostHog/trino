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

import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.VariantField;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Bind every catalog node by stable field ID, including fields inside containers.
 */
final class HoglakeParquetFields
{
    private HoglakeParquetFields() {}

    public static Optional<Field> construct(HoglakeColumnHandle column, ColumnIO physical)
    {
        return construct(column, physical, false);
    }

    /**
     * Whether the file stores a VARIANT column in shredded form, with a
     * {@code typed_value} field. {@link HoglakePageSourceProvider} reads
     * shredded top-level columns; {@link #construct} rejects shredded
     * VARIANT fields inside rows, lists, and maps.
     */
    public static boolean isShreddedVariant(HoglakeColumnHandle column, org.apache.parquet.schema.Type physical)
    {
        return column.type().equals(VARIANT) && !physical.isPrimitive() && physical.asGroupType().containsField("typed_value");
    }

    /**
     * @param mayRepeat whether the field may be repeated, as the element of a legacy 2-level
     *         list is: that list has no element field, so each element is the list's repeated
     *         field itself
     */
    private static Optional<Field> construct(HoglakeColumnHandle column, ColumnIO physical, boolean mayRepeat)
    {
        if (physical == null) {
            return Optional.empty();
        }
        // A repeated field holds any number of values per row. Read as anything but a list
        // element, it returns one value per element instead of one per row.
        if (physical.getType().isRepetition(REPEATED) && !mayRepeat) {
            throw unsupportedField(column, physical, "the field is repeated");
        }
        if (column.type().equals(VARIANT)) {
            if (physical instanceof GroupColumnIO group && group.getChild("typed_value") != null) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake supports shredded VARIANT files only in top-level columns: " + column.name());
            }
            // Another tool can store the column as a primitive, such as a JSON string
            if (!(physical instanceof GroupColumnIO group) || group.getChildrenCount() != 2) {
                throw unsupportedVariant(column);
            }
            PrimitiveColumnIO metadata = variantLeaf(column, group, "metadata");
            PrimitiveColumnIO value = variantLeaf(column, group, "value");
            // The leaves must stay required: the reader then returns one entry per non-null
            // variant, the null-suppressed layout ParquetReader.readVariant expects.
            return Optional.of(new VariantField(
                    column.type(),
                    physical.getRepetitionLevel(),
                    physical.getDefinitionLevel(),
                    physical.getType().getRepetition() != OPTIONAL,
                    constructField(VARBINARY, value).orElseThrow(),
                    constructField(VARBINARY, metadata).orElseThrow()));
        }
        List<Optional<Field>> children = new ArrayList<>();
        if (column.type() instanceof RowType) {
            GroupColumnIO group = asGroup(column, physical);
            for (HoglakeColumnHandle child : column.children()) {
                children.add(construct(child, bind(group, child), false));
            }
            if (children.stream().allMatch(Optional::isEmpty)) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot recover nested row nullability when every historical field is absent: " + column.name());
            }
        }
        else if (column.type() instanceof ArrayType) {
            ColumnIO element = listElement(column, physical);
            // Only the list's own repeated field is one level below the list. A repeated field
            // inside that one, as in a 3-level list, is a nested list.
            children.add(construct(column.children().getFirst(), element, element.getRepetitionLevel() == physical.getRepetitionLevel() + 1));
        }
        else if (column.type() instanceof MapType) {
            GroupColumnIO entries = mapEntries(column, physical);
            children.add(construct(column.children().get(0), entries.getChild(0), false));
            children.add(construct(column.children().get(1), entries.getChild(1), false));
        }
        else {
            if (!(physical instanceof PrimitiveColumnIO primitive)) {
                throw unsupportedField(column, physical, "the field is a group");
            }
            // ParquetTypeUtils.constructField assumes a top-level field, so it would reject the
            // repeated element of a legacy 2-level list
            return Optional.of(new PrimitiveField(
                    HoglakeUnsigned.physicalType(column),
                    primitive.getType().getRepetition() != OPTIONAL,
                    primitive.getColumnDescriptor(),
                    primitive.getId()));
        }
        return Optional.of(new GroupField(
                column.type(),
                physical.getRepetitionLevel(),
                physical.getDefinitionLevel(),
                physical.getType().getRepetition() != OPTIONAL,
                List.copyOf(children)));
    }

    private static PrimitiveColumnIO variantLeaf(HoglakeColumnHandle column, GroupColumnIO variant, String name)
    {
        if (!(variant.getChild(name) instanceof PrimitiveColumnIO leaf) ||
                leaf.getPrimitive() != BINARY ||
                leaf.getType().getRepetition() != REQUIRED) {
            throw unsupportedVariant(column);
        }
        return leaf;
    }

    private static TrinoException unsupportedVariant(HoglakeColumnHandle column)
    {
        return new TrinoException(NOT_SUPPORTED, "Hoglake supports only unshredded native VARIANT files: " + column.name());
    }

    private static GroupColumnIO asGroup(HoglakeColumnHandle column, ColumnIO physical)
    {
        if (physical instanceof GroupColumnIO group) {
            return group;
        }
        throw unsupportedField(column, physical, "the field is a primitive");
    }

    /**
     * The element of a list. Without a repeated field, such as in a struct, the reader would
     * return each row as a list of one element.
     */
    private static ColumnIO listElement(HoglakeColumnHandle column, ColumnIO physical)
    {
        GroupColumnIO list = asGroup(column, physical);
        if (list.getChildrenCount() == 1) {
            ColumnIO element = getArrayElementColumn(list.getChild(0));
            // The elements repeat below the list
            if (element.getRepetitionLevel() > physical.getRepetitionLevel()) {
                return element;
            }
        }
        throw unsupportedField(column, physical, "the field is not a list");
    }

    /**
     * The repeated group of a map's keys and values. Like
     * {@link io.trino.parquet.ParquetTypeUtils#getMapKeyValueColumn}, this skips groups with
     * a single child, but it checks each step instead of casting.
     */
    private static GroupColumnIO mapEntries(HoglakeColumnHandle column, ColumnIO physical)
    {
        GroupColumnIO entries = asGroup(column, physical);
        while (entries.getChildrenCount() == 1 && entries.getChild(0) instanceof GroupColumnIO child) {
            entries = child;
        }
        // Each key and value repeat one level below the map, once per entry
        if (entries.getChildrenCount() != 2 ||
                !entries.getType().isRepetition(REPEATED) ||
                entries.getRepetitionLevel() != physical.getRepetitionLevel() + 1) {
            throw unsupportedField(column, physical, "the field is not a map");
        }
        return entries;
    }

    /**
     * A file field that the column's type cannot be read from.
     */
    private static TrinoException unsupportedField(HoglakeColumnHandle column, ColumnIO physical, String reason)
    {
        return new TrinoException(NOT_SUPPORTED, "Unsupported Parquet field %s for column %s of type %s: %s".formatted(
                String.join(".", physical.getFieldPath()),
                column.name(),
                column.type().getDisplayName(),
                reason));
    }

    private static ColumnIO bind(GroupColumnIO group, HoglakeColumnHandle column)
    {
        for (int index = 0; index < group.getChildrenCount(); index++) {
            ColumnIO child = group.getChild(index);
            if (child.getType().getId() != null && child.getType().getId().intValue() == column.fieldId()) {
                return child;
            }
        }
        for (int index = 0; index < group.getChildrenCount(); index++) {
            ColumnIO child = group.getChild(index);
            if (child.getType().getId() == null && child.getName().equals(column.name())) {
                return child;
            }
        }
        for (int index = 0; index < group.getChildrenCount(); index++) {
            ColumnIO child = group.getChild(index);
            if (child.getType().getId() == null && child.getName().equalsIgnoreCase(column.name())) {
                return child;
            }
        }
        return null;
    }
}
