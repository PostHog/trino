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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Bind every catalog node by stable field ID, including fields inside containers.
 */
final class HoglakeParquetFields
{
    private HoglakeParquetFields() {}

    public static Optional<Field> construct(HoglakeColumnHandle column, ColumnIO physical)
    {
        if (physical == null) {
            return Optional.empty();
        }
        if (column.type().equals(VARIANT)) {
            GroupColumnIO group = (GroupColumnIO) physical;
            if (group.getChildrenCount() != 2 || group.getChild("metadata") == null || group.getChild("value") == null ||
                    group.getChild("metadata").getType().getRepetition() != REQUIRED ||
                    group.getChild("value").getType().getRepetition() != REQUIRED) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake supports only unshredded native VARIANT files");
            }
            PrimitiveField value = (PrimitiveField) constructField(VARBINARY, group.getChild("value")).orElseThrow();
            PrimitiveField metadata = (PrimitiveField) constructField(VARBINARY, group.getChild("metadata")).orElseThrow();
            return Optional.of(new VariantField(
                    column.type(),
                    physical.getRepetitionLevel(),
                    physical.getDefinitionLevel(),
                    physical.getType().getRepetition() != OPTIONAL,
                    new PrimitiveField(value.getType(), false, value.getDescriptor(), value.getId()),
                    new PrimitiveField(metadata.getType(), false, metadata.getDescriptor(), metadata.getId())));
        }
        List<Optional<Field>> children = new ArrayList<>();
        if (column.type() instanceof RowType) {
            GroupColumnIO group = (GroupColumnIO) physical;
            for (HoglakeColumnHandle child : column.children()) {
                children.add(construct(child, bind(group, child)));
            }
            if (children.stream().allMatch(Optional::isEmpty)) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot recover nested row nullability when every historical field is absent: " + column.name());
            }
        }
        else if (column.type() instanceof ArrayType) {
            GroupColumnIO group = (GroupColumnIO) physical;
            children.add(construct(column.children().getFirst(), getArrayElementColumn(group.getChild(0))));
        }
        else if (column.type() instanceof MapType) {
            GroupColumnIO entries = getMapKeyValueColumn((GroupColumnIO) physical);
            children.add(construct(column.children().get(0), entries.getChild(0)));
            children.add(construct(column.children().get(1), entries.getChild(1)));
        }
        else {
            return constructField(HoglakeUnsigned.physicalType(column), physical);
        }
        return Optional.of(new GroupField(
                column.type(),
                physical.getRepetitionLevel(),
                physical.getDefinitionLevel(),
                physical.getType().getRepetition() != OPTIONAL,
                List.copyOf(children)));
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
