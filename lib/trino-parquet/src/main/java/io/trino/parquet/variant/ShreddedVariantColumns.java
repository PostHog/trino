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

import io.trino.spi.block.Block;

import java.util.List;

import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

/// The columns of a shredded VARIANT group for a batch of rows, which
/// [ShreddedVariantAssembler] reads. The assembler of a pruned value asks for the
/// top-level `metadata` and `value` columns only when a row needs them, so a source can
/// read them on first use.
public interface ShreddedVariantColumns
{
    int positionCount();

    /// Whether the VARIANT group is null at `position`.
    boolean isNull(int position);

    /// The `metadata` column.
    Block metadata();

    /// The top-level `value` column.
    Block value();

    /// The top-level `typed_value` column. Only a group whose schema has it has it.
    Block typedValue();

    /// The columns of a block of [VariantShreddingSchema#physicalType()].
    static ShreddedVariantColumns of(Block group)
    {
        return new RowColumns(group);
    }

    final class RowColumns
            implements ShreddedVariantColumns
    {
        private final Block group;
        private final List<Block> fields;

        private RowColumns(Block group)
        {
            this.group = requireNonNull(group, "group is null");
            this.fields = getRowFieldsFromBlock(group);
        }

        @Override
        public int positionCount()
        {
            return group.getPositionCount();
        }

        @Override
        public boolean isNull(int position)
        {
            return group.isNull(position);
        }

        @Override
        public Block metadata()
        {
            return fields.get(0);
        }

        @Override
        public Block value()
        {
            return fields.get(1);
        }

        @Override
        public Block typedValue()
        {
            return fields.get(2);
        }
    }
}
