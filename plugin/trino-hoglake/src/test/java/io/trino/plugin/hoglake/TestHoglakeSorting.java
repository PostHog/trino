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

import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeSorting
{
    @Test
    void nestedNullParentAndSecondaryOrder()
    {
        RowType type = RowType.from(List.of(RowType.field("k", BIGINT)));
        var child = new HoglakeColumnHandle("k", 2, BIGINT, true);
        var row = new HoglakeColumnHandle("r", 1, type, true, List.of(child), "struct");
        var value = new HoglakeColumnHandle("v", 3, BIGINT, true);
        var columns = List.of(row, value);
        var fields = HoglakeSorting.parse(List.of("r.k ASC NULLS LAST", "v DESC"), columns);
        // Parent nullness must remain a null sort key.
        Block rows = RowBlock.fromNotNullSuppressedFieldBlocks(4, Optional.of(new long[] {0b1110}), new Block[] {nullableBlock()});
        var sorted = HoglakeSorting.sort(
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                List.of(new Page(rows, block(4, 9, 3, 7))),
                columns,
                fields);
        List<Long> actual = new ArrayList<>();
        while (sorted.hasNext()) {
            Page page = sorted.next();
            for (int position = 0; position < page.getPositionCount(); position++) {
                actual.add(BIGINT.getLong(page.getBlock(1), position));
            }
        }
        assertThat(actual).containsExactly(7L, 9L, 3L, 4L);
        assertThatThrownBy(() -> HoglakeSorting.parse(List.of("v", "v DESC"), columns)).hasMessageContaining("Duplicate");
    }

    private static Block nullableBlock()
    {
        var block = BIGINT.createBlockBuilder(null, 4);
        block.appendNull();
        BIGINT.writeLong(block, 2);
        BIGINT.writeLong(block, 2);
        BIGINT.writeLong(block, 1);
        return block.build();
    }

    private static Block block(long... values)
    {
        var block = BIGINT.createBlockBuilder(null, values.length);
        for (long value : values) {
            BIGINT.writeLong(block, value);
        }
        return block.build();
    }
}
