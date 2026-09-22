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
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
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
    void repeatedAndDictionaryKeysStayEncoded()
    {
        var type = VarcharType.VARCHAR;
        var values = type.createBlockBuilder(null, 1);
        type.writeString(values, "x".repeat(4096));
        var leaf = new HoglakeColumnHandle("k", 2, type, true);
        var row = new HoglakeColumnHandle("r", 1, RowType.from(List.of(RowType.field("k", type))), true, List.of(leaf), "struct");
        Block one = RowBlock.fromFieldBlocks(1, new Block[] {values.build()});
        Block repeated = RunLengthEncodedBlock.create(one, 1000);
        Block dictionary = DictionaryBlock.create(1000, one, new int[1000]);
        for (Block input : List.of(repeated, dictionary)) {
            PageSorter checkingSorter = (_, pages, _, _, _) -> {
                Block key = pages.getFirst().getBlock(1);
                assertThat(key.getRetainedSizeInBytes()).isLessThan(20_000);
                assertThat(key.getPositionCount()).isEqualTo(1000);
                return pages.iterator();
            };
            HoglakeSorting.sort(
                    checkingSorter,
                    List.of(new Page(input)),
                    List.of(row),
                    HoglakeSorting.parse(List.of("r.k"), List.of(row)));
        }
    }

    @Test
    void floatingOrderMatchesNativeHoglakeInBothDirections()
    {
        for (io.trino.spi.type.Type type : List.of(RealType.REAL, DoubleType.DOUBLE)) {
            var key = new HoglakeColumnHandle("k", 1, type, true);
            var tie = new HoglakeColumnHandle("tie", 2, BIGINT, true);
            var values = type.createBlockBuilder(null, 8);
            double[] inputs = {0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0, 1.0};
            for (double value : inputs) {
                if (type.equals(RealType.REAL)) {
                    type.writeLong(values, Float.floatToRawIntBits((float) value));
                }
                else {
                    type.writeDouble(values, value);
                }
            }
            values.appendNull();
            var columns = List.of(key, tie);
            Page input = new Page(values.build(), block(0, 1, 2, 3, 4, 5, 6, 7));
            for (String direction : List.of("ASC", "DESC")) {
                var sorted = HoglakeSorting.sort(
                        new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                        List.of(input),
                        columns,
                        HoglakeSorting.parse(List.of("k " + direction + " NULLS LAST", "tie ASC"), columns));
                List<Long> actual = new ArrayList<>();
                while (sorted.hasNext()) {
                    Page page = sorted.next();
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        actual.add(BIGINT.getLong(page.getBlock(1), position));
                    }
                }
                assertThat(actual).isEqualTo(direction.equals("ASC") ? List.of(4L, 5L, 1L, 0L, 6L, 3L, 2L, 7L) : List.of(2L, 3L, 6L, 0L, 1L, 5L, 4L, 7L));
            }
        }
    }

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
