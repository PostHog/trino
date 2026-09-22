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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.math.BigInteger;

import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_NOT_DISTINCT_FROM;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * uint64 has an unsigned physical INT64 and a lossless SQL DECIMAL(20,0).
 */
final class HoglakeUnsigned
{
    private HoglakeUnsigned() {}

    public static boolean needsConversion(HoglakeColumnHandle column)
    {
        return column.hoglakeType().equals("uint64") || column.children().stream().anyMatch(HoglakeUnsigned::needsConversion);
    }

    public static Type physicalType(HoglakeColumnHandle column)
    {
        if (!needsConversion(column)) {
            return column.type();
        }
        return switch (column.hoglakeType()) {
            case "uint64" -> BIGINT;
            case "list" -> new ArrayType(physicalType(column.children().getFirst()));
            case "map" -> new MapType(physicalType(column.children().get(0)), physicalType(column.children().get(1)), new TypeOperators());
            case "struct" -> RowType.from(column.children().stream().map(child -> RowType.field(child.name(), physicalType(child))).toList());
            default -> throw new IllegalArgumentException("Unexpected unsigned container: " + column);
        };
    }

    public static Block convert(HoglakeColumnHandle column, Block input, boolean writing)
    {
        if (!needsConversion(column)) {
            return input;
        }
        Type source = writing ? column.type() : physicalType(column);
        Type target = writing ? physicalType(column) : column.type();
        BlockBuilder output = target.createBlockBuilder(null, input.getPositionCount());
        for (int position = 0; position < input.getPositionCount(); position++) {
            if (input.isNull(position)) {
                output.appendNull();
                continue;
            }
            switch (column.hoglakeType()) {
                case "uint64" -> {
                    if (writing) {
                        BIGINT.writeLong(output, ((Int128) source.getObject(input, position)).toBigInteger().longValue());
                    }
                    else {
                        long value = BIGINT.getLong(input, position);
                        BigInteger unsigned = BigInteger.valueOf(value & Long.MAX_VALUE);
                        if (value < 0) {
                            unsigned = unsigned.setBit(63);
                        }
                        target.writeObject(output, Int128.valueOf(unsigned));
                    }
                }
                case "list" -> {
                    Block elements = ((ArrayType) source).getObject(input, position);
                    target.writeObject(output, convert(column.children().getFirst(), elements, writing));
                }
                case "map" -> {
                    SqlMap map = ((MapType) source).getObject(input, position);
                    Block keys = map.getRawKeyBlock().getRegion(map.getRawOffset(), map.getSize());
                    Block values = map.getRawValueBlock().getRegion(map.getRawOffset(), map.getSize());
                    target.writeObject(output, new SqlMap(
                            (MapType) target,
                            STRICT_NOT_DISTINCT_FROM,
                            convert(column.children().get(0), keys, writing),
                            convert(column.children().get(1), values, writing)));
                }
                case "struct" -> {
                    SqlRow row = ((RowType) source).getObject(input, position);
                    Block[] fields = new Block[column.children().size()];
                    for (int index = 0; index < fields.length; index++) {
                        fields[index] = convert(column.children().get(index), row.getRawFieldBlock(index).getRegion(row.getRawIndex(), 1), writing);
                    }
                    target.writeObject(output, new SqlRow(0, fields));
                }
                default -> throw new IllegalArgumentException("Unexpected unsigned container: " + column);
            }
        }
        return output.build();
    }
}
