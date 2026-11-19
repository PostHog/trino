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
package io.trino.plugin.ducklake.util;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.ducklake.util.DuckLakeTypes.isJson;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Converts {@code JSON} values to the text DuckLake stores them as.
 * <p>
 * A Trino {@code JSON} block holds each value either as the text it arrived as or in a binary
 * encoding of its own, so its bytes cannot be written to a file as they are. DuckDB stores a
 * {@code json} column as UTF-8 text, so every value is written as its JSON text. Values of other
 * types, including those beside a {@code JSON} field in a row, are copied unchanged.
 */
public final class DuckLakeJsonValues
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private DuckLakeJsonValues() {}

    /**
     * Whether the type is {@code JSON} or holds {@code JSON} values at any depth.
     */
    public static boolean containsJson(Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> containsJson(arrayType.getElementType());
            case MapType mapType -> containsJson(mapType.getKeyType()) || containsJson(mapType.getValueType());
            case RowType rowType -> rowType.getFields().stream().anyMatch(field -> containsJson(field.getType()));
            default -> isJson(type);
        };
    }

    /**
     * Rewrites a block of the given type so that each {@code JSON} value in it becomes a
     * {@code VARCHAR} holding its JSON text.
     */
    public static Block toText(Type type, Block block)
    {
        BlockBuilder output = textType(type).createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            appendText(type, block, position, output);
        }
        return output.build();
    }

    private static Type textType(Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> new ArrayType(textType(arrayType.getElementType()));
            case MapType mapType -> new MapType(textType(mapType.getKeyType()), textType(mapType.getValueType()), TYPE_OPERATORS);
            case RowType rowType -> {
                List<RowType.Field> fields = rowType.getFields().stream()
                        .map(field -> new RowType.Field(field.getName(), textType(field.getType())))
                        .collect(toImmutableList());
                yield RowType.from(fields);
            }
            default -> isJson(type) ? VARCHAR : type;
        };
    }

    private static void appendText(Type type, Block block, int position, BlockBuilder output)
    {
        if (block.isNull(position)) {
            output.appendNull();
            return;
        }
        if (!containsJson(type)) {
            output.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
            return;
        }
        switch (type) {
            case ArrayType arrayType -> {
                Block elements = arrayType.getObject(block, position);
                ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
                    for (int index = 0; index < elements.getPositionCount(); index++) {
                        appendText(arrayType.getElementType(), elements, index, elementBuilder);
                    }
                });
            }
            case MapType mapType -> {
                SqlMap map = mapType.getObject(block, position);
                ((MapBlockBuilder) output).buildEntry((keyBuilder, valueBuilder) -> {
                    for (int index = 0; index < map.getSize(); index++) {
                        appendText(mapType.getKeyType(), map.getRawKeyBlock(), map.getRawOffset() + index, keyBuilder);
                        appendText(mapType.getValueType(), map.getRawValueBlock(), map.getRawOffset() + index, valueBuilder);
                    }
                });
            }
            case RowType rowType -> {
                SqlRow row = rowType.getObject(block, position);
                ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                    for (int field = 0; field < row.getFieldCount(); field++) {
                        appendText(rowType.getFields().get(field).getType(), row.getRawFieldBlock(field), row.getRawIndex(), fieldBuilders.get(field));
                    }
                });
            }
            // the type renders the value as JSON text, whichever form the block holds it in
            default -> VARCHAR.writeSlice(output, type.getSlice(block, position));
        }
    }
}
