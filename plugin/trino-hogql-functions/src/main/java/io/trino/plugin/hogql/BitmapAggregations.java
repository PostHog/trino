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
package io.trino.plugin.hogql;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import java.util.LinkedHashSet;
import java.util.Set;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;

public final class BitmapAggregations
{
    private BitmapAggregations() {}

    private enum Operation
    {
        AND,
        OR,
        XOR,
    }

    private static Set<Long> values(BitmapState state)
    {
        return state.getBitmap() == null ? new LinkedHashSet<>() : HogQLScalarFunctions.decodeBitmap(state.getBitmap());
    }

    private static void add(BitmapState state, long value)
    {
        Set<Long> values = values(state);
        values.add(value);
        state.setBitmap(HogQLScalarFunctions.encodeBitmap(values));
    }

    private static void combineUnion(BitmapState state, BitmapState other)
    {
        Set<Long> values = values(state);
        values.addAll(values(other));
        state.setBitmap(HogQLScalarFunctions.encodeBitmap(values));
    }

    private static void apply(BitmapState state, Slice input, Operation operation)
    {
        Set<Long> current = values(state);
        Set<Long> incoming = HogQLScalarFunctions.decodeBitmap(input);
        if (state.getBitmap() == null) {
            current.addAll(incoming);
        }
        else if (operation == Operation.AND) {
            current.retainAll(incoming);
        }
        else if (operation == Operation.OR) {
            current.addAll(incoming);
        }
        else {
            Set<Long> intersection = new LinkedHashSet<>(current);
            intersection.retainAll(incoming);
            current.addAll(incoming);
            current.removeAll(intersection);
        }
        state.setBitmap(HogQLScalarFunctions.encodeBitmap(current));
    }

    private static void combine(BitmapState state, BitmapState other, Operation operation)
    {
        if (other.getBitmap() != null) {
            apply(state, other.getBitmap(), operation);
        }
    }

    private static void outputCardinality(BitmapState state, BlockBuilder output)
    {
        io.trino.spi.type.BigintType.BIGINT.writeLong(output, values(state).size());
    }

    private static void outputState(BitmapState state, BlockBuilder output)
    {
        io.trino.spi.type.VarbinaryType.VARBINARY.writeSlice(
                output,
                state.getBitmap() == null ? HogQLScalarFunctions.encodeBitmap(Set.of()) : state.getBitmap());
    }

    @AggregationFunction("hogql_group_bitmap")
    public static final class GroupBitmap
    {
        private GroupBitmap() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(BIGINT) long value)
        {
            add(state, value);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineUnion(state, other);
        }

        @OutputFunction(BIGINT)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputCardinality(state, output);
        }
    }

    @AggregationFunction("hogql_group_bitmap_state")
    public static final class GroupBitmapState
    {
        private GroupBitmapState() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(BIGINT) long value)
        {
            add(state, value);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineUnion(state, other);
        }

        @OutputFunction(VARBINARY)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputState(state, output);
        }
    }

    public abstract static class BitmapOperation
    {
        protected BitmapOperation() {}

        protected static void input(BitmapState state, Slice value, Operation operation)
        {
            apply(state, value, operation);
        }

        protected static void combineStates(BitmapState state, BitmapState other, Operation operation)
        {
            combine(state, other, operation);
        }
    }

    @AggregationFunction("hogql_group_bitmap_and")
    public static final class GroupBitmapAnd
            extends BitmapOperation
    {
        private GroupBitmapAnd() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(VARBINARY) Slice value)
        {
            input(state, value, Operation.AND);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineStates(state, other, Operation.AND);
        }

        @OutputFunction(BIGINT)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputCardinality(state, output);
        }
    }

    @AggregationFunction("hogql_group_bitmap_or")
    public static final class GroupBitmapOr
            extends BitmapOperation
    {
        private GroupBitmapOr() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(VARBINARY) Slice value)
        {
            input(state, value, Operation.OR);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineStates(state, other, Operation.OR);
        }

        @OutputFunction(BIGINT)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputCardinality(state, output);
        }
    }

    @AggregationFunction("hogql_group_bitmap_xor")
    public static final class GroupBitmapXor
            extends BitmapOperation
    {
        private GroupBitmapXor() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(VARBINARY) Slice value)
        {
            input(state, value, Operation.XOR);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineStates(state, other, Operation.XOR);
        }

        @OutputFunction(BIGINT)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputCardinality(state, output);
        }
    }

    @AggregationFunction("hogql_group_bitmap_and_state")
    public static final class GroupBitmapAndState
            extends BitmapOperation
    {
        private GroupBitmapAndState() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(VARBINARY) Slice value)
        {
            input(state, value, Operation.AND);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineStates(state, other, Operation.AND);
        }

        @OutputFunction(VARBINARY)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputState(state, output);
        }
    }

    @AggregationFunction("hogql_group_bitmap_or_state")
    public static final class GroupBitmapOrState
            extends BitmapOperation
    {
        private GroupBitmapOrState() {}

        @InputFunction
        public static void input(@AggregationState BitmapState state, @SqlType(VARBINARY) Slice value)
        {
            input(state, value, Operation.OR);
        }

        @CombineFunction
        public static void combine(@AggregationState BitmapState state, @AggregationState BitmapState other)
        {
            combineStates(state, other, Operation.OR);
        }

        @OutputFunction(VARBINARY)
        public static void output(@AggregationState BitmapState state, BlockBuilder output)
        {
            outputState(state, output);
        }
    }
}
