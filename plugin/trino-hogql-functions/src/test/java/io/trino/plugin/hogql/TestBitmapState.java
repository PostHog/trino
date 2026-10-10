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

import com.google.common.collect.ImmutableSet;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBitmapState
{
    @Test
    public void testStateRoundTrip()
    {
        AccumulatorStateFactory<BitmapState> factory = StateCompiler.generateStateFactory(BitmapState.class);
        AccumulatorStateSerializer<BitmapState> serializer = StateCompiler.generateStateSerializer(BitmapState.class);
        BitmapState state = factory.createSingleState();
        BitmapState restored = factory.createSingleState();
        BlockBuilder builder = serializer.getSerializedType().createBlockBuilder(null, 2);
        serializer.serialize(state, builder);
        BitmapAggregations.GroupBitmapState.input(state, 3);
        BitmapAggregations.GroupBitmapState.input(state, 1);
        serializer.serialize(state, builder);
        serializer.deserialize(builder.build(), 1, restored);
        assertThat(HogQLScalarFunctions.decodeBitmap(restored.getBitmap())).containsExactly(1L, 3L);
        serializer.deserialize(builder.build(), 0, restored);
        assertThat(restored.getBitmap()).isNull();
    }

    @Test
    public void testCombineUnion()
    {
        AccumulatorStateFactory<BitmapState> factory = StateCompiler.generateStateFactory(BitmapState.class);
        BitmapState left = factory.createSingleState();
        BitmapState right = factory.createSingleState();
        BitmapAggregations.GroupBitmapState.input(left, 1);
        BitmapAggregations.GroupBitmapState.input(right, 1);
        BitmapAggregations.GroupBitmapState.input(right, 2);
        BitmapAggregations.GroupBitmapState.combine(left, right);
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).containsExactly(1L, 2L);
        BitmapAggregations.GroupBitmapState.combine(left, factory.createSingleState());
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).containsExactly(1L, 2L);
    }

    @Test
    public void testCombineIntersectionWithEmptyState()
    {
        AccumulatorStateFactory<BitmapState> factory = StateCompiler.generateStateFactory(BitmapState.class);
        BitmapState left = factory.createSingleState();
        BitmapState right = factory.createSingleState();
        BitmapAggregations.GroupBitmapAndState.input(right, HogQLScalarFunctions.encodeBitmap(ImmutableSet.of(1L, 2L)));
        BitmapAggregations.GroupBitmapAndState.combine(left, right);
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).containsExactly(1L, 2L);
        BitmapAggregations.GroupBitmapAndState.combine(left, factory.createSingleState());
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).containsExactly(1L, 2L);
        BitmapState empty = factory.createSingleState();
        BitmapAggregations.GroupBitmapAndState.input(empty, HogQLScalarFunctions.encodeBitmap(ImmutableSet.of()));
        BitmapAggregations.GroupBitmapAndState.combine(left, empty);
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).isEmpty();
        BitmapAggregations.GroupBitmapAndState.combine(left, right);
        assertThat(HogQLScalarFunctions.decodeBitmap(left.getBitmap())).isEmpty();
    }
}
