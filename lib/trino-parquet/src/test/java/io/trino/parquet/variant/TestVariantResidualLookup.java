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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.objectHeader;
import static io.trino.spi.variant.VariantDecoder.valueSize;
import static io.trino.spi.variant.VariantUtils.writeOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVariantResidualLookup
{
    @Test
    public void testFieldsOutOfNameOrder()
    {
        // 70 keys in descending order, with fields in field id order and a false sorted flag, as DuckDB writes them
        List<Slice> keys = IntStream.range(0, 70)
                .mapToObj(key -> utf8Slice("k%02d".formatted(69 - key)))
                .collect(toImmutableList());
        Variant object = encode(keys, keys.stream().map(key -> Variant.ofLong(Long.parseLong(key.toStringUtf8().substring(1)))).toList(), true, false);
        assertThat(object.metadata().isSorted()).isTrue();
        // The binary search of the object finds only some of the keys
        assertThat(keys.stream().filter(key -> object.getObjectField(key).isPresent()).count()).isLessThan(keys.size());

        List<Slice> names = ImmutableList.of(utf8Slice("k00"), utf8Slice("k05"), utf8Slice("k69"), utf8Slice("k70"));
        assertThat(find(object, names)).containsExactly(0L, 5L, 69L, null);
    }

    @Test
    public void testLargeObject()
    {
        // More than 255 fields, so the object has a four-byte field count, two-byte field ids, and two-byte offsets
        Map<Slice, Variant> fields = new HashMap<>();
        for (int key = 0; key < 300; key++) {
            fields.put(utf8Slice("key%03d".formatted(key)), Variant.ofLong(key));
        }
        Variant object = Variant.ofObject(fields);
        assertThat(object.data().getByte(0) & 0b0100_0000).isNotZero();
        assertThat(find(object, ImmutableList.of(utf8Slice("key000"), utf8Slice("key299"), utf8Slice("key300")))).containsExactly(0L, 299L, null);
    }

    @Test
    public void testDuplicateNames()
    {
        // The dictionary has "k" twice, as DuckDB writes it, and the object has a field for each
        List<Slice> keys = ImmutableList.of(utf8Slice("k"), utf8Slice("k"));
        Variant object = encode(keys, ImmutableList.of(Variant.ofLong(1), Variant.ofLong(2)), true, true);
        assertThatThrownBy(() -> find(object, ImmutableList.of(utf8Slice("k"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT object has duplicate field k");

        // A field that is not requested is not checked
        assertThat(find(object, ImmutableList.of(utf8Slice("other")))).containsExactly((Long) null);

        // Two dictionary entries with the same name for different objects are not duplicate fields
        Variant single = encode(keys.subList(0, 1), ImmutableList.of(Variant.ofLong(3)), true, true);
        assertThat(find(single, ImmutableList.of(utf8Slice("k")))).containsExactly(3L);
    }

    @Test
    public void testCaseSensitive()
    {
        Variant object = Variant.ofObject(ImmutableMap.of(utf8Slice("$Browser"), Variant.ofLong(1), utf8Slice("$browser"), Variant.ofLong(2)));
        assertThat(find(object, ImmutableList.of(utf8Slice("$browser"), utf8Slice("$BROWSER"), utf8Slice("$Browser")))).containsExactly(2L, null, 1L);
    }

    @Test
    public void testEmptyObjectAndEmptyName()
    {
        assertThat(find(Variant.EMPTY_OBJECT, ImmutableList.of(utf8Slice("a")))).containsExactly((Long) null);
        Variant object = Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofLong(1)));
        assertThat(find(object, ImmutableList.of(utf8Slice("")))).containsExactly((Long) null);
    }

    @Test
    public void testInvalidObjects()
    {
        Variant object = Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofLong(1)));
        VariantResidualLookup lookup = new VariantResidualLookup();
        lookup.reset(Metadata.EMPTY_METADATA_SLICE);
        // The field id is not in the empty dictionary
        assertThatThrownBy(() -> lookup.find(object.data(), 0, ImmutableList.of(utf8Slice("a")), new int[1]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT object has field id 0, which is not in the dictionary");

        lookup.reset(object.metadata().toSlice());
        assertThatThrownBy(() -> lookup.find(object.data().slice(0, 3), 0, ImmutableList.of(utf8Slice("a")), new int[1]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT object is truncated");
        assertThatThrownBy(() -> lookup.find(Variant.ofLong(1).data(), 0, ImmutableList.of(utf8Slice("a")), new int[1]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT value is not an object");
        assertThatThrownBy(() -> lookup.reset(Slices.wrappedBuffer((byte) 0x02, (byte) 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported VARIANT metadata version: 2");
    }

    /// The values of the names in the object, or null for a missing name.
    private static List<Long> find(Variant object, List<Slice> names)
    {
        VariantResidualLookup lookup = new VariantResidualLookup();
        lookup.reset(object.metadata().toSlice());
        int[] starts = new int[names.size()];
        lookup.find(object.data(), 0, names, starts);
        return Arrays.stream(starts)
                .mapToObj(start -> start < 0 ? null : Variant.from(Metadata.EMPTY_METADATA, object.data().slice(start, valueSize(object.data(), start))).getLong())
                .toList();
    }

    /// An object whose dictionary has `keys` in their order, and whose fields are in
    /// field id order.
    private static Variant encode(List<Slice> keys, List<Variant> values, boolean sortedFlag, boolean allowDuplicates)
    {
        Slice metadata;
        if (allowDuplicates) {
            // Metadata.of rejects duplicate names, so write the dictionary of one-byte names here
            metadata = Slices.allocate(3 + 2 * keys.size());
            metadata.setByte(0, metadataHeader(false, 1));
            metadata.setByte(1, keys.size());
            for (int id = 0; id <= keys.size(); id++) {
                metadata.setByte(2 + id, id);
            }
            for (int id = 0; id < keys.size(); id++) {
                metadata.setByte(3 + keys.size() + id, keys.get(id).getByte(0));
            }
        }
        else {
            metadata = Metadata.of(keys).toSlice().copy();
        }
        metadata.setByte(0, metadataHeader(sortedFlag, metadataOffsetSize(metadata.getByte(0))));

        int dataLength = values.stream().mapToInt(value -> value.data().length()).sum();
        int offsetSize = dataLength < 0x100 ? 1 : 2;
        Slice data = Slices.allocate(2 + keys.size() + (keys.size() + 1) * offsetSize + dataLength);
        data.setByte(0, objectHeader(1, offsetSize, false));
        data.setByte(1, keys.size());
        int offsetsStart = 2 + keys.size();
        int valuesStart = offsetsStart + (keys.size() + 1) * offsetSize;
        int offset = 0;
        for (int id = 0; id < keys.size(); id++) {
            data.setByte(2 + id, id);
            writeOffset(data, offsetsStart + id * offsetSize, offset, offsetSize);
            data.setBytes(valuesStart + offset, values.get(id).data());
            offset += values.get(id).data().length();
        }
        writeOffset(data, offsetsStart + keys.size() * offsetSize, offset, offsetSize);
        return Variant.from(Metadata.from(metadata), data);
    }
}
