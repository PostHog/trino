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

import com.sun.management.ThreadMXBean;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHoglakeDeletionVectorLayout
{
    @Test
    void sizesMatchThePinnedDeserializer()
            throws Exception
    {
        ThreadMXBean allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(allocations.isThreadAllocatedMemoryEnabled()).isTrue();
        for (RoaringBitmap bitmap : bitmaps()) {
            byte[] bytes = serialize(bitmap);
            HoglakeDeletionVectorLayout layout = HoglakeDeletionVectorLayout.inspect(bytes, 0, bytes.length, "test");
            assertThat(layout.serializedBytes()).isEqualTo(bytes.length);
            // Warm the library and allocation counter before measuring. This
            // measures all deserializer allocations, a stronger bound than
            // its peak live scratch, independently of the sizing formulas.
            for (int i = 0; i < 5; i++) {
                deserialize(bytes);
                allocations.getCurrentThreadAllocatedBytes();
            }
            long before = allocations.getCurrentThreadAllocatedBytes();
            RoaringBitmap decoded = deserialize(bytes);
            long allocated = allocations.getCurrentThreadAllocatedBytes() - before;
            assertThat(layout.retainedBytes())
                    .describedAs("retained heap for %d containers", bitmap.getContainerCount())
                    .isEqualTo(TestingDeletionVectorMemory.retainedBytes(decoded));
            assertThat(layout.retainedBytes() + layout.scratchBytes())
                    .describedAs("all allocations for %d containers", bitmap.getContainerCount())
                    .isGreaterThanOrEqualTo(allocated);
            assertThat(decoded).isEqualTo(bitmap);
        }
    }

    @Test
    void everyTruncatedContainerShapeFailsPreflight()
            throws IOException
    {
        for (RoaringBitmap bitmap : bitmaps().subList(0, 8)) {
            byte[] bytes = serialize(bitmap);
            for (int length = 0; length < bytes.length; length++) {
                int truncatedLength = length;
                assertThatThrownBy(() -> HoglakeDeletionVectorLayout.inspect(bytes, 0, truncatedLength, "test"))
                        .isInstanceOf(TrinoException.class)
                        .hasMessageContaining("truncated");
            }
        }
    }

    @Test
    void hugeContainerCountWithNoDescriptorsFailsPreflight()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(12346).putInt(65_536).array();
        assertThatThrownBy(() -> HoglakeDeletionVectorLayout.inspect(bytes, 0, bytes.length, "test"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void invalidCountsAndOffsetsFailPreflight()
            throws IOException
    {
        for (int count : new int[] {-1, 65_537, Integer.MAX_VALUE}) {
            byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(12346).putInt(count).array();
            assertThatThrownBy(() -> HoglakeDeletionVectorLayout.inspect(bytes, 0, bytes.length, "test"))
                    .hasMessageContaining("container count");
        }
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1);
        byte[] bytes = serialize(bitmap);
        Arrays.fill(bytes, 12, 16, (byte) 0);
        assertThatThrownBy(() -> HoglakeDeletionVectorLayout.inspect(bytes, 0, bytes.length, "test"))
                .hasMessageContaining("container offset");
    }

    private static List<RoaringBitmap> bitmaps()
    {
        List<RoaringBitmap> bitmaps = new ArrayList<>();
        for (int count : new int[] {0, 1, 4, 5}) {
            RoaringBitmap bitmap = new RoaringBitmap();
            for (int i = 0; i < count; i++) {
                bitmap.add(i << 16);
            }
            bitmaps.add(bitmap);
        }
        for (int cardinality : new int[] {4096, 4097}) {
            RoaringBitmap bitmap = new RoaringBitmap();
            for (int i = 0; i < cardinality; i++) {
                bitmap.add(i * 2);
            }
            bitmaps.add(bitmap);
        }
        for (int containers : new int[] {1, 4, 5}) {
            RoaringBitmap bitmap = new RoaringBitmap();
            for (int i = 0; i < containers; i++) {
                bitmap.add((long) i << 16, ((long) i << 16) + 100);
                bitmap.add(((long) i << 16) + 200, ((long) i << 16) + 400);
            }
            bitmap.runOptimize();
            bitmaps.add(bitmap);
        }
        RoaringBitmap sparse = new RoaringBitmap();
        for (int i = 0; i < 65_536; i++) {
            sparse.add(i << 16);
        }
        bitmaps.add(sparse);
        return bitmaps;
    }

    private static byte[] serialize(RoaringBitmap bitmap)
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bitmap.serialize(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    private static RoaringBitmap deserialize(byte[] bytes)
            throws IOException
    {
        RoaringBitmap bitmap = new RoaringBitmap();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            bitmap.deserialize(input);
        }
        return bitmap;
    }
}
