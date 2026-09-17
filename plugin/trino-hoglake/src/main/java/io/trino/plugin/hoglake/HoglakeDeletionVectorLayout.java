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

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.BitmapContainer;
import org.roaringbitmap.RoaringArray;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RunContainer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfBooleanArray;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfCharArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.plugin.hoglake.HoglakeDeletionVector.invalid;

/**
 * Allocation preflight for Roaring 1.6.22's deserialize(DataInput). The input
 * remains immutable between inspection and decoding. No allocation is sized
 * from a header until its complete payload has been checked. Deserialization clears the
 * fresh bitmap's initial arrays, then retains containerCount entries and creates
 * temporary key, cardinality and container-kind arrays. Tests independently
 * walk the library's object graph to detect changes to this layout.
 */
record HoglakeDeletionVectorLayout(int serializedBytes, long retainedBytes, long scratchBytes)
{
    private static final int BITMAP_SIZE = instanceSize(RoaringBitmap.class);
    private static final int ARRAY_SIZE = instanceSize(RoaringArray.class);
    private static final int RUN_CONTAINER_SIZE = instanceSize(RunContainer.class);
    private static final int BITMAP_CONTAINER_SIZE = instanceSize(BitmapContainer.class);
    private static final int ARRAY_CONTAINER_SIZE = instanceSize(ArrayContainer.class);
    private static final int NO_RUN_COOKIE = 12346;
    private static final int RUN_COOKIE = 12347;
    private static final int MAX_CONTAINERS = 65_536;
    private static final long STREAM_BYTES = instanceSize(ByteArrayInputStream.class)
            + instanceSize(DataInputStream.class) + sizeOfByteArray(8);

    static HoglakeDeletionVectorLayout inspect(byte[] bytes, int offset, int length, String location)
    {
        ByteBuffer input = ByteBuffer.wrap(bytes, offset, length).slice().order(ByteOrder.LITTLE_ENDIAN);
        requireRemaining(input, Integer.BYTES, location);
        int cookie = input.getInt();
        boolean hasRuns = (cookie & 0xFFFF) == RUN_COOKIE;
        int count;
        if (hasRuns) {
            count = (cookie >>> 16) + 1;
        }
        else if (cookie == NO_RUN_COOKIE) {
            requireRemaining(input, Integer.BYTES, location);
            count = input.getInt();
        }
        else {
            throw invalid(location, "invalid roaring cookie", null);
        }
        if (count < 0 || count > MAX_CONTAINERS) {
            throw invalid(location, "invalid roaring container count %d".formatted(count), null);
        }
        int runFlags = input.position();
        if (hasRuns) {
            skip(input, (count + 7) / 8, location);
        }
        int descriptors = input.position();
        skip(input, count * 4, location);
        boolean hasOffsets = !hasRuns || count >= 4;
        int offsets = input.position();
        if (hasOffsets) {
            skip(input, count * Integer.BYTES, location);
        }

        int capacity = count;
        long retained = BITMAP_SIZE + ARRAY_SIZE
                + sizeOfCharArray(capacity) + sizeOfObjectArray(capacity);
        for (int i = 0; i < count; i++) {
            if (hasOffsets && Integer.toUnsignedLong(input.getInt(offsets + i * Integer.BYTES)) != input.position()) {
                throw invalid(location, "invalid roaring container offset", null);
            }
            int cardinality = Short.toUnsignedInt(input.getShort(descriptors + i * 4 + 2)) + 1;
            boolean run = hasRuns && (input.get(runFlags + i / 8) & (1 << (i % 8))) != 0;
            if (run) {
                requireRemaining(input, Short.BYTES, location);
                int runs = Short.toUnsignedInt(input.getShort());
                skip(input, runs * 4, location);
                retained += RUN_CONTAINER_SIZE + sizeOfCharArray(runs * 2);
            }
            else if (cardinality > 4096) {
                skip(input, 1024 * Long.BYTES, location);
                retained += BITMAP_CONTAINER_SIZE + sizeOfLongArray(1024);
            }
            else {
                skip(input, cardinality * Character.BYTES, location);
                retained += ARRAY_CONTAINER_SIZE + sizeOfCharArray(cardinality);
            }
        }
        long scratch = sizeOfCharArray(count) + sizeOfIntArray(count) + sizeOfBooleanArray(count) + STREAM_BYTES;
        if (hasRuns) {
            scratch += sizeOfByteArray((count + 7) / 8);
        }
        // Constructor arrays are discarded by deserialize's initial clear().
        // Cover their allocation as well, even though they are no longer live.
        scratch += sizeOfCharArray(4) + sizeOfObjectArray(4);
        return new HoglakeDeletionVectorLayout(input.position(), retained, scratch);
    }

    private static void skip(ByteBuffer input, int length, String location)
    {
        requireRemaining(input, length, location);
        input.position(input.position() + length);
    }

    private static void requireRemaining(ByteBuffer input, int length, String location)
    {
        if (length > input.remaining()) {
            throw invalid(location, "truncated deletion vector bitmap", null);
        }
    }
}
