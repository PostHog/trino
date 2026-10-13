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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

import static io.trino.spi.variant.Header.arrayHeader;
import static io.trino.spi.variant.Header.objectHeader;
import static io.trino.spi.variant.VariantUtils.getOffsetSize;
import static io.trino.spi.variant.VariantUtils.writeOffset;
import static java.lang.Math.max;

/// Encodes one Variant value at a time into a reusable buffer, for values whose object
/// field ids are given by the caller, such as the ids of a dictionary that is the same for
/// every value. A container is written after its elements: the writer records where each
/// element starts, and when the container ends it moves the elements behind the header,
/// whose size depends on their total length. The encoding is the one of
/// [io.trino.spi.variant.VariantEncoder]: the smallest field id and offset sizes, and a
/// large container only above 255 elements.
final class PrunedVariantWriter
{
    private static final int INITIAL_SIZE = 256;
    // A buffer that a large value grew is not kept for the next rows
    private static final int MAX_RETAINED_SIZE = 1024 * 1024;

    private byte[] buffer = new byte[INITIAL_SIZE];
    private Slice slice = Slices.wrappedBuffer(buffer);
    private int size;

    // The field id and the start of each element of the open containers
    private int[] elementIds = new int[16];
    private int[] elementStarts = new int[16];
    private int elementCount;

    public void reset()
    {
        size = 0;
        elementCount = 0;
        if (buffer.length > MAX_RETAINED_SIZE) {
            buffer = new byte[INITIAL_SIZE];
            slice = Slices.wrappedBuffer(buffer);
        }
    }

    public int size()
    {
        return size;
    }

    public Slice value()
    {
        return slice.slice(0, size);
    }

    /// Returns the buffer to write `length` bytes at [#size()] with
    /// [io.trino.spi.variant.VariantEncoder], which then [#advance(int)]s.
    public Slice reserve(int length)
    {
        ensureCapacity(size + length);
        return slice;
    }

    public void advance(int length)
    {
        size += length;
    }

    public void write(Slice data, int offset, int length)
    {
        ensureCapacity(size + length);
        slice.setBytes(size, data, offset, length);
        size += length;
    }

    /// Starts a container, and returns the mark to end it with.
    public int beginContainer()
    {
        return elementCount;
    }

    /// Starts an element of the open container, with the field id of an object field.
    public void beginElement(int fieldId)
    {
        if (elementCount == elementIds.length) {
            elementIds = Arrays.copyOf(elementIds, elementCount * 2);
            elementStarts = Arrays.copyOf(elementStarts, elementCount * 2);
        }
        elementIds[elementCount] = fieldId;
        elementStarts[elementCount] = size;
        elementCount++;
    }

    /// Drops the element that was started last, which wrote nothing.
    public void cancelElement()
    {
        elementCount--;
    }

    /// Ends an object that was started at `mark`, whose fields were written in field id order.
    public void endObject(int mark, int dataStart)
    {
        int count = elementCount - mark;
        int dataLength = size - dataStart;
        int maxFieldId = 0;
        for (int element = mark; element < elementCount; element++) {
            maxFieldId = max(maxFieldId, elementIds[element]);
        }
        boolean large = count > 255;
        int idSize = getOffsetSize(maxFieldId);
        int offsetSize = getOffsetSize(dataLength);
        int headerSize = 1 + (large ? Integer.BYTES : 1) + count * idSize + (count + 1) * offsetSize;
        moveData(dataStart, dataLength, headerSize);

        int position = dataStart;
        slice.setByte(position, objectHeader(idSize, offsetSize, large));
        position = writeCount(position + 1, count, large);
        for (int element = mark; element < elementCount; element++) {
            writeOffset(slice, position, elementIds[element], idSize);
            position += idSize;
        }
        writeOffsets(position, mark, dataStart, dataLength, offsetSize);
        elementCount = mark;
    }

    /// Ends an array that was started at `mark`.
    public void endArray(int mark, int dataStart)
    {
        int count = elementCount - mark;
        int dataLength = size - dataStart;
        boolean large = count > 255;
        int offsetSize = getOffsetSize(dataLength);
        int headerSize = 1 + (large ? Integer.BYTES : 1) + (count + 1) * offsetSize;
        moveData(dataStart, dataLength, headerSize);

        slice.setByte(dataStart, arrayHeader(offsetSize, large));
        int position = writeCount(dataStart + 1, count, large);
        writeOffsets(position, mark, dataStart, dataLength, offsetSize);
        elementCount = mark;
    }

    private void moveData(int dataStart, int dataLength, int headerSize)
    {
        ensureCapacity(size + headerSize);
        System.arraycopy(buffer, dataStart, buffer, dataStart + headerSize, dataLength);
        size += headerSize;
    }

    private int writeCount(int position, int count, boolean large)
    {
        if (large) {
            slice.setInt(position, count);
            return position + Integer.BYTES;
        }
        slice.setByte(position, count);
        return position + 1;
    }

    private void writeOffsets(int position, int mark, int dataStart, int dataLength, int offsetSize)
    {
        for (int element = mark; element < elementCount; element++) {
            writeOffset(slice, position, elementStarts[element] - dataStart, offsetSize);
            position += offsetSize;
        }
        writeOffset(slice, position, dataLength, offsetSize);
    }

    private void ensureCapacity(int capacity)
    {
        if (capacity > buffer.length) {
            buffer = Arrays.copyOf(buffer, max(capacity, buffer.length * 2));
            slice = Slices.wrappedBuffer(buffer);
        }
    }
}
