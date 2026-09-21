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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;

/**
 * Mutable position set and the existing Hoglake Puffin wire encoding.
 */
final class HoglakeDeleteBitmap
{
    // Coordinator finish hooks have no memory context. Workers also report usage.
    // Bound their compressed working sets rather than allowing unbounded heap use.
    static final long MAX_BYTES = 64L * 1024 * 1024;
    private final Map<Integer, RoaringBitmap> buckets = new TreeMap<>();

    void add(long position)
    {
        if (position < 0) {
            throw new IllegalArgumentException("Negative physical row position");
        }
        buckets.computeIfAbsent((int) (position >>> 32), _ -> new RoaringBitmap()).add((int) position);
    }

    void union(int key, RoaringBitmap bitmap)
    {
        buckets.computeIfAbsent(key, _ -> new RoaringBitmap()).or(bitmap);
        checkSize(retainedBytes());
    }

    long retainedBytes()
    {
        return 128L + buckets.values().stream().mapToLong(bitmap -> 64L + bitmap.getLongSizeInBytes()).sum();
    }

    static void checkSize(long size)
    {
        if (size > MAX_BYTES) {
            throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Hoglake DELETE exceeds the 64 MiB compressed position-set limit");
        }
    }

    long cardinality()
    {
        return buckets.values().stream().mapToLong(RoaringBitmap::getLongCardinality).sum();
    }

    byte[] encode(String dataPath)
            throws IOException
    {
        checkSize(retainedBytes());
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(body)) {
            output.write(new byte[] {(byte) 0xD1, (byte) 0xD3, 0x39, 0x64});
            output.writeLong(Long.reverseBytes(buckets.size()));
            for (var entry : buckets.entrySet()) {
                output.writeInt(Integer.reverseBytes(entry.getKey()));
                entry.getValue().serialize(output);
            }
        }
        byte[] bytes = body.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(bytes);
        byte[] footer = new ObjectMapper().writeValueAsBytes(Map.of("blobs", List.of(Map.of(
                "type", "deletion-vector-v1",
                "fields", List.of(),
                "snapshot-id", -1,
                "sequence-number", -1,
                "offset", 4,
                "length", bytes.length + 8,
                "properties", Map.of("referenced-data-file", dataPath, "cardinality", Long.toString(cardinality()))))));
        if (footer.length > HoglakeDeletionVector.MAX_FOOTER_BYTES) {
            throw new IOException("Deletion-vector footer exceeds the supported size");
        }
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(file)) {
            output.writeBytes("PFA1");
            output.writeInt(bytes.length);
            output.write(bytes);
            output.writeInt((int) crc.getValue());
            output.writeBytes("PFA1");
            output.write(footer);
            output.writeInt(Integer.reverseBytes(footer.length));
            output.writeInt(0);
            output.writeBytes("PFA1");
        }
        return file.toByteArray();
    }
}
