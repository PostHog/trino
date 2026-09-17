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
package io.trino.plugin.hoglake.testing;

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * A byte-level writer for Hoglake's one deletion-vector encoding: the
 * Iceberg v3 puffin container with a single decompressed
 * {@code deletion-vector-v1} blob. It mirrors the writer in Hoglake itself
 * ({@code server/.../PuffinTestFiles.kt} and the DuckDB client's
 * {@code hoglake_puffin.cpp}), so connector tests exercise the real on-disk
 * bytes rather than a convenient stand-in.
 *
 * <pre>
 * puffin file: "PFA1" | blob | "PFA1" | footer payload | size (4, LE) | flags (4, LE) | "PFA1"
 * blob:        declared length (4, BE) | magic D1 D3 39 64 | vector | CRC-32 (4, BE)
 * vector:      bucket count (8, LE), then per bucket key (4, LE) + portable 32-bit roaring
 * </pre>
 */
public final class PuffinDeletionVectorFixtures
{
    private static final byte[] PUFFIN_MAGIC = {'P', 'F', 'A', '1'};
    private static final byte[] DELETION_VECTOR_MAGIC = {(byte) 0xD1, (byte) 0xD3, 0x39, 0x64};
    /**
     * The data file the fixtures' vectors claim to belong to, matching the
     * path the connector tests register.
     */
    public static final String DEFAULT_DATA_FILE_PATH = "memory:///t/data.parquet";

    private PuffinDeletionVectorFixtures() {}

    /**
     * A complete puffin deletion vector for the given 0-based file row
     * positions.
     */
    public static byte[] deletionVector(long... positions)
    {
        return deletionVector(DEFAULT_DATA_FILE_PATH, positions);
    }

    /**
     * A vector whose blob's {@code referenced-data-file} property names the
     * given data file, so a test can pair it with a different one.
     */
    public static byte[] deletionVector(String dataFilePath, long... positions)
    {
        return puffin(deletionVectorBlob(toBoxed(positions)), dataFilePath, positions.length);
    }

    /**
     * The {@code deletion-vector-v1} blob: the part of the file the puffin
     * container wraps.
     */
    public static byte[] deletionVectorBlob(long... positions)
    {
        return deletionVectorBlob(toBoxed(positions));
    }

    private static byte[] deletionVectorBlob(List<Long> positions)
    {
        return blob(portableBitmap(positions));
    }

    /**
     * A complete vector whose single bucket holds a hand-written 32-bit
     * container, so tests can reach shapes the library's serializer would
     * never emit. {@code containerBytes} is a portable 32-bit bitmap:
     * cookie (4, LE), container key (2, LE), cardinality - 1 (2, LE), then the
     * stored values. The enclosing length prefix and CRC are computed over the
     * result, so only the container is malformed.
     */
    public static byte[] deletionVectorWithRawContainer(byte[] containerBytes)
    {
        ByteArrayOutputStream vector = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(vector)) {
            out.writeLong(Long.reverseBytes(1));
            out.writeInt(Integer.reverseBytes(0));
            out.write(containerBytes);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return puffin(blob(vector.toByteArray()), DEFAULT_DATA_FILE_PATH, 3);
    }

    /**
     * A well-formed blob around a caller-corrupted vector, with a correct
     * length prefix and CRC, so only the corruption is under test.
     */
    public static byte[] puffinFromVector(byte[] vector)
    {
        return puffin(blob(vector), DEFAULT_DATA_FILE_PATH, 0);
    }

    /**
     * The vector payload of a valid vector for the given positions.
     * Positions are not added; mutate the returned array to corrupt the
     * portable bitmap itself.
     */
    public static byte[] malformedVector(Consumer<byte[]> corrupt)
    {
        byte[] vector = portableBitmap(toBoxed(new long[] {1L, 2L, 3L}));
        corrupt.accept(vector);
        return vector;
    }

    /**
     * Wraps a blob in the puffin container with the given footer payload.
     */
    public static byte[] puffinWithPayload(String footerPayload, byte[] blob)
    {
        byte[] payload = footerPayload.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(PUFFIN_MAGIC);
        file.writeBytes(blob);
        file.writeBytes(PUFFIN_MAGIC);
        file.writeBytes(payload);
        file.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(payload.length).array());
        file.writeBytes(new byte[4]);
        file.writeBytes(PUFFIN_MAGIC);
        return file.toByteArray();
    }

    /**
     * The standard container: header magic, blob, footer magic, JSON footer
     * carrying exactly one deletion-vector-v1 blob, LE payload size, zero
     * flags, trailer magic.
     */
    public static byte[] puffin(byte[] blob, String referencedDataFilePath, int cardinality)
    {
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1","fields":[],"snapshot-id":-1,\
                "sequence-number":-1,"offset":%d,"length":%d,\
                "properties":{"referenced-data-file":"%s","cardinality":"%d"}}],\
                "properties":{"created-by":"hoglake-test"}}"""
                        .formatted(PUFFIN_MAGIC.length, blob.length, referencedDataFilePath, cardinality);
        return puffinWithPayload(payload, blob);
    }

    /**
     * The {@code deletion-vector-v1} blob around a vector payload: a
     * big-endian declared length, the magic, the vector, and the big-endian
     * CRC-32 over magic and vector.
     */
    private static byte[] blob(byte[] vector)
    {
        ByteArrayOutputStream blob = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(blob)) {
            out.write(ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(DELETION_VECTOR_MAGIC.length + vector.length)
                    .array());
            out.write(DELETION_VECTOR_MAGIC);
            out.write(vector);
            CRC32 crc = new CRC32();
            crc.update(DELETION_VECTOR_MAGIC);
            crc.update(vector);
            out.write(ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt((int) crc.getValue())
                    .array());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return blob.toByteArray();
    }

    /**
     * Portable 64-bit roaring bitmap: LE bucket count, then per bucket a LE
     * high-32-bit key and the portable 32-bit bitmap.
     */
    private static byte[] portableBitmap(List<Long> positions)
    {
        Map<Integer, RoaringBitmap> buckets = new TreeMap<>();
        for (long position : positions) {
            if (position < 0) {
                throw new IllegalArgumentException("negative delete position: " + position);
            }
            buckets.computeIfAbsent((int) (position >>> 32), _ -> new RoaringBitmap())
                    .add((int) position);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream stream = new DataOutputStream(out)) {
            stream.writeLong(Long.reverseBytes(buckets.size()));
            for (Map.Entry<Integer, RoaringBitmap> bucket : buckets.entrySet()) {
                stream.writeInt(Integer.reverseBytes(bucket.getKey()));
                bucket.getValue().serialize(stream);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static List<Long> toBoxed(long[] positions)
    {
        List<Long> boxed = new ArrayList<>(positions.length);
        for (long position : positions) {
            boxed.add(position);
        }
        return boxed;
    }
}
