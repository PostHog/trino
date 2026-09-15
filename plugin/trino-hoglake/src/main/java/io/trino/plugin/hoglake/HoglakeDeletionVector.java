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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.zip.CRC32;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_INVALID;
import static java.util.Objects.requireNonNull;

/**
 * The deleted row positions of one Hoglake data file, decoded from its
 * live deletion vector.
 *
 * <p>Positions are 0-based physical row ordinals within the data file
 * (Iceberg deletion-vector semantics), which is exactly the file row
 * position Trino's Parquet reader reports for a row: the offset of its row
 * group plus its offset inside the group. They are not Hoglake row ids, and
 * not indexes into a filtered, projected, or pruned result.
 *
 * <p>The one on-disk encoding Hoglake publishes is an Iceberg v3 puffin
 * container carrying a single uncompressed {@code deletion-vector-v1} blob
 * ({@code hog_delete_file.file_format = 'puffin-dv'}):
 *
 * <pre>
 * puffin file: "PFA1" | blob section | "PFA1" | footer payload | size (4, LE) | flags (4, LE) | "PFA1"
 * blob:        declared length (4, BE, covers magic + vector) | magic D1 D3 39 64
 *              | 64-bit portable roaring bitmap | CRC-32 of (magic + vector) (4, BE)
 * portable 64-bit roaring bitmap: bucket count (8, LE), then per bucket a
 *              high-32-bit key (4, LE) and a portable 32-bit roaring bitmap
 * </pre>
 *
 * <p>The encoding mixes endianness exactly as Iceberg's deletion-vector-v1
 * does: the blob's declared length and the checksum are big-endian, while
 * the roaring bitmap's own fields are little-endian. Hoglake's server
 * reader and writer and its DuckDB client all agree on this layout.
 *
 * <p>Every structural check throws {@link TrinoException}
 * ({@code HOGLAKE_DELETION_VECTOR_INVALID}). A deletion vector that cannot
 * be decoded fails the query: treating it as "no deleted rows" would
 * silently return rows the catalog says are gone.
 */
public final class HoglakeDeletionVector
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int PUFFIN_MAGIC_SIZE = 4;
    private static final byte[] PUFFIN_MAGIC = {'P', 'F', 'A', '1'};
    private static final byte[] DELETION_VECTOR_MAGIC = {(byte) 0xD1, (byte) 0xD3, 0x39, 0x64};
    private static final String DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1";

    /**
     * The wire's only deletion-vector file format
     * ({@code hog_delete_file.file_format}); the catalog schema accepts
     * nothing else.
     */
    public static final String PUFFIN_DELETION_VECTOR_FORMAT = "puffin-dv";
    private static final int MIN_FILE_SIZE = PUFFIN_MAGIC_SIZE * 3 + 12;
    private static final int MIN_BLOB_SIZE = 12;
    private static final int INSTANCE_SIZE = 96;

    /**
     * Bucket keys (high 32 bits of a position), strictly ascending and
     * sparse: nothing requires a vector to fill every key up to its largest.
     */
    private final int[] bucketKeys;
    /**
     * One 32-bit roaring bitmap per bucket key, index-aligned with
     * {@link #bucketKeys}.
     */
    private final RoaringBitmap[] buckets;
    private final long cardinality;
    private final Optional<String> referencedDataFile;

    private HoglakeDeletionVector(int[] bucketKeys, RoaringBitmap[] buckets, long cardinality, Optional<String> referencedDataFile)
    {
        this.bucketKeys = bucketKeys;
        this.buckets = buckets;
        this.cardinality = cardinality;
        this.referencedDataFile = referencedDataFile;
    }

    public boolean isRowDeleted(long filePosition)
    {
        if (filePosition < 0) {
            return false;
        }
        int bucket = Arrays.binarySearch(bucketKeys, (int) (filePosition >>> 32));
        return bucket >= 0 && buckets[bucket].contains((int) filePosition);
    }

    /**
     * Number of deleted positions: the bitmap's true cardinality. The
     * catalog's {@code delete_count} is only trusted after it has been
     * compared against this value.
     */
    public long cardinality()
    {
        return cardinality;
    }

    /**
     * The {@code referenced-data-file} blob property, when the writer set
     * it. Hoglake's own writers do; it is used to detect a vector that is
     * paired with the wrong data file.
     */
    public Optional<String> referencedDataFile()
    {
        return referencedDataFile;
    }

    /**
     * The largest deleted file position, if anything is deleted.
     */
    public OptionalLong maximumDeletedPosition()
    {
        for (int bucket = bucketKeys.length - 1; bucket >= 0; bucket--) {
            RoaringBitmap bitmap = buckets[bucket];
            if (!bitmap.isEmpty()) {
                return OptionalLong.of((((long) bucketKeys[bucket]) << 32) | Integer.toUnsignedLong(bitmap.last()));
            }
        }
        return OptionalLong.empty();
    }

    public long retainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(bucketKeys);
        for (RoaringBitmap bitmap : buckets) {
            if (bitmap != null) {
                size += bitmap.getLongSizeInBytes();
            }
        }
        return size;
    }

    /**
     * Decodes a {@code puffin-dv} file. {@code location} only appears in
     * error messages.
     */
    public static HoglakeDeletionVector read(byte[] bytes, String location)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(location, "location is null");
        try {
            return new PuffinDeletionVectorReader(bytes, location).read();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException | IOException e) {
            throw invalid(location, "not a decodable puffin deletion vector", e);
        }
    }

    static TrinoException invalid(String location, String message, Throwable cause)
    {
        return new TrinoException(
                HOGLAKE_DELETION_VECTOR_INVALID,
                "Invalid hoglake deletion vector %s: %s".formatted(location, message),
                cause);
    }

    private static final class PuffinDeletionVectorReader
    {
        private final byte[] bytes;
        private final String location;

        public PuffinDeletionVectorReader(byte[] bytes, String location)
        {
            this.bytes = bytes;
            this.location = location;
        }

        public HoglakeDeletionVector read()
                throws IOException
        {
            if (bytes.length < MIN_FILE_SIZE) {
                throw invalid(location, "file is too small to be a puffin container (%d bytes)".formatted(bytes.length), null);
            }
            requireMagic(0, "header");

            // Footer: magic | payload | payload size (4, LE) | flags (4, LE) | magic
            requireMagic(bytes.length - PUFFIN_MAGIC_SIZE, "trailer");
            int flags = ByteBuffer.wrap(bytes, bytes.length - 8, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if ((flags & 1) != 0) {
                throw invalid(location, "compressed puffin footer payloads are not supported", null);
            }
            if (flags != 0) {
                throw invalid(location, "unknown puffin footer flags 0x%08x".formatted(flags), null);
            }
            int payloadSize = ByteBuffer.wrap(bytes, bytes.length - 12, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
            int payloadStart = bytes.length - 12 - payloadSize;
            if (payloadSize < 0 || payloadStart < PUFFIN_MAGIC_SIZE * 2) {
                throw invalid(location, "puffin footer payload size %d is out of bounds".formatted(payloadSize), null);
            }
            requireMagic(payloadStart - PUFFIN_MAGIC_SIZE, "footer header");
            int blobSectionEnd = payloadStart - PUFFIN_MAGIC_SIZE;

            JsonNode payload;
            try {
                payload = JSON.readTree(new ByteArrayInputStream(bytes, payloadStart, payloadSize));
            }
            catch (IOException e) {
                throw invalid(location, "unparseable puffin footer payload", e);
            }
            JsonNode blobs = payload == null ? null : payload.get("blobs");
            if (blobs == null || !blobs.isArray()) {
                throw invalid(location, "puffin footer payload has no blobs array", null);
            }
            long offset = -1;
            long length = -1;
            Optional<String> referencedDataFile = Optional.empty();
            int vectors = 0;
            for (JsonNode blob : blobs) {
                if (!DELETION_VECTOR_BLOB_TYPE.equals(blob.path("type").asText())) {
                    continue;
                }
                JsonNode codec = blob.get("compression-codec");
                if (codec != null && !codec.isNull()) {
                    throw invalid(location, "compressed %s blobs are not supported".formatted(DELETION_VECTOR_BLOB_TYPE), null);
                }
                JsonNode offsetNode = blob.get("offset");
                JsonNode lengthNode = blob.get("length");
                if (offsetNode == null || lengthNode == null || !offsetNode.canConvertToLong() || !lengthNode.canConvertToLong()) {
                    throw invalid(location, "%s blob has no offset/length".formatted(DELETION_VECTOR_BLOB_TYPE), null);
                }
                offset = offsetNode.longValue();
                length = lengthNode.longValue();
                JsonNode referenced = blob.path("properties").path("referenced-data-file");
                if (referenced.isTextual()) {
                    referencedDataFile = Optional.of(referenced.asText());
                }
                vectors++;
            }
            if (vectors != 1) {
                throw invalid(location, "expected exactly one %s blob, found %d".formatted(DELETION_VECTOR_BLOB_TYPE, vectors), null);
            }
            // Neither value is trusted: range-check without adding them, so a
            // hostile offset and length cannot overflow the bounds check.
            if (offset < PUFFIN_MAGIC_SIZE || length < MIN_BLOB_SIZE || length > blobSectionEnd || offset > blobSectionEnd - length) {
                throw invalid(location, "%s blob range [%d, +%d) is out of bounds".formatted(DELETION_VECTOR_BLOB_TYPE, offset, length), null);
            }
            return decodeBlob((int) offset, (int) length, referencedDataFile);
        }

        /**
         * {@code blob = declared length (4, BE) | magic | vector | CRC-32 (4, BE)},
         * where the declared length and the checksum cover magic + vector.
         */
        private HoglakeDeletionVector decodeBlob(int offset, int length, Optional<String> referencedDataFile)
        {
            int declared = declaredLength(offset, length);
            for (int i = 0; i < DELETION_VECTOR_MAGIC.length; i++) {
                if (bytes[offset + Integer.BYTES + i] != DELETION_VECTOR_MAGIC[i]) {
                    throw invalid(location, "bad %s magic".formatted(DELETION_VECTOR_BLOB_TYPE), null);
                }
            }
            int vectorLength = declared - DELETION_VECTOR_MAGIC.length;
            CRC32 crc = new CRC32();
            crc.update(bytes, offset + Integer.BYTES, DELETION_VECTOR_MAGIC.length + vectorLength);
            int storedCrc = ByteBuffer.wrap(bytes, offset + 8 + vectorLength, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
            if ((int) crc.getValue() != storedCrc) {
                throw invalid(location, "%s CRC mismatch: stored %d, computed %d"
                        .formatted(DELETION_VECTOR_BLOB_TYPE, storedCrc, (int) crc.getValue()), null);
            }
            return deserializePortable(offset + 8, vectorLength, referencedDataFile);
        }

        /**
         * The length the blob declares, big-endian, which must describe the
         * blob it sits in.
         */
        private int declaredLength(int offset, int length)
        {
            int declared = ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
            if (declared != length - 8) {
                throw invalid(location, "%s length prefix %d does not match blob length %d"
                        .formatted(DELETION_VECTOR_BLOB_TYPE, declared, length), null);
            }
            return declared;
        }

        /**
         * Portable 64-bit roaring bitmap: LE bucket count, then per bucket a
         * LE high-32-bit key and a portable 32-bit roaring bitmap. The Java
         * stream format is the portable 32-bit format.
         */
        private HoglakeDeletionVector deserializePortable(int offset, int length, Optional<String> referencedDataFile)
        {
            try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes, offset, length))) {
                long bucketCount = Long.reverseBytes(stream.readLong());
                if (bucketCount < 0 || bucketCount > Integer.MAX_VALUE) {
                    throw invalid(location, "implausible bucket count %d".formatted(bucketCount), null);
                }
                // Every bucket costs at least its 4-byte key and an 8-byte
                // empty bitmap, so the count must fit the vector that
                // declares it. Checking before allocating keeps a small
                // blob from asking for gigabytes of arrays.
                long maximumBuckets = (length - Long.BYTES) / 12;
                if (bucketCount > maximumBuckets) {
                    throw invalid(location, "bucket count %d exceeds the %d buckets %d vector bytes can hold"
                            .formatted(bucketCount, maximumBuckets, length), null);
                }
                int bucketCountInt = (int) bucketCount;
                int[] bucketKeys = new int[bucketCountInt];
                RoaringBitmap[] buckets = new RoaringBitmap[bucketCountInt];
                long cardinality = 0;
                int previousKey = -1;
                for (int i = 0; i < bucketCountInt; i++) {
                    int key = Integer.reverseBytes(stream.readInt());
                    if (key < 0) {
                        // A negative high-32-bit key would sign-extend into a
                        // garbage file position; no valid position has bit 63 set.
                        throw invalid(location, "negative bucket key %d".formatted(key), null);
                    }
                    if (key <= previousKey) {
                        throw invalid(location, "bucket keys are not strictly ascending (%d after %d)".formatted(key, previousKey), null);
                    }
                    previousKey = key;
                    RoaringBitmap bitmap = new RoaringBitmap();
                    bitmap.deserialize(stream);
                    if (bitmap.isEmpty()) {
                        throw invalid(location, "empty bucket %d".formatted(key), null);
                    }
                    bucketKeys[i] = key;
                    buckets[i] = bitmap;
                    cardinality += bitmap.getLongCardinality();
                }
                if (stream.read() != -1) {
                    throw invalid(location, "trailing bytes after the deletion vector bitmap", null);
                }
                return new HoglakeDeletionVector(bucketKeys, buckets, cardinality, referencedDataFile);
            }
            catch (IOException e) {
                throw invalid(location, "truncated deletion vector bitmap", e);
            }
        }

        private void requireMagic(int at, String where)
        {
            for (int i = 0; i < PUFFIN_MAGIC_SIZE; i++) {
                if (bytes[at + i] != PUFFIN_MAGIC[i]) {
                    throw invalid(location, "bad puffin magic at %s (offset %d)".formatted(where, at), null);
                }
            }
        }
    }
}
