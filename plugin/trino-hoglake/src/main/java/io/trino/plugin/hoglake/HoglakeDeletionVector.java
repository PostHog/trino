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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.TrinoException;
import org.roaringbitmap.IntIterator;
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

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
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
    static final int MAX_FOOTER_BYTES = 64 * 1024;
    // A bounded streaming parser, with field-name interning/canonicalization
    // disabled: no object tree or table grows with the number of JSON fields.
    // At most 64 KiB of input supplies token/context strings; UTF-16 strings,
    // TextBuffer growth/copies, input buffers and 16 parser contexts fit in
    // this 1 MiB envelope. Keep it live until the referenced path is charged
    // as part of the decoded vector. This is separate from Roaring's budget.
    private static final long FOOTER_PARSER_BYTES = 1024 * 1024;
    private static final JsonFactory JSON = JsonFactory.builder()
            .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
            .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
            .disable(JsonFactory.Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNestingDepth(16)
                    .maxStringLength(MAX_FOOTER_BYTES)
                    .maxNameLength(MAX_FOOTER_BYTES)
                    .build())
            .build();
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
    private static final int INSTANCE_SIZE = instanceSize(HoglakeDeletionVector.class);
    private static final int OPTIONAL_SIZE = instanceSize(Optional.class);
    // Fixed cursors, the layout summary and the bitmap validation iterator.
    private static final long DECODE_CONTROL_BYTES = 1024;

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
    private final long retainedSizeInBytes;

    private HoglakeDeletionVector(int[] bucketKeys, RoaringBitmap[] buckets, long cardinality, Optional<String> referencedDataFile, long retainedSizeInBytes)
    {
        this.bucketKeys = bucketKeys;
        this.buckets = buckets;
        this.cardinality = cardinality;
        this.referencedDataFile = referencedDataFile;
        this.retainedSizeInBytes = retainedSizeInBytes;
    }

    void unionInto(HoglakeDeleteBitmap target)
    {
        for (int i = 0; i < bucketKeys.length; i++) {
            target.union(bucketKeys[i], buckets[i]);
        }
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
        return retainedSizeInBytes;
    }

    public static HoglakeDeletionVector read(byte[] bytes, String location)
    {
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try {
            return read(bytes, location, memory);
        }
        finally {
            memory.close();
        }
    }

    static HoglakeDeletionVector read(byte[] bytes, String location, AggregatedMemoryContext memory)
    {
        requireNonNull(bytes, "bytes is null");
        requireNonNull(location, "location is null");
        try {
            return new PuffinDeletionVectorReader(bytes, location, memory).read();
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
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
        private final AggregatedMemoryContext memory;

        public PuffinDeletionVectorReader(byte[] bytes, String location, AggregatedMemoryContext memory)
        {
            this.bytes = bytes;
            this.location = location;
            this.memory = requireNonNull(memory, "memory is null");
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

            if (payloadSize > MAX_FOOTER_BYTES) {
                throw invalid(location, "puffin footer exceeds the %d-byte limit".formatted(MAX_FOOTER_BYTES), null);
            }
            LocalMemoryContext footerMemory = memory.newLocalMemoryContext("hoglake_deletion_vector_footer");
            footerMemory.setBytes(FOOTER_PARSER_BYTES);
            PuffinBlob blob = readFooter(payloadStart, payloadSize);
            long offset = blob.offset();
            long length = blob.length();
            // Range-check without adding untrusted values.
            if (offset < PUFFIN_MAGIC_SIZE || length < MIN_BLOB_SIZE || length > blobSectionEnd || offset > blobSectionEnd - length) {
                throw invalid(location, "%s blob range [%d, +%d) is out of bounds".formatted(DELETION_VECTOR_BLOB_TYPE, offset, length), null);
            }
            HoglakeDeletionVector vector = decodeBlob((int) offset, (int) length, blob.referencedDataFile());
            // On failure the enclosing split owner closes all contexts. Do not
            // let another engine callback replace a failed reservation's error.
            footerMemory.close();
            return vector;
        }

        private PuffinBlob readFooter(int start, int length)
                throws IOException
        {
            try (JsonParser parser = JSON.createParser(new ByteArrayInputStream(bytes, start, length))) {
                if (parser.nextToken() != JsonToken.START_OBJECT) {
                    throw invalid(location, "puffin footer payload has no blobs array", null);
                }
                PuffinBlob selected = null;
                int vectors = 0;
                boolean hasBlobs = false;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String name = parser.currentName();
                    parser.nextToken();
                    if ("blobs".equals(name) && parser.currentToken() == JsonToken.START_ARRAY) {
                        hasBlobs = true;
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            PuffinBlob blob = readBlob(parser);
                            if (blob != null) {
                                selected = blob;
                                vectors++;
                            }
                        }
                    }
                    else {
                        parser.skipChildren();
                    }
                }
                if (!hasBlobs) {
                    throw invalid(location, "puffin footer payload has no blobs array", null);
                }
                if (vectors != 1) {
                    throw invalid(location, "expected exactly one %s blob, found %d".formatted(DELETION_VECTOR_BLOB_TYPE, vectors), null);
                }
                if (parser.nextToken() != null) {
                    throw invalid(location, "trailing content in puffin footer", null);
                }
                return selected;
            }
            catch (IOException e) {
                throw invalid(location, "unparseable puffin footer payload", e);
            }
        }

        private PuffinBlob readBlob(JsonParser parser)
                throws IOException
        {
            if (parser.currentToken() != JsonToken.START_OBJECT) {
                parser.skipChildren();
                return null;
            }
            boolean vector = false;
            boolean compressed = false;
            Long offset = null;
            Long length = null;
            Optional<String> referenced = Optional.empty();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.currentName();
                parser.nextToken();
                switch (name) {
                    case "type" -> vector = parser.currentToken() == JsonToken.VALUE_STRING && DELETION_VECTOR_BLOB_TYPE.equals(parser.getText());
                    case "compression-codec" -> compressed = parser.currentToken() != JsonToken.VALUE_NULL;
                    case "offset" -> offset = parser.currentToken() == JsonToken.VALUE_NUMBER_INT ? parser.getLongValue() : null;
                    case "length" -> length = parser.currentToken() == JsonToken.VALUE_NUMBER_INT ? parser.getLongValue() : null;
                    case "properties" -> {
                        if (parser.currentToken() == JsonToken.START_OBJECT) {
                            while (parser.nextToken() != JsonToken.END_OBJECT) {
                                String property = parser.currentName();
                                parser.nextToken();
                                if ("referenced-data-file".equals(property) && parser.currentToken() == JsonToken.VALUE_STRING) {
                                    referenced = Optional.of(parser.getText());
                                }
                                parser.skipChildren();
                            }
                        }
                    }
                }
                parser.skipChildren();
            }
            if (!vector) {
                return null;
            }
            if (compressed) {
                throw invalid(location, "compressed %s blobs are not supported".formatted(DELETION_VECTOR_BLOB_TYPE), null);
            }
            if (offset == null || length == null) {
                throw invalid(location, "%s blob has no offset/length".formatted(DELETION_VECTOR_BLOB_TYPE), null);
            }
            return new PuffinBlob(offset, length, referenced);
        }

        private record PuffinBlob(long offset, long length, Optional<String> referencedDataFile) {}

        /**
         * {@code blob = declared length (4, BE) | magic | vector | CRC-32 (4, BE)},
         * where the declared length and the checksum cover magic + vector.
         */
        private HoglakeDeletionVector decodeBlob(int offset, int length, Optional<String> referencedDataFile)
                throws IOException
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
                throws IOException
        {
            if (length < Long.BYTES) {
                throw invalid(location, "truncated deletion vector bitmap", null);
            }
            ByteBuffer input = ByteBuffer.wrap(bytes, offset, length).slice().order(ByteOrder.LITTLE_ENDIAN);
            long bucketCount = input.getLong();
            if (bucketCount < 0 || bucketCount > Integer.MAX_VALUE) {
                throw invalid(location, "implausible bucket count %d".formatted(bucketCount), null);
            }
            long maximumBuckets = (length - Long.BYTES) / 12;
            if (bucketCount > maximumBuckets) {
                throw invalid(location, "bucket count %d exceeds the %d buckets %d vector bytes can hold"
                        .formatted(bucketCount, maximumBuckets, length), null);
            }
            int count = (int) bucketCount;
            long retained = INSTANCE_SIZE + sizeOfIntArray(count) + sizeOfObjectArray(count)
                    + OPTIONAL_SIZE + referencedDataFile.map(value -> (long) estimatedSizeOf(value)).orElse(0L);
            LocalMemoryContext decodedMemory = memory.newLocalMemoryContext("hoglake_deletion_vector_bitmap");
            decodedMemory.setBytes(retained + DECODE_CONTROL_BYTES);
            int[] bucketKeys = new int[count];
            RoaringBitmap[] buckets = new RoaringBitmap[count];
            long cardinality = 0;
            int previousKey = -1;
            for (int i = 0; i < count; i++) {
                if (input.remaining() < Integer.BYTES) {
                    throw invalid(location, "truncated deletion vector bitmap", null);
                }
                int key = input.getInt();
                if (key < 0) {
                    throw invalid(location, "negative bucket key %d".formatted(key), null);
                }
                if (key <= previousKey) {
                    throw invalid(location, "bucket keys are not strictly ascending (%d after %d)".formatted(key, previousKey), null);
                }
                previousKey = key;
                HoglakeDeletionVectorLayout layout = HoglakeDeletionVectorLayout.inspect(bytes, offset + input.position(), input.remaining(), location);
                long nextRetained = Math.addExact(retained, layout.retainedBytes());
                decodedMemory.setBytes(Math.addExact(nextRetained, layout.scratchBytes() + DECODE_CONTROL_BYTES));
                RoaringBitmap bitmap = new RoaringBitmap();
                try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes, offset + input.position(), layout.serializedBytes()))) {
                    bitmap.deserialize(stream);
                    if (stream.read() != -1) {
                        throw invalid(location, "roaring bitmap did not consume its inspected layout", null);
                    }
                }
                input.position(input.position() + layout.serializedBytes());
                if (bitmap.isEmpty()) {
                    throw invalid(location, "empty bucket %d".formatted(key), null);
                }
                long bucketCardinality = bitmap.getLongCardinality();
                requireConsistentBitmap(bitmap, key, bucketCardinality);
                bucketKeys[i] = key;
                buckets[i] = bitmap;
                cardinality = Math.addExact(cardinality, bucketCardinality);
                retained = nextRetained;
                decodedMemory.setBytes(retained + DECODE_CONTROL_BYTES);
            }
            if (input.hasRemaining()) {
                throw invalid(location, "trailing bytes after the deletion vector bitmap", null);
            }
            HoglakeDeletionVector vector = new HoglakeDeletionVector(bucketKeys, buckets, cardinality, referencedDataFile, retained);
            decodedMemory.setBytes(retained);
            return vector;
        }

        /**
         * A decoded bucket must be a well-formed roaring bitmap, and must
         * agree with itself about the row set it describes.
         *
         * <p>{@link RoaringBitmap#validate()} performs the structural check —
         * container ordering, per-container sortedness and run layout — and
         * rejects a container whose stored values repeat or descend. It is
         * the library's own verdict on the bytes, so it is asked first.
         *
         * <p>It does not, however, tie the containers to the accessors this
         * connector uses, so the decoded bucket is walked once more and
         * required to report one consistent row set: the values it enumerates
         * must be strictly increasing, exactly as many as its cardinality,
         * values it reports as deleted, and topped out at the value it
         * reports as largest. A mismatch between any two of those views would
         * make the count path and a scan describe different tables, and a
         * garbage largest value would defeat the position bound.
         *
         * <p>The walk keeps a count, the previous value, and the maximum, so
         * it adds no per-position memory: a compressed vector's cardinality
         * is unbounded by the file's size.
         */
        private void requireConsistentBitmap(RoaringBitmap bitmap, int key, long cardinality)
        {
            if (!bitmap.validate()) {
                throw invalid(location, "bucket %d is not a well-formed roaring bitmap".formatted(key), null);
            }
            long enumerated = 0;
            long previous = -1;
            boolean ordered = true;
            boolean contained = true;
            long maximum = -1;
            IntIterator values = bitmap.getIntIterator();
            while (values.hasNext()) {
                long value = Integer.toUnsignedLong(values.next());
                if (value <= previous) {
                    ordered = false;
                }
                previous = value;
                maximum = value;
                enumerated++;
                if (contained && !bitmap.contains((int) value)) {
                    contained = false;
                }
            }
            if (!ordered) {
                throw invalid(location, "bucket %d enumerates values out of order or repeated".formatted(key), null);
            }
            if (enumerated != cardinality) {
                throw invalid(location, "bucket %d reports cardinality %d but enumerates %d values"
                        .formatted(key, cardinality, enumerated), null);
            }
            if (!contained) {
                throw invalid(location, "bucket %d reports cardinality %d but does not contain every value it enumerates"
                        .formatted(key, cardinality), null);
            }
            if (Integer.toUnsignedLong(bitmap.last()) != maximum) {
                throw invalid(location, "bucket %d reports %d as its largest value but enumerates %d"
                        .formatted(key, Integer.toUnsignedLong(bitmap.last()), maximum), null);
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
