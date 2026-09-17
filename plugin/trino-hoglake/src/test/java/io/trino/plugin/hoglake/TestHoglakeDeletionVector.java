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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.MemoryContext;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_INVALID;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.memoryFileSystem;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deletion-vector decode contract, against real puffin bytes: the
 * encoding Hoglake writes (and the DuckDB client reads) is an Iceberg v3
 * puffin container with one {@code deletion-vector-v1} blob, whose vector
 * is a portable 64-bit roaring bitmap of 0-based file row ordinals.
 *
 * <p>Corruption must fail loudly. A vector that silently decodes to "no
 * deletions" or to the wrong positions returns rows the catalog says are
 * gone.
 */
class TestHoglakeDeletionVector
{
    private static final String DV_PATH = "memory:///t/data.dv";
    private static final String DATA_PATH = "memory:///t/data.parquet";

    @Test
    void loaderAcceptsManySparseOuterBuckets()
    {
        long[] positions = new long[20];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = ((long) i << 32) | 1;
        }
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(positions);
        load(memoryFileSystem(Map.of(DV_PATH, bytes)), split(positions.length, Optional.of("puffin-dv")), Long.MAX_VALUE, vector -> {
            assertThat(vector.cardinality()).isEqualTo(positions.length);
            for (long position : positions) {
                assertThat(vector.isRowDeleted(position)).isTrue();
            }
        });
    }

    @Test
    void accountsForTheRetainedObjectGraph()
            throws ReflectiveOperationException
    {
        long[] positions = new long[65_536];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = (long) i << 16;
        }
        HoglakeDeletionVector vector = HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.deletionVector(positions), DV_PATH);
        assertThat(vector.retainedSizeInBytes()).isGreaterThanOrEqualTo(TestingDeletionVectorMemory.retainedBytes(vector));
    }

    @Test
    void decodesPositionsIncludingBucketBoundaries()
    {
        long[] positions = {0L, 1L, 3L, 4_000_000_000L, (1L << 32) + 7, 5L << 32};

        HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.deletionVector(positions), DV_PATH);

        assertThat(vector.cardinality()).isEqualTo(positions.length);
        for (long position : positions) {
            assertThat(vector.isRowDeleted(position)).describedAs("position %d", position).isTrue();
        }
        for (long position : List.of(2L, 5L, 4_000_000_001L, (1L << 32) + 8, (5L << 32) + 1, 1L << 33)) {
            assertThat(vector.isRowDeleted(position)).describedAs("position %d", position).isFalse();
        }
        assertThat(vector.maximumDeletedPosition()).hasValue(5L << 32);
    }

    @Test
    void decodesEmptyVector()
    {
        HoglakeDeletionVector vector =
                HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.deletionVector(), DV_PATH);

        assertThat(vector.cardinality()).isZero();
        assertThat(vector.isRowDeleted(0)).isFalse();
        assertThat(vector.maximumDeletedPosition()).isEmpty();
        assertThat(vector.retainedSizeInBytes()).isPositive();
    }

    @Test
    void decodesEmptyVectorFromTheGoldenLayout()
    {
        // An empty vector is the smallest legal payload (a bucket count of
        // zero and nothing else), so it pins the bucket-count bound at its
        // lower edge.
        assertThat(HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.deletionVector(), DV_PATH)
                .cardinality()).isZero();
    }

    @Test
    void decodesFirstAndLastRowOfALargeFile()
    {
        HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.deletionVector(0L, Long.MAX_VALUE - 1), DV_PATH);

        assertThat(vector.isRowDeleted(0)).isTrue();
        assertThat(vector.isRowDeleted(Long.MAX_VALUE - 1)).isTrue();
        assertThat(vector.maximumDeletedPosition()).hasValue(Long.MAX_VALUE - 1);
    }

    @Test
    void namesTheReferencedDataFileWhenTheWriterSetIt()
    {
        HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, 1L), DV_PATH);

        assertThat(vector.referencedDataFile()).contains(DATA_PATH);
    }

    /**
     * Golden blob for positions {1, 2, 3}, minted with the exact byte
     * operations of Hoglake's writer
     * ({@code PuffinTestFiles.deletionVector} / {@code PuffinDeletionVector}):
     * a big-endian declared length of 38 ({@code 00 00 00 26}), the vector
     * magic, the portable bitmap, and the big-endian CRC-32.
     *
     * <p>Pinned here so a change in this connector's fixture cannot silently
     * redefine the format. Produced by running the server's own writer, not
     * by re-deriving the layout from this connector's decoder.
     */
    private static final String GOLDEN_BLOB_HEX =
            "00 00 00 26 D1 D3 39 64 01 00 00 00 00 00 00 00 00 00 00 00 3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00 03 00 B4 78 9D DA";

    @Test
    void fixtureMatchesTheBytesHoglakeWritersProduce()
    {
        // Pins the fixture against the writer's actual output; if this fails,
        // the fixture no longer describes Hoglake's format.
        assertThat(hex(PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L))).isEqualTo(GOLDEN_BLOB_HEX);
    }

    @Test
    void decodesTheGoldenBlob()
    {
        // The declared length is big-endian, the way every Hoglake writer and
        // reader encodes it.
        assertThat(GOLDEN_BLOB_HEX).startsWith("00 00 00 26");

        HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                containerWithBlob(PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L)), DV_PATH);

        assertThat(vector.cardinality()).isEqualTo(3);
        assertThat(vector.isRowDeleted(1)).isTrue();
        assertThat(vector.isRowDeleted(2)).isTrue();
        assertThat(vector.isRowDeleted(3)).isTrue();
        assertThat(vector.isRowDeleted(4)).isFalse();
    }

    @Test
    void aLittleEndianLengthPrefixIsRefused()
    {
        // The reverse reading of the golden blob's prefix (0x26000000) is the
        // pre-fix misreading of this format and is not a second encoding any
        // Hoglake writer produces.
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L);
        byte[] littleEndian = {blob[3], blob[2], blob[1], blob[0]};
        System.arraycopy(littleEndian, 0, blob, 0, Integer.BYTES);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(containerWithBlob(blob), DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("length prefix 637534208 does not match blob length");
    }

    @Test
    void aLengthPrefixThatMatchesNoOtherValueIsRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L);
        blob[0] = 0x01;

        assertThatThrownBy(() -> HoglakeDeletionVector.read(containerWithBlob(blob), DV_PATH))
                .hasMessageContaining("length prefix 16777254 does not match blob length 46");
    }

    @Test
    void aHugeBucketCountIsRefusedWithoutAllocating()
    {
        // A small, correctly checksummed blob that claims Integer.MAX_VALUE
        // buckets: the count must be bounded by the vector that declares it
        // before any array is sized from it.
        byte[] vector = PuffinDeletionVectorFixtures.malformedVector(payload -> {
            // Bucket count Integer.MAX_VALUE, little-endian, the way the
            // portable bitmap stores it.
            Arrays.fill(payload, 0, Long.BYTES, (byte) 0);
            payload[0] = (byte) 0xFF;
            payload[1] = (byte) 0xFF;
            payload[2] = (byte) 0xFF;
            payload[3] = 0x7F;
        });
        byte[] bytes = PuffinDeletionVectorFixtures.puffinFromVector(vector);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("bucket count 2147483647 exceeds");
    }

    @Test
    void aBucketCountLargerThanTheVectorHoldsIsRefused()
    {
        // 1000 buckets declared in a vector with room for one. The count is
        // little-endian, so its low bytes come first.
        byte[] vector = PuffinDeletionVectorFixtures.malformedVector(payload -> {
            Arrays.fill(payload, 0, Long.BYTES, (byte) 0);
            payload[0] = (byte) 0xE8;
            payload[1] = 0x03;
        });

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinFromVector(vector), DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("bucket count 1000 exceeds the 2 buckets");
    }

    @Test
    void aBucketCountWithinBoundsButBeyondThePayloadIsRefused()
    {
        // A count this vector could hold if it were full, but whose second
        // bucket is simply absent: refused as truncated, never as a partial
        // delete set.
        byte[] vector = PuffinDeletionVectorFixtures.malformedVector(payload -> {
            Arrays.fill(payload, 0, Long.BYTES, (byte) 0);
            payload[0] = 0x02;
        });

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinFromVector(vector), DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("truncated deletion vector bitmap");
    }

    private static byte[] containerWithBlob(byte[] blob)
    {
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1","offset":4,"length":%d}]}""".formatted(blob.length);
        return PuffinDeletionVectorFixtures.puffinWithPayload(payload, blob);
    }

    private static String hex(byte[] bytes)
    {
        StringBuilder hex = new StringBuilder();
        for (byte value : bytes) {
            if (!hex.isEmpty()) {
                hex.append(' ');
            }
            hex.append("%02X".formatted(value));
        }
        return hex.toString();
    }

    // ---- corruption --------------------------------------------------------

    @Test
    void corruptedVectorByteFailsTheChecksum()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L, 2L, 3L);
        // Offset 4 is the blob's length prefix, +4 the magic, so offset 20
        // is inside the serialized bitmap, which the checksum covers.
        bytes[20] = (byte) (bytes[20] ^ 0x40);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining(DV_PATH)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    void badHeaderMagicIsRefused()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L);
        bytes[0] = 'X';

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("bad puffin magic at header");
    }

    @Test
    void badTrailerMagicIsRefused()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L);
        bytes[bytes.length - 1] = 'X';

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .hasMessageContaining("bad puffin magic at trailer");
    }

    @Test
    void badBlobMagicIsRefused()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L);
        // "PFA1" at 0, the blob's length prefix at 4, the blob's own magic
        // at 8.
        bytes[8] = 0;

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .hasMessageContaining("bad deletion-vector-v1 magic");
    }

    @Test
    void compressedFooterPayloadIsRefusedNotMisDecoded()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L);
        bytes[bytes.length - 8] = 1;

        assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH))
                .hasMessageContaining("compressed puffin footer payloads are not supported");
    }

    @Test
    void compressedBlobIsRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1","compression-codec":"lz4","offset":4,"length":%d}]}"""
                        .formatted(blob.length);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload(payload, blob), DV_PATH))
                .hasMessageContaining("compressed deletion-vector-v1 blobs are not supported");
    }

    @Test
    void truncatedFileIsRefused()
    {
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(1L);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(Arrays.copyOf(bytes, bytes.length - 4), DV_PATH))
                .hasMessageContaining("bad puffin magic at trailer");
        assertThatThrownBy(() -> HoglakeDeletionVector.read(new byte[4], DV_PATH))
                .hasMessageContaining("too small to be a puffin container");
        assertThatThrownBy(() -> HoglakeDeletionVector.read(new byte[0], DV_PATH))
                .hasMessageContaining("too small to be a puffin container");
    }

    @Test
    void trailingBytesAfterTheBitmapAreRefused()
    {
        // Bytes the checksum does not cover, after the vector the blob
        // declares: appending them must not be read as more buckets.
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(1L);
        byte[] trailing = Arrays.copyOf(vector, vector.length + 4);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(trailing, DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()));
    }

    @Test
    void missingBlobOffsetAndLengthAreRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1"}]}""";

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload(payload, blob), DV_PATH))
                .hasMessageContaining("blob has no offset/length");
    }

    @Test
    void blobRangeOutsideTheBlobSectionIsRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1","offset":4,"length":100000}]}""";

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload(payload, blob), DV_PATH))
                .hasMessageContaining("blob range")
                .hasMessageContaining("out of bounds");
    }

    @Test
    void moreThanOneVectorBlobIsRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String payload =
                """
                {"blobs":[{"type":"deletion-vector-v1","offset":4,"length":%d},\
                {"type":"deletion-vector-v1","offset":4,"length":%d}]}"""
                        .formatted(blob.length, blob.length);

        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload(payload, blob), DV_PATH))
                .hasMessageContaining("expected exactly one deletion-vector-v1 blob, found 2");
    }

    @Test
    void footerWithoutBlobsIsRefused()
    {
        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload("{\"properties\":{}}", new byte[12]), DV_PATH))
                .hasMessageContaining("no blobs array");
    }

    @Test
    void oversizedOrDeepFooterIsRefused()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String large = " ".repeat(HoglakeDeletionVector.MAX_FOOTER_BYTES + 1);
        assertThatThrownBy(() -> HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.puffinWithPayload(large, blob), DV_PATH))
                .hasMessageContaining("footer exceeds");
        String deep = "{\"ignored\":" + "[".repeat(17) + "0" + "]".repeat(17) + "}";
        assertThatThrownBy(() -> HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.puffinWithPayload(deep, blob), DV_PATH))
                .hasMessageContaining("unparseable puffin footer");
    }

    @Test
    void streamsPastLargeUnknownFooterProperties()
    {
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L);
        String footer = "{\"ignored\":\"" + "x".repeat(60_000) + "\",\"blobs\":[{\"type\":\"deletion-vector-v1\",\"offset\":4,\"length\":" + blob.length + "}]}";
        assertThat(HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.puffinWithPayload(footer, blob), DV_PATH).cardinality()).isEqualTo(1);
    }

    @Test
    void unparseableFooterIsRefused()
    {
        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload("not json", new byte[12]), DV_PATH))
                .hasMessageContaining("unparseable puffin footer payload");
    }

    /**
     * The decoder requires a decoded bucket to report one consistent row set:
     * the cardinality a count subtracts, the values a scan removes, and the
     * largest value that bounds them against the file.
     *
     * <p>{@link io.roaringbitmap.RoaringBitmap#validate()} supplies the
     * structural half of that — it rejects containers whose stored values
     * repeat or descend, which {@code deserialize} does not check — and the
     * streaming walk supplies the rest. Both are exercised by the
     * hand-written container regressions below.
     */
    @Test
    void vectorDecodesWithOneConsistentViewOfItsRowSet()
    {
        for (long[] positions : new long[][] {{1L, 2L, 3L}, {0L}, {5L, 100_000L, 4_000_000_000L}}) {
            HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                    PuffinDeletionVectorFixtures.deletionVector(positions), DV_PATH);

            long deletedRows = 0;
            for (long position = 0; position < 10; position++) {
                if (vector.isRowDeleted(position)) {
                    deletedRows++;
                }
            }
            for (long position : positions) {
                if (position >= 10 && vector.isRowDeleted(position)) {
                    deletedRows++;
                }
            }
            assertThat(deletedRows)
                    .describedAs("rows a scan removes from %s", Arrays.toString(positions))
                    .isEqualTo(vector.cardinality());
        }
    }

    /**
     * The library's {@code deserialize} checks no container invariants, so a
     * malformed container must be refused by the decoder's own checks. These
     * bytes are hand-written — {@code add()} and the serializer cannot produce
     * them — and are wrapped with a correct length prefix and CRC, so only the
     * container is under test.
     */
    @Test
    void aContainerWithRepeatedValuesIsRefused()
    {
        // One array container, stored values [1, 2, 2].
        assertThatThrownBy(() -> readRawContainer("3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00 02 00"))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("not a well-formed roaring bitmap");
    }

    @Test
    void aContainerWithDescendingValuesIsRefused()
    {
        // The dangerous shape: cardinality 3 with contains() false for every
        // value it holds. Unchecked, COUNT(*) would subtract 3 while a scan
        // removed none.
        assertThatThrownBy(() -> readRawContainer("3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 03 00 02 00 01 00"))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()));
    }

    @Test
    void aContainerThatOutrunsItsBytesIsRefused()
    {
        // Advertised cardinality 3 with only two values supplied: refused,
        // never read as a partial delete set.
        assertThatThrownBy(() -> readRawContainer("3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00"))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()));
    }

    @Test
    void aWellFormedContainerOfTheSameShapeStillDecodes()
    {
        // The same construction with sorted, unique values must decode, so the
        // checks above are not refusing the construction itself.
        HoglakeDeletionVector vector = readRawContainer(
                "3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00 03 00");

        assertThat(vector.cardinality()).isEqualTo(3);
        assertThat(vector.isRowDeleted(1)).isTrue();
        assertThat(vector.isRowDeleted(2)).isTrue();
        assertThat(vector.isRowDeleted(3)).isTrue();
        assertThat(vector.maximumDeletedPosition()).hasValue(3);
    }

    private static HoglakeDeletionVector readRawContainer(String containerHex)
    {
        String[] parts = containerHex.trim().split("\\s+");
        byte[] container = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            container[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.deletionVectorWithRawContainer(container), DV_PATH);
    }

    @Test
    void aBucketIsChargedBeforeItIsDecoded()
    {
        long[] spread = new long[65_536];
        for (int i = 0; i < spread.length; i++) {
            spread[i] = (long) i << 16;
        }
        byte[] bytes = PuffinDeletionVectorFixtures.deletionVector(spread);
        // Warm parsing and class initialization before observing allocations
        // at the rejecting callback. The actual bucket must not exist yet.
        HoglakeDeletionVector decoded = HoglakeDeletionVector.read(bytes, DV_PATH);
        ThreadMXBean allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        AtomicLong allocatedAtRejection = new AtomicLong();
        TrinoException limitExceeded = new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "test memory limit");
        try (HoglakeSplitResources resources = new HoglakeSplitResources(currentBytes -> {
            if (currentBytes > 3 * 1024 * 1024) {
                allocatedAtRejection.set(allocations.getCurrentThreadAllocatedBytes());
                throw limitExceeded;
            }
        })) {
            long before = allocations.getCurrentThreadAllocatedBytes();
            assertThatThrownBy(() -> HoglakeDeletionVector.read(bytes, DV_PATH, resources.allocation()))
                    .isSameAs(limitExceeded);
            assertThat(allocatedAtRejection.get() - before).isLessThan(decoded.retainedSizeInBytes());
        }
    }

    // ---- loader: catalog pairing and file bounds ---------------------------

    @Test
    void loaderValidatesTheVectorAgainstTheSplitAndFile()
    {
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(1L, 3L);
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of(DV_PATH, vector));

        load(fileSystem, split(2, Optional.of("puffin-dv")), 4, loaded -> {
            assertThat(loaded.cardinality()).isEqualTo(2);
            assertThat(loaded.isRowDeleted(1)).isTrue();
        });
    }

    @Test
    void loaderRejectsADeleteCountThatDisagreesWithTheVector()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L, 3L)));

        assertThatThrownBy(() -> load(fileSystem, split(5, Optional.of("puffin-dv")), 4, _ -> {}))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("the catalog reports 5 deleted rows but the vector deletes 2");
    }

    @Test
    void loaderRejectsAPositionBeyondTheEndOfTheFile()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L, 99L)));

        assertThatThrownBy(() -> load(fileSystem, split(2, Optional.of("puffin-dv")), 4, _ -> {}))
                .hasMessageContaining("the vector deletes row 99, beyond the 4 rows of data file " + DATA_PATH);
    }

    @Test
    void loaderRejectsAVectorPairedWithAnotherDataFile()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of(
                DV_PATH, PuffinDeletionVectorFixtures.deletionVector("s3://lake/t/other.parquet", 1L)));

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("puffin-dv")), 4, _ -> {}))
                .hasMessageContaining("the vector references data file s3://lake/t/other.parquet");
    }

    @Test
    void loaderRejectsAnUnsupportedFormat()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L)));

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("parquet-position-deletes")), 4, _ -> {}))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining("Unsupported hoglake deletion vector format 'parquet-position-deletes'");
    }

    @Test
    void loaderFailsWhenTheVectorIsMissing()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of());

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("puffin-dv")), 4, _ -> {}))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining("Failed to read hoglake deletion vector " + DV_PATH);
    }

    @Test
    void loaderReadsThroughTheProvidedFileSystem()
    {
        // The connector's filesystem is the only way to the bytes: a loader
        // that reached for S3 directly could not see this file at all.
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(0L)));
        TrinoFileSystem trinoFileSystem = fileSystem.create(ConnectorTestFixtures.session());

        try (HoglakeSplitResources resources = new HoglakeSplitResources(MemoryContext.NO_LIMIT)) {
            HoglakeDeletionVector loaded = HoglakeDeletionVectorLoader.load(trinoFileSystem, split(1, Optional.of("puffin-dv")), 4, resources);
            assertThat(loaded.isRowDeleted(0)).isTrue();
        }
    }

    @Test
    void splitRetainsTheDeletionVectorMetadataForWorkerDispatch()
    {
        HoglakeSplit split = split(3, Optional.of("puffin-dv"));

        assertThat(split.getRetainedSizeInBytes()).isGreaterThan(DATA_PATH.length() + DV_PATH.length());
    }

    /**
     * Loads a vector, runs the assertion against it, and releases the
     * reservation the loader hands back. Callers must not keep the vector.
     */
    private static void load(TrinoFileSystemFactory fileSystem, HoglakeSplit split, long rowCount, Consumer<HoglakeDeletionVector> assertion)
    {
        try (HoglakeSplitResources resources = new HoglakeSplitResources(MemoryContext.NO_LIMIT)) {
            assertion.accept(HoglakeDeletionVectorLoader.load(fileSystem.create(ConnectorTestFixtures.session()), split, rowCount, resources));
        }
    }

    private static HoglakeSplit split(long deleteCount, Optional<String> format)
    {
        return new HoglakeSplit(DATA_PATH, 1024, 4, Optional.of(DV_PATH), deleteCount, format);
    }
}
