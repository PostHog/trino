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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_INVALID;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND;
import static io.trino.plugin.hoglake.testing.ConnectorTestFixtures.memoryFileSystem;
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
    void decodesEmptyVectorFromEitherWriter()
    {
        // An empty vector is the smallest legal payload (a bucket count of
        // zero and nothing else), so it pins the bucket-count bound at its
        // lower edge in both layouts.
        assertThat(HoglakeDeletionVector.read(PuffinDeletionVectorFixtures.serverDeletionVector(), DV_PATH)
                .cardinality()).isZero();
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
     * Golden vectors for positions {1, 2, 3}, minted with the exact byte
     * operations of Hoglake's two writers and embedded so a change in this
     * connector's fixture cannot silently redefine the format.
     *
     * <p>Both layouts carry the same magic, bitmap, and big-endian CRC; they
     * differ only in the blob's 4-byte length prefix, because Hoglake's
     * server writer byte-swaps it into a big-endian {@code DataOutputStream}
     * (little-endian on disk) while the DuckDB client's
     * {@code Store<BSwap>(...)} writes it big-endian.
     */
    private static final String SERVER_BLOB_HEX =
            "26 00 00 00 D1 D3 39 64 01 00 00 00 00 00 00 00 00 00 00 00 3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00 03 00 B4 78 9D DA";
    private static final String DUCKDB_BLOB_HEX =
            "00 00 00 26 D1 D3 39 64 01 00 00 00 00 00 00 00 00 00 00 00 3A 30 00 00 01 00 00 00 00 00 02 00 10 00 00 00 01 00 02 00 03 00 B4 78 9D DA";

    @Test
    void fixtureMatchesTheBytesHoglakeWritersProduce()
    {
        // Pins the fixture against the writers' actual output; if this fails,
        // the fixture no longer describes Hoglake's format.
        assertThat(hex(PuffinDeletionVectorFixtures.serverDeletionVectorBlob(1L, 2L, 3L))).isEqualTo(SERVER_BLOB_HEX);
        assertThat(hex(PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L))).isEqualTo(DUCKDB_BLOB_HEX);
    }

    @Test
    void decodesTheServerWritersLittleEndianLengthPrefix()
    {
        // The server's compaction writer is the format's reference producer;
        // its prefix is the reverse of the DuckDB client's.
        assertThat(SERVER_BLOB_HEX).startsWith("26 00 00 00");
        assertThat(DUCKDB_BLOB_HEX).startsWith("00 00 00 26");

        HoglakeDeletionVector vector = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.serverDeletionVector(1L, 2L, 3L), DV_PATH);

        assertThat(vector.cardinality()).isEqualTo(3);
        assertThat(vector.isRowDeleted(1)).isTrue();
        assertThat(vector.isRowDeleted(2)).isTrue();
        assertThat(vector.isRowDeleted(3)).isTrue();
        assertThat(vector.isRowDeleted(4)).isFalse();
    }

    @Test
    void decodesBothWriterLayoutsToTheSameVector()
    {
        HoglakeDeletionVector server = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.serverDeletionVector(0L, 5L), DV_PATH);
        HoglakeDeletionVector duckdb = HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.deletionVector(0L, 5L), DV_PATH);

        assertThat(server.cardinality()).isEqualTo(duckdb.cardinality());
        for (long position : new long[] {0, 1, 5, 6}) {
            assertThat(server.isRowDeleted(position))
                    .describedAs("position %d", position)
                    .isEqualTo(duckdb.isRowDeleted(position));
        }
    }

    @Test
    void lengthPrefixInNeitherByteOrderIsRefused()
    {
        // A prefix whose big-endian reading (0x00002600 = 9728) and
        // little-endian reading (0x00260000 = 2490368) both disagree with
        // the blob's real length.
        byte[] blob = PuffinDeletionVectorFixtures.deletionVectorBlob(1L, 2L, 3L);
        blob[1] = 0x26;

        assertThatThrownBy(() -> HoglakeDeletionVector.read(containerWithBlob(blob), DV_PATH))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("length prefix does not match blob length");
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
    void unparseableFooterIsRefused()
    {
        assertThatThrownBy(() -> HoglakeDeletionVector.read(
                PuffinDeletionVectorFixtures.puffinWithPayload("not json", new byte[12]), DV_PATH))
                .hasMessageContaining("unparseable puffin footer payload");
    }

    // ---- loader: catalog pairing and file bounds ---------------------------

    @Test
    void loaderValidatesTheVectorAgainstTheSplitAndFile()
    {
        byte[] vector = PuffinDeletionVectorFixtures.deletionVector(1L, 3L);
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of(DV_PATH, vector));

        HoglakeDeletionVector loaded = load(fileSystem, split(2, Optional.of("puffin-dv")), 4);

        assertThat(loaded.cardinality()).isEqualTo(2);
        assertThat(loaded.isRowDeleted(1)).isTrue();
    }

    @Test
    void loaderRejectsADeleteCountThatDisagreesWithTheVector()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L, 3L)));

        assertThatThrownBy(() -> load(fileSystem, split(5, Optional.of("puffin-dv")), 4))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("the catalog reports 5 deleted rows but the vector deletes 2");
    }

    @Test
    void loaderRejectsAPositionBeyondTheEndOfTheFile()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L, 99L)));

        assertThatThrownBy(() -> load(fileSystem, split(2, Optional.of("puffin-dv")), 4))
                .hasMessageContaining("the vector deletes row 99, beyond the 4 rows of data file " + DATA_PATH);
    }

    @Test
    void loaderRejectsAVectorPairedWithAnotherDataFile()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of(
                DV_PATH, PuffinDeletionVectorFixtures.deletionVector("s3://lake/t/other.parquet", 1L)));

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("puffin-dv")), 4))
                .hasMessageContaining("the vector references data file s3://lake/t/other.parquet");
    }

    @Test
    void loaderRejectsAnUnsupportedFormat()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(
                Map.of(DV_PATH, PuffinDeletionVectorFixtures.deletionVector(1L)));

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("parquet-position-deletes")), 4))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining("Unsupported hoglake deletion vector format 'parquet-position-deletes'");
    }

    @Test
    void loaderFailsWhenTheVectorIsMissing()
    {
        TrinoFileSystemFactory fileSystem = memoryFileSystem(Map.of());

        assertThatThrownBy(() -> load(fileSystem, split(1, Optional.of("puffin-dv")), 4))
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

        assertThat(HoglakeDeletionVectorLoader.load(trinoFileSystem, split(1, Optional.of("puffin-dv")), 4)
                .isRowDeleted(0)).isTrue();
    }

    @Test
    void splitRetainsTheDeletionVectorMetadataForWorkerDispatch()
    {
        HoglakeSplit split = split(3, Optional.of("puffin-dv"));

        assertThat(split.getRetainedSizeInBytes()).isGreaterThan(DATA_PATH.length() + DV_PATH.length());
    }

    private static HoglakeDeletionVector load(TrinoFileSystemFactory fileSystem, HoglakeSplit split, long rowCount)
    {
        return HoglakeDeletionVectorLoader.load(fileSystem.create(ConnectorTestFixtures.session()), split, rowCount);
    }

    private static HoglakeSplit split(long deleteCount, Optional<String> format)
    {
        return new HoglakeSplit(DATA_PATH, 1024, 4, Optional.of(DV_PATH), deleteCount, format);
    }
}
