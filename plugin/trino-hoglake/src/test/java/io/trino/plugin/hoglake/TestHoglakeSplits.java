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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.filesystem.cache.CacheSplitAffinityProvider;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Split generation from /scan responses: the data-file/deletion-vector
 * pairing the scan reports is carried into the split unchanged, and a
 * pairing that cannot be trusted is rejected at planning.
 */
class TestHoglakeSplits
{
    private static final HoglakeDtos.DataFile DATA_FILE = new HoglakeDtos.DataFile(
            10, "s3://lake/t/a.parquet", "parquet", 25, 2048, 321L, 0, "provided", 3);
    private static final HoglakeDtos.DataFile DELETED_FROM = new HoglakeDtos.DataFile(
            11, "s3://lake/t/b.parquet", "parquet", 17, 1024, null, 25, "provided", 4);
    private static final HoglakeDtos.DeleteFile DELETE_FILE = new HoglakeDtos.DeleteFile(
            5, 11, "s3://lake/t/b.dv", "puffin-dv", 3, 64, 6);
    // Target split size for the byte-range planning tests.
    private static final long TARGET = 1000;
    private static final HoglakeSplit WHOLE = new HoglakeSplit("s3://lake/t/a.parquet", 2048, 25, Optional.empty(), 0);

    @Test
    void oneSplitPerDataFileCarryingScanMetadata()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)));

        assertThat(splits).hasSize(2);

        HoglakeSplit bare = splits.get(0);
        assertThat(bare.path()).isEqualTo("s3://lake/t/a.parquet");
        assertThat(bare.fileSizeBytes()).isEqualTo(2048);
        assertThat(bare.recordCount()).isEqualTo(25);
        assertThat(bare.deleteFilePath()).isEmpty();
        assertThat(bare.deleteFileFormat()).isEmpty();
        assertThat(bare.deleteCount()).isZero();

        HoglakeSplit paired = splits.get(1);
        assertThat(paired.path()).isEqualTo("s3://lake/t/b.parquet");
        assertThat(paired.deleteFilePath()).contains("s3://lake/t/b.dv");
        assertThat(paired.deleteFileFormat()).contains("puffin-dv");
        assertThat(paired.deleteCount()).isEqualTo(3);
        assertThat(paired.recordCount()).isEqualTo(17);
    }

    /**
     * Every file in the scan is planned; a deletion vector stays attached to
     * the file it deletes from, and its format rides along so the worker can
     * refuse one it cannot decode.
     */
    @Test
    void scanWithDeletionVectorsPlansEveryFile()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)));

        assertThat(splits).extracting(HoglakeSplit::path)
                .containsExactly("s3://lake/t/a.parquet", "s3://lake/t/b.parquet");
        assertThat(splits.get(1).deleteFilePath()).contains("s3://lake/t/b.dv");
    }

    /**
     * A scan that pairs a vector with a different data file cannot be
     * trusted at all: the pairing is refused before any split exists, rather
     * than letting a worker delete rows from the wrong file.
     */
    @Test
    void scanPairingAVectorWithAnotherDataFileIsRefused()
    {
        HoglakeDtos.DeleteFile mismatched =
                new HoglakeDtos.DeleteFile(5, 99, "s3://lake/t/b.dv", "puffin-dv", 3, 64, 6);

        assertThatThrownBy(() -> HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DELETED_FROM, mismatched))))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_INVALID_RESPONSE.toErrorCode()))
                .hasMessageContaining("s3://lake/t/b.dv")
                .hasMessageContaining("s3://lake/t/b.parquet");
    }

    /**
     * On a multi-node cluster every split crosses the wire as JSON
     * (coordinator -> worker task update). Use the engine's JSON codec to pin the round trip
     * for both DV shapes so a field change can't silently break workers.
     */
    @Test
    void splitSurvivesJsonRoundTripForWorkerDispatch()
            throws Exception
    {
        JsonCodec<HoglakeSplit> codec = JsonCodec.jsonCodec(HoglakeSplit.class);

        for (HoglakeSplit split : HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(DATA_FILE, null),
                new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE)))) {
            HoglakeSplit deserialized =
                    codec.fromJson(codec.toJson(split));
            assertThat(deserialized).isEqualTo(split);
        }
    }

    /**
     * Object-store splits: any worker may run any split (3-node semantics).
     */
    @Test
    void splitsAreRemotelyAccessibleWithNoAddressAffinity()
    {
        HoglakeSplit split = HoglakeSplitManager
                .toSplits(List.of(new HoglakeDtos.ScanFile(DATA_FILE, null)))
                .get(0);
        assertThat(split.isRemotelyAccessible()).isTrue();
        assertThat(split.getAddresses()).isEmpty();
    }

    // ---- byte-range planning ----------------------------------------------

    @Test
    void fileNoLargerThanTheTargetIsOneWholeFileSplit()
    {
        for (long size : new long[] {0, 1, TARGET - 1, TARGET}) {
            HoglakeDtos.DataFile file = new HoglakeDtos.DataFile(10, "s3://lake/t/small.parquet", "parquet", 25, size, 321L, 0, "provided", 3);
            List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(file, null)), TARGET);

            assertThat(splits).hasSize(1);
            HoglakeSplit split = splits.getFirst();
            assertThat(split.start()).isZero();
            assertThat(split.length()).isEqualTo(size);
            assertThat(split.wholeFile()).isTrue();
            assertThat(split.recordCount()).isEqualTo(25);
            assertThat(split.getSplitWeight()).isEqualTo(SplitWeight.standard());
            assertThat(split.getAffinityKey()).isEmpty();
            assertThat(split.footerSize()).isEqualTo(OptionalLong.of(321));
        }
    }

    @Test
    void defaultTargetKeepsSmallFilesWhole()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(DATA_FILE, null)));
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst().wholeFile()).isTrue();
        assertThat(splits.getFirst().footerSize()).isEqualTo(OptionalLong.of(321));
        assertThat(HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(DELETED_FROM, DELETE_FILE))).getFirst().footerSize()).isEmpty();

        // The default target is 1GB: a file of exactly that size stays whole, one byte more is cut in two.
        long target = HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE.toBytes();
        assertThat(target).isEqualTo(DataSize.of(1, GIGABYTE).toBytes());
        assertThat(HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(target, List.of()), null))))
                .singleElement()
                .satisfies(split -> assertThat(split.wholeFile()).isTrue());
        assertThat(HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(target + 1, List.of()), null))))
                .extracting(HoglakeSplit::start)
                .containsExactly(0L, target);
    }

    @Test
    void fileOfAWholeNumberOfTargetsIsCutIntoFullRanges()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(4 * TARGET, List.of()), null)), TARGET);

        assertThat(splits).hasSize(4);
        assertContiguousCover(splits, 4 * TARGET);
        assertThat(splits).extracting(HoglakeSplit::start).containsExactly(0L, TARGET, 2 * TARGET, 3 * TARGET);
        assertThat(splits).allSatisfy(split -> {
            assertThat(split.length()).isEqualTo(TARGET);
            // Every range carries the file's catalog count; only the first answers a metadata count from it.
            assertThat(split.recordCount()).isEqualTo(40);
            assertThat(split.wholeFile()).isFalse();
            assertThat(split.getSplitWeight()).isEqualTo(SplitWeight.standard());
            assertThat(split.footerSize()).isEqualTo(OptionalLong.of(50));
            assertThat(split.getAffinityKey()).isEmpty();
        });
    }

    @Test
    void affinityKeysComeFromTheFilesystemProvider()
    {
        List<HoglakeDtos.ScanFile> scan = List.of(
                new HoglakeDtos.ScanFile(largeFile(2 * TARGET, List.of()), null),
                new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(10, "s3://lake/t/small.parquet", "parquet", 25, TARGET, 321L, 0, "provided", 3), null));

        // Without a caching filesystem the provider is a no-op and scheduling is unconstrained.
        assertThat(HoglakeSplitManager.toSplits(scan, TARGET)).allSatisfy(split -> assertThat(split.getAffinityKey()).isEmpty());

        // With one, every range and every whole file carries the filesystem's own key.
        assertThat(HoglakeSplitManager.toSplits(scan, TARGET, new CacheSplitAffinityProvider()))
                .extracting(split -> split.getAffinityKey().orElseThrow())
                .containsExactly(
                        "s3://lake/t/large.parquet:0:1000",
                        "s3://lake/t/large.parquet:1000:1000",
                        "s3://lake/t/small.parquet:0:1000");
    }

    @Test
    void remainderBecomesAShorterLastRangeWithAProportionalWeight()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(2 * TARGET + 300, List.of()), null)), TARGET);

        assertThat(splits).hasSize(3);
        assertContiguousCover(splits, 2 * TARGET + 300);
        assertThat(splits.getLast().length()).isEqualTo(300);
        assertThat(splits.getLast().getSplitWeight()).isEqualTo(SplitWeight.fromProportion(0.3));
        assertThat(splits).allSatisfy(split -> assertThat(split.recordCount()).isEqualTo(40));

        // A tiny remainder still costs the minimum weight.
        HoglakeSplit tiny = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(TARGET + 1, List.of()), null)), TARGET).getLast();
        assertThat(tiny.length()).isEqualTo(1);
        assertThat(tiny.getSplitWeight()).isEqualTo(SplitWeight.fromProportion(0.05));
    }

    @Test
    void everyRangeOfAFileCarriesItsDeletionVector()
    {
        HoglakeDtos.DataFile file = new HoglakeDtos.DataFile(11, "s3://lake/t/b.parquet", "parquet", 17, 3 * TARGET + 1, null, 25, "provided", 4);
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(file, DELETE_FILE)), TARGET);

        assertThat(splits).hasSize(4);
        assertThat(splits).allSatisfy(split -> {
            assertThat(split.dataFileId()).isEqualTo(11);
            assertThat(split.deleteFilePath()).contains("s3://lake/t/b.dv");
            assertThat(split.deleteCount()).isEqualTo(3);
            assertThat(split.deleteFileFormat()).contains("puffin-dv");
            assertThat(split.footerSize()).isEmpty();
            assertThat(split.recordCount()).isEqualTo(17);
        });
    }

    @Test
    void filesArePlannedInScanOrderWithRangesAscending()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(largeFile(2 * TARGET, List.of()), null),
                new HoglakeDtos.ScanFile(DATA_FILE, null)), TARGET);

        assertThat(splits).extracting(split -> split.path() + "@" + split.start())
                .containsExactly(
                        "s3://lake/t/large.parquet@0",
                        "s3://lake/t/large.parquet@1000",
                        "s3://lake/t/a.parquet@0",
                        "s3://lake/t/a.parquet@1000",
                        "s3://lake/t/a.parquet@2000");
    }

    /**
     * With row-group offsets, consecutive row groups are packed while they fit
     * the target; cuts fall only on offsets, a row group larger than the target
     * is a range of its own, and the ranges still cover the whole file.
     */
    @Test
    void splitOffsetsPackRowGroupsUpToTheTarget()
    {
        // Row groups: [4, 400) [400, 800) [800, 2300) [2300, 2600) [2600, 2900) [2900, 3450);
        // the footer (542 bytes plus the 8-byte trailer) runs from 3450 to 4000.
        List<Long> offsets = List.of(4L, 400L, 800L, 2300L, 2600L, 2900L);
        List<HoglakeSplitManager.ByteRange> ranges = HoglakeSplitManager.planRanges(4000, OptionalLong.of(542), offsets, TARGET);

        assertThat(ranges).containsExactly(
                new HoglakeSplitManager.ByteRange(0, 800),
                new HoglakeSplitManager.ByteRange(800, 1500),
                new HoglakeSplitManager.ByteRange(2300, 600),
                new HoglakeSplitManager.ByteRange(2900, 1100));

        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(4000, offsets), null)), TARGET);
        assertContiguousCover(splits, 4000);
        assertThat(splits).extracting(HoglakeSplit::start).containsExactly(0L, 800L, 2300L, 2900L);
        assertThat(splits.get(1).getSplitWeight()).isEqualTo(SplitWeight.standard());
    }

    @Test
    void splitOffsetsWithoutAFooterSizeMeasureTheLastRowGroupToTheFileEnd()
    {
        // Without the footer size the last row group is [2900, 4000): 1100 bytes, so
        // it cannot join [2600, 2900); with it, it is 550 bytes and packs.
        List<Long> offsets = List.of(4L, 2600L, 2900L);
        assertThat(HoglakeSplitManager.planRanges(4000, OptionalLong.empty(), offsets, TARGET)).containsExactly(
                new HoglakeSplitManager.ByteRange(0, 2600),
                new HoglakeSplitManager.ByteRange(2600, 300),
                new HoglakeSplitManager.ByteRange(2900, 1100));
        assertThat(HoglakeSplitManager.planRanges(4000, OptionalLong.of(542), offsets, TARGET)).containsExactly(
                new HoglakeSplitManager.ByteRange(0, 2600),
                new HoglakeSplitManager.ByteRange(2600, 1400));
    }

    @Test
    void singleRowGroupFileStaysWholeWithItsCatalogCount()
    {
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(new HoglakeDtos.ScanFile(largeFile(4000, List.of(4L)), null)), TARGET);

        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst().wholeFile()).isTrue();
        assertThat(splits.getFirst().recordCount()).isEqualTo(40);
    }

    @Test
    void unusableSplitOffsetsFallBackToEvenCuts()
    {
        List<HoglakeSplitManager.ByteRange> evenCuts = HoglakeSplitManager.planRanges(2500, OptionalLong.of(10), List.of(), TARGET);
        assertThat(evenCuts).containsExactly(
                new HoglakeSplitManager.ByteRange(0, 1000),
                new HoglakeSplitManager.ByteRange(1000, 1000),
                new HoglakeSplitManager.ByteRange(2000, 500));

        for (List<Long> offsets : List.of(
                List.of(4L, 1200L, 900L),
                List.of(4L, 1200L, 1200L),
                List.of(4L, 1200L, 2500L),
                List.of(-1L, 1200L))) {
            assertThat(HoglakeSplitManager.planRanges(2500, OptionalLong.of(10), offsets, TARGET))
                    .describedAs("offsets %s", offsets)
                    .isEqualTo(evenCuts);
        }
    }

    @Test
    void splitOffsetsAreOptionalOnTheWire()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        String withoutOffsets =
                """
                {"data_file_id":1, "path":"s3://lake/t/a.parquet", "file_format":"parquet", "record_count":5,
                 "file_size_bytes":4000, "footer_size":100, "row_id_start":0, "stats_state":"provided", "begin_snapshot":1}
                """;
        assertThat(mapper.readValue(withoutOffsets, HoglakeDtos.DataFile.class).splitOffsets()).isEmpty();

        String withOffsets = withoutOffsets.replace("\"begin_snapshot\":1", "\"begin_snapshot\":1, \"split_offsets\":[4, 2000]");
        assertThat(mapper.readValue(withOffsets, HoglakeDtos.DataFile.class).splitOffsets()).containsExactly(4L, 2000L);

        String withNullOffsets = withoutOffsets.replace("\"begin_snapshot\":1", "\"begin_snapshot\":1, \"split_offsets\":null");
        assertThat(mapper.readValue(withNullOffsets, HoglakeDtos.DataFile.class).splitOffsets()).isEmpty();

        String withANullOffset = withoutOffsets.replace("\"begin_snapshot\":1", "\"begin_snapshot\":1, \"split_offsets\":[4, null]");
        assertThat(mapper.readValue(withANullOffset, HoglakeDtos.DataFile.class).splitOffsets()).isEmpty();
    }

    @Test
    void rangeSplitSurvivesJsonRoundTrip()
    {
        JsonCodec<HoglakeSplit> codec = JsonCodec.jsonCodec(HoglakeSplit.class);
        List<HoglakeSplit> splits = HoglakeSplitManager.toSplits(List.of(
                new HoglakeDtos.ScanFile(largeFile(2 * TARGET + 300, List.of()), null),
                new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(11, "s3://lake/t/b.parquet", "parquet", 17, 2 * TARGET, null, 25, "provided", 4), DELETE_FILE)), TARGET, new CacheSplitAffinityProvider());

        assertThat(splits).hasSize(5);
        for (HoglakeSplit split : splits) {
            HoglakeSplit deserialized = codec.fromJson(codec.toJson(split));
            assertThat(deserialized).isEqualTo(split);
            assertThat(deserialized.getSplitWeight()).isEqualTo(split.getSplitWeight());
            assertThat(deserialized.getAffinityKey()).isEqualTo(split.getAffinityKey()).isPresent();
            assertThat(deserialized.recordCount()).isEqualTo(split.recordCount()).isNotNegative();
        }
    }

    @Test
    void rangeMustLieInsideTheFile()
    {
        assertThatThrownBy(() -> WHOLE.withRange(-1, 10, SplitWeight.standard()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WHOLE.withRange(0, -1, SplitWeight.standard()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WHOLE.withRange(2000, 49, SplitWeight.standard()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(WHOLE.withRange(2000, 48, SplitWeight.standard()).recordCount()).isEqualTo(25);
        assertThat(WHOLE.withRange(0, 2048, SplitWeight.standard()).recordCount()).isEqualTo(25);
    }

    private static HoglakeDtos.DataFile largeFile(long size, List<Long> splitOffsets)
    {
        return new HoglakeDtos.DataFile(12, "s3://lake/t/large.parquet", "parquet", 40, size, 50L, 0, "provided", 3, splitOffsets);
    }

    private static void assertContiguousCover(List<HoglakeSplit> splits, long fileSize)
    {
        long next = 0;
        for (HoglakeSplit split : splits) {
            assertThat(split.start()).isEqualTo(next);
            assertThat(split.length()).isPositive();
            next = split.start() + split.length();
        }
        assertThat(next).isEqualTo(fileSize);
    }

    // ---- parquet column binding -------------------------------------------

    @Test
    void bindsByFieldIdWhenTheFileCarriesIds()
    {
        // File written after a rename: field id 1 is now called "renamed"
        // in the catalog but "id" in the file — the id wins.
        MessageType fileSchema = new MessageType(
                "t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).id(2).named("renamed"));

        HoglakeColumnHandle catalogColumn = new HoglakeColumnHandle("renamed", 1, BIGINT, false);
        Optional<org.apache.parquet.schema.Type> bound =
                HoglakePageSourceProvider.bindColumn(fileSchema, catalogColumn);
        assertThat(bound).isPresent();
        assertThat(bound.orElseThrow().getName()).isEqualTo("id");
    }

    @Test
    void fallsBackToNameForFilesWithoutFieldIds()
    {
        MessageType fileSchema = new MessageType(
                "t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("Score"));

        assertThat(HoglakePageSourceProvider.bindColumn(
                fileSchema, new HoglakeColumnHandle("id", 1, BIGINT, false)))
                .isPresent();
        // Case-insensitive fallback.
        assertThat(HoglakePageSourceProvider.bindColumn(
                        fileSchema, new HoglakeColumnHandle("score", 2, DOUBLE, true))
                .orElseThrow().getName()).isEqualTo("Score");
    }

    @Test
    void columnAbsentFromTheFileIsUnbound()
    {
        MessageType fileSchema = new MessageType(
                "t",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("id"));

        assertThat(HoglakePageSourceProvider.bindColumn(
                fileSchema, new HoglakeColumnHandle("added_later", 9, BIGINT, true)))
                .isEmpty();
    }
}
