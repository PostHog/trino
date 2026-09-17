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

import io.airlift.json.JsonCodec;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

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
