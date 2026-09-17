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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_INVALID;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deletion-vector reads through the real page-source path
 * (HoglakePageSourceProvider -> trino-parquet -> memory filesystem) with
 * real Parquet bytes and real puffin deletion-vector bytes.
 *
 * <p>The rows a query returns are compared against the rows the vector says
 * survive, not merely against an expected row count: a mask applied to the
 * wrong position returns the same number of rows with the wrong values.
 */
class TestHoglakeDeletionVectorReads
{
    private static final String DATA_PATH = "memory:///dv/reads.parquet";
    private static final String DV_PATH = "memory:///dv/reads.dv";

    private static final long[] VALUES = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};

    // ---- no deletion vector -------------------------------------------------

    @Test
    void fileWithoutDeletionVectorIsUnchanged()
    {
        assertThat(read(parquet(), List.of(value()), Optional.empty(), Optional.empty(), 10))
                .containsExactlyElementsOf(rows(VALUES));
    }

    @Test
    void emptyDeletionVectorDeletesNothing()
    {
        assertThat(read(parquet(), List.of(value()), vector(), Optional.empty(), 10))
                .containsExactlyElementsOf(rows(VALUES));
    }

    // ---- positions ----------------------------------------------------------

    @Test
    void deletesFirstAndLastRow()
    {
        assertThat(read(parquet(), List.of(value()), vector(0, 9), Optional.empty(), 10))
                .containsExactlyElementsOf(rows(10, 20, 30, 40, 50, 60, 70, 80));
    }

    @Test
    void deletesAnInteriorRun()
    {
        assertThat(read(parquet(), List.of(value()), vector(3, 4, 5), Optional.empty(), 10))
                .containsExactlyElementsOf(rows(0, 10, 20, 60, 70, 80, 90));
    }

    @Test
    void deletesEveryRow()
    {
        long[] all = new long[VALUES.length];
        for (int i = 0; i < VALUES.length; i++) {
            all[i] = i;
        }

        assertThat(read(parquet(), List.of(value()), vector(all), Optional.empty(), 10)).isEmpty();
    }

    @Test
    void deletedRowsAreGoneFromTheirProjection()
    {
        // Both columns are projected; the mask must drop the same positions
        // from every channel.
        assertThat(read(parquet(), List.of(value(), label()), vector(1, 3), Optional.empty(), 10))
                .containsExactly(
                        List.of(0L, "v0"),
                        List.of(20L, "v2"),
                        List.of(40L, "v4"),
                        List.of(50L, "v5"),
                        List.of(60L, "v6"),
                        List.of(70L, "v7"),
                        List.of(80L, "v8"),
                        List.of(90L, "v9"));
    }

    @Test
    void deletingDifferentColumnsIsIndependent()
    {
        // A projection that omits the deleted column still drops its rows.
        assertThat(read(parquet(), List.of(label()), vector(2), Optional.empty(), 10))
                .containsExactlyElementsOf(rowsOfLabelsExcept(2));
    }

    @Test
    void nullValuesSurviveAndDeletedNullsStayDeleted()
    {
        byte[] parquet = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("value"),
                BIGINT,
                Arrays.asList(1L, null, 3L, null, 5L))));

        assertThat(read(parquet, List.of(value()), vector(0, 3), Optional.empty(), 5))
                .containsExactlyElementsOf(Arrays.asList(
                        Arrays.asList((Object) null),
                        List.of(3L),
                        List.of(5L)));
    }

    // ---- pages, batches, and row groups -------------------------------------

    @Test
    void deletesAcrossManyPagesOfWork()
    {
        // The reader's batch is far smaller than this file, so the deleted
        // positions span several pages; positions must stay file-relative
        // across page boundaries.
        int rows = 20_000;
        byte[] parquet = sequentialParquet(rows);
        long[] deleted = {0, 1, 4_999, 5_000, 9_999, 12_345, 19_998, 19_999};

        List<List<Object>> result = read(parquet, List.of(value()), vector(deleted), Optional.empty(), rows);

        assertThat(result).hasSize(rows - deleted.length);
        assertThat(result.getFirst()).containsExactly(2L);
        assertThat(result.get(1)).containsExactly(3L);
        assertThat(result.getLast()).containsExactly((long) rows - 3);
        assertThat(result).extracting(row -> row.getFirst())
                .doesNotContain(0L, 1L, 4_999L, 5_000L, 9_999L, 12_345L, 19_998L, 19_999L);
    }

    @Test
    void deletesAcrossRowGroups()
    {
        // Four row groups of 2 rows each; delete a row from each group.
        byte[] parquet = ConnectorTestFixtures.writeParquet(
                List.of(new FileColumn(
                        Types.optional(PrimitiveTypeName.INT64).id(1).named("value"),
                        BIGINT,
                        Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L))),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(2).build());

        assertThat(read(parquet, List.of(value()), vector(0, 3, 4, 7), Optional.empty(), 8))
                .containsExactlyElementsOf(rows(1, 2, 5, 6));
    }

    @Test
    void deletesUseFilePositionsWhenAnEarlierRowGroupIsPruned()
    {
        // Row group 0 holds 0..3, row group 1 holds 100..103. The predicate
        // prunes row group 0, so the first row this scan reads is file row 4.
        // Deleting file row 1 (inside the pruned group) must delete nothing,
        // and deleting file row 5 must delete the second row that is read.
        byte[] parquet = ConnectorTestFixtures.writeParquet(
                List.of(new FileColumn(
                        Types.optional(PrimitiveTypeName.INT64).id(1).named("value"),
                        BIGINT,
                        Arrays.asList(0L, 1L, 2L, 3L, 100L, 101L, 102L, 103L))),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(4).build());

        assertThat(read(parquet, List.of(value()), vector(1), Optional.of(lowerBound(100)), 8))
                .containsExactlyElementsOf(rows(100, 101, 102, 103));
        assertThat(read(parquet, List.of(value()), vector(5), Optional.of(lowerBound(100)), 8))
                .containsExactlyElementsOf(rows(100, 102, 103));
    }

    // ---- projection and filters ---------------------------------------------

    /**
     * The connector prunes whole row groups; the engine evaluates the
     * residual filter. What the page source owes the engine is every row
     * that is not deleted, with the deleted ones gone: a deleted row must
     * never be handed to the filter that would have matched it.
     */
    @Test
    void rowsThatFailTheFilterAreStillReadWithoutTheDeletedOnes()
    {
        // The predicate prunes nothing here, and filters `value > 20` are
        // the engine's; file rows 4 and 5 (40 and 50) are deleted.
        List<List<Object>> rows = read(parquet(), List.of(value()), vector(4, 5), Optional.of(lowerBound(30)), 10);

        assertThat(rows).containsExactlyElementsOf(rows(0, 10, 20, 30, 60, 70, 80, 90));
        assertThat(rows).extracting(row -> row.getFirst()).doesNotContain(40L, 50L);
    }

    @Test
    void countOverTheReadRowsMatchesTheVisibleRows()
    {
        ConnectorPageSource pageSource = open(parquet(), List.of(), vector(1, 5, 9), Optional.empty(), 10);

        long visible = 0;
        while (!pageSource.isFinished()) {
            visible += pageSource.getNextSourcePage().getPositionCount();
        }

        assertThat(visible).isEqualTo(7);
    }

    // ---- failure handling ---------------------------------------------------

    @Test
    void corruptVectorFailsTheQuery()
    {
        byte[] corrupt = PuffinDeletionVectorFixtures.deletionVector(1L, 2L);
        corrupt[20] = (byte) (corrupt[20] ^ 0x40);

        assertThatThrownBy(() -> open(parquet(), List.of(value()), Optional.of(corrupt), Optional.empty(), 10))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining(DV_PATH);
    }

    @Test
    void missingVectorFailsTheQuery()
    {
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, parquet()));

        assertThatThrownBy(() -> open(fileSystem, List.of(value()), split(2)))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining(DV_PATH);
    }

    @Test
    void vectorDeletingAPositionBeyondTheFileFailsTheQuery()
    {
        assertThatThrownBy(() -> open(parquet(), List.of(value()), vector(3, 10), Optional.empty(), 10))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("the vector deletes row 10, beyond the 10 rows");
    }

    @Test
    void vectorDisagreeingWithTheCatalogDeleteCountFailsTheQuery()
    {
        // The split claims one deleted row; the vector deletes three.
        assertThatThrownBy(() -> open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, parquet(), DV_PATH, vector(1, 2, 3).orElseThrow())),
                List.of(value()),
                split(1)))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_INVALID.toErrorCode()))
                .hasMessageContaining("the catalog reports 1 deleted rows but the vector deletes 3");
    }

    @Test
    void unsupportedVectorFormatFailsTheQuery()
    {
        assertThatThrownBy(() -> open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, parquet(), DV_PATH, vector(1).orElseThrow())),
                List.of(value()),
                new HoglakeSplit(DATA_PATH, parquet().length, 10, Optional.of(DV_PATH), 1, Optional.of("parquet-position-deletes"))))
                .isInstanceOfSatisfying(TrinoException.class, e ->
                        assertThat(e.getErrorCode()).isEqualTo(HOGLAKE_DELETION_VECTOR_NOT_FOUND.toErrorCode()))
                .hasMessageContaining("Unsupported hoglake deletion vector format 'parquet-position-deletes'");
    }

    @Test
    void unreadableParquetFileStillFailsAfterAVectorIsValidated()
    {
        byte[] notParquet = {1, 2, 3, 4};

        assertThatThrownBy(() -> open(notParquet, List.of(value()), vector(0), Optional.empty(), 4))
                .isInstanceOf(TrinoException.class);
    }

    // ---- resources ----------------------------------------------------------

    @Test
    void closingThePageSourceReleasesTheVectorMemory()
    {
        ConnectorPageSource pageSource = open(parquet(), List.of(value()), vector(0, 1), Optional.empty(), 10);
        drainMaterializingBlocks(pageSource);
        close(pageSource);

        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getMemoryUsage()).isZero();
    }

    @Test
    void vectorMemoryIsReportedWhileThePageSourceIsOpen()
    {
        ConnectorPageSource pageSource = open(parquet(), List.of(value()), vector(0), Optional.empty(), 10);

        assertThat(pageSource.getMemoryUsage()).isPositive();
        close(pageSource);
    }

    /**
     * The input and the decoded vector coexist during loading, then only
     * the decoded vector remains charged to the owner.
     */
    @Test
    void decodingReservesItsBudgetBeforeTheVectorIsReturned()
    {
        long[] positions = new long[500_000];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i * 3L;
        }
        long fileRowCount = positions[positions.length - 1] + 1;
        byte[] deletionVector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, positions);
        long retained = HoglakeDeletionVector.read(deletionVector, DV_PATH).retainedSizeInBytes();

        RecordingMemoryContext engine = new RecordingMemoryContext();
        try (HoglakeSplitResources resources = new HoglakeSplitResources(engine)) {
            HoglakeDeletionVector loaded = HoglakeDeletionVectorLoader.load(
                    ConnectorTestFixtures.memoryFileSystem(Map.of(DV_PATH, deletionVector))
                            .create(ConnectorTestFixtures.session()),
                    new HoglakeSplit(DATA_PATH, parquet().length, fileRowCount, Optional.of(DV_PATH), positions.length, Optional.of("puffin-dv")),
                    fileRowCount,
                    resources);
            assertThat(loaded.retainedSizeInBytes()).isEqualTo(retained);
            assertThat(engine.peakBytes())
                    .describedAs("the construction budget is reserved before decoding")
                    .isGreaterThanOrEqualTo(deletionVector.length + retained);
            // The engine reports in whole megabytes, so the reconciled value
            // is bounded below by the bitmap rather than equal to it.
            assertThat(engine.lastBytes())
                    .describedAs("reconciled down to what the split retains")
                    .isGreaterThanOrEqualTo(retained);
        }

        assertThat(engine.lastBytes())
                .describedAs("released when the owner is closed")
                .isZero();
    }

    /**
     * Everything the split allocates is charged to one total: the deletion
     * vector's bitmap plus the Parquet reader's own buffers. The reader's
     * contribution is what makes the totals differ, so the file is large
     * enough that a materialized page holds real reader memory; comparing the
     * same scan with and without a vector is what tells the two apart.
     */
    @Test
    void readerAndBitmapBothContributeToTheReportedTotal()
    {
        long[] positions = new long[1_000_000];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i * 2L;
        }
        long fileRowCount = positions[positions.length - 1] + 1;
        byte[] wideFile = sequentialParquet(toIntExact(fileRowCount));
        byte[] deletionVector = PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, positions);
        long retained = HoglakeDeletionVector.read(deletionVector, DV_PATH).retainedSizeInBytes();

        RecordingMemoryContext withVector = new RecordingMemoryContext();
        ConnectorPageSource withVectorSource = open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, wideFile, DV_PATH, deletionVector)),
                List.of(value()),
                splitWithCatalogDeleteCount(wideFile.length, deletionVector, fileRowCount),
                Optional.empty(),
                withVector);
        long reportedWithVector;
        try {
            materializeOnePage(withVectorSource);
            reportedWithVector = withVectorSource.getMemoryUsage();
        }
        finally {
            close(withVectorSource);
        }

        RecordingMemoryContext withoutVector = new RecordingMemoryContext();
        ConnectorPageSource withoutVectorSource = open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, wideFile)),
                List.of(value()),
                new HoglakeSplit(DATA_PATH, wideFile.length, fileRowCount, Optional.empty(), 0),
                Optional.<TupleDomain<HoglakeColumnHandle>>empty(),
                withoutVector);
        long reportedWithoutVector;
        try {
            materializeOnePage(withoutVectorSource);
            reportedWithoutVector = withoutVectorSource.getMemoryUsage();
        }
        finally {
            close(withoutVectorSource);
        }

        assertThat(reportedWithoutVector)
                .describedAs("a materialized page holds reader buffers")
                .isPositive();
        assertThat(reportedWithVector)
                .describedAs("bitmap plus reader buffers")
                .isGreaterThanOrEqualTo(retained + reportedWithoutVector);
        assertThat(withVector.lastBytes())
                .describedAs("released when the split closes")
                .isZero();
    }

    /**
     * A load reserves its construction budget before reading anything, then
     * holds exactly the bitmap it decoded; releasing the owner returns the
     * accounting to zero.
     *
     * <p>The reservation is read from the owner rather than sampled from the
     * engine context: those are the same quantity, but the context reports in
     * whole megabytes and does not forward every intermediate update, so
     * sampling it cannot establish what is reserved at each step.
     */
    @Test
    void loadingReservesItsBudgetThenHoldsOnlyTheDecodedBitmap()
    {
        byte[] deletionVector = vector(0, 1).orElseThrow();
        long retained = HoglakeDeletionVector.read(deletionVector, DV_PATH).retainedSizeInBytes();
        long inputLength = deletionVector.length;

        RecordingMemoryContext engine = new RecordingMemoryContext();
        HoglakeSplitResources resources = new HoglakeSplitResources(engine);
        try (resources) {
            HoglakeDeletionVector loaded = HoglakeDeletionVectorLoader.load(
                    ConnectorTestFixtures.memoryFileSystem(Map.of(DV_PATH, deletionVector))
                            .create(ConnectorTestFixtures.session()),
                    new HoglakeSplit(DATA_PATH, parquet().length, 10, Optional.of(DV_PATH), 2, Optional.of("puffin-dv")),
                    10,
                    resources);
            assertThat(loaded.isRowDeleted(0)).isTrue();
            assertThat(resources.allocation().getBytes())
                    .describedAs("the split holds exactly the bitmap it decoded")
                    .isEqualTo(retained);
            assertThat(engine.peakBytes())
                    .describedAs("the construction budget was reserved before decoding")
                    .isGreaterThanOrEqualTo(inputLength + retained);
        }

        assertThat(resources.allocation().getBytes())
                .describedAs("releasing the owner returns the accounting to zero")
                .isZero();
        assertThat(engine.lastBytes())
                .describedAs("the engine is told the split holds nothing")
                .isZero();
    }

    /**
     * The catalog-count path reads and decodes a vector it then discards, so
     * the query must be charged while those bytes are held and must be back to
     * zero once the count is produced.
     */
    @Test
    void countPathChargesTheVectorToTheEngineContextAndKeepsNothing()
    {
        byte[] deletionVector = vector(0).orElseThrow();
        RecordingMemoryContext memory = new RecordingMemoryContext();
        ConnectorPageSource pageSource = open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, parquet(), DV_PATH, deletionVector)),
                List.of(),
                splitWithCatalogDeleteCount(parquet().length, deletionVector, 10),
                Optional.empty(),
                memory);
        try {
            drainMaterializingBlocks(pageSource);
        }
        finally {
            close(pageSource);
        }

        assertThat(memory.peakBytes())
                .describedAs("the vector's bytes are charged while they are read")
                .isGreaterThanOrEqualTo(deletionVector.length);
        assertThat(memory.lastBytes())
                .describedAs("a count keeps no vector memory")
                .isZero();
    }

    /**
     * A count whose vector reserves memory and then fails must release that
     * reservation. The owner is created before the load, so it — not the
     * resource the load would have returned — is what has to be protected.
     */
    @Test
    void aCountWhoseVectorFailsAfterReservingReleasesItsOwner()
    {
        byte[] corrupt = vector(0, 1).orElseThrow();
        // Damage a byte the checksum covers, so the failure happens after the
        // budget is reserved and the bytes are read.
        corrupt[20] = (byte) (corrupt[20] ^ 0x40);
        RecordingMemoryContext memory = new RecordingMemoryContext();

        assertThatThrownBy(() -> open(
                ConnectorTestFixtures.memoryFileSystem(Map.of(DATA_PATH, parquet(), DV_PATH, corrupt)),
                List.of(),
                splitWithCatalogDeleteCount(parquet().length, vector(0, 1).orElseThrow(), 10),
                Optional.empty(),
                memory))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("CRC mismatch");

        assertThat(memory.peakBytes())
                .describedAs("the budget was reserved before the failure")
                .isPositive();
        assertThat(memory.lastBytes())
                .describedAs("a failed count leaves nothing reserved")
                .isZero();
    }

    /**
     * Reads one page and loads its blocks, leaving the page source holding
     * whatever that page needs.
     */
    private static void materializeOnePage(ConnectorPageSource pageSource)
    {
        SourcePage page = pageSource.getNextSourcePage();
        if (page == null) {
            return;
        }
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            page.getBlock(channel);
        }
    }

    /**
     * Stands in for the engine's query memory context: records what the split
     * reports to it.
     */
    private static final class RecordingMemoryContext
            implements MemoryContext
    {
        private long lastBytes = -1;
        private long peakBytes;

        @Override
        public void setBytes(long currentBytes)
        {
            lastBytes = currentBytes;
            peakBytes = Math.max(peakBytes, currentBytes);
        }

        public long lastBytes()
        {
            return lastBytes;
        }

        public long peakBytes()
        {
            return peakBytes;
        }
    }

    /**
     * Reads every page and loads every block, which is what makes a page
     * source hold memory; a page whose blocks are never touched reports
     * nothing.
     */
    private static void drainMaterializingBlocks(ConnectorPageSource pageSource)
    {
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                break;
            }
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                page.getBlock(channel);
            }
        }
    }

    // ---- plumbing -----------------------------------------------------------

    private static byte[] parquet()
    {
        return ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(
                        Types.optional(PrimitiveTypeName.INT64).id(1).named("value"),
                        BIGINT,
                        boxed(VALUES)),
                new FileColumn(
                        Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).id(2).named("label"),
                        VARCHAR,
                        labels())));
    }

    private static byte[] sequentialParquet(int rows)
    {
        List<Object> values = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            values.add((long) i);
        }
        return ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("value"),
                BIGINT,
                values)));
    }

    private static HoglakeColumnHandle value()
    {
        return new HoglakeColumnHandle("value", 1, BIGINT, true);
    }

    private static HoglakeColumnHandle label()
    {
        return new HoglakeColumnHandle("label", 2, VARCHAR, true);
    }

    /**
     * A deletion vector that names this fixture's data file, so the
     * loader's pairing check passes and the test exercises the masking.
     */
    private static Optional<byte[]> vector(long... positions)
    {
        return Optional.of(PuffinDeletionVectorFixtures.deletionVector(DATA_PATH, positions));
    }

    private static TupleDomain<HoglakeColumnHandle> lowerBound(long lowerBound)
    {
        return TupleDomain.withColumnDomains(Map.of(
                value(), Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, lowerBound)), false)));
    }

    private static List<Object> boxed(long[] values)
    {
        List<Object> boxed = new ArrayList<>(values.length);
        for (long value : values) {
            boxed.add(value);
        }
        return boxed;
    }

    private static List<Object> labels()
    {
        List<Object> labels = new ArrayList<>(VALUES.length);
        for (int i = 0; i < VALUES.length; i++) {
            labels.add("v" + i);
        }
        return labels;
    }

    private static List<List<Object>> rows(long... values)
    {
        List<List<Object>> rows = new ArrayList<>(values.length);
        for (long value : values) {
            rows.add(List.of(value));
        }
        return rows;
    }

    private static List<List<Object>> rowsOfLabelsExcept(int deleted)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < VALUES.length; i++) {
            if (i != deleted) {
                rows.add(List.of("v" + i));
            }
        }
        return rows;
    }

    private static List<List<Object>> read(
            byte[] file,
            List<HoglakeColumnHandle> columns,
            Optional<byte[]> deletionVector,
            Optional<TupleDomain<HoglakeColumnHandle>> predicate,
            long recordCount)
    {
        ConnectorPageSource pageSource = open(file, columns, deletionVector, predicate, recordCount);
        try {
            return ConnectorTestFixtures.readAll(
                    pageSource,
                    columns.stream().map(HoglakeColumnHandle::type).toList());
        }
        finally {
            close(pageSource);
        }
    }

    private static ConnectorPageSource open(
            byte[] file,
            List<HoglakeColumnHandle> columns,
            Optional<byte[]> deletionVector,
            Optional<TupleDomain<HoglakeColumnHandle>> predicate,
            long recordCount)
    {
        Map<String, byte[]> files = new HashMap<>();
        files.put(DATA_PATH, file);
        deletionVector.ifPresent(vector -> files.put(DV_PATH, vector));
        HoglakeSplit split = deletionVector.isPresent()
                ? splitWithCatalogDeleteCount(file.length, deletionVector.orElseThrow(), recordCount)
                : new HoglakeSplit(DATA_PATH, file.length, recordCount, Optional.empty(), 0);
        return open(ConnectorTestFixtures.memoryFileSystem(files), columns, split, predicate);
    }

    private static ConnectorPageSource open(
            TrinoFileSystemFactory fileSystem,
            List<HoglakeColumnHandle> columns,
            HoglakeSplit split)
    {
        return open(fileSystem, columns, split, Optional.empty());
    }

    private static ConnectorPageSource open(
            TrinoFileSystemFactory fileSystem,
            List<HoglakeColumnHandle> columns,
            HoglakeSplit split,
            Optional<TupleDomain<HoglakeColumnHandle>> predicate)
    {
        return open(fileSystem, columns, split, predicate, MemoryContext.NO_LIMIT);
    }

    private static ConnectorPageSource open(
            TrinoFileSystemFactory fileSystem,
            List<HoglakeColumnHandle> columns,
            HoglakeSplit split,
            Optional<TupleDomain<HoglakeColumnHandle>> predicate,
            MemoryContext memoryContext)
    {
        return new HoglakePageSourceProvider(fileSystem).createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("analytics", "dv_reads", 1, "uuid-dv-reads", List.of(), predicate.orElse(TupleDomain.all())),
                Optional.empty(),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY,
                memoryContext);
    }

    /**
     * A split whose catalog delete count equals the vector's cardinality, so
     * tests exercising other behaviour do not have to restate it.
     */
    private static HoglakeSplit splitWithCatalogDeleteCount(long fileSizeBytes, byte[] deletionVector, long recordCount)
    {
        long deleteCount = HoglakeDeletionVector.read(deletionVector, DV_PATH).cardinality();
        return new HoglakeSplit(DATA_PATH, fileSizeBytes, recordCount, Optional.of(DV_PATH), deleteCount, Optional.of("puffin-dv"));
    }

    private static HoglakeSplit split(long deleteCount)
    {
        return new HoglakeSplit(DATA_PATH, parquet().length, 10, Optional.of(DV_PATH), deleteCount, Optional.of("puffin-dv"));
    }

    private static void close(ConnectorPageSource pageSource)
    {
        try {
            pageSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
