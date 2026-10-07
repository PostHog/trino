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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakePageSourceProvider
{
    private static final long ROW_COUNT = 10_000;
    private static final DuckLakeColumnHandle COLUMN = new DuckLakeColumnHandle(1, "id", "BIGINT", BIGINT, false, Optional.empty());

    private final TestingReadStatistics readStatistics = new TestingReadStatistics();

    @TempDir
    Path directory;

    @BeforeEach
    void createFiles()
            throws SQLException
    {
        // These fixtures need neither a catalog nor downloaded DuckDB extensions. All files fit
        // in the Parquet reader's small-file buffer, so their lengths are the exact bytes read.
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT range AS id FROM range(%s)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
                    .formatted(ROW_COUNT, directory.resolve("data.parquet")));
            statement.execute("COPY (SELECT * FROM (VALUES (BIGINT '1'), (BIGINT '3')) t(pos)) TO '%s' (FORMAT PARQUET)"
                    .formatted(directory.resolve("deletes.parquet")));
            statement.execute("COPY (SELECT NULL::BIGINT AS pos) TO '%s' (FORMAT PARQUET)"
                    .formatted(directory.resolve("invalid-deletes.parquet")));
            // a file holding the rows of four snapshots, like one DuckDB writes when it merges
            // adjacent files: the rows of each snapshot in turn, each tagged with its snapshot
            statement.execute("COPY (SELECT range AS id, 1 + range // %s AS _ducklake_internal_snapshot_id FROM range(%s)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
                    .formatted(ROW_COUNT / 4, ROW_COUNT, directory.resolve("partial.parquet")));
            // deletions of a row of the second snapshot and of a row of the fourth
            statement.execute("COPY (SELECT * FROM (VALUES (BIGINT '3000'), (BIGINT '9000')) t(pos)) TO '%s' (FORMAT PARQUET)"
                    .formatted(directory.resolve("partial-deletes.parquet")));
        }
    }

    @Test
    void testScanIncludesDeleteFileBytes()
            throws IOException
    {
        try (ConnectorPageSource source = source(Optional.of(deleteFile("deletes.parquet", 2)))) {
            assertThat(drain(source)).isEqualTo(ROW_COUNT - 2);
            assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet") + fileSize("deletes.parquet"));
            assertThat(source.getReadTimeNanos()).isPositive();
            assertThat(source.getReadTimeNanos()).isEqualTo(readStatistics.readTimeNanos);
        }
    }

    @Test
    void testScanWithoutDeletes()
            throws IOException
    {
        try (ConnectorPageSource source = source(Optional.empty())) {
            assertThat(drain(source)).isEqualTo(ROW_COUNT);
            assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet"));
        }
    }

    @Test
    void testStatisticsDoNotLoadDeletes()
            throws IOException
    {
        DuckLakeDeleteFileHandle deletes = deleteFile("deletes.parquet", 2);
        Files.delete(directory.resolve("deletes.parquet"));
        ConnectorPageSource source = source(Optional.of(deletes));
        try (source) {
            assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet"));
            assertThat(source.getReadTimeNanos()).isPositive();
        }
        assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet"));
    }

    @Test
    void testEarlyClosePreservesDeleteStatistics()
            throws IOException
    {
        ConnectorPageSource source = source(Optional.of(deleteFile("deletes.parquet", 2)));
        long bytes;
        long readTime;
        try (source) {
            assertThat(source.getNextSourcePage()).isNotNull();
            assertThat(source.isFinished()).isFalse();
            bytes = source.getCompletedBytes();
            readTime = source.getReadTimeNanos();
            assertThat(bytes).isEqualTo(fileSize("data.parquet") + fileSize("deletes.parquet"));
        }
        assertThat(source.getCompletedBytes()).isEqualTo(bytes);
        assertThat(source.getReadTimeNanos()).isEqualTo(readTime);
    }

    @Test
    void testInvalidDeletePositionsPreserveReadStatistics()
            throws IOException
    {
        ConnectorPageSource source = source(Optional.of(deleteFile("invalid-deletes.parquet", 1)));
        try (source) {
            assertThatThrownBy(source::getNextSourcePage).hasMessageContaining("contains a null position");
            assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet") + fileSize("invalid-deletes.parquet"));
        }
        assertThat(source.getCompletedBytes()).isEqualTo(fileSize("data.parquet") + fileSize("invalid-deletes.parquet"));
    }

    @Test
    void testEachSplitCountsItsDeleteReads()
            throws IOException
    {
        long size = fileSize("data.parquet");
        long rows = 0;
        long bytes = 0;
        for (long start : ImmutableList.of(0L, size / 2)) {
            long length = size / 2;
            if (start > 0) {
                length = size - start;
            }
            try (ConnectorPageSource source = source(Optional.of(deleteFile("deletes.parquet", 2)), start, length, ImmutableList.of(COLUMN))) {
                long splitRows = drain(source);
                assertThat(splitRows).isPositive();
                rows += splitRows;
                bytes += source.getCompletedBytes();
            }
        }
        assertThat(rows).isEqualTo(ROW_COUNT - 2);
        assertThat(bytes).isEqualTo(2 * (size + fileSize("deletes.parquet")));
    }

    @Test
    void testMetadataOnlyCountReadsNoFiles()
            throws IOException
    {
        try (ConnectorPageSource source = source(Optional.of(deleteFile("deletes.parquet", 2)), 0, fileSize("data.parquet"), ImmutableList.of())) {
            Files.delete(directory.resolve("data.parquet"));
            Files.delete(directory.resolve("deletes.parquet"));
            assertThat(drain(source)).isEqualTo(ROW_COUNT - 2);
            assertThat(source.getCompletedBytes()).isZero();
            assertThat(source.getReadTimeNanos()).isZero();
        }
    }

    /**
     * Only the rows of the snapshot read and older ones are read from a file also holding rows of
     * newer snapshots, and the snapshot column they are told apart by is not returned.
     */
    @Test
    void testRowsOfNewerSnapshotsAreLeftOut()
            throws IOException
    {
        try (ConnectorPageSource source = partialSource(Optional.empty(), ImmutableList.of(COLUMN), TupleDomain.all())) {
            List<Long> ids = readLongs(source, 0);
            assertThat(ids).hasSize(toIntExact(ROW_COUNT / 2));
            assertThat(ids).allMatch(id -> id < ROW_COUNT / 2);
        }
        // a count reads the snapshots of the rows rather than trusting the record count
        try (ConnectorPageSource source = partialSource(Optional.empty(), ImmutableList.of(), TupleDomain.all())) {
            assertThat(drain(source)).isEqualTo(ROW_COUNT / 2);
        }
        // the filter applies alongside a predicate pushed into the reader
        try (ConnectorPageSource source = partialSource(Optional.empty(), ImmutableList.of(COLUMN), TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 4000L)), false))))) {
            List<Long> ids = readLongs(source, 0);
            assertThat(ids).isNotEmpty();
            assertThat(ids).allMatch(id -> id < ROW_COUNT / 2);
            assertThat(ids).contains(4999L);
        }
    }

    /**
     * Deletions and row identifiers refer to the position of a row in the file, which leaving out
     * the rows of newer snapshots does not change.
     */
    @Test
    void testRowsOfNewerSnapshotsWithDeletesAndRowIds()
            throws IOException
    {
        DuckLakeDeleteFileHandle deletes = deleteFile("partial-deletes.parquet", 2);
        try (ConnectorPageSource source = partialSource(Optional.of(deletes), ImmutableList.of(COLUMN, DuckLakeMergeRowId.columnHandle()), TupleDomain.all())) {
            long rows = 0;
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                assertThat(page.getChannelCount()).isEqualTo(2);
                Block ids = page.getBlock(0);
                Block positions = ((RowBlock) page.getBlock(1)).getFieldBlock(DuckLakeMergeRowId.FILE_ROW_POSITION_CHANNEL);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    long id = BIGINT.getLong(ids, position);
                    // the id of each row is its position in the file
                    assertThat(BIGINT.getLong(positions, position)).isEqualTo(id);
                    assertThat(id).isLessThan(ROW_COUNT / 2).isNotEqualTo(3000L);
                }
                rows += page.getPositionCount();
            }
            assertThat(rows).isEqualTo(ROW_COUNT / 2 - 1);
        }
        try (ConnectorPageSource source = partialSource(Optional.of(deletes), ImmutableList.of(), TupleDomain.all())) {
            assertThat(drain(source)).isEqualTo(ROW_COUNT / 2 - 1);
        }
    }

    @Test
    void testRowsOfNewerSnapshotsWithoutSnapshotColumn()
            throws IOException
    {
        // read by name, the missing column would read as NULL and leave out every row
        assertThatThrownBy(() -> source(Optional.empty(), "data.parquet", 0, fileSize("data.parquet"), ImmutableList.of(COLUMN), OptionalLong.of(2), TupleDomain.all()))
                .hasMessage("Data file local:///data.parquet holds rows newer than snapshot 2, but has no _ducklake_internal_snapshot_id column to tell them apart");
    }

    private ConnectorPageSource partialSource(Optional<DuckLakeDeleteFileHandle> deletes, List<DuckLakeColumnHandle> columns, TupleDomain<DuckLakeColumnHandle> predicate)
            throws IOException
    {
        return source(deletes, "partial.parquet", 0, fileSize("partial.parquet"), columns, OptionalLong.of(2), predicate);
    }

    private static List<Long> readLongs(ConnectorPageSource source, int channel)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            Block block = page.getBlock(channel);
            for (int position = 0; position < page.getPositionCount(); position++) {
                values.add(BIGINT.getLong(block, position));
            }
        }
        return values.build();
    }

    private ConnectorPageSource source(Optional<DuckLakeDeleteFileHandle> deletes)
            throws IOException
    {
        return source(deletes, 0, fileSize("data.parquet"), ImmutableList.of(COLUMN));
    }

    private ConnectorPageSource source(Optional<DuckLakeDeleteFileHandle> deletes, long start, long length, List<DuckLakeColumnHandle> columns)
            throws IOException
    {
        return source(deletes, "data.parquet", start, length, columns, OptionalLong.empty(), TupleDomain.all());
    }

    private ConnectorPageSource source(
            Optional<DuckLakeDeleteFileHandle> deletes,
            String file,
            long start,
            long length,
            List<DuckLakeColumnHandle> columns,
            OptionalLong rowSnapshotFilter,
            TupleDomain<DuckLakeColumnHandle> predicate)
            throws IOException
    {
        ParquetReaderConfig readerConfig = new ParquetReaderConfig().setMaxReadBlockRowCount(128);
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new DuckLakeSessionProperties(new DuckLakeConfig(), readerConfig, new ParquetWriterConfig()).getSessionProperties())
                .build();
        DuckLakePageSourceProvider provider = new DuckLakePageSourceProvider(_ -> new LocalFileSystem(directory), readStatistics, readerConfig);
        DuckLakeSplit split = new DuckLakeSplit(
                1,
                "local:///" + file,
                start,
                length,
                fileSize(file),
                OptionalLong.empty(),
                ROW_COUNT - deletes.map(DuckLakeDeleteFileHandle::deleteCount).orElse(0L),
                OptionalLong.empty(),
                deletes,
                ImmutableMap.of(),
                Optional.empty(),
                SplitWeight.standard(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                rowSnapshotFilter);
        DuckLakeTableHandle table = new DuckLakeTableHandle("main", "test", 1, rowSnapshotFilter.orElse(1), "local:///", TupleDomain.all(), predicate, OptionalLong.empty());
        return provider.createPageSource(new HiveTransactionHandle(true), session, split, table, Optional.empty(), ImmutableList.copyOf(columns), EMPTY, _ -> {});
    }

    private DuckLakeDeleteFileHandle deleteFile(String name, long count)
            throws IOException
    {
        return new DuckLakeDeleteFileHandle("local:///" + name, fileSize(name), OptionalLong.empty(), count);
    }

    private long fileSize(String name)
            throws IOException
    {
        return Files.size(directory.resolve(name));
    }

    private static long drain(ConnectorPageSource source)
    {
        long rows = 0;
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page != null) {
                rows += page.getPage().getPositionCount();
            }
        }
        return rows;
    }

    private static final class TestingReadStatistics
            extends FileFormatDataSourceStats
    {
        private long readTimeNanos;

        @Override
        public void readDataBytesPerSecond(long bytes, long nanos)
        {
            super.readDataBytesPerSecond(bytes, nanos);
            readTimeNanos += nanos;
        }
    }
}
