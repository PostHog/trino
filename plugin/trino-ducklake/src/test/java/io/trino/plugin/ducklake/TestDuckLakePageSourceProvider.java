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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
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

    private ConnectorPageSource source(Optional<DuckLakeDeleteFileHandle> deletes)
            throws IOException
    {
        return source(deletes, 0, fileSize("data.parquet"), ImmutableList.of(COLUMN));
    }

    private ConnectorPageSource source(Optional<DuckLakeDeleteFileHandle> deletes, long start, long length, List<DuckLakeColumnHandle> columns)
            throws IOException
    {
        ParquetReaderConfig readerConfig = new ParquetReaderConfig().setMaxReadBlockRowCount(128);
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new DuckLakeSessionProperties(new DuckLakeConfig(), readerConfig, new ParquetWriterConfig()).getSessionProperties())
                .build();
        DuckLakePageSourceProvider provider = new DuckLakePageSourceProvider(_ -> new LocalFileSystem(directory), readStatistics, readerConfig);
        DuckLakeSplit split = new DuckLakeSplit(
                1,
                "local:///data.parquet",
                start,
                length,
                fileSize("data.parquet"),
                OptionalLong.empty(),
                ROW_COUNT - deletes.map(DuckLakeDeleteFileHandle::deleteCount).orElse(0L),
                OptionalLong.empty(),
                deletes,
                ImmutableMap.of(),
                Optional.empty(),
                SplitWeight.standard());
        DuckLakeTableHandle table = new DuckLakeTableHandle("main", "test", 1, 1, "local:///", TupleDomain.all(), TupleDomain.all(), OptionalLong.empty());
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
