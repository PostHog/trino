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
import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDuckLakeRowGroupPlanner
{
    private static final long ROW_COUNT = 10_000;
    private static final DuckLakeColumnHandle COLUMN = new DuckLakeColumnHandle(1, "id", "BIGINT", BIGINT, false, Optional.empty());

    @TempDir
    Path directory;

    private ParquetMetadata metadata;
    private long fileSize;

    @BeforeEach
    void createFile()
            throws SQLException, IOException
    {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT range AS id FROM range(%s)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
                    .formatted(ROW_COUNT, directory.resolve("data.parquet")));
            statement.execute("COPY (SELECT * FROM (VALUES (BIGINT '1'), (BIGINT '9001')) t(pos)) TO '%s' (FORMAT PARQUET)"
                    .formatted(directory.resolve("deletes.parquet")));
        }
        fileSize = Files.size(directory.resolve("data.parquet"));
        try (TrinoParquetDataSource source = new TrinoParquetDataSource(
                new LocalFileSystem(directory).newInputFile(Location.of("local:///data.parquet"), fileSize),
                ParquetReaderOptions.defaultOptions(),
                new FileFormatDataSourceStats())) {
            metadata = MetadataReader.readFooter(source, ParquetReaderOptions.defaultOptions(), Optional.empty(), Optional.empty());
        }
    }

    @Test
    void testOversizedRowGroupsAreIndivisible()
            throws IOException
    {
        List<DuckLakeRowGroupPlanner.RowGroupSplit> splits = DuckLakeRowGroupPlanner.plan(metadata, 1);
        assertThat(splits).hasSize(metadata.getBlocks().size());
        assertThat(splits).allSatisfy(split -> assertThat(split.recordCount()).isPositive());
        assertThat(splits).allSatisfy(split -> assertThat(split.metadata().allRowGroups()).isFalse());
        assertThat(splits.stream().mapToLong(DuckLakeRowGroupPlanner.RowGroupSplit::recordCount).sum()).isEqualTo(ROW_COUNT);
        long nextRow = 0;
        for (DuckLakeRowGroupPlanner.RowGroupSplit split : splits) {
            assertThat(split.metadata().firstRowIndex()).isEqualTo(nextRow);
            ParquetMetadata selected = split.metadata().read("local:///data.parquet");
            assertThat(selected.getBlocks()).hasSize(1);
            assertThat(selected.getBlocks().getFirst().fileRowCountOffset()).isEqualTo(nextRow);
            nextRow += split.recordCount();
        }
    }

    @Test
    void testSmallRowGroupsAreBatched()
            throws IOException
    {
        List<DuckLakeRowGroupPlanner.RowGroupSplit> splits = DuckLakeRowGroupPlanner.plan(metadata, Long.MAX_VALUE);
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst().recordCount()).isEqualTo(ROW_COUNT);
        assertThat(splits.getFirst().metadata().allRowGroups()).isTrue();
        assertThat(splits.getFirst().metadata().read("local:///data.parquet").getBlocks()).hasSize(metadata.getBlocks().size());
    }

    @Test
    void testRejectsInconsistentFileRowCount()
    {
        metadata.getParquetMetadata().setNum_rows(ROW_COUNT + 1);
        assertThatThrownBy(() -> DuckLakeRowGroupPlanner.plan(metadata, Long.MAX_VALUE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not match row group row count");
    }

    @Test
    void testRejectsEncryptedMetadata()
    {
        metadata.getParquetMetadata().setFooter_signing_key_metadata(new byte[] {1});
        assertThatThrownBy(() -> DuckLakeRowGroupPlanner.plan(metadata, Long.MAX_VALUE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Encrypted Parquet metadata is not supported");
    }

    @Test
    void testSplitSourcePlansFooterOnceAndBatchesSplits()
            throws Exception
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        List<ConnectorSplit> splits = new ArrayList<>();
        try (DuckLakeSplitSource source = new DuckLakeSplitSource(
                ImmutableList.of(fileSplit()),
                new LocalFileSystem(directory),
                ParquetReaderOptions.defaultOptions(),
                stats,
                1,
                Runnable::run)) {
            splits.addAll(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get());
            long plannedBytes = (long) stats.getReadBytes().getAllTime().getTotal();
            while (!source.isFinished()) {
                splits.addAll(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get());
            }
            assertThat((long) stats.getReadBytes().getAllTime().getTotal()).isEqualTo(plannedBytes);
        }
        assertThat(splits).hasSize(metadata.getBlocks().size());
        assertThat(splits).allSatisfy(split -> assertThat(((DuckLakeSplit) split).rowGroupMetadata()).isPresent());
    }

    @Test
    void testWorkersReadEveryRowOnceWithoutReadingTheFooter()
            throws IOException
    {
        List<DuckLakeRowGroupPlanner.RowGroupSplit> splits = DuckLakeRowGroupPlanner.plan(metadata, 1);
        // Any worker that tries to rediscover the footer will now fail. Column data is unchanged.
        corruptFooter();
        List<Long> values = new ArrayList<>();
        long bytes = 0;
        for (DuckLakeRowGroupPlanner.RowGroupSplit group : splits) {
            try (ConnectorPageSource source = source(group, Optional.empty())) {
                readValues(source, values);
                bytes += source.getCompletedBytes();
            }
        }
        assertThat(values).containsExactlyElementsOf(LongStream.range(0, ROW_COUNT).boxed().toList());
        assertThat(bytes).isEqualTo(splits.stream().mapToLong(DuckLakeRowGroupPlanner.RowGroupSplit::compressedBytes).sum());
    }

    @Test
    void testDeletesUseAbsoluteFilePositionsInLaterSplits()
            throws IOException
    {
        List<DuckLakeRowGroupPlanner.RowGroupSplit> splits = DuckLakeRowGroupPlanner.plan(metadata, 1);
        corruptFooter();
        DuckLakeDeleteFileHandle deletes = new DuckLakeDeleteFileHandle(
                "local:///deletes.parquet",
                Files.size(directory.resolve("deletes.parquet")),
                OptionalLong.empty(),
                2);
        List<Long> values = new ArrayList<>();
        for (DuckLakeRowGroupPlanner.RowGroupSplit group : splits) {
            try (ConnectorPageSource source = source(group, Optional.of(deletes))) {
                readValues(source, values);
            }
        }
        assertThat(values).containsExactlyElementsOf(LongStream.range(0, ROW_COUNT).filter(value -> value != 1 && value != 9001).boxed().toList());
    }

    private void corruptFooter()
            throws IOException
    {
        try (SeekableByteChannel channel = Files.newByteChannel(directory.resolve("data.parquet"), WRITE)) {
            channel.position(fileSize - 8);
            channel.write(ByteBuffer.wrap(new byte[8]));
        }
    }

    private ConnectorPageSource source(DuckLakeRowGroupPlanner.RowGroupSplit group, Optional<DuckLakeDeleteFileHandle> deletes)
    {
        ParquetReaderConfig config = new ParquetReaderConfig();
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new DuckLakeSessionProperties(new DuckLakeConfig(), config, new ParquetWriterConfig()).getSessionProperties())
                .build();
        DuckLakePageSourceProvider provider = new DuckLakePageSourceProvider(_ -> new LocalFileSystem(directory), new FileFormatDataSourceStats(), config);
        DuckLakeSplit split = new DuckLakeSplit(
                1,
                "local:///data.parquet",
                0,
                fileSize,
                fileSize,
                OptionalLong.empty(),
                group.recordCount(),
                OptionalLong.empty(),
                deletes,
                ImmutableMap.of(),
                Optional.empty(),
                SplitWeight.standard(),
                Optional.of(group.metadata()));
        JsonCodec<DuckLakeSplit> codec = JsonCodec.jsonCodec(DuckLakeSplit.class);
        DuckLakeSplit roundTrip = codec.fromJson(codec.toJson(split));
        assertThat(roundTrip).isEqualTo(split);
        split = roundTrip;
        DuckLakeTableHandle table = new DuckLakeTableHandle("main", "test", 1, 1, "local:///", TupleDomain.all(), TupleDomain.all(), OptionalLong.empty());
        return provider.createPageSource(new HiveTransactionHandle(true), session, split, table, Optional.empty(), ImmutableList.of(COLUMN), EMPTY, _ -> {});
    }

    private DuckLakeSplit fileSplit()
    {
        return new DuckLakeSplit(
                1,
                "local:///data.parquet",
                0,
                fileSize,
                fileSize,
                OptionalLong.empty(),
                ROW_COUNT,
                OptionalLong.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty(),
                SplitWeight.standard());
    }

    private static void readValues(ConnectorPageSource source, List<Long> values)
    {
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page != null) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    values.add(BIGINT.getLong(page.getBlock(0), position));
                }
            }
        }
    }
}
