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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.IndexReference;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// ExampleParquetWriter is not thread-safe
@Execution(SAME_THREAD)
class TestRedundantColumnIndexes
{
    private static final int ROW_COUNT = 4096;
    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message test {
                optional binary value (STRING);
                required int64 position;
                optional group nested (LIST) {
                    repeated group list {
                        optional int64 element;
                    }
                }
            }
            """);
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final List<String> COLUMN_NAMES = ImmutableList.of("value", "position", "nested");
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR, BIGINT, ARRAY_TYPE);
    private static final Consumer<FileMetaData> KEEP_STATISTICS = _ -> {};

    @Test
    void testNonSelectivePredicates()
            throws IOException
    {
        assertIndexReads(0, Domain.notNull(VARCHAR), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of(), false);
        assertIndexReads(0, range("0000", "4095"), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of(), false);
        assertIndexReads(0, range("0000", "9999"), Domain.notNull(BIGINT), KEEP_STATISTICS, ImmutableList.of(), false);
        assertIndexReads(ROW_COUNT, Domain.onlyNull(VARCHAR), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of(), false);
    }

    @Test
    void testSelectivePredicates()
            throws IOException
    {
        assertIndexReads(0, range("1024", "2047"), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of("value"), true);
        // The position filter still needs every projected column's offset index, but only its own column index.
        assertIndexReads(0, Domain.notNull(VARCHAR), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1024L, true, 2047L, true)), false), KEEP_STATISTICS, ImmutableList.of("position"), true);
        assertIndexReads(0, range("1024", "3071"), Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 2048L, true, 4095L, true)), false), KEEP_STATISTICS, ImmutableList.of("value", "position"), true);
    }

    @Test
    void testNullContainingColumn()
            throws IOException
    {
        assertIndexReads(1024, Domain.notNull(VARCHAR), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of("value"), true);
        // The page predicate conservatively allows nulls in every non-null page, so IS NULL does not prune them.
        assertIndexReads(1024, Domain.onlyNull(VARCHAR), Domain.all(BIGINT), KEEP_STATISTICS, ImmutableList.of("value"), false);
    }

    @Test
    void testMissingStatistics()
            throws IOException
    {
        assertIndexReads(1024, Domain.notNull(VARCHAR), Domain.all(BIGINT), metadata -> metadata.getRow_groups().forEach(rowGroup -> rowGroup.getColumns().forEach(column -> column.getMeta_data().unsetStatistics())), ImmutableList.of("value"), true);
    }

    @Test
    void testUnknownNullCount()
            throws IOException
    {
        Consumer<FileMetaData> removeNullCounts = metadata -> metadata.getRow_groups().forEach(rowGroup -> rowGroup.getColumns().forEach(column -> column.getMeta_data().getStatistics().unsetNull_count()));
        assertIndexReads(0, Domain.notNull(VARCHAR), Domain.all(BIGINT), removeNullCounts, ImmutableList.of("value"), false);
        assertIndexReads(1024, Domain.notNull(VARCHAR), Domain.all(BIGINT), removeNullCounts, ImmutableList.of("value"), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.7.0", "1.8.0"})
    void testUntrustedBounds(String writerVersion)
            throws IOException
    {
        assertIndexReads(0, range("1024", "2047"), Domain.all(BIGINT), metadata -> {
            metadata.setCreated_by("parquet-mr version " + writerVersion);
            metadata.getRow_groups().forEach(rowGroup -> {
                Statistics statistics = rowGroup.getColumns().getFirst().getMeta_data().getStatistics();
                // Buggy-writer bounds are ignored; later legacy UTF8 bounds are widened.
                // Neither case should disable useful page pruning based on these misleading bounds.
                statistics.setMin(utf8Slice("1024").getBytes());
                statistics.setMax(utf8Slice("2047").getBytes());
                statistics.unsetMin_value();
                statistics.unsetMax_value();
                org.apache.parquet.column.statistics.Statistics<?> parsedStatistics = MetadataReader.readStats(
                        Optional.of(metadata.getCreated_by()), Optional.of(statistics), SCHEMA.getType("value").asPrimitiveType());
                if (writerVersion.equals("1.7.0")) {
                    assertThat(parsedStatistics.hasNonNullValue()).isFalse();
                }
                else {
                    assertThat(parsedStatistics.genericGetMax()).isEqualTo(org.apache.parquet.io.api.Binary.fromString("3"));
                }
            });
        }, ImmutableList.of("value"), true);
    }

    @Test
    void testNullCountWithoutBounds()
            throws IOException
    {
        assertIndexReads(0, Domain.notNull(VARCHAR), Domain.all(BIGINT), metadata -> metadata.getRow_groups().forEach(rowGroup -> rowGroup.getColumns().forEach(column -> {
            var statistics = column.getMeta_data().getStatistics();
            statistics.unsetMin();
            statistics.unsetMax();
            statistics.unsetMin_value();
            statistics.unsetMax_value();
        })), ImmutableList.of(), false);
    }

    @Test
    void testRowGroupScopedDecision()
            throws IOException
    {
        // Only the first row group contains nulls and can benefit from page pruning.
        assertIndexReads(
                1024,
                Domain.notNull(VARCHAR),
                Domain.all(BIGINT),
                KEEP_STATISTICS,
                2048,
                ImmutableList.of(ImmutableList.of("value"), ImmutableList.of()),
                true);
        // Both groups filter on position, but only the first still needs the value predicate.
        assertIndexReads(
                1024,
                Domain.notNull(VARCHAR),
                Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 512L, true, 3583L, true)), false),
                KEEP_STATISTICS,
                2048,
                ImmutableList.of(ImmutableList.of("value", "position"), ImmutableList.of("position")),
                true);
    }

    @Test
    void testEmptyRowGroupMetadataValidation()
            throws IOException
    {
        ParquetReaderOptions options = ParquetReaderOptions.builder().build();
        var dataSource = new RecordingDataSource(writeFile(0, ROW_COUNT), options);
        FileMetaData thriftMetadata = MetadataReader.readFooter(dataSource, Optional.empty()).getParquetMetadata().deepCopy();
        thriftMetadata.getRow_groups().getFirst().setNum_rows(0);
        ParquetMetadata validEmptyMetadata = new ParquetMetadata(thriftMetadata.deepCopy(), dataSource.getId(), Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, validEmptyMetadata, options, newSimpleAggregatedMemoryContext(), TYPES, COLUMN_NAMES, TupleDomain.all())) {
            assertThat(reader.nextPage()).isNull();
        }

        thriftMetadata.getRow_groups().getFirst().getColumns().remove(1);
        ParquetMetadata invalidEmptyMetadata = new ParquetMetadata(thriftMetadata, dataSource.getId(), Optional.empty());
        assertThatThrownBy(() -> {
            try (ParquetReader reader = createParquetReader(dataSource, invalidEmptyMetadata, options, newSimpleAggregatedMemoryContext(), TYPES, COLUMN_NAMES, TupleDomain.all())) {
                reader.nextPage();
            }
        })
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("Metadata is missing for column")
                .hasMessageContaining("position");
    }

    private static Domain range(String low, String high)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice(low), true, utf8Slice(high), true)), false);
    }

    private static void assertIndexReads(
            int nullCount,
            Domain valueDomain,
            Domain positionDomain,
            Consumer<FileMetaData> modifyMetadata,
            List<String> expectedColumnIndexes,
            boolean expectPruning)
            throws IOException
    {
        assertIndexReads(nullCount, valueDomain, positionDomain, modifyMetadata, ROW_COUNT, ImmutableList.of(expectedColumnIndexes), expectPruning);
    }

    private static void assertIndexReads(
            int nullCount,
            Domain valueDomain,
            Domain positionDomain,
            Consumer<FileMetaData> modifyMetadata,
            int rowGroupSize,
            List<List<String>> expectedColumnIndexes,
            boolean expectPruning)
            throws IOException
    {
        ParquetReaderOptions options = ParquetReaderOptions.builder()
                .withMaxMergeDistance(DataSize.ofBytes(0))
                .build();
        Slice file = writeFile(nullCount, rowGroupSize);
        var dataSource = new RecordingDataSource(file, options);
        ParquetMetadata originalMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        FileMetaData thriftMetadata = originalMetadata.getParquetMetadata().deepCopy();
        modifyMetadata.accept(thriftMetadata);
        ParquetMetadata metadata = new ParquetMetadata(thriftMetadata, dataSource.getId(), Optional.empty());
        assertThat(metadata.getBlocks()).hasSize(expectedColumnIndexes.size());
        dataSource.reads.clear();

        List<Long> matchingPositions = new ArrayList<>();
        int rowsRead = 0;
        ByteArrayOutputStream indexLogs = new ByteArrayOutputStream();
        StreamHandler logHandler = new StreamHandler(indexLogs, new SimpleFormatter());
        logHandler.setEncoding(UTF_8.name());
        long testThreadId = Thread.currentThread().threadId();
        logHandler.setFilter(record -> record.getLongThreadID() == testThreadId);
        Logger indexLogger = Logger.getLogger(ColumnIndexFilter.class.getName());
        Level previousLevel = indexLogger.getLevel();
        indexLogger.setLevel(Level.INFO);
        indexLogger.addHandler(logHandler);
        try (ParquetReader reader = createParquetReader(
                dataSource,
                metadata,
                options,
                newSimpleAggregatedMemoryContext(),
                TYPES,
                COLUMN_NAMES,
                TupleDomain.withColumnDomains(ImmutableMap.of("value", valueDomain, "position", positionDomain)))) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                rowsRead += page.getPositionCount();
                Block valueBlock = page.getBlock(0);
                Block positionBlock = page.getBlock(1);
                Block nestedBlock = page.getBlock(2);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    long row = BIGINT.getLong(positionBlock, position);
                    assertThat(ARRAY_TYPE.getObjectValue(nestedBlock, position)).isEqualTo(row % 3 == 0 ? ImmutableList.of() : Arrays.asList(row, null));
                    Slice actualValue = valueBlock.isNull(position) ? null : VARCHAR.getSlice(valueBlock, position);
                    assertThat(actualValue).isEqualTo(row < nullCount ? null : value(row));
                    if (valueDomain.includesNullableValue(actualValue) && positionDomain.includesNullableValue(row)) {
                        matchingPositions.add(row);
                    }
                }
            }
        }
        finally {
            indexLogger.removeHandler(logHandler);
            indexLogger.setLevel(previousLevel);
            logHandler.close();
        }
        assertThat(indexLogs.toString(UTF_8)).doesNotContain("No column index for column");
        assertThat(matchingPositions).containsExactlyElementsOf(IntStream.range(0, ROW_COUNT)
                .filter(row -> valueDomain.includesNullableValue(row < nullCount ? null : value(row)) && positionDomain.includesNullableValue((long) row))
                .mapToObj(row -> (long) row)
                .toList());
        if (expectPruning) {
            assertThat(rowsRead).isBetween(1, ROW_COUNT - 1);
        }
        else {
            assertThat(rowsRead).isEqualTo(ROW_COUNT);
        }
        for (int rowGroup = 0; rowGroup < metadata.getBlocks().size(); rowGroup++) {
            for (var column : metadata.getBlocks().get(rowGroup).columns()) {
                assertThat(column.getColumnIndexReference()).isNotNull();
                assertThat(column.getOffsetIndexReference()).isNotNull();
                assertThat(dataSource.wasRead(column.getColumnIndexReference()))
                        .as("column index for %s in row group %s", column.getPath(), rowGroup)
                        .isEqualTo(expectedColumnIndexes.get(rowGroup).contains(column.getPath().toDotString()));
                assertThat(dataSource.wasRead(column.getOffsetIndexReference()))
                        .as("offset index for %s in row group %s", column.getPath(), rowGroup)
                        .isEqualTo(!expectedColumnIndexes.get(rowGroup).isEmpty());
            }
        }
    }

    private static Slice writeFile(int nullCount, int rowGroupSize)
            throws IOException
    {
        java.nio.file.Path path = Files.createTempFile("test-column-indexes", ".parquet");
        try {
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toUri()))
                    .withConf(new Configuration(false))
                    .withType(SCHEMA)
                    .withWriteMode(OVERWRITE)
                    .withDictionaryEncoding(false)
                    .withRowGroupRowCountLimit(rowGroupSize)
                    .withPageRowCountLimit(128)
                    .withMinRowCountForPageSizeCheck(128)
                    .withMaxRowCountForPageSizeCheck(128)
                    .build()) {
                SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
                for (int position = 0; position < ROW_COUNT; position++) {
                    Group group = factory.newGroup().append("position", (long) position);
                    if (position >= nullCount) {
                        group.append("value", value(position).toStringUtf8());
                    }
                    Group nested = group.addGroup("nested");
                    if (position % 3 != 0) {
                        nested.addGroup("list").append("element", (long) position);
                        nested.addGroup("list");
                    }
                    writer.write(group);
                }
            }
            return wrappedBuffer(Files.readAllBytes(path));
        }
        finally {
            Files.deleteIfExists(path);
        }
    }

    private static Slice value(long position)
    {
        return utf8Slice("%04d".formatted(position));
    }

    private static class RecordingDataSource
            extends TestingParquetDataSource
    {
        private final List<DiskRange> reads = new ArrayList<>();

        public RecordingDataSource(Slice input, ParquetReaderOptions options)
                throws IOException
        {
            super(input, options);
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            reads.add(new DiskRange(position, bufferLength));
            super.readInternal(position, buffer, bufferOffset, bufferLength);
        }

        public boolean wasRead(IndexReference index)
        {
            return reads.stream().anyMatch(read -> read.offset() < index.getOffset() + index.getLength() && index.getOffset() < read.end());
        }
    }
}
