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
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStatsRow;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.plugin.ducklake.util.StatsValueFormatter;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_WRITER_ERROR;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;

/**
 * Writes one DuckLake data file and collects the statistics the catalog records for it.
 * <p>
 * The statistics come from the Parquet footer, which already carries per-column value counts,
 * null counts, sizes and bounds. The one thing the footer cannot answer is whether a floating
 * point column holds {@code NaN} — Parquet leaves such values out of its bounds instead of
 * flagging them — so that is tracked while the rows are written.
 */
public class DuckLakeFileWriter
{
    private final ParquetFileWriter writer;
    private final DuckLakeParquetSchema schema;
    private final String relativePath;
    private final List<Optional<String>> partitionValues;
    private final Map<Integer, Integer> floatingPointChannels;
    private final Map<Long, Integer> floatingPointColumnIds;
    private final boolean[] containsNan;

    private long recordCount;
    private boolean closed;

    public DuckLakeFileWriter(
            ParquetFileWriter writer,
            DuckLakeParquetSchema schema,
            String relativePath,
            List<Optional<String>> partitionValues)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.relativePath = requireNonNull(relativePath, "relativePath is null");
        this.partitionValues = ImmutableList.copyOf(partitionValues);

        // only top-level floating point columns are tracked; a NaN nested inside a list or a
        // struct leaves the statistic unset, which readers treat as unknown
        Map<Integer, Integer> channels = new LinkedHashMap<>();
        Map<Long, Integer> columnIds = new LinkedHashMap<>();
        List<Type> columnTypes = schema.fileColumnTypes();
        for (int channel = 0; channel < columnTypes.size(); channel++) {
            Type type = columnTypes.get(channel);
            if (!REAL.equals(type) && !DOUBLE.equals(type)) {
                continue;
            }
            Optional<DuckLakeParquetSchema.LeafField> field = schema.leafField(ImmutableList.of(schema.fileColumnNames().get(channel)));
            if (field.isEmpty()) {
                continue;
            }
            int index = channels.size();
            channels.put(channel, index);
            columnIds.put(field.get().columnId(), index);
        }
        this.floatingPointChannels = channels;
        this.floatingPointColumnIds = columnIds;
        this.containsNan = new boolean[channels.size()];
    }

    public void appendRows(Page page)
    {
        recordNanValues(page);
        writer.appendRows(page);
        recordCount += page.getPositionCount();
    }

    public long recordCount()
    {
        return recordCount;
    }

    public long writtenBytes()
    {
        return writer.getWrittenBytes();
    }

    public long memoryUsage()
    {
        return writer.getMemoryUsage();
    }

    public DuckLakeDataFile close()
    {
        writer.commit();
        closed = true;
        return new DuckLakeDataFile(
                relativePath,
                recordCount,
                writer.getWrittenBytes(),
                writer.getFooterSize(),
                partitionValues,
                columnStatistics());
    }

    public void rollback()
    {
        if (!closed) {
            writer.rollback();
        }
    }

    private void recordNanValues(Page page)
    {
        for (Map.Entry<Integer, Integer> entry : floatingPointChannels.entrySet()) {
            int index = entry.getValue();
            if (containsNan[index]) {
                continue;
            }
            Block block = page.getBlock(entry.getKey());
            Type type = schema.fileColumnTypes().get(entry.getKey());
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    continue;
                }
                double value;
                if (REAL.equals(type)) {
                    value = Float.intBitsToFloat(REAL.getInt(block, position));
                }
                else {
                    value = DOUBLE.getDouble(block, position);
                }
                if (Double.isNaN(value)) {
                    containsNan[index] = true;
                    break;
                }
            }
        }
    }

    /**
     * Folds the per-row-group column chunk metadata of the footer into one statistic per column.
     */
    private List<DuckLakeFileColumnStatsRow> columnStatistics()
    {
        ParquetMetadata metadata;
        try {
            metadata = new ParquetMetadata(writer.getFileMetadata(), new ParquetDataSourceId(relativePath), Optional.empty());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(DUCKLAKE_WRITER_ERROR, "Failed to read back the metadata of " + relativePath, e);
        }

        Map<Long, ColumnAccumulator> accumulators = new LinkedHashMap<>();
        Map<Long, DuckLakeParquetSchema.LeafField> fields = new HashMap<>();
        List<BlockMetadata> blocks;
        try {
            blocks = metadata.getBlocks();
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(DUCKLAKE_WRITER_ERROR, "Failed to read back the metadata of " + relativePath, e);
        }
        for (BlockMetadata block : blocks) {
            for (ColumnChunkMetadata chunk : block.columns()) {
                Optional<DuckLakeParquetSchema.LeafField> field = schema.leafField(ImmutableList.copyOf(chunk.getPath().toArray()));
                if (field.isEmpty()) {
                    continue;
                }
                long columnId = field.get().columnId();
                fields.putIfAbsent(columnId, field.get());
                accumulators.computeIfAbsent(columnId, _ -> new ColumnAccumulator()).add(chunk);
            }
        }

        ImmutableList.Builder<DuckLakeFileColumnStatsRow> statistics = ImmutableList.builderWithExpectedSize(accumulators.size());
        for (Map.Entry<Long, ColumnAccumulator> entry : accumulators.entrySet()) {
            DuckLakeParquetSchema.LeafField field = fields.get(entry.getKey());
            statistics.add(entry.getValue().toStatistics(entry.getKey(), field, containsNanFor(field)));
        }
        return statistics.build();
    }

    /**
     * Whether the column is known to hold a {@code NaN}, for the columns that are tracked.
     */
    private Optional<Boolean> containsNanFor(DuckLakeParquetSchema.LeafField field)
    {
        return Optional.ofNullable(floatingPointColumnIds.get(field.columnId()))
                .map(index -> containsNan[index]);
    }

    private static final class ColumnAccumulator
    {
        private long valueCount;
        private long compressedSize;
        private long nullCount;
        private boolean nullCountKnown = true;
        private final List<org.apache.parquet.column.statistics.Statistics<?>> statistics = new ArrayList<>();

        private void add(ColumnChunkMetadata chunk)
        {
            valueCount += chunk.getValueCount();
            compressedSize += chunk.getTotalSize();
            org.apache.parquet.column.statistics.Statistics<?> chunkStatistics = chunk.getStatistics();
            if (chunkStatistics == null || !chunkStatistics.isNumNullsSet()) {
                nullCountKnown = false;
            }
            else {
                nullCount += chunkStatistics.getNumNulls();
            }
            if (chunkStatistics != null) {
                statistics.add(chunkStatistics);
            }
        }

        private DuckLakeFileColumnStatsRow toStatistics(long columnId, DuckLakeParquetSchema.LeafField field, Optional<Boolean> containsNan)
        {
            Optional<String> minValue = Optional.empty();
            Optional<String> maxValue = Optional.empty();
            if (field.statisticsSupported() && !containsNan.orElse(false)) {
                Comparable<Object> min = null;
                Comparable<Object> max = null;
                for (org.apache.parquet.column.statistics.Statistics<?> chunkStatistics : statistics) {
                    if (!chunkStatistics.hasNonNullValue()) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    Comparable<Object> chunkMin = (Comparable<Object>) chunkStatistics.genericGetMin();
                    @SuppressWarnings("unchecked")
                    Comparable<Object> chunkMax = (Comparable<Object>) chunkStatistics.genericGetMax();
                    if (min == null || chunkMin.compareTo(min) < 0) {
                        min = chunkMin;
                    }
                    if (max == null || chunkMax.compareTo(max) > 0) {
                        max = chunkMax;
                    }
                }
                if (min != null) {
                    minValue = StatsValueFormatter.format(field.type(), min);
                    maxValue = StatsValueFormatter.format(field.type(), max);
                }
            }
            return new DuckLakeFileColumnStatsRow(
                    columnId,
                    OptionalLong.of(compressedSize),
                    OptionalLong.of(valueCount),
                    nullCountKnown ? OptionalLong.of(nullCount) : OptionalLong.empty(),
                    minValue,
                    maxValue,
                    containsNan);
        }
    }
}
