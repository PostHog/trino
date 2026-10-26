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
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStatsRow;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema.LeafField;
import io.trino.plugin.ducklake.util.StatsValueFormatter;
import io.trino.spi.TrinoException;
import org.apache.parquet.column.statistics.Statistics;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;

/**
 * Statistics imported from footers; a missing bound in any row group leaves the file bound unknown.
 */
final class DuckLakeImportStatistics
{
    private DuckLakeImportStatistics() {}

    public static List<DuckLakeFileColumnStatsRow> collect(ParquetMetadata metadata, Map<List<String>, LeafField> fields)
            throws IOException
    {
        Map<Long, Accumulator> accumulators = new HashMap<>();
        for (var block : metadata.getBlocks()) {
            for (ColumnChunkMetadata chunk : block.columns()) {
                LeafField field = fields.get(ImmutableList.copyOf(chunk.getPath().toArray()));
                if (field != null) {
                    accumulators.computeIfAbsent(field.columnId(), _ -> new Accumulator(field)).add(chunk);
                }
            }
        }
        return accumulators.values().stream().sorted(Comparator.comparingLong(value -> value.field.columnId()))
                .map(Accumulator::result).collect(toImmutableList());
    }

    private static final class Accumulator
    {
        private final LeafField field;
        private long values;
        private long nulls;
        private long size;
        private boolean nullsKnown = true;
        private boolean boundsKnown = true;
        private Statistics<?> merged;

        public Accumulator(LeafField field)
        {
            this.field = field;
        }

        public void add(ColumnChunkMetadata chunk)
        {
            values = Math.addExact(values, chunk.getValueCount());
            size = Math.addExact(size, chunk.getTotalSize());
            Statistics<?> statistics = chunk.getStatistics();
            if (statistics == null || !statistics.isNumNullsSet()) {
                nullsKnown = false;
            }
            else {
                if (statistics.getNumNulls() < 0 || statistics.getNumNulls() > chunk.getValueCount()) {
                    throw new TrinoException(DuckLakeErrorCode.DUCKLAKE_BAD_DATA, "Invalid Parquet null count");
                }
                nulls = Math.addExact(nulls, statistics.getNumNulls());
            }
            if (statistics == null || (!statistics.hasNonNullValue() && (!statistics.isNumNullsSet() || statistics.getNumNulls() != chunk.getValueCount()))) {
                boundsKnown = false;
            }
            else if (statistics.hasNonNullValue()) {
                if (merged == null) {
                    merged = statistics.copy();
                }
                else {
                    merged.mergeStatistics(statistics);
                }
            }
        }

        public DuckLakeFileColumnStatsRow result()
        {
            boolean floating = field.type().equals(DOUBLE) || field.type().equals(REAL);
            Optional<String> minimum = Optional.empty();
            Optional<String> maximum = Optional.empty();
            // Parquet bounds exclude NaNs; the footer cannot establish their absence.
            if (boundsKnown && field.statisticsSupported() && !floating && merged != null) {
                minimum = StatsValueFormatter.format(field.type(), merged.genericGetMin());
                maximum = StatsValueFormatter.format(field.type(), merged.genericGetMax());
            }
            return new DuckLakeFileColumnStatsRow(
                    field.columnId(),
                    OptionalLong.of(size),
                    OptionalLong.of(values),
                    nullsKnown ? OptionalLong.of(nulls) : OptionalLong.empty(),
                    minimum,
                    maximum,
                    floating ? Optional.empty() : Optional.of(false));
        }
    }
}
