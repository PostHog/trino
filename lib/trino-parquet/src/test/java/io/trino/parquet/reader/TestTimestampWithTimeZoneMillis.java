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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.block.Block;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reading a Parquet {@code TIMESTAMP(MILLIS, isAdjustedToUTC=true)} column into a Trino
 * timestamp with time zone type. Files written outside of Trino store such values with
 * millisecond precision even when the table declares a higher precision column type, so the
 * values are widened on read.
 */
public class TestTimestampWithTimeZoneMillis
{
    private static final long[] EPOCH_MILLIS = {
            1755616478620L, // 2025-08-19T15:14:38.620Z
            1774453397001L, // 2026-03-25T15:43:17.001Z
            0L,
            -1000L,
            -1L,
    };

    @Test
    public void testReadAsMicros()
            throws IOException
    {
        assertReadAs(TIMESTAMP_TZ_MICROS);
    }

    @Test
    public void testReadAsNanos()
            throws IOException
    {
        assertReadAs(TIMESTAMP_TZ_NANOS);
    }

    @Test
    public void testReadAsMillis()
            throws IOException
    {
        assertReadAs(TIMESTAMP_TZ_MILLIS);
    }

    private static void assertReadAs(TimestampWithTimeZoneType type)
            throws IOException
    {
        int valueCount = EPOCH_MILLIS.length;
        PrimitiveField field = new PrimitiveField(
                type,
                true,
                new ColumnDescriptor(
                        new String[] {"ts"},
                        Types.required(INT64)
                                .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
                                .named("ts"),
                        0,
                        0),
                0);

        ValuesWriter writer = new PlainValuesWriter(1024, 1024, HeapByteBufferAllocator.getInstance());
        for (long epochMillis : EPOCH_MILLIS) {
            writer.writeLong(epochMillis);
        }
        Slice slice = Slices.wrappedBuffer(writer.getBytes().toByteArray());
        DataPage dataPage = new DataPageV2(
                valueCount,
                0,
                valueCount,
                EMPTY_SLICE,
                EMPTY_SLICE,
                PLAIN,
                slice,
                slice.length(),
                OptionalLong.empty(),
                null,
                false,
                0);

        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(DateTimeZone.UTC, ParquetReaderOptions.defaultOptions());
        ColumnReader reader = columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
        PageReader pageReader = new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, List.of(dataPage).iterator(), false, false, Optional.empty(), -1, -1);
        reader.setPageReader(pageReader, Optional.empty());
        reader.prepareNextRead(valueCount);
        Block block = reader.readPrimitive().getBlock();

        assertThat(block.getPositionCount()).isEqualTo(valueCount);
        for (int position = 0; position < valueCount; position++) {
            SqlTimestampWithTimeZone value = (SqlTimestampWithTimeZone) type.getObjectValue(block, position);
            assertThat(value.getEpochMillis()).isEqualTo(EPOCH_MILLIS[position]);
            assertThat(value.getPicosOfMilli()).isEqualTo(0);
        }
    }
}
