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
package io.trino.parquet.variant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation.ParquetWriteValidationBuilder;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.variant.ShreddedVariantTestFiles.ShreddedVariantCase;
import io.trino.parquet.variant.VariantShreddingSchema.ArrayValue;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectField;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedType;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.parquet.variant.VariantShreddingSchema.TypedValue;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.MULTIPLE_ROWS;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.SINGLE_ROW;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB_FIXTURES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.PARQUET_TESTING;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.loadParquetTestingCases;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariant;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariants;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.evaluate;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.key;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.objectFields;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.parseSchema;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readPhysicalColumn;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readVariantFile;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readVariants;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.toVariants;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.PrimitiveType.STRING;
import static io.trino.spi.variant.Header.arrayHeader;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.primitiveHeader;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal4;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantEncoder.encodeObject;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static java.util.Collections.nCopies;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestShreddedVariantWriter
{
    private static final ParquetReaderOptions READER_OPTIONS = ParquetReaderOptions.defaultOptions();
    private static final ParquetWriterOptions WRITER_OPTIONS = ParquetWriterOptions.builder().build();
    private static final List<String> RANDOM_KEYS = ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "A", "B", "x");
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    public void testParquetLayout()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(
                field("a", primitive(ShreddedType.STRING)),
                field("b", object(field("c", primitive(ShreddedType.INT64)))),
                field("d", array(primitive(ShreddedType.DOUBLE))),
                field("e", untyped())));
        GroupType group = schema.toParquetType("v", OPTIONAL);
        // Field groups and list elements are required, and every group has a value column
        assertThat(group.toString()).isEqualTo(
                """
                optional group v (VARIANT(1)) {
                  required binary metadata;
                  optional binary value;
                  optional group typed_value {
                    required group a {
                      optional binary value;
                      optional binary typed_value (STRING);
                    }
                    required group b {
                      optional binary value;
                      optional group typed_value {
                        required group c {
                          optional binary value;
                          optional int64 typed_value;
                        }
                      }
                    }
                    required group d {
                      optional binary value;
                      optional group typed_value (LIST) {
                        repeated group list {
                          required group element {
                            optional binary value;
                            optional double typed_value;
                          }
                        }
                      }
                    }
                    required group e {
                      optional binary value;
                    }
                  }
                }""");
        assertThat(VariantShreddingSchema.fromWriterSchema(group).value()).isEqualTo(schema.value());
        assertThat(VariantShreddingSchema.fromWriterSchema(group.withId(7)).value()).isEqualTo(schema.value());

        // Without a top-level typed_value, the group has the unshredded layout
        assertThat(VariantShreddingSchema.of(untyped()).toParquetType("v", REQUIRED).toString()).isEqualTo(
                """
                required group v (VARIANT(1)) {
                  required binary metadata;
                  required binary value;
                }""");

        // The footer has the same layout, and the reader parses the schema that was written
        Slice file = write(schema, ImmutableList.of(Optional.of(Variant.ofInt(1))));
        try (ParquetDataSource dataSource = new TestingParquetDataSource(file, READER_OPTIONS)) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
            assertThat(metadata.getFileMetaData().getSchema().getType("v")).isEqualTo(group);
            assertThat(parseSchema(metadata, "v", dataSource).value()).isEqualTo(schema.value());
        }
    }

    @Test
    public void testParquetTypes()
    {
        // The Parquet types of the specification, which the reader parses back to the same Variant types
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.BOOLEAN))).isEqualTo("optional boolean typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.INT8))).isEqualTo("optional int32 typed_value (INTEGER(8,true))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.INT16))).isEqualTo("optional int32 typed_value (INTEGER(16,true))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.INT32))).isEqualTo("optional int32 typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.INT64))).isEqualTo("optional int64 typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.FLOAT))).isEqualTo("optional float typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.DOUBLE))).isEqualTo("optional double typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.decimal(ShreddedType.DECIMAL4, 9, 2))).isEqualTo("optional int32 typed_value (DECIMAL(9,2))");
        assertThat(primitiveTypeString(PrimitiveValue.decimal(ShreddedType.DECIMAL8, 18, 0))).isEqualTo("optional int64 typed_value (DECIMAL(18,0))");
        assertThat(primitiveTypeString(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 38, 10))).isEqualTo("optional fixed_len_byte_array(16) typed_value (DECIMAL(38,10))");
        assertThat(primitiveTypeString(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 19, 0))).isEqualTo("optional fixed_len_byte_array(9) typed_value (DECIMAL(19,0))");
        assertThat(primitiveTypeString(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 1, 1))).isEqualTo("optional fixed_len_byte_array(1) typed_value (DECIMAL(1,1))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.DATE))).isEqualTo("optional int32 typed_value (DATE)");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.TIME_MICROS))).isEqualTo("optional int64 typed_value (TIME(MICROS,false))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.TIMESTAMP_MICROS))).isEqualTo("optional int64 typed_value (TIMESTAMP(MICROS,false))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.TIMESTAMP_NANOS))).isEqualTo("optional int64 typed_value (TIMESTAMP(NANOS,false))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.TIMESTAMP_TZ_MICROS))).isEqualTo("optional int64 typed_value (TIMESTAMP(MICROS,true))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.TIMESTAMP_TZ_NANOS))).isEqualTo("optional int64 typed_value (TIMESTAMP(NANOS,true))");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.BINARY))).isEqualTo("optional binary typed_value");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.STRING))).isEqualTo("optional binary typed_value (STRING)");
        assertThat(primitiveTypeString(PrimitiveValue.of(ShreddedType.UUID))).isEqualTo("optional fixed_len_byte_array(16) typed_value (UUID)");
    }

    private static String primitiveTypeString(PrimitiveValue primitive)
    {
        GroupType group = VariantShreddingSchema.of(new ShreddedValue(Optional.of(primitive))).toParquetType("v", OPTIONAL);
        // The schema that the reader parses has the same Variant type and Trino type
        assertThat(VariantShreddingSchema.fromWriterSchema(group).value().typedValue()).contains(primitive);
        return group.getType("typed_value").toString();
    }

    @Test
    public void testPrimitives()
            throws IOException
    {
        // Integers of all widths go to an integer column that holds them, and are read back with its width
        assertPrimitives(ShreddedType.INT8, ImmutableList.of(
                typed(Variant.ofByte(Byte.MIN_VALUE)),
                typed(Variant.ofByte(Byte.MAX_VALUE)),
                typed(Variant.ofShort((short) -5), Variant.ofByte((byte) -5)),
                typed(Variant.ofInt(100), Variant.ofByte((byte) 100)),
                typed(Variant.ofLong(1), Variant.ofByte((byte) 1)),
                untyped(Variant.ofShort((short) 128)),
                untyped(Variant.ofLong(Long.MIN_VALUE)),
                untyped(Variant.ofDecimal(new BigDecimal("1"))),
                untyped(Variant.ofString("1")),
                untyped(Variant.NULL_VALUE)));
        assertPrimitives(ShreddedType.INT16, ImmutableList.of(
                typed(Variant.ofShort(Short.MIN_VALUE)),
                typed(Variant.ofShort(Short.MAX_VALUE)),
                typed(Variant.ofByte((byte) 7), Variant.ofShort((short) 7)),
                typed(Variant.ofInt(-32768), Variant.ofShort((short) -32768)),
                untyped(Variant.ofInt(32768)),
                untyped(Variant.ofDouble(1))));
        assertPrimitives(ShreddedType.INT32, ImmutableList.of(
                typed(Variant.ofInt(Integer.MIN_VALUE)),
                typed(Variant.ofInt(Integer.MAX_VALUE)),
                typed(Variant.ofLong(Integer.MAX_VALUE), Variant.ofInt(Integer.MAX_VALUE)),
                typed(Variant.ofByte((byte) 0), Variant.ofInt(0)),
                untyped(Variant.ofLong(Integer.MAX_VALUE + 1L)),
                untyped(Variant.ofLong(Integer.MIN_VALUE - 1L)),
                untyped(Variant.ofBoolean(true))));
        assertPrimitives(ShreddedType.INT64, ImmutableList.of(
                typed(Variant.ofLong(Long.MIN_VALUE)),
                typed(Variant.ofLong(Long.MAX_VALUE)),
                typed(Variant.ofByte((byte) -1), Variant.ofLong(-1)),
                typed(Variant.ofShort((short) 300), Variant.ofLong(300)),
                typed(Variant.ofInt(Integer.MIN_VALUE), Variant.ofLong(Integer.MIN_VALUE)),
                untyped(decimal8(1, 0)),
                untyped(Variant.ofDouble(1)),
                untyped(Variant.ofFloat(1))));
        assertPrimitives(ShreddedType.BOOLEAN, ImmutableList.of(
                typed(Variant.ofBoolean(true)),
                typed(Variant.ofBoolean(false)),
                untyped(Variant.ofInt(1)),
                untyped(Variant.ofString("true"))));
        assertPrimitives(ShreddedType.FLOAT, ImmutableList.of(
                typed(Variant.ofFloat(1.5f)),
                typed(Variant.ofFloat(-0.0f)),
                typed(Variant.ofFloat(Float.NaN)),
                typed(Variant.ofFloat(Float.NEGATIVE_INFINITY)),
                typed(Variant.ofFloat(Float.MIN_VALUE)),
                untyped(Variant.ofDouble(1.5)),
                untyped(Variant.ofInt(1))));
        assertPrimitives(ShreddedType.DOUBLE, ImmutableList.of(
                typed(Variant.ofDouble(1.5)),
                typed(Variant.ofDouble(-0.0)),
                typed(Variant.ofDouble(Double.NaN)),
                typed(Variant.ofDouble(Double.POSITIVE_INFINITY)),
                typed(Variant.ofDouble(Double.MAX_VALUE)),
                untyped(Variant.ofFloat(1.5f)),
                untyped(Variant.ofLong(2)),
                untyped(decimal16(BigInteger.valueOf(15), 1))));
        assertPrimitives(ShreddedType.DATE, ImmutableList.of(
                typed(Variant.ofDate(0)),
                typed(Variant.ofDate(LocalDate.of(1900, 1, 1))),
                typed(Variant.ofDate(Integer.MIN_VALUE)),
                untyped(Variant.ofTimestampMicrosNtz(0)),
                untyped(Variant.ofInt(0))));
        // TIME(6) holds only times of day
        assertPrimitives(ShreddedType.TIME_MICROS, ImmutableList.of(
                typed(Variant.ofTimeMicrosNtz(0)),
                typed(Variant.ofTimeMicrosNtz(MICROSECONDS_PER_DAY - 1)),
                untyped(Variant.ofTimeMicrosNtz(MICROSECONDS_PER_DAY)),
                untyped(Variant.ofTimeMicrosNtz(-1)),
                untyped(Variant.ofLong(0))));
        assertPrimitives(ShreddedType.TIMESTAMP_MICROS, ImmutableList.of(
                typed(Variant.ofTimestampMicrosNtz(0)),
                typed(Variant.ofTimestampMicrosNtz(Long.MIN_VALUE)),
                typed(Variant.ofTimestampMicrosNtz(Long.MAX_VALUE)),
                untyped(Variant.ofTimestampMicrosUtc(0)),
                untyped(Variant.ofTimestampNanosNtz(0))));
        assertPrimitives(ShreddedType.TIMESTAMP_NANOS, ImmutableList.of(
                typed(Variant.ofTimestampNanosNtz(1)),
                typed(Variant.ofTimestampNanosNtz(-1)),
                typed(Variant.ofTimestampNanosNtz(Long.MIN_VALUE)),
                typed(Variant.ofTimestampNanosNtz(Long.MAX_VALUE)),
                untyped(Variant.ofTimestampNanosUtc(0)),
                untyped(Variant.ofTimestampMicrosNtz(0))));
        // TIMESTAMP WITH TIME ZONE stores epoch milliseconds in 52 bits
        long maxMillis = (1L << 51) - 1;
        assertPrimitives(ShreddedType.TIMESTAMP_TZ_MICROS, ImmutableList.of(
                typed(Variant.ofTimestampMicrosUtc(0)),
                typed(Variant.ofTimestampMicrosUtc(-1)),
                typed(Variant.ofTimestampMicrosUtc(maxMillis * 1000 + 999)),
                typed(Variant.ofTimestampMicrosUtc(-(maxMillis + 1) * 1000)),
                untyped(Variant.ofTimestampMicrosUtc((maxMillis + 1) * 1000)),
                untyped(Variant.ofTimestampMicrosUtc(-(maxMillis + 1) * 1000 - 1)),
                untyped(Variant.ofTimestampMicrosUtc(Long.MIN_VALUE)),
                untyped(Variant.ofTimestampMicrosNtz(0))));
        // The Parquet writer cannot write the epoch milliseconds of the smallest nanosecond values as nanoseconds
        assertPrimitives(ShreddedType.TIMESTAMP_TZ_NANOS, ImmutableList.of(
                typed(Variant.ofTimestampNanosUtc(0)),
                typed(Variant.ofTimestampNanosUtc(-1)),
                typed(Variant.ofTimestampNanosUtc(Long.MAX_VALUE)),
                typed(Variant.ofTimestampNanosUtc(Long.MIN_VALUE / 1_000_000 * 1_000_000)),
                untyped(Variant.ofTimestampNanosUtc(Long.MIN_VALUE)),
                untyped(Variant.ofTimestampMicrosUtc(0))));
        assertPrimitives(ShreddedType.BINARY, ImmutableList.of(
                typed(Variant.ofBinary(Slices.EMPTY_SLICE)),
                typed(Variant.ofBinary(Slices.wrappedBuffer(new byte[] {0, 1, (byte) 0xFF}))),
                untyped(Variant.ofString("abc"))));
        // A string goes to the column with either encoding
        Variant longEncodedShortString = longEncodedString("abc");
        assertThat(longEncodedShortString.primitiveType()).isEqualTo(STRING);
        assertPrimitives(ShreddedType.STRING, ImmutableList.of(
                typed(Variant.ofString("")),
                typed(Variant.ofString("a")),
                typed(Variant.ofString("ü".repeat(100))),
                typed(longEncodedShortString),
                untyped(Variant.ofBinary(utf8Slice("abc"))),
                untyped(Variant.ofInt(1))));
        assertPrimitives(ShreddedType.UUID, ImmutableList.of(
                typed(Variant.ofUuid(UUID.fromString("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"))),
                typed(Variant.ofUuid(new UUID(0, 0))),
                untyped(Variant.ofString("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"))));
    }

    @Test
    public void testDecimals()
            throws IOException
    {
        // A decimal of any width goes to a column of its scale whose precision holds it, and is read back with the width of the column
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL4, 5, 2), ImmutableList.of(
                typed(decimal4(12345, 2)),
                typed(decimal4(-99999, 2)),
                typed(decimal4(0, 2)),
                typed(decimal8(123, 2), decimal4(123, 2)),
                typed(decimal16(BigInteger.valueOf(-123), 2), decimal4(-123, 2)),
                untyped(decimal4(100000, 2)),
                untyped(decimal4(-100000, 2)),
                untyped(decimal4(Integer.MIN_VALUE, 2)),
                untyped(decimal8(100000, 2)),
                untyped(decimal16(BigInteger.valueOf(100000), 2)),
                untyped(decimal4(1230, 3)),
                untyped(decimal8(123, 1)),
                untyped(Variant.ofInt(1))));
        // A long has 19 digits, one more than DECIMAL(18)
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL8, 18, 4), ImmutableList.of(
                typed(decimal8(999_999_999_999_999_999L, 4)),
                typed(decimal8(-999_999_999_999_999_999L, 4)),
                typed(decimal4(1, 4), decimal8(1, 4)),
                typed(decimal4(Integer.MIN_VALUE, 4), decimal8(Integer.MIN_VALUE, 4)),
                typed(decimal16(BigInteger.valueOf(-999_999_999_999_999_999L), 4), decimal8(-999_999_999_999_999_999L, 4)),
                untyped(decimal8(1_000_000_000_000_000_000L, 4)),
                untyped(decimal8(Long.MIN_VALUE, 4)),
                untyped(decimal8(Long.MAX_VALUE, 4)),
                untyped(decimal16(BigInteger.valueOf(Long.MIN_VALUE), 4)),
                untyped(decimal8(1, 3)),
                untyped(decimal4(1, 5))));
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL8, 3, 0), ImmutableList.of(
                typed(decimal8(999, 0)),
                typed(decimal4(-999, 0), decimal8(-999, 0)),
                untyped(decimal8(1000, 0)),
                untyped(decimal4(1000, 0))));
        BigInteger maxUnscaled = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 38, 10), ImmutableList.of(
                typed(decimal16(maxUnscaled, 10)),
                typed(decimal16(maxUnscaled.negate(), 10)),
                typed(decimal16(BigInteger.valueOf(Long.MIN_VALUE), 10)),
                typed(decimal8(Long.MIN_VALUE, 10), decimal16(BigInteger.valueOf(Long.MIN_VALUE), 10)),
                typed(decimal4(1, 10), decimal16(BigInteger.ONE, 10)),
                untyped(decimal16(BigInteger.ONE, 9)),
                untyped(decimal8(1, 11))));
        // A DECIMAL16 column of a short decimal type, and unscaled values that need more than a long
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 10, 2), ImmutableList.of(
                typed(decimal16(BigInteger.valueOf(9_999_999_999L), 2)),
                typed(decimal16(BigInteger.valueOf(-9_999_999_999L), 2)),
                typed(decimal8(9_999_999_999L, 2), decimal16(BigInteger.valueOf(9_999_999_999L), 2)),
                typed(decimal4(-5, 2), decimal16(BigInteger.valueOf(-5), 2)),
                untyped(decimal16(BigInteger.valueOf(10_000_000_000L), 2)),
                untyped(decimal8(10_000_000_000L, 2)),
                untyped(decimal16(BigInteger.valueOf(Long.MIN_VALUE), 2))));
        BigInteger twentyDigits = BigInteger.TEN.pow(20).subtract(BigInteger.ONE);
        assertPrimitives(PrimitiveValue.decimal(ShreddedType.DECIMAL16, 20, 0), ImmutableList.of(
                typed(decimal16(twentyDigits, 0)),
                typed(decimal16(twentyDigits.negate(), 0)),
                typed(decimal16(BigInteger.valueOf(Long.MIN_VALUE), 0)),
                typed(decimal8(Long.MAX_VALUE, 0), decimal16(BigInteger.valueOf(Long.MAX_VALUE), 0)),
                untyped(decimal16(twentyDigits.add(BigInteger.ONE), 0)),
                untyped(decimal16(twentyDigits.add(BigInteger.ONE).negate(), 0))));
    }

    /// Writes each value as a row of a column with a `typed_value` of the type, and checks
    /// whether the value is in `typed_value` or in `value`, and the value that is read.
    private static void assertPrimitives(ShreddedType type, List<PrimitiveCase> cases)
            throws IOException
    {
        assertPrimitives(PrimitiveValue.of(type), cases);
    }

    private static void assertPrimitives(PrimitiveValue primitive, List<PrimitiveCase> cases)
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(new ShreddedValue(Optional.of(primitive)));
        Slice file = write(schema, cases.stream().map(testCase -> Optional.of(testCase.input())).collect(toImmutableList()));
        Block physical = physical(file);
        List<Optional<Variant>> actual = read(file);
        for (int row = 0; row < cases.size(); row++) {
            PrimitiveCase testCase = cases.get(row);
            String description = "%s row %s: %s".formatted(primitive, row, testCase.input());
            assertThat(field(physical, 2).isNull(row)).as(description + ": typed_value is null").isEqualTo(testCase.typed().isEmpty());
            // Outside objects, exactly one of value and typed_value is set
            assertThat(field(physical, 1).isNull(row)).as(description + ": value is null").isEqualTo(testCase.typed().isPresent());
            if (testCase.typed().isPresent()) {
                assertSameVariant(actual.get(row).orElseThrow(), testCase.typed().get(), description);
            }
            else {
                // A value in value is copied unchanged
                assertThat(actual.get(row).orElseThrow().data()).as(description).isEqualTo(testCase.input().data());
            }
        }
    }

    private static PrimitiveCase typed(Variant input)
    {
        return new PrimitiveCase(input, Optional.of(input));
    }

    private static PrimitiveCase typed(Variant input, Variant expected)
    {
        return new PrimitiveCase(input, Optional.of(expected));
    }

    private static PrimitiveCase untyped(Variant input)
    {
        return new PrimitiveCase(input, Optional.empty());
    }

    /// A value to write, and the value read back if it goes to `typed_value`.
    private record PrimitiveCase(Variant input, Optional<Variant> typed) {}

    @Test
    public void testNulls()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(field("a", primitive(ShreddedType.INT64)), field("b", primitive(ShreddedType.STRING))));
        List<Optional<Variant>> rows = ImmutableList.of(
                Optional.empty(),
                Optional.of(Variant.NULL_VALUE),
                Optional.of(variantObject()),
                Optional.of(variantObject("a", null)),
                Optional.of(variantObject("a", 1L, "c", null)),
                Optional.of(variantArray(null, 1L)),
                Optional.of(Variant.ofString("not an object")));
        Slice file = write(schema, rows);
        assertSameVariants(read(file), rows, "nulls");

        Block physical = physical(file);
        Block value = field(physical, 1);
        Block typedValue = field(physical, 2);
        Block a = field(physical, 2, 0);
        Block b = field(physical, 2, 1);

        // A SQL NULL is a null group, and a variant null is a value of variant null
        assertThat(physical.isNull(0)).isTrue();
        assertThat(physical.isNull(1)).isFalse();
        assertThat(VARBINARY.getSlice(value, 1)).isEqualTo(Variant.NULL_VALUE.data());
        assertThat(typedValue.isNull(1)).isTrue();

        // An empty object has a typed_value with all fields missing, and no value
        assertThat(value.isNull(2)).isTrue();
        assertThat(typedValue.isNull(2)).isFalse();
        assertMissing(a, 2);
        assertMissing(b, 2);

        // A field with a variant null has a value of variant null. A missing field has neither column.
        assertThat(VARBINARY.getSlice(field(a, 0), 3)).isEqualTo(Variant.NULL_VALUE.data());
        assertThat(field(a, 1).isNull(3)).isTrue();
        assertMissing(b, 3);

        // The fields that are not shredded are in value, also when they are null
        assertThat(objectFields(topLevelValue(physical, 4)).keySet()).containsExactly("c");
        assertThat(field(a, 0).isNull(4)).isTrue();
        assertThat(field(a, 1).isNull(4)).isFalse();

        // A value that is not an object is in value
        for (int row : new int[] {5, 6}) {
            assertThat(value.isNull(row)).isFalse();
            assertThat(typedValue.isNull(row)).isTrue();
        }
    }

    private static void assertMissing(Block fieldGroup, int row)
    {
        assertThat(fieldGroup.isNull(row)).as("field group is required").isFalse();
        assertThat(field(fieldGroup, 0).isNull(row)).isTrue();
        assertThat(field(fieldGroup, 1).isNull(row)).isTrue();
    }

    @Test
    public void testRequiredColumn()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(primitive(ShreddedType.INT64));
        List<Optional<Variant>> rows = ImmutableList.of(Optional.of(Variant.ofLong(1)), Optional.of(Variant.NULL_VALUE), Optional.of(Variant.ofString("x")));
        Slice file = write(schema, REQUIRED, rows, WRITER_OPTIONS);
        assertSameVariants(read(file), rows, "required");

        assertThatThrownBy(() -> write(schema, REQUIRED, ImmutableList.of(Optional.of(Variant.ofLong(1)), Optional.empty()), WRITER_OPTIONS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Required shredded VARIANT column has a NULL value");
    }

    @Test
    public void testObjects()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(
                field("name", primitive(ShreddedType.STRING)),
                field("address", object(field("city", primitive(ShreddedType.STRING)), field("zip", primitive(ShreddedType.INT32)))),
                field("payload", untyped()),
                // Field names of the shredding layout are also valid keys
                field("value", primitive(ShreddedType.INT32)),
                field("typed_value", untyped()),
                field("metadata", primitive(ShreddedType.BOOLEAN)),
                field("a.b", primitive(ShreddedType.STRING)),
                field("a", object(field("b", primitive(ShreddedType.STRING))))));
        List<Optional<Variant>> rows = ImmutableList.of(
                Optional.of(variantObject(
                        "name",
                        "alice",
                        "address",
                        variantObject("city", "Paris", "zip", 75001, "country", "FR"),
                        "payload",
                        variantObject("x", variantArray(1, "two")),
                        "extra",
                        1.5)),
                Optional.of(variantObject("name", 42, "address", "not an object", "payload", null)),
                Optional.of(variantObject("address", variantObject(), "other", variantObject("name", "nested name is not shredded"))),
                Optional.of(variantObject("value", 1, "typed_value", "t", "metadata", true)),
                Optional.of(variantObject("a.b", "dotted key", "a", variantObject("b", "nested key"))),
                Optional.of(variantObject("a.b", 1, "a", variantObject("b", 2, "c", 3))));
        Slice file = write(schema, rows);
        assertSameVariants(read(file), rows, "objects");

        Block physical = physical(file);
        // The fields that are not shredded are in value, as a partially shredded object
        assertThat(objectFields(topLevelValue(physical, 0)).keySet()).containsExactly("extra");
        assertThat(objectFields(groupValue(field(physical, 2, 1), 0, metadata(physical, 0))).keySet()).containsExactly("country");
        assertThat(field(physical, 1).isNull(1)).isTrue();
        assertThat(objectFields(topLevelValue(physical, 2)).keySet()).containsExactly("other");
        assertThat(field(physical, 1).isNull(3)).isTrue();
        assertThat(field(physical, 1).isNull(4)).isTrue();
    }

    @Test
    public void testKeysThatDifferOnlyByCase()
            throws IOException
    {
        // DuckDB 1.5.5 drops the values of every spelling but the first that it shreds
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(
                field("$browser", primitive(ShreddedType.STRING)),
                field("props", object(field("plan", primitive(ShreddedType.STRING))))));
        List<Optional<Variant>> rows = ImmutableList.of(
                Optional.of(variantObject("$browser", "Chrome", "$os", "Mac", "props", variantObject("Plan", "free", "plan", "pro"))),
                Optional.of(variantObject("$browser", "Firefox", "props", variantObject("plan", "team"))),
                Optional.of(variantObject("$Browser", "Safari", "props", variantObject("PLAN", "enterprise"))),
                Optional.of(variantObject("$browser", "Edge", "$Browser", "Edge2", "$BROWSER", "Edge3")));
        Slice file = write(schema, rows);
        assertSameVariants(read(file), rows, "case");

        // Only the exact names are shredded. The other spellings stay in value.
        Block physical = physical(file);
        assertThat(objectFields(topLevelValue(physical, 0)).keySet()).containsExactly("$os");
        assertThat(objectFields(groupValue(field(physical, 2, 1), 0, metadata(physical, 0))).keySet()).containsExactly("Plan");
        assertThat(field(physical, 1).isNull(1)).isTrue();
        assertThat(objectFields(topLevelValue(physical, 2)).keySet()).containsExactly("$Browser");
        assertThat(objectFields(groupValue(field(physical, 2, 1), 2, metadata(physical, 2))).keySet()).containsExactly("PLAN");
        assertThat(objectFields(topLevelValue(physical, 3)).keySet()).containsExactlyInAnyOrder("$Browser", "$BROWSER");
        assertThat(VARBINARY.getSlice(field(physical, 2, 0, 1), 3)).isEqualTo(utf8Slice("Edge"));

        // The reader finds Parquet columns by lowercase name, so two spellings cannot both be shredded
        assertThatThrownBy(() -> VariantShreddingSchema.of(object(field("plan", untyped()), field("Plan", untyped()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $ has fields that differ only by case: plan and Plan");
        assertThatThrownBy(() -> VariantShreddingSchema.of(object(field("props", object(field("É", untyped()), field("é", untyped()))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $.props has fields that differ only by case: É and é");
    }

    @Test
    public void testArrays()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(
                field("ids", array(primitive(ShreddedType.INT64))),
                field("events", array(object(field("type", primitive(ShreddedType.STRING))))),
                field("matrix", array(array(primitive(ShreddedType.DOUBLE)))),
                field("anything", array(untyped()))));
        List<Optional<Variant>> rows = ImmutableList.of(
                Optional.of(variantObject("ids", variantArray(1L, 2, (byte) 3), "events", variantArray(), "matrix", variantArray(variantArray(1.0, 2.0), variantArray()))),
                Optional.of(variantObject("ids", variantArray(null, "x", 4L), "events", variantArray(variantObject("type", "click", "x", 1), "not an object", null, variantObject()))),
                Optional.of(variantObject("ids", "not an array", "matrix", variantArray(null, variantArray(null, 1.5f, 2.0)), "anything", variantArray(1, "two", null))),
                Optional.of(variantObject("events", variantArray(variantObject("TYPE", "case"), variantObject("type", null)))),
                Optional.of(variantArray(1, 2)));
        Slice file = write(schema, rows);
        assertSameVariants(read(file), readBack(schema, rows), "arrays");

        Block physical = physical(file);
        ColumnarArray ids = toColumnarArray(field(physical, 2, 0, 1));
        Block idElements = ids.getElementsBlock();
        // Every element is present: a null element is a value of variant null
        int second = ids.getOffset(1);
        assertThat(VARBINARY.getSlice(field(idElements, 0), second)).isEqualTo(Variant.NULL_VALUE.data());
        assertThat(field(idElements, 1).isNull(second)).isTrue();
        assertThat(field(idElements, 0).isNull(second + 1)).isFalse();
        assertThat(field(idElements, 1).isNull(second + 2)).isFalse();
        for (int element = 0; element < idElements.getPositionCount(); element++) {
            assertThat(idElements.isNull(element)).as("element group is required").isFalse();
            assertThat(field(idElements, 0).isNull(element) ^ field(idElements, 1).isNull(element)).as("element %s", element).isTrue();
        }
        // A value that is not an array is in value
        assertThat(field(physical, 2, 0, 1).isNull(2)).isTrue();
        assertThat(field(physical, 2, 0, 0).isNull(2)).isFalse();
    }

    @Test
    public void testIntegersOfOtherWidths()
            throws IOException
    {
        // Trino's JSON to VARIANT conversion writes integers as INT32 or INT64, so a field of an INT64 column has both
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(field("n", primitive(ShreddedType.INT64)), field("small", primitive(ShreddedType.INT8))));
        Slice file = write(schema, ImmutableList.of(
                Optional.of(variantObject("n", 1, "small", 1L)),
                Optional.of(variantObject("n", 5_000_000_000L, "small", 300)),
                Optional.of(variantObject("n", (short) -2, "small", (byte) -128))));
        assertSameVariants(read(file), ImmutableList.of(
                Optional.of(variantObject("n", 1L, "small", (byte) 1)),
                Optional.of(variantObject("n", 5_000_000_000L, "small", 300)),
                Optional.of(variantObject("n", -2L, "small", (byte) -128))), "integers");
    }

    @Test
    public void testRepairs()
            throws IOException
    {
        // DuckDB marks every dictionary as sorted, and writes object fields in field id order. The
        // metadata lists 70 keys in descending order, and the object lists its fields in the same order.
        List<Slice> names = IntStream.range(0, 70)
                .mapToObj(key -> utf8Slice("k%02d".formatted(69 - key)))
                .collect(toImmutableList());
        Metadata metadata = falselySorted(Metadata.of(names));
        List<Slice> values = IntStream.range(0, 70)
                .mapToObj(id -> Variant.ofInt(69 - id).data())
                .collect(toImmutableList());
        Slice data = Slices.allocate(encodedObjectSize(69, 70, values.stream().mapToInt(Slice::length).sum()));
        encodeObject(70, id -> id, values::get, data, 0);
        Variant wideObject = Variant.from(metadata, data);
        // A lookup in an object of more than 64 fields assumes field name order
        assertThat(wideObject.getObjectField(utf8Slice("k00"))).isEmpty();

        VariantShreddingSchema schema = VariantShreddingSchema.of(object(field("k05", primitive(ShreddedType.INT32))));
        Slice file = write(schema, ImmutableList.of(Optional.of(wideObject)));
        Block physical = physical(file);
        Metadata writtenMetadata = metadata(physical, 0);
        assertSortedAndUnique(writtenMetadata);
        Variant otherFields = topLevelValue(physical, 0);
        assertThat(otherFields.objectFieldNames().map(Slice::toStringUtf8))
                .isSorted()
                .hasSize(69)
                .doesNotContain("k05");
        for (int key = 0; key < 70; key++) {
            if (key != 5) {
                assertThat(otherFields.getObjectField(utf8Slice("k%02d".formatted(key))).orElseThrow().getInt()).isEqualTo(key);
            }
        }
        assertThat(read(file).getFirst().orElseThrow().getObjectField(utf8Slice("k05")).orElseThrow().getInt()).isEqualTo(5);

        // A dictionary that is not sorted loses the flag, and keeps its strings
        Metadata unsortedMetadata = falselySorted(Metadata.of(ImmutableList.of(utf8Slice("b"), utf8Slice("a"))));
        // Fields "a" (id 1) and "b" (id 0), in field name order
        Slice object = Slices.allocate(encodedObjectSize(1, 2, 2 * Variant.ofInt(1).data().length()));
        encodeObject(2, index -> 1 - index, index -> Variant.ofInt(index + 1).data(), object, 0);
        file = write(VariantShreddingSchema.of(object(field("c", untyped()))), ImmutableList.of(Optional.of(Variant.from(unsortedMetadata, object))));
        writtenMetadata = metadata(physical(file), 0);
        assertThat(writtenMetadata.isSorted()).isFalse();
        assertThat(writtenMetadata.get(0)).isEqualTo(utf8Slice("b"));
        assertThat(writtenMetadata.get(1)).isEqualTo(utf8Slice("a"));
        assertThat(topLevelValue(physical(file), 0).getObjectField(utf8Slice("a")).orElseThrow().getInt()).isEqualTo(1);

        // An object with two fields of the same name cannot be written
        Metadata duplicateMetadata = Metadata.from(Slices.wrappedBuffer(new byte[] {0x01, 0x02, 0x00, 0x01, 0x02, 'k', 'k'}));
        Variant duplicateFields = Variant.from(duplicateMetadata, object);
        assertThatThrownBy(() -> write(schema, ImmutableList.of(Optional.of(duplicateFields))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT object has duplicate field k");
    }

    private static Metadata falselySorted(Metadata metadata)
    {
        Slice slice = metadata.toSlice().copy();
        slice.setByte(0, metadataHeader(true, metadataOffsetSize(slice.getByte(0))));
        Metadata falselySorted = Metadata.from(slice);
        assertThat(falselySorted.isSorted()).isTrue();
        return falselySorted;
    }

    private static void assertSortedAndUnique(Metadata metadata)
    {
        assertThat(metadata.isSorted()).isTrue();
        for (int id = 1; id < metadata.dictionarySize(); id++) {
            assertThat(metadata.get(id - 1).compareTo(metadata.get(id))).isNegative();
        }
    }

    @Test
    public void testStatistics()
            throws IOException
    {
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(field("n", primitive(ShreddedType.INT64)), field("s", primitive(ShreddedType.STRING))));
        Slice file = write(schema, ImmutableList.of(
                Optional.of(variantObject("n", 5L)),
                Optional.of(variantObject("n", 10)),
                Optional.of(variantObject("n", "999")),
                Optional.of(variantObject("s", "x")),
                Optional.empty(),
                Optional.of(Variant.NULL_VALUE),
                Optional.of(variantObject("big", "x".repeat(2000)))));
        try (ParquetDataSource dataSource = new TestingParquetDataSource(file, READER_OPTIONS)) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
            assertThat(metadata.getBlocks()).hasSize(1);
            Map<String, Statistics<?>> statistics = metadata.getBlocks().getFirst().columns().stream()
                    .collect(toImmutableMap(column -> column.getPath().toDotString(), ColumnChunkMetadata::getStatistics));
            assertThat(statistics.keySet()).containsExactly(
                    "v.metadata",
                    "v.value",
                    "v.typed_value.n.value",
                    "v.typed_value.n.typed_value",
                    "v.typed_value.s.value",
                    "v.typed_value.s.typed_value");

            // Every column has a null count, also a value column whose largest value is longer than the 1 KiB limit of bounds
            assertThat(statistics.get("v.metadata").getNumNulls()).isEqualTo(1);
            assertThat(statistics.get("v.value").getNumNulls()).isEqualTo(5);
            assertThat(statistics.get("v.value").isNumNullsSet()).isTrue();
            assertThat(statistics.get("v.typed_value.n.value").getNumNulls()).isEqualTo(6);
            assertThat(statistics.get("v.typed_value.s.value").getNumNulls()).isEqualTo(7);
            assertThat(statistics.get("v.typed_value.s.value").hasNonNullValue()).isFalse();

            // The bounds of a typed_value column cover only the values that it has
            Statistics<?> n = statistics.get("v.typed_value.n.typed_value");
            assertThat(n.getNumNulls()).isEqualTo(5);
            assertThat(n.genericGetMin()).isEqualTo(5L);
            assertThat(n.genericGetMax()).isEqualTo(10L);
            Statistics<?> s = statistics.get("v.typed_value.s.typed_value");
            assertThat(s.getNumNulls()).isEqualTo(6);
            assertThat(s.minAsString()).isEqualTo("x");
            assertThat(s.maxAsString()).isEqualTo("x");
        }
    }

    @Test
    public void testParquetTestingCasesRoundTrip()
            throws IOException
    {
        int cases = 0;
        for (ShreddedVariantCase testCase : loadParquetTestingCases()) {
            if (testCase.kind() != SINGLE_ROW && testCase.kind() != MULTIPLE_ROWS) {
                continue;
            }
            Path parquetFile = PARQUET_TESTING.resolve(testCase.parquetFile().orElseThrow());
            VariantShreddingSchema schema;
            try (ParquetDataSource dataSource = new FileParquetDataSource(parquetFile.toFile(), READER_OPTIONS)) {
                schema = VariantShreddingSchema.of(parseSchema(MetadataReader.readFooter(dataSource, Optional.empty()), "var", dataSource).value());
            }
            ImmutableList.Builder<Optional<Variant>> expected = ImmutableList.builder();
            for (Optional<String> variantFile : testCase.variantFiles()) {
                expected.add(variantFile.isEmpty() ? Optional.empty() : Optional.of(readVariantFile(PARQUET_TESTING.resolve(variantFile.get()))));
            }
            List<Optional<Variant>> rows = expected.build();
            String description = "case " + testCase.caseNumber();
            Slice file = write(schema, rows);
            assertSameVariants(read(file), readBack(schema, rows), description);
            assertLayout(schema, physical(file), Optional.of(physical(parquetFile, "var")), description);
            cases++;
        }
        assertThat(cases).isEqualTo(131);
    }

    @Test
    public void testDuckDbFilesRoundTrip()
            throws IOException
    {
        for (String fixture : DUCKDB_FIXTURES) {
            Path parquetFile = DUCKDB.resolve(fixture + ".parquet");
            VariantShreddingSchema schema;
            try (ParquetDataSource dataSource = new FileParquetDataSource(parquetFile.toFile(), READER_OPTIONS)) {
                schema = VariantShreddingSchema.of(parseSchema(MetadataReader.readFooter(dataSource, Optional.empty()), "v", dataSource).value());
            }
            List<Optional<Variant>> rows = readVariants(parquetFile, "v");
            Slice file = write(schema, rows);
            assertSameVariants(read(file), readBack(schema, rows), fixture);
            assertLayout(schema, physical(file), Optional.of(physical(parquetFile, "v")), fixture);
        }
    }

    @Test
    public void testManyRows()
            throws IOException
    {
        VariantShreddingSchema schema = randomSchema();
        List<Optional<Variant>> rows = randomRows(new Random(42), 5_000);
        ParquetWriterOptions options = ParquetWriterOptions.builder()
                .setMaxPageValueCount(100)
                .setBatchSize(37)
                .setMaxRowGroupRowCount(1_000)
                .build();
        Slice file = write(schema, OPTIONAL, rows, options);
        try (ParquetDataSource dataSource = new TestingParquetDataSource(file, READER_OPTIONS)) {
            assertThat(MetadataReader.readFooter(dataSource, Optional.empty()).getBlocks()).hasSize(5);
        }
        List<Optional<Variant>> expected = readBack(schema, rows);
        assertSameVariants(read(file), expected, "random");
        assertLayout(schema, physical(file), Optional.empty(), "random");

        // One row in each page of the reader
        ParquetReaderOptions smallPages = ParquetReaderOptions.builder().withMaxReadBlockRowCount(1).build();
        assertSameVariants(readPhysicalColumn(new TestingParquetDataSource(file, smallPages), "v", smallPages).variants(), expected, "small pages");
    }

    @Test
    public void testPrunedReads()
            throws IOException
    {
        Slice file = write(randomSchema(), randomRows(new Random(7), 500));
        assertPrunedPaths(file, List.of(List.of(key("a")), List.of(key("c"), key("d")), List.of(key("missing"))));
        assertPrunedPaths(file, List.of(List.of(key("c"), key("e"), new VariantPaths.ArrayElement())));
        assertPrunedPaths(file, List.of(List.of(key("f"), new VariantPaths.ArrayElement(), key("g"))));
        assertPrunedPaths(file, List.of(List.of(key("A")), List.of(key("b"))));
        assertPrunedPaths(file, List.of(List.of(key("c")), List.of(key("c"), key("d"))));
        assertPrunedPaths(file, List.of(List.of(new VariantPaths.ArrayElement())));
    }

    /// Checks that each path reads the same result, or fails the same way, from the
    /// pruned value as from the whole value.
    private static void assertPrunedPaths(Slice file, List<List<VariantPaths.Step>> paths)
            throws IOException
    {
        List<Optional<Variant>> whole = read(file);
        List<Optional<Variant>> pruned = readPhysicalColumn(new TestingParquetDataSource(file, READER_OPTIONS), "v", READER_OPTIONS, Optional.of(VariantPaths.of(paths))).variants();
        assertThat(pruned).hasSameSizeAs(whole);
        for (int row = 0; row < whole.size(); row++) {
            for (List<VariantPaths.Step> path : paths) {
                assertThat(evaluate(pruned.get(row), path)).as("row %s path %s", row, path).isEqualTo(evaluate(whole.get(row), path));
            }
        }
    }

    private static VariantShreddingSchema randomSchema()
    {
        return VariantShreddingSchema.of(object(
                field("a", primitive(ShreddedType.INT64)),
                field("b", primitive(ShreddedType.STRING)),
                field("c", object(field("d", primitive(ShreddedType.DOUBLE)), field("e", array(primitive(ShreddedType.INT32))))),
                field("f", array(object(field("g", primitive(ShreddedType.STRING)))))));
    }

    private static List<Optional<Variant>> randomRows(Random random, int count)
    {
        ImmutableList.Builder<Optional<Variant>> rows = ImmutableList.builder();
        for (int row = 0; row < count; row++) {
            rows.add(random.nextInt(20) == 0 ? Optional.empty() : Optional.of(Variant.fromObject(randomValue(random, 0))));
        }
        return rows.build();
    }

    private static Object randomValue(Random random, int depth)
    {
        int kind = random.nextInt(depth == 0 ? 3 : 12);
        return switch (kind) {
            case 0, 1 -> {
                Map<String, Object> object = new LinkedHashMap<>();
                int fields = random.nextInt(5);
                for (int field = 0; field < fields; field++) {
                    object.put(RANDOM_KEYS.get(random.nextInt(RANDOM_KEYS.size())), depth < 3 ? randomValue(random, depth + 1) : random.nextLong());
                }
                yield object;
            }
            case 2 -> {
                List<Object> array = new ArrayList<>();
                int elements = random.nextInt(4);
                for (int element = 0; element < elements; element++) {
                    array.add(depth < 3 ? randomValue(random, depth + 1) : "leaf");
                }
                yield array;
            }
            case 3 -> null;
            case 4 -> random.nextInt(100);
            case 5 -> random.nextLong();
            case 6 -> (byte) random.nextInt();
            case 7 -> random.nextDouble();
            case 8 -> random.nextFloat();
            case 9 -> "s" + random.nextInt(1000);
            case 10 -> random.nextBoolean();
            default -> new BigDecimal(BigInteger.valueOf(random.nextInt()), random.nextInt(5));
        };
    }

    @Test
    public void testDictionaryAndRunLengthEncodedInput()
            throws IOException
    {
        VariantShreddingSchema schema = randomSchema();
        List<Optional<Variant>> rows = randomRows(new Random(3), 50);
        Block variants = variantBlock(rows);
        // Each id twice, and some entries of the dictionary unused
        int[] ids = IntStream.range(0, 80).map(position -> 39 - position % 40).toArray();
        Slice file = writeBlock(schema, DictionaryBlock.create(ids.length, variants, ids));
        assertSameVariants(read(file), readBack(schema, IntStream.of(ids).mapToObj(rows::get).collect(toImmutableList())), "dictionary");

        Optional<Variant> first = rows.stream().filter(Optional::isPresent).findFirst().orElseThrow();
        file = writeBlock(schema, RunLengthEncodedBlock.create(variantBlock(ImmutableList.of(first)), 3));
        assertSameVariants(read(file), readBack(schema, nCopies(3, first)), "run length encoded");
        file = writeBlock(schema, RunLengthEncodedBlock.create(variantBlock(ImmutableList.of(Optional.empty())), 3));
        assertSameVariants(read(file), nCopies(3, Optional.empty()), "null run length encoded");

        // The shredded block keeps the encoding, so a large value that many positions share is shredded and copied once
        VariantShredder shredder = new VariantShredder(VariantShreddingSchema.of(primitive(ShreddedType.INT64)));
        Block large = variantBlock(ImmutableList.of(Optional.of(Variant.ofString("x".repeat(1 << 16))), Optional.of(Variant.ofLong(1)), Optional.empty()));
        int[] sharedIds = new int[1_000];
        sharedIds[1] = 2;
        Block shredded = shredder.shred(DictionaryBlock.create(sharedIds.length, large, sharedIds));
        assertThat(shredded).isInstanceOf(DictionaryBlock.class);
        assertThat(((DictionaryBlock) shredded).getDictionary().getPositionCount()).isEqualTo(2);
        assertThat(shredded.getRetainedSizeInBytes()).isLessThan(1 << 18);
        assertThat(shredded.isNull(1)).isTrue();
        // A dictionary with one used entry is compacted to a run-length encoded block
        shredded = shredder.shred(DictionaryBlock.create(sharedIds.length, large, new int[sharedIds.length]));
        assertThat(shredded).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(shredded.getRetainedSizeInBytes()).isLessThan(1 << 18);
        shredded = shredder.shred(RunLengthEncodedBlock.create(large.getRegion(0, 1), 1_000));
        assertThat(shredded).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(shredded.getPositionCount()).isEqualTo(1_000);
        assertThat(shredded.getRetainedSizeInBytes()).isLessThan(1 << 18);
    }

    @Test
    public void testNestedColumns()
            throws IOException
    {
        // The writer handles a shredded VARIANT at any position, although Trino readers read only top-level ones
        VariantShreddingSchema schema = randomSchema();
        MessageType messageType = Types.buildMessage()
                .addField(Types.optionalGroup().addField(schema.toParquetType("v", OPTIONAL)).named("s"))
                .addField(Types.optionalGroup().as(listType())
                        .addField(Types.repeatedGroup().addField(schema.toParquetType("element", OPTIONAL)).named("list"))
                        .named("l"))
                .addField(Types.optionalGroup().as(mapType())
                        .addField(Types.repeatedGroup()
                                .addField(Types.required(BINARY).as(stringType()).named("key"))
                                .addField(schema.toParquetType("value", OPTIONAL))
                                .named("key_value"))
                        .named("m"))
                // Required VARIANT groups whose parents can be null
                .addField(Types.optionalGroup().addField(schema.toParquetType("v", REQUIRED)).named("r"))
                .addField(Types.optionalGroup().as(listType())
                        .addField(Types.repeatedGroup().addField(schema.toParquetType("element", REQUIRED)).named("list"))
                        .named("rl"))
                .named("test");
        List<Optional<Variant>> rows = randomRows(new Random(11), 200);
        // A required VARIANT has a variant null where the others have a SQL NULL
        List<Optional<Variant>> nonNullRows = rows.stream()
                .map(row -> Optional.of(row.orElse(Variant.NULL_VALUE)))
                .collect(toImmutableList());

        // The values of each row of each column, or empty for a null struct, list or map
        List<List<Optional<List<Optional<Variant>>>>> expected = ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        for (int row = 0; row < rows.size(); row++) {
            expected.get(0).add(row % 7 == 0 ? Optional.empty() : Optional.of(ImmutableList.of(rows.get(row))));
            expected.get(1).add(row % 7 == 0 ? Optional.empty() : Optional.of(nestedValues(rows, row, row % 3)));
            expected.get(2).add(row % 5 == 0 ? Optional.empty() : Optional.of(nestedValues(rows, row, row % 4)));
            expected.get(3).add(row % 7 == 3 ? Optional.empty() : Optional.of(ImmutableList.of(nonNullRows.get(row))));
            expected.get(4).add(row % 6 == 0 ? Optional.empty() : Optional.of(nestedValues(nonNullRows, row, row % 4)));
        }

        RowType structType = RowType.from(ImmutableList.of(RowType.field("v", VARIANT)));
        ArrayType listType = new ArrayType(VARIANT);
        MapType mapType = new MapType(VARCHAR, VARIANT, TYPE_OPERATORS);
        RowBlockBuilder structs = structType.createBlockBuilder(null, rows.size());
        ArrayBlockBuilder lists = listType.createBlockBuilder(null, rows.size());
        MapBlockBuilder maps = mapType.createBlockBuilder(null, rows.size());
        RowBlockBuilder requiredStructs = structType.createBlockBuilder(null, rows.size());
        ArrayBlockBuilder requiredLists = listType.createBlockBuilder(null, rows.size());
        for (int row = 0; row < rows.size(); row++) {
            appendStruct(structs, expected.get(0).get(row));
            appendList(lists, expected.get(1).get(row));
            Optional<List<Optional<Variant>>> mapValues = expected.get(2).get(row);
            if (mapValues.isEmpty()) {
                maps.appendNull();
            }
            else {
                maps.buildEntry((keys, values) -> {
                    for (int entry = 0; entry < mapValues.get().size(); entry++) {
                        VARCHAR.writeString(keys, "k" + entry);
                        writeVariant(values, mapValues.get().get(entry));
                    }
                });
            }
            appendStruct(requiredStructs, expected.get(3).get(row));
            appendList(requiredLists, expected.get(4).get(row));
        }
        Slice file = writeParquetFile(
                WRITER_OPTIONS,
                messageType,
                ImmutableMap.of(ImmutableList.of("m", "key_value", "key"), VARCHAR),
                ImmutableList.of(new Page(structs.build(), lists.build(), maps.build(), requiredStructs.build(), requiredLists.build())));

        ShreddedVariantAssembler assembler = new ShreddedVariantAssembler(schema, new TestingParquetDataSource(file, READER_OPTIONS).getId());
        RowType physicalStructType = RowType.from(ImmutableList.of(RowType.field("v", schema.physicalType())));
        ArrayType physicalListType = new ArrayType(schema.physicalType());
        List<List<Optional<List<Optional<Variant>>>>> actual = ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        try (ParquetDataSource dataSource = new TestingParquetDataSource(file, READER_OPTIONS);
                ParquetReader reader = createParquetReader(
                        dataSource,
                        MetadataReader.readFooter(dataSource, Optional.empty()),
                        READER_OPTIONS,
                        newSimpleAggregatedMemoryContext(),
                        ImmutableList.of(physicalStructType, physicalListType, new MapType(VARCHAR, schema.physicalType(), TYPE_OPERATORS), physicalStructType, physicalListType),
                        ImmutableList.of("s", "l", "m", "r", "rl"),
                        TupleDomain.all())) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                for (int column : new int[] {0, 3}) {
                    Block struct = page.getBlock(column);
                    List<Optional<Variant>> values = toVariants(assembler.assemble(getRowFieldsFromBlock(struct).getFirst()));
                    for (int position = 0; position < struct.getPositionCount(); position++) {
                        actual.get(column).add(struct.isNull(position) ? Optional.empty() : Optional.of(ImmutableList.of(values.get(position))));
                    }
                }
                for (int column : new int[] {1, 4}) {
                    ColumnarArray array = toColumnarArray(page.getBlock(column));
                    List<Optional<Variant>> elements = toVariants(assembler.assemble(array.getElementsBlock()));
                    for (int position = 0; position < array.getPositionCount(); position++) {
                        actual.get(column).add(array.isNull(position)
                                ? Optional.empty()
                                : Optional.of(elements.subList(array.getOffset(position), array.getOffset(position) + array.getLength(position))));
                    }
                }
                ColumnarMap map = toColumnarMap(page.getBlock(2));
                List<Optional<Variant>> values = toVariants(assembler.assemble(map.getValuesBlock()));
                for (int position = 0; position < map.getPositionCount(); position++) {
                    if (map.isNull(position)) {
                        actual.get(2).add(Optional.empty());
                        continue;
                    }
                    for (int entry = 0; entry < map.getEntryCount(position); entry++) {
                        assertThat(VARCHAR.getSlice(map.getKeysBlock(), map.getOffset(position) + entry).toStringUtf8()).isEqualTo("k" + entry);
                    }
                    actual.get(2).add(Optional.of(values.subList(map.getOffset(position), map.getOffset(position) + map.getEntryCount(position))));
                }
            }
        }

        List<String> columns = ImmutableList.of("struct", "list", "map", "required in struct", "required in list");
        for (int column = 0; column < columns.size(); column++) {
            assertThat(actual.get(column)).as(columns.get(column)).hasSameSizeAs(expected.get(column));
            for (int row = 0; row < rows.size(); row++) {
                // A null struct, list or map stays null, and each row keeps its values
                String description = "%s row %s".formatted(columns.get(column), row);
                Optional<List<Optional<Variant>>> actualValues = actual.get(column).get(row);
                Optional<List<Optional<Variant>>> expectedValues = expected.get(column).get(row);
                assertThat(actualValues.isPresent()).as(description).isEqualTo(expectedValues.isPresent());
                if (actualValues.isPresent()) {
                    assertSameVariants(actualValues.get(), readBack(schema, expectedValues.get()), description);
                }
            }
        }
    }

    /// The values of `count` elements of a row: the value of the row and the values of the next rows
    private static List<Optional<Variant>> nestedValues(List<Optional<Variant>> rows, int row, int count)
    {
        return IntStream.range(0, count)
                .mapToObj(element -> rows.get((row + element) % rows.size()))
                .collect(toImmutableList());
    }

    private static void appendStruct(RowBlockBuilder builder, Optional<List<Optional<Variant>>> struct)
    {
        struct.ifPresentOrElse(fields -> builder.buildEntry(fieldBuilders -> writeVariant(fieldBuilders.getFirst(), fields.getFirst())), builder::appendNull);
    }

    private static void appendList(ArrayBlockBuilder builder, Optional<List<Optional<Variant>>> list)
    {
        list.ifPresentOrElse(elements -> builder.buildEntry(elementBuilder -> elements.forEach(element -> writeVariant(elementBuilder, element))), builder::appendNull);
    }

    @Test
    public void testLargeObjects()
            throws IOException
    {
        // The other fields of a partially shredded object are re-encoded: here more than 255 of
        // them, with field ids of two bytes, and a nested object longer than 65,535 bytes
        VariantShreddingSchema schema = VariantShreddingSchema.of(object(
                field("k000", primitive(ShreddedType.INT64)),
                field("nested", object(field("x", primitive(ShreddedType.STRING))))));
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int key = 0; key < 300; key++) {
            fields.put("k%03d".formatted(key), key % 3 == 0 ? (Object) (long) key : "v".repeat(key));
        }
        fields.put("nested", ImmutableMap.of("x", "y", "big", "z".repeat(70_000)));
        List<Optional<Variant>> rows = ImmutableList.of(Optional.of(Variant.fromObject(fields)), Optional.of(variantObject("k000", 1L, "k001", "a")));
        Slice file = write(schema, rows);
        assertSameVariants(read(file), readBack(schema, rows), "large objects");

        Block physical = physical(file);
        assertLayout(schema, physical, Optional.empty(), "large objects");
        assertThat(metadata(physical, 0).dictionarySize()).isGreaterThan(255);
        assertThat(topLevelValue(physical, 0).getObjectFieldCount()).isEqualTo(299);
        Variant nestedOtherFields = groupValue(field(physical, 2, 1), 0, metadata(physical, 0));
        assertThat(nestedOtherFields.data().length()).isGreaterThan(70_000);
        assertThat(objectFields(nestedOtherFields).keySet()).containsExactly("big");
    }

    @Test
    public void testDeeplyNestedValues()
            throws IOException
    {
        // The check of a value for repairs does not recurse, so a deeply nested value does not overflow the stack
        int depth = 100_000;
        int levelSize = 10;
        Slice data = Slices.allocate(depth * levelSize + 1);
        for (int level = 0; level < depth; level++) {
            // An array of one element, with offsets of four bytes
            int offset = level * levelSize;
            data.setByte(offset, arrayHeader(4, false));
            data.setByte(offset + 1, 1);
            data.setInt(offset + 2, 0);
            data.setInt(offset + 6, (depth - level - 1) * levelSize + 1);
        }
        data.setByte(depth * levelSize, primitiveHeader(PrimitiveType.NULL));
        Variant nested = Variant.from(EMPTY_METADATA, data);

        VariantShreddingSchema schema = VariantShreddingSchema.of(object(field("a", primitive(ShreddedType.INT64))));
        Slice file = write(schema, ImmutableList.of(Optional.of(nested)));
        assertThat(read(file).getFirst().orElseThrow().data()).isEqualTo(data);
    }

    @Test
    public void testWriteValidation()
    {
        // The validation reads the file back with a reader that does not read shredded VARIANT groups
        MessageType messageType = Types.buildMessage()
                .addField(Types.optionalGroup().addField(VariantShreddingSchema.of(primitive(ShreddedType.INT64)).toParquetType("v", OPTIONAL)).named("s"))
                .named("test");
        assertThatThrownBy(() -> new ParquetWriter(
                new ByteArrayOutputStream(),
                messageType,
                ImmutableMap.of(),
                WRITER_OPTIONS,
                CompressionCodec.SNAPPY,
                "test-version",
                Optional.empty(),
                Optional.of(new ParquetWriteValidationBuilder(ImmutableList.of(RowType.from(ImmutableList.of(RowType.field("v", VARIANT)))), ImmutableList.of("s")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Write validation does not support shredded VARIANT columns");
    }

    private static void writeVariant(BlockBuilder builder, Optional<Variant> variant)
    {
        variant.ifPresentOrElse(value -> VARIANT.writeObject(builder, value), builder::appendNull);
    }

    @Test
    public void testInvalidSchemas()
    {
        assertThatThrownBy(() -> VariantShreddingSchema.of(object()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $ has no fields");
        assertThatThrownBy(() -> VariantShreddingSchema.of(object(field("a", array(object(field("", untyped())))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $.a[*] has a field with an empty name");
        assertThatThrownBy(() -> VariantShreddingSchema.of(object(field("a", untyped()), field("a", primitive(ShreddedType.INT32)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $ has duplicate field a");
        assertThatThrownBy(() -> new PrimitiveValue(ShreddedType.INT8, BIGINT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type bigint does not hold shredded VARIANT values of type INT8");
        assertThatThrownBy(() -> PrimitiveValue.decimal(ShreddedType.DECIMAL4, 10, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type decimal(10,2) does not hold shredded VARIANT values of type DECIMAL4");
        assertThatThrownBy(() -> PrimitiveValue.decimal(ShreddedType.DECIMAL8, 19, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type decimal(19,2) does not hold shredded VARIANT values of type DECIMAL8");
        assertThatThrownBy(() -> PrimitiveValue.of(ShreddedType.DECIMAL16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A decimal column needs a precision and a scale: DECIMAL16");
        assertThatThrownBy(() -> VariantShreddingSchema.of(untyped()).toParquetType("v", Repetition.REPEATED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT group is repeated: v");
    }

    @Test
    public void testLayoutsThatAreNotWritten()
    {
        // The writer writes only the layout of the specification, which the Trino reader reads
        assertNotWritten(variantGroup(Types.optionalGroup()
                .addField(Types.optionalGroup().optional(BINARY).named("value").optional(BINARY).as(stringType()).named("typed_value").named("a"))
                .named("typed_value")));
        assertNotWritten(variantGroup(Types.optionalGroup()
                .addField(Types.requiredGroup().optional(BINARY).as(stringType()).named("typed_value").named("a"))
                .named("typed_value")));
        assertNotWritten(variantGroup(Types.optional(INT32).as(intType(32, true)).named("typed_value")));
        assertNotWritten(variantGroup(Types.optionalGroup().as(listType())
                .addField(Types.repeatedGroup().addField(Types.optionalGroup().optional(BINARY).named("value").optional(INT64).named("typed_value").named("element")).named("list"))
                .named("typed_value")));
        assertNotWritten(Types.optionalGroup().as(variantType((byte) 1))
                .required(BINARY).named("metadata")
                .required(BINARY).named("value")
                .optional(INT64).named("typed_value")
                .named("v"));
        assertNotWritten(Types.optionalGroup().as(variantType((byte) 1))
                .optional(BINARY).named("value")
                .optional(INT64).named("typed_value")
                .required(BINARY).named("metadata")
                .named("v"));

        assertThatThrownBy(() -> VariantShreddingSchema.fromWriterSchema(variantGroup(Types.optional(INT32).as(intType(32, false)).named("typed_value"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported shredded VARIANT value type: optional int32 typed_value (INTEGER(32,false))");
        assertThatThrownBy(() -> VariantShreddingSchema.fromWriterSchema(variantGroup(Types.optionalGroup()
                .addField(Types.requiredGroup().optional(BINARY).named("value").named("Plan"))
                .addField(Types.requiredGroup().optional(BINARY).named("value").named("plan"))
                .named("typed_value"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shredded VARIANT object $ has fields that differ only by case: Plan and plan");
    }

    private static GroupType variantGroup(org.apache.parquet.schema.Type typedValue)
    {
        return Types.optionalGroup().as(variantType((byte) 1))
                .required(BINARY).named("metadata")
                .optional(BINARY).named("value")
                .addField(typedValue)
                .named("v");
    }

    private static void assertNotWritten(GroupType group)
    {
        assertThatThrownBy(() -> VariantShreddingSchema.fromWriterSchema(group))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Shredded VARIANT group does not have the layout of the specification: " + group);
        MessageType messageType = Types.buildMessage().addField(group).named("test");
        assertThatThrownBy(() -> writeParquetFile(WRITER_OPTIONS, messageType, ImmutableMap.of(), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Shredded VARIANT group does not have the layout of the specification");
    }

    private static Slice write(VariantShreddingSchema schema, List<Optional<Variant>> rows)
            throws IOException
    {
        return write(schema, OPTIONAL, rows, WRITER_OPTIONS);
    }

    private static Slice write(VariantShreddingSchema schema, Repetition repetition, List<Optional<Variant>> rows, ParquetWriterOptions options)
            throws IOException
    {
        MessageType messageType = Types.buildMessage().addField(schema.toParquetType("v", repetition)).named("test");
        return writeParquetFile(options, messageType, ImmutableMap.of(), ImmutableList.of(new Page(variantBlock(rows))));
    }

    private static Slice writeBlock(VariantShreddingSchema schema, Block variants)
            throws IOException
    {
        MessageType messageType = Types.buildMessage().addField(schema.toParquetType("v", OPTIONAL)).named("test");
        return writeParquetFile(WRITER_OPTIONS, messageType, ImmutableMap.of(), ImmutableList.of(new Page(variants)));
    }

    private static Block variantBlock(List<Optional<Variant>> rows)
    {
        BlockBuilder builder = VARIANT.createBlockBuilder(null, rows.size());
        rows.forEach(row -> writeVariant(builder, row));
        return builder.build();
    }

    private static List<Optional<Variant>> read(Slice file)
            throws IOException
    {
        return readPhysicalColumn(new TestingParquetDataSource(file, READER_OPTIONS), "v", READER_OPTIONS).variants();
    }

    /// The rows of the `metadata`, `value`, and `typed_value` columns that the Parquet reader returns
    private static Block physical(Slice file)
            throws IOException
    {
        RowType physicalType;
        try (ParquetDataSource dataSource = new TestingParquetDataSource(file, READER_OPTIONS)) {
            physicalType = parseSchema(MetadataReader.readFooter(dataSource, Optional.empty()), "v", dataSource).physicalType();
        }
        return concat(physicalType, readPhysicalColumn(new TestingParquetDataSource(file, READER_OPTIONS), "v", READER_OPTIONS).blocks());
    }

    private static Block physical(Path file, String column)
            throws IOException
    {
        RowType physicalType;
        try (ParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), READER_OPTIONS)) {
            physicalType = parseSchema(MetadataReader.readFooter(dataSource, Optional.empty()), column, dataSource).physicalType();
        }
        return concat(physicalType, readPhysicalColumn(file, column, READER_OPTIONS).blocks());
    }

    /// The reader returns pages of increasing size
    private static Block concat(RowType type, List<Block> blocks)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 0);
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                builder.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
            }
        }
        return builder.build();
    }

    /// The rows that a column with the schema reads back: an integer or a decimal in
    /// `typed_value` has the width of its column
    private static List<Optional<Variant>> readBack(VariantShreddingSchema schema, List<Optional<Variant>> rows)
    {
        return rows.stream()
                .map(row -> row.map(value -> readBack(schema.value(), value)))
                .collect(toImmutableList());
    }

    private static Variant readBack(ShreddedValue schema, Variant value)
    {
        if (schema.typedValue().isEmpty()) {
            return value;
        }
        return switch (schema.typedValue().get()) {
            case PrimitiveValue primitive -> withColumnWidth(primitive, value);
            case ObjectValue object -> {
                if (value.basicType() != BasicType.OBJECT) {
                    yield value;
                }
                Map<String, ShreddedValue> shreddedFields = object.fields().stream().collect(toImmutableMap(ObjectField::name, ObjectField::value));
                Map<Slice, Variant> fields = new HashMap<>();
                objectFields(value).forEach((name, fieldValue) -> fields.put(
                        utf8Slice(name),
                        shreddedFields.containsKey(name) ? readBack(shreddedFields.get(name), fieldValue) : fieldValue));
                yield Variant.ofObject(fields);
            }
            case ArrayValue array -> {
                if (value.basicType() != BasicType.ARRAY) {
                    yield value;
                }
                List<Variant> elements = new ArrayList<>();
                for (int index = 0; index < value.getArrayLength(); index++) {
                    elements.add(readBack(array.element(), value.getArrayElement(index)));
                }
                yield Variant.ofArray(elements);
            }
        };
    }

    /// An integer or a decimal that the column holds, with the width of the column
    private static Variant withColumnWidth(PrimitiveValue column, Variant value)
    {
        if (value.basicType() != BasicType.PRIMITIVE) {
            return value;
        }
        PrimitiveType type = value.primitiveType();
        ShreddedType width = column.shreddedType();
        if (type == PrimitiveType.INT8 || type == PrimitiveType.INT16 || type == PrimitiveType.INT32 || type == PrimitiveType.INT64) {
            long number = ((Number) value.toObject()).longValue();
            if (width == ShreddedType.INT8 && number == (byte) number) {
                return Variant.ofByte((byte) number);
            }
            if (width == ShreddedType.INT16 && number == (short) number) {
                return Variant.ofShort((short) number);
            }
            if (width == ShreddedType.INT32 && number == (int) number) {
                return Variant.ofInt((int) number);
            }
            return width == ShreddedType.INT64 ? Variant.ofLong(number) : value;
        }
        if ((type == PrimitiveType.DECIMAL4 || type == PrimitiveType.DECIMAL8 || type == PrimitiveType.DECIMAL16) && column.type() instanceof DecimalType decimalType) {
            BigDecimal decimal = value.getDecimal();
            BigInteger unscaled = decimal.unscaledValue();
            if (decimal.scale() != decimalType.getScale() || unscaled.abs().compareTo(BigInteger.TEN.pow(decimalType.getPrecision())) >= 0) {
                return value;
            }
            if (width == ShreddedType.DECIMAL4) {
                return decimal4(unscaled.intValueExact(), decimal.scale());
            }
            if (width == ShreddedType.DECIMAL8) {
                return decimal8(unscaled.longValueExact(), decimal.scale());
            }
            return decimal16(unscaled, decimal.scale());
        }
        return value;
    }

    /// Checks the layout of shredded rows: outside objects, `value` and `typed_value` are
    /// not both set, an object or an array of a column of objects or lists is in
    /// `typed_value`, and a partially shredded object has none of the shredded fields in
    /// `value`. With the rows of another writer's file with the same schema, it also checks
    /// that each value that the file has in `typed_value` is in `typed_value`.
    private static void assertLayout(VariantShreddingSchema schema, Block physical, Optional<Block> reference, String description)
    {
        int[] rows = IntStream.range(0, physical.getPositionCount()).toArray();
        assertLayout(schema.value(), physical, 1, rows, field(physical, 0), reference.map(block -> new LayoutReference(block, rows)), description + " $");
    }

    /// The groups of a reference file, and the position in them of each position of the checked groups, or -1
    private record LayoutReference(Block groups, int[] positions) {}

    /// @param valueField the index of the `value` field of the groups
    /// @param rows the row of each position of the groups
    private static void assertLayout(ShreddedValue schema, Block groups, int valueField, int[] rows, Block metadata, Optional<LayoutReference> reference, String path)
    {
        if (schema.typedValue().isEmpty()) {
            return;
        }
        TypedValue typed = schema.typedValue().get();
        Block value = field(groups, valueField);
        Block typedValue = field(groups, valueField + 1);
        Optional<Block> referenceTypedValue = reference.map(layoutReference -> field(layoutReference.groups(), valueField + 1));
        // The position of each position in the typed_value of the reference, if the reference has a typed_value there
        int[] referencePositions = new int[groups.getPositionCount()];
        Arrays.fill(referencePositions, -1);
        for (int position = 0; position < groups.getPositionCount(); position++) {
            if (groups.isNull(position)) {
                continue;
            }
            String description = "%s row %s".formatted(path, rows[position]);
            if (reference.isPresent()) {
                int referencePosition = reference.get().positions()[position];
                if (referencePosition >= 0 && !reference.get().groups().isNull(referencePosition) && !referenceTypedValue.orElseThrow().isNull(referencePosition)) {
                    assertThat(typedValue.isNull(position)).as(description + ": the reference has a typed_value").isFalse();
                    referencePositions[position] = referencePosition;
                }
            }
            if (value.isNull(position)) {
                continue;
            }
            Variant variant = Variant.from(Metadata.from(VARBINARY.getSlice(metadata, rows[position])), VARBINARY.getSlice(value, position));
            if (typedValue.isNull(position)) {
                if (!(typed instanceof PrimitiveValue)) {
                    assertThat(variant.basicType()).as(description + ": a value that typed_value holds").isNotEqualTo(typed instanceof ObjectValue ? BasicType.OBJECT : BasicType.ARRAY);
                }
                continue;
            }
            // Only a partially shredded object has both, and its value has the fields that are not shredded
            assertThat(typed).as(description + ": value and typed_value").isInstanceOf(ObjectValue.class);
            assertThat(variant.basicType()).as(description).isEqualTo(BasicType.OBJECT);
            assertThat(objectFields(variant).keySet())
                    .as(description + ": fields in value")
                    .doesNotContainAnyElementsOf(((ObjectValue) typed).fields().stream().map(ObjectField::name).collect(toImmutableSet()));
        }

        Optional<LayoutReference> typedReference = referenceTypedValue.map(block -> new LayoutReference(block, referencePositions));
        switch (typed) {
            case PrimitiveValue _ -> {}
            case ObjectValue object -> {
                for (int index = 0; index < object.fields().size(); index++) {
                    int fieldIndex = index;
                    ObjectField objectField = object.fields().get(index);
                    assertLayout(
                            objectField.value(),
                            field(typedValue, index),
                            0,
                            rows,
                            metadata,
                            typedReference.map(layoutReference -> new LayoutReference(field(layoutReference.groups(), fieldIndex), layoutReference.positions())),
                            path + "." + objectField.name());
                }
            }
            case ArrayValue array -> {
                ColumnarArray lists = toColumnarArray(typedValue);
                Optional<ColumnarArray> referenceLists = typedReference.map(layoutReference -> toColumnarArray(layoutReference.groups()));
                int[] elementRows = new int[lists.getElementsBlock().getPositionCount()];
                int[] elementReferencePositions = new int[elementRows.length];
                Arrays.fill(elementReferencePositions, -1);
                for (int position = 0; position < lists.getPositionCount(); position++) {
                    for (int element = 0; element < lists.getLength(position); element++) {
                        int elementPosition = lists.getOffset(position) + element;
                        elementRows[elementPosition] = rows[position];
                        if (referencePositions[position] >= 0) {
                            ColumnarArray referenceList = referenceLists.orElseThrow();
                            assertThat(referenceList.getLength(referencePositions[position])).as("%s row %s", path, rows[position]).isEqualTo(lists.getLength(position));
                            elementReferencePositions[elementPosition] = referenceList.getOffset(referencePositions[position]) + element;
                        }
                    }
                }
                assertLayout(
                        array.element(),
                        lists.getElementsBlock(),
                        0,
                        elementRows,
                        metadata,
                        referenceLists.map(referenceList -> new LayoutReference(referenceList.getElementsBlock(), elementReferencePositions)),
                        path + "[*]");
            }
        }
    }

    /// The field of a row at the indexes, one index for each level of rows
    private static Block field(Block row, int... indexes)
    {
        Block block = row;
        for (int index : indexes) {
            block = getRowFieldsFromBlock(block).get(index);
        }
        return block;
    }

    private static Metadata metadata(Block physical, int row)
    {
        return Metadata.from(VARBINARY.getSlice(field(physical, 0), row));
    }

    /// The top-level `value` of a row
    private static Variant topLevelValue(Block physical, int row)
    {
        return Variant.from(metadata(physical, row), VARBINARY.getSlice(field(physical, 1), row));
    }

    /// The `value` of an object field group or a list element group
    private static Variant groupValue(Block group, int row, Metadata metadata)
    {
        return Variant.from(metadata, VARBINARY.getSlice(field(group, 0), row));
    }

    private static Variant variantObject(Object... keysAndValues)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int index = 0; index < keysAndValues.length; index += 2) {
            fields.put((String) keysAndValues[index], keysAndValues[index + 1]);
        }
        return Variant.fromObject(fields);
    }

    private static Variant variantArray(Object... elements)
    {
        return Variant.fromObject(Arrays.asList(elements));
    }

    private static Variant longEncodedString(String value)
    {
        Slice utf8 = utf8Slice(value);
        Slice data = Slices.allocate(5 + utf8.length());
        data.setByte(0, primitiveHeader(STRING));
        data.setInt(1, utf8.length());
        data.setBytes(5, utf8);
        return Variant.from(EMPTY_METADATA, data);
    }

    private static Variant decimal4(int unscaled, int scale)
    {
        Slice data = Slices.allocate(ENCODED_DECIMAL4_SIZE);
        encodeDecimal4(unscaled, scale, data, 0);
        return Variant.from(EMPTY_METADATA, data);
    }

    private static Variant decimal8(long unscaled, int scale)
    {
        Slice data = Slices.allocate(ENCODED_DECIMAL8_SIZE);
        encodeDecimal8(unscaled, scale, data, 0);
        return Variant.from(EMPTY_METADATA, data);
    }

    private static Variant decimal16(BigInteger unscaled, int scale)
    {
        Slice data = Slices.allocate(ENCODED_DECIMAL16_SIZE);
        encodeDecimal16(Int128.valueOf(unscaled), scale, data, 0);
        return Variant.from(EMPTY_METADATA, data);
    }

    private static ShreddedValue untyped()
    {
        return new ShreddedValue(Optional.empty());
    }

    private static ShreddedValue primitive(ShreddedType type)
    {
        return new ShreddedValue(Optional.of(PrimitiveValue.of(type)));
    }

    private static ShreddedValue object(ObjectField... fields)
    {
        return new ShreddedValue(Optional.of(new ObjectValue(ImmutableList.copyOf(fields))));
    }

    private static ObjectField field(String name, ShreddedValue value)
    {
        return new ObjectField(name, value);
    }

    private static ShreddedValue array(ShreddedValue element)
    {
        return new ShreddedValue(Optional.of(new ArrayValue(element)));
    }
}
