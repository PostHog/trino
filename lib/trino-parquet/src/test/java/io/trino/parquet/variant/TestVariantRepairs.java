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
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariant;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readPhysicalColumn;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.Header.objectHeader;
import static io.trino.spi.variant.VariantUtils.writeOffset;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Repairs of unshredded VARIANT values with the defects of DuckDB 1.5.5, which the
/// Parquet reader applies to every VARIANT that it reads unshredded.
public class TestVariantRepairs
{
    // The keys of the wide object in field name order
    private static final ImmutableList<Slice> KEYS = IntStream.range(0, 70)
            .mapToObj(key -> utf8Slice("k%02d".formatted(key)))
            .collect(toImmutableList());
    private static final String DUCKDB_CREATED_BY = "DuckDB version v1.5.5 (build d8cdaa33fd)";
    private static final RowType NESTED = RowType.from(List.of(field("v", VARIANT)));
    private static final MessageType SCHEMA = Types.buildMessage()
            .optionalGroup().as(variantType(Header.VERSION))
            .required(BINARY).named("metadata")
            .required(BINARY).named("value")
            .named("v")
            .optionalGroup()
            .optionalGroup().as(variantType(Header.VERSION))
            .required(BINARY).named("metadata")
            .required(BINARY).named("value")
            .named("v")
            .named("r")
            .named("test");

    @Test
    public void testDuckDbWideObject()
            throws IOException
    {
        // The 70-key object of row 3 of wide-object-unshredded.parquet, with the bytes that DuckDB wrote in `value`
        // The reader returns one row per page, because its first pages are small
        ParquetReaderOptions onePerPage = ParquetReaderOptions.builder().withMaxReadBlockRowCount(1).build();
        List<Block> fields = getRowFieldsFromBlock(readPhysicalColumn(DUCKDB.resolve("wide-object-unshredded.parquet"), "v", onePerPage).blocks().get(2));
        Variant duckDb = Variant.from(Metadata.from(VARBINARY.getSlice(fields.get(0), 0)), VARBINARY.getSlice(fields.get(1), 0));
        // The test helper writes the same bytes as DuckDB
        Variant written = duckDbObject(KEYS.reverse(), KEYS.reverse().stream().map(key -> Variant.ofLong(Long.parseLong(key.toStringUtf8().substring(1)))).toList());
        assertThat(written.metadata().toSlice()).isEqualTo(duckDb.metadata().toSlice());
        assertThat(written.data()).isEqualTo(duckDb.data());
        // Without a repair, lookups in the object miss most keys
        assertThat(KEYS.stream().filter(key -> duckDb.getObjectField(key).isPresent()).count()).isLessThan(KEYS.size());

        Variant trinoObject = Variant.ofObject(KEYS.stream().collect(toImmutableMap(key -> key, key -> Variant.ofLong(Long.parseLong(key.toStringUtf8().substring(1))))));
        List<List<Variant>> rows = readFile(Arrays.asList(
                Arrays.asList(duckDb, duckDb),
                Arrays.asList(trinoObject, null),
                Arrays.asList(null, Variant.ofInt(1)),
                Arrays.asList(Variant.ofInt(1), duckDb)));
        for (Variant variant : List.of(rows.get(0).get(0), rows.get(0).get(1), rows.get(3).get(1))) {
            for (Slice key : KEYS) {
                assertThat(variant.getObjectField(key)).as(key.toStringUtf8()).hasValue(Variant.ofLong(Long.parseLong(key.toStringUtf8().substring(1))));
            }
            assertSameVariant(variant, trinoObject, "repaired");
            // Equality and hashing compare object fields by position
            assertThat(variant).isEqualTo(trinoObject);
            assertThat(variant.longHashCode()).isEqualTo(trinoObject.longHashCode());
            assertThat(variant.metadata().isSorted()).isTrue();
        }
        assertThat(rows.get(1)).isEqualTo(Arrays.asList(trinoObject, null));
        assertThat(rows.get(2)).isEqualTo(Arrays.asList(null, Variant.ofInt(1)));
        assertThat(rows.get(3).getFirst()).isEqualTo(Variant.ofInt(1));
    }

    @Test
    public void testOnlyDuckDbFilesAreRepaired()
            throws IOException
    {
        assertThat(VariantRepairs.writesDefects(Optional.of(DUCKDB_CREATED_BY))).isTrue();
        assertThat(VariantRepairs.writesDefects(Optional.of("parquet-mr-trino version 484"))).isFalse();
        assertThat(VariantRepairs.writesDefects(Optional.empty())).isFalse();

        // The reader trusts the files of other writers, and returns their bytes unchanged
        Variant duckDb = duckDbObject(KEYS.reverse(), KEYS.reverse().stream().map(key -> Variant.ofLong(Long.parseLong(key.toStringUtf8().substring(1)))).toList());
        Variant read = readFile(List.of(Arrays.asList(duckDb, null)), "parquet-mr version 1.16.0").getFirst().getFirst();
        assertThat(read.metadata().toSlice()).isEqualTo(duckDb.metadata().toSlice());
        assertThat(read.data()).isEqualTo(duckDb.data());
    }

    @Test
    public void testFalseSortedFlag()
            throws IOException
    {
        // The fields are in field name order, but the dictionary, which is not sorted, sets sorted_strings
        Variant object = duckDbObject(List.of(utf8Slice("b"), utf8Slice("a")), List.of(Variant.ofInt(2), Variant.ofInt(1)));
        Variant inNameOrder = Variant.from(object.metadata(), Slices.wrappedBuffer(new byte[] {
                0x02, 0x02, 0x01, 0x00, 0x00, 0x05, 0x0A,
                0x14, 0x01, 0x00, 0x00, 0x00,
                0x14, 0x02, 0x00, 0x00, 0x00,
        }));
        assertThat(inNameOrder.metadata().isSorted()).isTrue();
        Variant primitive = Variant.ofInt(1);
        assertThat(VariantRepairs.repair(primitive)).isSameAs(primitive);

        Variant repaired = readFile(List.of(Arrays.asList(inNameOrder, null))).getFirst().getFirst();
        assertThat(repaired.metadata().isSorted()).isFalse();
        assertThat(repaired.metadata().get(0)).isEqualTo(utf8Slice("b"));
        assertThat(repaired.data()).isEqualTo(inNameOrder.data());
    }

    @Test
    public void testValuesInOrderAreNotCopied()
    {
        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, 3);
        VARIANT.writeObject(builder, Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofInt(1), utf8Slice("b"), Variant.ofString("x"))));
        builder.appendNull();
        VARIANT.writeObject(builder, Variant.ofArray(ImmutableList.of(Variant.ofInt(1), Variant.ofObject(ImmutableMap.of(utf8Slice("c"), Variant.NULL_VALUE)))));
        VariantBlock block = (VariantBlock) builder.build();
        assertThat(repair(block)).isSameAs(block);
        VariantBlock region = block.getRegion(1, 2);
        assertThat(repair(region)).isSameAs(region);
    }

    @Test
    public void testRepairOfRegion()
            throws IOException
    {
        Variant unsorted = duckDbObject(List.of(utf8Slice("b"), utf8Slice("a")), List.of(Variant.ofInt(2), Variant.ofInt(1)));
        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, 4);
        VARIANT.writeObject(builder, Variant.ofInt(0));
        VARIANT.writeObject(builder, Variant.ofInt(1));
        builder.appendNull();
        VARIANT.writeObject(builder, unsorted);
        VariantBlock region = ((VariantBlock) builder.build()).getRegion(1, 3);

        VariantBlock repaired = repair(region);
        assertThat(repaired.getPositionCount()).isEqualTo(3);
        assertThat(repaired.getVariant(0)).isEqualTo(Variant.ofInt(1));
        assertThat(repaired.isNull(1)).isTrue();
        Variant object = repaired.getVariant(2);
        assertThat(object.objectFieldNames().map(Slice::toStringUtf8)).containsExactly("a", "b");
        assertThat(object).isEqualTo(Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofInt(1), utf8Slice("b"), Variant.ofInt(2))));
    }

    @Test
    public void testInvalidValues()
    {
        // Metadata ["k", "k"], and an object with a field for each entry
        Variant duplicateFields = duckDbObject(List.of(utf8Slice("k"), utf8Slice("k")), List.of(Variant.ofInt(1), Variant.ofInt(2)));
        // The test reader wraps the exception of a block that it loads lazily
        assertThatThrownBy(() -> readFile(List.of(Arrays.asList(duplicateFields, null))))
                .hasCauseInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("Invalid VARIANT: VARIANT object has duplicate field k");

        // An INT32 header without its four value bytes
        Variant truncated = Variant.from(Metadata.EMPTY_METADATA, Slices.wrappedBuffer(new byte[] {0x14}));
        assertThatThrownBy(() -> readFile(List.of(Arrays.asList(null, truncated))))
                .hasCauseInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("Invalid VARIANT: VARIANT value is truncated");
    }

    private static VariantBlock repair(VariantBlock block)
    {
        try {
            return VariantRepairs.repair(block, new ParquetDataSourceId("test"));
        }
        catch (ParquetCorruptionException e) {
            throw new AssertionError(e);
        }
    }

    /// An object as DuckDB 1.5.5 writes it: the dictionary has the keys in the order of
    /// `keys` and sets `sorted_strings`, and the fields are in field id order.
    static Variant duckDbObject(List<Slice> keys, List<Variant> values)
    {
        Slice metadata = Metadata.of(keys.stream().distinct().toList()).toSlice();
        if (keys.stream().distinct().count() != keys.size()) {
            // Metadata.of rejects duplicate names, so write the dictionary of single-character names here
            checkSingleCharacterNames(keys);
            metadata = Slices.allocate(3 + keys.size() + keys.size());
            metadata.setByte(0, metadataHeader(false, 1));
            metadata.setByte(1, keys.size());
            for (int id = 0; id <= keys.size(); id++) {
                metadata.setByte(2 + id, id);
            }
            for (int id = 0; id < keys.size(); id++) {
                metadata.setByte(3 + keys.size() + id, keys.get(id).getByte(0));
            }
        }
        metadata = metadata.copy();
        metadata.setByte(0, metadataHeader(true, metadataOffsetSize(metadata.getByte(0))));

        int dataLength = values.stream().mapToInt(value -> value.data().length()).sum();
        int offsetSize = dataLength < 0x100 ? 1 : 2;
        Slice data = Slices.allocate(2 + keys.size() + (keys.size() + 1) * offsetSize + dataLength);
        data.setByte(0, objectHeader(1, offsetSize, false));
        data.setByte(1, keys.size());
        int offsetsStart = 2 + keys.size();
        int valuesStart = offsetsStart + (keys.size() + 1) * offsetSize;
        int offset = 0;
        for (int id = 0; id < keys.size(); id++) {
            data.setByte(2 + id, id);
            writeOffset(data, offsetsStart + id * offsetSize, offset, offsetSize);
            data.setBytes(valuesStart + offset, values.get(id).data());
            offset += values.get(id).data().length();
        }
        writeOffset(data, offsetsStart + keys.size() * offsetSize, offset, offsetSize);
        return Variant.from(Metadata.from(metadata), data);
    }

    private static void checkSingleCharacterNames(List<Slice> keys)
    {
        assertThat(keys).allSatisfy(key -> assertThat(key.length()).isEqualTo(1));
    }

    /// Writes rows of a top-level VARIANT column `v` and a column `r` of `row(v variant)`
    /// with the bytes of the variants, and reads them back with the Parquet reader as a
    /// file of DuckDB.
    private static List<List<Variant>> readFile(List<List<Variant>> rows)
            throws IOException
    {
        return readFile(rows, DUCKDB_CREATED_BY);
    }

    private static List<List<Variant>> readFile(List<List<Variant>> rows, String createdBy)
            throws IOException
    {
        VariantBlockBuilder topLevel = VARIANT.createBlockBuilder(null, rows.size());
        RowBlockBuilder nested = NESTED.createBlockBuilder(null, rows.size());
        for (List<Variant> row : rows) {
            writeVariant(topLevel, row.get(0));
            nested.buildEntry(fields -> writeVariant(fields.getFirst(), row.get(1)));
        }
        Slice file = writeParquetFile(ParquetWriterOptions.builder().build(), SCHEMA, ImmutableMap.of(), ImmutableList.of(new Page(topLevel.build(), nested.build())));
        ParquetDataSource dataSource = new TestingParquetDataSource(file, ParquetReaderOptions.defaultOptions());
        FileMetaData footer = MetadataReader.readFooter(dataSource, Optional.empty()).getParquetMetadata().deepCopy();
        footer.setCreated_by(createdBy);
        ParquetMetadata metadata = new ParquetMetadata(footer, dataSource.getId(), Optional.empty());
        List<List<Variant>> result = new ArrayList<>();
        List<Type> types = ImmutableList.of(VARIANT, NESTED);
        try (ParquetReader reader = createParquetReader(dataSource, metadata, types, ImmutableList.of("v", "r"))) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                Block variants = page.getBlock(0);
                Block nestedVariants = getRowFieldsFromBlock(page.getBlock(1)).getFirst();
                for (int position = 0; position < page.getPositionCount(); position++) {
                    result.add(Arrays.asList(toVariant(variants, position), toVariant(nestedVariants, position)));
                }
            }
        }
        return result;
    }

    private static void writeVariant(BlockBuilder builder, Variant variant)
    {
        if (variant == null) {
            builder.appendNull();
        }
        else {
            VARIANT.writeObject(builder, variant);
        }
    }

    private static Variant toVariant(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return VARIANT.getObject(block, position);
    }
}
