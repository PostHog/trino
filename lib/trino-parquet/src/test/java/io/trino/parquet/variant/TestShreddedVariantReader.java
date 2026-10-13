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
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.variant.ShreddedVariantTestFiles.ShreddedVariantCase;
import io.trino.parquet.variant.ShreddedVariantTestUtils.PhysicalColumn;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.ERROR;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.NO_FILES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB_FIXTURES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB_PROPERTIES_FIXTURES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.PARQUET_TESTING;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.loadParquetTestingCases;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariant;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariants;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.comparable;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.evaluate;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.key;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.objectFields;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.parseSchema;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readPhysicalColumn;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readVariantFile;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readVariants;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.toVariants;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.variantType;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.variant.Header.metadataHeader;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.nCopies;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestShreddedVariantReader
{
    // Error cases of apache/parquet-testing, with the message that this reader reports
    private static final Map<Integer, String> CORRUPTION_MESSAGES = ImmutableMap.of(
            40, "Shredded VARIANT value that is not an object has both value and typed_value",
            42, "Shredded VARIANT value that is not an object has both value and typed_value",
            87, "Shredded VARIANT object has a value that is not an object",
            128, "Shredded VARIANT object has a value that is not an object");
    private static final Map<Integer, String> NOT_SUPPORTED_MESSAGES = ImmutableMap.of(
            127, "Unsupported shredded VARIANT value type: optional int32 typed_value (INTEGER(32,false))",
            137, "Unsupported shredded VARIANT value type: optional fixed_len_byte_array(4) typed_value");

    @ParameterizedTest
    @MethodSource("parquetTestingCases")
    public void testParquetTestingCase(ShreddedVariantCase testCase)
            throws IOException
    {
        Path parquetFile = PARQUET_TESTING.resolve(testCase.parquetFile().orElseThrow());
        if (testCase.kind() == ERROR) {
            int caseNumber = testCase.caseNumber();
            if (NOT_SUPPORTED_MESSAGES.containsKey(caseNumber)) {
                assertTrinoExceptionThrownBy(() -> readVariants(parquetFile, "var"))
                        .hasErrorCode(NOT_SUPPORTED)
                        .hasMessage(NOT_SUPPORTED_MESSAGES.get(caseNumber));
            }
            else {
                assertThat(CORRUPTION_MESSAGES).as("expected message of error case %s", caseNumber).containsKey(caseNumber);
                assertThatThrownBy(() -> readVariants(parquetFile, "var"))
                        .isInstanceOf(ParquetCorruptionException.class)
                        .hasMessageContaining(CORRUPTION_MESSAGES.get(caseNumber));
            }
            return;
        }

        List<Optional<Variant>> actual = readVariants(parquetFile, "var");
        assertThat(actual).hasSize(testCase.variantFiles().size());
        for (int row = 0; row < actual.size(); row++) {
            Optional<String> variantFile = testCase.variantFiles().get(row);
            if (variantFile.isEmpty()) {
                assertThat(actual.get(row)).as("row %s", row).isEmpty();
            }
            else {
                assertThat(actual.get(row)).as("row %s", row).isPresent();
                assertSameVariant(actual.get(row).get(), readVariantFile(PARQUET_TESTING.resolve(variantFile.get())), "row " + row);
            }
        }
    }

    public static Stream<ShreddedVariantCase> parquetTestingCases()
    {
        return loadParquetTestingCases().stream()
                .filter(testCase -> testCase.kind() != NO_FILES);
    }

    @Test
    public void testDuckDbFiles()
            throws IOException
    {
        for (String fixture : DUCKDB_FIXTURES) {
            List<Optional<Variant>> actual = readVariants(DUCKDB.resolve(fixture + ".parquet"), "v");
            List<?> expected = Files.readAllLines(DUCKDB.resolve(fixture + ".duckdb.jsonl")).stream()
                    .map(line -> parseJson(line, Map.class).get("v"))
                    .toList();
            assertThat(actual).as(fixture).hasSize(expected.size());
            for (int row = 0; row < actual.size(); row++) {
                // DuckDB writes SQL NULL as variant null, so every row has a variant
                assertThat(actual.get(row)).as("%s row %s", fixture, row).isPresent();
                assertThat(comparable(actual.get(row).get().toObject()))
                        .as("%s row %s", fixture, row)
                        .isEqualTo(comparable(expected.get(row)));
            }
        }
    }

    @Test
    public void testDuckDbSqlNull()
            throws IOException
    {
        // Row 1 is a SQL NULL in DuckDB, and the file has a variant null for it
        assertThat(readVariants(DUCKDB.resolve("nulls.parquet"), "v").getFirst())
                .hasValueSatisfying(variant -> assertThat(variant.isNull()).isTrue());
    }

    @Test
    public void testDuckDbWideObjects()
            throws IOException
    {
        // DuckDB marks the 70-key dictionary as sorted, but writes it in descending order,
        // and writes the object fields in the same order. Lookups in objects with more
        // than 64 fields assume field name order, and find only some of the keys.
        List<Slice> keys = IntStream.range(0, 70)
                .mapToObj(key -> utf8Slice("k%02d".formatted(key)))
                .collect(toImmutableList());
        Variant shredded = readVariants(DUCKDB.resolve("wide-object.parquet"), "v").getFirst().orElseThrow();
        Variant unshredded = readVariants(DUCKDB.resolve("wide-object-unshredded.parquet"), "v").get(2).orElseThrow();
        for (Slice key : keys) {
            assertThat(shredded.getObjectField(key)).as("shredded %s", key.toStringUtf8()).isPresent();
            assertThat(unshredded.getObjectField(key)).as("unshredded %s", key.toStringUtf8()).isPresent();
        }
    }

    @Test
    public void testWithSortedObjectFields()
    {
        Variant sorted = Variant.ofObject(ImmutableMap.of(utf8Slice("a"), Variant.ofInt(1), utf8Slice("b"), Variant.ofInt(2)));
        assertThat(VariantRepairs.withSortedObjectFields(sorted)).isSameAs(sorted);

        // Metadata ["b", "a"], and an object that lists field 0 ("b") before field 1 ("a")
        Metadata metadata = Metadata.of(ImmutableList.of(utf8Slice("b"), utf8Slice("a")));
        Slice data = Slices.wrappedBuffer(new byte[] {
                0x02, 0x02, 0x00, 0x01, 0x00, 0x05, 0x0A,
                0x14, 0x02, 0x00, 0x00, 0x00,
                0x14, 0x01, 0x00, 0x00, 0x00,
        });
        Variant unsorted = Variant.from(metadata, data);
        assertThat(unsorted.objectFieldNames().map(Slice::toStringUtf8)).containsExactly("b", "a");

        Variant repaired = VariantRepairs.withSortedObjectFields(Variant.ofArray(ImmutableList.of(unsorted)));
        Variant object = repaired.getArrayElement(0);
        assertThat(object.objectFieldNames().map(Slice::toStringUtf8)).containsExactly("a", "b");
        assertThat(object.getObjectField(utf8Slice("a")).orElseThrow().getInt()).isEqualTo(1);
        assertThat(object.getObjectField(utf8Slice("b")).orElseThrow().getInt()).isEqualTo(2);

        // Metadata ["k", "k"], as DuckDB writes it, and an object with a field for each entry
        Metadata duplicateMetadata = Metadata.from(Slices.wrappedBuffer(new byte[] {0x01, 0x02, 0x00, 0x01, 0x02, 'k', 'k'}));
        Variant duplicateFields = Variant.from(duplicateMetadata, data);
        assertThatThrownBy(() -> VariantRepairs.withSortedObjectFields(duplicateFields))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("VARIANT object has duplicate field k");
    }

    @Test
    public void testWithVerifiedSortedFlag()
    {
        Metadata sorted = Metadata.of(ImmutableList.of(utf8Slice("a"), utf8Slice("b")));
        assertThat(sorted.isSorted()).isTrue();
        assertThat(VariantRepairs.withVerifiedSortedFlag(sorted)).isSameAs(sorted);

        Metadata unsorted = Metadata.of(ImmutableList.of(utf8Slice("b"), utf8Slice("a")));
        assertThat(unsorted.isSorted()).isFalse();
        assertThat(VariantRepairs.withVerifiedSortedFlag(unsorted)).isSameAs(unsorted);

        Slice falselySortedSlice = unsorted.toSlice().copy();
        falselySortedSlice.setByte(0, metadataHeader(true, metadataOffsetSize(falselySortedSlice.getByte(0))));
        Metadata falselySorted = Metadata.from(falselySortedSlice);
        assertThat(falselySorted.isSorted()).isTrue();
        Metadata verified = VariantRepairs.withVerifiedSortedFlag(falselySorted);
        assertThat(verified.isSorted()).isFalse();
        assertThat(verified.get(0)).isEqualTo(utf8Slice("b"));
        assertThat(verified.get(1)).isEqualTo(utf8Slice("a"));
    }

    @Test
    public void testFieldNamesThatDifferOnlyByCase()
            throws IOException
    {
        // The Parquet metadata has lowercase field names, so the reader cannot tell these fields apart
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                .required(BINARY).named("metadata")
                .optional(BINARY).named("value")
                .optionalGroup()
                .requiredGroup().optional(BINARY).named("value").optional(BINARY).as(stringType()).named("typed_value").named("Plan")
                .requiredGroup().optional(BINARY).named("value").optional(BINARY).as(stringType()).named("typed_value").named("plan")
                .named("typed_value")
                .named("v")
                .named("test");
        Map<List<String>, Type> primitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(ImmutableList.of("v", "metadata"), VARBINARY)
                .put(ImmutableList.of("v", "value"), VARBINARY)
                .put(ImmutableList.of("v", "typed_value", "Plan", "value"), VARBINARY)
                .put(ImmutableList.of("v", "typed_value", "Plan", "typed_value"), VARCHAR)
                .put(ImmutableList.of("v", "typed_value", "plan", "value"), VARBINARY)
                .put(ImmutableList.of("v", "typed_value", "plan", "typed_value"), VARCHAR)
                .buildOrThrow();
        Slice file = writeParquetFile(ParquetWriterOptions.builder().build(), schema, primitiveTypes, ImmutableList.of());

        ParquetDataSource dataSource = new TestingParquetDataSource(file, ParquetReaderOptions.defaultOptions());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertTrinoExceptionThrownBy(() -> parseSchema(metadata, "v", dataSource))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Shredded VARIANT object fields that differ only by case are not supported: plan in v.typed_value");
    }

    @Test
    public void testRepeatedVariantGroup()
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .repeatedGroup()
                .required(BINARY).named("metadata")
                .optional(BINARY).named("value")
                .named("v")
                .named("test");
        assertSchemaError(schema, ImmutableMap.of(ImmutableList.of("v", "metadata"), VARBINARY, ImmutableList.of("v", "value"), VARBINARY), "Shredded VARIANT group v is repeated");
    }

    @Test
    public void testTwoLevelListLayout()
            throws IOException
    {
        // The Parquet reader reads a repeated group named "array" as the element of a two-level list
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                .required(BINARY).named("metadata")
                .optional(BINARY).named("value")
                .optionalGroup().as(listType())
                .repeatedGroup()
                .requiredGroup().optional(BINARY).named("value").optional(BINARY).as(stringType()).named("typed_value").named("element")
                .named("array")
                .named("typed_value")
                .named("v")
                .named("test");
        Map<List<String>, Type> primitiveTypes = ImmutableMap.of(
                ImmutableList.of("v", "metadata"), VARBINARY,
                ImmutableList.of("v", "value"), VARBINARY,
                ImmutableList.of("v", "typed_value", "array", "element", "value"), VARBINARY,
                ImmutableList.of("v", "typed_value", "array", "element", "typed_value"), VARCHAR);
        assertSchemaError(schema, primitiveTypes, "Shredded VARIANT list v.typed_value does not have the three-level list structure");
    }

    @Test
    public void testSmallPages()
            throws IOException
    {
        // One row in each page
        ParquetReaderOptions options = ParquetReaderOptions.builder().withMaxReadBlockRowCount(1).build();
        List<Path> files = ImmutableList.<Path>builder()
                .add(PARQUET_TESTING.resolve("case-045.parquet"))
                .addAll(DUCKDB_FIXTURES.stream().map(fixture -> DUCKDB.resolve(fixture + ".parquet")).iterator())
                .build();
        for (Path file : files) {
            String column = file.startsWith(DUCKDB) ? "v" : "var";
            PhysicalColumn smallPages = readPhysicalColumn(file, column, options);
            assertThat(smallPages.blocks()).as(file.toString()).allSatisfy(block -> assertThat(block.getPositionCount()).isEqualTo(1));
            assertSameVariants(smallPages.variants(), readVariants(file, column), file.toString());
        }
    }

    @Test
    public void testDictionaryAndRunLengthEncodedBlocks()
            throws IOException
    {
        PhysicalColumn column = readPhysicalColumn(DUCKDB.resolve("objects.parquet"), "v", ParquetReaderOptions.defaultOptions());
        assertThat(column.blocks()).hasSizeGreaterThan(1);
        for (Block block : column.blocks()) {
            List<Optional<Variant>> expected = toVariants(column.assembler().assemble(block));
            int positionCount = block.getPositionCount();

            int[] ids = IntStream.range(0, positionCount).map(position -> positionCount - 1 - position).toArray();
            List<Optional<Variant>> dictionary = toVariants(column.assembler().assemble(DictionaryBlock.create(positionCount, block, ids)));
            assertSameVariants(dictionary, IntStream.of(ids).mapToObj(expected::get).collect(toImmutableList()), "dictionary");

            List<Optional<Variant>> runLengthEncoded = toVariants(column.assembler().assemble(RunLengthEncodedBlock.create(block.getSingleValueBlock(0), 3)));
            assertSameVariants(runLengthEncoded, nCopies(3, expected.getFirst()), "run length encoded");
        }
    }

    @Test
    public void testEmptyKeyAndWideEmptyDictionary()
            throws IOException
    {
        // Metadata [""] has two equal offsets. The empty dictionary uses two-byte offsets.
        ParquetDataSource dataSource = writeUnshreddedVariants(
                ImmutableList.of(new byte[] {0x01, 0x01, 0x00, 0x00}, new byte[] {0x41, 0x00, 0x00, 0x00, 0x00}),
                ImmutableList.of(new byte[] {0x02, 0x01, 0x00, 0x00, 0x05, 0x14, 0x01, 0x00, 0x00, 0x00}, new byte[] {0x02, 0x00, 0x00}));
        List<Optional<Variant>> variants = readPhysicalColumn(dataSource, "v", ParquetReaderOptions.defaultOptions()).variants();
        assertThat(variants.get(0).orElseThrow().getObjectField(utf8Slice("")).orElseThrow().getInt()).isEqualTo(1);
        assertThat(variants.get(1).orElseThrow().getObjectFieldCount()).isEqualTo(0);
    }

    @Test
    public void testTruncatedValue()
            throws IOException
    {
        // An INT32 header without its four value bytes
        ParquetDataSource dataSource = writeUnshreddedVariants(
                ImmutableList.of(new byte[] {0x01, 0x00, 0x00}),
                ImmutableList.of(new byte[] {0x14}));
        assertThatThrownBy(() -> readPhysicalColumn(dataSource, "v", ParquetReaderOptions.defaultOptions()))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("VARIANT value is truncated");

        // The same as the element of an array, and as the field of an object, whose offsets leave it one value byte
        ParquetDataSource arrayElement = writeUnshreddedVariants(
                ImmutableList.of(new byte[] {0x01, 0x00, 0x00}),
                ImmutableList.of(new byte[] {0x03, 0x01, 0x00, 0x02, 0x14, 0x00}));
        assertThatThrownBy(() -> readPhysicalColumn(arrayElement, "v", ParquetReaderOptions.defaultOptions()))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("VARIANT value is truncated");
        ParquetDataSource objectField = writeUnshreddedVariants(
                ImmutableList.of(new byte[] {0x01, 0x01, 0x00, 0x01, 'a'}),
                ImmutableList.of(new byte[] {0x02, 0x01, 0x00, 0x00, 0x02, 0x14, 0x00}));
        assertThatThrownBy(() -> readPhysicalColumn(objectField, "v", ParquetReaderOptions.defaultOptions()))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("VARIANT value is truncated");
    }

    @Test
    public void testPrunedLookupValidatesDictionary()
            throws IOException
    {
        // Dictionary ["a", "b", "c"] with offsets 0, 2, 1, 3, which decrease, and an object whose field 1 is 5
        ParquetDataSource dataSource = writeUnshreddedVariants(
                ImmutableList.of(new byte[] {0x01, 0x03, 0x00, 0x02, 0x01, 0x03, 'a', 'b', 'c'}),
                ImmutableList.of(new byte[] {0x02, 0x01, 0x01, 0x00, 0x05, 0x14, 0x05, 0x00, 0x00, 0x00}));
        assertThatThrownBy(() -> readPhysicalColumn(dataSource, "v", ParquetReaderOptions.defaultOptions()))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("Dictionary offsets must not decrease");
        // A lookup reads the dictionary in place, and checks it like a whole read
        assertThatThrownBy(() -> readPhysicalColumn(dataSource, "v", ParquetReaderOptions.defaultOptions(), Optional.of(VariantPaths.of(List.of(List.of(key("b")))))))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("Dictionary offsets must not decrease");
    }

    /// Writes a VARIANT group with only `metadata` and `value` columns, which the
    /// shredded reader also reads.
    private static ParquetDataSource writeUnshreddedVariants(List<byte[]> metadata, List<byte[]> values)
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                .required(BINARY).named("metadata")
                .optional(BINARY).named("value")
                .named("v")
                .named("test");
        BlockBuilder metadataBuilder = VARBINARY.createBlockBuilder(null, metadata.size());
        BlockBuilder valueBuilder = VARBINARY.createBlockBuilder(null, values.size());
        for (int row = 0; row < metadata.size(); row++) {
            VARBINARY.writeSlice(metadataBuilder, Slices.wrappedBuffer(metadata.get(row)));
            VARBINARY.writeSlice(valueBuilder, Slices.wrappedBuffer(values.get(row)));
        }
        Block variants = RowBlock.fromFieldBlocks(metadata.size(), new Block[] {metadataBuilder.build(), valueBuilder.build()});
        Slice file = writeParquetFile(
                ParquetWriterOptions.builder().build(),
                schema,
                ImmutableMap.of(ImmutableList.of("v", "metadata"), VARBINARY, ImmutableList.of("v", "value"), VARBINARY),
                ImmutableList.of(new Page(variants)));
        return new TestingParquetDataSource(file, ParquetReaderOptions.defaultOptions());
    }

    @Test
    public void testAssembleWithLimits()
            throws IOException
    {
        PhysicalColumn column = readPhysicalColumn(DUCKDB.resolve("objects.parquet"), "v", ParquetReaderOptions.defaultOptions());
        for (Block block : column.blocks()) {
            List<Optional<Variant>> expected = toVariants(column.assembler().assemble(block));
            int positionCount = block.getPositionCount();

            // Each value reaches a size limit of one byte, so each call assembles one position
            ImmutableList.Builder<Optional<Variant>> oneAtATime = ImmutableList.builder();
            for (int start = 0; start < positionCount; start++) {
                Block variants = column.assembler().assemble(block, start, positionCount - start, 1);
                assertThat(variants.getPositionCount()).isEqualTo(1);
                oneAtATime.addAll(toVariants(variants));
            }
            assertSameVariants(oneAtATime.build(), expected, "size limit");

            int limit = Math.min(2, positionCount);
            assertSameVariants(toVariants(column.assembler().assemble(block, 0, limit, Long.MAX_VALUE)), expected.subList(0, limit), "position limit");
            assertThat(column.assembler().assemble(block, positionCount, 0, 1).getPositionCount()).isEqualTo(0);
        }
    }

    @Test
    public void testPrune()
            throws IOException
    {
        assertPrunedPaths(DUCKDB.resolve("objects.parquet"), List.of(List.of(key("address"), key("city")), List.of(key("age")), List.of(key("missing"))));
        assertPrunedPaths(DUCKDB.resolve("objects.parquet"), List.of(List.of(key("tags"), new VariantPaths.ArrayElement())));
        assertPrunedPaths(DUCKDB.resolve("objects.parquet"), List.of(List.of(key("address")), List.of(key("address"), key("city"))));
        assertPrunedPaths(DUCKDB.resolve("arrays.parquet"), List.of(List.of(new VariantPaths.ArrayElement(), key("k"))));
        assertPrunedPaths(DUCKDB.resolve("nulls.parquet"), List.of(List.of(key("a"), key("c"))));
        assertPrunedPaths(DUCKDB.resolve("case-variant-keys.parquet"), List.of(List.of(key("props"), key("Plan")), List.of(key("$os"))));

        // Only the shredded fields of the paths remain
        try (ParquetDataSource dataSource = new FileParquetDataSource(DUCKDB.resolve("objects.parquet").toFile(), ParquetReaderOptions.defaultOptions())) {
            VariantShreddingSchema schema = parseSchema(MetadataReader.readFooter(dataSource, Optional.empty()), "v", dataSource);
            VariantShreddingSchema pruned = schema.prune(VariantPaths.of(List.of(List.of(key("address"), key("city")), List.of(key("age")))));
            VariantShreddingSchema.ObjectValue object = (VariantShreddingSchema.ObjectValue) pruned.value().typedValue().orElseThrow();
            assertThat(object.fields()).extracting(VariantShreddingSchema.ObjectField::name).containsExactlyInAnyOrder("address", "age");
            VariantShreddingSchema.ObjectValue address = (VariantShreddingSchema.ObjectValue) object.fields().stream()
                    .filter(field -> field.name().equals("address"))
                    .findFirst().orElseThrow()
                    .value().typedValue().orElseThrow();
            assertThat(address.fields()).extracting(VariantShreddingSchema.ObjectField::name).containsExactly("city");

            // A path that reads a field whole keeps all of its columns, even with a longer path below it
            VariantShreddingSchema.ObjectValue prefix = (VariantShreddingSchema.ObjectValue) schema.prune(VariantPaths.of(List.of(List.of(key("address")), List.of(key("address"), key("city")))))
                    .value().typedValue().orElseThrow();
            assertThat(prefix.fields()).containsExactly(((VariantShreddingSchema.ObjectValue) schema.value().typedValue().orElseThrow()).fields().stream()
                    .filter(field -> field.name().equals("address"))
                    .findFirst().orElseThrow());

            // An object keeps one field, so the reader can tell whether a value is an object. The first
            // field, address, is an object, so the field kept is one of a fixed-width primitive type.
            VariantShreddingSchema.ObjectValue anchor = (VariantShreddingSchema.ObjectValue) schema.prune(VariantPaths.of(List.of(List.of(key("missing")))))
                    .value().typedValue().orElseThrow();
            assertThat(anchor.fields()).extracting(VariantShreddingSchema.ObjectField::name).containsExactly("score");
        }
    }

    /// Checks that each path reads the same result, or fails the same way, from the
    /// pruned value as from the whole value.
    private static void assertPrunedPaths(Path file, List<List<VariantPaths.Step>> paths)
            throws IOException
    {
        assertPrunedPaths(file, "v", readVariants(file, "v"), paths);
    }

    private static void assertPrunedPaths(Path file, String column, List<Optional<Variant>> whole, List<List<VariantPaths.Step>> paths)
            throws IOException
    {
        VariantPaths tree = VariantPaths.of(paths);
        List<Optional<Variant>> pruned = readPhysicalColumn(file, column, ParquetReaderOptions.defaultOptions(), Optional.of(tree)).variants();
        assertThat(pruned).hasSameSizeAs(whole);
        for (int row = 0; row < whole.size(); row++) {
            assertThat(pruned.get(row).isPresent()).as("%s row %s paths %s", file.getFileName(), row, paths).isEqualTo(whole.get(row).isPresent());
            for (List<VariantPaths.Step> path : paths) {
                assertThat(evaluate(pruned.get(row), path))
                        .as("%s row %s path %s", file.getFileName(), row, path)
                        .isEqualTo(evaluate(whole.get(row), path));
            }
            if (pruned.get(row).isPresent()) {
                assertOnlyPaths(pruned.get(row).get(), whole.get(row).orElseThrow(), tree, "%s row %s paths %s".formatted(file.getFileName(), row, paths));
            }
        }
    }

    /// Checks that the objects on the paths have only the keys that the paths name, that
    /// other values keep their type, and that arrays keep their length.
    private static void assertOnlyPaths(Variant pruned, Variant whole, VariantPaths paths, String description)
    {
        assertThat(variantType(pruned)).as(description).isEqualTo(variantType(whole));
        if (paths.whole()) {
            assertSameVariant(pruned, whole, description);
            return;
        }
        switch (pruned.basicType()) {
            case OBJECT -> {
                Map<String, Variant> prunedFields = objectFields(pruned);
                Map<String, Variant> wholeFields = objectFields(whole);
                assertThat(prunedFields.keySet()).as(description).isEqualTo(Sets.intersection(wholeFields.keySet(), paths.keys().keySet()));
                prunedFields.forEach((name, value) -> assertOnlyPaths(value, wholeFields.get(name), paths.keys().get(name), description + "." + name));
            }
            case ARRAY -> {
                assertThat(pruned.getArrayLength()).as(description).isEqualTo(whole.getArrayLength());
                for (int index = 0; index < pruned.getArrayLength(); index++) {
                    if (paths.elements().isPresent()) {
                        assertOnlyPaths(pruned.getArrayElement(index), whole.getArrayElement(index), paths.elements().get(), description + "[" + index + "]");
                    }
                    else {
                        assertThat(pruned.getArrayElement(index).isNull()).as(description).isTrue();
                    }
                }
            }
            case PRIMITIVE, SHORT_STRING -> assertSameVariant(pruned, whole, description);
        }
    }

    @ParameterizedTest
    @MethodSource("prunedFiles")
    public void testPrunedPaths(Path file, String column)
            throws IOException
    {
        List<Optional<Variant>> whole = readVariants(file, column);
        for (List<List<VariantPaths.Step>> paths : generatedPaths(whole)) {
            assertPrunedPaths(file, column, whole, paths);
        }
    }

    public static Stream<Arguments> prunedFiles()
    {
        Stream<Arguments> parquetTesting = loadParquetTestingCases().stream()
                .filter(testCase -> testCase.kind() != NO_FILES && testCase.kind() != ERROR)
                .map(testCase -> Arguments.of(PARQUET_TESTING.resolve(testCase.parquetFile().orElseThrow()), "var"));
        Stream<Arguments> duckDb = Stream.concat(DUCKDB_FIXTURES.stream(), DUCKDB_PROPERTIES_FIXTURES.stream())
                .map(fixture -> Arguments.of(DUCKDB.resolve(fixture + ".parquet"), "v"));
        return Stream.concat(parquetTesting, duckDb);
    }

    /// Sets of paths of depth one and two from the keys of the values, with array
    /// elements, a key that no value has, and paths that read a node whole and below it.
    private static List<List<List<VariantPaths.Step>>> generatedPaths(List<Optional<Variant>> values)
    {
        Set<List<VariantPaths.Step>> firstLevel = new LinkedHashSet<>();
        Set<List<VariantPaths.Step>> secondLevel = new LinkedHashSet<>();
        for (Optional<Variant> value : values) {
            value.ifPresent(variant -> collectPaths(variant, ImmutableList.of(), firstLevel, secondLevel));
        }
        firstLevel.add(ImmutableList.of(key("absent")));
        ImmutableList.Builder<List<List<VariantPaths.Step>>> paths = ImmutableList.builder();
        firstLevel.forEach(path -> paths.add(ImmutableList.of(path)));
        secondLevel.forEach(path -> {
            paths.add(ImmutableList.of(path));
            // The first step whole, and a path below it
            paths.add(ImmutableList.of(path.subList(0, 1), path));
        });
        paths.add(ImmutableList.copyOf(firstLevel));
        if (!secondLevel.isEmpty()) {
            paths.add(ImmutableList.copyOf(secondLevel));
            paths.add(ImmutableList.<List<VariantPaths.Step>>builder().addAll(secondLevel).add(ImmutableList.of(key("absent"))).build());
        }
        return paths.build();
    }

    private static void collectPaths(Variant variant, List<VariantPaths.Step> prefix, Set<List<VariantPaths.Step>> firstLevel, Set<List<VariantPaths.Step>> secondLevel)
    {
        Set<List<VariantPaths.Step>> paths = prefix.isEmpty() ? firstLevel : secondLevel;
        switch (variant.basicType()) {
            case OBJECT -> objectFields(variant).forEach((name, value) -> {
                List<VariantPaths.Step> path = ImmutableList.<VariantPaths.Step>builder().addAll(prefix).add(key(name)).build();
                paths.add(path);
                if (prefix.isEmpty()) {
                    paths.add(ImmutableList.<VariantPaths.Step>builder().addAll(prefix).add(key(name), key("absent")).build());
                    collectPaths(value, path, firstLevel, secondLevel);
                }
            });
            case ARRAY -> {
                List<VariantPaths.Step> path = ImmutableList.<VariantPaths.Step>builder().addAll(prefix).add(new VariantPaths.ArrayElement()).build();
                paths.add(path);
                if (prefix.isEmpty()) {
                    for (int index = 0; index < variant.getArrayLength(); index++) {
                        collectPaths(variant.getArrayElement(index), path, firstLevel, secondLevel);
                    }
                }
            }
            case PRIMITIVE, SHORT_STRING -> {}
        }
    }

    private static void assertSchemaError(MessageType schema, Map<List<String>, Type> primitiveTypes, String message)
            throws IOException
    {
        Slice file = writeParquetFile(ParquetWriterOptions.builder().build(), schema, primitiveTypes, ImmutableList.of());
        ParquetDataSource dataSource = new TestingParquetDataSource(file, ParquetReaderOptions.defaultOptions());
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThatThrownBy(() -> parseSchema(metadata, "v", dataSource))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining(message);
    }
}
