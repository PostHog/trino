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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.PRIMITIVE;
import static io.trino.spi.variant.Header.BasicType.SHORT_STRING;
import static io.trino.spi.variant.Header.PrimitiveType.STRING;
import static io.trino.spi.variant.Header.metadataOffsetSize;
import static io.trino.spi.variant.VariantUtils.readOffset;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/// Reads shredded VARIANT columns, and compares Variant values by value and type.
final class ShreddedVariantTestUtils
{
    private ShreddedVariantTestUtils() {}

    static List<Optional<Variant>> readVariants(Path file, String column)
            throws IOException
    {
        return readPhysicalColumn(file, column, ParquetReaderOptions.defaultOptions()).variants();
    }

    static PhysicalColumn readPhysicalColumn(Path file, String column, ParquetReaderOptions options)
            throws IOException
    {
        return readPhysicalColumn(file, column, options, Optional.empty());
    }

    static PhysicalColumn readPhysicalColumn(Path file, String column, ParquetReaderOptions options, Optional<VariantPaths> paths)
            throws IOException
    {
        try (ParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), options)) {
            return readPhysicalColumn(dataSource, column, options, paths);
        }
    }

    static PhysicalColumn readPhysicalColumn(ParquetDataSource dataSource, String column, ParquetReaderOptions options)
            throws IOException
    {
        return readPhysicalColumn(dataSource, column, options, Optional.empty());
    }

    static PhysicalColumn readPhysicalColumn(ParquetDataSource dataSource, String column, ParquetReaderOptions options, Optional<VariantPaths> paths)
            throws IOException
    {
        return readPhysicalColumn(dataSource, column, options, paths, false);
    }

    static PhysicalColumn readPhysicalColumn(ParquetDataSource dataSource, String column, ParquetReaderOptions options, Optional<VariantPaths> paths, boolean topLevelVariantNullIsSqlNull)
            throws IOException
    {
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
        VariantShreddingSchema whole = parseSchema(metadata, column, dataSource);
        VariantShreddingSchema schema = paths.map(whole::prune).orElse(whole);
        ShreddedVariantAssembler assembler = new ShreddedVariantAssembler(schema, paths, topLevelVariantNullIsSqlNull, dataSource.getId());

        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        ImmutableList.Builder<Optional<Variant>> variants = ImmutableList.builder();
        try (ParquetReader reader = createParquetReader(dataSource, metadata, options, newSimpleAggregatedMemoryContext(), ImmutableList.of(schema.physicalType()), ImmutableList.of(column), TupleDomain.all())) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                Block block = page.getBlock(0);
                blocks.add(block);
                variants.addAll(toVariants(assembler.assemble(block)));
            }
        }
        return new PhysicalColumn(assembler, blocks.build(), variants.build());
    }

    static List<Optional<Variant>> toVariants(Block block)
    {
        ImmutableList.Builder<Optional<Variant>> variants = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                variants.add(Optional.empty());
            }
            else {
                variants.add(Optional.of(VARIANT.getObject(block, position)));
            }
        }
        return variants.build();
    }

    /// The blocks of a shredded VARIANT column as the Parquet reader returns them, and their variants.
    record PhysicalColumn(ShreddedVariantAssembler assembler, List<Block> blocks, List<Optional<Variant>> variants) {}

    static VariantShreddingSchema parseSchema(ParquetMetadata metadata, String column, ParquetDataSource dataSource)
            throws ParquetCorruptionException
    {
        MessageType fileSchema = metadata.getFileMetaData().getSchema();
        int index = fileSchema.getFieldIndex(column);
        ParquetOriginalFieldNames names = ParquetOriginalFieldNames.fromSchema(metadata.getParquetMetadata().getSchema()).children().get(index);
        return VariantShreddingSchema.fromParquet(fileSchema.getType(index).asGroupType(), names, dataSource.getId());
    }

    /// Reads a `.variant.bin` file, which has the variant metadata followed by the variant value.
    static Variant readVariantFile(Path file)
            throws IOException
    {
        Slice bytes = Slices.wrappedBuffer(Files.readAllBytes(file));
        byte header = bytes.getByte(0);
        int offsetSize = metadataOffsetSize(header);
        int dictionarySize = readOffset(bytes, 1, offsetSize);
        int dictionaryLength = readOffset(bytes, 1 + (dictionarySize + 1) * offsetSize, offsetSize);
        int metadataLength = 1 + (dictionarySize + 2) * offsetSize + dictionaryLength;
        Metadata metadata = Metadata.from(bytes.slice(0, metadataLength));
        return Variant.from(metadata, bytes.slice(metadataLength, bytes.length() - metadataLength));
    }

    static void assertSameVariants(List<Optional<Variant>> actual, List<Optional<Variant>> expected, String description)
    {
        assertThat(actual).as(description).hasSize(expected.size());
        for (int row = 0; row < actual.size(); row++) {
            assertThat(actual.get(row).isPresent()).as("%s row %s", description, row).isEqualTo(expected.get(row).isPresent());
            if (actual.get(row).isPresent()) {
                assertSameVariant(actual.get(row).get(), expected.get(row).get(), description + " row " + row);
            }
        }
    }

    /// Checks that two variants have the same value and the same Variant types.
    /// {@link Variant#equals} compares numbers of different types by value.
    static void assertSameVariant(Variant actual, Variant expected, String path)
    {
        assertThat(variantType(actual)).as(path).isEqualTo(variantType(expected));
        switch (actual.basicType()) {
            case OBJECT -> {
                Map<String, Variant> actualFields = objectFields(actual);
                Map<String, Variant> expectedFields = objectFields(expected);
                assertThat(actualFields.keySet()).as(path).isEqualTo(expectedFields.keySet());
                actualFields.forEach((name, value) -> assertSameVariant(value, expectedFields.get(name), path + "." + name));
            }
            case ARRAY -> {
                assertThat(actual.getArrayLength()).as(path).isEqualTo(expected.getArrayLength());
                for (int index = 0; index < actual.getArrayLength(); index++) {
                    assertSameVariant(actual.getArrayElement(index), expected.getArrayElement(index), path + "[" + index + "]");
                }
            }
            case PRIMITIVE, SHORT_STRING -> assertThat(actual.toObject()).as(path).isEqualTo(expected.toObject());
        }
    }

    static String variantType(Variant variant)
    {
        // A short string is a string with a shorter encoding
        if (variant.basicType() == SHORT_STRING || (variant.basicType() == PRIMITIVE && variant.primitiveType() == STRING)) {
            return "STRING";
        }
        if (variant.basicType() == PRIMITIVE) {
            return variant.primitiveType().name();
        }
        return variant.basicType().name();
    }

    static Map<String, Variant> objectFields(Variant variant)
    {
        return variant.objectFields()
                .collect(toImmutableMap(field -> variant.metadata().get(field.fieldId()).toStringUtf8(), ObjectFieldIdValue::value));
    }

    /// Converts the values of [Variant#toObject] and of parsed JSON to the same form.
    static Object comparable(Object value)
    {
        return switch (value) {
            case null -> null;
            case Map<?, ?> map -> {
                Map<Object, Object> result = new HashMap<>();
                map.forEach((key, entry) -> result.put(key, comparable(entry)));
                yield result;
            }
            case List<?> list -> {
                List<Object> result = new ArrayList<>();
                list.forEach(element -> result.add(comparable(element)));
                yield result;
            }
            case Number number -> new BigDecimal(number.toString()).stripTrailingZeros();
            default -> value;
        };
    }

    static String evaluate(Optional<Variant> variant, List<VariantPaths.Step> path)
    {
        return variant.map(value -> evaluate(value, path)).orElse("SQL NULL");
    }

    /// Evaluates a path like the VARIANT subscript operator: a key of a value that is
    /// not an object, or an element of a value that is not an array, is an error. An
    /// array step evaluates the rest of the path on every element, so the results of
    /// all indexes, and of indexes out of bounds, are compared.
    static String evaluate(Variant variant, List<VariantPaths.Step> path)
    {
        if (path.isEmpty()) {
            return variantType(variant) + " " + comparable(variant.toObject());
        }
        List<VariantPaths.Step> rest = path.subList(1, path.size());
        return switch (path.getFirst()) {
            case VariantPaths.Key(String name) -> {
                if (variant.basicType() != Header.BasicType.OBJECT) {
                    yield "error: " + variantType(variant) + " is not an object";
                }
                yield variant.getObjectField(utf8Slice(name))
                        .map(field -> evaluate(field, rest))
                        .orElse("missing");
            }
            case VariantPaths.ArrayElement _ -> {
                if (variant.basicType() != Header.BasicType.ARRAY) {
                    yield "error: " + variantType(variant) + " is not an array";
                }
                yield IntStream.range(0, variant.getArrayLength())
                        .mapToObj(index -> evaluate(variant.getArrayElement(index), rest))
                        .collect(joining(", ", "[", "]"));
            }
        };
    }

    static VariantPaths.Key key(String name)
    {
        return new VariantPaths.Key(name);
    }
}
