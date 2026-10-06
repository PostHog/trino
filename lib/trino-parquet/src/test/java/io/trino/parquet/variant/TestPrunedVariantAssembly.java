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
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.variant.ShreddedVariantTestUtils.PhysicalColumn;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectField;
import io.trino.parquet.variant.VariantShreddingSchema.ObjectValue;
import io.trino.parquet.variant.VariantShreddingSchema.PrimitiveValue;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedType;
import io.trino.parquet.variant.VariantShreddingSchema.ShreddedValue;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.variant.Variant;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.assertSameVariant;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.evaluate;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.key;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readPhysicalColumn;
import static io.trino.parquet.variant.ShreddedVariantTestUtils.readVariants;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.variant.Header.BasicType.OBJECT;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/// Pruned reads of `properties-shape.parquet` and `properties-shape-mixed.parquet`,
/// which DuckDB 1.5.5 wrote with object fields in field id order and a false
/// `sorted_strings` flag, and of the same values written by Trino in the order of the
/// specification. See `generate.sql` for the values.
public class TestPrunedVariantAssembly
{
    private static final ParquetReaderOptions OPTIONS = ParquetReaderOptions.defaultOptions();
    private static final Path SHAPE = DUCKDB.resolve("properties-shape.parquet");
    private static final Path MIXED = DUCKDB.resolve("properties-shape-mixed.parquet");
    private static final List<String> BROWSERS = ImmutableList.of("Chrome", "Firefox", "Safari", "Microsoft Edge", "Opera");
    private static final VariantShreddingSchema BROWSER_SCHEMA = VariantShreddingSchema.of(new ShreddedValue(Optional.of(new ObjectValue(ImmutableList.of(
            new ObjectField("$browser", new ShreddedValue(Optional.of(PrimitiveValue.of(ShreddedType.STRING)))))))));
    private static final List<List<List<VariantPaths.Step>>> PATHS = ImmutableList.of(
            ImmutableList.of(ImmutableList.of(key("$browser"))),
            ImmutableList.of(ImmutableList.of(key("$browser"), key("name"))),
            ImmutableList.of(ImmutableList.of(key("k05")), ImmutableList.of(key("k69"))),
            ImmutableList.of(ImmutableList.of(key("$browser")), ImmutableList.of(key("$os")), ImmutableList.of(key("absent"))),
            ImmutableList.of(ImmutableList.of(key("$set"), key("plan"))),
            ImmutableList.of(ImmutableList.of(key("$set")), ImmutableList.of(key("$set"), key("seats"))),
            ImmutableList.of(ImmutableList.of(key("tags"), new VariantPaths.ArrayElement())),
            ImmutableList.of(ImmutableList.of(key("tags"))),
            ImmutableList.of(ImmutableList.of(key("wide"), key("w05"))),
            ImmutableList.of(ImmutableList.of(key("wide"))),
            ImmutableList.of(ImmutableList.of(new VariantPaths.ArrayElement(), key("$browser"))),
            ImmutableList.of(ImmutableList.of(key("$browser")), ImmutableList.of(new VariantPaths.ArrayElement())));

    @Test
    public void testValues()
            throws IOException
    {
        List<Optional<Variant>> shape = readVariants(SHAPE, "v");
        assertThat(shape).hasSize(2100);
        for (int id = 0; id < shape.size(); id++) {
            Variant row = shape.get(id).orElseThrow();
            assertThat(row.getObjectFieldCount()).isEqualTo(id % 10 == 0 ? 76 : 77);
            assertThat(row.getObjectField(utf8Slice("$browser")).map(Variant::toObject).orElse(null)).isEqualTo(id % 10 == 0 ? null : BROWSERS.get(id % 5));
            assertThat(row.getObjectField(utf8Slice("k05")).orElseThrow().getLong()).isEqualTo(5000 + id);
            assertThat(row.getObjectField(utf8Slice("k69")).orElseThrow().getLong()).isEqualTo(69000 + id);
        }

        List<Optional<Variant>> mixed = readVariants(MIXED, "v");
        assertThat(mixed).hasSize(2100);
        // DuckDB writes a SQL NULL as a variant null
        assertThat(mixed.get(0).orElseThrow().isNull()).isTrue();
        assertThat(mixed.get(1).orElseThrow().isNull()).isTrue();
        assertThat(mixed.get(2).orElseThrow().toObject()).isEqualTo("a string");
        assertThat(mixed.get(6).orElseThrow().getObjectField(utf8Slice("$browser")).orElseThrow().getLong()).isEqualTo(7);
        assertSameVariant(mixed.get(9).orElseThrow().getObjectField(utf8Slice("wide")).orElseThrow(), wideObject(9), "wide");
    }

    @Test
    public void testPrunedReadsInBothEncodings()
            throws IOException
    {
        for (Path file : List.of(SHAPE, MIXED)) {
            List<Optional<Variant>> whole = readVariants(file, "v");
            // The same values, written by Trino in the order of the specification
            Slice trinoFile = write(BROWSER_SCHEMA, whole);
            for (List<List<VariantPaths.Step>> paths : PATHS) {
                List<Optional<Variant>> duckDb = readPruned(new FileParquetDataSource(file.toFile(), OPTIONS), paths, false).variants();
                List<Optional<Variant>> trino = readPruned(new TestingParquetDataSource(trinoFile, OPTIONS), paths, false).variants();
                for (int row = 0; row < whole.size(); row++) {
                    for (List<VariantPaths.Step> path : paths) {
                        String expected = evaluate(whole.get(row), path);
                        assertThat(evaluate(duckDb.get(row), path)).as("%s row %s path %s", file.getFileName(), row, path).isEqualTo(expected);
                        assertThat(evaluate(trino.get(row), path)).as("Trino copy of %s row %s path %s", file.getFileName(), row, path).isEqualTo(expected);
                    }
                }
            }
        }
    }

    @Test
    public void testPrunedLookupOutOfOrder()
            throws IOException
    {
        // The keys are in the partially shredded object, whose fields DuckDB writes out of
        // field name order, and they are found without a repair or new metadata
        PhysicalColumn column = readPruned(new FileParquetDataSource(SHAPE.toFile(), OPTIONS), ImmutableList.of(ImmutableList.of(key("k05")), ImmutableList.of(key("k69"))), false);
        assertThat(column.assembler().rowsWithNewMetadata()).isZero();
        for (int id = 0; id < column.variants().size(); id++) {
            Variant row = column.variants().get(id).orElseThrow();
            assertThat(row.getObjectFieldCount()).isEqualTo(2);
            assertThat(row.getObjectField(utf8Slice("k05")).orElseThrow().getLong()).isEqualTo(5000 + id);
            assertThat(row.getObjectField(utf8Slice("k69")).orElseThrow().getLong()).isEqualTo(69000 + id);
            assertThat(row.metadata().isSorted()).isTrue();
            assertThat(row.metadata().dictionarySize()).isEqualTo(2);
        }
    }

    @Test
    public void testPrunedTopLevelVariantNullIsSqlNull()
            throws IOException
    {
        List<List<VariantPaths.Step>> paths = ImmutableList.of(ImmutableList.of(key("$browser")));
        List<Optional<Variant>> sqlNulls = readPruned(new FileParquetDataSource(MIXED.toFile(), OPTIONS), paths, true).variants();
        List<Optional<Variant>> variantNulls = readPruned(new FileParquetDataSource(MIXED.toFile(), OPTIONS), paths, false).variants();
        for (int id = 0; id < sqlNulls.size(); id++) {
            // DuckDB writes a SQL NULL (row 0) and a JSON null (row 1) as a variant null
            boolean topLevelNull = id % 13 == 0 || id % 13 == 1;
            assertThat(sqlNulls.get(id).isEmpty()).as("row %s", id).isEqualTo(topLevelNull);
            assertThat(variantNulls.get(id).orElseThrow().isNull()).as("row %s", id).isEqualTo(topLevelNull);
        }
        // A field with a variant null stays
        assertThat(sqlNulls.get(7).orElseThrow().getObjectField(utf8Slice("$browser")).orElseThrow().isNull()).isTrue();
    }

    @Test
    public void testPrunedWholeObjectOfDuckDb()
            throws IOException
    {
        // An object of 70 keys that DuckDB wrote out of field name order, read whole
        PhysicalColumn column = readPruned(new FileParquetDataSource(MIXED.toFile(), OPTIONS), ImmutableList.of(ImmutableList.of(key("wide"))), false);
        int rowsWithWideObject = 0;
        for (int id = 0; id < column.variants().size(); id++) {
            Optional<Variant> wide = column.variants().get(id)
                    .filter(row -> row.basicType() == OBJECT)
                    .flatMap(row -> row.getObjectField(utf8Slice("wide")));
            assertThat(wide.isPresent()).as("row %s", id).isEqualTo(id % 13 == 9);
            if (wide.isPresent()) {
                rowsWithWideObject++;
                Variant expected = wideObject(id);
                for (int key = 0; key < 70; key++) {
                    assertThat(wide.get().getObjectField(utf8Slice("w%02d".formatted(key)))).hasValue(Variant.ofLong(key + id));
                }
                assertThat(wide.get()).isEqualTo(expected);
                assertThat(wide.get().longHashCode()).isEqualTo(expected.longHashCode());
            }
        }
        assertThat(rowsWithWideObject).isEqualTo(161);
        assertThat(column.assembler().rowsWithNewMetadata()).isEqualTo(rowsWithWideObject);
    }

    private static Variant wideObject(int id)
    {
        Map<Slice, Variant> fields = new HashMap<>();
        for (int key = 0; key < 70; key++) {
            fields.put(utf8Slice("w%02d".formatted(key)), Variant.ofLong(key + id));
        }
        return Variant.ofObject(fields);
    }

    private static PhysicalColumn readPruned(ParquetDataSource dataSource, List<List<VariantPaths.Step>> paths, boolean topLevelVariantNullIsSqlNull)
            throws IOException
    {
        try (dataSource) {
            return readPhysicalColumn(dataSource, "v", OPTIONS, Optional.of(VariantPaths.of(paths)), topLevelVariantNullIsSqlNull);
        }
    }

    private static Slice write(VariantShreddingSchema schema, List<Optional<Variant>> rows)
            throws IOException
    {
        MessageType messageType = Types.buildMessage().addField(schema.toParquetType("v", OPTIONAL)).named("test");
        BlockBuilder builder = VARIANT.createBlockBuilder(null, rows.size());
        for (Optional<Variant> row : rows) {
            if (row.isPresent()) {
                VARIANT.writeObject(builder, row.get());
            }
            else {
                builder.appendNull();
            }
        }
        return writeParquetFile(ParquetWriterOptions.builder().build(), messageType, ImmutableMap.of(), ImmutableList.of(new Page(builder.build())));
    }
}
