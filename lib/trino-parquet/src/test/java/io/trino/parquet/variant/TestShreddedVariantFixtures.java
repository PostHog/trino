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
import com.google.common.collect.ImmutableSet;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.FileParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind;
import io.trino.parquet.variant.ShreddedVariantTestFiles.ShreddedVariantCase;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.ERROR;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.MULTIPLE_ROWS;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.NO_FILES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.SINGLE_ROW;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.DUCKDB_FIXTURES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.PARQUET_TESTING;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.loadParquetTestingCases;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

/// Checks the integrity of the shredded VARIANT fixtures under
/// `variant/shredded`. [TestShreddedVariantReader] decodes their values.
public class TestShreddedVariantFixtures
{
    private static final Set<String> PARQUET_TESTING_UNREFERENCED_FILES = ImmutableSet.of("README.md", "LICENSE.txt", "cases.json");
    private static final String DUCKDB_CREATED_BY = "DuckDB version v1.5.5 (build d8cdaa33fd)";

    private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(US_ASCII);

    @Test
    public void testParquetTestingCases()
    {
        List<ShreddedVariantCase> cases = loadParquetTestingCases();

        assertThat(cases)
                .extracting(ShreddedVariantCase::caseNumber)
                .containsExactlyElementsOf(IntStream.rangeClosed(1, 138).boxed().toList());
        assertThat(caseNumbers(cases, ERROR)).containsExactly(40, 42, 87, 127, 128, 137);
        assertThat(caseNumbers(cases, MULTIPLE_ROWS)).containsExactly(45, 83, 126);
        assertThat(caseNumbers(cases, NO_FILES)).containsExactly(3);
        assertThat(caseNumbers(cases, SINGLE_ROW)).hasSize(128);

        // Files that break the specification, which readers may reject or read
        assertThat(cases.stream().filter(ShreddedVariantCase::invalid).map(ShreddedVariantCase::caseNumber))
                .containsExactly(41, 43, 84, 125, 131, 132, 138);
        for (ShreddedVariantCase testCase : cases) {
            assertThat(testCase.parquetFile().map(file -> file.contains("-INVALID")).orElse(false))
                    .as("case %s file name marks it invalid", testCase.caseNumber())
                    .isEqualTo(testCase.invalid());
        }

        // A missing variant file is a SQL NULL row
        assertThat(cases.stream()
                .flatMap(testCase -> testCase.variantFiles().stream())
                .filter(Optional::isEmpty))
                .hasSize(1);
    }

    @Test
    public void testParquetTestingFilesExist()
            throws IOException
    {
        Set<String> referencedFiles = new HashSet<>();
        for (ShreddedVariantCase testCase : loadParquetTestingCases()) {
            if (testCase.parquetFile().isPresent()) {
                String parquetFile = testCase.parquetFile().get();
                assertParquetFooter(PARQUET_TESTING.resolve(parquetFile));
                referencedFiles.add(parquetFile);
            }
            for (Optional<String> variantFile : testCase.variantFiles()) {
                if (variantFile.isPresent()) {
                    assertVariantFile(PARQUET_TESTING.resolve(variantFile.get()));
                    referencedFiles.add(variantFile.get());
                }
            }
        }

        referencedFiles.addAll(PARQUET_TESTING_UNREFERENCED_FILES);
        assertThat(listFileNames(PARQUET_TESTING)).containsExactlyInAnyOrderElementsOf(referencedFiles);
    }

    @Test
    public void testDuckDbFiles()
            throws IOException
    {
        ImmutableSet.Builder<String> expectedFiles = ImmutableSet.builder();
        expectedFiles.add("generate.sql");
        for (String fixture : DUCKDB_FIXTURES) {
            expectedFiles.add(fixture + ".parquet", fixture + ".duckdb.jsonl");

            FileMetaData footer = assertParquetFooter(DUCKDB.resolve(fixture + ".parquet")).getParquetMetadata();
            assertThat(footer.getCreated_by()).isEqualTo(DUCKDB_CREATED_BY);

            // DuckDB shreds every VARIANT column it writes
            List<SchemaElement> schema = footer.getSchema();
            int variantColumn = childIndex(schema, 0, "v");
            assertThat(schema.get(variantColumn).getLogicalType().isSetVARIANT()).isTrue();
            assertThat(childNames(schema, variantColumn)).containsExactly("metadata", "value", "typed_value");

            assertThat(Files.readAllLines(DUCKDB.resolve(fixture + ".duckdb.jsonl")))
                    .as("DuckDB read-back of %s", fixture)
                    .hasSize(toIntExact(footer.getNum_rows()));
        }
        assertThat(listFileNames(DUCKDB)).containsExactlyInAnyOrderElementsOf(expectedFiles.build());
    }

    @Test
    public void testParquetMetadataLowercasesShreddedFieldNames()
            throws IOException
    {
        ParquetMetadata metadata = assertParquetFooter(DUCKDB.resolve("case-variant-keys.parquet"));

        // Shredded object field names are case-sensitive variant keys, and only the
        // Thrift schema keeps their original spelling
        List<SchemaElement> schema = metadata.getParquetMetadata().getSchema();
        int typedValue = childIndex(schema, childIndex(schema, 0, "v"), "typed_value");
        assertThat(childNames(schema, typedValue)).containsExactly("props", "$os", "$browser");
        int props = childIndex(schema, childIndex(schema, typedValue, "props"), "typed_value");
        assertThat(childNames(schema, props)).containsExactly("Plan");

        GroupType variant = metadata.getFileMetaData().getSchema().getType("v").asGroupType();
        GroupType propsTypedValue = variant.getType("typed_value").asGroupType()
                .getType("props").asGroupType()
                .getType("typed_value").asGroupType();
        assertThat(propsTypedValue.getFields()).extracting(Type::getName).containsExactly("plan");
    }

    private static List<Integer> caseNumbers(List<ShreddedVariantCase> cases, CaseKind kind)
    {
        return cases.stream()
                .filter(testCase -> testCase.kind() == kind)
                .map(ShreddedVariantCase::caseNumber)
                .collect(toImmutableList());
    }

    private static ParquetMetadata assertParquetFooter(Path file)
            throws IOException
    {
        assertThat(file).isRegularFile();
        byte[] bytes = Files.readAllBytes(file);
        assertThat(Arrays.copyOfRange(bytes, 0, PARQUET_MAGIC.length)).as("header of %s", file).isEqualTo(PARQUET_MAGIC);
        assertThat(Arrays.copyOfRange(bytes, bytes.length - PARQUET_MAGIC.length, bytes.length)).as("trailer of %s", file).isEqualTo(PARQUET_MAGIC);

        try (FileParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), ParquetReaderOptions.defaultOptions())) {
            return MetadataReader.readFooter(dataSource, Optional.empty());
        }
    }

    private static void assertVariantFile(Path file)
            throws IOException
    {
        assertThat(file).isRegularFile();
        byte[] bytes = Files.readAllBytes(file);
        // Variant metadata followed by a variant value, each at least one byte
        assertThat(bytes.length).as("size of %s", file).isGreaterThanOrEqualTo(2);
        assertThat(bytes[0] & 0x0F).as("metadata version in %s", file).isEqualTo(1);
    }

    private static Set<String> listFileNames(Path directory)
            throws IOException
    {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).collect(toImmutableSet());
        }
    }

    /// Returns the index of the direct child of `schema[parent]` with the given name.
    private static int childIndex(List<SchemaElement> schema, int parent, String name)
    {
        int index = parent + 1;
        for (int child = 0; child < schema.get(parent).getNum_children(); child++) {
            if (schema.get(index).getName().equals(name)) {
                return index;
            }
            index = nextSibling(schema, index);
        }
        throw new AssertionError("No child %s in %s".formatted(name, schema.get(parent).getName()));
    }

    private static List<String> childNames(List<SchemaElement> schema, int parent)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        int index = parent + 1;
        for (int child = 0; child < schema.get(parent).getNum_children(); child++) {
            names.add(schema.get(index).getName());
            index = nextSibling(schema, index);
        }
        return names.build();
    }

    private static int nextSibling(List<SchemaElement> schema, int index)
    {
        // The Thrift schema is a depth-first list of elements
        int next = index + 1;
        for (int child = 0; child < schema.get(index).getNum_children(); child++) {
            next = nextSibling(schema, next);
        }
        return next;
    }
}
