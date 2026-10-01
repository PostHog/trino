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
package io.trino.plugin.hoglake;

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.plugin.hoglake.testing.PuffinDeletionVectorFixtures;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.VariantType;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reads of VARIANT columns that the file stores in shredded form, with
 * {@code value} and {@code typed_value} columns.
 */
class TestHoglakeShreddedVariant
{
    private static final String PATH = "memory:///shredded-variant.parquet";
    private static final String DELETION_VECTOR_PATH = "memory:///shredded-variant.dv";
    private static final String DUCKDB_CREATED_BY = "DuckDB version v1.5.5 (build d8cdaa33fd)";
    private static final HoglakeColumnHandle COLUMN = new HoglakeColumnHandle("v", 1, VARIANT, true);

    // typed_value shreds field "a" as a bigint and field "$Browser" as a string
    private static final RowType FIELD_A = RowType.from(List.of(field("value", VARBINARY), field("typed_value", BIGINT)));
    private static final RowType FIELD_BROWSER = RowType.from(List.of(field("value", VARBINARY), field("typed_value", VARCHAR)));
    private static final RowType SHREDDED = RowType.from(List.of(
            field("metadata", VARBINARY),
            field("value", VARBINARY),
            field("typed_value", RowType.from(List.of(field("a", FIELD_A), field("browser", FIELD_BROWSER))))));

    private static final Slice METADATA = Metadata.of(List.of(utf8Slice("$Browser"), utf8Slice("a"))).toSlice();

    @Test
    void shreddedColumnIsRead()
    {
        Variant partial = Variant.ofObject(Map.of(utf8Slice("c"), Variant.ofBoolean(true)));
        byte[] file = write(
                List.of(
                        row(METADATA, null, typedValue(Variant.ofLong(1), "Chrome")),
                        row(METADATA, null, typedValue(Variant.ofString("one"), null)),
                        // A residual object refers to its own metadata
                        row(partial.metadata().toSlice(), partial.data(), typedValue(Variant.ofLong(2), null)),
                        row(METADATA, Variant.ofInt(42).data(), null)),
                Optional.empty());
        assertThat(readVariantBytes(file, 4)).containsExactly(
                variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(1), utf8Slice("$Browser"), Variant.ofString("Chrome")))),
                variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofString("one")))),
                variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(2), utf8Slice("c"), Variant.ofBoolean(true)))),
                variantBytes(Variant.ofInt(42)));
    }

    @Test
    void nullRowsAreRead()
    {
        byte[] file = write(
                Arrays.asList(
                        null,
                        row(METADATA, Variant.NULL_VALUE.data(), null),
                        row(METADATA, null, typedValue(Variant.NULL_VALUE, null))),
                Optional.empty());
        assertThat(readVariantBytes(file, 3)).containsExactly(
                null,
                variantBytes(Variant.NULL_VALUE),
                variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.NULL_VALUE))));
    }

    @Test
    void duckDbVariantNullIsSqlNull()
    {
        // DuckDB writes a SQL NULL as a variant null at the top level. A variant null field stays.
        byte[] file = write(
                Arrays.asList(
                        row(METADATA, Variant.NULL_VALUE.data(), null),
                        row(METADATA, null, typedValue(Variant.NULL_VALUE, null))),
                Optional.of(DUCKDB_CREATED_BY));
        assertThat(readVariantBytes(file, 2)).containsExactly(
                null,
                variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.NULL_VALUE))));
    }

    @Test
    void writersOfSqlNullAsVariantNull()
    {
        assertThat(HoglakePageSourceProvider.writesSqlNullAsVariantNull(DUCKDB_CREATED_BY)).isTrue();
        assertThat(HoglakePageSourceProvider.writesSqlNullAsVariantNull("parquet-mr version 1.15.2")).isFalse();
        assertThat(HoglakePageSourceProvider.writesSqlNullAsVariantNull(null)).isFalse();
    }

    @Test
    void deletionVectorAppliesToShreddedColumn()
    {
        // The reader's first pages hold rows 0, then 1 to 2, then 3 to 6, so rows 1 and 5
        // are deleted from pages that keep other rows. Null rows make the reader return
        // dictionary blocks for the fields of the row.
        List<List<Object>> rows = Arrays.asList(
                row(METADATA, null, typedValue(Variant.ofLong(0), null)),
                row(METADATA, null, typedValue(Variant.ofLong(1), null)),
                row(METADATA, null, typedValue(Variant.ofLong(2), null)),
                null,
                row(METADATA, Variant.ofInt(4).data(), null),
                row(METADATA, null, typedValue(Variant.ofLong(5), null)),
                null,
                row(METADATA, null, typedValue(Variant.ofLong(7), null)));
        byte[] file = write(rows, Optional.empty());
        byte[] deletionVector = PuffinDeletionVectorFixtures.deletionVector(PATH, 1, 5);
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file, DELETION_VECTOR_PATH, deletionVector));
        HoglakeSplit split = new HoglakeSplit(PATH, file.length, 8, Optional.of(DELETION_VECTOR_PATH), 2, Optional.of("puffin-dv"));

        assertThat(read(fileSystem, split, List.of(COLUMN))).containsExactly(
                List.of(variantBytes(objectWithA(0))),
                List.of(variantBytes(objectWithA(2))),
                Arrays.asList((Object) null),
                List.of(variantBytes(Variant.ofInt(4))),
                Arrays.asList((Object) null),
                List.of(variantBytes(objectWithA(7))));
    }

    @Test
    void shreddedColumnIsBoundByFieldId()
    {
        // The catalog renamed the column, and the file has another column before it
        FileColumn id = new FileColumn(Types.optional(PrimitiveTypeName.INT64).id(2).named("id"), BIGINT, Arrays.asList(10L, 11L));
        byte[] file = write(
                List.of(id),
                List.of(row(METADATA, null, typedValue(Variant.ofLong(0), null)), row(METADATA, null, typedValue(Variant.ofLong(1), "Firefox"))),
                Optional.empty());
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("renamed", 1, VARIANT, true);
        HoglakeColumnHandle idColumn = new HoglakeColumnHandle("id", 2, BIGINT, true);
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file));

        assertThat(read(fileSystem, new HoglakeSplit(PATH, file.length, 2, Optional.empty(), 0), List.of(renamed, idColumn))).containsExactly(
                List.of(variantBytes(objectWithA(0)), 10L),
                List.of(variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(1), utf8Slice("$Browser"), Variant.ofString("Firefox")))), 11L));
    }

    @Test
    void duckDbVariantNullIsSqlNullInUnshreddedColumn()
    {
        // The same rule applies when DuckDB stores the column unshredded
        HoglakeParquetSchema schema = HoglakeParquetSchema.create(List.of(COLUMN));
        byte[] unshredded = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                schema.messageType().getType(0),
                VARIANT,
                Arrays.asList(Variant.NULL_VALUE, Variant.ofInt(1), null),
                schema.primitiveTypes())));
        byte[] duckDb = ConnectorTestFixtures.rewriteFooter(unshredded, metadata -> metadata.setCreated_by(DUCKDB_CREATED_BY));

        assertThat(readVariantBytes(duckDb, 3)).containsExactly(null, variantBytes(Variant.ofInt(1)), null);
        assertThat(readVariantBytes(unshredded, 3)).containsExactly(variantBytes(Variant.NULL_VALUE), variantBytes(Variant.ofInt(1)), null);
    }

    @Test
    void invalidShreddedLayoutFailsNamingTheDataFile()
    {
        // A repeated VARIANT group holds any number of values per row
        byte[] repeated = ConnectorTestFixtures.rewriteFooter(
                write(List.of(row(METADATA, null, null)), Optional.empty()),
                metadata -> metadata.getSchema().stream()
                        .filter(element -> element.getName().equals("v"))
                        .forEach(element -> element.setRepetition_type(FieldRepetitionType.REPEATED)));
        assertThatThrownBy(() -> readVariantBytes(repeated, 1))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageStartingWith("Cannot read column v from data file %s: ".formatted(PATH))
                .hasMessageContaining("Shredded VARIANT group v is repeated");

        // A VARIANT group has only metadata, value, and typed_value fields
        GroupType extraField = Types.optionalGroup()
                .id(1)
                .required(PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveTypeName.BINARY).named("value")
                .optional(PrimitiveTypeName.INT64).named("typed_value")
                .optional(PrimitiveTypeName.INT64).named("extra")
                .named("v");
        RowType extraFieldType = RowType.from(List.of(field("metadata", VARBINARY), field("value", VARBINARY), field("typed_value", BIGINT), field("extra", BIGINT)));
        byte[] extra = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                extraField,
                extraFieldType,
                new ArrayList<>(List.of(Arrays.asList(METADATA, null, 1L, 2L))),
                Map.of(List.of("v", "metadata"), VARBINARY, List.of("v", "value"), VARBINARY, List.of("v", "typed_value"), BIGINT, List.of("v", "extra"), BIGINT))));
        assertThatThrownBy(() -> readVariantBytes(extra, 1))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unexpected field extra in shredded VARIANT group v");
    }

    @Test
    void largeValuesAreSplitIntoPages()
            throws IOException
    {
        // The writer stores the repeated value once in a dictionary, so the reader sizes its
        // pages by that one copy. Assembly copies the value into every row.
        String browser = "x".repeat(16 * 1024);
        List<List<Object>> rows = new ArrayList<>();
        for (long a = 0; a < 64; a++) {
            rows.add(row(METADATA, null, typedValue(Variant.ofLong(a), browser)));
        }
        byte[] file = write(rows, Optional.empty());
        DataSize maxPageSize = DataSize.of(64, KILOBYTE);
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(
                ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file)),
                ParquetReaderOptions.builder().withMaxReadBlockSize(maxPageSize).build());

        ConnectorPageSource pageSource = provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                new HoglakeSplit(PATH, file.length, 64, Optional.empty(), 0),
                new HoglakeTableHandle("analytics", "shredded_variant_test", 1, "uuid-shredded-variant-test", List.of()),
                Optional.empty(),
                List.of((ColumnHandle) COLUMN),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
        List<Object> values = new ArrayList<>();
        int pages = 0;
        try {
            for (SourcePage page = pageSource.getNextSourcePage(); page != null; page = pageSource.getNextSourcePage()) {
                Block variants = page.getBlock(0);
                // A page ends with the first value that reaches the limit
                assertThat(variants.getSizeInBytes()).isLessThan(maxPageSize.toBytes() + 2L * browser.length());
                for (int position = 0; position < variants.getPositionCount(); position++) {
                    values.add(variantBytes(VARIANT.getObject(variants, position)));
                }
                pages++;
            }
        }
        finally {
            pageSource.close();
        }
        assertThat(pages).isGreaterThanOrEqualTo(16);
        assertThat(values).containsExactlyElementsOf(LongStream.range(0, 64)
                .mapToObj(a -> variantBytes(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(a), utf8Slice("$Browser"), Variant.ofString(browser)))))
                .toList());
    }

    @Test
    void invalidShreddedValueFailsNamingTheDataFile()
    {
        // Field "a" has both a value and a typed_value, but it is not an object
        byte[] file = write(
                List.of(row(METADATA, null, Arrays.asList(Arrays.asList(Variant.ofLong(5).data(), 5L), Arrays.asList(null, null)))),
                Optional.empty());
        assertThatThrownBy(() -> readVariantBytes(file, 1))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessage("Corrupt parquet data: " + PATH)
                .hasStackTraceContaining("Shredded VARIANT value that is not an object has both value and typed_value");
    }

    @Test
    void fieldNamesThatDifferOnlyByCaseFailNamingTheDataFile()
    {
        // Without the check, fields of different types fail when the column IO is built, and
        // fields of the same type fail while reading
        for (boolean sameType : new boolean[] {true, false}) {
            PrimitiveType planType = Types.optional(PrimitiveTypeName.INT64).named("typed_value");
            io.trino.spi.type.Type planTrinoType = BIGINT;
            if (sameType) {
                planType = Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value");
                planTrinoType = VARCHAR;
            }
            GroupType variant = Types.optionalGroup()
                    .id(1)
                    .required(PrimitiveTypeName.BINARY).named("metadata")
                    .optional(PrimitiveTypeName.BINARY).named("value")
                    .optionalGroup()
                    .requiredGroup().optional(PrimitiveTypeName.BINARY).named("value").addField(planType).named("Plan")
                    .requiredGroup().optional(PrimitiveTypeName.BINARY).named("value").optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value").named("plan")
                    .named("typed_value")
                    .named("v");
            RowType planField = RowType.from(List.of(field("value", VARBINARY), field("typed_value", planTrinoType)));
            RowType type = RowType.from(List.of(
                    field("metadata", VARBINARY),
                    field("value", VARBINARY),
                    field("typed_value", RowType.from(List.of(field("first", planField), field("second", FIELD_BROWSER))))));
            Map<List<String>, io.trino.spi.type.Type> primitiveTypes = Map.of(
                    List.of("v", "metadata"), VARBINARY,
                    List.of("v", "value"), VARBINARY,
                    List.of("v", "typed_value", "Plan", "value"), VARBINARY,
                    List.of("v", "typed_value", "Plan", "typed_value"), planTrinoType,
                    List.of("v", "typed_value", "plan", "value"), VARBINARY,
                    List.of("v", "typed_value", "plan", "typed_value"), VARCHAR);
            byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(variant, type, new ArrayList<>(List.of(row(METADATA, null, null))), primitiveTypes)));

            assertThatThrownBy(() -> readVariantBytes(file, 1))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                    .hasMessage("Cannot read column v from data file %s: Shredded VARIANT object fields that differ only by case are not supported: plan in v.typed_value".formatted(PATH));
        }
    }

    @Test
    void nestedShreddedVariantIsNotSupported()
    {
        MessageType schema = Types.buildMessage()
                .optionalGroup()
                .optionalGroup().id(2)
                .required(PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveTypeName.BINARY).named("value")
                .optional(PrimitiveTypeName.INT64).named("typed_value")
                .named("v")
                .id(1)
                .named("r")
                .named("test");
        HoglakeColumnHandle column = new HoglakeColumnHandle("r", 1, RowType.from(List.of(field("v", VARIANT))), true, List.of(new HoglakeColumnHandle("v", 2, VARIANT, true)), "struct");
        assertThatThrownBy(() -> HoglakeParquetFields.construct(column, new ColumnIOFactory().getColumnIO(schema).getChild("r")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Hoglake supports shredded VARIANT files only in top-level columns: v");
    }

    /**
     * A file with one shredded VARIANT column. The Trino writer cannot write a
     * shredded VARIANT group, so it writes a plain group, and the footer then
     * gets the VARIANT annotation and, optionally, another writer's name.
     */
    private static byte[] write(List<List<Object>> rows, Optional<String> createdBy)
    {
        return write(List.of(), rows, createdBy);
    }

    private static byte[] write(List<FileColumn> leadingColumns, List<List<Object>> rows, Optional<String> createdBy)
    {
        GroupType variant = Types.optionalGroup()
                .id(1)
                .required(PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveTypeName.BINARY).named("value")
                .optionalGroup()
                .requiredGroup().optional(PrimitiveTypeName.BINARY).named("value").optional(PrimitiveTypeName.INT64).named("typed_value").named("a")
                .requiredGroup().optional(PrimitiveTypeName.BINARY).named("value").optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value").named("$Browser")
                .named("typed_value")
                .named("v");
        List<FileColumn> columns = new ArrayList<>(leadingColumns);
        columns.add(new FileColumn(variant, SHREDDED, new ArrayList<>(rows), primitiveTypes("a", "$Browser")));
        byte[] file = ConnectorTestFixtures.writeParquet(columns);
        return ConnectorTestFixtures.rewriteFooter(file, metadata -> {
            for (SchemaElement element : metadata.getSchema()) {
                if (element.getName().equals("v")) {
                    element.setLogicalType(LogicalType.VARIANT(new VariantType()));
                }
            }
            createdBy.ifPresent(metadata::setCreated_by);
        });
    }

    private static Map<List<String>, io.trino.spi.type.Type> primitiveTypes(String firstField, String secondField)
    {
        return Map.of(
                List.of("v", "metadata"), VARBINARY,
                List.of("v", "value"), VARBINARY,
                List.of("v", "typed_value", firstField, "value"), VARBINARY,
                List.of("v", "typed_value", firstField, "typed_value"), BIGINT,
                List.of("v", "typed_value", secondField, "value"), VARBINARY,
                List.of("v", "typed_value", secondField, "typed_value"), VARCHAR);
    }

    private static List<Object> row(Slice metadata, Slice value, List<Object> typedValue)
    {
        return Arrays.asList(metadata, value, typedValue);
    }

    /**
     * The typed_value of an object: field "a" in its value column unless it is
     * a bigint, and field "$Browser" as a string, or missing when null.
     */
    private static List<Object> typedValue(Variant a, String browser)
    {
        List<Object> fieldA;
        if (a.primitiveType() == io.trino.spi.variant.Header.PrimitiveType.INT64) {
            fieldA = Arrays.asList(null, a.getLong());
        }
        else {
            fieldA = Arrays.asList(a.data(), null);
        }
        return Arrays.asList(fieldA, Arrays.asList(null, browser));
    }

    private static Variant objectWithA(long a)
    {
        return Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(a)));
    }

    private static List<Object> readVariantBytes(byte[] file, long recordCount)
    {
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file));
        return read(fileSystem, new HoglakeSplit(PATH, file.length, recordCount, Optional.empty(), 0), List.of(COLUMN)).stream()
                .map(List::getFirst)
                .toList();
    }

    /**
     * The rows of the columns, with each variant as its encoded bytes.
     */
    private static List<List<Object>> read(TrinoFileSystemFactory fileSystem, HoglakeSplit split, List<HoglakeColumnHandle> columns)
    {
        ConnectorPageSource pageSource = new HoglakePageSourceProvider(fileSystem).createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("analytics", "shredded_variant_test", 1, "uuid-shredded-variant-test", List.of()),
                Optional.empty(),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
        try {
            return ConnectorTestFixtures.readAll(pageSource, columns.stream().map(HoglakeColumnHandle::type).toList()).stream()
                    .map(row -> row.stream().map(TestHoglakeShreddedVariant::comparable).toList())
                    .toList();
        }
        finally {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static Object comparable(Object value)
    {
        if (value instanceof Variant variant) {
            return variantBytes(variant);
        }
        return value;
    }

    /**
     * The encoded metadata and value of a variant: SQL equality would treat,
     * for example, an int32 5 and an int64 5 as the same value.
     */
    private static Object variantBytes(Variant variant)
    {
        if (variant == null) {
            return null;
        }
        return HexFormat.of().formatHex(variant.metadata().toSlice().getBytes()) + ":" + HexFormat.of().formatHex(variant.data().getBytes());
    }
}
