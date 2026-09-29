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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.GroupField;
import io.trino.parquet.PrimitiveField;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.variant.Variant;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rename/evolution binding matrix, read through the REAL page-source
 * path (HoglakePageSourceProvider -> trino-parquet -> memory filesystem)
 * with real parquet bytes — no server, no Docker.
 *
 * Files are written with trino-parquet's writer, which round-trips
 * PARQUET:field_id (Hardwood does not, so hydrator-written files always
 * take the name-fallback path — exactly the fragile half of this matrix).
 */
class TestHoglakeParquetBinding
{
    private static final String PATH = "memory:///binding-test.parquet";

    @Test
    void nestedIdlessFieldsPreferExactName()
    {
        var schema = Types.buildMessage()
                .optionalGroup()
                .optional(PrimitiveTypeName.INT64).named("A")
                .optional(PrimitiveTypeName.INT64).named("a")
                .named("r")
                .named("test");
        var child = new HoglakeColumnHandle("a", 2, BIGINT, true);
        var rowType = RowType.from(List.of(RowType.field("a", BIGINT)));
        var column = new HoglakeColumnHandle("r", 1, rowType, true, List.of(child), "struct");
        var physical = new ColumnIOFactory().getColumnIO(schema).getChild("r");
        var field = (GroupField) HoglakeParquetFields.construct(column, physical).orElseThrow();
        var leaf = (PrimitiveField) field.getChildren().getFirst().orElseThrow();
        assertThat(leaf.getDescriptor().getPath()).containsExactly("r", "a");
    }

    @Test
    void testNativeUnsignedInt32ReadsLosslessly()
    {
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, false)).id(1).named("u"),
                INTEGER,
                Arrays.asList(0L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, -1L))));
        HoglakeColumnHandle column = new HoglakeColumnHandle("u", 1, BIGINT, true, List.of(), "uint32");
        assertThat(read(file, List.of(column), 4))
                .containsExactly(List.of(0L), List.of(2147483647L), List.of(2147483648L), List.of(4294967295L));
    }

    // ---- rename matrix -----------------------------------------------------

    @Test
    void renamedColumn_fileWithFieldIds_bindsByIdCorrectly()
    {
        // File written before the rename: field 1 was called "user_id".
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("user_id"),
                BIGINT,
                Arrays.asList(10L, 20L, 30L))));
        // Catalog after RENAME COLUMN user_id TO user_identifier: same field id.
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("user_identifier", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(renamed), 3);

        assertThat(rows).containsExactly(List.of(10L), List.of(20L), List.of(30L));
    }

    @Test
    void renamedColumn_fileWithoutFieldIds_silentlyReadsAllNulls()
    {
        // Hardwood-style file (no field ids) written before the rename.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).named("user_id"),
                BIGINT,
                Arrays.asList(10L, 20L, 30L))));
        // Catalog after RENAME COLUMN user_id TO user_identifier. The name
        // fallback looks for "user_identifier", finds nothing, and the
        // column falls into the added-after-write null path.
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("user_identifier", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(renamed), 3);

        // Field-ID binding contract:
        // the data exists in the file (as "user_id") but a rename makes it
        // unreachable for every id-less file — silent NULLs. The connector's
        // id-authoritative binding is correct and stays (no name-fallback
        // heuristics); the fix is catalog-side: field ids are becoming a
        // registration contract, and the server will refuse renames while
        // id-less files are live, making this state unreachable. Until that
        // lands, this pins what the connector does with such files.
        assertThat(rows).containsExactly(
                Arrays.asList((Object) null),
                Arrays.asList((Object) null),
                Arrays.asList((Object) null));
    }

    @Test
    void renameThenReaddOldName_fileWithoutFieldIds_bindsOldDataToWrongColumn()
    {
        // File (no ids) written when the table had column "a" = 100, 200.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).named("a"),
                BIGINT,
                Arrays.asList(100L, 200L))));
        // Catalog since evolved: RENAME COLUMN a TO c, then ADD COLUMN a
        // (a brand-new column, field id 3, no data in old files).
        HoglakeColumnHandle c = new HoglakeColumnHandle("c", 1, BIGINT, true);
        HoglakeColumnHandle newA = new HoglakeColumnHandle("a", 3, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(c, newA), 2);

        // Field-ID binding contract:
        // exactly inverted from the truth — "c" (which IS the old "a",
        // renamed) reads null; the NEW column "a" reads the old column's
        // values via the name fallback. The server-side refusal of renames
        // while id-less files are live (field ids as a registration
        // contract) is what closes this; the connector binding stays
        // id-authoritative and unchanged.
        assertThat(rows).containsExactly(
                Arrays.asList(null, 100L),
                Arrays.asList(null, 200L));
    }

    // ---- type evolution ----------------------------------------------------

    @Test
    void typeWidening_int32FilePromotedToLong_coercesCorrectly()
    {
        // File written while the column was `int`; catalog since promoted it
        // to `long`. trino-parquet's INT32->BIGINT decoder handles this.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT32).id(1).named("n"),
                INTEGER,
                Arrays.asList(1, 2, 3))));
        HoglakeColumnHandle promoted = new HoglakeColumnHandle("n", 1, BIGINT, true);

        List<List<Object>> rows = read(file, List.of(promoted), 3);

        assertThat(rows).containsExactly(List.of(1L), List.of(2L), List.of(3L));
    }

    @Test
    void typeMismatch_int64FileReadAsInteger_succeedsInRangeThrowsOnOverflow()
    {
        // The inverse (illegal) direction: file has int64, catalog says int.
        // With the head race closed (pinned handles) this state is only
        // reachable through catalog corruption. trino-parquet narrows via
        // Math.toIntExact: in-range values read SILENTLY (no schema-level
        // rejection of the mismatch), out-of-range values throw at decode
        // time.
        byte[] inRange = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("n"),
                BIGINT,
                Arrays.asList(1L, 2L, 3L))));
        HoglakeColumnHandle narrowed = new HoglakeColumnHandle("n", 1, INTEGER, true);

        // Documented gap: an int64 file column read through an INTEGER
        // handle is a schema violation that goes undetected while the data
        // happens to fit; only an overflowing value surfaces it, mid-read.
        // Connector-side physical-type verification would close it, but the
        // state requires corrupted catalog metadata to reach at all.
        assertThat(read(inRange, List.of(narrowed), 3))
                .containsExactly(List.of(1L), List.of(2L), List.of(3L));

        byte[] overflowing = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("n"),
                BIGINT,
                Arrays.asList(1L, 5_000_000_000L))));
        assertThatThrownBy(() -> read(overflowing, List.of(narrowed), 2))
                .isInstanceOf(Exception.class);
    }

    @Test
    void typeMismatch_stringFileReadAsBigint_failsInsteadOfCorrupting()
    {
        // The old drop+recreate payload (now unreachable through the head
        // race thanks to pinned handles; still reachable via catalog
        // corruption): field id 1 in a foreign incarnation's file is a
        // string, but the column handle says BIGINT.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).id(1).named("name"),
                VARCHAR,
                Arrays.asList("x", "y"))));
        HoglakeColumnHandle stale = new HoglakeColumnHandle("total", 1, BIGINT, true);

        assertThatThrownBy(() -> read(file, List.of(stale), 2))
                .isInstanceOf(Exception.class);
    }

    // ---- presence/absence --------------------------------------------------

    @Test
    void extraFileColumns_droppedFromCatalog_areIgnored()
    {
        // File carries a column that was dropped from the catalog after the
        // write; only requested columns are read.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(
                new FileColumn(
                        Types.optional(PrimitiveTypeName.INT64).id(1).named("keep"),
                        BIGINT,
                        Arrays.asList(1L, 2L)),
                new FileColumn(
                        Types.optional(PrimitiveTypeName.INT64).id(9).named("dropped"),
                        BIGINT,
                        Arrays.asList(99L, 98L))));
        HoglakeColumnHandle keep = new HoglakeColumnHandle("keep", 1, BIGINT, true);

        assertThat(read(file, List.of(keep), 2))
                .containsExactly(List.of(1L), List.of(2L));
    }

    @Test
    void catalogColumnAddedAfterWrite_readsAsNulls()
    {
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("a"),
                BIGINT,
                Arrays.asList(1L, 2L))));
        HoglakeColumnHandle a = new HoglakeColumnHandle("a", 1, BIGINT, true);
        HoglakeColumnHandle added = new HoglakeColumnHandle("added", 7, VARCHAR, true);

        assertThat(read(file, List.of(a, added), 2))
                .containsExactly(Arrays.asList(1L, null), Arrays.asList(2L, null));
    }

    @Test
    void fileWithForeignFieldIds_nameMatchIsNotUsed()
    {
        // Id-authoritative binding: when the file DOES carry ids but none
        // matches, the name fallback is deliberately skipped (an id-bearing
        // file with a same-named field of a different id is a different
        // column lineage). The column reads as nulls.
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(5).named("a"),
                BIGINT,
                Arrays.asList(1L, 2L))));
        HoglakeColumnHandle a = new HoglakeColumnHandle("a", 1, BIGINT, true);

        assertThat(read(file, List.of(a), 2))
                .containsExactly(
                        Arrays.asList((Object) null),
                        Arrays.asList((Object) null));
    }

    // ---- VARIANT -----------------------------------------------------------
    //
    // Each case has 15 rows, which the reader reads in batches of 1, 2, 4,
    // and 8 rows; the nullable cases put NULLs at the start, middle, and end
    // of a batch. Object variants carry per-row metadata, so a misaligned
    // metadata leaf shows up too.

    @Test
    void nullableVariantReadsEveryRow()
    {
        assertVariantsRoundTrip(
                new HoglakeColumnHandle("v", 1, VARIANT, true),
                Arrays.asList(
                        variant(1),
                        null,
                        variant(2),
                        variant(3),
                        null,
                        objectVariant(4),
                        variant(5),
                        variant(6),
                        null,
                        variant(7),
                        null,
                        null,
                        objectVariant(8),
                        variant(9),
                        null));
    }

    @Test
    void requiredVariantReadsEveryRow()
    {
        assertVariantsRoundTrip(
                new HoglakeColumnHandle("v", 1, VARIANT, false),
                IntStream.rangeClosed(1, 15)
                        .mapToObj(value -> value % 4 == 0 ? objectVariant(value) : variant(value))
                        .toList());
    }

    @Test
    void variantInRowReadsEveryRow()
    {
        RowType rowType = RowType.from(List.of(RowType.field("v", VARIANT)));
        HoglakeColumnHandle field = new HoglakeColumnHandle("v", 2, VARIANT, true);
        assertVariantsRoundTrip(
                new HoglakeColumnHandle("r", 1, rowType, true, List.of(field), "struct"),
                Arrays.asList(
                        List.of(variant(1)),
                        singletonList(null),
                        List.of(variant(2)),
                        null,
                        singletonList(null),
                        List.of(variant(3)),
                        List.of(variant(4)),
                        List.of(variant(5)),
                        singletonList(null),
                        null,
                        List.of(objectVariant(6)),
                        singletonList(null),
                        singletonList(null),
                        List.of(variant(7)),
                        null));
    }

    @Test
    void variantInArrayReadsEveryRow()
    {
        HoglakeColumnHandle element = new HoglakeColumnHandle("element", 2, VARIANT, true);
        assertVariantsRoundTrip(
                new HoglakeColumnHandle("a", 1, new ArrayType(VARIANT), true, List.of(element), "list"),
                Arrays.asList(
                        Arrays.asList(variant(1), null),
                        null,
                        List.of(variant(2)),
                        List.of(),
                        singletonList(null),
                        Arrays.asList(null, variant(3), null),
                        List.of(variant(4), variant(5)),
                        Arrays.asList(variant(6), null, objectVariant(7)),
                        null,
                        List.of(),
                        Arrays.asList(null, null),
                        List.of(variant(8)),
                        singletonList(null),
                        List.of(variant(9)),
                        Arrays.asList(null, variant(10))));
    }

    @Test
    void variantInMapReadsEveryRow()
    {
        HoglakeColumnHandle key = new HoglakeColumnHandle("key", 2, VARCHAR, false);
        HoglakeColumnHandle value = new HoglakeColumnHandle("value", 3, VARIANT, true);
        assertVariantsRoundTrip(
                new HoglakeColumnHandle("m", 1, new MapType(VARCHAR, VARIANT, new TypeOperators()), true, List.of(key, value), "map"),
                Arrays.asList(
                        variantMap("a", variant(1), "b", null),
                        null,
                        variantMap("a", null, "b", variant(2)),
                        Map.of(),
                        singletonMap("a", null),
                        variantMap("a", variant(3), "b", objectVariant(4)),
                        Map.of("a", variant(5)),
                        variantMap("a", null, "b", null),
                        null,
                        Map.of("a", variant(6)),
                        Map.of(),
                        variantMap("a", objectVariant(7), "b", null),
                        singletonMap("a", null),
                        Map.of("a", variant(8)),
                        variantMap("a", variant(9), "b", null)));
    }

    // ---- resource behavior through the same path ---------------------------

    @Test
    void pageSourceCloseIsIdempotentAndFinishes()
    {
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                Types.optional(PrimitiveTypeName.INT64).id(1).named("a"),
                BIGINT,
                Arrays.asList(1L, 2L))));
        ConnectorPageSource pageSource = open(
                file,
                List.of(new HoglakeColumnHandle("a", 1, BIGINT, true)),
                2);

        // Close before reading anything (query cancellation path), twice.
        close(pageSource);
        close(pageSource);
        assertThat(pageSource.isFinished()).isTrue();
        assertThat(pageSource.getMemoryUsage()).isZero();
    }

    // ---- plumbing ----------------------------------------------------------

    private static List<List<Object>> read(byte[] file, List<HoglakeColumnHandle> columns, long recordCount)
    {
        ConnectorPageSource pageSource = open(file, columns, recordCount);
        try {
            return ConnectorTestFixtures.readAll(
                    pageSource,
                    columns.stream().map(HoglakeColumnHandle::type).toList());
        }
        finally {
            close(pageSource);
        }
    }

    private static void assertVariantsRoundTrip(HoglakeColumnHandle column, List<?> values)
    {
        HoglakeParquetSchema schema = HoglakeParquetSchema.create(List.of(column));
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(new FileColumn(
                schema.messageType().getType(0),
                column.type(),
                new ArrayList<>(values),
                schema.primitiveTypes())));
        List<Object> actual = read(file, List.of(column), values.size()).stream()
                .map(row -> variantBytes(row.getFirst()))
                .toList();
        assertThat(actual).containsExactlyElementsOf(values.stream().map(TestHoglakeParquetBinding::variantBytes).toList());
    }

    private static Variant variant(int value)
    {
        return Variant.ofInt(value);
    }

    private static Variant objectVariant(int value)
    {
        return Variant.ofObject(Map.of(utf8Slice("key_" + value), variant(value)));
    }

    /**
     * A two-entry map that, unlike {@link Map#of}, allows null values.
     */
    private static Map<String, Variant> variantMap(String firstKey, Variant firstValue, String secondKey, Variant secondValue)
    {
        Map<String, Variant> map = new LinkedHashMap<>();
        map.put(firstKey, firstValue);
        map.put(secondKey, secondValue);
        return map;
    }

    /**
     * Replaces each variant with its encoded metadata and value bytes: SQL
     * equality would treat, for example, an int8 5 and an int64 5 as the same
     * value.
     */
    private static Object variantBytes(Object value)
    {
        return switch (value) {
            case null -> null;
            case Variant variant -> HexFormat.of().formatHex(variant.metadata().toSlice().getBytes()) + ":" + HexFormat.of().formatHex(variant.data().getBytes());
            case List<?> list -> list.stream().map(TestHoglakeParquetBinding::variantBytes).toList();
            case Map<?, ?> map -> {
                Map<Object, Object> bytes = new HashMap<>();
                map.forEach((key, entry) -> bytes.put(key, variantBytes(entry)));
                yield bytes;
            }
            default -> throw new IllegalArgumentException("Unexpected value: " + value);
        };
    }

    private static void close(ConnectorPageSource pageSource)
    {
        try {
            pageSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ConnectorPageSource open(byte[] file, List<HoglakeColumnHandle> columns, long recordCount)
    {
        TrinoFileSystemFactory fileSystem = ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file));
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(fileSystem);
        HoglakeSplit split = new HoglakeSplit(PATH, file.length, recordCount, Optional.empty(), 0);
        return provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                split,
                new HoglakeTableHandle("analytics", "binding_test", 1, "uuid-binding-test", List.of()),
                Optional.empty(),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
    }
}
