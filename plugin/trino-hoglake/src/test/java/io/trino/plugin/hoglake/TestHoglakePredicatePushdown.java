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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.FeaturesConfig;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures.FileColumn;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestHoglakePredicatePushdown
{
    private static final String PATH = "memory:///predicate.parquet";
    private static final HoglakeColumnHandle NUMBER = new HoglakeColumnHandle("number", 1, BIGINT, true);
    private static final HoglakeColumnHandle TIMESTAMP = new HoglakeColumnHandle("timestamp", 2, TIMESTAMP_MICROS, true);
    private static final HoglakeTableHandle TABLE = new HoglakeTableHandle("test", "events", 7, "test-table", List.of(NUMBER, TIMESTAMP));

    @Test
    void testFilterRemainsResidualAndIntersects()
    {
        try (HoglakeClient client = new HoglakeClient("http://localhost:1", "test")) {
            HoglakeMetadata metadata = new HoglakeMetadata(client);
            Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(NUMBER, Domain.singleValue(BIGINT, 10L))));
            var result = metadata.applyFilter(ConnectorTestFixtures.session(), TABLE, constraint).orElseThrow();
            HoglakeTableHandle filtered = (HoglakeTableHandle) result.getHandle();
            assertThat(result.getRemainingFilter()).isEqualTo(constraint.getSummary());
            assertThat(result.getRemainingExpression()).contains(constraint.getExpression());
            assertThat(filtered).isEqualTo(TABLE.withConstraint(constraint.getSummary().transformKeys(HoglakeColumnHandle.class::cast)));
            assertThat(metadata.applyFilter(ConnectorTestFixtures.session(), filtered, constraint)).isEmpty();
            Constraint disjoint = new Constraint(TupleDomain.withColumnDomains(Map.of(NUMBER, Domain.singleValue(BIGINT, 20L))));
            HoglakeTableHandle empty = (HoglakeTableHandle) metadata.applyFilter(ConnectorTestFixtures.session(), filtered, disjoint).orElseThrow().getHandle();
            assertThat(empty.constraint().isNone()).isTrue();
            // No server is running: an impossible scan must not request the catalog.
            assertThat(new HoglakeSplitManager(client).getSplits(
                    HoglakeTransactionHandle.INSTANCE,
                    ConnectorTestFixtures.session(),
                    empty,
                    Set.of(),
                    Constraint.alwaysTrue()).isFinished()).isTrue();
        }
    }

    @Test
    void testHandleJsonRoundTrip()
    {
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(
                new BlockEncodingManager(new BlockEncodingSimdSupport(new FeaturesConfig())), TESTING_TYPE_MANAGER);
        JsonCodecFactory codecFactory = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde)))
                .withJsonDeserializers(Map.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde))).get());
        JsonCodec<HoglakeTableHandle> codec = codecFactory.jsonCodec(HoglakeTableHandle.class);
        for (TupleDomain<HoglakeColumnHandle> predicate : List.of(
                TupleDomain.<HoglakeColumnHandle>all(),
                TupleDomain.<HoglakeColumnHandle>none(),
                TupleDomain.withColumnDomains(Map.of(TIMESTAMP, range(TIMESTAMP_MICROS, -10, 20))))) {
            HoglakeTableHandle handle = TABLE.withConstraint(predicate);
            assertThat(codec.fromJson(codec.toJson(handle))).isEqualTo(handle);
            assertThat(codec.fromJson(codec.toJson(handle.withCountOnly()))).isEqualTo(handle.withCountOnly());
            assertThat(codec.fromJson(codec.toJson(handle.withCountOnly())).countOnly()).isTrue();
            JsonCodec<HoglakeDeleteHandle> mergeCodec = codecFactory.jsonCodec(HoglakeDeleteHandle.class);
            for (Optional<String> insertFailure : List.of(Optional.<String>empty(), Optional.of("Writing sorted Hoglake tables is not supported"))) {
                HoglakeDeleteHandle merge = new HoglakeDeleteHandle(handle, "memory:///warehouse/", "synthetic", insertFailure);
                assertThat(mergeCodec.fromJson(mergeCodec.toJson(merge))).isEqualTo(merge);
            }
        }
    }

    @Test
    void testTimestampRangePrunesRowGroupsWithUnprojectedFilterColumn()
            throws IOException
    {
        byte[] file = timestampFile();
        // Two row groups: [-10, 0] and [10, 20]. Half-open bounds exclude the first.
        assertThat(read(file, List.of(NUMBER), TIMESTAMP, range(TIMESTAMP_MICROS, 10, 21)))
                .containsExactly(List.of(3L), List.of(4L));
        assertThat(read(file, List.of(NUMBER), TIMESTAMP, range(TIMESTAMP_MICROS, 1, 10))).isEmpty();
        assertThat(read(file, List.of(), TIMESTAMP, range(TIMESTAMP_MICROS, 10, 21))).hasSize(2);
        assertThat(read(file, List.of(NUMBER), TIMESTAMP, range(TIMESTAMP_MICROS, -10, 0)))
                .containsExactly(List.of(1L), List.of(2L)); // The engine must apply the residual to remove timestamp=0.
    }

    @Test
    void testTimestampWithTimeZonePruning()
            throws IOException
    {
        HoglakeColumnHandle column = new HoglakeColumnHandle("timestamp", 2, TIMESTAMP_TZ_MICROS, true);
        byte[] file = ConnectorTestFixtures.writeParquet(List.of(
                        new FileColumn(
                                optional(INT64).as(timestampType(true, MICROS)).id(2).named("timestamp"),
                                TIMESTAMP_TZ_MICROS,
                                Arrays.asList(zoned(-10, "UTC"), zoned(0, "UTC"), zoned(10, "UTC"), zoned(20, "UTC")))),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(2).build());
        assertThat(read(file, List.of(column), column, Domain.all(TIMESTAMP_TZ_MICROS))).hasSize(4);
        // The page source does not apply the SQL residual: these counts prove row-group pruning.
        assertThat(read(file, List.of(column), column, zonedRange(1, true, 10, false))).isEmpty();
        assertThat(read(file, List.of(column), column, zonedRange(0, false, 10, true)))
                .containsExactly(List.of(zoned(10, "UTC")), List.of(zoned(20, "UTC")));
        assertThat(read(file, List.of(column), column, zonedRange(-10, true, 0, false)))
                .containsExactly(List.of(zoned(-10, "UTC")), List.of(zoned(0, "UTC")));
        assertThat(read(file, List.of(column), column, Domain.singleValue(TIMESTAMP_TZ_MICROS, zoned(20, "+05:30"))))
                .containsExactly(List.of(zoned(10, "UTC")), List.of(zoned(20, "UTC")));
        assertThat(read(file, List.of(column), column, Domain.create(ValueSet.ofRanges(
                Range.greaterThan(TIMESTAMP_TZ_MICROS, zoned(20, "-07:00"))), false))).isEmpty();

        byte[] nullable = ConnectorTestFixtures.writeParquet(List.of(
                        new FileColumn(
                                optional(INT64).as(timestampType(true, MICROS)).id(2).named("timestamp"),
                                TIMESTAMP_TZ_MICROS,
                                Arrays.asList(null, null, zoned(10, "UTC"), zoned(20, "UTC")))),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(2).build());
        assertThat(read(nullable, List.of(column), column, Domain.onlyNull(TIMESTAMP_TZ_MICROS)))
                .containsExactly(Arrays.asList((Object) null), Arrays.asList((Object) null));
        assertThat(read(nullable, List.of(column), column, Domain.notNull(TIMESTAMP_TZ_MICROS))).hasSize(2);
        assertThat(read(nullable, List.of(column), column, Domain.create(ValueSet.of(TIMESTAMP_TZ_MICROS, zoned(10, "UTC")), true))).hasSize(4);
    }

    @Test
    void testTimestampWithTimeZoneFallback()
    {
        for (var field : List.of(
                optional(INT64).as(timestampType(false, MICROS)).id(2).named("timestamp"),
                optional(INT64).as(timestampType(true, MILLIS)).id(2).named("timestamp"),
                optional(INT64).as(timestampType(true, NANOS)).id(2).named("timestamp"),
                optional(INT64).id(2).named("timestamp"),
                optional(INT96).id(2).named("timestamp"))) {
            MessageType schema = new MessageType("test", field);
            HoglakeColumnHandle column = new HoglakeColumnHandle("timestamp", 2, TIMESTAMP_TZ_MICROS, true);
            assertThat(HoglakePageSourceProvider.parquetPredicate(
                    schema,
                    getDescriptors(schema, schema),
                    TupleDomain.withColumnDomains(Map.of(column, zonedRange(0, true, 10, false)))).isAll()).isTrue();
        }
        MessageType schema = new MessageType("test", optional(INT64).as(timestampType(true, MICROS)).id(2).named("timestamp"));
        for (Type type : List.of(TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_NANOS)) {
            HoglakeColumnHandle column = new HoglakeColumnHandle("timestamp", 2, type, true, List.of(), "timestamptz");
            assertThat(HoglakePageSourceProvider.parquetPredicate(
                    schema,
                    getDescriptors(schema, schema),
                    TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(
                            type,
                            type.equals(TIMESTAMP_TZ_MILLIS) ? packDateTimeWithZone(0, TimeZoneKey.UTC_KEY) : zoned(0, "UTC"))))).isAll()).isTrue();
        }
        HoglakeColumnHandle column = new HoglakeColumnHandle("timestamp", 2, TIMESTAMP_TZ_MICROS, true);
        LongTimestampWithTimeZone subMicrosecond = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 1, TimeZoneKey.UTC_KEY);
        assertThat(HoglakePageSourceProvider.parquetPredicate(
                schema,
                getDescriptors(schema, schema),
                TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(TIMESTAMP_TZ_MICROS, subMicrosecond)))).isAll()).isTrue();
    }

    private static Domain zonedRange(long lower, boolean lowerInclusive, long upper, boolean upperInclusive)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(
                TIMESTAMP_TZ_MICROS,
                zoned(lower, "+05:30"),
                lowerInclusive,
                zoned(upper, "-07:00"),
                upperInclusive)), false);
    }

    private static LongTimestampWithTimeZone zoned(long epochMicros, String zone)
    {
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                Math.floorDiv(epochMicros, 1_000), (int) Math.floorMod(epochMicros, 1_000) * 1_000_000, TimeZoneKey.getTimeZoneKey(zone));
    }

    @Test
    void testDeletePositionsAfterRowGroupPruning()
            throws IOException
    {
        assertThat(read(timestampFile(), List.of(HoglakeColumnHandle.ROW_ID), TIMESTAMP, range(TIMESTAMP_MICROS, 10, 21)))
                .containsExactly(List.of(List.of(0L, 2L)), List.of(List.of(0L, 3L)));
    }

    @Test
    void testRenamedAndNameFallbackColumns()
            throws IOException
    {
        HoglakeColumnHandle renamed = new HoglakeColumnHandle("renamed", 1, BIGINT, true);
        byte[] withId = numbers(optional(INT64).id(1).named("old"), Arrays.asList(10L, 20L, 30L, 40L));
        assertThat(read(withId, List.of(renamed), renamed, range(BIGINT, 30, 41))).containsExactly(List.of(30L), List.of(40L));
        byte[] noId = numbers(optional(INT64).named("NUMBER"), Arrays.asList(10L, 20L, 30L, 40L));
        assertThat(read(noId, List.of(NUMBER), NUMBER, range(BIGINT, 30, 41))).containsExactly(List.of(30L), List.of(40L));
    }

    @Test
    void testNullAndMissingColumns()
            throws IOException
    {
        byte[] file = numbers(optional(INT64).id(1).named("number"), Arrays.asList(null, null, 30L, 40L));
        assertThat(read(file, List.of(NUMBER), NUMBER, Domain.onlyNull(BIGINT)))
                .containsExactly(Arrays.asList((Object) null), Arrays.asList((Object) null));
        assertThat(read(file, List.of(NUMBER), NUMBER, Domain.notNull(BIGINT))).containsExactly(List.of(30L), List.of(40L));
        assertThat(read(file, List.of(NUMBER), NUMBER, Domain.create(ValueSet.of(BIGINT, 30L), true)))
                .hasSize(4);
        HoglakeColumnHandle missing = new HoglakeColumnHandle("missing", 9, BIGINT, true);
        assertThat(read(file, List.of(NUMBER), missing, Domain.onlyNull(BIGINT))).hasSize(4);
        assertThat(read(file, List.of(NUMBER), missing, Domain.notNull(BIGINT))).isEmpty();
        // Matching name with a foreign field id is also a missing column.
        HoglakeColumnHandle foreign = new HoglakeColumnHandle("number", 9, BIGINT, true);
        assertThat(read(file, List.of(NUMBER), foreign, Domain.onlyNull(BIGINT))).hasSize(4);
        assertThat(read(file, List.of(NUMBER), foreign, Domain.notNull(BIGINT))).isEmpty();
    }

    @Test
    void testMissingAndCorruptStatisticsRetainData()
            throws IOException
    {
        byte[] file = numbers(optional(INT64).id(1).named("number"), Arrays.asList(10L, 20L, 30L, 40L));
        byte[] missing = rewriteFooter(file, metadata -> metadata.getRow_groups().forEach(group ->
                group.getColumns().forEach(column -> column.getMeta_data().unsetStatistics())));
        assertThat(read(missing, List.of(NUMBER), NUMBER, range(BIGINT, 10, 41))).hasSize(4);
        byte[] corrupt = rewriteFooter(file, metadata -> metadata.getRow_groups().forEach(group ->
                group.getColumns().forEach(column -> {
                    column.getMeta_data().getStatistics().setMin_value(littleEndianLong(100));
                    column.getMeta_data().getStatistics().setMax_value(littleEndianLong(-100));
                })));
        assertThat(read(corrupt, List.of(NUMBER), NUMBER, range(BIGINT, 10, 21))).hasSize(4);
    }

    @Test
    void testMissingNullCountsRetainNulls()
            throws IOException
    {
        byte[] file = numbers(optional(INT64).id(1).named("number"), Arrays.asList(null, 20L, 30L, 40L));
        byte[] missingNullCounts = rewriteFooter(file, metadata -> metadata.getRow_groups().forEach(group ->
                group.getColumns().forEach(column -> column.getMeta_data().getStatistics().unsetNull_count())));
        assertThat(read(missingNullCounts, List.of(NUMBER), NUMBER, Domain.onlyNull(BIGINT)))
                .contains(Arrays.asList((Object) null));
    }

    @Test
    void testPromotedPhysicalTypesRemainResidual()
    {
        for (var physical : List.of(INT32, FLOAT)) {
            Type type = physical == INT32 ? BIGINT : DOUBLE;
            Object value = physical == INT32 ? (Object) 1L : 1.0;
            HoglakeColumnHandle column = new HoglakeColumnHandle("promoted", 1, type, true);
            MessageType schema = new MessageType("test", optional(physical).id(1).named("old_name"));
            assertThat(HoglakePageSourceProvider.parquetPredicate(
                    schema,
                    getDescriptors(schema, schema),
                    TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(type, value)))).isAll()).isTrue();
        }
    }

    @Test
    void testUuidBoundsAreNotPushed()
    {
        HoglakeColumnHandle uuid = new HoglakeColumnHandle("uuid", 1, UUID, true);
        MessageType schema = new MessageType("test", optional(FIXED_LEN_BYTE_ARRAY).length(16).id(1).named("uuid"));
        assertThat(HoglakePageSourceProvider.parquetPredicate(
                schema,
                getDescriptors(schema, schema),
                TupleDomain.withColumnDomains(Map.of(uuid, Domain.notNull(UUID)))).isAll()).isTrue();
    }

    private static Domain range(Type type, long lower, long upper)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(type, lower, true, upper, false)), false);
    }

    private static byte[] timestampFile()
    {
        return ConnectorTestFixtures.writeParquet(List.of(
                        new FileColumn(optional(INT64).id(1).named("number"), BIGINT, Arrays.asList(1L, 2L, 3L, 4L)),
                        new FileColumn(optional(INT64).as(timestampType(false, MICROS)).id(2).named("timestamp"), TIMESTAMP_MICROS, Arrays.asList(-10L, 0L, 10L, 20L))),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(2).build());
    }

    private static byte[] numbers(org.apache.parquet.schema.Type field, List<Object> values)
    {
        return ConnectorTestFixtures.writeParquet(
                List.of(new FileColumn(field, BIGINT, values)),
                ParquetWriterOptions.builder().setMaxRowGroupRowCount(2).build());
    }

    private static List<List<Object>> read(byte[] file, List<HoglakeColumnHandle> columns, HoglakeColumnHandle filterColumn, Domain domain)
            throws IOException
    {
        HoglakePageSourceProvider provider = new HoglakePageSourceProvider(ConnectorTestFixtures.memoryFileSystem(Map.of(PATH, file)));
        try (ConnectorPageSource source = provider.createPageSource(
                HoglakeTransactionHandle.INSTANCE,
                ConnectorTestFixtures.session(),
                new HoglakeSplit(PATH, file.length, 4, Optional.empty(), 0),
                TABLE.withConstraint(TupleDomain.withColumnDomains(Map.of(filterColumn, domain))),
                Optional.empty(),
                columns.stream().map(ColumnHandle.class::cast).toList(),
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT)) {
            return ConnectorTestFixtures.readAll(source, columns.stream().map(HoglakeColumnHandle::type).toList());
        }
    }

    private static byte[] rewriteFooter(byte[] file, Consumer<FileMetaData> mutation)
            throws IOException
    {
        int footerLength = ByteBuffer.wrap(file, file.length - 8, 4).order(LITTLE_ENDIAN).getInt();
        int footerOffset = file.length - 8 - footerLength;
        FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(file, footerOffset, footerLength));
        mutation.accept(metadata);
        ByteArrayOutputStream footer = new ByteArrayOutputStream();
        Util.writeFileMetaData(metadata, footer);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        result.write(file, 0, footerOffset);
        footer.writeTo(result);
        result.write(ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(footer.size()).array());
        result.write(file, file.length - 4, 4);
        return result.toByteArray();
    }

    private static byte[] littleEndianLong(long value)
    {
        return ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array();
    }
}
