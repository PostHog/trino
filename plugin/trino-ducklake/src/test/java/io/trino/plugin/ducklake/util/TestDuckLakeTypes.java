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
package io.trino.plugin.ducklake.util;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakeTypes
{
    private static final TypeManager TYPE_MANAGER = TESTING_TYPE_MANAGER;

    @Test
    void testScalarTypes()
    {
        assertThat(DuckLakeTypes.toTrinoType("boolean", TYPE_MANAGER)).isEqualTo(BOOLEAN);
        assertThat(DuckLakeTypes.toTrinoType("int8", TYPE_MANAGER)).isEqualTo(TINYINT);
        assertThat(DuckLakeTypes.toTrinoType("int16", TYPE_MANAGER)).isEqualTo(SMALLINT);
        assertThat(DuckLakeTypes.toTrinoType("int32", TYPE_MANAGER)).isEqualTo(INTEGER);
        assertThat(DuckLakeTypes.toTrinoType("int64", TYPE_MANAGER)).isEqualTo(BIGINT);
        assertThat(DuckLakeTypes.toTrinoType("uint8", TYPE_MANAGER)).isEqualTo(SMALLINT);
        assertThat(DuckLakeTypes.toTrinoType("uint16", TYPE_MANAGER)).isEqualTo(INTEGER);
        assertThat(DuckLakeTypes.toTrinoType("uint32", TYPE_MANAGER)).isEqualTo(BIGINT);
        assertThat(DuckLakeTypes.toTrinoType("uint64", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(20, 0));
        assertThat(DuckLakeTypes.toTrinoType("int128", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(38, 0));
        assertThat(DuckLakeTypes.toTrinoType("float32", TYPE_MANAGER)).isEqualTo(REAL);
        assertThat(DuckLakeTypes.toTrinoType("float64", TYPE_MANAGER)).isEqualTo(DOUBLE);
        assertThat(DuckLakeTypes.toTrinoType("decimal(10,2)", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(10, 2));
        assertThat(DuckLakeTypes.toTrinoType("decimal(38,10)", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(38, 10));
        assertThat(DuckLakeTypes.toTrinoType("varchar", TYPE_MANAGER)).isEqualTo(VARCHAR);
        assertThat(DuckLakeTypes.toTrinoType("json", TYPE_MANAGER)).isEqualTo(JSON);
        assertThat(DuckLakeTypes.toTrinoType("blob", TYPE_MANAGER)).isEqualTo(VARBINARY);
        assertThat(DuckLakeTypes.toTrinoType("uuid", TYPE_MANAGER)).isEqualTo(UUID);
        assertThat(DuckLakeTypes.toTrinoType("date", TYPE_MANAGER)).isEqualTo(DATE);
        assertThat(DuckLakeTypes.toTrinoType("time", TYPE_MANAGER)).isEqualTo(TIME_MICROS);
        assertThat(DuckLakeTypes.toTrinoType("timestamp", TYPE_MANAGER)).isEqualTo(TIMESTAMP_MICROS);
        assertThat(DuckLakeTypes.toTrinoType("timestamp_s", TYPE_MANAGER)).isEqualTo(TIMESTAMP_SECONDS);
        assertThat(DuckLakeTypes.toTrinoType("timestamp_ms", TYPE_MANAGER)).isEqualTo(TIMESTAMP_MILLIS);
        assertThat(DuckLakeTypes.toTrinoType("timestamp_ns", TYPE_MANAGER)).isEqualTo(TIMESTAMP_NANOS);
        assertThat(DuckLakeTypes.toTrinoType("timestamptz", TYPE_MANAGER)).isEqualTo(TIMESTAMP_TZ_MICROS);
    }

    @Test
    void testAliasSpellings()
    {
        assertThat(DuckLakeTypes.toTrinoType("BOOLEAN", TYPE_MANAGER)).isEqualTo(BOOLEAN);
        assertThat(DuckLakeTypes.toTrinoType("TINYINT", TYPE_MANAGER)).isEqualTo(TINYINT);
        assertThat(DuckLakeTypes.toTrinoType("SMALLINT", TYPE_MANAGER)).isEqualTo(SMALLINT);
        assertThat(DuckLakeTypes.toTrinoType("INTEGER", TYPE_MANAGER)).isEqualTo(INTEGER);
        assertThat(DuckLakeTypes.toTrinoType("BIGINT", TYPE_MANAGER)).isEqualTo(BIGINT);
        assertThat(DuckLakeTypes.toTrinoType("HUGEINT", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(38, 0));
        assertThat(DuckLakeTypes.toTrinoType("UBIGINT", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(20, 0));
        assertThat(DuckLakeTypes.toTrinoType("FLOAT", TYPE_MANAGER)).isEqualTo(REAL);
        assertThat(DuckLakeTypes.toTrinoType("DOUBLE", TYPE_MANAGER)).isEqualTo(DOUBLE);
        assertThat(DuckLakeTypes.toTrinoType("VARCHAR", TYPE_MANAGER)).isEqualTo(VARCHAR);
        assertThat(DuckLakeTypes.toTrinoType("Timestamp_US", TYPE_MANAGER)).isEqualTo(TIMESTAMP_MICROS);
        assertThat(DuckLakeTypes.toTrinoType("NUMERIC(5,2)", TYPE_MANAGER)).isEqualTo(DecimalType.createDecimalType(5, 2));
    }

    @Test
    void testUnsupportedTypes()
    {
        assertUnsupported("uint128");
        assertUnsupported("timetz");
        assertUnsupported("interval");
        assertUnsupported("variant");
        assertUnsupported("geometry");
        assertUnsupported("unknown");
        assertUnsupported("something-else");
    }

    @Test
    void testListType()
    {
        DuckLakeColumnEntry parent = column(1, "items", "list", OptionalLong.empty());
        DuckLakeColumnEntry child = column(2, "element", "int32", OptionalLong.of(1));
        ListMultimap<Long, DuckLakeColumnEntry> childrenByParent = ImmutableListMultimap.of(1L, child);

        assertThat(DuckLakeTypes.toTrinoType(parent, childrenByParent, TYPE_MANAGER))
                .isEqualTo(new ArrayType(INTEGER));
    }

    @Test
    void testStructType()
    {
        DuckLakeColumnEntry parent = column(1, "record", "struct", OptionalLong.empty());
        DuckLakeColumnEntry first = column(2, "a", "int64", OptionalLong.of(1));
        DuckLakeColumnEntry second = column(3, "b", "varchar", OptionalLong.of(1));
        ListMultimap<Long, DuckLakeColumnEntry> childrenByParent = ImmutableListMultimap.of(1L, first, 1L, second);

        assertThat(DuckLakeTypes.toTrinoType(parent, childrenByParent, TYPE_MANAGER))
                .isEqualTo(RowType.rowType(RowType.field("a", BIGINT), RowType.field("b", VARCHAR)));
    }

    @Test
    void testMapType()
    {
        DuckLakeColumnEntry parent = column(1, "properties", "map", OptionalLong.empty());
        DuckLakeColumnEntry key = column(2, "key", "varchar", OptionalLong.of(1));
        DuckLakeColumnEntry value = column(3, "value", "int32", OptionalLong.of(1));
        ListMultimap<Long, DuckLakeColumnEntry> childrenByParent = ImmutableListMultimap.of(1L, key, 1L, value);

        assertThat(DuckLakeTypes.toTrinoType(parent, childrenByParent, TYPE_MANAGER))
                .isEqualTo(new MapType(VARCHAR, INTEGER, new TypeOperators()));
    }

    @Test
    void testNestedNestedType()
    {
        DuckLakeColumnEntry parent = column(1, "records", "list", OptionalLong.empty());
        DuckLakeColumnEntry structChild = column(2, "element", "struct", OptionalLong.of(1));
        DuckLakeColumnEntry field = column(3, "x", "float64", OptionalLong.of(2));
        ListMultimap<Long, DuckLakeColumnEntry> childrenByParent = ImmutableListMultimap.of(1L, structChild, 2L, field);

        assertThat(DuckLakeTypes.toTrinoType(parent, childrenByParent, TYPE_MANAGER))
                .isEqualTo(new ArrayType(RowType.rowType(RowType.field("x", DOUBLE))));
    }

    private static void assertUnsupported(String duckLakeType)
    {
        assertThatThrownBy(() -> DuckLakeTypes.toTrinoType(duckLakeType, TYPE_MANAGER))
                .isInstanceOfSatisfying(TrinoException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(DUCKLAKE_UNSUPPORTED_TYPE.toErrorCode()))
                .hasMessageContaining(duckLakeType);
    }

    private static DuckLakeColumnEntry column(long columnId, String name, String type, OptionalLong parentColumn)
    {
        return new DuckLakeColumnEntry(columnId, columnId, name, type, parentColumn, true, Optional.empty(), Optional.empty());
    }
}
