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
package io.trino.plugin.ducklake;

import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public record DuckLakeColumnHandle(
        long columnId,
        String name,
        String duckLakeType,
        Type type,
        boolean nullsAllowed,
        Optional<String> initialDefault)
        implements ColumnHandle
{
    /**
     * The column a scan produces once a {@code count(*)} over the table has been pushed into the
     * catalog: a single row holding the row count. It is the only column such a scan reads, so it
     * never appears beside a column of the table; the negative id keeps it distinguishable anyway.
     */
    public static final DuckLakeColumnHandle ROW_COUNT_COLUMN = new DuckLakeColumnHandle(-1, "$row_count", "BIGINT", BIGINT, false, Optional.empty());

    public DuckLakeColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(duckLakeType, "duckLakeType is null");
        requireNonNull(type, "type is null");
        requireNonNull(initialDefault, "initialDefault is null");
    }

    public boolean isUnsignedBigint()
    {
        return DuckLakeTypes.isUnsignedBigint(duckLakeType);
    }

    public boolean isInt128()
    {
        return DuckLakeTypes.isInt128(duckLakeType);
    }

    public boolean isUnsignedInteger()
    {
        return DuckLakeTypes.isUnsignedInteger(duckLakeType);
    }

    /**
     * Converts to a {@link HiveColumnHandle} for reading Parquet files. Columns are matched
     * by name, so the Hive column index is a fake value. Unsigned integer columns wider than
     * their physical Parquet type are read as the physical type and reinterpreted afterwards:
     * uint32 as {@code INTEGER} (zero-extended to {@code BIGINT}), uint64 as {@code BIGINT}
     * (reinterpreted to {@code DECIMAL(20, 0)}), and int128 as {@code DOUBLE} (its physical
     * Parquet representation, converted to {@code DECIMAL(38, 0)}).
     */
    public HiveColumnHandle toHiveColumnHandle()
    {
        return toHiveColumnHandle(name);
    }

    /**
     * Converts to a {@link HiveColumnHandle} that reads the Parquet column with the given name,
     * used for data files whose columns are resolved through a DuckLake name mapping.
     */
    public HiveColumnHandle toHiveColumnHandle(String parquetColumnName)
    {
        Type readType = type;
        if (isUnsignedInteger()) {
            readType = INTEGER;
        }
        if (isUnsignedBigint()) {
            readType = BIGINT;
        }
        if (isInt128()) {
            readType = DOUBLE;
        }
        return new HiveColumnHandle(
                parquetColumnName,
                0, // fake index; columns are resolved by name
                DuckLakeTypes.toHiveType(readType),
                readType,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullsAllowed)
                .build();
    }
}
