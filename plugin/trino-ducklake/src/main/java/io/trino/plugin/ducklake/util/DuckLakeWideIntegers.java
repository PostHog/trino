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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.util.function.UnaryOperator;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;

/**
 * Converts the engine values of DuckLake's unsigned and 128-bit integer columns into the narrower
 * representations their data files store.
 * <p>
 * Trino has no unsigned types, so the read path widens each of them to the next type that can hold
 * every value: {@code uint8} to {@code SMALLINT}, {@code uint32} to {@code BIGINT}, {@code uint64}
 * to {@code DECIMAL(20, 0)}, and 128-bit integers to {@code DECIMAL(38, 0)}. Writing reverses that,
 * and rejects a value the column cannot hold instead of letting it wrap around into a different
 * one. Columns of these types are only ever created by DuckDB; Trino writes them when inserting
 * into a table it did not create.
 */
public final class DuckLakeWideIntegers
{
    private static final BigInteger UNSIGNED_LONG_LIMIT = BigInteger.ONE.shiftLeft(Long.SIZE);
    private static final long UNSIGNED_INT_LIMIT = 1L << Integer.SIZE;

    private DuckLakeWideIntegers() {}

    /**
     * Checks that every value fits the unsigned range the physical column has, leaving the block
     * itself alone. Used where the engine type is already narrow enough to be written directly.
     */
    public static UnaryOperator<Block> checkUnsignedRange(Type type, long limit, String duckLakeType)
    {
        return block -> {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    continue;
                }
                long value = type.getLong(block, position);
                if (value < 0 || value >= limit) {
                    throw outOfRange(duckLakeType, Long.toString(value));
                }
            }
            return block;
        };
    }

    /**
     * {@code BIGINT} to the 32 bits an unsigned integer column stores.
     */
    public static Block toUnsignedInteger(Block block)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = BIGINT.getLong(block, position);
            if (value < 0 || value >= UNSIGNED_INT_LIMIT) {
                throw outOfRange("uint32", Long.toString(value));
            }
            INTEGER.writeInt(builder, (int) value);
        }
        return builder.build();
    }

    /**
     * {@code DECIMAL(20, 0)} to the 64 bits an unsigned bigint column stores. Values above
     * {@link Long#MAX_VALUE} are stored with their top bit set, which is how an unsigned 64-bit
     * value is carried in a signed one.
     */
    public static Block toUnsignedBigint(Block block)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            BigInteger value = ((Int128) DuckLakeTypes.UNSIGNED_BIGINT_DECIMAL_TYPE.getObject(block, position)).toBigInteger();
            if (value.signum() < 0 || value.compareTo(UNSIGNED_LONG_LIMIT) >= 0) {
                throw outOfRange("uint64", value.toString());
            }
            BIGINT.writeLong(builder, value.longValue());
        }
        return builder.build();
    }

    /**
     * {@code DECIMAL(38, 0)} to the {@code DOUBLE} a 128-bit integer column is stored as. DuckDB
     * writes these columns as doubles, so values beyond the 53 bits a double holds exactly lose
     * their low digits here exactly as they would there.
     */
    public static Block toInt128Double(Block block)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            BigInteger value = ((Int128) DuckLakeTypes.INT128_DECIMAL_TYPE.getObject(block, position)).toBigInteger();
            DOUBLE.writeDouble(builder, value.doubleValue());
        }
        return builder.build();
    }

    private static TrinoException outOfRange(String duckLakeType, String value)
    {
        return new TrinoException(DUCKLAKE_BAD_DATA, "Value out of range for a DuckLake %s column: %s".formatted(duckLakeType, value));
    }
}
