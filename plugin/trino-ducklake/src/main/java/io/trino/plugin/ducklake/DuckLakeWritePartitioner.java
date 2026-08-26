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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.ducklake.util.DuckLakePartitionValues;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/**
 * Derives the partition keys of each row and turns them into the values the catalog records and
 * the directory the file is written into.
 * <p>
 * The temporal transforms follow DuckDB: {@code year} is the calendar year, {@code month} the
 * month of the year, {@code day} the day of the month and {@code hour} the hour of the day, each
 * an independent component rather than a truncation. The read path relies on exactly that reading
 * when it turns a file's partition values back into a range to prune on.
 */
public final class DuckLakeWritePartitioner
        implements DuckLakePageSink.DuckLakePartitioner
{
    public static final String IDENTITY_TRANSFORM = "identity";
    public static final String YEAR_TRANSFORM = "year";
    public static final String MONTH_TRANSFORM = "month";
    public static final String DAY_TRANSFORM = "day";
    public static final String HOUR_TRANSFORM = "hour";

    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;

    private final List<DuckLakePartitioning.Field> fields;
    private final List<Type> sourceTypes;
    private final List<Type> partitionTypes;

    public DuckLakeWritePartitioner(DuckLakePartitioning partitioning, List<DuckLakeWriteColumn> columns)
    {
        this.fields = ImmutableList.copyOf(requireNonNull(partitioning, "partitioning is null").fields());
        this.sourceTypes = fields.stream()
                .map(field -> columns.get(field.sourceChannel()).type())
                .collect(toImmutableList());
        this.partitionTypes = fields.stream()
                .map(field -> {
                    if (isIdentity(field)) {
                        return columns.get(field.sourceChannel()).type();
                    }
                    return (Type) INTEGER;
                })
                .collect(toImmutableList());
    }

    @Override
    public List<Type> partitionTypes()
    {
        return partitionTypes;
    }

    @Override
    public Page partitionColumns(Page page)
    {
        Block[] blocks = new Block[fields.size()];
        for (int index = 0; index < fields.size(); index++) {
            DuckLakePartitioning.Field field = fields.get(index);
            Block source = page.getBlock(field.sourceChannel());
            if (isIdentity(field)) {
                blocks[index] = source;
            }
            else {
                blocks[index] = applyTemporalTransform(sourceTypes.get(index), field.transform(), source);
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public List<Optional<String>> partitionValues(Page partitionColumns, int position)
    {
        ImmutableList.Builder<Optional<String>> values = ImmutableList.builderWithExpectedSize(fields.size());
        for (int index = 0; index < fields.size(); index++) {
            values.add(DuckLakePartitionValues.format(partitionTypes.get(index), partitionColumns.getBlock(index), position));
        }
        return values.build();
    }

    @Override
    public String partitionPath(List<Optional<String>> partitionValues)
    {
        StringBuilder path = new StringBuilder();
        for (int index = 0; index < fields.size(); index++) {
            path.append(DuckLakePartitionValues.directorySegment(directoryKey(fields.get(index)), partitionValues.get(index)));
        }
        return path.toString();
    }

    /**
     * The name a partition key is filed under in the directory layout. DuckDB names a transformed
     * key after the transform rather than the column, which keeps two transforms of the same
     * column apart.
     */
    private static String directoryKey(DuckLakePartitioning.Field field)
    {
        if (isIdentity(field)) {
            return field.columnName();
        }
        return field.transform().toLowerCase(Locale.ENGLISH);
    }

    private static boolean isIdentity(DuckLakePartitioning.Field field)
    {
        return field.transform().toLowerCase(Locale.ENGLISH).equals(IDENTITY_TRANSFORM);
    }

    private static Block applyTemporalTransform(Type sourceType, String transform, Block source)
    {
        int positionCount = source.getPositionCount();
        BlockBuilder values = INTEGER.createFixedSizeBlockBuilder(positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (source.isNull(position)) {
                values.appendNull();
                continue;
            }
            INTEGER.writeInt(values, component(transform, timestampAt(sourceType, source, position)));
        }
        return values.build();
    }

    private static LocalDateTime timestampAt(Type sourceType, Block block, int position)
    {
        if (DATE.equals(sourceType)) {
            return LocalDate.ofEpochDay(DATE.getLong(block, position)).atStartOfDay();
        }
        if (sourceType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return fromEpochMicros(timestampType.getLong(block, position));
            }
            return fromEpochMicros(((LongTimestamp) timestampType.getObject(block, position)).getEpochMicros());
        }
        throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "A temporal partition transform cannot be applied to a column of type " + sourceType);
    }

    private static LocalDateTime fromEpochMicros(long micros)
    {
        long seconds = floorDiv(micros, MICROSECONDS_PER_SECOND);
        long nanos = floorMod(micros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
    }

    private static int component(String transform, LocalDateTime timestamp)
    {
        return switch (transform.toLowerCase(Locale.ENGLISH)) {
            case YEAR_TRANSFORM -> timestamp.getYear();
            case MONTH_TRANSFORM -> timestamp.getMonthValue();
            case DAY_TRANSFORM -> timestamp.getDayOfMonth();
            case HOUR_TRANSFORM -> timestamp.getHour();
            default -> throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Unsupported partition transform: " + transform);
        };
    }

    /**
     * Checks that a column can be partitioned by the given transform, so that an unusable
     * partitioning is rejected when the table is defined rather than when it is first written to.
     */
    public static void validateTransform(String transform, String columnName, Type type)
    {
        String normalized = transform.toLowerCase(Locale.ENGLISH);
        if (normalized.equals(IDENTITY_TRANSFORM)) {
            if (!DuckLakePartitionValues.isPartitionable(type)) {
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Column '%s' of type %s cannot be a partition key".formatted(columnName, type));
            }
            return;
        }
        if (!normalized.equals(YEAR_TRANSFORM) && !normalized.equals(MONTH_TRANSFORM) && !normalized.equals(DAY_TRANSFORM) && !normalized.equals(HOUR_TRANSFORM)) {
            throw new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "Unsupported partition transform: " + transform);
        }
        if (!DATE.equals(type) && !(type instanceof TimestampType)) {
            // the transform of a value with a time zone depends on the zone it is read in, which
            // would make the recorded partition value disagree with what another engine computes
            throw new TrinoException(
                    DUCKLAKE_UNSUPPORTED_TYPE,
                    "Partition transform %s requires a DATE or TIMESTAMP column; column '%s' is %s".formatted(transform, columnName, type));
        }
    }
}
