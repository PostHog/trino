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
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * The properties a DuckLake table can be created with.
 * <p>
 * {@code partitioning} lists the keys rows are filed under, each either a column name or a
 * transform applied to one, written as {@code year(created_at)}. It is the same vocabulary
 * DuckDB's {@code ALTER TABLE ... SET PARTITIONED BY} accepts, so a table partitioned by either
 * engine describes itself the same way to the other.
 */
public class DuckLakeTableProperties
{
    public static final String PARTITIONING_PROPERTY = "partitioning";

    private static final ArrayType VARCHAR_ARRAY = new ArrayType(VARCHAR);

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DuckLakeTableProperties()
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition keys, each a column name or a transform of one such as year(created_at)",
                        VARCHAR_ARRAY,
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(String.class::cast)
                                .collect(toImmutableList()),
                        value -> value));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitioning(Map<String, Object> tableProperties)
    {
        List<String> partitioning = (List<String>) tableProperties.get(PARTITIONING_PROPERTY);
        return partitioning == null ? ImmutableList.of() : ImmutableList.copyOf(partitioning);
    }

    /**
     * Splits one partition key into the column it reads and the transform applied to it. A bare
     * column name is the identity transform.
     */
    public static PartitionKey parsePartitionKey(String key)
    {
        String trimmed = key.trim();
        int open = trimmed.indexOf('(');
        if (open < 0) {
            return new PartitionKey(trimmed, DuckLakeWritePartitioner.IDENTITY_TRANSFORM);
        }
        if (!trimmed.endsWith(")")) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid partition key: " + key);
        }
        String transform = trimmed.substring(0, open).trim().toLowerCase(Locale.ENGLISH);
        String columnName = trimmed.substring(open + 1, trimmed.length() - 1).trim();
        if (transform.isEmpty() || columnName.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid partition key: " + key);
        }
        return new PartitionKey(columnName, transform);
    }

    /**
     * Renders a partition key the way it is written in the table property.
     */
    public static String formatPartitionKey(String columnName, String transform)
    {
        if (transform.equalsIgnoreCase(DuckLakeWritePartitioner.IDENTITY_TRANSFORM)) {
            return columnName;
        }
        return transform.toLowerCase(Locale.ENGLISH) + "(" + columnName + ")";
    }

    public record PartitionKey(String columnName, String transform)
    {
        public Optional<String> asColumnName()
        {
            if (transform.equals(DuckLakeWritePartitioner.IDENTITY_TRANSFORM)) {
                return Optional.of(columnName);
            }
            return Optional.empty();
        }
    }
}
