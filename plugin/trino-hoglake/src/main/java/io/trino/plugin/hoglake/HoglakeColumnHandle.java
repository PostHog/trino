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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * A hoglake column. {@code fieldId} is the catalog's stable field id —
 * the same id writers embed into parquet as PARQUET:field_id — and is
 * the primary binding between catalog columns and file columns; name
 * binding is the fallback for files written without ids.
 */
public record HoglakeColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("fieldId") long fieldId,
        @JsonProperty("type") Type type,
        @JsonProperty("nullable") boolean nullable)
        implements ColumnHandle
{
    @JsonCreator
    public HoglakeColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .build();
    }

    @Override
    public String toString()
    {
        return name + ":" + type.getDisplayName();
    }
}
