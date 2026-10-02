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
import com.google.common.collect.ImmutableList;
import io.trino.parquet.variant.VariantPaths;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * A hoglake column. {@code fieldId} is the catalog's stable field id —
 * the same id writers embed into parquet as PARQUET:field_id — and is
 * the primary binding between catalog columns and file columns; name
 * binding is the fallback for files written without ids.
 *
 * <p>A VARIANT column with {@code variantPaths} holds only the parts of
 * each value that those paths read; see {@link HoglakeVariantProjections}.
 */
public record HoglakeColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("fieldId") long fieldId,
        @JsonProperty("type") Type type,
        @JsonProperty("nullable") boolean nullable,
        @JsonProperty("children") List<HoglakeColumnHandle> children,
        @JsonProperty("hoglakeType") String hoglakeType,
        @JsonProperty("comment") String comment,
        @JsonProperty("variantPaths") List<List<HoglakeVariantPathStep>> variantPaths)
        implements ColumnHandle
{
    static final HoglakeColumnHandle ROW_ID = new HoglakeColumnHandle(
            "$row_id",
            -1,
            RowType.anonymous(List.of(BIGINT, BIGINT)),
            false);

    public HoglakeColumnHandle(String name, long fieldId, Type type, boolean nullable)
    {
        this(name, fieldId, type, nullable, List.of(), null);
    }

    public HoglakeColumnHandle(String name, long fieldId, Type type, boolean nullable, List<HoglakeColumnHandle> children, String hoglakeType)
    {
        this(name, fieldId, type, nullable, children, hoglakeType, null);
    }

    public HoglakeColumnHandle(String name, long fieldId, Type type, boolean nullable, List<HoglakeColumnHandle> children, String hoglakeType, String comment)
    {
        this(name, fieldId, type, nullable, children, hoglakeType, comment, List.of());
    }

    @JsonCreator
    public HoglakeColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        hoglakeType = hoglakeType == null ? HoglakeTypes.toHoglakeType(type) : hoglakeType;
        children = children == null ? List.of() : ImmutableList.copyOf(children);
        variantPaths = variantPaths == null ? List.of() : variantPaths.stream().map(ImmutableList::copyOf).collect(toImmutableList());
        checkArgument(variantPaths.isEmpty() || type.equals(VARIANT), "Only a VARIANT column has variant paths: %s", name);
    }

    /**
     * The same column, holding only the parts of each value that the paths read.
     */
    public HoglakeColumnHandle withVariantPaths(List<List<HoglakeVariantPathStep>> paths)
    {
        checkArgument(!paths.isEmpty(), "paths is empty");
        return new HoglakeColumnHandle(name, fieldId, type, nullable, children, hoglakeType, comment, paths);
    }

    /**
     * The tree of {@link #variantPaths}, or empty for a whole column.
     */
    public Optional<VariantPaths> variantPathTree()
    {
        if (variantPaths.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(VariantPaths.of(variantPaths.stream()
                .map(path -> path.stream().map(HoglakeVariantPathStep::toVariantPathStep).toList())
                .toList()));
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .setComment(Optional.ofNullable(comment))
                .build();
    }

    @Override
    public String toString()
    {
        if (variantPaths.isEmpty()) {
            return name + ":" + type.getDisplayName();
        }
        return name + ":" + type.getDisplayName() + " pruned to " + variantPaths.stream()
                .map(path -> path.stream().map(HoglakeVariantPathStep::toString).collect(joining()))
                .collect(joining(", ", "[", "]"));
    }
}
