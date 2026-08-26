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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.trino.plugin.ducklake.DuckLakeWriteColumn;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeColumnRow;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Multimaps.index;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;

/**
 * Translates between the columns of a Trino table and the rows of {@code ducklake_column}.
 * <p>
 * DuckLake numbers columns per table rather than per catalog, and gives every field of a nested
 * type a number of its own, stored as a row pointing at its enclosing column. Identifiers are
 * never reused: a dropped column keeps its number reserved, because data files written while it
 * existed still refer to it.
 */
public final class DuckLakeColumns
{
    private DuckLakeColumns() {}

    /**
     * Builds the column tree of a new table, numbering every field in the order it is declared.
     */
    public static List<DuckLakeWriteColumn> assignColumnIds(List<ColumnMetadata> columns)
    {
        ColumnIdAllocator allocator = new ColumnIdAllocator(0);
        return columns.stream()
                .map(column -> buildColumn(column.getName(), column.getType(), column.isNullable(), allocator))
                .collect(toImmutableList());
    }

    /**
     * Builds the tree of one column being added to an existing table, numbering it above every
     * identifier the table has ever used.
     */
    public static DuckLakeWriteColumn assignColumnIds(ColumnMetadata column, long maxExistingColumnId)
    {
        return buildColumn(column.getName(), column.getType(), column.isNullable(), new ColumnIdAllocator(maxExistingColumnId));
    }

    private static DuckLakeWriteColumn buildColumn(String name, Type type, boolean nullable, ColumnIdAllocator allocator)
    {
        long columnId = allocator.next();
        String duckLakeType = DuckLakeTypes.toDuckLakeType(type);
        List<DuckLakeWriteColumn> children = switch (type) {
            case ArrayType arrayType -> ImmutableList.of(
                    buildColumn(DuckLakeParquetSchema.listElementName(), arrayType.getElementType(), true, allocator));
            case MapType mapType -> ImmutableList.of(
                    buildColumn(DuckLakeParquetSchema.mapKeyName(), mapType.getKeyType(), false, allocator),
                    buildColumn(DuckLakeParquetSchema.mapValueName(), mapType.getValueType(), true, allocator));
            case RowType rowType -> rowType.getFields().stream()
                    .map(field -> buildColumn(
                            field.getName().orElseThrow(() -> new TrinoException(DUCKLAKE_UNSUPPORTED_TYPE, "DuckLake does not support anonymous row fields: " + rowType)),
                            field.getType(),
                            true,
                            allocator))
                    .collect(toImmutableList());
            default -> ImmutableList.of();
        };
        return new DuckLakeWriteColumn(columnId, name, duckLakeType, type, nullable, children);
    }

    /**
     * The rows describing the column tree, in the order they have to be inserted so that a parent
     * always precedes its fields.
     */
    public static List<DuckLakeColumnRow> toColumnRows(List<DuckLakeWriteColumn> columns)
    {
        ImmutableList.Builder<DuckLakeColumnRow> rows = ImmutableList.builder();
        for (DuckLakeWriteColumn column : columns) {
            addColumnRows(column, OptionalLong.empty(), rows);
        }
        return rows.build();
    }

    public static List<DuckLakeColumnRow> toColumnRows(DuckLakeWriteColumn column)
    {
        ImmutableList.Builder<DuckLakeColumnRow> rows = ImmutableList.builder();
        addColumnRows(column, OptionalLong.empty(), rows);
        return rows.build();
    }

    private static void addColumnRows(DuckLakeWriteColumn column, OptionalLong parentColumn, ImmutableList.Builder<DuckLakeColumnRow> rows)
    {
        rows.add(new DuckLakeColumnRow(
                column.columnId(),
                // the identifier doubles as the position, which orders sibling fields and top-level columns alike
                column.columnId(),
                column.name(),
                column.duckLakeType(),
                parentColumn,
                column.nullsAllowed(),
                Optional.empty(),
                Optional.empty()));
        for (DuckLakeWriteColumn child : column.children()) {
            addColumnRows(child, OptionalLong.of(column.columnId()), rows);
        }
    }

    /**
     * Rebuilds the column tree of an existing table from the rows visible at a snapshot.
     */
    public static List<DuckLakeWriteColumn> fromCatalog(List<DuckLakeColumnEntry> entries)
    {
        ListMultimap<Long, DuckLakeColumnEntry> childrenByParent = index(
                entries.stream()
                        .filter(entry -> entry.parentColumn().isPresent())
                        .collect(toImmutableList()),
                entry -> entry.parentColumn().orElseThrow());
        return entries.stream()
                .filter(entry -> entry.parentColumn().isEmpty())
                .map(entry -> toWriteColumn(entry, childrenByParent))
                .collect(toImmutableList());
    }

    private static DuckLakeWriteColumn toWriteColumn(DuckLakeColumnEntry entry, ListMultimap<Long, DuckLakeColumnEntry> childrenByParent)
    {
        List<DuckLakeWriteColumn> children = childrenByParent.get(entry.columnId()).stream()
                .map(child -> toWriteColumn(child, childrenByParent))
                .collect(toImmutableList());
        return new DuckLakeWriteColumn(
                entry.columnId(),
                entry.columnName(),
                entry.columnType(),
                DuckLakeTypes.toTrinoType(entry, childrenByParent),
                entry.nullsAllowed(),
                children);
    }

    /**
     * Finds one top-level column of the table by its Trino (lowercase) name.
     */
    public static DuckLakeColumnEntry findColumn(List<DuckLakeColumnEntry> entries, String columnName)
    {
        List<DuckLakeColumnEntry> matches = new ArrayList<>();
        for (DuckLakeColumnEntry entry : entries) {
            if (entry.parentColumn().isEmpty() && entry.columnName().equalsIgnoreCase(columnName)) {
                matches.add(entry);
            }
        }
        if (matches.size() > 1) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Ambiguous column name '%s': multiple columns differ only in case".formatted(columnName));
        }
        if (matches.isEmpty()) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Column '%s' does not exist".formatted(columnName));
        }
        return matches.getFirst();
    }

    private static final class ColumnIdAllocator
    {
        private long nextColumnId;

        private ColumnIdAllocator(long maxExistingColumnId)
        {
            this.nextColumnId = maxExistingColumnId + 1;
        }

        private long next()
        {
            return nextColumnId++;
        }
    }
}
