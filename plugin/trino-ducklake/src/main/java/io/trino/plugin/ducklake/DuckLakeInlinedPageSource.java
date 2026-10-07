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
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedColumn;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeInlinedValues;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Reads the rows of a {@link DuckLakeInlinedSplit} from the catalog database.
 * <p>
 * Only the columns the scan asks for are read. A column the inlined data table does not hold,
 * because it was added to the table after the rows were written, reads as {@code NULL}, which is
 * what DuckDB returns for it; a column dropped since is not asked for and so not read. Every column
 * is read by its DuckLake identifier, so a column renamed since the rows were written still reads
 * the values written under its old name.
 */
public class DuckLakeInlinedPageSource
        implements ConnectorPageSource
{
    private static final int FETCH_SIZE = 1024;
    /**
     * Positions of a page holding no column, which carries only its position count.
     */
    private static final int MAX_EMPTY_PAGE_POSITIONS = 1024 * 1024;

    private final JdbcDuckLakeMetastore metastore;
    private final DuckLakeInlinedSplit split;
    private final List<Type> types;
    /**
     * For each requested column, the reader of its values and the index of the column read for it
     * in the query, counted from one, or {@code null} for a column read as {@code NULL}.
     */
    private final List<ColumnSource> sources;
    private final List<String> storedColumnNames;

    private JdbcDuckLakeMetastore.InlinedRows rows;
    private boolean finished;
    private long completedPositions;
    private long completedBytes;
    private long readTimeNanos;

    public DuckLakeInlinedPageSource(JdbcDuckLakeMetastore metastore, DuckLakeInlinedSplit split, List<DuckLakeColumnHandle> columns)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.split = requireNonNull(split, "split is null");
        Map<Long, DuckLakeInlinedColumn> storedColumns = split.columns().stream()
                .collect(toImmutableMap(DuckLakeInlinedColumn::columnId, identity()));

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<ColumnSource> sources = ImmutableList.builder();
        Map<String, Integer> queryColumns = new LinkedHashMap<>();
        for (DuckLakeColumnHandle column : columns) {
            if (DuckLakeMergeRowId.isRowIdColumn(column)) {
                // a merge is refused for a table with inlined rows before any of them is read
                throw new TrinoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Table %s.%s has inlined data, which cannot be modified".formatted(split.schemaName(), split.tableName()));
            }
            types.add(column.type());
            DuckLakeInlinedColumn stored = storedColumns.get(column.columnId());
            if (stored == null) {
                sources.add(new ColumnSource(null, 0));
                continue;
            }
            int index = queryColumns.computeIfAbsent(stored.name(), _ -> queryColumns.size() + 1);
            sources.add(new ColumnSource(DuckLakeInlinedValues.valueReader(column.name(), column.type(), stored.postgresType()), index));
        }
        this.types = types.build();
        this.sources = sources.build();
        this.storedColumnNames = ImmutableList.copyOf(queryColumns.keySet());
    }

    private record ColumnSource(DuckLakeInlinedValues.ValueReader reader, int index) {}

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (rows == null) {
                rows = metastore.openInlinedRows(
                        split.tableId(),
                        split.snapshotId(),
                        split.watermarkSnapshotId(),
                        split.inlinedTableName(),
                        storedColumnNames,
                        FETCH_SIZE);
            }
            Page page = types.isEmpty() ? countRows() : readRows();
            if (page == null) {
                return null;
            }
            completedPositions += page.getPositionCount();
            completedBytes += page.getSizeInBytes();
            return SourcePage.create(page);
        }
        catch (SQLException e) {
            throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Failed to read inlined data of table %s.%s from %s: %s".formatted(split.schemaName(), split.tableName(), split.inlinedTableName(), e.getMessage()), e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    private Page readRows()
            throws SQLException
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        while (!pageBuilder.isFull()) {
            if (!rows.next()) {
                finish();
                break;
            }
            ResultSet row = rows.row();
            pageBuilder.declarePosition();
            for (int channel = 0; channel < sources.size(); channel++) {
                ColumnSource source = sources.get(channel);
                BlockBuilder output = pageBuilder.getBlockBuilder(channel);
                if (source.reader() == null) {
                    output.appendNull();
                }
                else {
                    source.reader().read(row, source.index(), output);
                }
            }
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }
        return pageBuilder.build();
    }

    private Page countRows()
            throws SQLException
    {
        int positions = 0;
        while (positions < MAX_EMPTY_PAGE_POSITIONS) {
            if (!rows.next()) {
                finish();
                break;
            }
            positions++;
        }
        if (positions == 0) {
            return null;
        }
        return new Page(positions);
    }

    private void finish()
    {
        finished = true;
        closeRows();
    }

    private void closeRows()
    {
        if (rows != null) {
            rows.close();
            rows = null;
        }
    }

    @Override
    public void close()
    {
        finished = true;
        closeRows();
    }
}
