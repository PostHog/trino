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

import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Identifies a row of a DuckLake table well enough to delete it.
 * <p>
 * DuckLake deletes are positional: a delete file names a data file and the ordinals of the rows
 * of it that are gone. A row is therefore identified by the file it lives in and its position in
 * that file. The file is named by its catalog identifier rather than its path, which keeps the
 * value small and lets the commit resolve the path once per file instead of once per row.
 */
public final class DuckLakeMergeRowId
{
    /**
     * Identifier of the synthetic column, outside the range of the per-table column numbering so
     * that it can never collide with a column of the table.
     */
    private static final long COLUMN_ID = -1;
    private static final String COLUMN_NAME = "$row_id";
    private static final String DATA_FILE_ID_FIELD = "data_file_id";
    private static final String FILE_ROW_POSITION_FIELD = "file_row_position";

    public static final int DATA_FILE_ID_CHANNEL = 0;
    public static final int FILE_ROW_POSITION_CHANNEL = 1;

    public static final RowType TYPE = RowType.rowType(
            RowType.field(DATA_FILE_ID_FIELD, BIGINT),
            RowType.field(FILE_ROW_POSITION_FIELD, BIGINT));

    private DuckLakeMergeRowId() {}

    public static DuckLakeColumnHandle columnHandle()
    {
        return new DuckLakeColumnHandle(COLUMN_ID, COLUMN_NAME, "struct", TYPE, false, Optional.empty());
    }

    public static boolean isRowIdColumn(DuckLakeColumnHandle column)
    {
        return column.columnId() == COLUMN_ID;
    }

    public static boolean isRowIdColumn(Type type)
    {
        return TYPE.equals(type);
    }
}
