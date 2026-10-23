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
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;

import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class DuckLakeAddFilesProcedure
{
    public static final String NAME = "ADD_FILES";

    private DuckLakeAddFilesProcedure() {}

    public static TableProcedureMetadata metadata()
    {
        return new TableProcedureMetadata(
                NAME,
                coordinatorOnly(),
                ImmutableList.of(
                        new PropertyMetadata<>("files", "Explicit immutable Parquet file locations", new ArrayType(VARCHAR), List.class, null, false, value -> (List<?>) value, value -> value),
                        stringProperty("import_id", "Stable identifier for retries of the same import", null, false),
                        booleanProperty("allow_missing", "Read absent nullable columns as NULL", false, false),
                        booleanProperty("ignore_extra_columns", "Ignore file columns absent from the table", false, false),
                        booleanProperty("hive_partitioning", "Read partition values from Hive-style paths", false, false),
                        booleanProperty("replace", "Replace the entire table contents in the registration snapshot", false, false),
                        stringProperty("replace_range_column", "Top-level column defining the replacement interval", null, false),
                        stringProperty("replace_range_start", "Inclusive lower bound in the column type's catalog string format", null, false),
                        stringProperty("replace_range_end", "Exclusive upper bound in the column type's catalog string format", null, false)));
    }
}
