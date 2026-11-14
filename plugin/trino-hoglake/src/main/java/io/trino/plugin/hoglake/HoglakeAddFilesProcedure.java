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

import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;

import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * {@code ALTER TABLE ... EXECUTE add_files(...)}. The name and the property names match the DuckLake
 * connector's procedure, so one access-control rule on {@code ADD_FILES} covers both.
 */
public final class HoglakeAddFilesProcedure
{
    public static final String NAME = "ADD_FILES";

    private HoglakeAddFilesProcedure() {}

    public static TableProcedureMetadata metadata()
    {
        return new TableProcedureMetadata(
                NAME,
                coordinatorOnly(),
                List.of(
                        stringProperty("location", "Directory whose Parquet files are registered, listed recursively", null, false),
                        new PropertyMetadata<>("files", "Explicit Parquet file locations", new ArrayType(VARCHAR), List.class, null, false, value -> (List<?>) value, value -> value),
                        stringProperty("file_prefix", "Register only the files of the location whose names start with this", null, false),
                        stringProperty("import_id", "Stable identifier for retries of the same import", null, false),
                        longProperty("expected_rows", "Refuse unless the files to register hold exactly this many rows", null, false),
                        booleanProperty("allow_missing", "Read table columns that a file lacks as NULL when they are nullable", false, false),
                        booleanProperty("delete_on_failure", "Delete the listed files when validation refuses them before any commit is sent", false, false),
                        booleanProperty("replace", "End every data file of the table in the registration snapshot", false, false),
                        stringProperty("replace_range_column", "Column of the day partition whose days the registration replaces", null, false),
                        stringProperty("replace_range_start", "Inclusive UTC day boundary", null, false),
                        stringProperty("replace_range_end", "Exclusive UTC day boundary", null, false)));
    }
}
