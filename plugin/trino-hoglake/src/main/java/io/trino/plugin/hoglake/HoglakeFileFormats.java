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

import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;

import java.util.Map;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

final class HoglakeFileFormats
{
    static final String PARQUET = "parquet";
    static final String CLICKHOUSE_MERGETREE_PACKED = "clickhouse-mergetree-packed";
    static final String WRITE_FORMAT_DEFAULT_PROPERTY = "write.format.default";

    private HoglakeFileFormats() {}

    static void checkReadableTable(HoglakeDtos.Table table)
    {
        checkTableProperties(table.properties(), table.namespace() + "." + table.name());
    }

    static void checkTableProperties(Map<String, String> properties, String table)
    {
        String format = properties.getOrDefault(WRITE_FORMAT_DEFAULT_PROPERTY, PARQUET);
        if (!PARQUET.equalsIgnoreCase(format)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Hoglake table %s uses unsupported format '%s'; Trino supports only '%s'"
                            .formatted(table, format, PARQUET));
        }
    }

    static void checkDataFile(String format, String path)
    {
        if (format == null) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "Hoglake data file %s has no file_format".formatted(path));
        }
        if (!PARQUET.equals(format)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Hoglake data file %s uses unsupported format '%s'; Trino supports only '%s'"
                            .formatted(path, format, PARQUET));
        }
    }

    static void checkWriterFile(String format, String path)
    {
        if (!PARQUET.equals(format)) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "Hoglake writer file %s uses unsupported format '%s'; Trino supports only '%s'"
                            .formatted(path, format, PARQUET));
        }
    }
}
