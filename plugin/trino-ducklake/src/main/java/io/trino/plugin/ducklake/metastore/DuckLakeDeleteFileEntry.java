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
package io.trino.plugin.ducklake.metastore;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record DuckLakeDeleteFileEntry(
        long deleteFileId,
        long dataFileId,
        String path,
        boolean pathIsRelative,
        String format,
        long deleteCount,
        long fileSizeBytes,
        OptionalLong footerSize,
        Optional<String> encryptionKey)
{
    public DuckLakeDeleteFileEntry
    {
        requireNonNull(path, "path is null");
        requireNonNull(format, "format is null");
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
    }
}
