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

/**
 * @param partialMax the newest snapshot whose deletions the file holds, present when the file holds
 *         deletions of several snapshots, each tagged with the snapshot that made it. Such a file is
 *         visible from its first snapshot on, but only applies in full from this one.
 */
public record DuckLakeDeleteFileEntry(
        long deleteFileId,
        long dataFileId,
        String path,
        boolean pathIsRelative,
        String format,
        long deleteCount,
        long fileSizeBytes,
        OptionalLong footerSize,
        Optional<String> encryptionKey,
        OptionalLong partialMax)
{
    public DuckLakeDeleteFileEntry
    {
        requireNonNull(path, "path is null");
        requireNonNull(format, "format is null");
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
        requireNonNull(partialMax, "partialMax is null");
    }
}
