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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record DuckLakeDataFileEntry(
        long dataFileId,
        String path,
        boolean pathIsRelative,
        String fileFormat,
        long recordCount,
        long fileSizeBytes,
        OptionalLong footerSize,
        OptionalLong rowIdStart,
        OptionalLong partitionId,
        Optional<String> encryptionKey,
        OptionalLong mappingId,
        OptionalLong partialMax,
        Map<Integer, Optional<String>> partitionValues)
{
    public DuckLakeDataFileEntry
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(rowIdStart, "rowIdStart is null");
        requireNonNull(partitionId, "partitionId is null");
        requireNonNull(encryptionKey, "encryptionKey is null");
        requireNonNull(mappingId, "mappingId is null");
        requireNonNull(partialMax, "partialMax is null");
        partitionValues = ImmutableMap.copyOf(partitionValues);
    }
}
