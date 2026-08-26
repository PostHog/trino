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

import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * @param footerSize length of the Parquet footer of the file as the catalog records it, excluding
 *         the postscript that follows it
 */
public record DuckLakeDeleteFileHandle(
        String path,
        long fileSizeBytes,
        OptionalLong footerSize,
        long deleteCount)
{
    private static final int INSTANCE_SIZE = (int) instanceSize(DuckLakeDeleteFileHandle.class);

    public DuckLakeDeleteFileHandle
    {
        requireNonNull(path, "path is null");
        requireNonNull(footerSize, "footerSize is null");
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(path) + sizeOf(footerSize);
    }
}
