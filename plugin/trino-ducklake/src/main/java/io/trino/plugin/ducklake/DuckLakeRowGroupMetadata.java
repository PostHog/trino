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

import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.ParquetMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.Util.readFileMetaData;

/**
 * File schema and only the row groups assigned to a split, with their original file row offset.
 */
public record DuckLakeRowGroupMetadata(long firstRowIndex, boolean allRowGroups, byte[] metadata)
{
    private static final int INSTANCE_SIZE = instanceSize(DuckLakeRowGroupMetadata.class);

    public DuckLakeRowGroupMetadata
    {
        checkArgument(firstRowIndex >= 0, "firstRowIndex is negative: %s", firstRowIndex);
        metadata = requireNonNull(metadata, "metadata is null").clone();
    }

    @Override
    public byte[] metadata()
    {
        return metadata.clone();
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof DuckLakeRowGroupMetadata that
                && firstRowIndex == that.firstRowIndex
                && allRowGroups == that.allRowGroups
                && Arrays.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(firstRowIndex, allRowGroups, Arrays.hashCode(metadata));
    }

    public ParquetMetadata read(String path)
            throws IOException
    {
        return new ParquetMetadata(readFileMetaData(new ByteArrayInputStream(metadata)), new ParquetDataSourceId(path), Optional.empty(), firstRowIndex);
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(metadata);
    }
}
