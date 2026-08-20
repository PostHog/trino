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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A byte range {@code [start, start + length)} of a data file. The Parquet reader restricts
 * itself to the row groups that begin inside the range, so the splits of a file must cover it
 * without gaps or overlaps for the file to be read exactly once.
 *
 * @param fileSizeBytes size of the whole file, not of the byte range
 * @param recordCount number of visible rows of the byte range, apportioned by its share of the file
 */
public record DuckLakeSplit(
        String path,
        long start,
        long length,
        long fileSizeBytes,
        OptionalLong footerSize,
        long recordCount,
        OptionalLong rowIdStart,
        Optional<DuckLakeDeleteFileHandle> deleteFile,
        Map<Integer, Optional<String>> partitionValues,
        Optional<DuckLakeNameMapping> nameMapping,
        SplitWeight splitWeight)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeSplit.class));

    public DuckLakeSplit
    {
        requireNonNull(path, "path is null");
        checkArgument(start >= 0, "start is negative: %s", start);
        checkArgument(length >= 0, "length is negative: %s", length);
        checkArgument(start + length <= fileSizeBytes, "byte range [%s, %s) exceeds the file size %s", start, start + length, fileSizeBytes);
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(rowIdStart, "rowIdStart is null");
        requireNonNull(deleteFile, "deleteFile is null");
        partitionValues = ImmutableMap.copyOf(partitionValues);
        requireNonNull(nameMapping, "nameMapping is null");
        requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + sizeOf(footerSize)
                + sizeOf(rowIdStart)
                + sizeOf(deleteFile, DuckLakeDeleteFileHandle::retainedSizeInBytes)
                + estimatedSizeOf(partitionValues, SizeOf::sizeOf, value -> sizeOf(value, SizeOf::estimatedSizeOf))
                + sizeOf(nameMapping, DuckLakeNameMapping::retainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(fileSizeBytes)
                .toString();
    }
}
