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
 * A set of whole, adjacent Parquet row groups from a data file. New splits carry the selected row
 * group metadata, while {@code start} and {@code length} preserve compatibility with older splits
 * that selected row groups by byte range.
 *
 * @param dataFileId identifier of the data file, used to address it when rows of it are deleted
 * @param fileSizeBytes size of the whole file
 * @param recordCount number of stored rows in the selected row groups, or exact visible rows for a
 *         metadata-only whole-file split
 * @param rowGroupMetadata file schema and metadata for only the row groups assigned to this split
 */
public record DuckLakeSplit(
        long dataFileId,
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
        SplitWeight splitWeight,
        Optional<DuckLakeRowGroupMetadata> rowGroupMetadata)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(DuckLakeSplit.class));

    public DuckLakeSplit
    {
        requireNonNull(path, "path is null");
        checkArgument(fileSizeBytes >= 0, "fileSizeBytes is negative: %s", fileSizeBytes);
        checkArgument(start >= 0, "start is negative: %s", start);
        checkArgument(length >= 0, "length is negative: %s", length);
        checkArgument(start <= fileSizeBytes - length, "byte range [%s, %s) exceeds the file size %s", start, start + length, fileSizeBytes);
        checkArgument(recordCount >= 0, "recordCount is negative: %s", recordCount);
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(rowIdStart, "rowIdStart is null");
        requireNonNull(deleteFile, "deleteFile is null");
        partitionValues = ImmutableMap.copyOf(partitionValues);
        requireNonNull(nameMapping, "nameMapping is null");
        requireNonNull(splitWeight, "splitWeight is null");
        requireNonNull(rowGroupMetadata, "rowGroupMetadata is null");
        checkArgument(rowGroupMetadata.isEmpty() || (start == 0 && length == fileSizeBytes),
                "row-group metadata requires a whole-file byte range");
    }

    public DuckLakeSplit(
            long dataFileId,
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
    {
        this(dataFileId,
                path,
                start,
                length,
                fileSizeBytes,
                footerSize,
                recordCount,
                rowIdStart,
                deleteFile,
                partitionValues,
                nameMapping,
                splitWeight,
                Optional.empty());
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
                + splitWeight.getRetainedSizeInBytes()
                + sizeOf(rowGroupMetadata, DuckLakeRowGroupMetadata::retainedSizeInBytes);
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
