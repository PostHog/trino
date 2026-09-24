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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * One byte range of a data file from GET /scan, carrying the deletion
 * vector the scan paired with that file at the query's pinned snapshot.
 * The page source reads the vector from object storage and drops the rows
 * it marks deleted.
 *
 * <p>A split covers {@code [start, start + length)} of the file. The worker
 * reads the row groups whose first column chunk starts inside that range, so
 * the ranges of one file together read every row group exactly once. A file
 * no larger than the target split size is a single split covering the whole
 * file.
 *
 * <p>{@code recordCount} is the catalog's row count for the whole file, or
 * {@code -1} when the catalog has none. Every range of a file carries it, so
 * the range that starts at offset 0 can answer a metadata count for the whole
 * file; it never describes the rows inside a range.
 *
 * <p>{@code deleteCount} is the catalog's count of rows deleted from the
 * data file. It is compared against the decoded bitmap before a single row
 * is dropped, so a catalog that disagrees with its own vector fails the
 * query instead of returning a wrong row set.
 */
public record HoglakeSplit(
        @JsonProperty("dataFileId") long dataFileId,
        @JsonProperty("path") String path,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
        @JsonProperty("deleteCount") long deleteCount,
        @JsonProperty("deleteFileFormat") Optional<String> deleteFileFormat,
        @JsonProperty("start") long start,
        @JsonProperty("length") long length,
        @JsonProperty("splitWeight") SplitWeight splitWeight,
        @JsonProperty("footerSize") OptionalLong footerSize,
        @JsonProperty("affinityKey") Optional<String> affinityKey)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(HoglakeSplit.class);

    @JsonCreator
    public HoglakeSplit
    {
        requireNonNull(path, "path is null");
        requireNonNull(deleteFilePath, "deleteFilePath is null");
        requireNonNull(deleteFileFormat, "deleteFileFormat is null");
        requireNonNull(splitWeight, "splitWeight is null");
        requireNonNull(footerSize, "footerSize is null");
        requireNonNull(affinityKey, "affinityKey is null");
        checkArgument(start >= 0, "start is negative: %s", start);
        checkArgument(length >= 0, "length is negative: %s", length);
        checkArgument(start + length <= fileSizeBytes, "range [%s, %s) exceeds file size %s", start, start + length, fileSizeBytes);
    }

    /**
     * A split covering a whole data file, with a standard weight, no
     * catalog footer size and no scheduling affinity.
     */
    public HoglakeSplit(
            long dataFileId,
            String path,
            long fileSizeBytes,
            long recordCount,
            Optional<String> deleteFilePath,
            long deleteCount,
            Optional<String> deleteFileFormat)
    {
        this(dataFileId, path, fileSizeBytes, recordCount, deleteFilePath, deleteCount, deleteFileFormat, 0, fileSizeBytes, SplitWeight.standard(), OptionalLong.empty(), Optional.empty());
    }

    /**
     * A split for a data file without a deletion vector, or for callers that
     * do not carry the wire's {@code file_format}.
     */
    public HoglakeSplit(String path, long fileSizeBytes, long recordCount, Optional<String> deleteFilePath, long deleteCount)
    {
        this(0, path, fileSizeBytes, recordCount, deleteFilePath, deleteCount, Optional.empty());
    }

    public HoglakeSplit(String path, long fileSizeBytes, long recordCount, Optional<String> deleteFilePath, long deleteCount, Optional<String> deleteFileFormat)
    {
        this(0, path, fileSizeBytes, recordCount, deleteFilePath, deleteCount, deleteFileFormat);
    }

    /**
     * The same file restricted to {@code [start, start + length)}. The range
     * keeps the catalog's whole-file record count, so the range starting at
     * offset 0 can answer a metadata count for the file.
     */
    public HoglakeSplit withRange(long start, long length, SplitWeight splitWeight)
    {
        return withRange(start, length, splitWeight, Optional.empty());
    }

    /**
     * The same file restricted to {@code [start, start + length)}, scheduled
     * with the given affinity key (see {@link #getAffinityKey()}).
     */
    public HoglakeSplit withRange(long start, long length, SplitWeight splitWeight, Optional<String> affinityKey)
    {
        return new HoglakeSplit(dataFileId, path, fileSizeBytes, recordCount, deleteFilePath, deleteCount, deleteFileFormat, start, length, splitWeight, footerSize, affinityKey);
    }

    /**
     * Whether this split reads every row group of its file, which is when the
     * catalog's whole-file record count describes it.
     */
    public boolean wholeFile()
    {
        return start == 0 && length == fileSizeBytes;
    }

    /**
     * Object-store data: any worker can read any split.
     */
    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    /**
     * The key the split manager obtained from the filesystem's
     * {@code SplitAffinityProvider}: present only when the catalog caches
     * filesystem data, so that the same byte range of the same file is
     * preferably scheduled on the worker whose cache holds it. Empty for an
     * uncached catalog, which leaves scheduling unconstrained.
     */
    @Override
    public Optional<String> getAffinityKey()
    {
        return affinityKey;
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
                + sizeOf(deleteFilePath, SizeOf::estimatedSizeOf)
                + sizeOf(deleteFileFormat, SizeOf::estimatedSizeOf)
                + splitWeight.getRetainedSizeInBytes()
                + sizeOf(footerSize)
                + sizeOf(affinityKey, SizeOf::estimatedSizeOf);
    }
}
