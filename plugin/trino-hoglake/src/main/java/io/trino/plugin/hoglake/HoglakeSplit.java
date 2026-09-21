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
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * One split per data file from GET /scan, carrying the deletion vector the
 * scan paired with that file at the query's pinned snapshot. The page
 * source reads the vector from object storage and drops the rows it marks
 * deleted.
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
        @JsonProperty("deleteFileFormat") Optional<String> deleteFileFormat)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(HoglakeSplit.class);

    @JsonCreator
    public HoglakeSplit
    {
        requireNonNull(path, "path is null");
        requireNonNull(deleteFilePath, "deleteFilePath is null");
        requireNonNull(deleteFileFormat, "deleteFileFormat is null");
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

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + sizeOf(deleteFilePath, SizeOf::estimatedSizeOf)
                + sizeOf(deleteFileFormat, SizeOf::estimatedSizeOf);
    }
}
