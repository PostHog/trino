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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * One worker's contribution to a row-level change: either a data file holding rows it wrote, or
 * the positions of rows it removed from an existing data file.
 * <p>
 * Removed positions are carried as a packed array rather than as JSON numbers, because a delete
 * that spares part of a file produces one position per row and the fragments of a large delete
 * would otherwise dominate the work of finishing the statement.
 */
public record DuckLakeMergeFragment(
        @JsonProperty Optional<DuckLakeDataFile> insertedFile,
        @JsonProperty Optional<DeletedRows> deletedRows)
{
    @JsonCreator
    public DuckLakeMergeFragment
    {
        requireNonNull(insertedFile, "insertedFile is null");
        requireNonNull(deletedRows, "deletedRows is null");
        checkArgument(insertedFile.isPresent() != deletedRows.isPresent(), "a fragment holds either an inserted file or deleted rows");
    }

    public static DuckLakeMergeFragment inserted(DuckLakeDataFile dataFile)
    {
        return new DuckLakeMergeFragment(Optional.of(dataFile), Optional.empty());
    }

    public static DuckLakeMergeFragment deleted(long dataFileId, long[] positions)
    {
        return new DuckLakeMergeFragment(Optional.empty(), Optional.of(new DeletedRows(dataFileId, pack(positions))));
    }

    public record DeletedRows(@JsonProperty long dataFileId, @JsonProperty String positions)
    {
        @JsonCreator
        public DeletedRows
        {
            requireNonNull(positions, "positions is null");
        }

        public long[] unpack()
        {
            byte[] bytes = Base64.getDecoder().decode(positions);
            long[] values = new long[bytes.length / Long.BYTES];
            ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(values);
            return values;
        }
    }

    private static String pack(long[] positions)
    {
        ByteBuffer buffer = ByteBuffer.allocate(positions.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.asLongBuffer().put(positions);
        return Base64.getEncoder().encodeToString(buffer.array());
    }
}
