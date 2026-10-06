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

import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * The positions of the rows of a data file that DuckDB deleted by recording them in the catalog
 * database rather than in a delete file. Like the positions of a delete file, they are indexes of
 * rows within the data file, counted from zero.
 */
public final class DuckLakeInlinedDeletions
{
    private static final int INSTANCE_SIZE = instanceSize(DuckLakeInlinedDeletions.class);

    private final long[] positions;

    /**
     * @param positions the deleted positions, sorted and free of duplicates
     */
    @JsonCreator
    public DuckLakeInlinedDeletions(@JsonProperty("positions") long[] positions)
    {
        requireNonNull(positions, "positions is null");
        for (int index = 0; index < positions.length; index++) {
            checkArgument(positions[index] >= 0, "position is negative: %s", positions[index]);
            checkArgument(index == 0 || positions[index - 1] < positions[index], "positions are not sorted and distinct");
        }
        this.positions = positions.clone();
    }

    @JsonProperty
    public long[] positions()
    {
        return positions.clone();
    }

    public int size()
    {
        return positions.length;
    }

    public long position(int index)
    {
        return positions[index];
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positions);
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof DuckLakeInlinedDeletions that && Arrays.equals(positions, that.positions);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(positions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positions", positions.length)
                .toString();
    }
}
