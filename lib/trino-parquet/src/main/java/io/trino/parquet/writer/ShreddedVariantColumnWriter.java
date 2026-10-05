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
package io.trino.parquet.writer;

import io.trino.parquet.variant.VariantShredder;
import io.trino.spi.block.Block;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/// Writes a VARIANT column as a shredded VARIANT group. Each block of VARIANT values is
/// split into the row of the group's columns, which a writer of that row writes.
public class ShreddedVariantColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(ShreddedVariantColumnWriter.class);

    private final VariantShredder shredder;
    private final ColumnWriter groupWriter;
    private final boolean required;

    /// @param groupWriter the writer of a row of the `metadata`, `value`, and `typed_value`
    ///         columns, in that order
    /// @param required whether the VARIANT group is `required`
    public ShreddedVariantColumnWriter(VariantShredder shredder, ColumnWriter groupWriter, boolean required)
    {
        this.shredder = requireNonNull(shredder, "shredder is null");
        this.groupWriter = requireNonNull(groupWriter, "groupWriter is null");
        this.required = required;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        Block block = columnChunk.getBlock();
        if (required && block.mayHaveNull()) {
            // A null in a required group would be written with an invalid definition level
            for (int position = 0; position < block.getPositionCount(); position++) {
                checkArgument(!block.isNull(position), "Required shredded VARIANT column has a NULL value");
            }
        }
        groupWriter.writeBlock(new ColumnChunk(
                shredder.shred(block),
                columnChunk.getDefLevelWriterProviders(),
                columnChunk.getRepLevelWriterProviders()));
    }

    @Override
    public void close()
    {
        groupWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        return groupWriter.getBuffer();
    }

    @Override
    public long getEstimatedBufferedBytes(CompressionStats compressionStats)
    {
        return groupWriter.getEstimatedBufferedBytes(compressionStats);
    }

    @Override
    public CompressionStats getCompressionStats()
    {
        return groupWriter.getCompressionStats();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + groupWriter.getRetainedBytes();
    }
}
