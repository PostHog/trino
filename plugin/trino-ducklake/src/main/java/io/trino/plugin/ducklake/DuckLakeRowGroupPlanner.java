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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.format.Util.writeFileMetaData;

final class DuckLakeRowGroupPlanner
{
    private DuckLakeRowGroupPlanner() {}

    public static List<RowGroupSplit> plan(ParquetMetadata metadata, long targetBytes)
            throws IOException
    {
        checkArgument(targetBytes > 0, "targetBytes must be positive: %s", targetBytes);
        // Validate the physical schema and obtain sizes from the individual column chunks. A
        // row group's total_byte_size is uncompressed and older writers omit total_compressed_size.
        FileMetaData file = metadata.getParquetMetadata();
        if (file.isSetEncryption_algorithm() || file.isSetFooter_signing_key_metadata()) {
            throw new IOException("Encrypted Parquet metadata is not supported");
        }
        List<BlockMetadata> blocks = metadata.getBlocks();
        List<RowGroup> rowGroups = Optional.ofNullable(file.getRow_groups()).orElse(ImmutableList.of());
        if (blocks.size() != rowGroups.size()) {
            throw new IOException("Parquet block count does not match row group count");
        }
        ImmutableList.Builder<RowGroupSplit> splits = ImmutableList.builder();
        List<RowGroup> selected = new ArrayList<>();
        long firstRowIndex = 0;
        long rows = 0;
        long bytes = 0;
        long fileRows = 0;
        for (int index = 0; index < blocks.size(); index++) {
            BlockMetadata block = blocks.get(index);
            if (block.rowCount() < 0) {
                throw new IOException("Parquet row group row count is negative: " + block.rowCount());
            }
            fileRows = addExact(fileRows, block.rowCount(), "Parquet file row count exceeds long range");
            if (block.rowCount() == 0) {
                continue;
            }
            long groupBytes = compressedSize(block);
            // Keep row groups indivisible even when one is larger than the target.
            if (!selected.isEmpty() && (bytes >= targetBytes || groupBytes > targetBytes - bytes)) {
                splits.add(split(file, selected, firstRowIndex, rows, bytes, false));
                firstRowIndex = addExact(firstRowIndex, rows, "Parquet row offset exceeds long range");
                selected.clear();
                rows = 0;
                bytes = 0;
            }
            selected.add(rowGroups.get(index));
            rows = addExact(rows, block.rowCount(), "Parquet split row count exceeds long range");
            bytes = addExact(bytes, groupBytes, "Parquet split compressed size exceeds long range");
        }
        if (fileRows != file.getNum_rows()) {
            throw new IOException("Parquet file row count %s does not match row group row count %s".formatted(file.getNum_rows(), fileRows));
        }
        if (!selected.isEmpty()) {
            splits.add(split(file, selected, firstRowIndex, rows, bytes, firstRowIndex == 0 && rows == fileRows));
        }
        return splits.build();
    }

    private static long compressedSize(BlockMetadata block)
            throws IOException
    {
        long bytes = 0;
        for (var column : block.columns()) {
            if (column.getTotalSize() < 0) {
                throw new IOException("Parquet column compressed size is negative: " + column.getTotalSize());
            }
            bytes = addExact(bytes, column.getTotalSize(), "Parquet row group compressed size exceeds long range");
        }
        return bytes;
    }

    private static long addExact(long left, long right, String message)
            throws IOException
    {
        try {
            return Math.addExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new IOException(message, e);
        }
    }

    private static RowGroupSplit split(FileMetaData file, List<RowGroup> groups, long firstRowIndex, long rows, long bytes, boolean allRowGroups)
            throws IOException
    {
        // Copy the file schema, not all of the file's row groups. Sending the original footer
        // with every split would merely move its quadratic amplification to coordinator traffic.
        FileMetaData selected = new FileMetaData(file.getVersion(), file.getSchema(), rows, ImmutableList.copyOf(groups));
        if (file.isSetCreated_by()) {
            selected.setCreated_by(file.getCreated_by());
        }
        if (file.isSetKey_value_metadata()) {
            selected.setKey_value_metadata(file.getKey_value_metadata());
        }
        if (file.isSetColumn_orders()) {
            selected.setColumn_orders(file.getColumn_orders());
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeFileMetaData(selected, output);
        return new RowGroupSplit(rows, bytes, new DuckLakeRowGroupMetadata(firstRowIndex, allRowGroups, output.toByteArray()));
    }

    record RowGroupSplit(long recordCount, long compressedBytes, DuckLakeRowGroupMetadata metadata) {}
}
