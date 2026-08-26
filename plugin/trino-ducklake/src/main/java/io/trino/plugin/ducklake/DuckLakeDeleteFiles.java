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
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.HiveType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;

/**
 * Reads and writes the Parquet files that record which rows of a data file are gone.
 * <p>
 * A DuckLake delete file names, for each removed row, the data file it belonged to and its
 * position in it. At most one delete file applies to a data file at a time, so removing further
 * rows means writing a file that covers all of them, which is what {@link #write} produces from
 * the union of the positions already recorded and the ones a statement adds.
 */
public final class DuckLakeDeleteFiles
{
    private static final int DOMAIN_COMPACTION_THRESHOLD = 100;

    private DuckLakeDeleteFiles() {}

    /**
     * The positions a delete file records. The file names its data file in every row, but a
     * DuckLake delete file only ever covers one, so the paths are not read back.
     */
    public static LongOpenHashSet readPositions(
            TrinoFileSystem fileSystem,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions options,
            String path,
            long fileSizeBytes,
            long deleteCount)
    {
        LongOpenHashSet positions = new LongOpenHashSet(toIntExact(deleteCount));
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path), fileSizeBytes);
        try (ConnectorPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                0,
                fileSizeBytes,
                ImmutableList.of(positionColumn()),
                ImmutableList.of(TupleDomain.all()),
                true, // resolve columns by name
                DateTimeZone.UTC,
                stats,
                options,
                Optional.empty(),
                Optional.empty(),
                DOMAIN_COMPACTION_THRESHOLD,
                OptionalLong.of(fileSizeBytes),
                _ -> {})) {
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Delete file %s contains a null position".formatted(path));
                    }
                    positions.add(BIGINT.getLong(block, position));
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Failed to read delete file %s".formatted(path), e);
        }
        return positions;
    }

    /**
     * Writes a delete file listing the given positions of one data file, and returns its size and
     * the size of its footer.
     */
    public static WrittenFile write(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            DuckLakeWriterFactory writerFactory,
            String path,
            String dataFilePath,
            long[] positions)
    {
        DuckLakeParquetSchema schema = DuckLakeParquetSchema.forDeleteFile();
        ParquetFileWriter writer = writerFactory.createParquetWriter(session, fileSystem, path, schema);
        try {
            BlockBuilder paths = VARCHAR.createBlockBuilder(null, positions.length);
            BlockBuilder rowPositions = BIGINT.createFixedSizeBlockBuilder(positions.length);
            for (long position : positions) {
                VARCHAR.writeSlice(paths, Slices.utf8Slice(dataFilePath));
                BIGINT.writeLong(rowPositions, position);
            }
            writer.appendRows(new Page(positions.length, paths.build(), rowPositions.build()));
        }
        catch (RuntimeException e) {
            writer.rollback();
            throw e;
        }
        writer.commit();
        return new WrittenFile(writer.getWrittenBytes(), writer.getFooterSize());
    }

    /**
     * The Hive column handle used to read the position column back out of a delete file.
     */
    private static HiveColumnHandle positionColumn()
    {
        return new HiveColumnHandle(
                DuckLakeParquetSchema.deleteFilePositionColumnName(),
                0, // fake index; the column is resolved by name
                HiveType.HIVE_LONG,
                BIGINT,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    public record WrittenFile(long fileSizeBytes, long footerSize) {}
}
