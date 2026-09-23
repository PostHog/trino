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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hoglake.HoglakeConfig.DEFAULT_MAX_SPLIT_SIZE;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static java.util.Objects.requireNonNull;

/**
 * Split planning: GET /scan at the handle's pinned snapshot, then byte-range
 * splits per data file, each carrying the deletion vector that scan paired
 * with the file. A 410 here is the typed "snapshot expired during query"
 * failure; a 404 is the table vanishing beneath the query (TABLE_NOT_FOUND)
 * — never a generic internal error.
 *
 * <p>Ranges are planned from the catalog's file sizes (and row-group offsets
 * when the scan reports them) without opening any file, so planning costs the
 * same single REST call however large the files are. A file no larger than
 * the target split size stays one whole-file split.
 *
 * <p>The vector's bytes are not fetched at planning: a split describes a
 * worker's job, and the page source reads the vector through the
 * connector's filesystem for the split it is actually running. What is
 * checked here is that the scan's pairing is internally consistent, so an
 * inconsistent scan is rejected before any split reaches the engine rather
 * than being papered over per worker.
 */
public class HoglakeSplitManager
        implements ConnectorSplitManager
{
    // Parquet ends with a 4-byte footer length and the 4-byte "PAR1" magic.
    private static final int PARQUET_TRAILER_SIZE = Integer.BYTES + 4;
    private static final double MINIMUM_SPLIT_WEIGHT = 0.05;

    private final HoglakeClient client;
    private final Function<ConnectorSession, DataSize> maxSplitSize;

    /**
     * A split manager with the default target split size, for connectors
     * that register no session properties.
     */
    public HoglakeSplitManager(HoglakeClient client)
    {
        this(client, _ -> DEFAULT_MAX_SPLIT_SIZE);
    }

    public HoglakeSplitManager(HoglakeClient client, Function<ConnectorSession, DataSize> maxSplitSize)
    {
        this.client = requireNonNull(client, "client is null");
        this.maxSplitSize = requireNonNull(maxSplitSize, "maxSplitSize is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        HoglakeTableHandle handle = (HoglakeTableHandle) table;
        if (handle.constraint().isNone() || constraint.getSummary().isNone()) {
            return new FixedSplitSource(List.of());
        }
        List<HoglakeDtos.ScanFile> scan = scan(client, handle);
        return new FixedSplitSource(toSplits(scan, maxSplitSize.apply(session).toBytes()));
    }

    static List<HoglakeDtos.ScanFile> scan(HoglakeClient client, HoglakeTableHandle handle)
    {
        Map<Long, HoglakeDtos.DeleteFile> deletes = handle.stagedDeletes().stream()
                .collect(Collectors.toMap(HoglakeDtos.DeleteFile::dataFileId, file -> file));
        return Stream.concat(
                        client.scan(handle.schemaName(), handle.tableName(), handle.snapshotId()).stream(),
                        handle.stagedFiles().stream())
                .map(file -> new HoglakeDtos.ScanFile(file.dataFile(), deletes.getOrDefault(file.dataFile().dataFileId(), file.deleteFile())))
                .toList();
    }

    /**
     * Pure split construction with the default target split size,
     * unit-testable without a server.
     */
    static List<HoglakeSplit> toSplits(List<HoglakeDtos.ScanFile> scan)
    {
        return toSplits(scan, DEFAULT_MAX_SPLIT_SIZE.toBytes());
    }

    /**
     * Pure split construction, unit-testable without a server. Each file
     * becomes one or more byte-range splits of at most {@code maxSplitSize}
     * bytes (a single row group larger than that stays whole), emitted in
     * file order and ascending range order.
     */
    static List<HoglakeSplit> toSplits(List<HoglakeDtos.ScanFile> scan, long maxSplitSize)
    {
        checkArgument(maxSplitSize > 0, "maxSplitSize must be positive: %s", maxSplitSize);
        ImmutableList.Builder<HoglakeSplit> splits = ImmutableList.builder();
        for (HoglakeDtos.ScanFile file : scan) {
            HoglakeSplit wholeFile = toSplit(file);
            long fileSizeBytes = wholeFile.fileSizeBytes();
            if (fileSizeBytes <= maxSplitSize) {
                // Unchanged from whole-file planning: one split, the catalog's
                // record count, and a standard weight.
                splits.add(wholeFile);
                continue;
            }
            for (ByteRange range : planRanges(fileSizeBytes, wholeFile.footerSize(), file.dataFile().splitOffsets(), maxSplitSize)) {
                splits.add(wholeFile.withRange(range.start(), range.length(), splitWeight(range.length(), maxSplitSize)));
            }
        }
        return splits.build();
    }

    /**
     * A contiguous cover of {@code [0, fileSizeBytes)}: the worker reads the
     * row groups whose first column chunk starts inside its range, so any
     * cover reads every row group exactly once. Row-group offsets from the
     * catalog, when usable, only decide where the cuts fall.
     */
    static List<ByteRange> planRanges(long fileSizeBytes, OptionalLong footerSize, List<Long> splitOffsets, long maxSplitSize)
    {
        checkArgument(maxSplitSize > 0, "maxSplitSize must be positive: %s", maxSplitSize);
        if (fileSizeBytes <= maxSplitSize) {
            return ImmutableList.of(new ByteRange(0, fileSizeBytes));
        }
        List<Long> cuts;
        if (isUsable(splitOffsets, fileSizeBytes)) {
            cuts = rowGroupCuts(splitOffsets, dataEnd(fileSizeBytes, footerSize, splitOffsets.getLast()), maxSplitSize);
        }
        else {
            cuts = evenCuts(fileSizeBytes, maxSplitSize);
        }
        ImmutableList.Builder<ByteRange> ranges = ImmutableList.builder();
        long start = 0;
        for (long cut : cuts) {
            ranges.add(new ByteRange(start, cut - start));
            start = cut;
        }
        ranges.add(new ByteRange(start, fileSizeBytes - start));
        return ranges.build();
    }

    /**
     * Interior cut points that pack consecutive row groups into ranges of at
     * most {@code maxSplitSize} bytes. A row group runs from its offset to the
     * next one's; the last runs to {@code dataEnd}. A row group larger than
     * the target is never divided and becomes a range of its own.
     */
    private static List<Long> rowGroupCuts(List<Long> offsets, long dataEnd, long maxSplitSize)
    {
        ImmutableList.Builder<Long> cuts = ImmutableList.builder();
        long rangeStart = offsets.getFirst();
        for (int i = 1; i < offsets.size(); i++) {
            long rowGroupEnd = (i + 1 < offsets.size()) ? offsets.get(i + 1) : dataEnd;
            if (rowGroupEnd - rangeStart > maxSplitSize) {
                // Row group i does not fit in the current range: start a new one with it.
                cuts.add(offsets.get(i));
                rangeStart = offsets.get(i);
            }
        }
        return cuts.build();
    }

    private static List<Long> evenCuts(long fileSizeBytes, long maxSplitSize)
    {
        ImmutableList.Builder<Long> cuts = ImmutableList.builder();
        for (long cut = maxSplitSize; cut < fileSizeBytes; cut += maxSplitSize) {
            cuts.add(cut);
        }
        return cuts.build();
    }

    /**
     * Offsets can only place cuts when they are non-empty, strictly
     * increasing and inside the file. Anything else is ignored rather than
     * trusted, since even cuts are always correct.
     */
    private static boolean isUsable(List<Long> offsets, long fileSizeBytes)
    {
        if (offsets.isEmpty() || offsets.getFirst() < 0 || offsets.getLast() >= fileSizeBytes) {
            return false;
        }
        for (int i = 1; i < offsets.size(); i++) {
            if (offsets.get(i) <= offsets.get(i - 1)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Where the last row group's data ends: before the footer and its 8-byte
     * trailer when the catalog knows the footer size, otherwise the file end.
     */
    private static long dataEnd(long fileSizeBytes, OptionalLong footerSize, long lastOffset)
    {
        if (footerSize.isPresent()) {
            long dataEnd = fileSizeBytes - footerSize.orElseThrow() - PARQUET_TRAILER_SIZE;
            if (dataEnd > lastOffset && dataEnd <= fileSizeBytes) {
                return dataEnd;
            }
        }
        return fileSizeBytes;
    }

    /**
     * The scheduler balances splits by weight, so a short range is charged
     * in proportion to its size, with a floor so tiny ranges are not free.
     */
    static SplitWeight splitWeight(long length, long maxSplitSize)
    {
        return SplitWeight.fromProportion(Math.clamp((double) length / maxSplitSize, MINIMUM_SPLIT_WEIGHT, 1.0));
    }

    record ByteRange(long start, long length) {}

    static HoglakeSplit toSplit(HoglakeDtos.ScanFile file)
    {
        HoglakeDtos.DataFile dataFile = file.dataFile();
        if (dataFile == null) {
            throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "hoglake scan returned an entry without a data file");
        }
        OptionalLong footerSize = dataFile.footerSize() == null ? OptionalLong.empty() : OptionalLong.of(dataFile.footerSize());
        HoglakeDtos.DeleteFile deleteFile = file.deleteFile();
        if (deleteFile == null) {
            return wholeFileSplit(dataFile, Optional.empty(), 0, Optional.empty(), footerSize);
        }
        if (deleteFile.path() == null) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake scan paired data file %s with a deletion vector that has no path".formatted(dataFile.path()));
        }
        // The scan pairs a vector with the file it deletes from; a mismatch
        // means the pairing cannot be trusted at all, so the query fails
        // here instead of a worker deleting rows from the wrong file.
        if (deleteFile.dataFileId() != dataFile.dataFileId()) {
            throw new TrinoException(
                    HOGLAKE_INVALID_RESPONSE,
                    "hoglake scan paired deletion vector %s (data_file_id %d) with data file %s (data_file_id %d)"
                            .formatted(deleteFile.path(), deleteFile.dataFileId(), dataFile.path(), dataFile.dataFileId()));
        }
        return wholeFileSplit(dataFile, Optional.of(deleteFile.path()), deleteFile.deleteCount(), Optional.ofNullable(deleteFile.fileFormat()), footerSize);
    }

    private static HoglakeSplit wholeFileSplit(
            HoglakeDtos.DataFile dataFile,
            Optional<String> deleteFilePath,
            long deleteCount,
            Optional<String> deleteFileFormat,
            OptionalLong footerSize)
    {
        return new HoglakeSplit(
                dataFile.dataFileId(),
                dataFile.path(),
                dataFile.fileSizeBytes(),
                dataFile.recordCount(),
                deleteFilePath,
                deleteCount,
                deleteFileFormat,
                0,
                dataFile.fileSizeBytes(),
                SplitWeight.standard(),
                footerSize);
    }
}
