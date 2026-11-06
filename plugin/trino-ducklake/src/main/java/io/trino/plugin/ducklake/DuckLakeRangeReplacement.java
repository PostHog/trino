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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeleteFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeDeletions;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedData;
import io.trino.plugin.ducklake.metastore.DuckLakeInlinedDataTable;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeColumns;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_CONFLICT;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isFileStatisticsPruningEnabled;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isParquetIgnoreStatistics;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * Plans metadata removals first, reading only the range column of the files the catalog does not
 * place outside the range.
 * <p>
 * Catalog statistics only skip a file they place wholly outside the range, while file statistics
 * pruning is enabled. A mistake there leaves a duplicate of a replaced row. They never end a file
 * whole, because a mistake there loses every row of the file outside the range, and other engines
 * write those statistics without anything checking them against the file. Every other file has
 * its Parquet footer read, in parallel, and the footer statistics, derived with the rule that
 * vets the replacement files, decide: a file ends whole when they prove every row non-NULL and
 * inside the range, and is skipped when they place it outside. A file they do not settle is read,
 * for its NULLs alone when they bound the values inside the range, else for its rows. A file with
 * a visible row outside the range is a boundary file, and its rows inside the range are deleted by
 * position.
 */
public final class DuckLakeRangeReplacement
{
    private static final Logger log = Logger.get(DuckLakeRangeReplacement.class);

    private static final long MAX_BOUNDARY_POSITIONS = 5_000_000;
    private static final long MAX_READER_MEMORY = 128L * 1024 * 1024;

    private final JdbcDuckLakeMetastore metastore;
    private final DuckLakePageSourceProvider pageSources;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final int maxConcurrentFooterReads;

    @Inject
    public DuckLakeRangeReplacement(
            JdbcDuckLakeMetastore metastore,
            DuckLakePageSourceProvider pageSources,
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            DuckLakeConfig config)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.pageSources = requireNonNull(pageSources, "pageSources is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.maxConcurrentFooterReads = config.getAddFilesProcedureMaxConcurrentFileReads();
    }

    /**
     * @param wholeFiles the data files to end
     * @param positions the positions to delete, by data file. They leave out the positions a
     *         delete file or an inline deletion already deletes.
     * @param inlinedDeletions the positions DuckDB deleted inline, by data file, for the data files
     *         to end or to delete positions of. They stay in the catalog database and keep
     *         applying to a data file for as long as it is visible, so a new delete file does not
     *         repeat them.
     */
    public record Plan(Set<Long> wholeFiles, Map<Long, LongOpenHashSet> positions, Map<Long, long[]> inlinedDeletions)
    {
        public Plan
        {
            wholeFiles = ImmutableSet.copyOf(wholeFiles);
            positions = ImmutableMap.copyOf(positions);
            inlinedDeletions = ImmutableMap.copyOf(inlinedDeletions);
        }
    }

    /**
     * Whether the table holds rows inlined in the catalog database, visible in the snapshot, with
     * a value of the range column inside the range. A replacement cannot remove such rows. Rows
     * outside the range stay as they are, so they do not stand in its way.
     * <p>
     * Call this before {@link #plan}. DuckDB flushes inlined rows to a data file registered under
     * the snapshots the rows were written in, so a row this check misses because a flush moved it
     * is in a data file the plan lists. A flush committed while the rows are read fails the read.
     */
    public boolean hasInlinedRowsInRange(DuckLakeTableHandle table, DuckLakeReplacementRange replacement)
    {
        DuckLakeInlinedData inlinedData = metastore.inlinedData(table.snapshotId(), table.tableId());
        Domain range = replacement.domain();
        Type type = replacement.column().type();
        for (DuckLakeInlinedDataTable inlinedTable : inlinedData.tables()) {
            DuckLakeInlinedSplit split = new DuckLakeInlinedSplit(
                    table.schemaName(),
                    table.tableName(),
                    table.tableId(),
                    table.snapshotId(),
                    inlinedData.watermarkSnapshotId(),
                    inlinedTable.tableName(),
                    inlinedTable.columns());
            try (ConnectorPageSource source = new DuckLakeInlinedPageSource(metastore, split, ImmutableList.of(replacement.column()))) {
                while (!source.isFinished()) {
                    checkInterrupted();
                    SourcePage page = source.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }
                    Block values = page.getBlock(0);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        if (range.includesNullableValue(readNativeValue(type, values, position))) {
                            return true;
                        }
                    }
                }
            }
            catch (IOException e) {
                throw new TrinoException(DUCKLAKE_METASTORE_ERROR, "Could not read inlined rows for range replacement", e);
            }
        }
        return false;
    }

    public Plan plan(ConnectorSession session, DuckLakeTableHandle table, DuckLakeReplacementRange replacement)
    {
        List<DuckLakeDataFileEntry> files = metastore.dataFiles(table.snapshotId(), table.tableId());
        Map<Long, DuckLakeFileColumnStats> statistics = ImmutableMap.of();
        if (isFileStatisticsPruningEnabled(session)) {
            statistics = metastore.fileColumnStats(table.snapshotId(), table.tableId(), Set.of(replacement.column().columnId())).stream()
                    .collect(toImmutableMap(DuckLakeFileColumnStats::dataFileId, value -> value));
        }
        // delete files and inline deletions read from one state of the catalog; see DuckLakeDeletions
        DuckLakeDeletions allDeletions = metastore.deletions(table.snapshotId(), table.tableId());
        Map<Long, DuckLakeDeleteFileEntry> deletions = allDeletions.deleteFiles().stream()
                .collect(toImmutableMap(DuckLakeDeleteFileEntry::dataFileId, value -> value, (first, _) -> {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Multiple delete files are visible for data file %s of table %s".formatted(first.dataFileId(), table.schemaTableName()));
                }));
        Map<Long, long[]> inlinedDeletions = allDeletions.inlinedDeletions();
        Map<Long, DuckLakeNameMapping> mappings = DuckLakePageSourceProvider.withNestedReadTypes(
                metastore.nameMappings(files.stream()
                        .filter(file -> file.mappingId().isPresent()).map(file -> file.mappingId().orElseThrow()).collect(toImmutableSet())),
                () -> DuckLakeColumns.fromCatalog(metastore.columns(table.snapshotId(), table.tableId())));
        Domain range = replacement.domain();
        List<Candidate> candidates = new ArrayList<>();
        for (DuckLakeDataFileEntry file : files) {
            checkInterrupted();
            DuckLakeDeleteFileEntry deletion = deletions.get(file.dataFileId());
            if (file.partialMax().orElse(0) > table.snapshotId() || (deletion != null && deletion.partialMax().orElse(0) > table.snapshotId())) {
                throw new TrinoException(DUCKLAKE_COMMIT_CONFLICT, "Files changed while planning range replacement; run the statement again");
            }
            Optional<Domain> catalogDomain = Optional.ofNullable(statistics.get(file.dataFileId()))
                    .flatMap(value -> DuckLakeSplitManager.statisticsDomain(replacement.column().type(), value, file.recordCount()));
            if (catalogDomain.isPresent() && !range.overlaps(catalogDomain.orElseThrow())) {
                continue;
            }
            candidates.add(new Candidate(file, Optional.ofNullable(deletion), Optional.ofNullable(inlinedDeletions.get(file.dataFileId())), catalogDomain));
        }
        List<Optional<Domain>> footerDomains = readFooterDomains(session, table, replacement, candidates, mappings);
        // a footer not read proves nothing, which tells nothing about the catalog statistics
        boolean footersRead = !isParquetIgnoreStatistics(session);
        ImmutableSet.Builder<Long> wholeFiles = ImmutableSet.builder();
        Map<Long, LongOpenHashSet> positions = new LinkedHashMap<>();
        ImmutableMap.Builder<Long, long[]> plannedInlinedDeletions = ImmutableMap.builder();
        long totalPositions = 0;
        for (int index = 0; index < candidates.size(); index++) {
            checkInterrupted();
            Candidate candidate = candidates.get(index);
            DuckLakeDataFileEntry file = candidate.file();
            Optional<Domain> footerDomain = footerDomains.get(index);
            boolean provenInside = footerDomain.isPresent() && range.contains(footerDomain.orElseThrow());
            if (footersRead && !provenInside && candidate.catalogDomain().isPresent() && range.contains(candidate.catalogDomain().orElseThrow())) {
                log.warn("Catalog statistics place data file %s (%s) of table %s inside the replacement range, but its footer statistics do not prove it; the footer and the rows of the file decide instead", file.dataFileId(), file.path(), table.schemaTableName());
            }
            if (footerDomain.isPresent() && !range.overlaps(footerDomain.orElseThrow())) {
                continue;
            }
            if (provenInside || allRowsInRange(session, table, replacement, candidate, footerDomain, mappings)) {
                wholeFiles.add(file.dataFileId());
                candidate.inlinedDeletions().ifPresent(value -> plannedInlinedDeletions.put(file.dataFileId(), value));
                continue;
            }
            LongOpenHashSet removed = readBoundary(session, table, replacement, file, candidate.deletion(), candidate.inlinedDeletions(), mappings, MAX_BOUNDARY_POSITIONS - totalPositions);
            if (!removed.isEmpty()) {
                positions.put(file.dataFileId(), removed);
                candidate.inlinedDeletions().ifPresent(value -> plannedInlinedDeletions.put(file.dataFileId(), value));
                totalPositions += removed.size();
            }
        }
        return new Plan(wholeFiles.build(), positions, plannedInlinedDeletions.buildOrThrow());
    }

    /**
     * A data file the catalog statistics do not place outside the range, with what is needed to
     * read it.
     */
    private record Candidate(DuckLakeDataFileEntry file, Optional<DuckLakeDeleteFileEntry> deletion, Optional<long[]> inlinedDeletions, Optional<Domain> catalogDomain) {}

    /**
     * The values of the range column of each candidate, as its footer proves them, in the order of
     * the candidates. The footers are read concurrently, and each read keeps only the small domain
     * it derives, so memory holds no more footers than the reads in flight.
     * <p>
     * With {@code parquet_ignore_statistics} the footers are not used, and every domain is empty.
     */
    private List<Optional<Domain>> readFooterDomains(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            List<Candidate> candidates,
            Map<Long, DuckLakeNameMapping> mappings)
    {
        if (candidates.isEmpty()) {
            return List.of();
        }
        if (isParquetIgnoreStatistics(session)) {
            return candidates.stream().map(_ -> Optional.<Domain>empty()).collect(toImmutableList());
        }
        long columnId = replacement.column().columnId();
        Optional<DuckLakeFooterStatistics.Column> column = DuckLakeColumns.fromCatalog(metastore.columns(table.snapshotId(), table.tableId())).stream()
                .filter(value -> value.columnId() == columnId)
                .findFirst()
                .map(DuckLakeFooterStatistics.Column::of);
        if (column.isEmpty()) {
            return candidates.stream().map(_ -> Optional.<Domain>empty()).collect(toImmutableList());
        }
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        List<Callable<Optional<Domain>>> reads = candidates.stream()
                .<Callable<Optional<Domain>>>map(candidate -> () -> {
                    checkInterrupted();
                    return DuckLakeFooterStatistics.readDomain(fileSystem, parquetReaderOptions, fileFormatDataSourceStats, table, candidate.file(), mappings, column.orElseThrow());
                })
                .collect(toImmutableList());
        return DuckLakeParallelFileReads.readAll(
                reads,
                maxConcurrentFooterReads,
                "ducklake-range-replacement-%s",
                "Range replacement was interrupted",
                DUCKLAKE_FILESYSTEM_ERROR,
                "Could not read the footers of files for range replacement");
    }

    /**
     * Whether every visible row of the file has a value of the range column inside the range, so
     * that the file can end whole, for a file whose footer does not prove it.
     * <p>
     * DuckDB records no null count for a file it registers with {@code ducklake_add_data_files},
     * so such a file may hold a NULL, which lies outside every range. When the footer bounds the
     * values of the file inside the range, only a NULL can lie outside it, so the file is read for
     * its NULLs alone. The Parquet reader skips every row group whose footer rules out a NULL, from
     * its null count or from a column that cannot hold one, whatever the type of the column. A file
     * without usable footer bounds is read for the values themselves.
     */
    private boolean allRowsInRange(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            Candidate candidate,
            Optional<Domain> footerDomain,
            Map<Long, DuckLakeNameMapping> mappings)
    {
        Domain range = replacement.domain();
        if (footerDomain.isPresent() && range.contains(Domain.create(footerDomain.orElseThrow().getValues(), false))) {
            return !hasVisibleNull(session, table, replacement, candidate.file(), candidate.deletion(), candidate.inlinedDeletions(), mappings);
        }
        return allVisibleRowsInRange(session, table, replacement, candidate.file(), candidate.deletion(), candidate.inlinedDeletions(), mappings);
    }

    private boolean hasVisibleNull(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Optional<long[]> inlinedDeletions,
            Map<Long, DuckLakeNameMapping> mappings)
    {
        try (ConnectorPageSource source = openFile(session, table, replacement, file, deletion, inlinedDeletions, mappings, Domain.onlyNull(replacement.column().type()), false)) {
            while (!source.isFinished()) {
                checkInterrupted();
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block values = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (values.isNull(position)) {
                        return true;
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not read file for range replacement", e);
        }
        return false;
    }

    /**
     * Whether every visible row of the file has a value of the range column inside the range,
     * read from the file itself.
     * <p>
     * The file is read with the complement of the range as the Parquet predicate. The reader skips
     * every row group whose footer statistics show only non-NULL values inside the range. It takes
     * no bounds from the footer for some types, such as timestamp with time zone, and reads their
     * pages instead. The read stops at the first visible row outside the range.
     */
    private boolean allVisibleRowsInRange(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Optional<long[]> inlinedDeletions,
            Map<Long, DuckLakeNameMapping> mappings)
    {
        Domain range = replacement.domain();
        Type type = replacement.column().type();
        try (ConnectorPageSource source = openFile(session, table, replacement, file, deletion, inlinedDeletions, mappings, range.complement(), false)) {
            while (!source.isFinished()) {
                checkInterrupted();
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block values = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!range.includesNullableValue(readNativeValue(type, values, position))) {
                        return false;
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not read file for range replacement", e);
        }
        return true;
    }

    private LongOpenHashSet readBoundary(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Optional<long[]> inlinedDeletions,
            Map<Long, DuckLakeNameMapping> mappings,
            long remainingPositions)
    {
        Domain range = replacement.domain();
        LongOpenHashSet removed = new LongOpenHashSet();
        try (ConnectorPageSource source = openFile(session, table, replacement, file, deletion, inlinedDeletions, mappings, range, true)) {
            while (!source.isFinished()) {
                checkInterrupted();
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                Block values = page.getBlock(0);
                Block rowPositions = getRowFieldsFromBlock(page.getBlock(1)).get(DuckLakeMergeRowId.FILE_ROW_POSITION_CHANNEL);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (range.includesNullableValue(readNativeValue(replacement.column().type(), values, position))) {
                        removed.add(BIGINT.getLong(rowPositions, position));
                        if (removed.size() > remainingPositions) {
                            throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Range replacement exceeds five million boundary row positions; use smaller ranges or align files to ranges");
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not read boundary file for range replacement", e);
        }
        return removed;
    }

    /**
     * Opens the visible rows of the file, with the range column first and, if requested, the row
     * identifier second. The reader skips row groups whose footer statistics show no value of the
     * range column in {@code predicate}.
     */
    private ConnectorPageSource openFile(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Optional<long[]> inlinedDeletions,
            Map<Long, DuckLakeNameMapping> mappings,
            Domain predicate,
            boolean rowIds)
    {
        if (!file.fileFormat().equalsIgnoreCase("parquet") || file.encryptionKey().isPresent()) {
            throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Range replacement requires unencrypted Parquet files");
        }
        Optional<DuckLakeNameMapping> mapping = Optional.empty();
        if (file.mappingId().isPresent()) {
            mapping = Optional.ofNullable(mappings.get(file.mappingId().orElseThrow()));
            if (mapping.isEmpty()) {
                throw new TrinoException(DUCKLAKE_INVALID_METADATA, "File name mapping is missing");
            }
        }
        Optional<DuckLakeDeleteFileHandle> deleteFile = deletion.map(value -> new DuckLakeDeleteFileHandle(
                PathResolver.resolve(table.tableLocation(), value.path(), value.pathIsRelative()),
                value.fileSizeBytes(),
                value.footerSize(),
                value.deleteCount(),
                true));
        DuckLakeSplit split = new DuckLakeSplit(
                file.dataFileId(),
                PathResolver.resolve(table.tableLocation(), file.path(), file.pathIsRelative()),
                0,
                file.fileSizeBytes(),
                file.fileSizeBytes(),
                file.footerSize(),
                file.recordCount(),
                file.rowIdStart(),
                deleteFile,
                file.partitionValues(),
                mapping,
                SplitWeight.standard(),
                Optional.empty(),
                Optional.empty(),
                // a position already deleted inline must stay out of the new delete file, see
                // DuckLakeMetadata.rewriteDeleteFiles
                inlinedDeletions.map(DuckLakeInlinedDeletions::new));
        DuckLakeTableHandle filteredTable = new DuckLakeTableHandle(
                table.schemaName(),
                table.tableName(),
                table.tableId(),
                table.snapshotId(),
                table.tableLocation(),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(Map.of(replacement.column(), predicate)),
                OptionalLong.empty());
        List<ColumnHandle> columns = rowIds
                ? ImmutableList.of(replacement.column(), DuckLakeMergeRowId.columnHandle())
                : ImmutableList.of(replacement.column());
        return pageSources.createPageSource(
                session,
                split,
                filteredTable,
                columns,
                DynamicFilter.EMPTY,
                bytes -> {
                    if (bytes > MAX_READER_MEMORY) {
                        throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Range replacement boundary reader exceeded 128MB; use smaller files");
                    }
                });
    }

    private static void checkInterrupted()
    {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(USER_CANCELED, "Range replacement was interrupted");
        }
    }
}
