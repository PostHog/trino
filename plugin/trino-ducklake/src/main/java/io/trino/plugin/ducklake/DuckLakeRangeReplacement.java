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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_COMMIT_CONFLICT;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_METASTORE_ERROR;
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isFileStatisticsPruningEnabled;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * Plans metadata removals first, reading only the range column of the files the catalog
 * statistics do not place. Catalog statistics place a file wholly inside or outside the range only
 * while file statistics pruning is enabled. A file they do not place still ends whole when its
 * footer statistics or its rows show that every visible row lies inside the range. Otherwise it is
 * a boundary file, and its rows inside the range are deleted by position.
 */
public final class DuckLakeRangeReplacement
{
    private static final long MAX_BOUNDARY_POSITIONS = 5_000_000;
    private static final long MAX_READER_MEMORY = 128L * 1024 * 1024;

    private final JdbcDuckLakeMetastore metastore;
    private final DuckLakePageSourceProvider pageSources;

    @Inject
    public DuckLakeRangeReplacement(JdbcDuckLakeMetastore metastore, DuckLakePageSourceProvider pageSources)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.pageSources = requireNonNull(pageSources, "pageSources is null");
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
        ImmutableSet.Builder<Long> wholeFiles = ImmutableSet.builder();
        Map<Long, LongOpenHashSet> positions = new LinkedHashMap<>();
        ImmutableMap.Builder<Long, long[]> plannedInlinedDeletions = ImmutableMap.builder();
        long totalPositions = 0;
        for (DuckLakeDataFileEntry file : files) {
            checkInterrupted();
            DuckLakeDeleteFileEntry deletion = deletions.get(file.dataFileId());
            if (file.partialMax().orElse(0) > table.snapshotId() || (deletion != null && deletion.partialMax().orElse(0) > table.snapshotId())) {
                throw new TrinoException(DUCKLAKE_COMMIT_CONFLICT, "Files changed while planning range replacement; run the statement again");
            }
            Optional<long[]> inlined = Optional.ofNullable(inlinedDeletions.get(file.dataFileId()));
            DuckLakeFileColumnStats stats = statistics.get(file.dataFileId());
            Optional<Domain> fileDomain = Optional.ofNullable(stats)
                    .flatMap(value -> DuckLakeSplitManager.statisticsDomain(replacement.column().type(), value, file.recordCount()));
            if (fileDomain.isPresent() && !range.overlaps(fileDomain.orElseThrow())) {
                continue;
            }
            if (allRowsInRange(session, table, replacement, file, fileDomain, Optional.ofNullable(deletion), inlined, mappings)) {
                wholeFiles.add(file.dataFileId());
                inlined.ifPresent(value -> plannedInlinedDeletions.put(file.dataFileId(), value));
                continue;
            }
            LongOpenHashSet removed = readBoundary(session, table, replacement, file, Optional.ofNullable(deletion), inlined, mappings, MAX_BOUNDARY_POSITIONS - totalPositions);
            if (!removed.isEmpty()) {
                positions.put(file.dataFileId(), removed);
                inlined.ifPresent(value -> plannedInlinedDeletions.put(file.dataFileId(), value));
                totalPositions += removed.size();
            }
        }
        return new Plan(wholeFiles.build(), positions, plannedInlinedDeletions.buildOrThrow());
    }

    /**
     * Whether every visible row of the file has a value of the range column inside the range, so
     * that the file can end whole.
     * <p>
     * The catalog cannot always tell. DuckDB records no null count for a file it registers with
     * {@code ducklake_add_data_files}, so such a file may hold a NULL, which lies outside every
     * range. When the catalog bounds the values of the file inside the range, only a NULL can lie
     * outside it, so the file is read for its NULLs alone. The Parquet reader skips every row
     * group whose footer rules out a NULL, from its null count or from a column that cannot hold
     * one, whatever the type of the column. A file whose footer rules out a NULL in every row
     * group is so settled without a read of its pages.
     */
    private boolean allRowsInRange(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<Domain> fileDomain,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Optional<long[]> inlinedDeletions,
            Map<Long, DuckLakeNameMapping> mappings)
    {
        Domain range = replacement.domain();
        if (fileDomain.isPresent()) {
            Domain values = fileDomain.orElseThrow();
            if (range.contains(values)) {
                return true;
            }
            if (range.contains(Domain.create(values.getValues(), false))) {
                return !hasVisibleNull(session, table, replacement, file, deletion, inlinedDeletions, mappings);
            }
        }
        return allVisibleRowsInRange(session, table, replacement, file, deletion, inlinedDeletions, mappings);
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
