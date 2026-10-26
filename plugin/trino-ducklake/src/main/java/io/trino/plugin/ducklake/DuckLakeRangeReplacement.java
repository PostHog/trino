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
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.JdbcDuckLakeMetastore;
import io.trino.plugin.ducklake.util.DuckLakeColumns;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
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
import static io.trino.plugin.ducklake.DuckLakeSessionProperties.isFileStatisticsPruningEnabled;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * Plans metadata removals first, reading only the range column of boundary files. Catalog
 * statistics decide which files lie wholly inside or outside the range only while file
 * statistics pruning is enabled; otherwise every file is read like a boundary file.
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

    public record Plan(Set<Long> wholeFiles, Map<Long, LongOpenHashSet> positions)
    {
        public Plan
        {
            wholeFiles = ImmutableSet.copyOf(wholeFiles);
            positions = ImmutableMap.copyOf(positions);
        }
    }

    public Plan plan(ConnectorSession session, DuckLakeTableHandle table, DuckLakeReplacementRange replacement)
    {
        List<DuckLakeDataFileEntry> files = metastore.dataFiles(table.snapshotId(), table.tableId());
        Map<Long, DuckLakeFileColumnStats> statistics = ImmutableMap.of();
        if (isFileStatisticsPruningEnabled(session)) {
            statistics = metastore.fileColumnStats(table.snapshotId(), table.tableId(), Set.of(replacement.column().columnId())).stream()
                    .collect(toImmutableMap(DuckLakeFileColumnStats::dataFileId, value -> value));
        }
        Map<Long, DuckLakeDeleteFileEntry> deletions = metastore.deleteFiles(table.snapshotId(), table.tableId()).stream()
                .collect(toImmutableMap(DuckLakeDeleteFileEntry::dataFileId, value -> value, (first, _) -> {
                    throw new TrinoException(DUCKLAKE_INVALID_METADATA, "Multiple delete files are visible for data file %s of table %s".formatted(first.dataFileId(), table.schemaTableName()));
                }));
        Map<Long, DuckLakeNameMapping> mappings = DuckLakePageSourceProvider.withNestedReadTypes(
                metastore.nameMappings(files.stream()
                        .filter(file -> file.mappingId().isPresent()).map(file -> file.mappingId().orElseThrow()).collect(toImmutableSet())),
                () -> DuckLakeColumns.fromCatalog(metastore.columns(table.snapshotId(), table.tableId())));
        Domain range = replacement.domain();
        ImmutableSet.Builder<Long> wholeFiles = ImmutableSet.builder();
        Map<Long, LongOpenHashSet> positions = new LinkedHashMap<>();
        long totalPositions = 0;
        for (DuckLakeDataFileEntry file : files) {
            checkInterrupted();
            DuckLakeDeleteFileEntry deletion = deletions.get(file.dataFileId());
            if (file.partialMax().orElse(0) > table.snapshotId() || (deletion != null && deletion.partialMax().orElse(0) > table.snapshotId())) {
                throw new TrinoException(DUCKLAKE_COMMIT_CONFLICT, "Files changed while planning range replacement; run the statement again");
            }
            DuckLakeFileColumnStats stats = statistics.get(file.dataFileId());
            Optional<Domain> fileDomain = Optional.ofNullable(stats)
                    .flatMap(value -> DuckLakeSplitManager.statisticsDomain(replacement.column().type(), value, file.recordCount()));
            if (fileDomain.isPresent()) {
                if (!range.overlaps(fileDomain.orElseThrow())) {
                    continue;
                }
                if (range.contains(fileDomain.orElseThrow())) {
                    wholeFiles.add(file.dataFileId());
                    continue;
                }
            }
            LongOpenHashSet removed = readBoundary(session, table, replacement, file, Optional.ofNullable(deletion), mappings, MAX_BOUNDARY_POSITIONS - totalPositions);
            if (!removed.isEmpty()) {
                positions.put(file.dataFileId(), removed);
                totalPositions += removed.size();
            }
        }
        return new Plan(wholeFiles.build(), positions);
    }

    private LongOpenHashSet readBoundary(
            ConnectorSession session,
            DuckLakeTableHandle table,
            DuckLakeReplacementRange replacement,
            DuckLakeDataFileEntry file,
            Optional<DuckLakeDeleteFileEntry> deletion,
            Map<Long, DuckLakeNameMapping> mappings,
            long remainingPositions)
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
                SplitWeight.standard());
        Domain range = replacement.domain();
        DuckLakeTableHandle filteredTable = new DuckLakeTableHandle(
                table.schemaName(),
                table.tableName(),
                table.tableId(),
                table.snapshotId(),
                table.tableLocation(),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(Map.of(replacement.column(), range)),
                OptionalLong.empty());
        LongOpenHashSet removed = new LongOpenHashSet();
        try (ConnectorPageSource source = pageSources.createPageSource(
                session,
                split,
                filteredTable,
                ImmutableList.of(replacement.column(), DuckLakeMergeRowId.columnHandle()),
                DynamicFilter.EMPTY,
                bytes -> {
                    if (bytes > MAX_READER_MEMORY) {
                        throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Range replacement boundary reader exceeded 128MB; use smaller files");
                    }
                })) {
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

    private static void checkInterrupted()
    {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(USER_CANCELED, "Range replacement was interrupted");
        }
    }
}
