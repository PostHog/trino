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

import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeDataFileEntry;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStats;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMapping;
import io.trino.plugin.ducklake.metastore.DuckLakeNameMappingEntry;
import io.trino.plugin.ducklake.util.DuckLakeFieldIds;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema.LeafField;
import io.trino.plugin.ducklake.util.PathResolver;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static java.util.Objects.requireNonNull;

/**
 * Derives the values a data file holds in one column from the statistics of its Parquet footer,
 * with the rule {@link DuckLakeReplacementRange#validateImportedFiles} applies to the files that
 * replace a range.
 * <p>
 * The catalog statistics of a file can come from another engine, and nothing ties them to the
 * file. The footer is the file's own statement about its rows, so a decision that loses rows when
 * it is wrong rests on the footer alone.
 */
final class DuckLakeFooterStatistics
{
    private DuckLakeFooterStatistics() {}

    /**
     * The column to derive the domain of, with the Parquet type and the statistics field that
     * DuckLake writes for its current type.
     */
    record Column(DuckLakeWriteColumn column, Type expectedType, LeafField leaf)
    {
        static Column of(DuckLakeWriteColumn column)
        {
            DuckLakeParquetSchema schema = DuckLakeParquetSchema.create(List.of(column));
            return new Column(
                    column,
                    schema.messageType().getType(column.name()),
                    schema.leafField(List.of(column.name())).orElseThrow());
        }
    }

    /**
     * Reads the footer of the file and returns the domain of the column, or empty when the footer
     * does not prove one. That is the case when the footer disagrees with the catalog on the number
     * of rows, when the file does not store the column as a top-level column of the type the table
     * has now, and when the footer statistics are invalid or unknown. A file without a proven domain
     * is read instead; it is never refused for this.
     * <p>
     * The footer is read with one ranged request, because the catalog records the length of the
     * file and usually the length of the footer.
     */
    static Optional<Domain> readDomain(
            TrinoFileSystem fileSystem,
            ParquetReaderOptions options,
            FileFormatDataSourceStats stats,
            DuckLakeTableHandle table,
            DuckLakeDataFileEntry file,
            Map<Long, DuckLakeNameMapping> mappings,
            Column column)
    {
        requireNonNull(column, "column is null");
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
        String path = PathResolver.resolve(table.tableLocation(), file.path(), file.pathIsRelative());
        ParquetMetadata metadata = readFooter(fileSystem, options, stats, file, path);
        try {
            return footerDomain(metadata, file.recordCount(), mapping, column);
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet file: " + path, e);
        }
        catch (TrinoException _) {
            // invalid footer statistics prove nothing; the file is read instead
            return Optional.empty();
        }
    }

    private static Optional<Domain> footerDomain(ParquetMetadata metadata, long recordCount, Optional<DuckLakeNameMapping> mapping, Column column)
            throws IOException
    {
        long rows = metadata.getParquetMetadata().getNum_rows();
        if (rows != recordCount || metadata.getBlocks().stream().mapToLong(block -> block.rowCount()).reduce(0, Math::addExact) != rows) {
            return Optional.empty();
        }
        MessageType schema = metadata.getFileMetaData().getSchema();
        Optional<String> name = columnName(schema, mapping, column.column());
        if (name.isEmpty()) {
            return Optional.empty();
        }
        Type physical = getParquetTypeByName(name.orElseThrow(), schema);
        if (physical == null || !physical.isPrimitive() || !DuckLakeFileImporter.typeMatches(physical, column.expectedType())) {
            return Optional.empty();
        }
        return DuckLakeImportStatistics.collect(metadata, Map.of(List.of(physical.getName()), column.leaf())).stream()
                .filter(value -> value.columnId() == column.column().columnId())
                .findFirst()
                .flatMap(value -> DuckLakeSplitManager.statisticsDomain(column.column().type(), new DuckLakeFileColumnStats(
                        0, value.columnId(), value.valueCount(), value.nullCount(), value.minValue(), value.maxValue(), value.containsNan()), rows));
    }

    /**
     * The name the file gives the column, resolved as the page source resolves it: through the name
     * mapping of the file when the catalog has one, else through the Parquet field identifiers,
     * else by the name of the column in the catalog.
     */
    private static Optional<String> columnName(MessageType schema, Optional<DuckLakeNameMapping> mapping, DuckLakeWriteColumn column)
    {
        if (mapping.isPresent()) {
            return mapping.orElseThrow().entry(column.columnId())
                    .filter(entry -> !entry.hivePartition() && !entry.hasNestedFields())
                    .map(DuckLakeNameMappingEntry::sourceName);
        }
        Map<Integer, String> namesByFieldId = DuckLakeFieldIds.columnNamesByFieldId(schema);
        if (namesByFieldId.isEmpty()) {
            return Optional.of(column.name());
        }
        return DuckLakeFieldIds.columnName(namesByFieldId, column.columnId());
    }

    private static ParquetMetadata readFooter(TrinoFileSystem fileSystem, ParquetReaderOptions options, FileFormatDataSourceStats stats, DuckLakeDataFileEntry file, String path)
    {
        ParquetReaderOptions footerOptions = DuckLakePageSourceProvider.withCatalogFooterSize(
                ParquetReaderOptions.builder(options).withSmallFileThreshold(DataSize.ofBytes(0)).build(),
                file.footerSize(),
                file.fileSizeBytes());
        TrinoInputFile input = fileSystem.newInputFile(Location.of(path), file.fileSizeBytes());
        try (var source = new TrinoParquetDataSource(input, footerOptions, stats)) {
            return MetadataReader.readFooter(source, footerOptions, Optional.empty(), Optional.empty());
        }
        catch (ParquetCorruptionException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet file: " + path, e);
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not read the footer of a file for range replacement: " + path, e);
        }
    }
}
