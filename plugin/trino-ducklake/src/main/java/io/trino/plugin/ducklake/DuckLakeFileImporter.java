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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import io.airlift.concurrent.Threads;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.metastore.DuckLakeCommit.NameMappingColumn;
import io.trino.plugin.ducklake.metastore.DuckLakeFileColumnStatsRow;
import io.trino.plugin.ducklake.util.DuckLakeParquetSchema;
import io.trino.plugin.ducklake.util.DuckLakePartitionValues;
import io.trino.plugin.ducklake.util.DuckLakeTypes;
import io.trino.plugin.ducklake.util.StatsValueParser;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TypeUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.io.IOException;
import java.net.URLDecoder;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

final class DuckLakeFileImporter
{
    private DuckLakeFileImporter() {}

    public record ImportedFile(DuckLakeDataFile file, List<NameMappingColumn> mapping, String footerHash) {}

    public static List<ImportedFile> readFiles(
            TrinoFileSystem fileSystem,
            String dataPath,
            DuckLakeAddFilesHandle handle,
            List<DuckLakeWriteColumn> columns,
            Optional<DuckLakePartitioning> partitioning,
            ParquetReaderOptions options,
            FileFormatDataSourceStats stats)
    {
        options = ParquetReaderOptions.builder(options).withSmallFileThreshold(DataSize.ofBytes(0)).build();
        ImmutableList.Builder<ImportedFile> files = ImmutableList.builder();
        if (Location.of(handle.table().tableLocation()).scheme().orElse("").equals("local")) {
            try {
                fileSystem.createDirectory(Location.of(handle.table().tableLocation()));
            }
            catch (IOException e) {
                throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not create the local table directory", e);
            }
        }
        if (handle.files().isEmpty()) {
            return ImmutableList.of();
        }
        var executor = Executors.newFixedThreadPool(Math.min(8, handle.files().size()), Threads.daemonThreadsNamed("ducklake-add-files-%s"));
        List<Future<ImportedFile>> pending = new ArrayList<>();
        try {
            ParquetReaderOptions readerOptions = options;
            for (String path : handle.files()) {
                pending.add(executor.submit(() -> readFile(fileSystem, dataPath, handle, columns, partitioning, readerOptions, stats, path)));
            }
            for (var future : pending) {
                files.add(future.get());
            }
            return files.build();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(StandardErrorCode.USER_CANCELED, "File registration was interrupted", e);
        }
        catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Could not inspect imported Parquet files", e.getCause());
        }
        finally {
            pending.forEach(future -> future.cancel(true));
            executor.shutdownNow();
        }
    }

    private static ImportedFile readFile(
            TrinoFileSystem fileSystem,
            String dataPath,
            DuckLakeAddFilesHandle handle,
            List<DuckLakeWriteColumn> columns,
            Optional<DuckLakePartitioning> partitioning,
            ParquetReaderOptions options,
            FileFormatDataSourceStats stats,
            String path)
    {
        validateLocation(dataPath, path);
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(StandardErrorCode.USER_CANCELED, "File registration was interrupted");
        }
        try {
            TrinoInputFile input = fileSystem.newInputFile(Location.of(path));
            long length = input.length();
            if (length < 12) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Parquet file is too short: " + path);
            }
            long footerSize;
            try (var in = input.newInput()) {
                if (!in.readFully(0, 4).toStringUtf8().equals("PAR1")) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet header: " + path);
                }
                Slice tail = in.readTail(8);
                if (!tail.slice(4, 4).toStringUtf8().equals("PAR1")) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet magic: " + path);
                }
                footerSize = Integer.toUnsignedLong(tail.getInt(0));
            }
            if (footerSize > length - 12) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet footer length: " + path);
            }
            if (footerSize + 8 > options.getMaxFooterReadSize().toBytes()) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Parquet footer exceeds the configured maximum: " + path);
            }
            String footerHash;
            ParquetMetadata metadata;
            try (var source = new TrinoParquetDataSource(input, options, stats)) {
                Slice footer = MetadataReader.readFooterBytes(source, ParquetReaderOptions.builder(options)
                        .withFooterReadSize(DataSize.ofBytes(footerSize + 8)).build());
                footerHash = Hashing.sha256().hashBytes(footer.getBytes()).toString();
                metadata = MetadataReader.parseFooter(source.getId(), length, footer, Optional.empty(), Optional.empty());
            }
            MessageType schema = metadata.getFileMetaData().getSchema();
            if (metadata.getParquetMetadata().isSetEncryption_algorithm()) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Encrypted Parquet files cannot be registered: " + path);
            }
            long rows = metadata.getParquetMetadata().getNum_rows();
            if (rows < 0 || metadata.getBlocks().stream().mapToLong(block -> block.rowCount()).reduce(0, Math::addExact) != rows) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Inconsistent Parquet row count: " + path);
            }
            Map<String, String> hiveValues = handle.hivePartitioning() ? hiveValues(path) : Map.of();
            ImmutableList.Builder<NameMappingColumn> mapping = ImmutableList.builder();
            Map<List<String>, DuckLakeParquetSchema.LeafField> leaves = new LinkedHashMap<>();
            mapColumns(schema, DuckLakeParquetSchema.create(columns).messageType(), columns, OptionalLong.empty(), handle, hiveValues, mapping, ImmutableList.of(), leaves);
            List<NameMappingColumn> mappings = mapping.build();
            if (mappings.isEmpty()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "A file must map at least one table column");
            }
            List<DuckLakeFileColumnStatsRow> columnStats = DuckLakeImportStatistics.collect(metadata, leaves);
            List<DuckLakeFileColumnStatsRow> completeStats = completeStatistics(columns, mappings, hiveValues, rows, columnStats);
            List<Optional<String>> partitions = partitioning.map(value -> partitionValues(value, columns, completeStats)).orElse(ImmutableList.of());
            return new ImportedFile(new DuckLakeDataFile(path, rows, length, footerSize, partitions, completeStats), mappings, footerHash);
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not inspect Parquet file: " + path, e);
        }
    }

    static String fingerprint(DuckLakeAddFilesHandle handle, List<ImportedFile> files)
    {
        var hash = Hashing.sha256().newHasher()
                .putInt(1).putLong(handle.table().tableId())
                .putBoolean(handle.allowMissing()).putBoolean(handle.ignoreExtraColumns())
                .putBoolean(handle.hivePartitioning()).putBoolean(handle.replace()).putInt(files.size());
        handle.replacementRange().ifPresent(range -> {
            hash.putString("replacement_range_v1", UTF_8);
            hash.putLong(range.column().columnId());
            for (String value : ImmutableList.of(range.column().type().toString(), range.start(), range.end())) {
                byte[] bytes = value.getBytes(UTF_8);
                hash.putInt(bytes.length).putBytes(bytes);
            }
        });
        for (ImportedFile file : files) {
            byte[] path = file.file().path().getBytes(UTF_8);
            hash.putInt(path.length).putBytes(path).putLong(file.file().fileSizeBytes()).putString(file.footerHash(), UTF_8);
        }
        return hash.hash().toString();
    }

    static void validateLocation(String dataPath, String path)
    {
        String prefix = dataPath.endsWith("/") ? dataPath : dataPath + "/";
        Location.of(path);
        String decoded = URLDecoder.decode(path.replace("+", "%2B"), UTF_8);
        if (!path.startsWith(prefix) || path.equals(prefix) || path.contains("?") || path.contains("#") || decoded.indexOf('\\') >= 0 || decoded.indexOf(0) >= 0) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Imported files must be under the catalog data path");
        }
        for (String segment : decoded.split("/")) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Imported file paths cannot contain traversal segments");
            }
        }
    }

    static Map<String, String> hiveValues(String path)
    {
        Map<String, String> values = new HashMap<>();
        String directories = Location.of(path).path();
        directories = directories.substring(0, Math.max(0, directories.lastIndexOf('/')));
        for (String segment : directories.split("/")) {
            int separator = segment.indexOf('=');
            if (separator > 0) {
                String key = URLDecoder.decode(segment.substring(0, separator).replace("+", "%2B"), UTF_8).toLowerCase(ENGLISH);
                String value = URLDecoder.decode(segment.substring(separator + 1).replace("+", "%2B"), UTF_8);
                if (values.putIfAbsent(key, value) != null) {
                    throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Duplicate Hive partition key: " + key);
                }
            }
        }
        return values;
    }

    private static List<DuckLakeFileColumnStatsRow> completeStatistics(
            List<DuckLakeWriteColumn> columns,
            List<NameMappingColumn> mapping,
            Map<String, String> hiveValues,
            long rows,
            List<DuckLakeFileColumnStatsRow> statistics)
    {
        Map<Long, DuckLakeFileColumnStatsRow> stats = new LinkedHashMap<>();
        statistics.forEach(value -> stats.put(value.columnId(), value));
        Map<Long, NameMappingColumn> mapped = new HashMap<>();
        mapping.forEach(value -> mapped.put(value.targetFieldId(), value));
        for (DuckLakeWriteColumn column : columns) {
            NameMappingColumn entry = mapped.get(column.columnId());
            if (entry == null || entry.hivePartition()) {
                Optional<String> value = Optional.empty();
                if (entry != null) {
                    String raw = hiveValues.get(entry.sourceName().toLowerCase(ENGLISH));
                    if (!raw.equals("__HIVE_DEFAULT_PARTITION__")) {
                        Object parsed = StatsValueParser.parse(column.type(), raw)
                                .orElseThrow(() -> new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Hive partition value for " + column.name()));
                        var builder = column.type().createBlockBuilder(null, 1);
                        TypeUtils.writeNativeValue(column.type(), builder, parsed);
                        value = DuckLakePartitionValues.format(column.type(), builder.build(), 0);
                    }
                }
                stats.put(column.columnId(), new DuckLakeFileColumnStatsRow(
                        column.columnId(),
                        OptionalLong.of(0),
                        OptionalLong.of(rows),
                        OptionalLong.of(value.isEmpty() ? rows : 0),
                        value,
                        value,
                        Optional.of(false)));
            }
            if (!column.nullsAllowed()) {
                DuckLakeFileColumnStatsRow value = stats.get(column.columnId());
                if (value == null || value.nullCount().isEmpty() || value.nullCount().orElseThrow() != 0) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Cannot prove that required column contains no NULLs: " + column.name());
                }
            }
        }
        return ImmutableList.copyOf(stats.values());
    }

    private static List<Optional<String>> partitionValues(
            DuckLakePartitioning partitioning,
            List<DuckLakeWriteColumn> columns,
            List<DuckLakeFileColumnStatsRow> statistics)
    {
        Map<Long, DuckLakeFileColumnStatsRow> stats = new HashMap<>();
        statistics.forEach(value -> stats.put(value.columnId(), value));
        ImmutableList.Builder<Optional<String>> values = ImmutableList.builder();
        for (DuckLakePartitioning.Field field : partitioning.fields()) {
            DuckLakeFileColumnStatsRow value = stats.get(field.columnId());
            // Only publish a partition constraint proven by the footer or a path constant.
            // A file spanning partitions remains unpartitioned and is never incorrectly pruned.
            if (value == null || value.nullCount().isEmpty() || value.valueCount().isEmpty()) {
                return ImmutableList.of();
            }
            if (value.nullCount().orElseThrow() == value.valueCount().orElseThrow()) {
                values.add(Optional.empty());
                continue;
            }
            if (value.nullCount().orElseThrow() != 0 || value.minValue().isEmpty() || value.maxValue().isEmpty()) {
                return ImmutableList.of();
            }
            String minimum = value.minValue().orElseThrow();
            String maximum = value.maxValue().orElseThrow();
            String transform = field.transform().toLowerCase(ENGLISH);
            if (!transform.equals("identity")) {
                DuckLakeWriteColumn column = columns.stream().filter(item -> item.columnId() == field.columnId()).findFirst().orElseThrow();
                if (!column.type().equals(DateType.DATE) && !(column.type() instanceof TimestampType)) {
                    return ImmutableList.of();
                }
                try {
                    LocalDateTime low = parseTemporal(minimum);
                    LocalDateTime high = parseTemporal(maximum);
                    if (!partitionInterval(low, transform).equals(partitionInterval(high, transform))) {
                        return ImmutableList.of();
                    }
                    minimum = temporalPartition(low, transform);
                    maximum = temporalPartition(high, transform);
                }
                catch (DateTimeException e) {
                    return ImmutableList.of();
                }
            }
            if (!minimum.equals(maximum)) {
                return ImmutableList.of();
            }
            values.add(Optional.of(minimum));
        }
        return values.build();
    }

    private static LocalDateTime parseTemporal(String value)
    {
        if (!value.contains(" ")) {
            return LocalDate.parse(value).atStartOfDay();
        }
        return LocalDateTime.parse(value.replace(' ', 'T'));
    }

    private static LocalDateTime partitionInterval(LocalDateTime value, String transform)
    {
        return switch (transform) {
            case "year" -> value.withDayOfYear(1).toLocalDate().atStartOfDay();
            case "month" -> value.withDayOfMonth(1).toLocalDate().atStartOfDay();
            case "day" -> value.toLocalDate().atStartOfDay();
            case "hour" -> value.truncatedTo(ChronoUnit.HOURS);
            default -> throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Unsupported partition transform: " + transform);
        };
    }

    private static String temporalPartition(LocalDateTime value, String transform)
    {
        return Integer.toString(switch (transform) {
            case "year" -> value.getYear();
            case "month" -> value.getMonthValue();
            case "day" -> value.getDayOfMonth();
            case "hour" -> value.getHour();
            default -> throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Unsupported partition transform: " + transform);
        });
    }

    private static void mapColumns(
            GroupType source,
            GroupType expected,
            List<DuckLakeWriteColumn> columns,
            OptionalLong parent,
            DuckLakeAddFilesHandle handle,
            Map<String, String> hiveValues,
            ImmutableList.Builder<NameMappingColumn> mapping,
            List<String> parentPath,
            Map<List<String>, DuckLakeParquetSchema.LeafField> leaves)
    {
        Map<String, Type> byName = new HashMap<>();
        for (Type field : source.getFields()) {
            if (field.getName().startsWith("$ducklake_missing_column$")) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Reserved Parquet field name: " + field.getName());
            }
            if (byName.putIfAbsent(field.getName().toLowerCase(ENGLISH), field) != null) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Ambiguous Parquet column name: " + field.getName());
            }
        }
        for (DuckLakeWriteColumn column : columns) {
            if (parent.isPresent() && (DuckLakeTypes.isUnsignedInteger(column.duckLakeType())
                    || DuckLakeTypes.isUnsignedBigint(column.duckLakeType()) || DuckLakeTypes.isInt128(column.duckLakeType()))) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Nested name mappings requiring integer widening are not supported: " + column.name());
            }
            Type physical = byName.remove(column.name().toLowerCase(ENGLISH));
            if (physical == null) {
                if (parent.isEmpty() && hiveValues.containsKey(column.name().toLowerCase(ENGLISH))) {
                    mapping.add(new NameMappingColumn(column.name(), column.columnId(), parent, true));
                    continue;
                }
                if (!handle.allowMissing() || !column.nullsAllowed()) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Missing required column: " + column.name());
                }
                continue;
            }
            Type expectedType = expected.getType(column.name());
            validateType(physical, expectedType, column.name());
            mapping.add(new NameMappingColumn(physical.getName(), column.columnId(), parent, false));
            List<String> sourcePath = ImmutableList.<String>builder().addAll(parentPath).add(physical.getName()).build();
            if (physical.isPrimitive()) {
                leaves.put(sourcePath, DuckLakeParquetSchema.create(ImmutableList.of(column)).leafField(ImmutableList.of(column.name())).orElseThrow());
            }
            if (!column.children().isEmpty()) {
                GroupType physicalGroup = physical.asGroupType();
                GroupType expectedGroup = expectedType.asGroupType();
                if (column.type() instanceof ArrayType || column.type() instanceof MapType) {
                    if (physicalGroup.getFieldCount() != 1 || physicalGroup.getType(0).isPrimitive()
                            || physicalGroup.getType(0).getRepetition() != Repetition.REPEATED) {
                        throw new TrinoException(DUCKLAKE_BAD_DATA, "Expected the standard Parquet list/map encoding for " + column.name());
                    }
                    sourcePath = ImmutableList.<String>builder().addAll(sourcePath).add(physicalGroup.getType(0).getName()).build();
                    physicalGroup = physicalGroup.getType(0).asGroupType();
                    expectedGroup = expectedGroup.getType(0).asGroupType();
                }
                int previousLeaves = leaves.size();
                mapColumns(physicalGroup, expectedGroup, column.children(), OptionalLong.of(column.columnId()), handle, Map.of(), mapping, sourcePath, leaves);
                if (column.type() instanceof RowType && leaves.size() == previousLeaves) {
                    // The Parquet reader needs one physical field to recover the parent's NULL
                    // bitmap. Otherwise a non-NULL row silently becomes a NULL row.
                    throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "A row must map at least one physical field: " + column.name());
                }
            }
        }
        if (!handle.ignoreExtraColumns() && !byName.isEmpty()) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Parquet file contains columns absent from the table: " + byName.keySet());
        }
    }

    private static void validateType(Type source, Type expected, String name)
    {
        boolean matches = source.isPrimitive() == expected.isPrimitive()
                && source.getRepetition() != Repetition.REPEATED;
        if (expected.getRepetition() == Repetition.REQUIRED && source.getRepetition() != Repetition.REQUIRED) {
            matches = false;
        }
        if (matches && source.isPrimitive()) {
            matches = source.asPrimitiveType().getPrimitiveTypeName() == expected.asPrimitiveType().getPrimitiveTypeName()
                    && Objects.equals(source.getLogicalTypeAnnotation(), expected.getLogicalTypeAnnotation());
            if (matches && source.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                matches = source.asPrimitiveType().getTypeLength() == expected.asPrimitiveType().getTypeLength();
            }
        }
        else if (matches) {
            matches = Objects.equals(source.getLogicalTypeAnnotation(), expected.getLogicalTypeAnnotation());
        }
        if (!matches) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Parquet type does not match column '%s': %s versus %s".formatted(name, source, expected));
        }
    }
}
