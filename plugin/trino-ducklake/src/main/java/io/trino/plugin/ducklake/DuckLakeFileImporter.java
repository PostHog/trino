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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.concurrent.Threads;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetCorruptionException;
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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static io.trino.plugin.ducklake.DuckLakeErrorCode.DUCKLAKE_FILESYSTEM_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

final class DuckLakeFileImporter
{
    // the footer length and the magic number that follow the footer at the end of the file
    private static final int FOOTER_POSTSCRIPT_SIZE = Integer.BYTES + 4;
    private static final String PLAINTEXT_FOOTER_MAGIC = "PAR1";

    private DuckLakeFileImporter() {}

    public record ImportedFile(DuckLakeDataFile file, List<NameMappingColumn> mapping) {}

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
            long footerSize;
            ParquetMetadata metadata;
            // a single read of the file tail holds the footer, unless it exceeds the footer read size
            try (var source = new TrinoParquetDataSource(input, options, stats)) {
                Slice footer = MetadataReader.readFooterBytes(source, options);
                if (!footer.slice(footer.length() - PLAINTEXT_FOOTER_MAGIC.length(), PLAINTEXT_FOOTER_MAGIC.length()).toStringUtf8().equals(PLAINTEXT_FOOTER_MAGIC)) {
                    throw new TrinoException(DUCKLAKE_BAD_DATA, "Encrypted Parquet files cannot be registered: " + path);
                }
                footerSize = (long) footer.length() - FOOTER_POSTSCRIPT_SIZE;
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
            Map<String, String> hiveValues = Map.of();
            if (handle.hivePartitioning()) {
                verifyDistinctHivePartitionKeys(path);
                hiveValues = hiveValues(path);
            }
            ImmutableList.Builder<NameMappingColumn> mapping = ImmutableList.builder();
            Map<List<String>, DuckLakeParquetSchema.LeafField> leaves = new LinkedHashMap<>();
            mapColumns(schema, DuckLakeParquetSchema.create(columns).messageType(), columns, OptionalLong.empty(), handle, hiveValues, mapping, ImmutableList.of(), leaves);
            List<NameMappingColumn> mappings = mapping.build();
            if (mappings.isEmpty()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "A file must map at least one table column");
            }
            List<DuckLakeFileColumnStatsRow> columnStats = DuckLakeImportStatistics.collect(metadata, leaves);
            List<DuckLakeFileColumnStatsRow> completeStats = completeStatistics(columns, schema, mappings, hiveValues, rows, columnStats);
            List<Optional<String>> partitions = partitioning.map(value -> partitionValues(value, columns, completeStats)).orElse(ImmutableList.of());
            return new ImportedFile(new DuckLakeDataFile(path, rows, length, footerSize, partitions, completeStats), mappings);
        }
        catch (ParquetCorruptionException e) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Invalid Parquet file: " + path, e);
        }
        catch (IOException e) {
            throw new TrinoException(DUCKLAKE_FILESYSTEM_ERROR, "Could not inspect Parquet file: " + path, e);
        }
    }

    /**
     * Identifies an import request: the target table, the options, and the files named. A retry of
     * the request is recognized from this alone, so it neither reads the files again nor
     * depends on the table schema, which may have changed since the import was applied.
     */
    static String fingerprint(DuckLakeAddFilesHandle handle)
    {
        Hasher hash = Hashing.sha256().newHasher()
                .putInt(2).putLong(handle.table().tableId())
                .putBoolean(handle.allowMissing()).putBoolean(handle.ignoreExtraColumns())
                .putBoolean(handle.hivePartitioning()).putBoolean(handle.replace()).putInt(handle.files().size());
        handle.replacementRange().ifPresent(range -> {
            hash.putString("replacement_range", UTF_8);
            hash.putLong(range.column().columnId());
            for (String value : range.canonicalBounds()) {
                putString(hash, value);
            }
        });
        for (String path : handle.files()) {
            putString(hash, path);
        }
        return hash.hash().toString();
    }

    private static void putString(Hasher hash, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        hash.putInt(bytes.length).putBytes(bytes);
    }

    static void validateLocation(String dataPath, String path)
    {
        String prefix = dataPath.endsWith("/") ? dataPath : dataPath + "/";
        try {
            Location.of(path);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid file location: " + path, e);
        }
        String decoded = decodePercentEscapes(path);
        if (!path.startsWith(prefix) || path.equals(prefix) || path.contains("?") || path.contains("#") || decoded.indexOf('\\') >= 0 || decoded.indexOf(0) >= 0) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Imported files must be under the catalog data path");
        }
        for (String segment : decoded.split("/")) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Imported file paths cannot contain traversal segments");
            }
        }
    }

    /**
     * The Hive partition values in the directories of a file location, keyed by the lowercase key.
     * They are parsed the way DuckDB parses them, so that both engines read the same values: the
     * key as written, the value with its %XX escapes decoded, and the first directory naming a key
     * supplies its value.
     */
    static Map<String, String> hiveValues(String path)
    {
        Map<String, String> values = new HashMap<>();
        for (Map.Entry<String, String> partition : hivePartitions(path)) {
            values.putIfAbsent(partition.getKey().toLowerCase(ENGLISH), decodePercentEscapes(partition.getValue()));
        }
        return values;
    }

    private static void verifyDistinctHivePartitionKeys(String path)
    {
        Set<String> keys = new HashSet<>();
        for (Map.Entry<String, String> partition : hivePartitions(path)) {
            if (!keys.add(partition.getKey().toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Duplicate Hive partition key: " + partition.getKey());
            }
        }
    }

    private static List<Map.Entry<String, String>> hivePartitions(String path)
    {
        String directories = Location.of(path).path();
        directories = directories.substring(0, Math.max(0, directories.lastIndexOf('/')));
        ImmutableList.Builder<Map.Entry<String, String>> partitions = ImmutableList.builder();
        for (String segment : directories.split("/")) {
            int separator = segment.indexOf('=');
            if (separator > 0) {
                partitions.add(Map.entry(segment.substring(0, separator), segment.substring(separator + 1)));
            }
        }
        return partitions.build();
    }

    /**
     * Decodes the %XX escapes of a location like DuckDB does: a '%' not followed by two hexadecimal
     * digits stays as it is, and so does a '+'. Object store keys may contain either.
     */
    static String decodePercentEscapes(String value)
    {
        if (value.indexOf('%') < 0) {
            return value;
        }
        byte[] encoded = value.getBytes(UTF_8);
        ByteArrayOutputStream decoded = new ByteArrayOutputStream(encoded.length);
        int index = 0;
        while (index < encoded.length) {
            if (encoded[index] == '%' && index + 2 < encoded.length) {
                int high = Character.digit(encoded[index + 1], 16);
                int low = Character.digit(encoded[index + 2], 16);
                if (high >= 0 && low >= 0) {
                    decoded.write((high << 4) | low);
                    index += 3;
                    continue;
                }
            }
            decoded.write(encoded[index]);
            index++;
        }
        return decoded.toString(UTF_8);
    }

    private static List<DuckLakeFileColumnStatsRow> completeStatistics(
            List<DuckLakeWriteColumn> columns,
            MessageType schema,
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
            if (!column.nullsAllowed() && rows > 0 && !provesNoNulls(schema, entry, stats.get(column.columnId()))) {
                throw new TrinoException(DUCKLAKE_BAD_DATA, "Cannot prove that required column contains no NULLs: " + column.name());
            }
        }
        return ImmutableList.copyOf(stats.values());
    }

    /**
     * Whether the file proves that a column holds no NULLs: a REQUIRED field cannot hold one, and
     * otherwise the footer has to count none. Footer statistics exist for primitive columns only,
     * so a nested column has to be REQUIRED.
     */
    private static boolean provesNoNulls(MessageType schema, NameMappingColumn entry, DuckLakeFileColumnStatsRow stats)
    {
        if (entry != null && !entry.hivePartition() && schema.getType(entry.sourceName()).getRepetition() == Repetition.REQUIRED) {
            return true;
        }
        return stats != null && stats.nullCount().isPresent() && stats.nullCount().orElseThrow() == 0;
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
                    if (!isStandardEncoding(physicalGroup, column)) {
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

    /**
     * Whether a list or map uses the standard three-level encoding: a repeated group holding exactly
     * the element, or the key and the value, under the names the table gives them. They cannot be
     * missing or extra, whatever the options. A file naming the element differently, as legacy
     * encodings do, would otherwise register an array whose elements the engines resolve
     * differently.
     */
    private static boolean isStandardEncoding(GroupType group, DuckLakeWriteColumn column)
    {
        if (group.getFieldCount() != 1 || group.getType(0).isPrimitive() || group.getType(0).getRepetition() != Repetition.REPEATED) {
            return false;
        }
        GroupType repeated = group.getType(0).asGroupType();
        Set<String> names = repeated.getFields().stream()
                .map(field -> field.getName().toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        Set<String> expectedNames = column.children().stream()
                .map(child -> child.name().toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        return repeated.getFieldCount() == column.children().size() && names.equals(expectedNames);
    }

    private static void validateType(Type source, Type expected, String name)
    {
        boolean matches = source.isPrimitive() == expected.isPrimitive()
                && source.getRepetition() != Repetition.REPEATED;
        if (expected.getRepetition() == Repetition.REQUIRED && source.getRepetition() != Repetition.REQUIRED) {
            matches = false;
        }
        if (matches && source.isPrimitive()) {
            matches = primitiveTypeMatches(source.asPrimitiveType(), expected.asPrimitiveType());
        }
        else if (matches) {
            matches = Objects.equals(source.getLogicalTypeAnnotation(), expected.getLogicalTypeAnnotation());
        }
        if (!matches) {
            throw new TrinoException(DUCKLAKE_BAD_DATA, "Parquet type does not match column '%s': %s versus %s".formatted(name, source, expected));
        }
    }

    /**
     * Whether the reader decodes the values of the file as those the table column holds. Writers
     * encode some types differently from DuckDB: a signed 32 or 64 bit integer may carry no
     * annotation, and a decimal may use any physical type that holds its precision, like the
     * minimal width fixed length arrays Spark and Arrow write.
     */
    private static boolean primitiveTypeMatches(PrimitiveType source, PrimitiveType expected)
    {
        if (expected.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation decimal) {
            if (!decimal.equals(source.getLogicalTypeAnnotation())) {
                return false;
            }
            return switch (source.getPrimitiveTypeName()) {
                case INT32 -> decimal.getPrecision() <= 9;
                case INT64 -> decimal.getPrecision() <= 18;
                case FIXED_LEN_BYTE_ARRAY -> true;
                default -> false;
            };
        }
        if (source.getPrimitiveTypeName() != expected.getPrimitiveTypeName()) {
            return false;
        }
        if (source.getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && source.getTypeLength() != expected.getTypeLength()) {
            return false;
        }
        return Objects.equals(effectiveAnnotation(source), effectiveAnnotation(expected));
    }

    private static LogicalTypeAnnotation effectiveAnnotation(PrimitiveType type)
    {
        if (type.getLogicalTypeAnnotation() != null) {
            return type.getLogicalTypeAnnotation();
        }
        // a plain INT32 or INT64 holds signed integers of that width
        return switch (type.getPrimitiveTypeName()) {
            case INT32 -> LogicalTypeAnnotation.intType(32, true);
            case INT64 -> LogicalTypeAnnotation.intType(64, true);
            default -> null;
        };
    }
}
