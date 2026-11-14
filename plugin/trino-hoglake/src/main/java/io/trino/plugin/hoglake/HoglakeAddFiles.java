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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hoglake.HoglakeAddFilesHandle.ReplacementRange;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.floorDiv;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Registers Parquet files that already sit in the catalog's storage, and optionally ends the
 * data files they replace in the same snapshot.
 *
 * <p>Planning ({@link #plan}) checks the properties and the table, and fails before any file is
 * read. Execution ({@link #execute}) checks the receipt of the import first, so a repeated
 * statement does nothing. It then lists the files, reads their footers, validates each against
 * the table, and commits through Hoglake: an append when nothing is replaced, otherwise a
 * replacement that ends files and appends files in one snapshot.
 *
 * <p>Files are never deleted once a commit request was sent, whatever the answer. Only files that
 * validation refused are deleted, and only when {@code delete_on_failure} asks for it.
 */
final class HoglakeAddFiles
{
    static final String ADDED_DATA_FILES = "added_data_files";
    static final String ENDED_DATA_FILES = "ended_data_files";
    static final String REPLACEMENT_CAPABILITY = "atomic-partition-replacement-v1";
    static final String IDEMPOTENT_APPEND_CAPABILITY = "idempotent-append-v1";

    private static final Logger log = Logger.get(HoglakeAddFiles.class);
    private static final int MAX_FILES = 10_000;
    private static final int MAX_RANGE_DAYS = 3660;
    private static final int MAX_CONCURRENT_FOOTER_READS = 8;
    private static final Pattern IMPORT_ID = Pattern.compile("[A-Za-z0-9_-]{1,128}");
    private static final Pattern RANGE_BOUND = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})(?:[ T](\\d{2}:\\d{2}:\\d{2})(?:\\.(\\d{1,9}))?)?(?: ?(?:UTC|Z|\\+00:00))?");
    private static final Set<String> DAY_SOURCE_TYPES = Set.of("date", "timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns", "timestamptz");
    private static final Set<String> RANGE_SOURCE_TYPES = Set.of("date", "timestamp", "timestamptz");

    private HoglakeAddFiles() {}

    /**
     * Whether the statement replaces data: {@code replace}, or any replacement range property.
     * The caller needs the permission to delete from the table for it.
     */
    static boolean replaces(Map<String, Object> properties)
    {
        return Boolean.TRUE.equals(properties.get("replace")) ||
                properties.get("replace_range_column") != null ||
                properties.get("replace_range_start") != null ||
                properties.get("replace_range_end") != null;
    }

    static HoglakeAddFilesHandle plan(HoglakeClient client, HoglakeTableHandle table, Map<String, Object> properties)
    {
        Optional<String> location = Optional.ofNullable((String) properties.get("location"));
        Object rawFiles = properties.get("files");
        Optional<String> prefix = Optional.ofNullable((String) properties.get("file_prefix"));
        boolean deleteOnFailure = Boolean.TRUE.equals(properties.get("delete_on_failure"));
        boolean replace = Boolean.TRUE.equals(properties.get("replace"));
        if (location.isPresent() == (rawFiles != null)) {
            throw argument("Exactly one of location and files is required");
        }
        if (location.isPresent() && location.get().isBlank()) {
            throw argument("location must not be blank");
        }
        if (prefix.isPresent() && location.isEmpty()) {
            throw argument("file_prefix requires location");
        }
        if (deleteOnFailure && location.isEmpty()) {
            throw argument("delete_on_failure requires location");
        }
        Optional<Long> expectedRows = Optional.ofNullable((Long) properties.get("expected_rows"));
        if (expectedRows.isPresent() && expectedRows.get() < 0) {
            throw argument("expected_rows must not be negative");
        }
        if (!(properties.get("import_id") instanceof String importId) || !IMPORT_ID.matcher(importId).matches()) {
            throw argument("import_id must contain 1 to 128 letters, digits, underscores or hyphens");
        }
        List<String> files = ImmutableList.of();
        if (rawFiles != null) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            Set<String> unique = new HashSet<>();
            if (!(rawFiles instanceof List<?> values) || values.size() > MAX_FILES) {
                throw argument("files must contain at most " + MAX_FILES + " Parquet locations");
            }
            for (Object value : values) {
                if (!(value instanceof String path) || path.isBlank() || !unique.add(canonical(path))) {
                    throw argument("files must contain distinct non-null locations");
                }
                builder.add(path);
            }
            files = builder.build().stream().sorted().toList();
        }

        Optional<ReplacementRange> range = replacementRange(table, properties);
        if (replace && range.isPresent()) {
            throw argument("replace and replace_range_column are mutually exclusive");
        }
        if (location.isEmpty() && files.isEmpty() && !replace && range.isEmpty()) {
            throw argument("files must contain at least one Parquet location, unless the registration replaces data");
        }

        HoglakeDtos.Catalog catalog = client.getCatalog();
        List<String> capabilities = catalog.capabilities() == null ? List.of() : catalog.capabilities();
        if (!capabilities.contains(IDEMPOTENT_APPEND_CAPABILITY)) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support " + IDEMPOTENT_APPEND_CAPABILITY + " required for add_files");
        }
        if ((replace || range.isPresent()) && !capabilities.contains(REPLACEMENT_CAPABILITY)) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake server does not support " + REPLACEMENT_CAPABILITY + " required to replace data with add_files");
        }
        String dataPath = catalog.dataPath().endsWith("/") ? catalog.dataPath() : catalog.dataPath() + "/";
        location.ifPresent(value -> checkInsideDataPath(dataPath, value.endsWith("/") ? value : value + "/"));
        files.forEach(value -> checkInsideDataPath(dataPath, value));

        HoglakeDtos.Table definition = client.getTable(table.schemaName(), table.tableName(), table.snapshotId())
                .orElseThrow(() -> new TrinoException(StandardErrorCode.TABLE_NOT_FOUND, "Table no longer exists: " + table.schemaTableName()));
        List<HoglakeDtos.PartitionField> partitionFields = HoglakePartitioning.read(definition.partitionSpec());
        HoglakePartitioning.validate(partitionFields, table.columns());
        Optional<HoglakeColumnHandle> partitionColumn = partitionColumn(partitionFields, table);
        if (range.isPresent()) {
            HoglakeColumnHandle column = partitionColumn.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Replacing a range requires a table partitioned by day(%s)".formatted(range.get().column())));
            if (!column.name().equals(range.get().column()) || !RANGE_SOURCE_TYPES.contains(column.hoglakeType())) {
                throw new TrinoException(NOT_SUPPORTED, "Replacing a range requires a table partitioned by day(%s) on a date, timestamp or timestamp with time zone column".formatted(range.get().column()));
            }
        }
        return new HoglakeAddFilesHandle(table, location, files, prefix, importId, expectedRows, Boolean.TRUE.equals(properties.get("allow_missing")), deleteOnFailure, replace, range, partitionFields);
    }

    /**
     * The one column of a table partitioned by a single {@code day} field. An unpartitioned table has none.
     */
    private static Optional<HoglakeColumnHandle> partitionColumn(List<HoglakeDtos.PartitionField> fields, HoglakeTableHandle table)
    {
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        if (fields.size() != 1 || !fields.getFirst().transform().equals("day")) {
            throw new TrinoException(NOT_SUPPORTED, "add_files supports tables that are unpartitioned or partitioned by one day() field");
        }
        List<HoglakeColumnHandle> chain = HoglakePartitioning.find(table.columns(), fields.getFirst().sourceFieldId());
        if (chain.size() != 1 || !DAY_SOURCE_TYPES.contains(chain.getFirst().hoglakeType())) {
            throw new TrinoException(NOT_SUPPORTED, "add_files supports day() partitions only on top-level date and timestamp columns");
        }
        return Optional.of(chain.getFirst());
    }

    private static Optional<ReplacementRange> replacementRange(HoglakeTableHandle table, Map<String, Object> properties)
    {
        String name = (String) properties.get("replace_range_column");
        String start = (String) properties.get("replace_range_start");
        String end = (String) properties.get("replace_range_end");
        if (name == null && start == null && end == null) {
            return Optional.empty();
        }
        if (name == null || start == null || end == null) {
            throw argument("replace_range_column, replace_range_start and replace_range_end must be specified together");
        }
        HoglakeColumnHandle column = table.columns().stream().filter(value -> value.name().equalsIgnoreCase(name)).findFirst()
                .orElseThrow(() -> argument("Replacement range column does not exist: " + name));
        long startDay = boundDay("replace_range_start", start);
        long endDay = boundDay("replace_range_end", end);
        if (startDay >= endDay) {
            throw argument("replace_range_start must be before replace_range_end");
        }
        if (endDay - startDay > MAX_RANGE_DAYS) {
            throw argument("A replacement range covers at most %s days".formatted(MAX_RANGE_DAYS));
        }
        return Optional.of(new ReplacementRange(column.name(), startDay, endDay));
    }

    /**
     * The day since the epoch of a bound that lies on a UTC day boundary, such as
     * {@code 2026-11-09 00:00:00.000000 UTC} or {@code 2026-11-09}.
     */
    private static long boundDay(String property, String value)
    {
        Matcher matcher = RANGE_BOUND.matcher(value.trim());
        if (!matcher.matches()) {
            throw argument("%s must be a UTC timestamp such as '2026-11-09 00:00:00.000000 UTC'".formatted(property));
        }
        LocalDate date;
        try {
            date = LocalDate.parse(matcher.group(1));
        }
        catch (DateTimeParseException e) {
            throw argument("%s is not a valid date: %s".formatted(property, value));
        }
        boolean midnight = (matcher.group(2) == null || matcher.group(2).equals("00:00:00")) &&
                (matcher.group(3) == null || matcher.group(3).chars().allMatch(digit -> digit == '0'));
        if (!midnight) {
            throw argument("%s must lie on a UTC day boundary: %s".formatted(property, value));
        }
        return date.toEpochDay();
    }

    /**
     * Files may be registered only from the catalog's own storage, which the server enforces as well.
     */
    private static void checkInsideDataPath(String dataPath, String location)
    {
        if (!location.startsWith(dataPath) || location.contains("/../") || location.endsWith("/..")) {
            throw argument("Location is outside the catalog data path %s: %s".formatted(dataPath, location));
        }
    }

    private static String canonical(String location)
    {
        return Location.of(location).toString();
    }

    static Map<String, Long> execute(ConnectorSession session, HoglakeClient client, TrinoFileSystem fileSystem, HoglakeAddFilesHandle handle)
    {
        HoglakeTableHandle table = handle.table();
        String key = idempotencyKey(table, handle.importId());
        // A repeat of an applied import is recognized before anything is listed or read
        if (client.commitReceipt(key).isPresent()) {
            return metrics(0, 0);
        }

        List<ListedFile> listed = list(fileSystem, handle);
        List<HoglakeDtos.FileRegistration> registrations;
        try {
            registrations = validate(fileSystem, handle, listed);
            long rows = registrations.stream().mapToLong(HoglakeDtos.FileRegistration::recordCount).sum();
            if (handle.expectedRows().isPresent() && handle.expectedRows().get() != rows) {
                throw new FilesRefused("The %s files to register hold %s rows, but expected_rows is %s".formatted(registrations.size(), rows, handle.expectedRows().get()));
            }
        }
        catch (FilesRefused refusal) {
            if (handle.deleteOnFailure()) {
                deleteQuietly(fileSystem, listed);
            }
            throw refusal;
        }

        List<HoglakeDtos.FileRegistration> files = registrations.stream().sorted(comparing(HoglakeDtos.FileRegistration::path)).toList();
        List<HoglakeDtos.Append> appends = files.isEmpty() ? List.of() : List.of(new HoglakeDtos.Append(table.schemaName(), table.tableName(), table.tableUuid(), files));
        if (!handle.replacement()) {
            if (files.isEmpty()) {
                return metrics(0, 0);
            }
            client.commit(new HoglakeDtos.Commit(table.snapshotId(), appends, key));
            return metrics(files.size(), 0);
        }
        if (session.getUser().isBlank()) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake requires a nonblank user for add_files");
        }
        HoglakeDtos.FileEnd end = handle.range()
                .map(range -> new HoglakeDtos.FileEnd(table.schemaName(), table.tableName(), table.tableUuid(), range.partitionValues().stream().map(List::of).toList(), false))
                .orElseGet(() -> new HoglakeDtos.FileEnd(table.schemaName(), table.tableName(), table.tableUuid(), null, true));
        OptionalLong ended = client.commitReplacement(new HoglakeDtos.ReplacementCommit(
                table.snapshotId(),
                appends,
                List.of(),
                List.of(end),
                key,
                session.getUser(),
                "Trino add_files, query %s".formatted(session.getQueryId())));
        // Only a receipt confirmed the commit when the count is missing, and a receipt does not carry it
        return metrics(files.size(), ended.orElse(-1));
    }

    private static Map<String, Long> metrics(long added, long ended)
    {
        return ImmutableMap.of(ADDED_DATA_FILES, added, ENDED_DATA_FILES, ended);
    }

    static String idempotencyKey(HoglakeTableHandle table, String importId)
    {
        return UUID.nameUUIDFromBytes(("hoglake-add-files:" + table.tableUuid() + ":" + importId).getBytes(UTF_8)).toString();
    }

    private record ListedFile(Location location, long length) {}

    private static List<ListedFile> list(TrinoFileSystem fileSystem, HoglakeAddFilesHandle handle)
    {
        List<ListedFile> result = new ArrayList<>();
        try {
            if (handle.location().isPresent()) {
                String directory = handle.location().get();
                FileIterator iterator = fileSystem.listFiles(Location.of(directory.endsWith("/") ? directory : directory + "/"));
                while (iterator.hasNext()) {
                    FileEntry entry = iterator.next();
                    String name = entry.location().fileName();
                    if (name.endsWith(".parquet") && handle.filePrefix().map(name::startsWith).orElse(true)) {
                        result.add(new ListedFile(entry.location(), entry.length()));
                    }
                }
            }
            else {
                for (String file : handle.files()) {
                    Location location = Location.of(file);
                    try {
                        result.add(new ListedFile(location, fileSystem.newInputFile(location).length()));
                    }
                    catch (FileNotFoundException e) {
                        throw argument("File does not exist: " + file);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to list files to register: " + e.getMessage(), e);
        }
        if (result.size() > MAX_FILES) {
            throw argument("At most %s files can be registered at once, found %s".formatted(MAX_FILES, result.size()));
        }
        result.sort(comparing(file -> file.location().toString()));
        return result;
    }

    private static void deleteQuietly(TrinoFileSystem fileSystem, List<ListedFile> listed)
    {
        try {
            fileSystem.deleteFiles(listed.stream().map(ListedFile::location).toList());
        }
        catch (IOException | RuntimeException e) {
            log.warn(e, "Could not delete files refused by add_files");
        }
    }

    /**
     * A file that cannot be registered, as opposed to a failure to read it. Only the first deletes files.
     */
    private static final class FilesRefused
            extends TrinoException
    {
        private FilesRefused(String message)
        {
            super(INVALID_PROCEDURE_ARGUMENT, message);
        }
    }

    private static List<HoglakeDtos.FileRegistration> validate(TrinoFileSystem fileSystem, HoglakeAddFilesHandle handle, List<ListedFile> listed)
    {
        if (listed.isEmpty()) {
            return List.of();
        }
        ExecutorService executor = Executors.newFixedThreadPool(min(MAX_CONCURRENT_FOOTER_READS, listed.size()), runnable -> {
            Thread thread = new Thread(runnable, "hoglake-add-files-footer");
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<Future<Optional<HoglakeDtos.FileRegistration>>> futures = new ArrayList<>();
            for (ListedFile file : listed) {
                futures.add(executor.submit(() -> validateFile(fileSystem, handle, file)));
            }
            List<HoglakeDtos.FileRegistration> result = new ArrayList<>();
            RuntimeException failure = null;
            for (Future<Optional<HoglakeDtos.FileRegistration>> future : futures) {
                try {
                    future.get().ifPresent(result::add);
                }
                catch (ExecutionException e) {
                    RuntimeException cause = e.getCause() instanceof RuntimeException runtime ? runtime : new RuntimeException(e.getCause());
                    // A refusal outranks a read failure: the files are bad whatever else went wrong
                    if (failure == null || (cause instanceof FilesRefused && !(failure instanceof FilesRefused))) {
                        failure = cause;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Interrupted while reading footers", e);
                }
            }
            if (failure != null) {
                throw failure;
            }
            return result;
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static Optional<HoglakeDtos.FileRegistration> validateFile(TrinoFileSystem fileSystem, HoglakeAddFilesHandle handle, ListedFile file)
    {
        String path = file.location().toString();
        ParquetMetadata metadata;
        long footerSize;
        ParquetReaderOptions options = ParquetReaderOptions.builder().build();
        try (HoglakeParquetDataSource dataSource = new HoglakeParquetDataSource(fileSystem.newInputFile(file.location(), file.length()), file.length(), options)) {
            if (file.length() < 12) {
                throw new FilesRefused("Not a Parquet file, it has %s bytes: %s".formatted(file.length(), path));
            }
            Slice tail = dataSource.readTail(8);
            if (!tail.slice(4, 4).toStringUtf8().equals("PAR1")) {
                throw new FilesRefused("Not a Parquet file: " + path);
            }
            footerSize = tail.getInt(0);
            Slice footer = MetadataReader.readFooterBytes(dataSource, options);
            metadata = MetadataReader.parseFooter(dataSource.getId(), dataSource.getEstimatedSize(), footer, Optional.empty(), Optional.empty());
        }
        catch (ParquetCorruptionException e) {
            throw new FilesRefused("Not a readable Parquet file: %s: %s".formatted(path, e.getMessage()));
        }
        catch (FileNotFoundException e) {
            throw argument("File does not exist: " + path);
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to read the footer of %s: %s".formatted(path, e.getMessage()), e);
        }
        long rows = blocks(metadata).stream().mapToLong(BlockMetadata::rowCount).sum();
        if (rows == 0) {
            // Nothing to register, and no statistics to derive a partition from
            return Optional.empty();
        }
        List<String> partitionValues = checkAgainstTable(handle, metadata, path);
        return Optional.of(new HoglakeDtos.FileRegistration(path, rows, file.length(), footerSize, partitionValues));
    }

    /**
     * Refuses a file the table cannot read, and returns its partition values.
     */
    private static List<String> checkAgainstTable(HoglakeAddFilesHandle handle, ParquetMetadata metadata, String path)
    {
        List<HoglakeColumnHandle> columns = handle.table().columns();
        MessageType fileSchema = metadata.getFileMetaData().getSchema();
        Map<Type, HoglakeColumnHandle> bound = new IdentityHashMap<>();
        List<Optional<Type>> bindings = new ArrayList<>();
        for (HoglakeColumnHandle column : columns) {
            Optional<Type> field = HoglakePageSourceProvider.bindColumn(fileSchema, column);
            bindings.add(field);
            if (field.isPresent()) {
                bound.put(field.get(), column);
            }
            else if (!handle.allowMissing()) {
                throw new FilesRefused("File %s has no column %s, and allow_missing is not set".formatted(path, column.name()));
            }
            else if (!column.nullable()) {
                throw new FilesRefused("File %s has no column %s, which is not nullable".formatted(path, column.name()));
            }
        }
        for (Type field : fileSchema.getFields()) {
            if (!bound.containsKey(field)) {
                throw new FilesRefused("File %s has column %s, which is not a column of the table".formatted(path, field.getName()));
            }
        }
        try {
            HoglakePageSourceProvider.checkNoCaseCollision(fileSchema, metadata, columns, path);
        }
        catch (TrinoException e) {
            throw new FilesRefused(e.getRawMessage());
        }
        for (int i = 0; i < columns.size(); i++) {
            if (bindings.get(i).isPresent()) {
                checkColumn(columns.get(i), bindings.get(i).get(), true, metadata, path);
            }
        }
        checkReadable(columns, bindings, fileSchema, path);

        Optional<HoglakeColumnHandle> partitionColumn = partitionColumn(handle.partitionFields(), handle.table());
        if (partitionColumn.isEmpty()) {
            return List.of();
        }
        long day = partitionDay(partitionColumn.get(), bindings.get(columns.indexOf(partitionColumn.get())), metadata, path);
        String value = HoglakePartitioning.dayValue(day);
        if (handle.range().isPresent() && !handle.range().get().partitionValues().contains(value)) {
            throw new FilesRefused("File %s holds day %s (%s), outside the replaced range".formatted(path, LocalDate.ofEpochDay(day), value));
        }
        return List.of(value);
    }

    /**
     * Builds the reader's own field for each bound column, which fails for shapes it cannot read.
     */
    private static void checkReadable(List<HoglakeColumnHandle> columns, List<Optional<Type>> bindings, MessageType fileSchema, String path)
    {
        List<Type> boundFields = bindings.stream().flatMap(Optional::stream).toList();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, new MessageType(fileSchema.getName(), boundFields));
        for (int i = 0; i < columns.size(); i++) {
            if (bindings.get(i).isEmpty() || HoglakeParquetFields.isShreddedVariant(columns.get(i), bindings.get(i).get())) {
                continue;
            }
            try {
                HoglakeParquetFields.construct(columns.get(i), lookupColumnByName(messageColumn, bindings.get(i).get().getName()));
            }
            catch (TrinoException e) {
                throw new FilesRefused("Cannot read column %s from file %s: %s".formatted(columns.get(i).name(), path, e.getRawMessage()));
            }
        }
    }

    private static void checkColumn(HoglakeColumnHandle column, Type file, boolean topLevel, ParquetMetadata metadata, String path)
    {
        if (file.isRepetition(REPEATED)) {
            throw refusedType(column, file, path, "the file column is repeated");
        }
        if (!column.nullable() && file.isRepetition(OPTIONAL)) {
            // Only a column whose statistics show no nulls can be read as not nullable
            if (!topLevel || !file.isPrimitive() || !hasNoNulls(metadata, file.getName())) {
                throw refusedType(column, file, path, "the table column is not nullable, and the file column may hold nulls");
            }
        }
        switch (column.hoglakeType()) {
            case "variant" -> {
                if (file.isPrimitive()) {
                    throw refusedType(column, file, path, "the file column is not a group");
                }
            }
            case "struct" -> {
                if (file.isPrimitive()) {
                    throw refusedType(column, file, path, "the file column is not a group");
                }
                for (HoglakeColumnHandle child : column.children()) {
                    Optional<Type> childField = findChild(file.asGroupType(), child);
                    if (childField.isPresent()) {
                        checkColumn(child, childField.get(), false, metadata, path);
                    }
                    else if (!child.nullable()) {
                        throw refusedType(column, file, path, "the file column lacks the field %s, which is not nullable".formatted(child.name()));
                    }
                }
            }
            case "list" -> {
                GroupType repeated = repeatedChild(file, column, ListLogicalTypeAnnotation.class, 1, path);
                checkColumn(column.children().getFirst(), repeated.getType(0), false, metadata, path);
            }
            case "map" -> {
                GroupType repeated = repeatedChild(file, column, MapLogicalTypeAnnotation.class, 2, path);
                checkColumn(column.children().get(0), repeated.getType(0), false, metadata, path);
                checkColumn(column.children().get(1), repeated.getType(1), false, metadata, path);
            }
            default -> {
                if (!file.isPrimitive() || !readable(column, file.asPrimitiveType())) {
                    throw refusedType(column, file, path, "the types are not compatible");
                }
            }
        }
    }

    /**
     * The repeated group of a three-level list or map. Other layouts are refused.
     */
    private static GroupType repeatedChild(Type file, HoglakeColumnHandle column, Class<? extends LogicalTypeAnnotation> annotation, int fields, String path)
    {
        if (file.isPrimitive() || !annotation.isInstance(file.getLogicalTypeAnnotation())) {
            throw refusedType(column, file, path, "the file column is not a " + column.hoglakeType());
        }
        GroupType group = file.asGroupType();
        if (group.getFieldCount() != 1 || group.getType(0).isPrimitive() || !group.getType(0).isRepetition(REPEATED) || group.getType(0).asGroupType().getFieldCount() != fields) {
            throw refusedType(column, file, path, "the file column does not have the standard three-level layout");
        }
        return group.getType(0).asGroupType();
    }

    private static Optional<Type> findChild(GroupType group, HoglakeColumnHandle column)
    {
        for (Type child : group.getFields()) {
            if (child.getId() != null && child.getId().intValue() == column.fieldId()) {
                return Optional.of(child);
            }
        }
        for (Type child : group.getFields()) {
            if (child.getId() == null && child.getName().equals(column.name())) {
                return Optional.of(child);
            }
        }
        for (Type child : group.getFields()) {
            if (child.getId() == null && child.getName().equalsIgnoreCase(column.name())) {
                return Optional.of(child);
            }
        }
        return Optional.empty();
    }

    /**
     * Whether the file's primitive type can be read as the column, with the physical and logical
     * types the connector writes for it. The checks stay strict where a reader could silently
     * misread: timestamps need the same unit, but either UTC adjustment; strings may lack the
     * STRING annotation, as some writers omit it; a wider or narrower integer is refused.
     */
    private static boolean readable(HoglakeColumnHandle column, PrimitiveType file)
    {
        PrimitiveType expected = HoglakeParquetSchema.create(List.of(column)).messageType().getType(0).asPrimitiveType();
        LogicalTypeAnnotation actualAnnotation = file.getLogicalTypeAnnotation();
        return switch (column.hoglakeType()) {
            case "decimal" -> actualAnnotation instanceof DecimalLogicalTypeAnnotation &&
                    actualAnnotation.equals(expected.getLogicalTypeAnnotation()) &&
                    Set.of(INT32, INT64, FIXED_LEN_BYTE_ARRAY).contains(file.getPrimitiveTypeName());
            case "string" -> file.getPrimitiveTypeName() == BINARY && (actualAnnotation == null || actualAnnotation instanceof StringLogicalTypeAnnotation);
            case "int" -> file.getPrimitiveTypeName() == INT32 && (actualAnnotation == null || actualAnnotation.equals(LogicalTypeAnnotation.intType(32, true)));
            case "long" -> file.getPrimitiveTypeName() == INT64 && (actualAnnotation == null || actualAnnotation.equals(LogicalTypeAnnotation.intType(64, true)));
            default -> file.getPrimitiveTypeName() == expected.getPrimitiveTypeName() &&
                    file.getTypeLength() == expected.getTypeLength() &&
                    Objects.equals(unadjusted(expected.getLogicalTypeAnnotation()), unadjusted(actualAnnotation));
        };
    }

    private static LogicalTypeAnnotation unadjusted(LogicalTypeAnnotation annotation)
    {
        return switch (annotation) {
            case TimestampLogicalTypeAnnotation timestamp -> LogicalTypeAnnotation.timestampType(false, timestamp.getUnit());
            case TimeLogicalTypeAnnotation time -> LogicalTypeAnnotation.timeType(false, time.getUnit());
            case null, default -> annotation;
        };
    }

    private static FilesRefused refusedType(HoglakeColumnHandle column, Type file, String path, String reason)
    {
        return new FilesRefused("File %s cannot be read as column %s of type %s: %s (file column %s)".formatted(path, column.name(), column.type().getDisplayName(), reason, file));
    }

    private static List<BlockMetadata> blocks(ParquetMetadata metadata)
    {
        try {
            return metadata.getBlocks();
        }
        catch (ParquetCorruptionException e) {
            throw new FilesRefused("Not a readable Parquet file: " + e.getMessage());
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to read the row groups: " + e.getMessage(), e);
        }
    }

    private static List<ColumnChunkMetadata> chunks(ParquetMetadata metadata, String column)
    {
        List<ColumnChunkMetadata> chunks = new ArrayList<>();
        for (BlockMetadata block : blocks(metadata)) {
            if (block.rowCount() == 0) {
                continue;
            }
            block.columns().stream()
                    .filter(chunk -> chunk.getPath().size() == 1 && chunk.getPath().toArray()[0].equals(column))
                    .findFirst()
                    .ifPresent(chunks::add);
        }
        return chunks;
    }

    /**
     * Whether the statistics of every row group say that the column holds no nulls.
     */
    private static boolean hasNoNulls(ParquetMetadata metadata, String column)
    {
        List<ColumnChunkMetadata> chunks = chunks(metadata, column);
        return chunks.size() == (int) blocks(metadata).stream().filter(block -> block.rowCount() > 0).count() &&
                chunks.stream().allMatch(chunk -> chunk.getStatistics().isNumNullsSet() && chunk.getStatistics().getNumNulls() == 0);
    }

    /**
     * The day of every row of the file, from the statistics of its partition column. The smallest and
     * the largest value must fall in the same day, and the column must hold no nulls.
     */
    private static long partitionDay(HoglakeColumnHandle column, Optional<Type> binding, ParquetMetadata metadata, String path)
    {
        if (binding.isEmpty()) {
            throw new FilesRefused("File %s lacks the partition column %s".formatted(path, column.name()));
        }
        Type file = binding.get();
        if (!file.isRepetition(REQUIRED) && !hasNoNulls(metadata, file.getName())) {
            throw new FilesRefused("File %s has no statistics proving that partition column %s holds no nulls".formatted(path, column.name()));
        }
        List<ColumnChunkMetadata> chunks = chunks(metadata, file.getName());
        long minDay = Long.MAX_VALUE;
        long maxDay = Long.MIN_VALUE;
        for (ColumnChunkMetadata chunk : chunks) {
            Statistics<?> statistics = chunk.getStatistics();
            if (!statistics.hasNonNullValue() || !(statistics.genericGetMin() instanceof Number min) || !(statistics.genericGetMax() instanceof Number max)) {
                throw new FilesRefused("File %s has no min and max statistics for partition column %s".formatted(path, column.name()));
            }
            minDay = Math.min(minDay, toDay(min.longValue(), file.asPrimitiveType(), path));
            maxDay = Math.max(maxDay, toDay(max.longValue(), file.asPrimitiveType(), path));
        }
        if (chunks.isEmpty()) {
            throw new FilesRefused("File %s has no statistics for partition column %s".formatted(path, column.name()));
        }
        if (minDay != maxDay) {
            throw new FilesRefused("File %s spans more than one day of partition column %s: %s to %s".formatted(path, column.name(), LocalDate.ofEpochDay(minDay), LocalDate.ofEpochDay(maxDay)));
        }
        return minDay;
    }

    private static long toDay(long value, PrimitiveType file, String path)
    {
        LogicalTypeAnnotation annotation = file.getLogicalTypeAnnotation();
        if (annotation instanceof DateLogicalTypeAnnotation) {
            return value;
        }
        if (annotation instanceof TimestampLogicalTypeAnnotation timestamp) {
            return switch (timestamp.getUnit()) {
                case MILLIS -> floorDiv(value, 86_400_000L);
                case MICROS -> floorDiv(value, 86_400_000_000L);
                case NANOS -> floorDiv(value, 86_400_000_000_000L);
            };
        }
        throw new FilesRefused("File %s stores partition column %s as %s, which has no day".formatted(path, file.getName(), file));
    }

    private static TrinoException argument(String message)
    {
        return new TrinoException(INVALID_PROCEDURE_ARGUMENT, requireNonNull(message, "message is null"));
    }
}
