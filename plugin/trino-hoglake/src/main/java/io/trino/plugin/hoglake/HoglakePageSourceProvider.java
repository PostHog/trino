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

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.hoglake.HoglakeParquetFooterCache.ParsedFooter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Reads a split's parquet file from object storage through Trino's own
 * parquet reader (trino-parquet) and S3 filesystem (trino-filesystem-s3)
 * — the same data path the bundled hive/iceberg connectors use.
 *
 * Column binding: catalog columns bind to file columns by embedded
 * PARQUET:field_id when the file carries ids; files written without ids
 * bind by name (exact, then case-insensitive). Catalog columns absent
 * from the file (added after the file was written) read as nulls.
 *
 * <p>When the split carries a deletion vector, the file's rows are read
 * with their original file row positions and the rows the vector marks
 * deleted are dropped, so the vector stays tied to the snapshot the scan
 * pinned.
 */
public class HoglakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final int DOMAIN_COMPACTION_THRESHOLD = 100;
    // Parquet ends with a 4-byte footer length and the 4-byte "PAR1" magic.
    private static final int PARQUET_TRAILER_SIZE = Integer.BYTES + 4;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final HoglakeParquetFooterCache footerCache;

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        this(fileSystemFactory, HoglakeParquetFooterCache.disabled());
    }

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory, HoglakeParquetFooterCache footerCache)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.footerCache = requireNonNull(footerCache, "footerCache is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        HoglakeSplit hoglakeSplit = (HoglakeSplit) split;

        TupleDomain<HoglakeColumnHandle> predicate = ((HoglakeTableHandle) table).constraint();
        if (predicate.isNone()) {
            return new EmptyPageSource();
        }

        // With no columns or reader-side predicate, only row cardinality is needed (for
        // example, COUNT(*)). The catalog's record count describes a whole file at the
        // query's pinned snapshot, so only a whole-file split can answer from it. A range
        // split falls through to the reader with no columns, which counts the rows of its
        // row groups from the footer without reading any data page.
        if (columns.isEmpty() && predicate.isAll() && hoglakeSplit.recordCount() >= 0 && hoglakeSplit.wholeFile()) {
            return createCountPageSource(session, hoglakeSplit, memoryContext);
        }

        List<HoglakeColumnHandle> hoglakeColumns = columns.stream()
                .map(HoglakeColumnHandle.class::cast)
                .toList();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(hoglakeSplit.path()), hoglakeSplit.fileSizeBytes());

        ParquetReaderOptions options = readerOptions(ParquetReaderOptions.defaultOptions(), hoglakeSplit);
        ParquetDataSource dataSource = null;
        // The split's scope exists before any allocation on its behalf, and
        // owns everything until a page source adopts it.
        HoglakeSplitResources resources = new HoglakeSplitResources(memoryContext);
        ConnectorPageSource pageSource = null;
        try {
            dataSource = createDataSource(inputFile, hoglakeSplit.fileSizeBytes(), options);
            ParsedFooter footer = readFooter(dataSource, hoglakeSplit, options);
            // Every range of a file with a deletion vector loads the whole vector:
            // its positions are file row ordinals, and the reader numbers a range's
            // rows from the file's first row, so the vector applies unchanged.
            HoglakeDeletionVector deletionVector = loadDeletionVector(fileSystem, footer.fileRowCount(), hoglakeSplit, resources);
            pageSource = createParquetPageSource(
                    dataSource,
                    footer.metadata(),
                    deletionVector,
                    hoglakeSplit,
                    hoglakeColumns,
                    predicate.simplify(DOMAIN_COMPACTION_THRESHOLD),
                    resources,
                    options);
            // The page source adopts the owner. Its aggregate already reports
            // allocations directly to the engine, including lazy reader loads.
            return pageSource;
        }
        catch (Exception e) {
            // Nothing was returned, so the construction scope releases what it
            // acquired: the split's scope when no page source adopted it, or
            // the page source that did.
            if (pageSource != null) {
                closeOnFailure(pageSource, e);
            }
            else {
                closeOnFailure(resources, e);
            }
            if (dataSource != null) {
                closeOnFailure(dataSource, e);
            }
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed to open parquet file: " + hoglakeSplit.path(),
                    e);
        }
    }

    /**
     * Releases a resource while a failure is propagating, without letting a
     * release failure replace the failure that caused it.
     */
    private static void closeOnFailure(AutoCloseable closeable, Exception primary)
    {
        try {
            closeable.close();
        }
        catch (Exception | Error closeException) {
            // A failure while releasing must not replace the failure that
            // caused it, nor stop the remaining resources from being released.
            if (primary != closeException) {
                primary.addSuppressed(closeException);
            }
        }
    }

    /**
     * The catalog metadata path: a file's visible row count is its physical
     * record count minus the rows its deletion vector removes. The vector is
     * still read and validated first, so a vector that is missing, corrupt,
     * or disagrees with the catalog's {@code delete_count} fails the count
     * instead of being mistaken for no deletes at all.
     *
     * <p>This path keeps nothing: the loader charges the query while it reads
     * and decodes and releases that memory before returning, so a count
     * leaves no reservation behind.
     */
    private ConnectorPageSource createCountPageSource(ConnectorSession session, HoglakeSplit split, MemoryContext memoryContext)
    {
        long visibleRows = split.recordCount();
        if (split.deleteFilePath().isPresent()) {
            // A count keeps nothing, so its split scope is released as soon as
            // the cardinality has been read. The owner itself is the protected
            // resource: a load that reserves and then throws must still release,
            // and a resource that never finished initializing would not.
            try (HoglakeSplitResources resources = new HoglakeSplitResources(memoryContext)) {
                HoglakeDeletionVector vector = HoglakeDeletionVectorLoader.load(
                        fileSystemFactory.create(session), split, split.recordCount(), resources);
                visibleRows -= vector.cardinality();
            }
        }
        return new HoglakeCountPageSource(visibleRows);
    }

    /**
     * The split's parsed footer. Every range of a file needs the whole footer,
     * so the ranges a worker reads share one parse through the footer cache;
     * a hit reads nothing from the file. A miss fetches the footer in a single
     * tail request sized from the catalog's {@code footer_size}.
     */
    private ParsedFooter readFooter(ParquetDataSource dataSource, HoglakeSplit split, ParquetReaderOptions options)
            throws IOException
    {
        // The decoded footer is shared read-only between splits, which is safe
        // only because no decryption properties are passed: the metadata then
        // holds no per-reader decryption state. If file decryption is ever
        // added, encrypted files must bypass this cache.
        return footerCache.get(new HoglakeParquetFooterCache.Key(split.path(), split.fileSizeBytes()), () -> {
            Slice footerBytes = MetadataReader.readFooterBytes(dataSource, options);
            ParquetMetadata metadata = MetadataReader.parseFooter(dataSource.getId(), dataSource.getEstimatedSize(), footerBytes, Optional.empty(), Optional.empty());
            return ParsedFooter.of(metadata, footerBytes.length());
        });
    }

    /**
     * Reads a split's deletion vector through the connector's filesystem, so
     * object-storage configuration, authentication, and filesystem caching
     * apply to deletion vectors exactly as they do to Parquet data. The
     * bytes and the decode are charged to the query's memory context, since
     * a large vector is held before any page source exists to report it.
     */
    private static HoglakeDeletionVector loadDeletionVector(
            TrinoFileSystem fileSystem,
            long fileRowCount,
            HoglakeSplit split,
            HoglakeSplitResources resources)
            throws IOException
    {
        if (split.deleteFilePath().isEmpty()) {
            return null;
        }
        // Deleted positions are file row ordinals, so bound them with the
        // file's real row count from its own footer rather than the
        // catalog's record_count, which a wrong vector would be measured
        // against.
        return HoglakeDeletionVectorLoader.load(fileSystem, split, fileRowCount, resources);
    }

    /**
     * Reader options for one split. The catalog's footer size lets the footer
     * be fetched in one request. A small file is normally buffered whole on
     * first read, which serves its only split well; a file cut into ranges
     * would be buffered whole once per range, so ranges read only their own
     * bytes. (Ranges exist only for files larger than the target split size,
     * so with the defaults this never applies.)
     */
    static ParquetReaderOptions readerOptions(ParquetReaderOptions options, HoglakeSplit split)
    {
        options = withCatalogFooterSize(options, split.footerSize(), split.fileSizeBytes());
        if (!split.wholeFile()) {
            options = ParquetReaderOptions.builder(options)
                    .withSmallFileThreshold(DataSize.ofBytes(0))
                    .build();
        }
        return options;
    }

    /**
     * Sizes the first footer read from the catalog's {@code footer_size}, so
     * the footer and its trailer arrive in a single request instead of after a
     * guess that may be too small.
     */
    static ParquetReaderOptions withCatalogFooterSize(ParquetReaderOptions options, OptionalLong footerSize, long fileSizeBytes)
    {
        if (footerSize.isEmpty()) {
            return options;
        }
        long footerLength = footerSize.orElseThrow();
        if (footerLength < 0 || footerLength + PARQUET_TRAILER_SIZE > fileSizeBytes) {
            // The catalog disagrees with the file, so let the reader find the footer on its own.
            return options;
        }
        // Reading beyond the configured maximum is wasted, because a footer that long is rejected.
        long footerReadSize = min(footerLength + PARQUET_TRAILER_SIZE, options.getMaxFooterReadSize().toBytes());
        return ParquetReaderOptions.builder(options)
                .withFooterReadSize(DataSize.ofBytes(footerReadSize))
                .build();
    }

    // Package-private to inject failures at the real Parquet planRead boundary.
    ParquetDataSource createDataSource(TrinoInputFile inputFile, long fileSize, ParquetReaderOptions options)
            throws IOException
    {
        return new HoglakeParquetDataSource(inputFile, fileSize, options);
    }

    private static ConnectorPageSource createParquetPageSource(
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            HoglakeDeletionVector deletionVector,
            HoglakeSplit split,
            List<HoglakeColumnHandle> columns,
            TupleDomain<HoglakeColumnHandle> predicate,
            HoglakeSplitResources resources,
            ParquetReaderOptions options)
            throws IOException
    {
        FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetadata.getSchema();

        // Bind each requested catalog column to a file column.
        List<Optional<org.apache.parquet.schema.Type>> bindings = columns.stream()
                .map(column -> bindColumn(fileSchema, column))
                .toList();

        // The projected file schema contains only the bound fields.
        List<org.apache.parquet.schema.Type> boundFields = bindings.stream()
                .flatMap(Optional::stream)
                .toList();
        MessageType requestedSchema = new MessageType(fileSchema.getName(), boundFields);
        MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

        // Reader columns + page adaptations (nulls for unbound columns).
        List<Column> parquetColumns = new ArrayList<>();
        List<HoglakePageSource.ColumnAdaptation> adaptations = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            HoglakeColumnHandle column = columns.get(i);
            if (column.equals(HoglakeColumnHandle.ROW_ID)) {
                adaptations.add(new HoglakePageSource.RowIdColumn(split.dataFileId()));
                continue;
            }
            Optional<Field> field = bindings.get(i).flatMap(parquetField ->
                    HoglakeParquetFields.construct(column, lookupColumnByName(messageColumn, parquetField.getName())));
            if (field.isPresent()) {
                adaptations.add(HoglakeUnsigned.needsConversion(column)
                        ? new HoglakePageSource.UnsignedColumn(parquetColumns.size(), column)
                        : new HoglakePageSource.SourceColumn(parquetColumns.size()));
                parquetColumns.add(new Column(column.name(), field.get()));
            }
            else {
                adaptations.add(new HoglakePageSource.NullColumn(column.type()));
            }
        }

        // Predicate columns need footer metadata even when they are not projected. Use the
        // same field-id/name binding as the reader, including renamed and missing columns.
        MessageType predicateSchema = new MessageType(fileSchema.getName(), Stream.concat(
                        boundFields.stream(),
                        predicate.getDomains().orElseThrow().keySet().stream()
                                .flatMap(column -> bindColumn(fileSchema, column).stream()))
                .distinct()
                .toList());
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, predicateSchema);
        TupleDomain<ColumnDescriptor> parquetDomain = parquetPredicate(fileSchema, descriptorsByPath, predicate);
        List<RowGroupInfo> rowGroups;
        try {
            rowGroups = filterRowGroups(split, dataSource, parquetMetadata, parquetDomain, descriptorsByPath, options);
        }
        catch (ParquetCorruptionException e) {
            // Unusable statistics must not exclude data. Retry without pruning; structural
            // corruption will still fail when constructing metadata or reading the data.
            rowGroups = filterRowGroups(split, dataSource, parquetMetadata, TupleDomain.all(), descriptorsByPath, options);
        }

        // The reader charges into the split's one aggregation, alongside the
        // deletion vector's bitmap, so there is a single total to report.
        AggregatedMemoryContext allocation = resources.allocation();
        // The reader appends the file row position of every row as an extra
        // channel when a deletion vector must be applied. That position is
        // the row's original, file-relative ordinal — row-group offsets
        // included — which is exactly the numbering a deletion vector uses,
        // so pruned row groups and multi-page reads cannot shift it.
        ParquetReader parquetReader = new ParquetReader(
                Optional.ofNullable(fileMetadata.getCreatedBy()),
                parquetColumns,
                deletionVector != null || columns.contains(HoglakeColumnHandle.ROW_ID),
                rowGroups,
                dataSource,
                DateTimeZone.UTC,
                allocation,
                options,
                exception -> HoglakePageSource.handleException(dataSource.getId(), exception),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new HoglakePageSource(parquetReader, adaptations, deletionVector, resources);
    }

    /**
     * The row groups whose first column chunk starts inside the split's byte
     * range, minus those the predicate prunes. Each kept row group carries
     * its offset among all the file's rows, not just the range's, so row
     * positions stay file-absolute for deletion vectors and row ids.
     */
    private static List<RowGroupInfo> filterRowGroups(
            HoglakeSplit split,
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            TupleDomain<ColumnDescriptor> parquetDomain,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            ParquetReaderOptions options)
            throws IOException
    {
        TupleDomainParquetPredicate parquetPredicate =
                buildPredicate(parquetMetadata.getFileMetaData().getSchema(), parquetDomain, descriptorsByPath, DateTimeZone.UTC);
        return getFilteredRowGroups(
                split.start(),
                split.length(),
                dataSource,
                parquetMetadata,
                List.of(parquetDomain),
                List.of(parquetPredicate),
                descriptorsByPath,
                DateTimeZone.UTC,
                DOMAIN_COMPACTION_THRESHOLD,
                options);
    }

    static TupleDomain<ColumnDescriptor> parquetPredicate(
            MessageType fileSchema,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<HoglakeColumnHandle> predicate)
    {
        if (predicate.isNone()) {
            return TupleDomain.none();
        }
        Map<ColumnDescriptor, Domain> domains = new HashMap<>();
        for (Map.Entry<HoglakeColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
            HoglakeColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            Optional<org.apache.parquet.schema.Type> binding = bindColumn(fileSchema, column);
            if (binding.isEmpty()) {
                if (!domain.isNullAllowed()) {
                    return TupleDomain.none();
                }
                continue;
            }
            // UUID ordering differs from Parquet's binary ordering. Nested fields have no
            // statistics for the whole value. Unsupported statistics types remain residuals.
            if (!binding.get().isPrimitive() || column.type().equals(UUID) || column.hoglakeType().startsWith("uint")) {
                continue;
            }
            // Bloom filters hash the physical value width. A widened SQL domain would
            // probe INT32 as a long (or FLOAT as a double) and can lose matching rows.
            var physicalType = binding.get().asPrimitiveType().getPrimitiveTypeName();
            if ((column.type().equals(BIGINT) && physicalType == INT32) ||
                    (column.type().equals(DOUBLE) && physicalType == FLOAT)) {
                continue;
            }
            if (column.type() instanceof TimestampWithTimeZoneType) {
                // Match the exact microsecond decoder semantics. Other encodings and
                // precisions retain value predicates as residuals, but can still prune nulls.
                if (column.type().equals(TIMESTAMP_TZ_MICROS) && physicalType == INT64 &&
                        binding.get().asPrimitiveType().getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation annotation &&
                        annotation.isAdjustedToUTC() && annotation.getUnit() == MICROS) {
                    try {
                        domain = utcTimestampDomain(domain);
                    }
                    catch (ArithmeticException e) {
                        // A bound outside the plain timestamp representation cannot be pushed.
                        continue;
                    }
                }
                else if (!domain.getValues().isAll() && !domain.getValues().isNone()) {
                    continue;
                }
            }
            ColumnDescriptor descriptor = descriptorsByPath.get(List.of(binding.get().getName()));
            if (descriptor != null) {
                domains.merge(descriptor, domain, Domain::intersect);
            }
        }
        return TupleDomain.withColumnDomains(domains);
    }

    private static Domain utcTimestampDomain(Domain domain)
    {
        List<Range> ranges = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            Range converted = Range.all(TIMESTAMP_MICROS);
            if (!range.isLowUnbounded()) {
                long low = epochMicros((LongTimestampWithTimeZone) range.getLowBoundedValue());
                converted = converted.intersect(range.isLowInclusive()
                        ? Range.greaterThanOrEqual(TIMESTAMP_MICROS, low)
                        : Range.greaterThan(TIMESTAMP_MICROS, low)).orElseThrow();
            }
            if (!range.isHighUnbounded()) {
                long high = epochMicros((LongTimestampWithTimeZone) range.getHighBoundedValue());
                converted = converted.intersect(range.isHighInclusive()
                        ? Range.lessThanOrEqual(TIMESTAMP_MICROS, high)
                        : Range.lessThan(TIMESTAMP_MICROS, high)).orElseThrow();
            }
            ranges.add(converted);
        }
        return Domain.create(SortedRangeSet.copyOf(TIMESTAMP_MICROS, ranges), domain.isNullAllowed());
    }

    private static long epochMicros(LongTimestampWithTimeZone value)
    {
        // Zone keys describe presentation; the stored epoch already identifies the instant.
        if (value.getPicosOfMilli() % 1_000_000 != 0) {
            throw new ArithmeticException("Timestamp bound is not an exact microsecond");
        }
        return Math.addExact(Math.multiplyExact(value.getEpochMillis(), 1_000), value.getPicosOfMilli() / 1_000_000);
    }

    /**
     * Field-id binding with name fallback: a file field carrying the
     * catalog column's field_id wins; otherwise the column binds to a
     * same-named field (exact match first, then case-insensitive).
     */
    static Optional<org.apache.parquet.schema.Type> bindColumn(MessageType fileSchema, HoglakeColumnHandle column)
    {
        if (column.equals(HoglakeColumnHandle.ROW_ID)) {
            return Optional.empty();
        }
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (field.getId() != null && field.getId().intValue() == column.fieldId()) {
                return Optional.of(field);
            }
        }
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (field.getId() == null && field.getName().equals(column.name())) {
                return Optional.of(field);
            }
        }
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (field.getId() == null && field.getName().equalsIgnoreCase(column.name())) {
                return Optional.of(field);
            }
        }
        return Optional.empty();
    }
}
