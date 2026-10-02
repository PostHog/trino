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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheFileSystem;
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
import io.trino.parquet.variant.ParquetOriginalFieldNames;
import io.trino.parquet.variant.ShreddedVariantAssembler;
import io.trino.parquet.variant.VariantShreddingSchema;
import io.trino.plugin.hoglake.HoglakeParquetFooterCache.Lookup;
import io.trino.plugin.hoglake.HoglakeParquetFooterCache.ParsedFooter;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.CacheKey;
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
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;
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
    private final Optional<CacheKey> catalogCacheKey;
    private final boolean s3SecurityMappingEnabled;
    private final ParquetReaderOptions readerOptions;

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        this(fileSystemFactory, HoglakeParquetFooterCache.disabled());
    }

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory, HoglakeParquetFooterCache footerCache)
    {
        this(fileSystemFactory, footerCache, Optional.empty(), false);
    }

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory, HoglakeParquetFooterCache footerCache, Optional<CacheKey> catalogCacheKey, boolean s3SecurityMappingEnabled)
    {
        this(fileSystemFactory, footerCache, catalogCacheKey, s3SecurityMappingEnabled, ParquetReaderOptions.defaultOptions());
    }

    @VisibleForTesting
    HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory, ParquetReaderOptions readerOptions)
    {
        this(fileSystemFactory, HoglakeParquetFooterCache.disabled(), Optional.empty(), false, readerOptions);
    }

    private HoglakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            HoglakeParquetFooterCache footerCache,
            Optional<CacheKey> catalogCacheKey,
            boolean s3SecurityMappingEnabled,
            ParquetReaderOptions readerOptions)
    {
        this.readerOptions = requireNonNull(readerOptions, "readerOptions is null");
        this.catalogCacheKey = requireNonNull(catalogCacheKey, "catalogCacheKey is null");
        this.s3SecurityMappingEnabled = s3SecurityMappingEnabled;
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
        // example, COUNT(*)). The catalog's record count describes the whole file at the
        // query's pinned snapshot, so the range starting at offset 0 reports it, less the
        // rows its validated deletion vector removes, exactly as a whole-file split does,
        // and the file's other ranges report no rows. Split planning covers every file
        // from offset 0, so each file is counted once; its deletion vector is read and
        // validated once per query rather than once per range, and no range reads the
        // Parquet footer or any data.
        if (columns.isEmpty() && predicate.isAll() && hoglakeSplit.recordCount() >= 0) {
            if (hoglakeSplit.start() == 0) {
                return createCountPageSource(session, hoglakeSplit, memoryContext);
            }
            return new EmptyPageSource();
        }

        List<HoglakeColumnHandle> hoglakeColumns = columns.stream()
                .map(HoglakeColumnHandle.class::cast)
                .toList();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile;
        boolean validateFileAccess = false;
        if (fileSystem instanceof CacheFileSystem cacheFileSystem && catalogCacheKey.isPresent() && hoglakeSplit.dataFileId() > 0) {
            // Registered data files are immutable. Re-registration gets a new ID,
            // so the catalog supplies the version without an S3 modification-time lookup.
            CacheKey version = catalogCacheKey.orElseThrow()
                    .append(Long.toString(hoglakeSplit.dataFileId()))
                    .append(Long.toString(hoglakeSplit.fileSizeBytes()));
            inputFile = cacheFileSystem.newInputFile(Location.of(hoglakeSplit.path()), hoglakeSplit.fileSizeBytes(), version);
            validateFileAccess = s3SecurityMappingEnabled || !session.getIdentity().getExtraCredentials().isEmpty();
        }
        else {
            inputFile = fileSystem.newInputFile(Location.of(hoglakeSplit.path()), hoglakeSplit.fileSizeBytes());
        }

        ParquetReaderOptions options = readerOptions(readerOptions, hoglakeSplit);
        ParquetDataSource dataSource = null;
        // The split's scope exists before any allocation on its behalf, and
        // owns everything until a page source adopts it.
        HoglakeSplitResources resources = new HoglakeSplitResources(memoryContext);
        ConnectorPageSource pageSource = null;
        try {
            if (validateFileAccess) {
                // A shared cache hit must not bypass storage authorization for the current
                // identity. Keep the metadata request when credentials can vary by session,
                // while retaining the registered identity for byte and footer cache entries.
                inputFile.lastModified();
            }
            dataSource = createDataSource(inputFile, hoglakeSplit.fileSizeBytes(), options);
            Lookup footerLookup = readFooter(dataSource, hoglakeSplit, options);
            ParsedFooter footer = footerLookup.footer();
            // Every range of a file with a deletion vector loads the whole vector:
            // its positions are file row ordinals, and the reader numbers a range's
            // rows from the file's first row, so the vector applies unchanged.
            HoglakeDeletionVector deletionVector = loadDeletionVector(fileSystem, footer.fileRowCount(), hoglakeSplit, resources);
            pageSource = createParquetPageSource(
                    dataSource,
                    footer.metadata(),
                    footerLookup.hit(),
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
     * tail request sized from the catalog's {@code footer_size}. The lookup
     * also says whether this split found the footer already decoded, which
     * the split reports in its metrics.
     */
    private Lookup readFooter(ParquetDataSource dataSource, HoglakeSplit split, ParquetReaderOptions options)
            throws IOException
    {
        // The decoded footer is shared read-only between splits, which is safe
        // only because no decryption properties are passed: the metadata then
        // holds no per-reader decryption state. If file decryption is ever
        // added, encrypted files must bypass this cache.
        return footerCache.get(new HoglakeParquetFooterCache.Key(split.path(), split.fileSizeBytes(), split.dataFileId()), () -> {
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
            boolean footerCacheHit,
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
        checkNoCaseCollision(fileSchema, parquetMetadata, Stream.concat(columns.stream(), predicate.getDomains().orElseThrow().keySet().stream()).toList(), split.path());

        // Parse shredded VARIANT columns first, which rejects shredded object fields that differ
        // only by case. They have the same lowercase name, so the column IO and the reader below
        // cannot tell them apart, and fail with errors that do not name the column.
        List<Optional<VariantShreddingSchema>> shreddings = new ArrayList<>();
        Optional<ParquetOriginalFieldNames> originalNames = Optional.empty();
        for (int i = 0; i < columns.size(); i++) {
            Optional<VariantShreddingSchema> shredding = Optional.empty();
            if (bindings.get(i).isPresent() && HoglakeParquetFields.isShreddedVariant(columns.get(i), bindings.get(i).get())) {
                // Shredded object keys are case-sensitive, and only the Thrift schema keeps their case
                if (originalNames.isEmpty()) {
                    originalNames = Optional.of(ParquetOriginalFieldNames.fromSchema(parquetMetadata.getParquetMetadata().getSchema()));
                }
                shredding = Optional.of(shreddingSchema(columns.get(i), bindings.get(i).get(), fileSchema, originalNames.get(), dataSource, split.path()));
            }
            shreddings.add(shredding);
        }

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
            if (shreddings.get(i).isPresent()) {
                VariantShreddingSchema shredding = shreddings.get(i).get();
                Field field = constructField(shredding.physicalType(), lookupColumnByName(messageColumn, bindings.get(i).get().getName())).orElseThrow();
                adaptations.add(new HoglakePageSource.ShreddedVariantColumn(
                        parquetColumns.size(),
                        new ShreddedVariantAssembler(shredding, dataSource.getId()),
                        writesSqlNullAsVariantNull(fileMetadata.getCreatedBy())));
                parquetColumns.add(new Column(column.name(), field));
                continue;
            }
            Optional<Field> field = bindings.get(i).flatMap(parquetField ->
                    readerField(column, lookupColumnByName(messageColumn, parquetField.getName()), split.path()));
            if (field.isPresent()) {
                if (HoglakeUnsigned.needsConversion(column)) {
                    adaptations.add(new HoglakePageSource.UnsignedColumn(parquetColumns.size(), column));
                }
                else if (column.type().equals(VARIANT) && writesSqlNullAsVariantNull(fileMetadata.getCreatedBy())) {
                    adaptations.add(new HoglakePageSource.VariantNullAsSqlNullColumn(parquetColumns.size()));
                }
                else {
                    adaptations.add(new HoglakePageSource.SourceColumn(parquetColumns.size()));
                }
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
        return new HoglakePageSource(
                parquetReader,
                adaptations,
                deletionVector,
                resources,
                HoglakePageSource.splitMetrics(footerCacheHit, rowGroups.size()),
                options.getMaxReadBlockSize().toBytes());
    }

    /**
     * The layout of a shredded top-level VARIANT column. Like {@link #readerField},
     * an unsupported layout fails naming the column and the data file.
     */
    private static VariantShreddingSchema shreddingSchema(
            HoglakeColumnHandle column,
            org.apache.parquet.schema.Type parquetField,
            MessageType fileSchema,
            ParquetOriginalFieldNames originalNames,
            ParquetDataSource dataSource,
            String path)
    {
        // The binding is one of the file schema's own fields, which have the same order as the Thrift schema
        int index = IntStream.range(0, fileSchema.getFieldCount())
                .filter(field -> fileSchema.getType(field) == parquetField)
                .findFirst()
                .orElseThrow();
        try {
            VariantShreddingSchema shredding = VariantShreddingSchema.fromParquet(parquetField.asGroupType(), originalNames.children().get(index), dataSource.getId());
            // A column of pushed-down subscripts reads only the shredded columns of their paths
            return column.variantPathTree().map(shredding::prune).orElse(shredding);
        }
        catch (TrinoException e) {
            throw new TrinoException(e::getErrorCode, "Cannot read column %s from data file %s: %s".formatted(column.name(), path, e.getRawMessage()), e);
        }
        catch (ParquetCorruptionException e) {
            // Like an unshredded VARIANT group with an unexpected shape
            throw new TrinoException(NOT_SUPPORTED, "Cannot read column %s from data file %s: %s".formatted(column.name(), path, e.getMessage()), e);
        }
    }

    /**
     * DuckDB writes a SQL NULL VARIANT as a variant null, and reads a variant null
     * back as SQL NULL, so a top-level variant null in its files is SQL NULL.
     */
    @VisibleForTesting
    static boolean writesSqlNullAsVariantNull(String createdBy)
    {
        return createdBy != null && createdBy.startsWith("DuckDB");
    }

    /**
     * The reader field for a bound file column. A column whose shape cannot be
     * read fails with its error code, naming the column and the data file:
     * other files of the same table may store that column correctly.
     */
    private static Optional<Field> readerField(HoglakeColumnHandle column, ColumnIO physical, String path)
    {
        try {
            return HoglakeParquetFields.construct(column, physical);
        }
        catch (TrinoException e) {
            throw new TrinoException(e::getErrorCode, "Cannot read column %s from data file %s: %s".formatted(column.name(), path, e.getRawMessage()), e);
        }
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
     * Fails if a column binds to a file column whose name, or the name of one of
     * its fields, differs from another one's only by case. The footer reader
     * lowercases names, so the reader cannot tell such columns or fields apart: it
     * reads the values of the other one, or fails with an error that does not name
     * the column. A VARIANT column checks its shredded fields when it is parsed.
     */
    private static void checkNoCaseCollision(MessageType fileSchema, ParquetMetadata parquetMetadata, List<HoglakeColumnHandle> columns, String path)
    {
        // The names are lowercase, so names that differ only by case are repeated
        if (!hasRepeatedName(fileSchema)) {
            return;
        }
        // Only the Thrift schema keeps the original case of the names
        ParquetOriginalFieldNames originalNames = ParquetOriginalFieldNames.fromSchema(parquetMetadata.getParquetMetadata().getSchema());
        for (HoglakeColumnHandle column : columns) {
            Optional<org.apache.parquet.schema.Type> binding = bindColumn(fileSchema, column);
            if (binding.isEmpty()) {
                continue;
            }
            List<String> collidingNames = originalNames.children().stream()
                    .filter(field -> field.name().toLowerCase(ENGLISH).equals(binding.get().getName()))
                    .map(ParquetOriginalFieldNames::path)
                    .toList();
            if (collidingNames.size() == 1 && !column.type().equals(VARIANT)) {
                int index = IntStream.range(0, fileSchema.getFieldCount())
                        .filter(field -> fileSchema.getType(field) == binding.get())
                        .findFirst()
                        .orElseThrow();
                collidingNames = collidingFieldNames(binding.get(), originalNames.children().get(index));
            }
            if (collidingNames.size() > 1) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot read column %s from data file %s: Names that differ only by case are not supported: %s".formatted(column.name(), path, String.join(", ", collidingNames)));
            }
        }
    }

    private static boolean hasRepeatedName(GroupType group)
    {
        Set<String> names = new HashSet<>();
        return group.getFields().stream().anyMatch(field ->
                !names.add(field.getName()) || (!field.isPrimitive() && hasRepeatedName(field.asGroupType())));
    }

    /**
     * The original paths of the first fields of a group, at any depth, whose
     * names differ only by case, or an empty list.
     */
    private static List<String> collidingFieldNames(org.apache.parquet.schema.Type field, ParquetOriginalFieldNames originalNames)
    {
        if (field.isPrimitive()) {
            return List.of();
        }
        GroupType group = field.asGroupType();
        Set<String> names = new HashSet<>();
        Set<String> repeatedNames = group.getFields().stream()
                .map(org.apache.parquet.schema.Type::getName)
                .filter(name -> !names.add(name))
                .collect(toImmutableSet());
        if (!repeatedNames.isEmpty()) {
            return originalNames.children().stream()
                    .filter(child -> repeatedNames.contains(child.name().toLowerCase(ENGLISH)))
                    .map(ParquetOriginalFieldNames::path)
                    .toList();
        }
        for (int i = 0; i < group.getFieldCount(); i++) {
            List<String> collidingNames = collidingFieldNames(group.getType(i), originalNames.children().get(i));
            if (!collidingNames.isEmpty()) {
                return collidingNames;
            }
        }
        return List.of();
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
