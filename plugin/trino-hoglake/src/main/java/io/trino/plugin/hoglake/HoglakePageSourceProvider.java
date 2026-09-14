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

import io.trino.filesystem.Location;
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
import io.trino.spi.connector.MemoryUsageReportingPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.UuidType.UUID;
import static java.util.Objects.requireNonNull;

/**
 * Reads a split's parquet file from object storage through Trino's own
 * parquet reader (trino-parquet) and S3 filesystem (trino-filesystem-s3)
 * — the same data path the bundled hive/iceberg connectors use.
 *
 * Column binding: catalog columns bind to file columns by embedded
 * PARQUET:field_id when the file carries ids; files written without ids
 * bind by name (exact, then case-insensitive). Catalog columns absent
 * from the file (added after the file was written) read as nulls.
 */
public class HoglakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final int DOMAIN_COMPACTION_THRESHOLD = 100;

    private final TrinoFileSystemFactory fileSystemFactory;

    public HoglakePageSourceProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
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
        ensureNoRowLevelDeletes(hoglakeSplit);

        TupleDomain<HoglakeColumnHandle> predicate = ((HoglakeTableHandle) table).constraint();
        if (predicate.isNone()) {
            return new EmptyPageSource();
        }

        // Splits cover whole files at the query's pinned snapshot. With no columns or
        // reader-side predicate, only row cardinality is needed (for example, COUNT(*)).
        if (columns.isEmpty() && predicate.isAll() && hoglakeSplit.recordCount() >= 0) {
            return new HoglakeCountPageSource(hoglakeSplit.recordCount());
        }

        List<HoglakeColumnHandle> hoglakeColumns = columns.stream()
                .map(HoglakeColumnHandle.class::cast)
                .toList();

        TrinoInputFile inputFile = fileSystemFactory.create(session)
                .newInputFile(Location.of(hoglakeSplit.path()), hoglakeSplit.fileSizeBytes());

        ParquetReaderOptions options = ParquetReaderOptions.defaultOptions();
        ParquetDataSource dataSource = null;
        try {
            dataSource = new HoglakeParquetDataSource(inputFile, hoglakeSplit.fileSizeBytes(), options);
            return new MemoryUsageReportingPageSource(createParquetPageSource(dataSource, hoglakeColumns, predicate.simplify(DOMAIN_COMPACTION_THRESHOLD), options), memoryContext);
        }
        catch (Exception e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException closeException) {
                    if (e != closeException) {
                        e.addSuppressed(closeException);
                    }
                }
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
     * Defense-in-depth: the authoritative DV refusal fires at split
     * generation ({@link HoglakeSplitManager#ensureNoRowLevelDeletes}),
     * before any split reaches the engine. This worker-side guard backs
     * it up in case a DV-carrying split ever arrives anyway — silently
     * returning deleted rows is not an option.
     */
    static void ensureNoRowLevelDeletes(HoglakeSplit split)
    {
        if (split.deleteFilePath().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED,
                    "table has row-level deletes; DV application not yet implemented in the hoglake connector"
                            + " (data file " + split.path() + " has deletion vector "
                            + split.deleteFilePath().get() + " covering " + split.deleteCount() + " rows)");
        }
    }

    private static ConnectorPageSource createParquetPageSource(
            ParquetDataSource dataSource,
            List<HoglakeColumnHandle> columns,
            TupleDomain<HoglakeColumnHandle> predicate,
            ParquetReaderOptions options)
            throws IOException
    {
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
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
            Optional<Field> field = bindings.get(i).flatMap(parquetField ->
                    constructField(column.type(), lookupColumnByName(messageColumn, parquetField.getName())));
            if (field.isPresent()) {
                adaptations.add(new HoglakePageSource.SourceColumn(parquetColumns.size()));
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
            rowGroups = filterRowGroups(dataSource, parquetMetadata, parquetDomain, descriptorsByPath, options);
        }
        catch (ParquetCorruptionException e) {
            // Unusable statistics must not exclude data. Retry without pruning; structural
            // corruption will still fail when constructing metadata or reading the data.
            rowGroups = filterRowGroups(dataSource, parquetMetadata, TupleDomain.all(), descriptorsByPath, options);
        }

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader parquetReader = new ParquetReader(
                Optional.ofNullable(fileMetadata.getCreatedBy()),
                parquetColumns,
                false,
                rowGroups,
                dataSource,
                DateTimeZone.UTC,
                memoryContext,
                options,
                exception -> HoglakePageSource.handleException(dataSource.getId(), exception),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new HoglakePageSource(parquetReader, adaptations);
    }

    private static List<RowGroupInfo> filterRowGroups(
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
                0,
                dataSource.getEstimatedSize(),
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
            if (!binding.get().isPrimitive() || column.type().equals(UUID)) {
                continue;
            }
            ColumnDescriptor descriptor = descriptorsByPath.get(List.of(binding.get().getName()));
            if (descriptor != null) {
                domains.merge(descriptor, domain, Domain::intersect);
            }
        }
        return TupleDomain.withColumnDomains(domains);
    }

    /**
     * Field-id binding with name fallback: a file field carrying the
     * catalog column's field_id wins; otherwise the column binds to a
     * same-named field (exact match first, then case-insensitive).
     */
    static Optional<org.apache.parquet.schema.Type> bindColumn(MessageType fileSchema, HoglakeColumnHandle column)
    {
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
