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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_WRITE_ERROR;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;

/**
 * Writes immutable files; only the coordinator registers them with the catalog.
 */
public class HoglakePageSink
        implements ConnectorPageSink
{
    private static final com.fasterxml.jackson.databind.ObjectReader JSON_READER = new ObjectMapper().reader()
            .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;

    private final TrinoFileSystem fileSystem;
    private final HoglakeWriteHandle handle;
    private final HoglakeParquetSchema schema;
    private final String trinoVersion;
    private final io.trino.spi.PageSorter pageSorter;
    private final List<Page> sortBuffer = new ArrayList<>();
    private long sortBytes;
    private long sortPositions;
    private static final long SORT_BUFFER_BYTES = 32L * 1024 * 1024;
    private final AggregatedMemoryContext memoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
    private final List<Location> locations = new ArrayList<>();
    private final List<Slice> fragments = new ArrayList<>();
    private ParquetWriter writer;
    private List<String> partitionValues = List.of();
    private Location location;
    private long rows;
    private long completedBytes;
    private boolean finished;
    private boolean aborted;
    private final io.trino.plugin.hoglake.rest.HoglakeClient uploadClient;
    private long lastRenewal = System.nanoTime();

    public HoglakePageSink(TrinoFileSystem fileSystem, HoglakeWriteHandle handle, String trinoVersion)
    {
        this(fileSystem, handle, trinoVersion, null);
    }

    public HoglakePageSink(TrinoFileSystem fileSystem, HoglakeWriteHandle handle, String trinoVersion, io.trino.spi.PageSorter pageSorter)
    {
        this(fileSystem, handle, trinoVersion, pageSorter, null);
    }

    public HoglakePageSink(TrinoFileSystem fileSystem, HoglakeWriteHandle handle, String trinoVersion, io.trino.spi.PageSorter pageSorter, io.trino.plugin.hoglake.rest.HoglakeClient client)
    {
        this.uploadClient = handle.claimUploads() ? requireNonNull(client, "client required for upload claims") : null;
        this.pageSorter = handle.sortFields().isEmpty() ? pageSorter : requireNonNull(pageSorter, "pageSorter is required for sorted writes");
        HoglakeSorting.validate(handle.sortFields(), handle.columns());
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.handle = requireNonNull(handle, "handle is null");
        HoglakePartitioning.validate(handle.partitionFields(), handle.columns());
        this.schema = HoglakeParquetSchema.create(handle.columns());
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return 3 * sortBytes + 16 * sortPositions + memoryContext.getBytes() + (writer == null ? 0 : writer.getRetainedBytes()) +
                fragments.stream().mapToLong(fragment -> fragment.getRetainedSize() + 2L * Long.BYTES).sum();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        renewUploads(false);
        if (finished || aborted) {
            throw new IllegalStateException("Sink is finished");
        }
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }
        Block[] blocks = new Block[handle.columns().size()];
        for (int index = 0; index < blocks.length; index++) {
            HoglakeColumnHandle column = handle.columns().get(index);
            int channel = handle.inputColumns().indexOf(column);
            Block block = channel < 0 ? RunLengthEncodedBlock.create(column.type(), null, page.getPositionCount()) : page.getBlock(channel);
            for (int position = 0; position < page.getPositionCount(); position++) {
                validateValue(column, block, position);
            }
            blocks[index] = block;
        }
        Page logical = new Page(page.getPositionCount(), blocks);
        try {
            if (handle.partitionFields().isEmpty()) {
                acceptPage(logical, List.of());
            }
            else {
                // Group only this input page and keep one writer open. Memory does not
                // grow with table partition cardinality; interleaved keys may make small files.
                Map<List<String>, List<Integer>> groups = new java.util.LinkedHashMap<>();
                for (int position = 0; position < logical.getPositionCount(); position++) {
                    var values = HoglakePartitioning.values(handle.partitionFields(), handle.columns(), logical, position);
                    groups.computeIfAbsent(values, _ -> new ArrayList<>()).add(position);
                }
                for (var group : groups.entrySet()) {
                    int[] positions = group.getValue().stream().mapToInt(Integer::intValue).toArray();
                    acceptPage(logical.getPositions(positions, 0, positions.length), group.getKey());
                }
            }
            return NOT_BLOCKED;
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_WRITE_ERROR, "Failed to write Hoglake Parquet file", e);
        }
    }

    private void acceptPage(Page page, List<String> values)
            throws IOException
    {
        if (handle.sortFields().isEmpty()) {
            writePage(page, values);
            return;
        }
        if (!partitionValues.equals(values)) {
            flushSorted();
            partitionValues = values;
        }
        // Copy a routed page so a tiny partition does not retain unrelated input rows.
        Page retained = page.getRegion(0, page.getPositionCount());
        retained.compact();
        sortBuffer.add(retained);
        sortBytes += retained.getRetainedSizeInBytes();
        sortPositions += retained.getPositionCount();
        if (3 * sortBytes + 16 * sortPositions >= SORT_BUFFER_BYTES) {
            flushSorted();
        }
    }

    private void flushSorted()
            throws IOException
    {
        if (sortBuffer.isEmpty()) {
            return;
        }
        var sorted = HoglakeSorting.sort(pageSorter, sortBuffer, handle.columns(), handle.sortFields());
        int[] channels = java.util.stream.IntStream.range(0, handle.columns().size()).toArray();
        while (sorted.hasNext()) {
            writePage(sorted.next().getColumns(channels), partitionValues);
        }
        // Each run owns complete files. Appending the next sorted run to this
        // file would destroy the declared ordering between the two runs.
        closeFile();
        sortBuffer.clear();
        sortBytes = 0;
        sortPositions = 0;
    }

    private void writePage(Page page, List<String> values)
            throws IOException
    {
        if (!partitionValues.equals(values)) {
            closeFile();
            partitionValues = values;
        }
        if (writer == null) {
            String dataPath = handle.dataPath();
            // Location requires a slash after the authority, even for a bucket root.
            location = Location.of(dataPath.endsWith("/") ? dataPath : dataPath + "/")
                    .appendPath("data/" + UUID.randomUUID() + ".parquet");
            if (uploadClient != null) {
                location = Location.of(uploadClient.claimUpload(uploadOwner(), handle.dataPath(), "data"));
            }
            locations.add(location);
            writer = new ParquetWriter(
                    fileSystem.newOutputFile(location).create(memoryContext),
                    schema.messageType(),
                    schema.primitiveTypes(),
                    ParquetWriterOptions.builder().build(),
                    SNAPPY,
                    trinoVersion,
                    Optional.empty(),
                    Optional.empty());
        }
        Block[] physical = new Block[handle.columns().size()];
        for (int index = 0; index < physical.length; index++) {
            physical[index] = HoglakeUnsigned.convert(handle.columns().get(index), page.getBlock(index), true);
        }
        writer.write(new Page(page.getPositionCount(), physical));
        rows += page.getPositionCount();
        if (writer.getEstimatedWrittenBytes() >= TARGET_FILE_SIZE) {
            closeFile();
        }
    }

    private static void validateValue(HoglakeColumnHandle column, Block block, int position)
    {
        if (block.isNull(position)) {
            if (!column.nullable()) {
                throw new TrinoException(CONSTRAINT_VIOLATION, "NULL value for required column: " + column.name());
            }
            return;
        }
        if (column.hoglakeType().startsWith("uint")) {
            BigInteger value = column.hoglakeType().equals("uint64")
                    ? ((io.trino.spi.type.Int128) column.type().getObject(block, position)).toBigInteger()
                    : BigInteger.valueOf(column.type().getLong(block, position));
            int bits = Integer.parseInt(column.hoglakeType().substring(4));
            if (value.signum() < 0 || value.bitLength() > bits) {
                throw new TrinoException(CONSTRAINT_VIOLATION, "Value outside " + column.hoglakeType() + " range: " + column.name());
            }
        }
        if (column.hoglakeType().equals("json")) {
            try {
                var json = JSON_READER.readTree(column.type().getSlice(block, position).toStringUtf8());
                if (json == null || json.isMissingNode()) {
                    throw new TrinoException(CONSTRAINT_VIOLATION, "Invalid JSON value: " + column.name());
                }
            }
            catch (IOException e) {
                throw new TrinoException(CONSTRAINT_VIOLATION, "Invalid JSON value: " + column.name(), e);
            }
        }
        if (column.hoglakeType().equals("timestamp_s") || column.hoglakeType().equals("timestamp_ms")) {
            long unit = column.hoglakeType().equals("timestamp_s") ? 1_000_000 : 1_000;
            if (column.type().getLong(block, position) % unit != 0) {
                throw new TrinoException(CONSTRAINT_VIOLATION, "Timestamp precision exceeds " + column.hoglakeType() + ": " + column.name());
            }
        }
        if (column.type() instanceof ArrayType array) {
            Block elements = array.getObject(block, position);
            for (int index = 0; index < elements.getPositionCount(); index++) {
                validateValue(column.children().getFirst(), elements, index);
            }
        }
        else if (column.type() instanceof MapType map) {
            var value = map.getObject(block, position);
            for (int index = 0; index < value.getSize(); index++) {
                validateValue(column.children().get(0), value.getRawKeyBlock(), value.getRawOffset() + index);
                validateValue(column.children().get(1), value.getRawValueBlock(), value.getRawOffset() + index);
            }
        }
        else if (column.type() instanceof RowType row) {
            var value = row.getObject(block, position);
            for (int index = 0; index < column.children().size(); index++) {
                validateValue(column.children().get(index), value.getRawFieldBlock(index), value.getRawIndex());
            }
        }
        else if (column.type().equals(TimestampType.TIMESTAMP_NANOS)) {
            LongTimestamp value = (LongTimestamp) column.type().getObject(block, position);
            BigInteger nanos = BigInteger.valueOf(value.getEpochMicros()).multiply(BigInteger.valueOf(1000))
                    .add(BigInteger.valueOf(value.getPicosOfMicro() / 1000));
            if (nanos.bitLength() > 63 || value.getPicosOfMicro() % 1000 != 0) {
                throw new TrinoException(CONSTRAINT_VIOLATION, "Timestamp cannot be represented losslessly as int64 nanoseconds: " + column.name());
            }
        }
    }

    private void closeFile()
            throws IOException
    {
        if (writer == null) {
            return;
        }
        writer.close();
        long size = fileSystem.newInputFile(location).length();
        HoglakeDtos.FileRegistration file = new HoglakeDtos.FileRegistration(location.toString(), rows, size, writer.getFooterSize(), partitionValues);
        fragments.add(wrappedBuffer(new ObjectMapper().writeValueAsBytes(file)));
        completedBytes += size;
        writer = null;
        rows = 0;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (aborted) {
            throw new IllegalStateException("Sink is aborted");
        }
        try {
            renewUploads(true);
            flushSorted();
            closeFile();
            finished = true;
            memoryContext.close();
            List<Slice> result = List.copyOf(fragments);
            fragments.clear();
            return completedFuture(result);
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_WRITE_ERROR, "Failed to finish Hoglake Parquet file", e);
        }
    }

    private String uploadOwner()
    {
        return handle.creationOperation().or(() -> handle.insertOperation()).orElseThrow();
    }

    private void renewUploads(boolean force)
    {
        if (uploadClient != null && (force || System.nanoTime() - lastRenewal > java.util.concurrent.TimeUnit.MINUTES.toNanos(5))) {
            uploadClient.renewUploads(uploadOwner());
            lastRenewal = System.nanoTime();
        }
    }

    @Override
    public void abort()
    {
        // After fragments have been handed off, a catalog timeout can mean a successful
        // commit. Never delete those files: orphan cleanup belongs to the catalog operator.
        if (finished || aborted) {
            return;
        }
        aborted = true;
        sortBuffer.clear();
        sortBytes = 0;
        sortPositions = 0;
        try {
            if (writer != null) {
                writer.close();
            }
        }
        catch (IOException e) {
            // Still attempt to remove every file opened by this sink.
        }
        finally {
            writer = null;
            fragments.clear();
            memoryContext.close();
        }
        try {
            if (uploadClient != null) {
                uploadClient.abandonUploads(uploadOwner(), locations.stream().map(Location::toString).toList());
            }
            else {
                fileSystem.deleteFiles(locations);
            }
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_WRITE_ERROR, "Failed to clean up aborted Hoglake write", e);
        }
    }
}
