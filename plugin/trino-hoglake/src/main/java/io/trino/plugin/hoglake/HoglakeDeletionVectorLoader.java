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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.plugin.hoglake.HoglakeDeletionVector.invalid;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_DELETION_VECTOR_NOT_FOUND;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Loads and validates the deletion vector a scan paired with a data file.
 *
 * <p>The file is read through the connector's {@link TrinoFileSystem}, so
 * object-storage configuration, authentication, and filesystem caching
 * apply to deletion vectors exactly as they do to Parquet data. The bytes
 * are decoded once per split by {@link HoglakeDeletionVector}; callers keep
 * the decoded vector for the life of the split's page source.
 *
 * <p>Validation is deliberately strict and fails the query instead of
 * falling back to "no deleted rows":
 * <ul>
 * <li>the catalog's {@code file_format} must be the supported {@code puffin-dv} (when reported),</li>
 * <li>the file must exist and be within a size bound that keeps an
 *     unchecked catalog length from becoming an unchecked allocation,</li>
 * <li>the puffin container and its {@code deletion-vector-v1} blob must
 *     decode with the format's structural checks and CRC,</li>
 * <li>the catalog's {@code delete_count} must equal the bitmap's cardinality,</li>
 * <li>every deleted position must fall inside the supplied row bound, and</li>
 * <li>a vector that names a different data file than the one it is paired
 *     with is rejected.</li>
 * </ul>
 */
final class HoglakeDeletionVectorLoader
{
    /**
     * The largest serialized vector this connector reads into memory.
     * Decoded allocations are reserved separately against query memory.
     */
    static final long MAX_DELETION_VECTOR_BYTES = 256L * 1024 * 1024;

    private HoglakeDeletionVectorLoader() {}

    /**
     * Reads, decodes, and validates a split's deletion vector.
     *
     * <p>Everything the load allocates is charged through the split's owner:
     * the input before reading, then each inspected bucket before decoding.
     * The owner must be closed on both success and failure.
     *
     * @param rowCountBound physical rows from the Parquet footer for scans,
     *         or the catalog row count for metadata-only counts
     * @param resources the split's accounting and ownership
     */
    static HoglakeDeletionVector load(
            TrinoFileSystem fileSystem,
            HoglakeSplit split,
            long rowCountBound,
            HoglakeSplitResources resources)
    {
        requireNonNull(fileSystem, "fileSystem is null");
        requireNonNull(split, "split is null");
        requireNonNull(resources, "resources is null");
        if (split.deleteFilePath().isEmpty()) {
            throw new IllegalArgumentException("split has no deletion vector");
        }
        String path = split.deleteFilePath().orElseThrow();
        Optional<String> format = split.deleteFileFormat();
        if (format.isPresent() && !HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT.equals(format.get())) {
            throw new TrinoException(
                    HOGLAKE_DELETION_VECTOR_NOT_FOUND,
                    "Unsupported hoglake deletion vector format '%s' for %s (only '%s' is supported)"
                            .formatted(format.get(), path, HoglakeDeletionVector.PUFFIN_DELETION_VECTOR_FORMAT));
        }
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path));
        long length;
        try {
            length = inputFile.length();
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_DELETION_VECTOR_NOT_FOUND, "Failed to read hoglake deletion vector " + path, e);
        }
        if (length < 0 || length > MAX_DELETION_VECTOR_BYTES) {
            throw new TrinoException(
                    HOGLAKE_DELETION_VECTOR_NOT_FOUND,
                    "hoglake deletion vector %s is %d bytes, outside the 0-to-%d-byte limit this connector reads"
                            .formatted(path, length, MAX_DELETION_VECTOR_BYTES));
        }
        LocalMemoryContext inputMemory = resources.allocation().newLocalMemoryContext("hoglake_deletion_vector_input");
        inputMemory.setBytes(sizeOfByteArray(toIntExact(length)));
        HoglakeDeletionVector vector = read(inputFile, toIntExact(length), resources);
        validate(vector, split, rowCountBound);
        // The enclosing owner releases every context on failure. On success,
        // read() has returned, so the raw bytes are no longer held.
        inputMemory.close();
        return vector;
    }

    private static HoglakeDeletionVector read(TrinoInputFile inputFile, int length, HoglakeSplitResources resources)
    {
        // This scope ends before the caller releases the input reservation.
        byte[] bytes = new byte[length];
        try (TrinoInput input = inputFile.newInput()) {
            input.readFully(0, bytes, 0, bytes.length);
        }
        catch (IOException e) {
            throw new TrinoException(HOGLAKE_DELETION_VECTOR_NOT_FOUND, "Failed to read hoglake deletion vector " + inputFile.location(), e);
        }
        return HoglakeDeletionVector.read(bytes, inputFile.location().toString(), resources.allocation());
    }

    private static void validate(HoglakeDeletionVector deletionVector, HoglakeSplit split, long rowCount)
    {
        String path = split.deleteFilePath().orElseThrow();
        if (deletionVector.cardinality() != split.deleteCount()) {
            throw invalid(path, "the catalog reports %d deleted rows but the vector deletes %d"
                    .formatted(split.deleteCount(), deletionVector.cardinality()), null);
        }
        deletionVector.referencedDataFile().ifPresent(referenced -> {
            if (!referenced.equals(split.path())) {
                throw invalid(path, "the vector references data file %s but is paired with %s"
                        .formatted(referenced, split.path()), null);
            }
        });
        // Scans supply the footer's physical row count; metadata counts
        // deliberately trust the catalog's row count.
        deletionVector.maximumDeletedPosition().ifPresent(lastDeleted -> {
            if (lastDeleted >= rowCount) {
                throw invalid(path, "the vector deletes row %d, beyond the %d rows of data file %s"
                        .formatted(lastDeleted, rowCount, split.path()), null);
            }
        });
    }
}
