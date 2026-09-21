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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_INVALID_RESPONSE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static java.util.stream.Collectors.toMap;

/**
 * Coordinator-only: combine worker sets, preserve old deletes, publish once.
 */
final class HoglakeDeletePublisher
{
    private final HoglakeClient client;
    private final TrinoFileSystem fileSystem;

    HoglakeDeletePublisher(HoglakeClient client, TrinoFileSystem fileSystem)
    {
        this.client = client;
        this.fileSystem = fileSystem;
    }

    private static void checkCancelled()
    {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(USER_CANCELED, "Hoglake DELETE cancelled before publication");
        }
    }

    void publish(HoglakeDeleteHandle handle, Collection<Slice> fragments)
    {
        HoglakeTableHandle table = handle.table();
        Map<Long, HoglakeDeleteBitmap> changes = new TreeMap<>();
        Map<Long, HoglakeDtos.ScanFile> files = client.scan(table.schemaName(), table.tableName(), table.snapshotId()).stream()
                .collect(toMap(file -> file.dataFile().dataFileId(), file -> file));
        long fragmentBytes = 0;
        for (Slice fragment : fragments) {
            fragmentBytes = Math.addExact(fragmentBytes, fragment.length());
            HoglakeDeleteBitmap.checkSize(fragmentBytes);
            if (fragment.length() <= Long.BYTES) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "Invalid DELETE fragment");
            }
            long fileId = fragment.getLong(0);
            HoglakeDtos.ScanFile file = files.get(fileId);
            if (file == null) {
                throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "DELETE fragment targets a file outside the pinned snapshot");
            }
            try (HoglakeSplitResources resources = new HoglakeSplitResources(HoglakeDeleteBitmap::checkSize)) {
                HoglakeDeletionVector vector = HoglakeDeletionVector.read(fragment.getBytes(Long.BYTES, fragment.length() - Long.BYTES), "DELETE fragment", resources.allocation());
                if (vector.maximumDeletedPosition().orElse(-1) >= file.dataFile().recordCount()) {
                    throw new TrinoException(HOGLAKE_INVALID_RESPONSE, "DELETE position exceeds file row count");
                }
                vector.unionInto(changes.computeIfAbsent(fileId, _ -> new HoglakeDeleteBitmap()));
                HoglakeDeleteBitmap.checkSize(changes.values().stream().mapToLong(HoglakeDeleteBitmap::retainedBytes).sum());
            }
        }
        List<Location> uploads = new ArrayList<>();
        List<HoglakeDtos.DeleteRegistration> registrations = new ArrayList<>();
        boolean publicationStarted = false;
        try {
            for (var change : changes.entrySet()) {
                checkCancelled();
                HoglakeSplit split = HoglakeSplitManager.toSplit(files.get(change.getKey()));
                HoglakeDeleteBitmap bitmap = change.getValue();
                if (split.deleteFilePath().isPresent()) {
                    try (HoglakeSplitResources resources = new HoglakeSplitResources(HoglakeDeleteBitmap::checkSize)) {
                        HoglakeDeletionVectorLoader.load(fileSystem, split, split.recordCount(), resources).unionInto(bitmap);
                    }
                }
                HoglakeDeleteBitmap.checkSize(changes.values().stream().mapToLong(HoglakeDeleteBitmap::retainedBytes).sum());
                byte[] bytes = bitmap.encode(split.path());
                Location location = Location.of(handle.dataPath()).appendPath("trino-delete/" + UUID.randomUUID() + ".puffin");
                uploads.add(location);
                fileSystem.newOutputFile(location).createOrOverwrite(bytes);
                registrations.add(new HoglakeDtos.DeleteRegistration(change.getKey(), location.toString(), bitmap.cardinality(), bytes.length));
            }
            // Even a zero-row DELETE validates identity and the DDL conflict window.
            // Once submitted, neither cancellation nor missing receipts authorizes cleanup.
            checkCancelled();
            publicationStarted = true;
            client.commit(new HoglakeDtos.Commit(
                    table.snapshotId(),
                    List.of(),
                    List.of(new HoglakeDtos.Deletes(table.schemaName(), table.tableName(), table.tableUuid(), registrations)),
                    handle.operationId()));
        }
        catch (IOException | RuntimeException failure) {
            if (!publicationStarted) {
                for (Location upload : uploads) {
                    try {
                        fileSystem.deleteFile(upload);
                    }
                    catch (IOException cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
            }
            if (failure instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to publish Hoglake DELETE", failure);
        }
    }
}
