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

import io.trino.plugin.hoglake.rest.HoglakeClient;
import io.trino.plugin.hoglake.rest.HoglakeDtos;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Coordinator-private DML delta. Published only by Connector.commit.
 */
final class HoglakeTransactionState
{
    private final HoglakeClient client;
    private final String operation = UUID.randomUUID().toString();
    private final Map<SchemaTableName, TableChanges> tables = new LinkedHashMap<>();
    private final List<String> uploads = new ArrayList<>();
    private Long snapshot;
    private long nextFileId = -2;
    private long retainedBytes;
    private boolean publicationStarted;

    HoglakeTransactionState(HoglakeClient client)
    {
        this.client = client;
    }

    synchronized long snapshot()
    {
        if (snapshot == null) {
            snapshot = client.getCatalog().headSnapshotId();
        }
        return snapshot;
    }

    String operation()
    {
        return operation;
    }

    void checkWriteSupport(HoglakeDtos.Catalog catalog)
    {
        if (catalog.capabilities() == null || !catalog.capabilities().containsAll(List.of("atomic-dml-transactions-v1", "claimed-uploads-v1"))) {
            throw new TrinoException(NOT_SUPPORTED, "Explicit Hoglake write transactions require atomic-dml-transactions-v1 and claimed-uploads-v1");
        }
        client.renewUploads(operation);
    }

    synchronized HoglakeTableHandle overlay(HoglakeTableHandle handle)
    {
        TableChanges changes = tables.get(handle.schemaTableName());
        if (changes == null) {
            return handle;
        }
        List<HoglakeDtos.ScanFile> files = changes.appends.entrySet().stream().map(entry -> {
            HoglakeDtos.FileRegistration file = entry.getValue();
            return new HoglakeDtos.ScanFile(new HoglakeDtos.DataFile(entry.getKey(), file.path(), "parquet", file.recordCount(), file.fileSizeBytes(), file.footerSize(), 0, "pending", snapshot()), null);
        }).toList();
        List<HoglakeDtos.DeleteFile> deletes = changes.deletes.values().stream()
                .map(file -> new HoglakeDtos.DeleteFile(0, file.dataFileId(), file.path(), file.deleteCount(), file.fileSizeBytes(), snapshot()))
                .toList();
        return new HoglakeTableHandle(handle.schemaName(), handle.tableName(), handle.snapshotId(), handle.tableUuid(), handle.columns(), handle.constraint(), files, deletes);
    }

    synchronized void stage(HoglakeDtos.Commit request)
    {
        if (publicationStarted || request.readSnapshot() != snapshot() || !operation.equals(request.operationId())) {
            throw new IllegalStateException("Invalid transaction staging state");
        }
        for (HoglakeDtos.Append append : request.appends()) {
            TableChanges changes = table(append.namespace(), append.table(), append.expectedTableUuid());
            for (HoglakeDtos.FileRegistration file : append.files()) {
                if (changes.paths.containsKey(file.path())) {
                    continue;
                }
                reserve(file.path(), 512L + file.partitionValues().stream().mapToLong(value -> value == null ? 8 : 2L * value.length()).sum());
                long id = nextFileId--;
                changes.paths.put(file.path(), id);
                changes.appends.put(id, file);
            }
        }
        for (HoglakeDtos.Deletes deletion : request.deletes()) {
            TableChanges changes = table(deletion.namespace(), deletion.table(), deletion.expectedTableUuid());
            for (HoglakeDtos.DeleteRegistration file : deletion.files()) {
                reserve(file.path(), 256);
                changes.deletes.put(file.dataFileId(), file);
            }
        }
    }

    private TableChanges table(String namespace, String name, String uuid)
    {
        SchemaTableName key = new SchemaTableName(namespace, name);
        TableChanges table = tables.get(key);
        if (table == null) {
            retainedBytes = Math.addExact(retainedBytes, 512L + 2L * (namespace.length() + name.length() + uuid.length()));
            if (retainedBytes > 64L * 1024 * 1024 || tables.size() >= 10000) {
                throw new TrinoException(NOT_SUPPORTED, "Hoglake transaction exceeds metadata or table limit");
            }
            table = new TableChanges(uuid);
            tables.put(key, table);
        }
        if (!table.uuid.equals(uuid)) {
            throw new IllegalStateException("Transaction table identity changed");
        }
        return table;
    }

    private void reserve(String path, long overhead)
    {
        retainedBytes = Math.addExact(retainedBytes, overhead + 2L * path.length());
        if (retainedBytes > 64L * 1024 * 1024 || uploads.size() >= 10000) {
            throw new TrinoException(NOT_SUPPORTED, "Hoglake transaction exceeds 64 MiB metadata or 10000 staged uploads");
        }
        uploads.add(path);
    }

    synchronized void commit()
    {
        if (tables.isEmpty()) {
            return;
        }
        List<HoglakeDtos.Append> appends = new ArrayList<>();
        List<HoglakeDtos.Deletes> deletes = new ArrayList<>();
        tables.forEach((name, changes) -> {
            if (!changes.appends.isEmpty()) {
                appends.add(new HoglakeDtos.Append(name.getSchemaName(), name.getTableName(), changes.uuid, List.copyOf(changes.appends.values())));
            }
            List<HoglakeDtos.DeleteRegistration> vectors = changes.deletes.values().stream().map(file -> {
                if (file.dataFileId() >= 0) {
                    return file;
                }
                HoglakeDtos.FileRegistration pending = changes.appends.get(file.dataFileId());
                if (pending == null) {
                    throw new IllegalStateException("Unknown pending delete target");
                }
                return new HoglakeDtos.DeleteRegistration(0, file.path(), file.deleteCount(), file.fileSizeBytes(), pending.path());
            }).toList();
            // Empty vectors still guard zero-row writes against concurrent changes.
            deletes.add(new HoglakeDtos.Deletes(name.getSchemaName(), name.getTableName(), changes.uuid, vectors));
        });
        client.renewUploads(operation);
        publicationStarted = true;
        client.commitTransaction(new HoglakeDtos.Commit(snapshot(), appends, deletes, operation));
    }

    synchronized void rollback()
    {
        if (!publicationStarted && !uploads.isEmpty()) {
            client.abandonUploads(operation, List.copyOf(uploads));
        }
    }

    private static final class TableChanges
    {
        private final String uuid;
        private final Map<String, Long> paths = new LinkedHashMap<>();
        private final Map<Long, HoglakeDtos.FileRegistration> appends = new LinkedHashMap<>();
        private final Map<Long, HoglakeDtos.DeleteRegistration> deletes = new LinkedHashMap<>();

        private TableChanges(String uuid)
        {
            this.uuid = uuid;
        }
    }
}
