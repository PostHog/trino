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

import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

public record HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, java.util.List<io.trino.plugin.hoglake.rest.HoglakeDtos.PartitionField> partitionFields, java.util.List<io.trino.plugin.hoglake.rest.HoglakeDtos.SortField> sortFields, boolean claimUploads)
        implements ConnectorMergeTableHandle
{
    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId)
    {
        this(table, dataPath, operationId, Optional.empty());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure)
    {
        this(table, dataPath, operationId, insertFailure, java.util.List.of());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, java.util.List<io.trino.plugin.hoglake.rest.HoglakeDtos.PartitionField> partitionFields)
    {
        this(table, dataPath, operationId, insertFailure, partitionFields, java.util.List.of());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, java.util.List<io.trino.plugin.hoglake.rest.HoglakeDtos.PartitionField> partitionFields, java.util.List<io.trino.plugin.hoglake.rest.HoglakeDtos.SortField> sortFields)
    {
        this(table, dataPath, operationId, insertFailure, partitionFields, sortFields, false);
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return table;
    }
}
