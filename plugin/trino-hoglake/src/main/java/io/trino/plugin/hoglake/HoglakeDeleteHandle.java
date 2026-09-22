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

import io.trino.plugin.hoglake.rest.HoglakeDtos.PartitionField;
import io.trino.plugin.hoglake.rest.HoglakeDtos.SortField;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

public record HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, List<PartitionField> partitionFields, List<SortField> sortFields, boolean claimUploads)
        implements ConnectorMergeTableHandle
{
    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId)
    {
        this(table, dataPath, operationId, Optional.empty());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure)
    {
        this(table, dataPath, operationId, insertFailure, List.of());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, List<PartitionField> partitionFields)
    {
        this(table, dataPath, operationId, insertFailure, partitionFields, List.of());
    }

    public HoglakeDeleteHandle(HoglakeTableHandle table, String dataPath, String operationId, Optional<String> insertFailure, List<PartitionField> partitionFields, List<SortField> sortFields)
    {
        this(table, dataPath, operationId, insertFailure, partitionFields, sortFields, false);
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return table;
    }
}
