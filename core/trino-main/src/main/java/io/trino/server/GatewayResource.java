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
package io.trino.server;

import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.QueuedStatementResource;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManager;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static java.util.Objects.requireNonNull;

@Path("/v1/integrations/gateway")
@ResourceSecurity(MANAGEMENT_READ)
public class GatewayResource
{
    private static final Pattern QUERY_ID_PATTERN = Pattern.compile("[0-9]{8}_[0-9]{6}_[0-9]{5}_[a-z2-9]{5}");

    private final ClusterMemoryManager clusterMemoryManager;
    private final QueuedStatementResource queuedStatementResource;
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final ExecutingStatementResource executingStatementResource;
    private final String nodeId;
    private final String coordinatorId;

    @Inject
    public GatewayResource(
            ClusterMemoryManager clusterMemoryManager,
            QueuedStatementResource queuedStatementResource,
            DispatchManager dispatchManager,
            QueryManager queryManager,
            ExecutingStatementResource executingStatementResource,
            NodeInfo nodeInfo,
            QueryIdGenerator queryIdGenerator)
    {
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
        this.queuedStatementResource = requireNonNull(queuedStatementResource, "queuedStatementResource is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.executingStatementResource = requireNonNull(executingStatementResource, "executingStatementResource is null");
        this.nodeId = requireNonNull(nodeInfo, "nodeInfo is null").getNodeId();
        this.coordinatorId = requireNonNull(queryIdGenerator, "queryIdGenerator is null").getCoordinatorId();
    }

    @GET
    @Path("query/{queryId}/lifecycle")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryLifecycle queryLifecycle(@PathParam("queryId") String queryIdValue)
    {
        if (!QUERY_ID_PATTERN.matcher(queryIdValue).matches()) {
            throw new BadRequestException("Invalid query ID");
        }
        if (!queryIdValue.endsWith("_" + coordinatorId)) {
            throw new ClientErrorException("Query belongs to another coordinator process", CONFLICT);
        }

        QueryId queryId = new QueryId(queryIdValue);
        // Check registries in handoff order. Queued entries precede the first client response and outlive dispatch registration.
        // A completed execution can still have a cached client result, so metadata absence alone is insufficient.
        boolean queryPresent = queuedStatementResource.hasQuery(queryId) ||
                dispatchManager.isQueryRegistered(queryId) ||
                queryManager.hasQuery(queryId) ||
                executingStatementResource.hasQuery(queryId);
        return new QueryLifecycle(queryIdValue, nodeId, coordinatorId, queryPresent);
    }

    public record QueryLifecycle(String queryId, String nodeId, String coordinatorId, boolean queryPresent) {}

    @GET
    @Path("metrics")
    public ClusterMetrics getClusterMetrics()
    {
        Map<String, Optional<MemoryInfo>> memoryInfo = clusterMemoryManager.getAllNodesMemoryInfo();
        long totalFreeBytes = memoryInfo
                .values()
                .stream()
                .flatMap(Optional::stream)
                .map(MemoryInfo::getPool)
                .mapToLong(MemoryPoolInfo::getFreeBytes)
                .sum();
        double aggregatedSystemLoad = memoryInfo
                .values()
                .stream()
                .flatMap(Optional::stream)
                .mapToDouble(MemoryInfo::getSystemCpuLoad)
                .sum();
        return new ClusterMetrics(memoryInfo.size(), totalFreeBytes, aggregatedSystemLoad);
    }

    /**
     * Represents metrics aggregated from all nodes in the Trino cluster
     */
    public record ClusterMetrics(long clusterSize, long totalFreeBytes, double aggregatedSystemLoad) {}
}
