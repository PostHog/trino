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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.QueuedStatementResource;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManager;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;

import java.util.function.Predicate;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/query/{queryId}/drain-status")
@ResourceSecurity(MANAGEMENT_READ)
public class QueryDrainStatusResource
{
    private final String nodeId;
    private final String coordinatorId;
    private final Predicate<QueryId> queuedQueries;
    private final Predicate<QueryId> dispatchedQueries;
    private final Predicate<QueryId> executingQueries;
    private final Predicate<QueryId> retainedResults;

    @Inject
    public QueryDrainStatusResource(
            NodeInfo nodeInfo,
            QueryIdGenerator queryIdGenerator,
            QueuedStatementResource queuedStatementResource,
            DispatchManager dispatchManager,
            QueryManager queryManager,
            ExecutingStatementResource executingStatementResource)
    {
        this(nodeInfo.getNodeId(),
                queryIdGenerator.getCoordinatorId(),
                queuedStatementResource::hasQuery,
                dispatchManager::isQueryRegistered,
                queryManager::hasQuery,
                executingStatementResource::hasQuery);
    }

    @VisibleForTesting
    QueryDrainStatusResource(
            String nodeId,
            String coordinatorId,
            Predicate<QueryId> queuedQueries,
            Predicate<QueryId> dispatchedQueries,
            Predicate<QueryId> executingQueries,
            Predicate<QueryId> retainedResults)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.coordinatorId = requireNonNull(coordinatorId, "coordinatorId is null");
        this.queuedQueries = requireNonNull(queuedQueries, "queuedQueries is null");
        this.dispatchedQueries = requireNonNull(dispatchedQueries, "dispatchedQueries is null");
        this.executingQueries = requireNonNull(executingQueries, "executingQueries is null");
        this.retainedResults = requireNonNull(retainedResults, "retainedResults is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public QueryDrainStatus drainStatus(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        // Read in lifecycle order; callers must fence requests that can outlive registry removal.
        boolean absent = queryId.id().matches("[0-9]{8}_[0-9]{6}_[0-9]{5}_" + coordinatorId)
                && !queuedQueries.test(queryId)
                && !dispatchedQueries.test(queryId)
                && !executingQueries.test(queryId)
                && !retainedResults.test(queryId);
        return new QueryDrainStatus(nodeId, coordinatorId, absent);
    }

    public record QueryDrainStatus(String nodeId, String coordinatorId, boolean absent) {}
}
