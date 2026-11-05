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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import java.util.function.BooleanSupplier;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Whether this coordinator process holds any query work at all.
 *
 * <p>{@link QueryDrainStatusResource} proves one named query absent. A drain controller also has to
 * settle requests whose query identity it never learned, such as a statement submission whose router
 * process died before the response was recorded. For those, the only sound evidence is that the
 * coordinator holds no work at all: nothing submitted but not yet dispatched, nothing queued, running
 * or finishing, and no results waiting to be fetched. A finished query that only remains in history is
 * not work.
 *
 * <p>Callers must fence requests that can start after the reading: this reports the moment of the read.
 */
@Path("/v1/drain-status")
@ResourceSecurity(MANAGEMENT_READ)
public class CoordinatorDrainStatusResource
{
    private final String nodeId;
    private final String coordinatorId;
    private final BooleanSupplier pendingSubmissions;
    private final BooleanSupplier dispatchedQueries;
    private final BooleanSupplier executingQueries;
    private final BooleanSupplier retainedResults;

    @Inject
    public CoordinatorDrainStatusResource(
            NodeInfo nodeInfo,
            QueryIdGenerator queryIdGenerator,
            QueuedStatementResource queuedStatementResource,
            DispatchManager dispatchManager,
            QueryManager queryManager,
            ExecutingStatementResource executingStatementResource)
    {
        this(nodeInfo.getNodeId(),
                queryIdGenerator.getCoordinatorId(),
                queuedStatementResource::hasPendingSubmission,
                () -> dispatchManager.getQueries().stream().anyMatch(query -> !query.getState().isDone()),
                () -> queryManager.getQueries().stream().anyMatch(query -> !query.getState().isDone()),
                executingStatementResource::hasQueries);
    }

    @VisibleForTesting
    CoordinatorDrainStatusResource(
            String nodeId,
            String coordinatorId,
            BooleanSupplier pendingSubmissions,
            BooleanSupplier dispatchedQueries,
            BooleanSupplier executingQueries,
            BooleanSupplier retainedResults)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.coordinatorId = requireNonNull(coordinatorId, "coordinatorId is null");
        this.pendingSubmissions = requireNonNull(pendingSubmissions, "pendingSubmissions is null");
        this.dispatchedQueries = requireNonNull(dispatchedQueries, "dispatchedQueries is null");
        this.executingQueries = requireNonNull(executingQueries, "executingQueries is null");
        this.retainedResults = requireNonNull(retainedResults, "retainedResults is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public CoordinatorDrainStatus drainStatus()
    {
        // Read in lifecycle order, as QueryDrainStatusResource does: a query that moves to a later
        // registry during the reading is still seen there. A failing registry throws; it never reads idle.
        boolean idle = !pendingSubmissions.getAsBoolean()
                && !dispatchedQueries.getAsBoolean()
                && !executingQueries.getAsBoolean()
                && !retainedResults.getAsBoolean();
        return new CoordinatorDrainStatus(nodeId, coordinatorId, idle);
    }

    public record CoordinatorDrainStatus(String nodeId, String coordinatorId, boolean idle) {}
}
