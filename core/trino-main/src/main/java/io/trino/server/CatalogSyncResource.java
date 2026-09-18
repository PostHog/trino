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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.node.NodeInfo;
import io.trino.connector.CatalogSyncFailure;
import io.trino.connector.CatalogSynchronizer;
import io.trino.connector.CatalogSynchronizer.CatalogSyncState;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.NodeVersion;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Reports how far this coordinator has followed the catalogs published in the catalog store. A
 * controller that publishes a catalog uses this to find out whether a specific coordinator process
 * actually applied a specific revision, instead of inferring it from a healthy HTTP endpoint.
 *
 * <p>The answer is bound to a process: {@code processId} changes whenever the coordinator restarts,
 * so a readiness receipt collected for an earlier process is recognizably stale.
 */
@Path("/v1/catalog/sync")
public class CatalogSyncResource
{
    private final String nodeId;
    private final String processId;
    private final NodeVersion nodeVersion;
    private final CatalogSynchronizer catalogSynchronizer;
    private final CatalogManager catalogManager;
    private final Optional<QueryIdGenerator> queryIdGenerator;
    private final Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager;
    private final GroupProviderManager groupProviderManager;
    private final AccessControlManager accessControlManager;

    @Inject
    public CatalogSyncResource(
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            CatalogSynchronizer catalogSynchronizer,
            CatalogManager catalogManager,
            Optional<QueryIdGenerator> queryIdGenerator,
            Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager,
            GroupProviderManager groupProviderManager,
            AccessControlManager accessControlManager)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        this.nodeId = nodeInfo.getNodeId();
        this.processId = nodeInfo.getInstanceId();
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.catalogSynchronizer = requireNonNull(catalogSynchronizer, "catalogSynchronizer is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.groupProviderManager = requireNonNull(groupProviderManager, "groupProviderManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    @Produces(APPLICATION_JSON)
    public CatalogSyncStatus catalogSyncStatus()
    {
        CatalogSyncState state = catalogSynchronizer.catalogSyncState();
        boolean enabled = catalogSynchronizer.isEnabled();
        boolean ready = enabled && state.isUpToDate();
        return new CatalogSyncStatus(
                nodeId,
                nodeVersion.version(),
                processId,
                queryIdGenerator.map(QueryIdGenerator::getCoordinatorId).orElse(null),
                enabled,
                ready,
                boxed(state.observedRevision()),
                boxed(state.appliedRevision()),
                catalogManager.getCatalogNames().size(),
                state.failedCatalogs(),
                boxed(state.lastSuccessMillis()),
                boxed(state.lastSuccessMillis().stream().map(millis -> System.currentTimeMillis() - millis).findFirst()),
                state.lastFailure().map(CatalogSyncFailure::name).orElse(null),
                securityRevisions());
    }

    /**
     * What the security components of this process have loaded. This is reported beside the
     * catalog revision and is deliberately not folded into {@code ready}: a catalog revision says
     * nothing about which credentials, groups or authorization data are in effect, and a caller
     * that needs both has to check both.
     */
    private List<ComponentRevision> securityRevisions()
    {
        ImmutableList.Builder<ComponentRevision> revisions = ImmutableList.builder();
        passwordAuthenticatorManager.ifPresent(manager -> revisions.addAll(manager.loadedRevisions()));
        groupProviderManager.loadedRevision().ifPresent(revisions::add);
        revisions.addAll(accessControlManager.loadedRevisions());
        return revisions.build();
    }

    /**
     * A revision that was never observed is reported as a missing value rather than as a number
     * that could be mistaken for a real revision.
     */
    private static Long boxed(OptionalLong value)
    {
        return value.stream().boxed().findFirst().orElse(null);
    }

    public record CatalogSyncStatus(
            String nodeId,
            String nodeVersion,
            String processId,
            String coordinatorId,
            boolean enabled,
            boolean ready,
            Long observedRevision,
            Long appliedRevision,
            int activeCatalogs,
            int failedCatalogs,
            Long lastSuccessMillis,
            Long lastSuccessAgeMillis,
            String lastFailure,
            List<ComponentRevision> securityRevisions) {}
}
