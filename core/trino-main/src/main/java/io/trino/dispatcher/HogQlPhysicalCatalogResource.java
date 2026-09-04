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
package io.trino.dispatcher;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.hogql.HogQlPhysicalCatalog;
import io.trino.hogql.HogQlPhysicalCatalogProvider;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.SessionContext;
import io.trino.server.SessionSupplier;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.security.Identity;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;

import java.util.Optional;

import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Path("/v1/hogql/compatibility/physical-catalog")
public class HogQlPhysicalCatalogResource
{
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final SessionSupplier sessionSupplier;
    private final QueryIdGenerator queryIdGenerator;
    private final HogQlPhysicalCatalogProvider physicalCatalogProvider;

    @Inject
    public HogQlPhysicalCatalogResource(
            HttpRequestSessionContextFactory sessionContextFactory,
            SessionSupplier sessionSupplier,
            QueryIdGenerator queryIdGenerator,
            HogQlPhysicalCatalogProvider physicalCatalogProvider)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.physicalCatalogProvider = requireNonNull(physicalCatalogProvider, "physicalCatalogProvider is null");
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Produces(APPLICATION_JSON)
    public HogQlPhysicalCatalog getPhysicalCatalog(
            @QueryParam("catalog") String catalog,
            @QueryParam("protocolVersion") Integer protocolVersion,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders)
    {
        if (catalog == null || catalog.isBlank()) {
            throw new BadRequestException("Catalog is required");
        }
        if (protocolVersion == null || protocolVersion != HogQlPhysicalCatalog.PROTOCOL_VERSION) {
            throw new BadRequestException("Unsupported physical catalog protocol version");
        }
        catalog = catalog.toLowerCase(ENGLISH);

        Optional<Identity> identity = authenticatedIdentity(servletRequest);
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw new ForbiddenException("Internal communication can not be used to read a physical catalog");
        }

        SessionContext sessionContext = sessionContextFactory.createSessionContext(
                httpHeaders.getRequestHeaders(),
                Optional.ofNullable(servletRequest.getRemoteAddr()),
                identity);
        if (sessionContext.getTransactionId().isPresent()) {
            throw new BadRequestException("A transaction ID is not supported for physical catalog requests");
        }
        Session session = sessionSupplier.createSession(queryIdGenerator.createNextQueryId(), Span.getInvalid(), sessionContext);
        return physicalCatalogProvider.load(session, catalog);
    }
}
