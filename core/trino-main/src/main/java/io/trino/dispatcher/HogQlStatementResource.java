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
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import static io.trino.execution.QuerySubmission.hogQl;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/hogql")
public class HogQlStatementResource
{
    private final QueuedStatementResource queuedStatementResource;

    @Inject
    public HogQlStatementResource(QueuedStatementResource queuedStatementResource)
    {
        this.queuedStatementResource = requireNonNull(queuedStatementResource, "queuedStatementResource is null");
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String requestBody,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @BeanParam ExternalUriInfo externalUriInfo)
    {
        HogQlCompileEnvelope envelope;
        try {
            envelope = HogQlRequest.fromJson(requestBody).toCompileEnvelope();
        }
        catch (RuntimeException _) {
            throw new BadRequestException("Invalid HogQL request");
        }
        return queuedStatementResource.postStatement(hogQl(envelope), servletRequest, httpHeaders, externalUriInfo);
    }
}
