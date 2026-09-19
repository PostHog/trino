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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.spi.security.LoadedConfiguration;
import io.trino.spi.security.SystemAccessControl;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static io.airlift.http.client.HttpStatus.OK;
import static io.trino.plugin.opa.TestConstants.SYSTEM_ACCESS_CONTROL_CONTEXT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A controller that publishes authorization data needs to know when a coordinator decides with it.
 * This access control cannot answer that from anything local: the data lives in OPA, so the answer
 * is read from the OPA that this coordinator actually asks.
 */
final class TestOpaPolicyRevision
{
    private static final URI POLICY_URI = URI.create("http://localhost:8181/v1/data/trino/allow");
    private static final URI REVISION_URI = URI.create("http://localhost:8181/v1/data/trino/revision");

    @Test
    void testReportsTheRevisionOpaServes()
    {
        SystemAccessControl accessControl = createAccessControl(
                Optional.of(REVISION_URI),
                request -> {
                    assertThat(request.getMethod()).isEqualTo("GET");
                    assertThat(request.getUri()).isEqualTo(REVISION_URI);
                    return jsonResponse(OK, "{\"result\":\"policy-2026-09-18.1\"}");
                });

        assertThat(((LoadedConfiguration) accessControl).loadedRevision()).isEqualTo("policy-2026-09-18.1");
    }

    /**
     * A bundle that does not define the document answers with no result at all. That is not a
     * revision, and it must not be turned into one.
     */
    @Test
    void testUndefinedRevisionIsNotAnAcknowledgement()
    {
        SystemAccessControl accessControl = createAccessControl(
                Optional.of(REVISION_URI),
                _ -> jsonResponse(OK, "{}"));

        assertThatThrownBy(() -> ((LoadedConfiguration) accessControl).loadedRevision())
                .isInstanceOf(OpaQueryException.class)
                .hasRootCauseMessage("OPA answered with no policy revision at " + REVISION_URI);
    }

    @Test
    void testUnreachableOpaIsNotAnAcknowledgement()
    {
        SystemAccessControl accessControl = createAccessControl(
                Optional.of(REVISION_URI),
                _ -> jsonResponse(INTERNAL_SERVER_ERROR, "{}"));

        assertThatThrownBy(() -> ((LoadedConfiguration) accessControl).loadedRevision())
                .isInstanceOf(OpaQueryException.class);
    }

    /**
     * Without the setting nothing is claimed. A readiness report lists the access control without a
     * revision, which is the honest answer, rather than inventing one from a local file.
     */
    @Test
    void testNothingIsClaimedWhenTheRevisionUriIsNotConfigured()
    {
        SystemAccessControl accessControl = createAccessControl(
                Optional.empty(),
                _ -> {
                    throw new AssertionError("OPA must not be queried when no revision URI is configured");
                });

        assertThatThrownBy(() -> ((LoadedConfiguration) accessControl).loadedRevision())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("opa.policy.revision-uri is not configured");
    }

    private static SystemAccessControl createAccessControl(Optional<URI> revisionUri, TestingHttpClient.Processor processor)
    {
        ImmutableMap.Builder<String, String> config = ImmutableMap.<String, String>builder()
                .put("opa.policy.uri", POLICY_URI.toString());
        revisionUri.ifPresent(uri -> config.put("opa.policy.revision-uri", uri.toString()));
        Map<String, String> properties = config.buildOrThrow();
        HttpClient httpClient = new TestingHttpClient(processor);
        return OpaAccessControlFactory.create(properties, Optional.of(httpClient), Optional.of(SYSTEM_ACCESS_CONTROL_CONTEXT));
    }

    private static Response jsonResponse(HttpStatus status, String body)
    {
        return new TestingResponse(
                status,
                ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                body.getBytes(UTF_8));
    }
}
