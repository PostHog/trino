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
package io.trino.hogql;

import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogException.Failure;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HttpStatus.CONFLICT;
import static io.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static io.airlift.http.client.HttpStatus.NOT_FOUND;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.testing.TestingResponse.contentType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogHttpTransport
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("Sales & Growth/2026", true);
    private static final URI BASE_URI = URI.create("https://duckgres.example/control-plane/");
    private static final String SECRET = "secret-metadata-response";
    private static final String AUTHENTICATION_TOKEN = "test-authentication-token";

    @Test
    public void testConstructsEncodedLatestAndPinnedUris()
    {
        List<Request> requests = new ArrayList<>();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requests.add(request);
            return response(OK, "{}");
        });
        HogQlSemanticCatalogHttpTransport transport = new HogQlSemanticCatalogHttpTransport(BASE_URI, httpClient, () -> AUTHENTICATION_TOKEN);

        assertThat(transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join())
                .isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
        assertThat(transport.load(LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 42)).toCompletableFuture().join())
                .isEqualTo("{}".getBytes(StandardCharsets.UTF_8));

        assertThat(requests).extracting(request -> request.getUri().toASCIIString()).containsExactly(
                "https://duckgres.example/control-plane/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=1.0.0&catalog=Sales%20%26%20Growth/2026&catalogDelimited=true",
                "https://duckgres.example/control-plane/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=1.0.0&catalog=Sales%20%26%20Growth/2026&catalogDelimited=true&generation=42");
        assertThat(requests).allSatisfy(request -> {
            assertThat(request.getMethod()).isEqualTo("GET");
            assertThat(request.getHeader(ACCEPT)).isEqualTo(JSON_UTF_8.withoutParameters().toString());
            assertThat(request.getHeader("X-Duckgres-Internal-Secret")).isEqualTo(AUTHENTICATION_TOKEN);
        });
    }

    @Test
    public void testReadsAuthenticationTokenForEveryRequest()
    {
        List<Request> requests = new ArrayList<>();
        List<String> tokens = new ArrayList<>(List.of("first-token", "second-token"));
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requests.add(request);
            return response(OK, "{}");
        });
        HogQlSemanticCatalogHttpTransport transport = new HogQlSemanticCatalogHttpTransport(BASE_URI, httpClient, () -> tokens.removeFirst());

        transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join();
        transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join();

        assertThat(requests).extracting(request -> request.getHeader("X-Duckgres-Internal-Secret"))
                .containsExactly("first-token", "second-token");
    }

    @Test
    public void testReadsRotatingAuthenticationTokenFileForEveryRequest(@TempDir Path temporaryDirectory)
            throws Exception
    {
        Path tokenFile = temporaryDirectory.resolve("hogql-catalog-token");
        Files.writeString(tokenFile, "first-token\n");
        List<Request> requests = new ArrayList<>();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requests.add(request);
            return response(OK, "{}");
        });
        HogQlSemanticCatalogHttpTransport transport = new HogQlSemanticCatalogHttpTransport(
                new HogQlSemanticCatalogConfig()
                        .setUri(BASE_URI)
                        .setAuthenticationTokenFile(tokenFile.toString()),
                httpClient);

        transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join();
        Files.writeString(tokenFile, "second-token");
        transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join();

        assertThat(requests).extracting(request -> request.getHeader("X-Duckgres-Internal-Secret"))
                .containsExactly("first-token", "second-token");
    }

    @ParameterizedTest
    @MethodSource("invalidAuthenticationTokens")
    public void testRejectsInvalidAuthenticationTokensWithoutDisclosure(String token)
    {
        HogQlSemanticCatalogHttpTransport transport = new HogQlSemanticCatalogHttpTransport(
                BASE_URI,
                new TestingHttpClient(_ -> response(OK, "{}")),
                () -> token);

        assertThatThrownBy(() -> transport.load(LoadRequest.latest(CATALOG, LANGUAGE_VERSION)).toCompletableFuture().join())
                .cause()
                .isInstanceOfSatisfying(HogQlSemanticCatalogException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(Failure.UNAVAILABLE);
                    if (!token.isEmpty()) {
                        assertThat(exception).hasMessageNotContaining(token);
                    }
                });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statusFailures")
    public void testMapsHttpStatusWithoutResponseDisclosure(String name, HttpStatus status, LoadRequest request, Failure failure)
    {
        TestingHttpClient httpClient = new TestingHttpClient(_ -> response(status, SECRET));
        HogQlSemanticCatalogHttpTransport transport = new HogQlSemanticCatalogHttpTransport(BASE_URI, httpClient, () -> AUTHENTICATION_TOKEN);

        assertThatThrownBy(() -> transport.load(request).toCompletableFuture().join())
                .cause()
                .isInstanceOfSatisfying(HogQlSemanticCatalogException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(failure);
                    assertThat(exception).hasMessageNotContaining(SECRET);
                    assertThat(exception.getMessage()).hasSizeLessThan(128);
                });
    }

    private static Stream<Arguments> statusFailures()
    {
        return Stream.of(
                Arguments.of("latest missing", NOT_FOUND, LoadRequest.latest(CATALOG, LANGUAGE_VERSION), Failure.UNAVAILABLE),
                Arguments.of("pinned missing", NOT_FOUND, LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 7), Failure.GENERATION_MISMATCH),
                Arguments.of("generation conflict", CONFLICT, LoadRequest.pinned(CATALOG, LANGUAGE_VERSION, 7), Failure.GENERATION_MISMATCH),
                Arguments.of("server failure", INTERNAL_SERVER_ERROR, LoadRequest.latest(CATALOG, LANGUAGE_VERSION), Failure.UNAVAILABLE));
    }

    private static Stream<String> invalidAuthenticationTokens()
    {
        return Stream.of("", "   ", "line-one\nline-two");
    }

    private static Response response(HttpStatus status, String body)
    {
        return new TestingResponse(status, contentType(JSON_UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }
}
