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

import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
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

public class TestHogQlExchangeRateHttpTransport
{
    private static final URI BASE_URI = URI.create("https://duckgres.example/control-plane/");
    private static final String SECRET = "secret-metadata-response";
    private static final String AUTHENTICATION_TOKEN = "test-authentication-token";

    @Test
    public void testConstructsAuthenticatedLatestAndExactRequests()
    {
        List<Request> requests = new ArrayList<>();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requests.add(request);
            return response(OK, "{}");
        });
        HogQlExchangeRateHttpTransport transport = new HogQlExchangeRateHttpTransport(BASE_URI, httpClient, () -> AUTHENTICATION_TOKEN);

        assertThat(transport.load(LoadRequest.latest()).toCompletableFuture().join()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
        assertThat(transport.load(LoadRequest.pinned(42)).toCompletableFuture().join()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));

        assertThat(requests).extracting(request -> request.getUri().toASCIIString()).containsExactly(
                "https://duckgres.example/control-plane/v1/hogql/compatibility/exchange-rates?protocolVersion=1",
                "https://duckgres.example/control-plane/v1/hogql/compatibility/exchange-rates?protocolVersion=1&generation=42");
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
        HogQlExchangeRateHttpTransport transport = new HogQlExchangeRateHttpTransport(BASE_URI, httpClient, tokens::removeFirst);

        transport.load(LoadRequest.latest()).toCompletableFuture().join();
        transport.load(LoadRequest.latest()).toCompletableFuture().join();

        assertThat(requests).extracting(request -> request.getHeader("X-Duckgres-Internal-Secret"))
                .containsExactly("first-token", "second-token");
    }

    @Test
    public void testRejectsOversizedOrNonJsonResponses()
    {
        HogQlExchangeRateHttpTransport oversized = new HogQlExchangeRateHttpTransport(
                BASE_URI,
                new TestingHttpClient(_ -> response(OK, "12345")),
                4,
                () -> AUTHENTICATION_TOKEN);
        assertUnavailable(oversized, LoadRequest.latest());

        TestingResponse plainText = new TestingResponse(OK, ImmutableListMultimap.of(), "plain text".getBytes(StandardCharsets.UTF_8));
        HogQlExchangeRateHttpTransport nonJson = new HogQlExchangeRateHttpTransport(
                BASE_URI,
                new TestingHttpClient(_ -> plainText),
                () -> AUTHENTICATION_TOKEN);
        assertUnavailable(nonJson, LoadRequest.latest());
    }

    @Test
    public void testRejectsInvalidAuthenticationWithoutDisclosure()
    {
        HogQlExchangeRateHttpTransport transport = new HogQlExchangeRateHttpTransport(
                BASE_URI,
                new TestingHttpClient(_ -> response(OK, "{}")),
                () -> "line-one\nline-two");

        assertThatThrownBy(() -> transport.load(LoadRequest.latest()).toCompletableFuture().join())
                .cause()
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(Failure.UNAVAILABLE);
                    assertThat(exception).hasMessageNotContaining("line-one");
                });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statusFailures")
    public void testMapsHttpStatusWithoutResponseDisclosure(String name, HttpStatus status, LoadRequest request, Failure failure)
    {
        HogQlExchangeRateHttpTransport transport = new HogQlExchangeRateHttpTransport(
                BASE_URI,
                new TestingHttpClient(_ -> response(status, SECRET)),
                () -> AUTHENTICATION_TOKEN);

        assertThatThrownBy(() -> transport.load(request).toCompletableFuture().join())
                .cause()
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> {
                    assertThat(exception.failure()).isEqualTo(failure);
                    assertThat(exception).hasMessageNotContaining(SECRET);
                    assertThat(exception.getMessage()).hasSizeLessThan(128);
                });
    }

    private static Stream<Arguments> statusFailures()
    {
        return Stream.of(
                Arguments.of("latest missing", NOT_FOUND, LoadRequest.latest(), Failure.UNAVAILABLE),
                Arguments.of("exact missing", NOT_FOUND, LoadRequest.pinned(7), Failure.GENERATION_MISMATCH),
                Arguments.of("generation conflict", CONFLICT, LoadRequest.pinned(7), Failure.GENERATION_MISMATCH),
                Arguments.of("server failure", INTERNAL_SERVER_ERROR, LoadRequest.latest(), Failure.UNAVAILABLE));
    }

    private static void assertUnavailable(HogQlExchangeRateHttpTransport transport, LoadRequest request)
    {
        assertThatThrownBy(() -> transport.load(request).toCompletableFuture().join())
                .cause()
                .isInstanceOfSatisfying(HogQlExchangeRateException.class, exception -> assertThat(exception.failure()).isEqualTo(Failure.UNAVAILABLE));
    }

    private static Response response(HttpStatus status, String body)
    {
        return new TestingResponse(status, contentType(JSON_UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }
}
