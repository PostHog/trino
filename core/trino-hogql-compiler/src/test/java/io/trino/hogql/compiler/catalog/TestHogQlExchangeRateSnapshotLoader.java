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
package io.trino.hogql.compiler.catalog;

import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.JsonTransport;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlExchangeRateSnapshotLoader
{
    @Test
    public void testPreservesLatestAndPinnedSemanticsThroughJsonTransport()
    {
        List<LoadRequest> requests = new ArrayList<>();
        JsonTransport transport = request -> {
            requests.add(request);
            long generation = request.expectedGeneration().orElse(9);
            return CompletableFuture.completedFuture(snapshotJson(generation).getBytes(StandardCharsets.UTF_8));
        };
        HogQlExchangeRateSnapshotLoader loader = HogQlExchangeRateSnapshotLoader.fromJsonTransport(
                transport,
                new HogQlExchangeRateSnapshotJsonDecoder());

        assertThat(loader.load(LoadRequest.latest()).toCompletableFuture().join().generation()).isEqualTo(9);
        assertThat(loader.load(LoadRequest.pinned(7)).toCompletableFuture().join().generation()).isEqualTo(7);
        assertThat(requests).containsExactly(LoadRequest.latest(), LoadRequest.pinned(7));
    }

    @Test
    public void testRejectsInvalidPinnedGenerationBeforeTransport()
    {
        assertThatThrownBy(() -> LoadRequest.pinned(0)).isInstanceOf(IllegalArgumentException.class);
    }

    private static String snapshotJson(long generation)
    {
        return """
               {"protocolVersion":1,"schemaVersion":1,"generation":%s,"baseCurrency":"USD","decimalScale":10,
                "rates":[{"currency":"USD","effectiveDate":"1970-01-01","unscaledRate":"10000000000"}]}
               """.formatted(generation);
    }
}
