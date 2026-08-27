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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.parser.HogQlLanguageVersion;

import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface HogQlSemanticCatalogSnapshotLoader
{
    CompletionStage<HogQlSemanticCatalogSnapshot> load(LoadRequest request);

    static HogQlSemanticCatalogSnapshotLoader fromJsonTransport(
            JsonTransport transport,
            HogQlSemanticCatalogSnapshotJsonDecoder decoder)
    {
        requireNonNull(transport, "transport is null");
        requireNonNull(decoder, "decoder is null");
        return request -> {
            requireNonNull(request, "request is null");
            CompletionStage<byte[]> response = requireNonNull(transport.load(request), "transport returned null");
            return response.thenApply(payload -> decoder.decode(payload, request));
        };
    }

    @FunctionalInterface
    interface JsonTransport
    {
        CompletionStage<byte[]> load(LoadRequest request);
    }

    record LoadRequest(PhysicalIdentifier catalog, HogQlLanguageVersion languageVersion, OptionalLong expectedGeneration)
    {
        public LoadRequest
        {
            catalog = requireNonNull(catalog, "catalog is null");
            languageVersion = requireNonNull(languageVersion, "languageVersion is null");
            expectedGeneration = requireNonNull(expectedGeneration, "expectedGeneration is null");
            if (expectedGeneration.isPresent() && expectedGeneration.orElseThrow() <= 0) {
                throw new IllegalArgumentException("expected generation must be positive");
            }
        }

        public static LoadRequest latest(PhysicalIdentifier catalog, HogQlLanguageVersion languageVersion)
        {
            return new LoadRequest(catalog, languageVersion, OptionalLong.empty());
        }

        public static LoadRequest pinned(PhysicalIdentifier catalog, HogQlLanguageVersion languageVersion, long generation)
        {
            return new LoadRequest(catalog, languageVersion, OptionalLong.of(generation));
        }
    }
}
