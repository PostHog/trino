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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.hogql.compiler.catalog.BoundedAsyncHogQlSemanticCatalogSnapshotCache;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotCache;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotJsonDecoder;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader.LoadRequest;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlLanguageVersion;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class HogQlSemanticCatalogManager
{
    private final ThreadPoolExecutor loaderExecutor;
    private final HogQlSemanticCatalogSnapshotLoader loader;
    private final BoundedAsyncHogQlSemanticCatalogSnapshotCache cache;

    @Inject
    public HogQlSemanticCatalogManager(
            HogQlSemanticCatalogConfig config,
            HogQlSemanticCatalogHttpTransport transport,
            HogQlSemanticCatalogSnapshotJsonDecoder decoder)
    {
        this(config,
                HogQlLanguageContract.current().languageVersion(),
                System::nanoTime,
                jsonLoaderFactory(transport, decoder));
    }

    @VisibleForTesting
    HogQlSemanticCatalogManager(
            HogQlSemanticCatalogConfig config,
            HogQlSemanticCatalogSnapshotLoader loader,
            HogQlLanguageVersion languageVersion,
            LongSupplier ticker)
    {
        this(config, languageVersion, ticker, _ -> loader);
    }

    private HogQlSemanticCatalogManager(
            HogQlSemanticCatalogConfig config,
            HogQlLanguageVersion languageVersion,
            LongSupplier ticker,
            Function<Executor, HogQlSemanticCatalogSnapshotLoader> loaderFactory)
    {
        requireNonNull(config, "config is null");
        requireNonNull(languageVersion, "languageVersion is null");
        requireNonNull(ticker, "ticker is null");
        requireNonNull(loaderFactory, "loaderFactory is null");
        loaderExecutor = new ThreadPoolExecutor(
                config.getLoaderThreads(),
                config.getLoaderThreads(),
                0,
                MILLISECONDS,
                new ArrayBlockingQueue<>(config.getLoaderQueueCapacity()),
                daemonThreadsNamed("hogql-semantic-catalog-loader-%s"),
                new ThreadPoolExecutor.AbortPolicy());
        loader = requireNonNull(loaderFactory.apply(loaderExecutor), "loader is null");
        cache = new BoundedAsyncHogQlSemanticCatalogSnapshotCache(
                config.getMaximumEntries(),
                config.getRefreshAfter().toJavaTime(),
                config.getExpireAfter().toJavaTime(),
                config.getFailureBackoff().toJavaTime(),
                ticker,
                loaderExecutor,
                catalog -> loader.load(LoadRequest.latest(catalog, languageVersion)));
    }

    public HogQlSemanticCatalogSnapshotLoader loader()
    {
        return loader;
    }

    private static Function<Executor, HogQlSemanticCatalogSnapshotLoader> jsonLoaderFactory(
            HogQlSemanticCatalogHttpTransport transport,
            HogQlSemanticCatalogSnapshotJsonDecoder decoder)
    {
        requireNonNull(transport, "transport is null");
        requireNonNull(decoder, "decoder is null");
        return executor -> request -> transport.load(request).thenApplyAsync(payload -> decoder.decode(payload, request), executor);
    }

    public HogQlSemanticCatalogSnapshotCache cache()
    {
        return cache;
    }

    public CompletionStage<HogQlSemanticCatalogSnapshot> prewarm(PhysicalIdentifier catalog)
    {
        return cache.prewarm(catalog);
    }

    @PreDestroy
    public void shutdown()
    {
        loaderExecutor.shutdownNow();
    }

    @VisibleForTesting
    boolean isLoaderExecutorShutdown()
    {
        return loaderExecutor.isShutdown();
    }
}
