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

import com.google.inject.Inject;
import io.trino.hogql.compiler.catalog.BoundedAsyncHogQlExchangeRateSnapshotCache;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateException.Failure;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshot;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotJsonDecoder;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotLoader.LoadRequest;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotProvider;
import io.trino.hogql.compiler.catalog.HogQlExchangeRateSnapshotProvider.PinnedSnapshot;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class HogQlExchangeRateManager
        implements HogQlExchangeRateSnapshotProvider
{
    private final Object engineLock = new Object();
    private final int maximumEngines;
    private final Duration requestTimeout;
    private final ThreadPoolExecutor loaderExecutor;
    private final BoundedAsyncHogQlExchangeRateSnapshotCache cache;
    private final LinkedHashMap<Long, HogQlExchangeRateConversionEngine> engines = new LinkedHashMap<>(16, 0.75f, true);

    @Inject
    public HogQlExchangeRateManager(
            HogQlSemanticCatalogConfig config,
            HogQlExchangeRateHttpTransport transport,
            HogQlExchangeRateSnapshotJsonDecoder decoder)
    {
        requireNonNull(config, "config is null");
        maximumEngines = config.getMaximumEntries();
        requestTimeout = config.getRequestTimeout().toJavaTime();
        loaderExecutor = new ThreadPoolExecutor(
                config.getLoaderThreads(),
                config.getLoaderThreads(),
                0,
                MILLISECONDS,
                new ArrayBlockingQueue<>(config.getLoaderQueueCapacity()),
                daemonThreadsNamed("hogql-exchange-rate-loader-%s"),
                new ThreadPoolExecutor.AbortPolicy());
        HogQlExchangeRateSnapshotLoader loader = HogQlExchangeRateSnapshotLoader.fromJsonTransport(transport, decoder);
        cache = new BoundedAsyncHogQlExchangeRateSnapshotCache(
                config.getMaximumEntries(),
                config.getRefreshAfter().toJavaTime(),
                config.getExpireAfter().toJavaTime(),
                config.getFailureBackoff().toJavaTime(),
                System::nanoTime,
                loaderExecutor,
                expectedGeneration -> loader.load(expectedGeneration.isPresent()
                        ? LoadRequest.pinned(expectedGeneration.orElseThrow())
                        : LoadRequest.latest()));
        cache.currentSnapshot(OptionalLong.empty());
    }

    @Override
    public PinnedSnapshot pin(OptionalLong expectedGeneration)
    {
        return new PinnedSnapshot(loadSnapshot(expectedGeneration));
    }

    public HogQlExchangeRateConversionEngine engine(long generation)
    {
        synchronized (engineLock) {
            HogQlExchangeRateConversionEngine engine = engines.get(generation);
            if (engine != null) {
                return engine;
            }
        }

        HogQlExchangeRateSnapshot snapshot = loadSnapshot(OptionalLong.of(generation));
        HogQlExchangeRateConversionEngine created = new HogQlExchangeRateConversionEngine(snapshot);
        synchronized (engineLock) {
            HogQlExchangeRateConversionEngine existing = engines.get(generation);
            if (existing != null) {
                return existing;
            }
            engines.put(generation, created);
            while (engines.size() > maximumEngines) {
                Long oldestGeneration = engines.keySet().iterator().next();
                engines.remove(oldestGeneration);
            }
            return created;
        }
    }

    private HogQlExchangeRateSnapshot loadSnapshot(OptionalLong expectedGeneration)
    {
        Optional<HogQlExchangeRateSnapshot> current = cache.currentSnapshot(expectedGeneration);
        if (current.isPresent()) {
            return current.orElseThrow();
        }
        try {
            return cache.prewarm(expectedGeneration)
                    .toCompletableFuture()
                    .get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw unavailable(e);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof HogQlExchangeRateException exchangeRateException) {
                throw exchangeRateException;
            }
            throw unavailable(e.getCause());
        }
        catch (TimeoutException | RuntimeException e) {
            throw unavailable(e);
        }
    }

    private static HogQlExchangeRateException unavailable(Throwable cause)
    {
        return new HogQlExchangeRateException(Failure.UNAVAILABLE, "HogQL exchange-rate snapshot is unavailable", cause);
    }

    @PreDestroy
    public void shutdown()
    {
        loaderExecutor.shutdownNow();
    }
}
