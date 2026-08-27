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

import io.airlift.units.Duration;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotLoader;
import io.trino.hogql.parser.HogQlLanguageVersion;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlSemanticCatalogManager
{
    private static final HogQlLanguageVersion LANGUAGE_VERSION = HogQlLanguageVersion.valueOf("1.0.0");
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);

    @Test
    public void testColdAndExpiredReadsStayLocalWhileRefreshIsAsynchronous()
    {
        AtomicLong ticker = new AtomicLong();
        AtomicInteger loads = new AtomicInteger();
        CompletableFuture<Void> firstLoadStarted = new CompletableFuture<>();
        CompletableFuture<Void> secondLoadStarted = new CompletableFuture<>();
        CompletableFuture<HogQlSemanticCatalogSnapshot> firstLoad = new CompletableFuture<>();
        CompletableFuture<HogQlSemanticCatalogSnapshot> secondLoad = new CompletableFuture<>();
        HogQlSemanticCatalogSnapshotLoader loader = _ -> switch (loads.incrementAndGet()) {
            case 1 -> {
                firstLoadStarted.complete(null);
                yield firstLoad;
            }
            case 2 -> {
                secondLoadStarted.complete(null);
                yield secondLoad;
            }
            default -> throw new IllegalStateException("unexpected semantic catalog load");
        };
        HogQlSemanticCatalogManager manager = new HogQlSemanticCatalogManager(config(), loader, LANGUAGE_VERSION, ticker::get);
        try {
            Optional<HogQlSemanticCatalogSnapshot> cold = manager.cache().currentSnapshot(CATALOG);
            assertThat(cold).isEmpty();
            firstLoadStarted.join();
            CompletableFuture<HogQlSemanticCatalogSnapshot> firstRefresh = manager.prewarm(CATALOG).toCompletableFuture();
            firstLoad.complete(snapshot(1));
            assertThat(firstRefresh.join().generation()).isEqualTo(1);
            assertThat(manager.cache().currentSnapshot(CATALOG)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);

            ticker.set(3 * SECONDS.toNanos(1));
            Optional<HogQlSemanticCatalogSnapshot> expired = manager.cache().currentSnapshot(CATALOG);
            assertThat(expired).isEmpty();
            secondLoadStarted.join();
            CompletableFuture<HogQlSemanticCatalogSnapshot> secondRefresh = manager.prewarm(CATALOG).toCompletableFuture();
            secondLoad.complete(snapshot(2));
            assertThat(secondRefresh.join().generation()).isEqualTo(2);
            assertThat(loads).hasValue(2);
        }
        finally {
            manager.shutdown();
        }
    }

    @Test
    public void testShutdownTerminatesDedicatedLoaderExecutor()
    {
        HogQlSemanticCatalogManager manager = new HogQlSemanticCatalogManager(
                config(),
                _ -> CompletableFuture.completedFuture(snapshot(1)),
                LANGUAGE_VERSION,
                System::nanoTime);

        assertThat(manager.isLoaderExecutorShutdown()).isFalse();
        manager.shutdown();
        assertThat(manager.isLoaderExecutorShutdown()).isTrue();
    }

    @Test
    public void testCatalogLifecyclePrewarmsWithoutWaitingForMetadata()
    {
        PhysicalIdentifier unusualCatalog = new PhysicalIdentifier("sales-data", true);
        AtomicInteger loads = new AtomicInteger();
        CompletableFuture<Void> loadStarted = new CompletableFuture<>();
        CompletableFuture<HogQlSemanticCatalogSnapshot> metadata = new CompletableFuture<>();
        HogQlSemanticCatalogManager manager = new HogQlSemanticCatalogManager(
                config(),
                request -> {
                    assertThat(request.catalog()).isEqualTo(unusualCatalog);
                    loads.incrementAndGet();
                    loadStarted.complete(null);
                    return metadata;
                },
                LANGUAGE_VERSION,
                System::nanoTime);
        try {
            CompletableFuture<Void> listenerInvocation = CompletableFuture.runAsync(
                    () -> new HogQlSemanticCatalogPrewarmListener(manager).catalogLoaded(new CatalogName("sales-data")));

            assertThat(listenerInvocation).succeedsWithin(10, SECONDS);
            loadStarted.join();
            assertThat(loads).hasValue(1);
            assertThat(manager.cache().currentSnapshot(unusualCatalog)).isEmpty();
        }
        finally {
            metadata.complete(new HogQlSemanticCatalogSnapshot(2, LANGUAGE_VERSION, unusualCatalog, 1, List.of()));
            manager.shutdown();
        }
    }

    @Test
    public void testRejectedLoaderWorkIsBackedOffBeforeRetry()
    {
        AtomicLong ticker = new AtomicLong();
        PhysicalIdentifier runningCatalog = new PhysicalIdentifier("running", false);
        PhysicalIdentifier queuedCatalog = new PhysicalIdentifier("queued", false);
        PhysicalIdentifier rejectedCatalog = new PhysicalIdentifier("rejected", false);
        CompletableFuture<Void> runningStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseRunning = new CompletableFuture<>();
        CompletableFuture<Void> rejectedStarted = new CompletableFuture<>();
        CompletableFuture<HogQlSemanticCatalogSnapshot> rejectedLoad = new CompletableFuture<>();
        AtomicInteger rejectedLoads = new AtomicInteger();
        HogQlSemanticCatalogSnapshotLoader loader = request -> switch (request.catalog().value()) {
            case "running" -> {
                runningStarted.complete(null);
                releaseRunning.join();
                yield CompletableFuture.completedFuture(snapshot(runningCatalog, 1));
            }
            case "queued" -> CompletableFuture.completedFuture(snapshot(queuedCatalog, 1));
            case "rejected" -> {
                rejectedLoads.incrementAndGet();
                rejectedStarted.complete(null);
                yield rejectedLoad;
            }
            default -> throw new IllegalStateException("unexpected catalog load");
        };
        HogQlSemanticCatalogConfig config = config()
                .setMaximumEntries(4)
                .setFailureBackoff(new Duration(10, SECONDS))
                .setLoaderQueueCapacity(1);
        HogQlSemanticCatalogManager manager = new HogQlSemanticCatalogManager(config, loader, LANGUAGE_VERSION, ticker::get);
        try {
            CompletionStage<HogQlSemanticCatalogSnapshot> runningRefresh = manager.prewarm(runningCatalog);
            runningStarted.join();
            CompletionStage<HogQlSemanticCatalogSnapshot> queuedRefresh = manager.prewarm(queuedCatalog);
            CompletionStage<HogQlSemanticCatalogSnapshot> rejectedRefresh = manager.prewarm(rejectedCatalog);

            assertThatThrownBy(rejectedRefresh.toCompletableFuture()::join)
                    .cause()
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(rejectedLoads).hasValue(0);
            assertThat(manager.cache().currentSnapshot(rejectedCatalog)).isEmpty();

            releaseRunning.complete(null);
            runningRefresh.toCompletableFuture().join();
            queuedRefresh.toCompletableFuture().join();

            ticker.set(SECONDS.toNanos(10) - 1);
            assertThat(manager.cache().currentSnapshot(rejectedCatalog)).isEmpty();
            assertThat(rejectedLoads).hasValue(0);

            ticker.set(SECONDS.toNanos(10));
            assertThat(manager.cache().currentSnapshot(rejectedCatalog)).isEmpty();
            rejectedStarted.join();
            assertThat(rejectedLoads).hasValue(1);
            rejectedLoad.complete(snapshot(rejectedCatalog, 1));
            assertThat(manager.cache().currentSnapshot(rejectedCatalog)).get().extracting(HogQlSemanticCatalogSnapshot::generation).isEqualTo(1L);
        }
        finally {
            releaseRunning.complete(null);
            rejectedLoad.completeExceptionally(new IllegalStateException("test shutdown"));
            manager.shutdown();
        }
    }

    private static HogQlSemanticCatalogConfig config()
    {
        return new HogQlSemanticCatalogConfig()
                .setMaximumEntries(2)
                .setRefreshAfter(new Duration(1, SECONDS))
                .setExpireAfter(new Duration(2, SECONDS))
                .setFailureBackoff(new Duration(0, SECONDS))
                .setLoaderThreads(1)
                .setLoaderQueueCapacity(2);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(long generation)
    {
        return snapshot(CATALOG, generation);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(PhysicalIdentifier catalog, long generation)
    {
        return new HogQlSemanticCatalogSnapshot(2, LANGUAGE_VERSION, catalog, generation, List.of());
    }
}
