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
package io.trino.execution;

import com.google.inject.Inject;
import io.trino.hogql.HogQlConfig;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.hogql.HogQlCoordinatorErrorCode.HOGQL_COMPILATION_QUEUE_FULL;
import static io.trino.hogql.HogQlCoordinatorErrorCode.HOGQL_COMPILATION_TIMEOUT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class HogQlCompilationExecutor
{
    private final Executor executor;
    private final Optional<ThreadPoolExecutor> ownedExecutor;
    private final OptionalLong timeoutMillis;
    private final Set<FutureTask<?>> tasks = ConcurrentHashMap.newKeySet();

    @Inject
    public HogQlCompilationExecutor(HogQlConfig config)
    {
        requireNonNull(config, "config is null");
        BlockingQueue<Runnable> queue = config.getCompilationQueueCapacity() == 0
                ? new SynchronousQueue<>()
                : new ArrayBlockingQueue<>(config.getCompilationQueueCapacity());
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                config.getCompilationThreads(),
                config.getCompilationThreads(),
                0,
                MILLISECONDS,
                queue,
                daemonThreadsNamed("hogql-compilation-%s"),
                new ThreadPoolExecutor.AbortPolicy());
        this.executor = executor;
        this.ownedExecutor = Optional.of(executor);
        this.timeoutMillis = OptionalLong.of(config.getCompilationTimeout().toMillis());
    }

    private HogQlCompilationExecutor(Executor executor)
    {
        this.executor = requireNonNull(executor, "executor is null");
        ownedExecutor = Optional.empty();
        timeoutMillis = OptionalLong.empty();
    }

    static HogQlCompilationExecutor directExecutor()
    {
        return new HogQlCompilationExecutor(Runnable::run);
    }

    public <T> T execute(Supplier<T> compilation)
    {
        requireNonNull(compilation, "compilation is null");
        FutureTask<T> task = new FutureTask<>(compilation::get);
        tasks.add(task);
        try {
            try {
                executor.execute(task);
            }
            catch (java.util.concurrent.RejectedExecutionException failure) {
                throw new TrinoException(HOGQL_COMPILATION_QUEUE_FULL, "HogQL compilation capacity is exhausted; retry later", failure);
            }

            try {
                if (timeoutMillis.isPresent()) {
                    return task.get(timeoutMillis.orElseThrow(), MILLISECONDS);
                }
                return task.get();
            }
            catch (InterruptedException failure) {
                task.cancel(true);
                Thread.currentThread().interrupt();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "HogQL compilation was interrupted", failure);
            }
            catch (CancellationException failure) {
                throw new TrinoException(HOGQL_COMPILATION_QUEUE_FULL, "HogQL compilation capacity is unavailable; retry later", failure);
            }
            catch (TimeoutException failure) {
                task.cancel(true);
                throw new TrinoException(HOGQL_COMPILATION_TIMEOUT, "HogQL compilation exceeded its time limit; retry later", failure);
            }
            catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "HogQL compilation failed", cause);
            }
        }
        finally {
            tasks.remove(task);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        ownedExecutor.ifPresent(executor -> {
            executor.shutdownNow();
            tasks.forEach(task -> task.cancel(true));
        });
    }

    boolean isShutdown()
    {
        return ownedExecutor.map(ThreadPoolExecutor::isShutdown).orElse(false);
    }
}
