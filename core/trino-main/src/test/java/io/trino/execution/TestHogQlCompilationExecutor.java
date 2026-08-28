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

import io.airlift.units.Duration;
import io.trino.hogql.HogQlCompilationEvent;
import io.trino.hogql.HogQlConfig;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.execution.QuerySubmission.hogQl;
import static io.trino.hogql.HogQlCompilationEvent.Outcome.INSUFFICIENT_RESOURCES;
import static io.trino.hogql.HogQlCompilationEvent.Phase.COMPILATION;
import static io.trino.hogql.HogQlCoordinatorErrorCode.HOGQL_COMPILATION_QUEUE_FULL;
import static io.trino.hogql.HogQlCoordinatorErrorCode.HOGQL_COMPILATION_TIMEOUT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHogQlCompilationExecutor
{
    @Test
    public void testCompilationRunsOnDedicatedWorker()
    {
        HogQlCompilationExecutor executor = new HogQlCompilationExecutor(config(1, 0));
        try {
            assertThat(executor.execute(() -> Thread.currentThread().getName()))
                    .startsWith("hogql-compilation-");
        }
        finally {
            executor.shutdown();
        }
    }

    @Test
    public void testSaturationRejectsHogQlWithoutAffectingTrinoSql()
            throws Exception
    {
        HogQlCompilationExecutor executor = new HogQlCompilationExecutor(config(1, 0));
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        List<HogQlCompilationEvent> events = new ArrayList<>();
        QueryPreparer queryPreparer = new QueryPreparer(
                new SqlParser(),
                new HogQlCompiler(),
                events::add,
                Optional.empty(),
                executor);

        ExecutorService caller = newSingleThreadExecutor();
        try {
            Future<?> occupiedWorker = caller.submit(() -> executor.execute(() -> {
                workerStarted.countDown();
                await(releaseWorker);
                return null;
            }));
            assertThat(workerStarted.await(10, SECONDS)).isTrue();

            try {
                assertThatThrownBy(() -> queryPreparer.prepareQuery(testSessionBuilder().build(), hogQl(envelope("SELECT 1"))))
                        .isInstanceOfSatisfying(TrinoException.class, exception -> {
                            assertThat(exception.getErrorCode()).isEqualTo(HOGQL_COMPILATION_QUEUE_FULL.toErrorCode());
                            assertThat(exception).hasMessageContaining("retry");
                        });
                assertThat(queryPreparer.prepareQuery(testSessionBuilder().build(), "SELECT 1").getStatement()).isNotNull();

                assertThat(events).singleElement().satisfies(event -> {
                    assertThat(event.outcome()).isEqualTo(INSUFFICIENT_RESOURCES);
                    assertThat(event.failedPhase()).contains(COMPILATION);
                });
            }
            finally {
                releaseWorker.countDown();
            }

            occupiedWorker.get(10, SECONDS);
        }
        finally {
            releaseWorker.countDown();
            caller.shutdownNow();
            executor.shutdown();
        }
    }

    @Test
    public void testShutdownInterruptsWorkAndRejectsNewCompilations()
            throws Exception
    {
        HogQlCompilationExecutor executor = new HogQlCompilationExecutor(config(1, 1));
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);

        ExecutorService caller = newSingleThreadExecutor();
        try {
            Future<?> occupiedWorker = caller.submit(() -> executor.execute(() -> {
                workerStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                }
                catch (InterruptedException _) {
                    workerInterrupted.countDown();
                    Thread.currentThread().interrupt();
                }
                return null;
            }));
            assertThat(workerStarted.await(10, SECONDS)).isTrue();

            executor.shutdown();

            assertThat(workerInterrupted.await(10, SECONDS)).isTrue();
            try {
                occupiedWorker.get(10, SECONDS);
            }
            catch (ExecutionException exception) {
                assertThat(exception.getCause())
                        .isInstanceOfSatisfying(TrinoException.class, cause ->
                                assertThat(cause.getErrorCode()).isEqualTo(HOGQL_COMPILATION_QUEUE_FULL.toErrorCode()));
            }
            assertThat(occupiedWorker).isDone();
            assertThat(executor.isShutdown()).isTrue();
            assertThatThrownBy(() -> executor.execute(() -> null))
                    .isInstanceOfSatisfying(TrinoException.class, exception ->
                            assertThat(exception.getErrorCode()).isEqualTo(HOGQL_COMPILATION_QUEUE_FULL.toErrorCode()));
        }
        finally {
            caller.shutdownNow();
            executor.shutdown();
        }
    }

    @Test
    public void testCompilationTimeoutInterruptsWorker()
            throws Exception
    {
        HogQlCompilationExecutor executor = new HogQlCompilationExecutor(config(1, 0)
                .setCompilationTimeout(new Duration(10, MILLISECONDS)));
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        try {
            assertThatThrownBy(() -> executor.execute(() -> {
                awaitInterruption(workerInterrupted);
                return null;
            }))
                    .isInstanceOfSatisfying(TrinoException.class, exception -> {
                        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_COMPILATION_TIMEOUT.toErrorCode());
                        assertThat(exception).hasMessageContaining("time limit");
                    });
            assertThat(workerInterrupted.await(10, SECONDS)).isTrue();
        }
        finally {
            executor.shutdown();
        }
    }

    private static HogQlConfig config(int threads, int queueCapacity)
    {
        return new HogQlConfig()
                .setCompilationThreads(threads)
                .setCompilationQueueCapacity(queueCapacity);
    }

    private static HogQlCompileEnvelope envelope(String query)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.empty());
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    private static void awaitInterruption(CountDownLatch interrupted)
    {
        try {
            new CountDownLatch(1).await();
        }
        catch (InterruptedException exception) {
            interrupted.countDown();
            Thread.currentThread().interrupt();
        }
    }
}
