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
package io.trino.plugin.ducklake;

import com.google.common.base.Throwables;
import io.airlift.concurrent.Threads;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

/**
 * Runs the reads of the files of one statement concurrently.
 * <p>
 * Inspecting a file waits on the file system for its length and the tail holding its footer, and
 * spends little time computing, so the threads mostly wait. A statement owns a pool for the time
 * of its reads only, and every read of the pool has finished or been cancelled when the call
 * returns, whatever the outcome.
 */
final class DuckLakeParallelFileReads
{
    private DuckLakeParallelFileReads() {}

    /**
     * Runs the reads, at most {@code maxConcurrentReads} at a time, and returns their results in
     * the order of the reads. A read that throws an unchecked exception fails the call with that
     * exception. Any other failure surfaces as a {@link TrinoException} with the given code and
     * message. An interrupt cancels the reads and surfaces as {@code USER_CANCELED}.
     */
    static <T> List<T> readAll(
            List<Callable<T>> reads,
            int maxConcurrentReads,
            String threadNameFormat,
            String interruptedMessage,
            ErrorCodeSupplier failureCode,
            String failureMessage)
    {
        requireNonNull(reads, "reads is null");
        if (reads.isEmpty()) {
            return List.of();
        }
        var executor = Executors.newFixedThreadPool(Math.min(maxConcurrentReads, reads.size()), Threads.daemonThreadsNamed(threadNameFormat));
        List<Future<T>> pending = new ArrayList<>();
        try {
            for (Callable<T> read : reads) {
                pending.add(executor.submit(read));
            }
            List<T> results = new ArrayList<>(pending.size());
            for (Future<T> future : pending) {
                results.add(future.get());
            }
            return results;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(StandardErrorCode.USER_CANCELED, interruptedMessage, e);
        }
        catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new TrinoException(failureCode, failureMessage, e.getCause());
        }
        finally {
            pending.forEach(future -> future.cancel(true));
            executor.shutdownNow();
        }
    }
}
