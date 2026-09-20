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
package io.trino.server;

import com.google.common.collect.ImmutableList;

import java.util.IdentityHashMap;
import java.util.Set;

import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.joining;

/**
 * Describes a failure by the types it is made of, without its messages.
 *
 * <p>Readiness and catalog synchronization run against things that hold credentials: a JDBC driver
 * whose exception can quote the connection URL, a file reader that names a password file, an
 * authorization backend that names its endpoint. Those messages are useful and also the reason this
 * exists: the ordinary log line says what kind of failure happened, and the message and stack trace
 * are logged at debug level, where an operator has to ask for them.
 */
public final class FailureSummary
{
    private static final int MAX_CAUSES = 5;

    private FailureSummary() {}

    public static String summarize(Throwable failure)
    {
        Set<Throwable> seen = newSetFromMap(new IdentityHashMap<>());
        ImmutableList.Builder<String> types = ImmutableList.builder();
        Throwable current = failure;
        int depth = 0;
        while (current != null && depth < MAX_CAUSES && seen.add(current)) {
            types.add(current.getClass().getName());
            current = current.getCause();
            depth++;
        }
        String chain = types.build().stream().collect(joining(" caused by "));
        if (depth == MAX_CAUSES && current != null) {
            // Ended because the chain is long, not because it closed on itself
            return chain + " caused by ...";
        }
        return chain;
    }
}
