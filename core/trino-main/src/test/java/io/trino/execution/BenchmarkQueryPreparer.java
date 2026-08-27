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

import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.sql.parser.SqlParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkQueryPreparer
{
    private static final String QUERY = "SELECT event, properties FROM ducklake.default.events";

    private final SqlParser sqlParser = new SqlParser();
    private final QueryPreparer sqlOnlyQueryPreparer = new QueryPreparer(sqlParser);
    private final QueryPreparer hogQlCapableQueryPreparer = new QueryPreparer(sqlParser, new HogQlCompiler());

    @Benchmark
    public QueryPreparer.PreparedQuery prepareStandardSqlWithoutHogQl()
    {
        return sqlOnlyQueryPreparer.prepareQuery(TEST_SESSION, QUERY);
    }

    @Benchmark
    public QueryPreparer.PreparedQuery prepareStandardSqlWithHogQlAvailable()
    {
        return hogQlCapableQueryPreparer.prepareQuery(TEST_SESSION, QUERY);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkQueryPreparer.class).run();
    }
}
