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
package io.trino.hogql.compiler;

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.sql.tree.Statement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkHogQlCompiler
{
    private static final String SIMPLE_QUERY = "SELECT event, distinct_id FROM events WHERE team_id = 42";
    private static final String JOIN_WINDOW_QUERY =
            """
            SELECT e.event, p.person_id,
                row_number() OVER (PARTITION BY e.distinct_id ORDER BY e.timestamp) AS row_number
            FROM events AS e
            LEFT JOIN persons AS p ON e.distinct_id = p.distinct_id
            WHERE e.event = 'signup'
            """;
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("ducklake", false);
    private static final HogQlSemanticCatalogSnapshot SEMANTIC_SNAPSHOT = semanticSnapshot();
    private static final Optional<HogQlSemanticCatalogContext> SEMANTIC_CONTEXT = Optional.of(
            new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SEMANTIC_SNAPSHOT)));
    private static final HogQlCompileEnvelope SEMANTIC_QUERY = new HogQlCompileEnvelope(
            "SELECT lowerEvent FROM events WHERE event = 'signup'",
            HogQlCompileEnvelope.PROTOCOL_VERSION,
            HogQlLanguageContract.current().languageVersion(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            OptionalLong.of(1));

    private final HogQlCompiler compiler = new HogQlCompiler();

    @Benchmark
    public Statement compileSimpleQuery()
    {
        return compiler.compile(SIMPLE_QUERY);
    }

    @Benchmark
    public Statement compileJoinWindowQuery()
    {
        return compiler.compile(JOIN_WINDOW_QUERY);
    }

    @Benchmark
    public HogQlCompilationResult compileSemanticCatalogQuery()
    {
        return compiler.compile(SEMANTIC_QUERY, SEMANTIC_CONTEXT);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkHogQlCompiler.class)
                .withOptions(options -> options.addProfiler(GCProfiler.class))
                .run();
    }

    private static HogQlSemanticCatalogSnapshot semanticSnapshot()
    {
        LogicalFieldDefinition event = new LogicalFieldDefinition(
                "event",
                new PhysicalIdentifier("event_name", false),
                "varchar",
                LogicalType.STRING,
                false,
                true);
        LogicalFieldDefinition distinctId = new LogicalFieldDefinition(
                "distinct_id",
                new PhysicalIdentifier("distinct_id", false),
                "varchar",
                LogicalType.STRING,
                false,
                true);
        LogicalTableDefinition events = new LogicalTableDefinition(
                "events",
                new PhysicalQualifiedName(
                        CATALOG,
                        new PhysicalIdentifier("analytics", false),
                        new PhysicalIdentifier("events_data", false)),
                List.of(event, distinctId),
                List.of(),
                List.of());
        ExpressionFieldDefinition lowerEvent = new ExpressionFieldDefinition(
                "events",
                "lowerEvent",
                "varchar",
                LogicalType.STRING,
                false,
                true,
                new FunctionCallRecipe("lower", List.of(new FieldReferenceRecipe("events", "event"))));
        FunctionCapabilityDefinition lower = new FunctionCapabilityDefinition(
                "lower",
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of(new PhysicalIdentifier("lower", false)),
                List.of(new FunctionSignature(List.of("varchar"), "varchar", false)),
                true,
                false,
                false,
                false,
                false);
        return new HogQlSemanticCatalogSnapshot(
                HogQlSemanticCatalogSnapshot.PROTOCOL_VERSION,
                HogQlSemanticCatalogSnapshot.SCHEMA_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                1,
                List.of(events),
                List.of(lowerEvent),
                List.of(),
                List.of(),
                List.of(),
                List.of(lower),
                List.of());
    }
}
