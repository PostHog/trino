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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.hogql.parser.HogQlParser;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_UNSUPPORTED_FEATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlFunctionResolver
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);

    private final HogQlParser parser = new HogQlParser();
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testMapsStockAndCompatibilityFunctionsToQualifiedTrinoNames()
    {
        HogQlQuery query = resolve(
                "SELECT hogUpper('one'), hogCompat('two')",
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false),
                function("hogCompat", FunctionKind.SCALAR, FunctionImplementation.UDF, List.of("ducklake", "compat", "hog_compat"), signature(1), false, false, false, false));

        assertThat(TrinoAstFactory.createStatement(query, Map.of())).isEqualTo(sqlParser.createStatement(
                "SELECT system.builtin.upper('one'), ducklake.compat.hog_compat('two')"));
    }

    @Test
    public void testCompilerPinsOnceAndResolvesUserFunctions()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlSemanticCatalogSnapshot snapshot = snapshot(List.of(
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false)));
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(snapshot);
        });

        HogQlCompilationResult result = new HogQlCompiler().compile(envelope("SELECT hogUpper('one')"), Optional.of(context));

        assertThat(pins).hasValue(1);
        assertThat(result.catalogGeneration()).hasValue(7);
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement("SELECT system.builtin.upper('one')"));
    }

    @Test
    public void testCompilerPreservesOrdinaryFunctionsWithoutCatalogContext()
    {
        HogQlCompilationResult result = new HogQlCompiler().compile(envelope("SELECT ordinary(1)"));

        assertThat(result.catalogGeneration()).isEmpty();
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement("SELECT ordinary(1)"));
    }

    @ParameterizedTest
    @MethodSource("nestedFunctionQueries")
    public void testResolvesFunctionsThroughoutQueryTree(String query, String expected)
    {
        HogQlQuery resolved = resolve(
                query,
                function("hogUpper", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("system", "builtin", "upper"), signature(1), false, false, false, false));

        assertThat(TrinoAstFactory.createStatement(resolved, Map.of())).isEqualTo(sqlParser.createStatement(expected));
    }

    private static Stream<Arguments> nestedFunctionQueries()
    {
        return Stream.of(
                Arguments.of(
                        "WITH cte AS (SELECT hogUpper('cte') AS value) " +
                                "SELECT hogUpper(value) FROM cte WHERE hogUpper(value) = 'x' " +
                                "GROUP BY hogUpper(value) HAVING hogUpper(value) = 'y' ORDER BY hogUpper(value)",
                        "WITH cte AS (SELECT system.builtin.upper('cte') AS value) " +
                                "SELECT system.builtin.upper(value) FROM cte WHERE system.builtin.upper(value) = 'x' " +
                                "GROUP BY system.builtin.upper(value) HAVING system.builtin.upper(value) = 'y' ORDER BY system.builtin.upper(value)"),
                Arguments.of(
                        "SELECT hogUpper('left') UNION ALL SELECT hogUpper('right')",
                        "SELECT system.builtin.upper('left') UNION ALL SELECT system.builtin.upper('right')"),
                Arguments.of(
                        "SELECT * FROM (VALUES (hogUpper('value')))",
                        "SELECT * FROM (VALUES (system.builtin.upper('value')))"));
    }

    @Test
    public void testAcceptsExactAndVariadicArities()
    {
        FunctionCapabilityDefinition exact = function("exact", FunctionKind.SCALAR, FunctionImplementation.STOCK, List.of("exact"), signature(2), false, false, false, false);
        FunctionCapabilityDefinition variadic = function(
                "variadic",
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of("variadic"),
                new FunctionSignature(List.of("varchar", "varchar"), "varchar", true),
                false,
                false,
                false,
                false);

        resolve("SELECT exact('one', 'two'), variadic('one'), variadic('one', 'two', 'three')", exact, variadic);
    }

    @Test
    public void testMapsWindowFunctionsAndPreservesWindowSpecifications()
    {
        HogQlQuery query = resolve(
                "SELECT hogRank(value) OVER (PARTITION BY team_id ORDER BY timestamp ROWS CURRENT ROW)",
                function("hogRank", FunctionKind.WINDOW, FunctionImplementation.STOCK, List.of("analytics", "rank_value"), signature(1), false, false, false, true));

        assertThat(TrinoAstFactory.createStatement(query, Map.of())).isEqualTo(sqlParser.createStatement(
                "SELECT analytics.rank_value(value) OVER (PARTITION BY team_id ORDER BY timestamp ROWS CURRENT ROW)"));
    }

    @Test
    public void testRejectsCallsOutsideEveryDeclaredArity()
    {
        FunctionCapabilityDefinition function = function(
                "exact",
                FunctionKind.SCALAR,
                FunctionImplementation.STOCK,
                List.of("exact"),
                List.of(signature(1), signature(3)),
                false,
                false,
                false,
                false);

        assertResolutionError("SELECT exact('one', 'two')", function, "HogQL function exact does not accept 2 arguments");
    }

    @ParameterizedTest
    @MethodSource("unsupportedInvocationFeatures")
    public void testRejectsUnsupportedInvocationFeatures(
            String query,
            FunctionCapabilityDefinition function,
            String message)
    {
        assertUnsupportedError(query, function, message);
    }

    private static Stream<Arguments> unsupportedInvocationFeatures()
    {
        return Stream.of(
                Arguments.of(
                        "SELECT aggregate(DISTINCT value)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), false, false, false, true),
                        "HogQL function aggregate does not support DISTINCT"),
                Arguments.of(
                        "SELECT aggregate(value ORDER BY value)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), true, false, false, true),
                        "HogQL function aggregate does not support ORDER BY"),
                Arguments.of(
                        "SELECT aggregate(value) FILTER (WHERE true)",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), true, true, false, true),
                        "HogQL function aggregate does not support FILTER"),
                Arguments.of(
                        "SELECT windowOnly(value)",
                        function("windowOnly", FunctionKind.WINDOW, FunctionImplementation.STOCK, List.of("window_only"), signature(1), false, false, false, true),
                        "HogQL window function windowOnly requires an OVER clause"),
                Arguments.of(
                        "SELECT aggregate(value) OVER ()",
                        function("aggregate", FunctionKind.AGGREGATE, FunctionImplementation.STOCK, List.of("aggregate"), signature(1), false, false, false, false),
                        "HogQL function aggregate does not support OVER"),
                Arguments.of(
                        "SELECT tableOnly(value)",
                        function("tableOnly", FunctionKind.TABLE, FunctionImplementation.STOCK, List.of("table_only"), signature(1), false, false, false, false),
                        "HogQL table function tableOnly cannot be used as an expression"));
    }

    @Test
    public void testRejectsUnknownAndRewriteOnlyFunctionsAtCallLocation()
    {
        assertResolutionError("SELECT missing('one')", "Unknown HogQL function: missing");

        FunctionCapabilityDefinition rewrite = function(
                "rewriteMe",
                FunctionKind.SCALAR,
                FunctionImplementation.REWRITE,
                List.of(),
                signature(1),
                false,
                false,
                false,
                false);
        assertUnsupportedError("SELECT rewriteMe('one')", rewrite, "HogQL function rewriteMe requires a compiler rewrite");
    }

    private HogQlQuery resolve(String query, FunctionCapabilityDefinition... functions)
    {
        return HogQlFunctionResolver.resolve(new PinnedSnapshot(snapshot(List.of(functions))), parser.parseStatement(query));
    }

    private void assertResolutionError(String query, FunctionCapabilityDefinition function, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query, function));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private void assertResolutionError(String query, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private void assertUnsupportedError(String query, FunctionCapabilityDefinition function, String message)
    {
        TrinoException exception = catchThrowableOfType(TrinoException.class, () -> resolve(query, function));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_UNSUPPORTED_FEATURE.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: " + message);
    }

    private static FunctionCapabilityDefinition function(
            String name,
            FunctionKind kind,
            FunctionImplementation implementation,
            List<String> trinoName,
            FunctionSignature signature,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow)
    {
        return function(name, kind, implementation, trinoName, List.of(signature), supportsDistinct, supportsOrderBy, supportsFilter, supportsWindow);
    }

    private static FunctionCapabilityDefinition function(
            String name,
            FunctionKind kind,
            FunctionImplementation implementation,
            List<String> trinoName,
            List<FunctionSignature> signatures,
            boolean supportsDistinct,
            boolean supportsOrderBy,
            boolean supportsFilter,
            boolean supportsWindow)
    {
        return new FunctionCapabilityDefinition(
                name,
                kind,
                implementation,
                trinoName.stream().map(value -> new PhysicalIdentifier(value, false)).toList(),
                signatures,
                true,
                supportsDistinct,
                supportsOrderBy,
                supportsFilter,
                supportsWindow);
    }

    private static FunctionSignature signature(int arity)
    {
        return new FunctionSignature(Stream.generate(() -> "varchar").limit(arity).toList(), "varchar", false);
    }

    private static HogQlSemanticCatalogSnapshot snapshot(List<FunctionCapabilityDefinition> functions)
    {
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                7,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                functions,
                List.of());
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
}
