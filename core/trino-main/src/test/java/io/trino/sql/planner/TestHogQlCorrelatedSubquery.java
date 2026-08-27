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
package io.trino.sql.planner;

import io.trino.hogql.compiler.HogQlCompilationResult;
import io.trino.hogql.compiler.HogQlCompileEnvelope;
import io.trino.hogql.compiler.HogQlCompiler;
import io.trino.hogql.compiler.HogQlSemanticCatalogContext;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.TrinoException;
import io.trino.sql.SqlFormatter;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlCorrelatedSubquery
        extends BasePlanTest
{
    private final HogQlCompiler compiler = new HogQlCompiler();

    @Test
    public void testAnalyzerBindsCorrelatedOuterColumn()
    {
        assertThatCode(() -> plan(compile(
                "SELECT o.orderkey FROM orders o " +
                        "WHERE o.custkey IN (" +
                        "SELECT c.custkey FROM customer c WHERE c.custkey = o.custkey)")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testAnalyzerBindsCorrelatedOuterColumnInScalarSubquery()
    {
        assertThatCode(() -> plan(compile(
                "SELECT o.orderkey, (" +
                        "SELECT c.custkey FROM customer c WHERE c.custkey = o.custkey) " +
                        "FROM orders o")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testAnalyzerBindsResolvedCorrelatedLogicalField()
    {
        assertThatCode(() -> plan(compileSemantic(
                "SELECT e.event FROM events e WHERE e.personId IN (" +
                        "SELECT p.personId FROM persons p WHERE p.personId = e.personId)")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testAnalyzerRejectsMissingCorrelatedOuterColumn()
    {
        AssertionError failure = catchThrowableOfType(
                AssertionError.class,
                () -> plan(compile(
                        "SELECT o.orderkey FROM orders o " +
                                "WHERE o.custkey IN (" +
                                "SELECT c.custkey FROM customer c WHERE c.custkey = missing.custkey)")));

        assertThat(failure.getCause())
                .isInstanceOfSatisfying(TrinoException.class, cause -> {
                    assertThat(cause.getErrorCode()).isEqualTo(COLUMN_NOT_FOUND.toErrorCode());
                    assertThat(cause).hasMessageContaining("Column 'missing.custkey' cannot be resolved");
                });
    }

    private String compile(String hogql)
    {
        return SqlFormatter.formatSql(compiler.compile(hogql));
    }

    private String compileSemantic(String hogql)
    {
        PhysicalIdentifier catalog = new PhysicalIdentifier(TEST_CATALOG_NAME, false);
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(
                catalog,
                _ -> new PinnedSnapshot(TestHogQlProjectionPruning.testingSnapshot()));
        HogQlCompilationResult result = compiler.compile(
                new HogQlCompileEnvelope(
                        hogql,
                        HogQlCompileEnvelope.PROTOCOL_VERSION,
                        HogQlLanguageContract.current().languageVersion(),
                        Map.of(),
                        Map.of(),
                        Map.of(),
                        Map.of(),
                        OptionalLong.of(1)),
                Optional.of(context));
        return SqlFormatter.formatSql(result.statement());
    }
}
