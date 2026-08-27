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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.CastRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ExpressionFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FieldReferenceRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCallRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionCapabilityDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionImplementation;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.FunctionSignature;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralEncoding;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LiteralRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.MaterializedViewReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.OperatorRecipe;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.ReferencedField;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationKind;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.RelationReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SavedQueryReference;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.SemanticOperator;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.TypedLiteral;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.VirtualProjection;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.VirtualTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_COMPILER_LIMIT_EXCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlSemanticExpansion
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = snapshot();

    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testExpandsEveryExpressionRecipeToStockAst()
    {
        HogQlCompilationResult result = compile("SELECT constant, upperEvent, added, castEvent, missingEvent FROM events");

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT CAST(7 AS bigint) AS constant, " +
                        "system.builtin.upper(event_name) AS upperEvent, " +
                        "CAST(7 AS bigint) + CAST(1 AS bigint) AS added, " +
                        "CAST(event_name AS varchar) AS castEvent, " +
                        "event_name IS NULL AS missingEvent " +
                        "FROM analytics.data.raw_events"));
    }

    @Test
    public void testExpandsVirtualSavedAndMaterializedRelationsWithDeclaredOutputs()
    {
        assertThat(compile("SELECT * FROM event_view").statement()).isEqualTo(sqlParser.createStatement(
                "SELECT \"eventTitle\" AS \"eventTitle\", \"constant\" AS \"constant\" " +
                        "FROM (SELECT system.builtin.upper(event_name) AS \"eventTitle\", CAST(7 AS bigint) AS \"constant\" " +
                        "FROM analytics.data.raw_events)"));
        assertThat(compile("SELECT * FROM saved_view").statement()).isEqualTo(sqlParser.createStatement(
                "SELECT \"eventTitle\" AS \"eventTitle\" FROM (" +
                        "SELECT \"eventTitle\" AS \"eventTitle\" FROM (" +
                        "SELECT system.builtin.upper(event_name) AS \"eventTitle\", CAST(7 AS bigint) AS \"constant\" " +
                        "FROM analytics.data.raw_events))"));
        assertThat(compile("SELECT * FROM daily_events").statement()).isEqualTo(sqlParser.createStatement(
                "SELECT \"day\" AS \"day\" FROM analytics.data.daily_events"));
    }

    @Test
    public void testPinsOnceAcrossRecursiveExpansionAndPreservesPlaceholderOrder()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(SNAPSHOT);
        });
        HogQlCompileEnvelope envelope = new HogQlCompileEnvelope(
                "SELECT {first}, eventTitle, {second} FROM saved_view",
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(
                        "first", new HogQlTypedValue("bigint", new HogQlTypedValue.NumberValue("1")),
                        "second", new HogQlTypedValue("bigint", new HogQlTypedValue.NumberValue("2"))),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.of(7));

        HogQlCompilationResult result = compiler.compile(envelope, Optional.of(context));

        assertThat(pins).hasValue(1);
        assertThat(result.catalogGeneration()).hasValue(7);
        assertThat(result.parameterNames()).containsExactly("first", "second");
    }

    @Test
    public void testBoundsRecursiveExpressionExpansionAtOriginalLocation()
    {
        List<ExpressionFieldDefinition> expressions = new ArrayList<>();
        expressions.add(expression("branch0", new LiteralRecipe(new TypedLiteral("bigint", LiteralEncoding.INTEGER, "1"))));
        for (int index = 1; index <= 14; index++) {
            String previous = "branch" + (index - 1);
            expressions.add(expression("branch" + index, new OperatorRecipe(SemanticOperator.ADD, List.of(
                    new FieldReferenceRecipe("events", previous),
                    new FieldReferenceRecipe("events", previous)))));
        }
        HogQlSemanticCatalogSnapshot expansiveSnapshot = new HogQlSemanticCatalogSnapshot(
                SNAPSHOT.protocolVersion(),
                SNAPSHOT.schemaVersion(),
                SNAPSHOT.languageVersion(),
                SNAPSHOT.catalog(),
                SNAPSHOT.generation(),
                SNAPSHOT.logicalTables(),
                expressions,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of());

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compile("SELECT branch14 FROM events", expansiveSnapshot));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_COMPILER_LIMIT_EXCEEDED.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: HogQL semantic expansion exceeded node limit");
    }

    private HogQlCompilationResult compile(String query)
    {
        return compile(query, SNAPSHOT);
    }

    private HogQlCompilationResult compile(String query, HogQlSemanticCatalogSnapshot snapshot)
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(snapshot));
        return compiler.compile(new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                OptionalLong.of(7)), Optional.of(context));
    }

    private static HogQlSemanticCatalogSnapshot snapshot()
    {
        LogicalFieldDefinition event = new LogicalFieldDefinition("event", new PhysicalIdentifier("event_name", false), "varchar", LogicalType.STRING, false, true);
        List<ExpressionFieldDefinition> expressions = List.of(
                expression("constant", new LiteralRecipe(new TypedLiteral("bigint", LiteralEncoding.INTEGER, "7"))),
                expression("upperEvent", new FunctionCallRecipe("hogUpper", List.of(new FieldReferenceRecipe("events", "event")))),
                expression("added", new OperatorRecipe(SemanticOperator.ADD, List.of(
                        new FieldReferenceRecipe("events", "constant"),
                        new LiteralRecipe(new TypedLiteral("bigint", LiteralEncoding.INTEGER, "1"))))),
                expression("castEvent", new CastRecipe(new FieldReferenceRecipe("events", "event"), "varchar")),
                expression("missingEvent", new OperatorRecipe(SemanticOperator.IS_NULL, List.of(new FieldReferenceRecipe("events", "event")))));
        ReferencedField eventTitle = new ReferencedField("eventTitle", "varchar", LogicalType.STRING, false, true);
        return new HogQlSemanticCatalogSnapshot(
                1,
                2,
                HogQlLanguageContract.current().languageVersion(),
                CATALOG,
                7,
                List.of(new LogicalTableDefinition(
                        "events",
                        physicalName("raw_events"),
                        List.of(event),
                        List.of(),
                        List.of())),
                expressions,
                List.of(new VirtualTableDefinition(
                        "event_view",
                        new RelationReference(RelationKind.LOGICAL_TABLE, "events"),
                        List.of(
                                new VirtualProjection("eventTitle", "upperEvent", true),
                                new VirtualProjection("constant", "constant", true)))),
                List.of(new SavedQueryReference(
                        "saved_view",
                        "saved-7",
                        new RelationReference(RelationKind.VIRTUAL_TABLE, "event_view"),
                        List.of(eventTitle))),
                List.of(new MaterializedViewReference(
                        "daily_events",
                        physicalName("daily_events"),
                        List.of(new ReferencedField("day", "date", LogicalType.DATE, false, true)))),
                List.of(new FunctionCapabilityDefinition(
                        "hogUpper",
                        FunctionKind.SCALAR,
                        FunctionImplementation.STOCK,
                        List.of(
                                new PhysicalIdentifier("system", false),
                                new PhysicalIdentifier("builtin", false),
                                new PhysicalIdentifier("upper", false)),
                        List.of(new FunctionSignature(List.of("varchar"), "varchar", false)),
                        true,
                        false,
                        false,
                        false,
                        false)),
                List.of());
    }

    private static ExpressionFieldDefinition expression(String name, HogQlSemanticCatalogSnapshot.ExpressionRecipe recipe)
    {
        return new ExpressionFieldDefinition("events", name, "bigint", LogicalType.INTEGER, false, true, recipe);
    }

    private static PhysicalQualifiedName physicalName(String table)
    {
        return new PhysicalQualifiedName(CATALOG, new PhysicalIdentifier("data", false), new PhysicalIdentifier(table, false));
    }
}
