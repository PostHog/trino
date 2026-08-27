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
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalType;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalQualifiedName;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinRequest;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.HogQlLanguageContract;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestHogQlSemanticResolution
{
    private static final PhysicalIdentifier CATALOG = new PhysicalIdentifier("analytics", false);
    private static final HogQlSemanticCatalogSnapshot SNAPSHOT = new HogQlSemanticCatalogSnapshot(
            1,
            2,
            HogQlLanguageContract.current().languageVersion(),
            CATALOG,
            7,
            List.of(
                    new LogicalTableDefinition(
                            "events",
                            new PhysicalQualifiedName(
                                    CATALOG,
                                    new PhysicalIdentifier("Hog Data", true),
                                    new PhysicalIdentifier("raw-events", true)),
                            List.of(
                                    new LogicalFieldDefinition("event", new PhysicalIdentifier("event_name", false), "varchar", LogicalType.STRING, false, true),
                                    new LogicalFieldDefinition("personId", new PhysicalIdentifier("Person ID", true), "varchar", LogicalType.STRING, false, true),
                                    new LogicalFieldDefinition("hidden", new PhysicalIdentifier("hidden", false), "bigint", LogicalType.INTEGER, true, false)),
                            List.of(),
                            List.of()),
                    new LogicalTableDefinition(
                            "persons",
                            new PhysicalQualifiedName(
                                    CATALOG,
                                    new PhysicalIdentifier("Hog Data", true),
                                    new PhysicalIdentifier("raw-persons", true)),
                            List.of(
                                    new LogicalFieldDefinition("personId", new PhysicalIdentifier("person_id", false), "varchar", LogicalType.STRING, false, true),
                                    new LogicalFieldDefinition("name", new PhysicalIdentifier("full_name", false), "varchar", LogicalType.STRING, false, true)),
                            List.of(),
                            List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(new FunctionCapabilityDefinition(
                    "count",
                    FunctionKind.AGGREGATE,
                    FunctionImplementation.STOCK,
                    List.of(new PhysicalIdentifier("count", false)),
                    List.of(new FunctionSignature(List.of(), "bigint", false)),
                    true,
                    true,
                    true,
                    true,
                    true)),
            List.of());

    private final HogQlCompiler compiler = new HogQlCompiler();
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testPinsAndResolvesLogicalTableFieldsAndStar()
    {
        AtomicReference<PinRequest> request = new AtomicReference<>();
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, pinRequest -> {
            request.set(pinRequest);
            return new PinnedSnapshot(SNAPSHOT);
        });

        HogQlCompilationResult result = compiler.compile(
                envelope("SELECT *, event FROM events WHERE personId = 'synthetic' GROUP BY event HAVING count(*) > 0 ORDER BY event", OptionalLong.of(7)),
                Optional.of(context));

        assertThat(request.get()).isEqualTo(new PinRequest(CATALOG, HogQlLanguageContract.current().languageVersion(), OptionalLong.of(7)));
        assertThat(result.catalogGeneration()).hasValue(7);
        QuerySpecification query = querySpecification(result);
        assertThat(((Table) query.getFrom().orElseThrow()).getName().getOriginalParts())
                .extracting(Identifier::getValue)
                .containsExactly("analytics", "Hog Data", "raw-events");
        assertThat(((Table) query.getFrom().orElseThrow()).getName().getOriginalParts())
                .extracting(Identifier::isDelimited)
                .containsExactly(false, true, true);
        assertThat(query.getSelect().getSelectItems()).hasSize(3);
        assertThat(query.getSelect().getSelectItems())
                .allSatisfy(item -> assertThat(item).isInstanceOf(SingleColumn.class));
        assertThat(query.getSelect().getSelectItems().subList(0, 2))
                .extracting(item -> ((SingleColumn) item).getAlias().orElseThrow().getValue())
                .containsExactly("event", "personId");
        assertThat(query.getSelect().getSelectItems().subList(0, 2))
                .extracting(item -> ((Identifier) ((SingleColumn) item).getExpression()).getValue())
                .containsExactly("event_name", "Person ID");
    }

    @Test
    public void testResolvesLogicalFieldsInsideNamedWindows()
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));

        HogQlCompilationResult result = compiler.compile(
                envelope(
                        "SELECT count(*) OVER recent FROM events " +
                                "WINDOW recent AS (PARTITION BY event ORDER BY personId ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
                        OptionalLong.of(7)),
                Optional.of(context));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT count(*) OVER recent FROM analytics.\"Hog Data\".\"raw-events\" " +
                        "WINDOW recent AS (PARTITION BY event_name ORDER BY \"Person ID\" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"));
    }

    @Test
    public void testResolvesLogicalFieldsInsideCollectionSubscripts()
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));

        HogQlCompilationResult result = compiler.compile(
                envelope("SELECT [event][1], [personId][1], [event][1].label FROM events", OptionalLong.of(7)),
                Optional.of(context));

        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT ARRAY[event_name][1], ARRAY[\"Person ID\"][1], ARRAY[event_name][1].label " +
                        "FROM analytics.\"Hog Data\".\"raw-events\""));
    }

    @Test
    public void testCatalogIndependentAndPhysicalQueriesDoNotFetchSemanticMetadata()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(SNAPSHOT);
        });

        HogQlCompilationResult literal = compiler.compile(envelope("SELECT 1", OptionalLong.empty()), Optional.of(context));
        HogQlCompilationResult physical = compiler.compile(envelope("SELECT event_name FROM analytics.default.events", OptionalLong.empty()), Optional.of(context));

        assertThat(pins).hasValue(0);
        assertThat(literal.catalogGeneration()).isEmpty();
        assertThat(physical.catalogGeneration()).isEmpty();
    }

    @Test
    public void testCteScopeShadowsLogicalCatalogNamesWithoutFetchingMetadata()
    {
        AtomicInteger pins = new AtomicInteger();
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> {
            pins.incrementAndGet();
            return new PinnedSnapshot(SNAPSHOT);
        });

        HogQlCompilationResult result = compiler.compile(
                envelope("WITH events AS (SELECT 1 AS event), next AS (SELECT event FROM events) SELECT event FROM next", OptionalLong.empty()),
                Optional.of(context));

        assertThat(pins).hasValue(0);
        assertThat(result.catalogGeneration()).isEmpty();
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "WITH events AS (SELECT 1 AS event), next AS (SELECT event FROM events) SELECT event FROM next"));
    }

    @Test
    public void testUnknownLogicalFieldFailsAtOriginalLocation()
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile(envelope("SELECT missing FROM events", OptionalLong.empty()), Optional.of(context)));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, 8));
        assertThat(exception).hasMessage("line 1:8: Unknown HogQL field: missing");
    }

    @Test
    public void testResolvesAliasedLogicalTablesAndJoinCriteria()
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));

        HogQlCompilationResult result = compiler.compile(
                envelope("SELECT e.event, p.name FROM events e JOIN persons p ON e.personId = p.personId", OptionalLong.of(7)),
                Optional.of(context));

        assertThat(result.catalogGeneration()).hasValue(7);
        assertThat(result.statement()).isEqualTo(sqlParser.createStatement(
                "SELECT e.event_name AS event, p.full_name AS name " +
                        "FROM analytics.\"Hog Data\".\"raw-events\" e " +
                        "JOIN analytics.\"Hog Data\".\"raw-persons\" p ON e.\"Person ID\" = p.person_id"));
    }

    @Test
    public void testRejectsLogicalUsingWhenPhysicalColumnNamesDiffer()
    {
        HogQlSemanticCatalogContext context = new HogQlSemanticCatalogContext(CATALOG, _ -> new PinnedSnapshot(SNAPSHOT));
        String hogql = "SELECT e.event FROM events e JOIN persons p USING (personId)";

        TrinoException exception = catchThrowableOfType(
                TrinoException.class,
                () -> compiler.compile(envelope(hogql, OptionalLong.empty()), Optional.of(context)));

        assertThat(exception.getErrorCode()).isEqualTo(HOGQL_RESOLUTION_ERROR.toErrorCode());
        assertThat(exception.getLocation()).contains(new Location(1, hogql.lastIndexOf("personId") + 1));
        assertThat(exception).hasMessageContaining("HogQL USING field maps to different physical columns: personId");
    }

    private static QuerySpecification querySpecification(HogQlCompilationResult result)
    {
        return (QuerySpecification) ((Query) result.statement()).getQueryBody();
    }

    private static HogQlCompileEnvelope envelope(String query, OptionalLong generation)
    {
        return new HogQlCompileEnvelope(
                query,
                HogQlCompileEnvelope.PROTOCOL_VERSION,
                HogQlLanguageContract.current().languageVersion(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                generation);
    }
}
