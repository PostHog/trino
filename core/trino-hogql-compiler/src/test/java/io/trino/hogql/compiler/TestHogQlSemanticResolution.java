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
            HogQlLanguageContract.current().languageVersion(),
            CATALOG,
            7,
            List.of(new LogicalTableDefinition(
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
                    List.of())));

    private final HogQlCompiler compiler = new HogQlCompiler();

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
