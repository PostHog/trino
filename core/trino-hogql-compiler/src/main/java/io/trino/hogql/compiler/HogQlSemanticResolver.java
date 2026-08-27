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

import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalFieldDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.LogicalTableDefinition;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshot.PhysicalIdentifier;
import io.trino.hogql.compiler.catalog.HogQlSemanticCatalogSnapshotProvider.PinnedSnapshot;
import io.trino.hogql.parser.tree.HogQlQuery;
import io.trino.hogql.parser.tree.HogQlQuery.ArrayExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BetweenExpression;
import io.trino.hogql.parser.tree.HogQlQuery.BinaryExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseExpression;
import io.trino.hogql.parser.tree.HogQlQuery.CaseWhen;
import io.trino.hogql.parser.tree.HogQlQuery.CastExpression;
import io.trino.hogql.parser.tree.HogQlQuery.ColumnReference;
import io.trino.hogql.parser.tree.HogQlQuery.Expression;
import io.trino.hogql.parser.tree.HogQlQuery.ExpressionProjection;
import io.trino.hogql.parser.tree.HogQlQuery.FunctionCall;
import io.trino.hogql.parser.tree.HogQlQuery.Identifier;
import io.trino.hogql.parser.tree.HogQlQuery.InExpression;
import io.trino.hogql.parser.tree.HogQlQuery.IsNullExpression;
import io.trino.hogql.parser.tree.HogQlQuery.Literal;
import io.trino.hogql.parser.tree.HogQlQuery.Placeholder;
import io.trino.hogql.parser.tree.HogQlQuery.Projection;
import io.trino.hogql.parser.tree.HogQlQuery.SortItem;
import io.trino.hogql.parser.tree.HogQlQuery.Star;
import io.trino.hogql.parser.tree.HogQlQuery.TableReference;
import io.trino.hogql.parser.tree.HogQlQuery.TupleExpression;
import io.trino.hogql.parser.tree.HogQlQuery.UnaryExpression;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_RESOLUTION_ERROR;
import static java.util.Objects.requireNonNull;

final class HogQlSemanticResolver
{
    private final LogicalTableDefinition logicalTable;
    private final Map<String, LogicalFieldDefinition> fields;

    private HogQlSemanticResolver(LogicalTableDefinition logicalTable)
    {
        this.logicalTable = requireNonNull(logicalTable, "logicalTable is null");
        Map<String, LogicalFieldDefinition> fields = new HashMap<>();
        logicalTable.fields().forEach(field -> fields.put(canonical(field.name()), field));
        this.fields = Map.copyOf(fields);
    }

    public static Optional<ResolvedQuery> resolve(PinnedSnapshot snapshot, HogQlQuery query)
    {
        requireNonNull(snapshot, "snapshot is null");
        requireNonNull(query, "query is null");
        if (!(query.from().orElse(null) instanceof TableReference table) || table.parts().size() != 1) {
            return Optional.empty();
        }
        Optional<LogicalTableDefinition> logicalTable = snapshot.logicalTable(table.parts().getFirst().value());
        return logicalTable.map(definition -> new HogQlSemanticResolver(definition).resolveQuery(query, table));
    }

    private ResolvedQuery resolveQuery(HogQlQuery query, TableReference originalTable)
    {
        List<Projection> projections = new ArrayList<>();
        query.projections().forEach(projection -> projections.addAll(resolveProjection(projection)));
        HogQlQuery resolved = new HogQlQuery(
                query.distinct(),
                projections,
                Optional.of(resolveTable(originalTable)),
                query.where().map(this::resolveExpression),
                query.groupBy().stream().map(this::resolveExpression).toList(),
                query.having().map(this::resolveExpression),
                resolveSortItems(query.orderBy()),
                query.limit().map(this::resolveExpression),
                query.offset().map(this::resolveExpression),
                query.span());
        return new ResolvedQuery(resolved);
    }

    private List<Projection> resolveProjection(Projection projection)
    {
        return switch (projection) {
            case Star star -> logicalTable.fields().stream()
                    .filter(LogicalFieldDefinition::starVisible)
                    .map(field -> new ExpressionProjection(
                            physicalColumn(field, star.span()),
                            Optional.of(new Identifier(field.name(), true, star.span()))))
                    .map(Projection.class::cast)
                    .toList();
            case ExpressionProjection expressionProjection -> {
                Expression resolved = resolveExpression(expressionProjection.expression());
                Optional<Identifier> alias = expressionProjection.alias();
                if (alias.isEmpty() && expressionProjection.expression() instanceof ColumnReference reference) {
                    alias = Optional.of(reference.parts().getLast());
                }
                yield List.of(new ExpressionProjection(resolved, alias));
            }
        };
    }

    private TableReference resolveTable(TableReference table)
    {
        return new TableReference(
                List.of(
                        identifier(logicalTable.physicalTable().catalog(), table),
                        identifier(logicalTable.physicalTable().schema(), table),
                        identifier(logicalTable.physicalTable().table(), table)),
                table.span());
    }

    private List<SortItem> resolveSortItems(List<SortItem> sortItems)
    {
        return sortItems.stream()
                .map(sortItem -> new SortItem(resolveExpression(sortItem.expression()), sortItem.direction(), sortItem.nullPlacement(), sortItem.span()))
                .toList();
    }

    private Expression resolveExpression(Expression expression)
    {
        return switch (expression) {
            case ArrayExpression array -> new ArrayExpression(array.values().stream().map(this::resolveExpression).toList(), array.span());
            case BetweenExpression between -> new BetweenExpression(
                    resolveExpression(between.value()),
                    resolveExpression(between.min()),
                    resolveExpression(between.max()),
                    between.negated(),
                    between.predicateSpan(),
                    between.span());
            case BinaryExpression binary -> new BinaryExpression(
                    binary.operator(),
                    resolveExpression(binary.left()),
                    resolveExpression(binary.right()),
                    binary.span());
            case CaseExpression caseExpression -> new CaseExpression(
                    caseExpression.operand().map(this::resolveExpression),
                    caseExpression.whenClauses().stream()
                            .map(when -> new CaseWhen(resolveExpression(when.operand()), resolveExpression(when.result()), when.span()))
                            .toList(),
                    caseExpression.defaultValue().map(this::resolveExpression),
                    caseExpression.span());
            case CastExpression cast -> new CastExpression(resolveExpression(cast.value()), cast.type(), cast.safe(), cast.span());
            case ColumnReference reference -> resolveColumn(reference);
            case FunctionCall function -> new FunctionCall(
                    function.name(),
                    function.arguments().stream().map(this::resolveExpression).toList(),
                    function.distinct(),
                    resolveSortItems(function.orderBy()),
                    function.filter().map(this::resolveExpression),
                    function.span());
            case InExpression in -> new InExpression(
                    resolveExpression(in.value()),
                    in.values().stream().map(this::resolveExpression).toList(),
                    in.negated(),
                    in.predicateSpan(),
                    in.span());
            case IsNullExpression isNull -> new IsNullExpression(
                    resolveExpression(isNull.value()),
                    isNull.negated(),
                    isNull.predicateSpan(),
                    isNull.span());
            case Literal literal -> literal;
            case Placeholder placeholder -> placeholder;
            case TupleExpression tuple -> new TupleExpression(tuple.values().stream().map(this::resolveExpression).toList(), tuple.span());
            case UnaryExpression unary -> new UnaryExpression(unary.operator(), resolveExpression(unary.operand()), unary.span());
        };
    }

    private ColumnReference resolveColumn(ColumnReference reference)
    {
        List<Identifier> parts = reference.parts();
        if (parts.size() == 2 && !canonical(parts.getFirst().value()).equals(canonical(logicalTable.name()))) {
            throw resolutionError(reference, parts.getLast().value());
        }
        if (parts.size() > 2) {
            throw resolutionError(reference, parts.getLast().value());
        }
        String logicalName = parts.getLast().value();
        LogicalFieldDefinition field = fields.get(canonical(logicalName));
        if (field == null) {
            throw resolutionError(reference, logicalName);
        }
        return physicalColumn(field, reference.span());
    }

    private static ColumnReference physicalColumn(LogicalFieldDefinition field, HogQlQuery.SourceSpan span)
    {
        return new ColumnReference(List.of(new Identifier(field.physicalColumn().value(), field.physicalColumn().delimited(), span)), span);
    }

    private static Identifier identifier(PhysicalIdentifier identifier, TableReference source)
    {
        return new Identifier(identifier.value(), identifier.delimited(), source.span());
    }

    private static String canonical(String value)
    {
        return value.toLowerCase(Locale.ENGLISH);
    }

    private static TrinoException resolutionError(ColumnReference reference, String name)
    {
        return new TrinoException(
                HOGQL_RESOLUTION_ERROR,
                Optional.of(new Location(reference.span().startLine(), reference.span().startColumn())),
                "Unknown HogQL field: " + name,
                null);
    }

    record ResolvedQuery(HogQlQuery query)
    {
        ResolvedQuery
        {
            query = requireNonNull(query, "query is null");
        }
    }
}
